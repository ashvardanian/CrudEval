//! UStore benchmark for key-value records, JSON documents and graphs, on its NVMe or RAM engine.
//!
//! Requires a C++ compiler and CMake to build the engines. Every call runs on its worker's executor, in batches.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features ustore-backend \
//!     --bin crud-eval-ustore -- --records 100K --threads 4
//! ```

use std::{
    alloc::System,
    collections::BTreeMap,
    io::Write as _,
    path::{Path, PathBuf},
};

use clap::Parser;
use serde_json::json;
use ustore::{
    Arena, CollectionID, Column, DataDirectory, Database, Edge, Engine, Entry, Executor, FieldEntry, IOOptions, Json,
    Modality, Modification, NVMEOptions, On, RAMOptions, Role, Roles, Status, Text,
};

use crudeval::{
    backend::{
        directory_bytes, Backend, BackendCapabilities, BackendSession, BatchMode, DataModel, DocumentInput,
        DocumentOutput, DocumentPatch, DocumentRef, DocumentSession, Durability, GraphEdge, GraphInput, GraphOutput,
        GraphPatch, GraphSession, Key, KeysOutput, RecordInput, RecordOutput, Result, TransactionSession, VertexRef,
    },
    output, run, CommonArgs,
};

/// Deltas a transaction may buffer, the engine's ceiling; only a transaction's bytes are taken up front.
const TRANSACTION_MAX_DELTAS: usize = 1 << 20;

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
    /// UStore engine: nvme, or ram, which keeps its image in the data directory.
    #[arg(long, env = "CRUDEVAL_ENGINE", default_value = "nvme", value_parser = |text: &str| text.parse::<Engine>())]
    engine: Engine,
}

fn to_store(key: &Key) -> ustore::Key {
    ustore::Key::from(*key.as_bytes())
}
fn from_store(key: &ustore::Key) -> Key {
    Key::from_bytes(key.0 .0)
}
/// Edge identifiers tell apart the edges between two vertices, so each carries its slot.
fn slot_key(slot: u32) -> ustore::Key {
    ustore::Key::from(u64::from(slot))
}
fn slot_of(edge: &Edge) -> Result<u32> {
    u32::try_from(u128::from_be_bytes(edge.id.0 .0)).map_err(|_| "edge identifier is not a slot".into())
}
fn version_of(value: &[u8]) -> Result<u64> {
    Ok(u64::from_le_bytes(
        value.try_into().map_err(|_| "vertex version is not 8 bytes")?,
    ))
}

/// The rows of `keys` that `collection` holds, read without their values.
fn present<Parent>(
    view: &On<'_, Parent>,
    collection: CollectionID,
    keys: &[ustore::Key],
    arena: &mut Arena,
    rows: &mut Vec<usize, System>,
) -> Result<usize> {
    let presences = view.contains(collection, keys, arena).map_err(|e| e.to_string())?;
    rows.clear();
    rows.extend((0..keys.len()).filter(|&row| presences.get(row)));
    Ok(rows.len())
}

/// Copies the `rows` of `keys` into `found`.
fn gather_found(keys: &[ustore::Key], rows: &[usize], found: &mut Vec<ustore::Key, System>) {
    found.clear();
    found.extend(rows.iter().map(|&row| keys[row]));
}

/// The collection named `name`, created to hold `modality` when missing.
fn collection(database: &Database, executor: &Executor, name: &str, modality: Modality) -> Result<CollectionID> {
    let mut arena = Arena::new_in(executor).map_err(|e| e.to_string())?;
    match database.head().on(executor).collection_find(name, &mut arena) {
        Ok((collection, found)) if found == modality => Ok(collection),
        Ok((_, found)) => Err(format!("collection {name} holds {found}, not {modality}")),
        Err(failure) if failure.status == Status::NotFound => {
            database.collection_create(name, modality).map_err(|e| e.to_string())
        }
        Err(failure) => Err(failure.to_string()),
    }
}

struct UStoreBackend {
    // Declared before the executor, so it closes before the executor drains.
    database: Database,
    /// Opens the database; calls run on each worker's own, as one executor runs its calls in order.
    _executor: Executor,
    engine: Engine,
    executor_threads: usize,
    data_model: DataModel,
    /// Values, documents, or vertex versions.
    records: CollectionID,
    /// Graph edges, identified by their slots.
    edges: CollectionID,
    path: PathBuf,
    /// Data directories after the manifest's own, each on a drive of its own.
    further: Vec<PathBuf>,
    durability: Durability,
    writes: ustore::Durability,
}
impl UStoreBackend {
    /// The head of the database, on `executor`.
    fn head<'a>(&'a self, executor: &'a Executor) -> On<'a> {
        self.database.head().on(executor)
    }
    fn open(
        path: &Path,
        further: &[PathBuf],
        engine: Engine,
        data_model: DataModel,
        durability: Durability,
        workers: usize,
    ) -> Result<Self> {
        std::fs::create_dir_all(path).map_err(|e| e.to_string())?;
        let directory = path.to_str().ok_or("data directory is not UTF-8")?;
        let budget = output::budget();
        // The workers split the cores, as RocksDB's background jobs get all of them; with O_DIRECT reads the engine
        // has no page cache to lean on, so its own memory takes the whole limit.
        let executor_threads = (budget.cores / workers).max(1);
        // Threads outside the opening executor share these channels, so every worker's thread gets one.
        let io = IOOptions {
            shared_io_channels: workers,
            ..IOOptions::default()
        };
        let (database, executor) = match engine {
            Engine::NVME => {
                let executor = Executor::for_nvme(1, &io).map_err(|e| e.to_string())?;
                let manifest = format!("{directory}/crudeval.ustore.json");
                let mut data_directories = Vec::with_capacity(further.len());
                for directory in further {
                    data_directories.push(DataDirectory {
                        path: directory.to_str().ok_or("data directory is not UTF-8")?,
                        max_bytes: 0,
                    });
                }
                let options = NVMEOptions {
                    data_directories: &data_directories,
                    executor: Some(&executor),
                    transaction_max_deltas: TRANSACTION_MAX_DELTAS,
                    memory_limit: budget.memory_limit,
                    ..NVMEOptions::at(&manifest)
                };
                (Database::open_nvme(&options).map_err(|e| e.to_string())?, executor)
            }
            Engine::RAM => {
                let executor = Executor::for_ram(&[], 1).map_err(|e| e.to_string())?;
                let options = RAMOptions {
                    executor: Some(&executor),
                    ..RAMOptions::default()
                };
                (
                    Database::open_ram(directory, &options).map_err(|e| e.to_string())?,
                    executor,
                )
            }
            other => return Err(format!("UStore's {other} engine is not benchmarked; pick nvme or ram")),
        };
        let (records, edges) = match data_model {
            DataModel::KeyValue => (
                collection(&database, &executor, "records", Modality::Blobs)?,
                CollectionID::default(),
            ),
            DataModel::Documents => (
                collection(&database, &executor, "documents", Modality::Docs)?,
                CollectionID::default(),
            ),
            DataModel::Graph => (
                collection(&database, &executor, "vertices", Modality::Blobs)?,
                collection(&database, &executor, "edges", Modality::Graph)?,
            ),
        };
        Ok(Self {
            database,
            _executor: executor,
            engine,
            executor_threads,
            data_model,
            records,
            edges,
            path: path.into(),
            further: further.into(),
            durability,
            writes: match durability {
                Durability::Flushed => ustore::Durability::Flushed,
                Durability::None | Durability::Buffered => ustore::Durability::Buffered,
            },
        })
    }
}
impl Backend for UStoreBackend {
    fn metadata(&self) -> BTreeMap<String, serde_json::Value> {
        let mut metadata = BTreeMap::new();
        metadata.extend([
            ("backend".into(), json!("ustore")),
            ("library_version".into(), json!(ustore::version())),
            ("engine".into(), json!(self.engine.to_string())),
            ("executor_threads".into(), json!(self.executor_threads)),
            (
                "memory_limit".into(),
                json!(matches!(self.engine, Engine::NVME).then(|| output::budget().memory_limit)),
            ),
            ("data_directories".into(), json!(1 + self.further.len())),
            ("durability".into(), json!(self.durability)),
            ("write_durability".into(), json!(self.writes.to_string())),
            ("conditional_writes".into(), json!("presence_reads")),
            (
                "graph_storage".into(),
                json!("versions in a blobs collection, edges in a graph collection keyed by slot"),
            ),
            ("graph_mutations".into(), json!("transactions")),
        ]);
        metadata
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            data_models: &[DataModel::KeyValue, DataModel::Documents, DataModel::Graph],
            ordered_ranges: true,
            multiple_data_dirs: self.engine == Engine::NVME,
            batch_read: BatchMode::Native,
            batch_insert: BatchMode::Native,
            batch_update: BatchMode::Native,
            batch_delete: BatchMode::Native,
            bulk_load: BatchMode::Native,
            ..Default::default()
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_, System>> {
        let executor = match self.engine {
            Engine::NVME => Executor::for_nvme(self.executor_threads, &IOOptions::default()),
            _ => Executor::for_ram(&[], self.executor_threads),
        }
        .map_err(|e| e.to_string())?;
        let arena = || Arena::new_in(&executor).map_err(|e| e.to_string());
        Ok(Box::new_in(
            UStoreSession {
                backend: self,
                keys: Vec::new_in(System),
                found: Vec::new_in(System),
                rows: Vec::new_in(System),
                arena: arena()?,
                values_arena: arena()?,
                columns: [Column::new::<u64>("score"), Column::new::<Text>("payload")],
                texts: Vec::new_in(System),
                ends: Vec::new_in(System),
                versions: Vec::new_in(System),
                edges: Vec::new_in(System),
                stale: Vec::new_in(System),
                adjacency: Vec::new_in(System),
                executor,
            },
            System,
        ))
    }
    fn flush(&self) -> Result<()> {
        let mut transaction = self.database.transaction().map_err(|e| e.to_string())?;
        transaction
            .commit(ustore::Durability::Flushed)
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    fn disk_bytes(&self) -> Result<u64> {
        std::iter::once(&self.path)
            .chain(&self.further)
            .map(|directory| directory_bytes(directory))
            .sum()
    }
}

struct UStoreSession<'a> {
    backend: &'a UStoreBackend,
    keys: Vec<ustore::Key, System>,
    found: Vec<ustore::Key, System>,
    rows: Vec<usize, System>,
    arena: Arena,
    values_arena: Arena,
    columns: [Column<'static>; 2],
    texts: Vec<u8, System>,
    ends: Vec<usize, System>,
    versions: Vec<[u8; 8], System>,
    edges: Vec<Edge, System>,
    stale: Vec<Edge, System>,
    adjacency: Vec<GraphEdge, System>,
    executor: Executor,
}
impl UStoreSession<'_> {
    fn stage(&mut self, keys: &[Key]) {
        self.keys.clear();
        self.keys.extend(keys.iter().map(to_store));
    }
    /// The rows of the staged keys holding a document, through their scores, since presence reads take only blobs.
    fn present_documents(&mut self) -> Result<usize> {
        let gathered = self
            .backend
            .head(&self.executor)
            .docs_gather(
                self.backend.records,
                self.keys.as_slice(),
                &self.columns[..1],
                &mut self.arena,
            )
            .map_err(|e| e.to_string())?;
        let found = gathered
            .validities(0)
            .map_err(|e| e.to_string())?
            .ok_or("gather lost its score column")?;
        self.rows.clear();
        self.rows.extend((0..self.keys.len()).filter(|&row| found.get(row)));
        Ok(self.rows.len())
    }
    /// Stages the keys, versions and slot-identified edges of a batch of vertices.
    fn stage_vertices(&mut self, keys: &[Key], values: &GraphInput<'_>) -> Result<()> {
        self.stage(keys);
        self.versions.clear();
        self.edges.clear();
        for (index, &source) in self.keys.iter().enumerate() {
            let vertex = values.get(index).ok_or("missing input vertex")?;
            self.versions.push(vertex.version.to_le_bytes());
            self.edges.extend(
                vertex
                    .edges
                    .iter()
                    .map(|edge| Edge::with_id(source, to_store(&edge.target), slot_key(edge.slot))),
            );
        }
        Ok(())
    }
    /// The staged keys, each with its staged version.
    fn version_entries(&self) -> Vec<Entry<'_>, System> {
        let mut entries = Vec::with_capacity_in(self.keys.len(), System);
        entries.extend(
            self.keys
                .iter()
                .zip(&self.versions)
                .map(|(&key, version)| Entry::new(key, version)),
        );
        entries
    }
    /// Stages up to `limit` keys of the records from `start`, answering how many.
    fn stage_range(&mut self, start: Key, limit: usize) -> Result<usize> {
        self.keys.clear();
        if limit == 0 {
            return Ok(0);
        }
        let head = self.backend.head(&self.executor);
        let start = to_store(&start);
        let keys = match self.backend.data_model {
            DataModel::Documents => head
                .docs_scan(self.backend.records, start, limit, &mut self.arena)
                .map(|keys| self.keys.extend_from_slice(keys)),
            DataModel::KeyValue | DataModel::Graph => head
                .scan(self.backend.records, start, limit, &mut self.arena)
                .map(|scanned| self.keys.extend_from_slice(scanned.keys)),
        };
        keys.map_err(|e| e.to_string())?;
        Ok(self.keys.len())
    }
    fn read_values(&mut self, output: &mut RecordOutput<'_>) -> Result<usize> {
        output.clear();
        let values = self
            .backend
            .head(&self.executor)
            .read(self.backend.records, self.keys.as_slice(), &mut self.values_arena)
            .map_err(|e| e.to_string())?;
        let mut count = 0;
        for index in 0..self.keys.len() {
            let value = values.get(index);
            count += usize::from(value.is_some());
            output.push(value)?;
        }
        Ok(count)
    }
    fn read_documents(&mut self, output: &mut DocumentOutput<'_>) -> Result<usize> {
        output.clear();
        let gathered = self
            .backend
            .head(&self.executor)
            .docs_gather(
                self.backend.records,
                self.keys.as_slice(),
                &self.columns,
                &mut self.values_arena,
            )
            .map_err(|e| e.to_string())?;
        let scores = gathered.scalars::<u64>(0).map_err(|e| e.to_string())?;
        let payloads = gathered.strings(1).map_err(|e| e.to_string())?;
        let found = gathered
            .validities(0)
            .map_err(|e| e.to_string())?
            .ok_or("gather lost its score column")?;
        let mut count = 0;
        for (index, &score) in scores.iter().enumerate() {
            let document = match found.get(index) {
                true => Some(DocumentRef {
                    score,
                    payload: std::str::from_utf8(payloads.get(index).ok_or("document lost its payload")?)
                        .map_err(|e| e.to_string())?,
                }),
                false => None,
            };
            count += usize::from(document.is_some());
            output.push(document)?;
        }
        Ok(count)
    }
    fn read_vertices(&mut self, output: &mut GraphOutput<'_>) -> Result<usize> {
        output.clear();
        let head = self.backend.head(&self.executor);
        let versions = head
            .read(self.backend.records, self.keys.as_slice(), &mut self.arena)
            .map_err(|e| e.to_string())?;
        let neighborhoods = head
            .graph_read_edges(
                self.backend.edges,
                self.keys.as_slice(),
                Roles::Every(Role::Source),
                &mut self.values_arena,
            )
            .map_err(|e| e.to_string())?;
        let mut count = 0;
        for (index, run) in neighborhoods.iter().enumerate() {
            let Some(version) = versions.get(index) else {
                output.push(None)?;
                continue;
            };
            self.adjacency.clear();
            for edge in run.unwrap_or_default() {
                self.adjacency.push(GraphEdge {
                    slot: slot_of(edge)?,
                    target: from_store(&edge.target),
                });
            }
            self.adjacency.sort_unstable_by_key(|edge| edge.slot);
            output.push(Some(VertexRef {
                version: version_of(version)?,
                edges: &self.adjacency,
            }))?;
            count += 1;
        }
        Ok(count)
    }
    fn push_keys(&self, keys: &mut KeysOutput<'_>) -> Result<()> {
        keys.clear();
        for key in &self.keys {
            keys.push(from_store(key))?;
        }
        Ok(())
    }
}
impl TransactionSession for UStoreSession<'_> {}

impl BackendSession for UStoreSession<'_> {
    fn insert(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        let mut entries = Vec::with_capacity_in(keys.len(), System);
        for (index, key) in keys.iter().enumerate() {
            entries.push(Entry::new(
                to_store(key),
                values.get(index).ok_or("missing input value")?,
            ));
        }
        self.backend
            .head(&self.executor)
            .write(self.backend.records, entries.as_slice(), self.backend.writes)
            .map_err(|e| e.to_string())?;
        Ok(keys.len())
    }
    fn read(&mut self, keys: &[Key], output: &mut RecordOutput<'_>) -> Result<usize> {
        self.stage(keys);
        self.read_values(output)
    }
    fn update(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        self.stage(keys);
        let head = self.backend.head(&self.executor);
        let count = present(&head, self.backend.records, &self.keys, &mut self.arena, &mut self.rows)?;
        let mut entries = Vec::with_capacity_in(count, System);
        for &row in &self.rows {
            entries.push(Entry::new(
                self.keys[row],
                values.get(row).ok_or("missing input value")?,
            ));
        }
        head.write(self.backend.records, entries.as_slice(), self.backend.writes)
            .map_err(|e| e.to_string())?;
        Ok(count)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        self.stage(keys);
        let head = self.backend.head(&self.executor);
        let count = present(&head, self.backend.records, &self.keys, &mut self.arena, &mut self.rows)?;
        gather_found(&self.keys, &self.rows, &mut self.found);
        head.remove(self.backend.records, self.found.as_slice(), self.backend.writes)
            .map_err(|e| e.to_string())?;
        Ok(count)
    }
    fn range_read(
        &mut self,
        start: Key,
        limit: usize,
        keys: &mut KeysOutput<'_>,
        output: &mut RecordOutput<'_>,
    ) -> Result<usize> {
        self.stage_range(start, limit)?;
        self.push_keys(keys)?;
        self.read_values(output)?;
        Ok(keys.len())
    }
    fn documents(&mut self) -> Option<&mut dyn DocumentSession> {
        (self.backend.data_model == DataModel::Documents).then_some(self)
    }
    fn graph(&mut self) -> Option<&mut dyn GraphSession> {
        (self.backend.data_model == DataModel::Graph).then_some(self)
    }
}

impl DocumentSession for UStoreSession<'_> {
    fn insert(&mut self, keys: &[Key], values: &DocumentInput<'_>) -> Result<usize> {
        self.texts.clear();
        self.ends.clear();
        for (index, key) in keys.iter().enumerate() {
            let document = values.get(index).ok_or("missing input document")?;
            write!(
                self.texts,
                r#"{{"_id":"{}","score":{},"payload":"{}"}}"#,
                key.hyphenated(),
                document.score,
                document.payload
            )
            .map_err(|e| e.to_string())?;
            self.ends.push(self.texts.len());
        }
        let texts = std::str::from_utf8(&self.texts).map_err(|e| e.to_string())?;
        let mut entries = Vec::with_capacity_in(keys.len(), System);
        let mut begin = 0;
        for (key, &end) in keys.iter().zip(&self.ends) {
            entries.push(ustore::DocumentEntry::<Json>::new(to_store(key), &texts[begin..end]));
            begin = end;
        }
        self.backend
            .head(&self.executor)
            .docs_write(
                self.backend.records,
                entries.as_slice(),
                Modification::Upsert,
                self.backend.writes,
            )
            .map_err(|e| e.to_string())?;
        Ok(keys.len())
    }
    fn read(&mut self, keys: &[Key], output: &mut DocumentOutput<'_>) -> Result<usize> {
        self.stage(keys);
        self.read_documents(output)
    }
    fn update(&mut self, keys: &[Key], patches: &[DocumentPatch]) -> Result<usize> {
        if keys.len() != patches.len() {
            return Err("document patch count mismatch".into());
        }
        self.stage(keys);
        let count = self.present_documents()?;
        let head = self.backend.head(&self.executor);
        let mut fields = Vec::with_capacity_in(count, System);
        fields.extend(
            self.rows
                .iter()
                .map(|&row| FieldEntry::<u64>::new(self.keys[row], "score", &patches[row].score)),
        );
        head.docs_write(
            self.backend.records,
            fields.as_slice(),
            Modification::Update,
            self.backend.writes,
        )
        .map_err(|e| e.to_string())?;
        Ok(count)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        self.stage(keys);
        let count = self.present_documents()?;
        gather_found(&self.keys, &self.rows, &mut self.found);
        self.backend
            .head(&self.executor)
            .docs_remove(self.backend.records, self.found.as_slice(), self.backend.writes)
            .map_err(|e| e.to_string())?;
        Ok(count)
    }
    fn range_read(
        &mut self,
        start: Key,
        limit: usize,
        keys: &mut KeysOutput<'_>,
        output: &mut DocumentOutput<'_>,
    ) -> Result<usize> {
        self.stage_range(start, limit)?;
        self.push_keys(keys)?;
        self.read_documents(output)?;
        Ok(keys.len())
    }
}

impl GraphSession for UStoreSession<'_> {
    fn insert(&mut self, keys: &[Key], values: &GraphInput<'_>) -> Result<usize> {
        self.stage_vertices(keys, values)?;
        let entries = self.version_entries();
        let backend = self.backend;
        let mut transaction = backend.database.transaction().map_err(|e| e.to_string())?;
        {
            let view = transaction.on(&self.executor);
            view.graph_write_edges(backend.edges, self.edges.as_slice(), ustore::Durability::Buffered)
                .map_err(|e| e.to_string())?;
            view.write(backend.records, entries.as_slice(), ustore::Durability::Buffered)
                .map_err(|e| e.to_string())?;
        }
        transaction.commit(backend.writes).map_err(|e| e.to_string())?;
        Ok(keys.len())
    }
    /// Writes at the head, since nothing reads during a load and its batches outgrow a transaction's buffer.
    fn bulk_load(&mut self, keys: &[Key], values: &GraphInput<'_>) -> Result<usize> {
        self.stage_vertices(keys, values)?;
        let entries = self.version_entries();
        let head = self.backend.head(&self.executor);
        head.graph_write_edges(self.backend.edges, self.edges.as_slice(), self.backend.writes)
            .map_err(|e| e.to_string())?;
        head.write(self.backend.records, entries.as_slice(), self.backend.writes)
            .map_err(|e| e.to_string())?;
        Ok(keys.len())
    }
    fn read(&mut self, keys: &[Key], output: &mut GraphOutput<'_>) -> Result<usize> {
        self.stage(keys);
        self.read_vertices(output)
    }
    fn update(&mut self, keys: &[Key], patches: &[GraphPatch]) -> Result<usize> {
        if keys.len() != patches.len() {
            return Err("graph patch count mismatch".into());
        }
        self.stage(keys);
        let backend = self.backend;
        let mut transaction = backend.database.transaction().map_err(|e| e.to_string())?;
        let count = {
            let view = transaction.on(&self.executor);
            let count = present(&view, backend.records, &self.keys, &mut self.arena, &mut self.rows)?;
            gather_found(&self.keys, &self.rows, &mut self.found);
            let neighborhoods = view
                .graph_read_edges(
                    backend.edges,
                    self.found.as_slice(),
                    Roles::Every(Role::Source),
                    &mut self.values_arena,
                )
                .map_err(|e| e.to_string())?;
            self.stale.clear();
            for run in neighborhoods.iter() {
                self.stale.extend(
                    run.unwrap_or_default()
                        .iter()
                        .filter(|edge| edge.id == slot_key(0))
                        .copied(),
                );
            }
            self.edges.clear();
            self.versions.clear();
            for &row in &self.rows {
                let patch = &patches[row];
                if let Some(neighbor) = patch.neighbor {
                    self.edges
                        .push(Edge::with_id(self.keys[row], to_store(&neighbor), slot_key(0)));
                }
                self.versions.push(patch.version.to_le_bytes());
            }
            let mut entries = Vec::with_capacity_in(count, System);
            entries.extend(
                self.found
                    .iter()
                    .zip(&self.versions)
                    .map(|(&key, version)| Entry::new(key, version)),
            );
            view.graph_remove_edges(backend.edges, self.stale.as_slice(), ustore::Durability::Buffered)
                .map_err(|e| e.to_string())?;
            view.graph_write_edges(backend.edges, self.edges.as_slice(), ustore::Durability::Buffered)
                .map_err(|e| e.to_string())?;
            view.write(backend.records, entries.as_slice(), ustore::Durability::Buffered)
                .map_err(|e| e.to_string())?;
            count
        };
        transaction.commit(backend.writes).map_err(|e| e.to_string())?;
        Ok(count)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        self.stage(keys);
        let backend = self.backend;
        let mut transaction = backend.database.transaction().map_err(|e| e.to_string())?;
        let count = {
            let view = transaction.on(&self.executor);
            let count = present(&view, backend.records, &self.keys, &mut self.arena, &mut self.rows)?;
            gather_found(&self.keys, &self.rows, &mut self.found);
            view.remove(backend.records, self.found.as_slice(), ustore::Durability::Buffered)
                .map_err(|e| e.to_string())?;
            view.graph_remove_vertices(
                backend.edges,
                self.found.as_slice(),
                Roles::Every(Role::Any),
                ustore::Durability::Buffered,
            )
            .map_err(|e| e.to_string())?;
            count
        };
        transaction.commit(backend.writes).map_err(|e| e.to_string())?;
        Ok(count)
    }
    fn range_read(
        &mut self,
        start: Key,
        limit: usize,
        keys: &mut KeysOutput<'_>,
        output: &mut GraphOutput<'_>,
    ) -> Result<usize> {
        self.stage_range(start, limit)?;
        self.push_keys(keys)?;
        self.read_vertices(output)?;
        Ok(keys.len())
    }
    fn expand_neighbors(&mut self, start: Key, limit: usize, keys: &mut KeysOutput<'_>) -> Result<usize> {
        keys.clear();
        let head = self.backend.head(&self.executor);
        let origin = to_store(&start);
        let first = head
            .graph_read_edges(
                self.backend.edges,
                [origin],
                Roles::Every(Role::Source),
                &mut self.arena,
            )
            .map_err(|e| e.to_string())?;
        self.found.clear();
        self.found.extend(first.edges().iter().map(|edge| edge.target));
        let second = head
            .graph_read_edges(
                self.backend.edges,
                self.found.as_slice(),
                Roles::Every(Role::Source),
                &mut self.values_arena,
            )
            .map_err(|e| e.to_string())?;
        self.keys.clear();
        self.keys.extend(
            second
                .edges()
                .iter()
                .map(|edge| edge.target)
                .filter(|&target| target != origin),
        );
        self.keys.sort_unstable();
        self.keys.dedup();
        self.keys.truncate(limit);
        self.push_keys(keys)?;
        Ok(keys.len())
    }
}

fn main() -> Result<()> {
    let cli: Cli = crudeval::parse_cli();
    let settings = [("Engine", cli.engine.to_string())];
    run(
        cli.common,
        json!({"engine": cli.engine.to_string()}),
        &settings,
        move |args, path| {
            Ok(Box::new_in(
                UStoreBackend::open(
                    path,
                    &args.further_data_dirs(path),
                    cli.engine,
                    args.data_model,
                    args.durability,
                    args.threads[0].0.get(),
                )?,
                System,
            ))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opened(engine: Engine, data_model: DataModel, directory: &tempfile::TempDir) -> UStoreBackend {
        UStoreBackend::open(directory.path(), &[], engine, data_model, Durability::None, 1).unwrap()
    }

    #[test]
    fn embedded_contract() {
        for engine in [Engine::RAM, Engine::NVME] {
            let directory = tempfile::tempdir().unwrap();
            crudeval::assert_backend_contract!(&opened(engine, DataModel::KeyValue, &directory));
        }
    }

    #[test]
    fn data_directories_contract() {
        let directories = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
        let further = [directories[1].path().to_path_buf()];
        let backend = UStoreBackend::open(
            directories[0].path(),
            &further,
            Engine::NVME,
            DataModel::KeyValue,
            Durability::None,
            1,
        )
        .unwrap();
        crudeval::assert_backend_contract!(&backend);
        assert!(backend.capabilities().multiple_data_dirs);
    }

    #[test]
    fn document_contract() {
        for engine in [Engine::RAM, Engine::NVME] {
            let directory = tempfile::tempdir().unwrap();
            crudeval::assert_document_contract!(&opened(engine, DataModel::Documents, &directory));
        }
    }

    #[test]
    fn graph_contract() {
        for engine in [Engine::RAM, Engine::NVME] {
            let directory = tempfile::tempdir().unwrap();
            crudeval::assert_graph_contract!(&opened(engine, DataModel::Graph, &directory));
        }
    }
}

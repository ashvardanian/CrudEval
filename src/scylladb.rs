//! ScyllaDB benchmark for key-value records and documents over CQL.
//!
//! Requires Docker; the binary manages its own pinned ScyllaDB container.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features scylladb-backend \
//!     --bin crud-eval-scylladb -- --records 100K --threads 4
//! ```

use std::{alloc::System, collections::BTreeMap, future::Future, path::Path, time::Duration};

use clap::Parser;
use futures::{stream, StreamExt, TryStreamExt};
use scylla::{
    client::{execution_profile::ExecutionProfile, session::Session, session_builder::SessionBuilder},
    response::query_result::QueryRowsResult,
    serialize::row::SerializeRow,
    statement::{prepared::PreparedStatement, Consistency},
};
use serde_json::{json, Value};
use tokio::runtime::Runtime;

use crudeval::{
    backend::{
        Backend, BackendCapabilities, BackendSession, BatchMode, DataModel, DocumentInput, DocumentOutput,
        DocumentPatch, DocumentRef, DocumentSession, Durability, Key, KeysOutput, RecordInput, RecordOutput, Result,
        TransactionSession,
    },
    docker::{ContainerHandle, Startup},
    run, spell_duration, BetweenWorkloads, CommonArgs, Port,
};

const IMAGE: &str = "scylladb/scylla:2026.3.2";
/// Statements in flight per worker while one batch is pipelined.
const PIPELINE_DEPTH: usize = 256;

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
    /// Host port for CQL on localhost; unset lets Docker pick a free one.
    #[arg(long, value_parser = crudeval::parse_port)]
    port: Option<Port>,
    /// Time limit for container start and readiness, like 120s.
    #[arg(long, default_value = "120s", value_parser = crudeval::parse_duration_flag)]
    startup_time_limit: Duration,
}
struct Statements {
    insert: PreparedStatement,
    read: PreparedStatement,
    update: PreparedStatement,
    delete: PreparedStatement,
    exists: PreparedStatement,
}
struct ScyllaBackend {
    container: ContainerHandle,
    runtime: Runtime,
    session: Session,
    statements: Statements,
    data_model: DataModel,
    durability: Durability,
}
struct ScyllaSession<'a>(&'a ScyllaBackend);

fn open(args: &CommonArgs, path: &Path, port: Option<Port>, time_limit: Duration) -> Result<Box<dyn Backend, System>> {
    if args.between_workloads != BetweenWorkloads::Keep {
        return Err("Docker backends support only --between-workloads keep".into());
    }
    if args.data_model == DataModel::Graph {
        return Err("ScyllaDB supports key-value records and documents, not graphs".into());
    }
    let commitlog_sync = if args.durability == Durability::Flushed {
        "batch"
    } else {
        "periodic"
    };
    let container = ContainerHandle::start_with(
        IMAGE,
        9042,
        path,
        "/var/lib/scylla",
        &[],
        &[
            "--developer-mode",
            "1",
            "--overprovisioned",
            "1",
            "--broadcast-rpc-address",
            "127.0.0.1",
            "--commitlog-sync",
            commitlog_sync,
        ],
        &Startup {
            host_port: port,
            time_limit,
            ..Startup::default()
        },
    )?;
    let runtime = Runtime::new().map_err(|e| e.to_string())?;
    let profile = ExecutionProfile::builder()
        .consistency(Consistency::LocalQuorum)
        .build();
    let mut session = None;
    container.ready(|| {
        let builder = SessionBuilder::new()
            .known_node(format!("127.0.0.1:{}", container.port))
            .disallow_shard_aware_port(true)
            .default_execution_profile_handle(profile.clone().into_handle());
        session = Some(runtime.block_on(builder.build()).map_err(|e| e.to_string())?);
        Ok(())
    })?;
    let session = session.ok_or("ScyllaDB session missing after readiness")?;
    let keyspace = format!(
        "CREATE KEYSPACE IF NOT EXISTS crudeval WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND durable_writes = {}",
        args.durability != Durability::None
    );
    let (table, insert, read, update) = match args.data_model {
        DataModel::Documents => (
            "CREATE TABLE IF NOT EXISTS crudeval.records (id blob PRIMARY KEY, score bigint, payload text)",
            "INSERT INTO crudeval.records (id, score, payload) VALUES (?, ?, ?)",
            "SELECT score, payload FROM crudeval.records WHERE id = ?",
            "UPDATE crudeval.records SET score = ? WHERE id = ?",
        ),
        _ => (
            "CREATE TABLE IF NOT EXISTS crudeval.records (id blob PRIMARY KEY, value blob)",
            "INSERT INTO crudeval.records (id, value) VALUES (?, ?)",
            "SELECT value FROM crudeval.records WHERE id = ?",
            "UPDATE crudeval.records SET value = ? WHERE id = ?",
        ),
    };
    let statements = runtime.block_on(async {
        for schema in [keyspace.as_str(), table] {
            session.query_unpaged(schema, &[]).await.map_err(|e| e.to_string())?;
        }
        let session = &session;
        let prepare = |text: &'static str| async move { session.prepare(text).await.map_err(|e| e.to_string()) };
        Ok::<_, String>(Statements {
            insert: prepare(insert).await?,
            read: prepare(read).await?,
            update: prepare(update).await?,
            delete: prepare("DELETE FROM crudeval.records WHERE id = ?").await?,
            exists: prepare("SELECT id FROM crudeval.records WHERE id = ?").await?,
        })
    })?;
    Ok(Box::new_in(
        ScyllaBackend {
            container,
            runtime,
            session,
            statements,
            data_model: args.data_model,
            durability: args.durability,
        },
        System,
    ))
}
impl Backend for ScyllaBackend {
    fn metadata(&self) -> BTreeMap<String, Value> {
        let mut metadata = BTreeMap::new();
        metadata.extend([
            ("backend".into(), json!("scylladb")),
            ("image".into(), json!(IMAGE)),
            ("durability".into(), json!(self.durability)),
            ("durable_writes".into(), json!(self.durability != Durability::None)),
            (
                "commitlog_sync".into(),
                json!(if self.durability == Durability::Flushed {
                    "batch"
                } else {
                    "periodic"
                }),
            ),
            ("consistency".into(), json!("LOCAL_QUORUM")),
            ("replication_factor".into(), json!(1)),
            ("pipeline_depth".into(), json!(PIPELINE_DEPTH)),
            (
                "update_and_delete".into(),
                json!("read the key, then write only if present; not atomic"),
            ),
            ("flush".into(), json!("nodetool flush")),
        ]);
        metadata
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            data_models: &[DataModel::KeyValue, DataModel::Documents],
            ordered_ranges: false,
            transactions: false,
            batch_read: BatchMode::Pipelined,
            batch_insert: BatchMode::Pipelined,
            batch_update: BatchMode::Pipelined,
            batch_delete: BatchMode::Pipelined,
            bulk_load: BatchMode::Pipelined,
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_, System>> {
        Ok(Box::new_in(ScyllaSession(self), System))
    }
    fn flush(&self) -> Result<()> {
        self.container.exec(&["nodetool", "flush", "crudeval"]).map(drop)
    }
    fn server_usage(&self) -> Result<Option<Value>> {
        self.container.stats().map(Some)
    }
    fn disk_bytes(&self) -> Result<u64> {
        // Compaction deletes sstables while `du` walks them; it still prints the total, so its status is ignored.
        let output = self
            .container
            .exec(&["sh", "-c", "du -sk /var/lib/scylla 2>/dev/null; true"])?;
        output
            .split_whitespace()
            .next()
            .and_then(|kilobytes| kilobytes.parse::<u64>().ok())
            .map(|kilobytes| kilobytes * 1024)
            .ok_or_else(|| format!("Unexpected disk usage: {output}"))
    }
}
impl ScyllaBackend {
    /// Runs the calls with up to `PIPELINE_DEPTH` in flight and sums the records they affected.
    fn pipelined(&self, calls: impl Iterator<Item = impl Future<Output = Result<usize>>>) -> Result<usize> {
        self.runtime.block_on(
            stream::iter(calls)
                .buffer_unordered(PIPELINE_DEPTH)
                .try_fold(0, |sum, count| async move { Ok(sum + count) }),
        )
    }
    async fn write(&self, statement: &PreparedStatement, values: impl SerializeRow) -> Result<usize> {
        self.session
            .execute_unpaged(statement, values)
            .await
            .map(|_| 1)
            .map_err(|e| e.to_string())
    }
    /// Writes only when `key` exists, so updates and deletes never create or count a missing record.
    async fn write_existing(
        &self,
        key: &Key,
        statement: &PreparedStatement,
        values: impl SerializeRow,
    ) -> Result<usize> {
        let rows = self
            .session
            .execute_unpaged(&self.statements.exists, (key.as_bytes().as_slice(),))
            .await
            .map_err(|e| e.to_string())?
            .into_rows_result()
            .map_err(|e| e.to_string())?;
        if rows.rows_num() == 0 {
            return Ok(0);
        }
        self.write(statement, values).await
    }
    /// Reads every key with up to `PIPELINE_DEPTH` in flight, handing each result to `push` in key order.
    fn read(&self, keys: &[Key], mut push: impl FnMut(Option<&QueryRowsResult>) -> Result<bool>) -> Result<usize> {
        self.runtime.block_on(async {
            let mut results = stream::iter(keys)
                .map(|key| {
                    self.session
                        .execute_unpaged(&self.statements.read, (key.as_bytes().as_slice(),))
                })
                .buffered(PIPELINE_DEPTH);
            let mut found = 0;
            while let Some(result) = results.next().await {
                let rows = result
                    .map_err(|e| e.to_string())?
                    .into_rows_result()
                    .map_err(|e| e.to_string())?;
                found += usize::from(push((rows.rows_num() != 0).then_some(&rows))?);
            }
            Ok(found)
        })
    }
}
impl TransactionSession for ScyllaSession<'_> {}
impl BackendSession for ScyllaSession<'_> {
    fn documents(&mut self) -> Option<&mut dyn DocumentSession> {
        (self.0.data_model == DataModel::Documents).then_some(self)
    }
    fn insert(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        let backend = self.0;
        backend.pipelined(keys.iter().enumerate().map(|(i, key)| async move {
            let value = values.get(i).ok_or("Missing insert value")?;
            backend
                .write(&backend.statements.insert, (key.as_bytes().as_slice(), value))
                .await
        }))
    }
    fn read(&mut self, keys: &[Key], output: &mut RecordOutput<'_>) -> Result<usize> {
        output.clear();
        self.0.read(keys, |rows| match rows {
            Some(rows) => {
                let (value,) = rows.single_row::<(&[u8],)>().map_err(|e| e.to_string())?;
                output.push(Some(value)).map(|()| true)
            }
            None => output.push(None).map(|()| false),
        })
    }
    fn update(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        let backend = self.0;
        backend.pipelined(keys.iter().enumerate().map(|(i, key)| async move {
            let value = values.get(i).ok_or("Missing update value")?;
            backend
                .write_existing(key, &backend.statements.update, (value, key.as_bytes().as_slice()))
                .await
        }))
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        let backend = self.0;
        backend.pipelined(
            keys.iter()
                .map(|key| backend.write_existing(key, &backend.statements.delete, (key.as_bytes().as_slice(),))),
        )
    }
    fn range_read(&mut self, _: Key, _: usize, _: &mut KeysOutput<'_>, _: &mut RecordOutput<'_>) -> Result<usize> {
        Err("ScyllaDB hashes partition keys and does not support ordered key ranges".into())
    }
}
impl DocumentSession for ScyllaSession<'_> {
    fn insert(&mut self, keys: &[Key], values: &DocumentInput<'_>) -> Result<usize> {
        let backend = self.0;
        backend.pipelined(keys.iter().enumerate().map(|(i, key)| async move {
            let value = values.get(i).ok_or("Missing document")?;
            backend
                .write(
                    &backend.statements.insert,
                    (key.as_bytes().as_slice(), value.score as i64, value.payload),
                )
                .await
        }))
    }
    fn read(&mut self, keys: &[Key], output: &mut DocumentOutput<'_>) -> Result<usize> {
        output.clear();
        self.0.read(keys, |rows| match rows {
            Some(rows) => {
                let (score, payload) = rows.single_row::<(i64, &str)>().map_err(|e| e.to_string())?;
                output
                    .push(Some(DocumentRef {
                        score: score as u64,
                        payload,
                    }))
                    .map(|()| true)
            }
            None => output.push(None).map(|()| false),
        })
    }
    fn update(&mut self, keys: &[Key], patches: &[DocumentPatch]) -> Result<usize> {
        let backend = self.0;
        backend.pipelined(keys.iter().zip(patches).map(|(key, patch)| {
            backend.write_existing(
                key,
                &backend.statements.update,
                (patch.score as i64, key.as_bytes().as_slice()),
            )
        }))
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        BackendSession::delete(self, keys)
    }
    fn range_read(&mut self, _: Key, _: usize, _: &mut KeysOutput<'_>, _: &mut DocumentOutput<'_>) -> Result<usize> {
        Err("ScyllaDB hashes partition keys and does not support ordered document ranges".into())
    }
}
fn main() {
    let cli: Cli = crudeval::parse_cli();
    let settings = [
        (
            "Port",
            cli.port.map_or_else(|| "random".into(), |port| port.to_string()),
        ),
        ("Startup time limit", spell_duration(cli.startup_time_limit)),
    ];
    if let Err(error) = run(cli.common, IMAGE, &settings, |args, path| {
        open(args, path, cli.port, cli.startup_time_limit)
    }) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
#[test]
#[ignore = "starts pinned Docker servers"]
fn native_protocol_contract() {
    let path = tempfile::tempdir().unwrap();
    let mut args = Cli::parse_from(["contract"]).common;
    args.data_model = DataModel::Documents;
    args.durability = Durability::None;
    let backend = open(&args, path.path(), None, Duration::from_secs(120)).unwrap();
    crudeval::assert_document_contract!(backend.as_ref());
}

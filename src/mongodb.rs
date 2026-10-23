//! MongoDB-protocol benchmark for key-value records and documents.
//!
//! Requires Docker; MongoDB and FerretDB use pinned server images.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features mongodb-backend \
//!     --bin crud-eval-mongodb -- --records 100K --threads 4 --durability buffered
//! ```

use std::{alloc::System, collections::BTreeMap, path::Path};

use clap::{Parser, ValueEnum};
use mongodb::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, Document},
    sync::{Client, Collection, Database},
};
use serde_json::{json, Value};

use crudeval::{
    backend::{
        Backend, BackendCapabilities, BackendSession, BatchMode, DataModel, DocumentInput, DocumentOutput,
        DocumentPatch, DocumentRef, DocumentSession, Durability, Key, KeyWidth, KeysOutput, RecordInput, RecordOutput,
        Result, TransactionSession,
    },
    docker::{ContainerHandle, NetworkHandle, Startup},
    run, BetweenWorkloads, CommonArgs,
};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Server {
    Mongodb,
    Ferretdb,
}
#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long, env = "CRUDEVAL_SERVER", value_enum, default_value = "mongodb")]
    server: Server,
}
struct MongoDbBackend {
    container: ContainerHandle,
    postgres: Option<ContainerHandle>,
    _network: Option<NetworkHandle>,
    server: Server,
    image: &'static str,
    client: Client,
    data_model: DataModel,
    key_width: KeyWidth,
    durability: Durability,
}
struct MongoDbSession {
    collection: Collection<Document>,
    database: Database,
    rows: Vec<(Key, Document), System>,
    data_model: DataModel,
    key_width: KeyWidth,
}
fn id(key_width: KeyWidth, key: Key) -> Bson {
    Bson::Binary(Binary {
        subtype: BinarySubtype::Generic,
        bytes: key_width.bytes(&key).to_vec(),
    })
}
fn open(args: &CommonArgs, path: &Path, server: Server) -> Result<Box<dyn Backend, System>> {
    if args.between_workloads != BetweenWorkloads::Keep {
        return Err("Docker backends support only --between-workloads keep".into());
    }

    if args.data_model == DataModel::Graph {
        return Err("MongoDB supports kv and docs modalities".into());
    }
    if args.durability == Durability::None && matches!(server, Server::Mongodb) {
        return Err("MongoDB cannot disable journaling; choose --durability buffered or flushed".into());
    }
    let image = match server {
        Server::Mongodb => "mongodb/mongodb-community-server:9.0.2-ubi9-slim",
        Server::Ferretdb => "ghcr.io/ferretdb/ferretdb:2.7.0",
    };
    let network = if matches!(server, Server::Ferretdb) {
        Some(NetworkHandle::create()?)
    } else {
        None
    };
    let postgres = if let Some(network) = &network {
        let fsync = if args.durability == Durability::None {
            "off"
        } else {
            "on"
        };
        let sync = if args.durability == Durability::Flushed {
            "on"
        } else {
            "off"
        };
        let postgres = ContainerHandle::start_with(
            "ghcr.io/ferretdb/postgres-documentdb:17-0.107.0-ferretdb-2.7.0",
            5432,
            &path.join("postgres"),
            "/var/lib/postgresql/data",
            &[
                ("POSTGRES_USER", "crudeval"),
                ("POSTGRES_PASSWORD", "crudeval"),
                ("POSTGRES_DB", "postgres"),
            ],
            &[
                "postgres",
                "-c",
                &format!("fsync={fsync}"),
                "-c",
                &format!("synchronous_commit={sync}"),
            ],
            &Startup {
                network: Some(network),
                ..Startup::default()
            },
        )?;
        postgres.ready(|| {
            postgres
                .exec(&["pg_isready", "-U", "crudeval", "-d", "postgres"])
                .map(|_| ())
        })?;
        Some(postgres)
    } else {
        None
    };
    let postgres_url = postgres
        .as_ref()
        .map(|pg| {
            pg.name()
                .map(|name| format!("postgres://crudeval:crudeval@{name}:5432/postgres"))
        })
        .transpose()?;
    let env = if let Some(url) = &postgres_url {
        vec![
            ("FERRETDB_POSTGRESQL_URL", url.as_str()),
            ("FERRETDB_TELEMETRY", "disabled"),
        ]
    } else {
        vec![]
    };
    let container = ContainerHandle::start_with(
        image,
        27017,
        &path.join("mongo"),
        if matches!(server, Server::Ferretdb) {
            "/tmp/crudeval"
        } else {
            "/data/db"
        },
        &env,
        &[],
        &Startup {
            network: network.as_ref(),
            ..Startup::default()
        },
    )?;
    let uri = match server {
        Server::Mongodb => format!(
            "mongodb://127.0.0.1:{}/?serverSelectionTimeoutMS=1000&journal={}",
            container.port,
            args.durability == Durability::Flushed
        ),
        Server::Ferretdb => format!(
            "mongodb://crudeval:crudeval@127.0.0.1:{}/?serverSelectionTimeoutMS=1000",
            container.port
        ),
    };
    let client = Client::with_uri_str(uri).map_err(|e| e.to_string())?;
    container.ready(|| {
        client
            .database("admin")
            .run_command(doc! {"ping":1})
            .run()
            .map(|_| ())
            .map_err(|e| e.to_string())
    })?;
    Ok(Box::new_in(
        MongoDbBackend {
            container,
            postgres,
            _network: network,
            server,
            image,
            client,
            data_model: args.data_model,
            key_width: args.key_width()?,
            durability: args.durability,
        },
        System,
    ))
}
impl Backend for MongoDbBackend {
    fn metadata(&self) -> BTreeMap<String, Value> {
        {
            let mut metadata = BTreeMap::new();
            metadata.extend([
                ("backend".into(), json!(format!("{:?}", self.server).to_lowercase())),
                ("image".into(), json!(self.image)),
                ("durability".into(), json!(self.durability)),
                ("journal_or_wal_enabled".into(), json!(true)),
                (
                    "batch_writes".into(),
                    json!({"insert":"native", "delete":"native", "update":"native update command"}),
                ),
            ]);
            metadata
        }
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            data_models: &[DataModel::KeyValue, DataModel::Documents],
            ordered_ranges: true,
            transactions: false,
            batch_read: BatchMode::Native,
            batch_insert: BatchMode::Native,
            batch_update: BatchMode::Native,
            batch_delete: BatchMode::Native,
            bulk_load: BatchMode::Native,
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_, System>> {
        Ok(Box::new_in(
            MongoDbSession {
                collection: self.client.database("crudeval").collection("records"),
                database: self.client.database("crudeval"),
                rows: Vec::new_in(System),
                data_model: self.data_model,
                key_width: self.key_width,
            },
            System,
        ))
    }
    fn flush(&self) -> Result<()> {
        if let Some(postgres) = &self.postgres {
            return postgres
                .exec(&["psql", "-U", "crudeval", "-d", "postgres", "-c", "CHECKPOINT"])
                .map(|_| ());
        }
        self.client
            .database("admin")
            .run_command(doc! {"fsync":1})
            .run()
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    fn server_usage(&self) -> Result<Option<Value>> {
        let frontend = self.container.stats()?;
        match &self.postgres {
            Some(postgres) => Ok(Some(json!({"frontend":frontend,"storage":postgres.stats()?}))),
            None => Ok(Some(frontend)),
        }
    }
    fn disk_bytes(&self) -> Result<u64> {
        match &self.postgres {
            Some(pg) => pg.disk_bytes(),
            None => self.container.disk_bytes(),
        }
    }
}
fn document_key(key_width: KeyWidth, doc: &Document) -> Result<Key> {
    key_width.from_bytes(doc.get_binary_generic("_id").map_err(|e| e.to_string())?)
}
fn document_ref(doc: &Document) -> Result<DocumentRef<'_>> {
    Ok(DocumentRef {
        score: doc.get_i64("score").map_err(|e| e.to_string())? as u64,
        payload: doc.get_str("payload").map_err(|e| e.to_string())?,
    })
}
impl MongoDbSession {
    fn fetch(&mut self, keys: &[Key]) -> Result<()> {
        self.rows.clear();
        let ids: Vec<_> = keys.iter().map(|key| id(self.key_width, *key)).collect();
        let cursor = self
            .collection
            .find(doc! {"_id":{"$in":ids}})
            .run()
            .map_err(|e| e.to_string())?;
        for row in cursor {
            let row = row.map_err(|e| e.to_string())?;
            self.rows.push((document_key(self.key_width, &row)?, row));
        }
        self.rows.sort_unstable_by_key(|row| row.0);
        Ok(())
    }
    fn row(&self, key: Key) -> Option<&Document> {
        self.rows
            .binary_search_by_key(&key, |row| row.0)
            .ok()
            .map(|i| &self.rows[i].1)
    }
    fn write_updates(&self, updates: Vec<Document>) -> Result<usize> {
        if updates.is_empty() {
            return Ok(0);
        }
        let reply = self
            .database
            .run_command(doc! {"update":"records","updates":updates,"ordered":true})
            .run()
            .map_err(|e| e.to_string())?;
        if reply.contains_key("writeErrors") {
            return Err(format!("MongoDB update failed: {reply:?}"));
        }
        match reply.get("n") {
            Some(Bson::Int32(n)) => Ok(*n as usize),
            Some(Bson::Int64(n)) => Ok(*n as usize),
            _ => Err("Missing MongoDB matched count".into()),
        }
    }
}
impl TransactionSession for MongoDbSession {}
impl BackendSession for MongoDbSession {
    fn documents(&mut self) -> Option<&mut dyn DocumentSession> {
        (self.data_model == DataModel::Documents).then_some(self)
    }
    fn insert(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }
        let docs=keys.iter().enumerate().map(|(i,key)|Ok(doc! {"_id":id(self.key_width, *key),"value":Binary {subtype:BinarySubtype::Generic,bytes:values.get(i).ok_or("Missing value")?.to_vec()}})).collect::<Result<Vec<_>>>()?;
        self.collection
            .insert_many(docs)
            .run()
            .map(|r| r.inserted_ids.len())
            .map_err(|e| e.to_string())
    }
    fn read(&mut self, keys: &[Key], out: &mut RecordOutput<'_>) -> Result<usize> {
        out.clear();
        self.fetch(keys)?;
        let mut found = 0;
        for key in keys {
            let value = self
                .row(*key)
                .map(|doc| {
                    doc.get_binary_generic("value")
                        .map(Vec::as_slice)
                        .map_err(|e| e.to_string())
                })
                .transpose()?;
            found += usize::from(value.is_some());
            out.push(value)?;
        }
        Ok(found)
    }
    fn update(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        let updates=keys.iter().enumerate().map(|(i,key)|Ok(doc! {"q":{"_id":id(self.key_width, *key)},"u":{"$set":{"value":Binary {subtype:BinarySubtype::Generic,bytes:values.get(i).ok_or("Missing value")?.to_vec()}}},"upsert":false})).collect::<Result<Vec<_>>>()?;
        self.write_updates(updates)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        let ids: Vec<_> = keys.iter().map(|key| id(self.key_width, *key)).collect();
        self.collection
            .delete_many(doc! {"_id":{"$in":ids}})
            .run()
            .map(|r| r.deleted_count as usize)
            .map_err(|e| e.to_string())
    }
    fn range_read(
        &mut self,
        start: Key,
        limit: usize,
        keys: &mut KeysOutput<'_>,
        out: &mut RecordOutput<'_>,
    ) -> Result<usize> {
        keys.clear();
        out.clear();
        if limit == 0 {
            return Ok(0);
        }
        let cursor = self
            .collection
            .find(doc! {"_id":{"$gte":id(self.key_width, start)}})
            .sort(doc! {"_id":1})
            .limit(limit as i64)
            .run()
            .map_err(|e| e.to_string())?;
        for doc in cursor {
            let doc = doc.map_err(|e| e.to_string())?;
            keys.push(document_key(self.key_width, &doc)?)?;
            out.push(Some(doc.get_binary_generic("value").map_err(|e| e.to_string())?))?;
        }
        Ok(keys.len())
    }
}
impl DocumentSession for MongoDbSession {
    fn insert(&mut self, keys: &[Key], values: &DocumentInput<'_>) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }
        let docs = keys
            .iter()
            .enumerate()
            .map(|(i, key)| {
                let value = values.get(i).ok_or("Missing document")?;
                Ok(doc! {"_id":id(self.key_width, *key),"score":value.score as i64,"payload":value.payload})
            })
            .collect::<Result<Vec<_>>>()?;
        self.collection
            .insert_many(docs)
            .run()
            .map(|r| r.inserted_ids.len())
            .map_err(|e| e.to_string())
    }
    fn read(&mut self, keys: &[Key], out: &mut DocumentOutput<'_>) -> Result<usize> {
        out.clear();
        self.fetch(keys)?;
        let mut found = 0;
        for key in keys {
            let value = self.row(*key).map(document_ref).transpose()?;
            found += usize::from(value.is_some());
            out.push(value)?;
        }
        Ok(found)
    }
    fn update(&mut self, keys: &[Key], patches: &[DocumentPatch]) -> Result<usize> {
        let updates = keys
            .iter()
            .zip(patches)
            .map(|(key, patch)| doc! {"q":{"_id":id(self.key_width, *key)},"u":{"$set":{"score":patch.score as i64}},"upsert":false})
            .collect();
        self.write_updates(updates)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        BackendSession::delete(self, keys)
    }
    fn range_read(
        &mut self,
        start: Key,
        limit: usize,
        keys: &mut KeysOutput<'_>,
        out: &mut DocumentOutput<'_>,
    ) -> Result<usize> {
        keys.clear();
        out.clear();
        if limit == 0 {
            return Ok(0);
        }
        let cursor = self
            .collection
            .find(doc! {"_id":{"$gte":id(self.key_width, start)}})
            .sort(doc! {"_id":1})
            .limit(limit as i64)
            .run()
            .map_err(|e| e.to_string())?;
        for doc in cursor {
            let doc = doc.map_err(|e| e.to_string())?;
            keys.push(document_key(self.key_width, &doc)?)?;
            out.push(Some(document_ref(&doc)?))?;
        }
        Ok(keys.len())
    }
}
fn main() {
    let cli: Cli = crudeval::parse_cli();
    let settings = [("Server", crudeval::spell_value(&cli.server))];
    if let Err(error) = run(cli.common, format!("{:?}", cli.server), &settings, |args, path| {
        open(args, path, cli.server)
    }) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
#[test]
#[ignore = "starts pinned Docker servers"]
fn native_protocol_contract() {
    for server in [Server::Mongodb, Server::Ferretdb] {
        for data_model in [DataModel::Documents] {
            let path = tempfile::tempdir().unwrap();
            let mut args = Cli::parse_from(["contract"]).common;
            args.data_model = data_model;
            args.durability = Durability::Buffered;
            let backend = open(&args, path.path(), server).unwrap();
            match data_model {
                DataModel::Documents => crudeval::assert_document_contract!(backend.as_ref()),
                DataModel::Graph => crudeval::assert_graph_contract!(backend.as_ref()),
                _ => unreachable!(),
            };
        }
    }
}

//! Redis-protocol key-value and native JSON document benchmarks.
//!
//! Requires Docker; the binary manages its own pinned server container.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features redis-backend \
//!     --bin crud-eval-redis -- --records 100K --threads 4
//! ```

use std::{alloc::System, collections::BTreeMap, num::NonZeroU16, path::Path};

use clap::{Parser, ValueEnum};
use redis::{Client, Connection};
use serde_json::{json, Value};

use crudeval::{
    backend::{
        Backend, BackendCapabilities, BackendSession, BatchMode, DataModel, DocumentInput, DocumentOutput,
        DocumentPatch, DocumentRef, DocumentSession, Durability, Key, KeysOutput, RecordInput, RecordOutput, Result,
        TransactionSession,
    },
    docker::ContainerHandle,
    run, BetweenWorkloads, CommonArgs,
};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Server {
    Redis,
    Valkey,
    Dragonfly,
    Garnet,
    Kvrocks,
}
#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long, value_enum, default_value = "redis")]
    server: Server,
    /// Dragonfly I/O threads; defaults to the server's automatic selection.
    #[arg(long, value_parser = |text: &str| crudeval::parse_count(text).and_then(|count| u16::try_from(count).ok().and_then(NonZeroU16::new)).ok_or("expected a positive count"))]
    dragonfly_threads: Option<NonZeroU16>,
}
struct RedisBackend {
    container: ContainerHandle,
    client: Client,
    server: Server,
    durability: Durability,
    image: &'static str,
    dragonfly_threads: Option<NonZeroU16>,
    data_model: DataModel,
}
struct RedisSession {
    server: Server,
    connection: Connection,
    data_model: DataModel,
    pipeline: redis::Pipeline,
    wire: Vec<u8, System>,
}

fn open(
    args: &CommonArgs,
    path: &Path,
    server: Server,
    dragonfly_threads: Option<NonZeroU16>,
) -> Result<Box<dyn Backend, System>> {
    if dragonfly_threads.is_some() && !matches!(server, Server::Dragonfly) {
        return Err("--dragonfly-threads requires --server dragonfly".into());
    }
    if args.between_workloads != BetweenWorkloads::Keep {
        return Err("Docker backends support only --between-workloads keep".into());
    }

    if args.data_model == DataModel::Graph {
        return Err("Redis-compatible servers support key-value and native JSON documents, not graphs".into());
    }
    let image = match server {
        Server::Redis => "redis:8.10.2",
        Server::Valkey => "valkey/valkey-bundle:9.1.2",
        Server::Dragonfly => "ghcr.io/dragonflydb/dragonfly:v2.0.0",
        Server::Garnet => "crudeval-garnet-json:2.1.8",
        Server::Kvrocks => "apache/kvrocks:2.17.0",
    };
    if matches!(server, Server::Garnet)
        && !std::process::Command::new("docker")
            .args(["image", "inspect", image])
            .output()
            .map_err(|e| e.to_string())?
            .status
            .success()
    {
        let status = std::process::Command::new("docker")
            .args([
                "build",
                "-f",
                concat!(env!("CARGO_MANIFEST_DIR"), "/docker/garnet-json.Dockerfile"),
                "-t",
                image,
                concat!(env!("CARGO_MANIFEST_DIR"), "/docker"),
            ])
            .status()
            .map_err(|e| e.to_string())?;
        if !status.success() {
            return Err("GarnetJSON image build failed".into());
        }
    }
    let append = if args.durability == Durability::None {
        "no"
    } else {
        "yes"
    };
    let sync = if args.durability == Durability::Flushed {
        "always"
    } else {
        "everysec"
    };
    let mut command = match server {
        Server::Redis | Server::Valkey => vec!["--save", "", "--appendonly", append, "--appendfsync", sync],
        Server::Dragonfly => {
            if args.durability != Durability::None {
                return Err("Dragonfly snapshot persistence does not implement buffered/flushed WAL durability; use --durability none".into());
            }
            vec![
                "--dir=/data",
                "--dbfilename=crudeval",
                "--snapshot_cron=",
                "--logtostderr",
            ]
        }
        Server::Garnet => vec![
            "--lua",
            "--lua-transaction-mode",
            "--loadmodulecs",
            "/app/modules/GarnetJSON.dll",
            "--extension-bin-paths",
            "/app/modules",
            "--bind",
            "0.0.0.0",
            "--port",
            "6379",
            "--checkpointdir",
            "/data/checkpoint",
            "--logdir",
            "/data/log",
            "--storage-tier",
            "--recover",
            "--memory",
            "256m",
            "--index",
            "64m",
        ],
        Server::Kvrocks => vec![
            "--bind",
            "0.0.0.0",
            "--port",
            "6379",
            "--dir",
            "/data",
            "--rocksdb.write_options.disable_wal",
            if args.durability == Durability::None {
                "yes"
            } else {
                "no"
            },
            "--rocksdb.write_options.sync",
            if args.durability == Durability::Flushed {
                "yes"
            } else {
                "no"
            },
        ],
    };
    if matches!(server, Server::Garnet) && args.durability != Durability::None {
        command.extend([
            "--aof",
            "--aof-commit-freq",
            if args.durability == Durability::Flushed {
                "0"
            } else {
                "1000"
            },
        ]);
        if args.durability == Durability::Flushed {
            command.push("--aof-commit-wait");
        }
    }
    let threads = dragonfly_threads.map(|n| format!("--proactor_threads={n}"));
    if let Some(threads) = &threads {
        command.push(threads.as_str());
    }
    let container = ContainerHandle::start(image, 6379, path, "/data", &[], &command)?;
    let client = Client::open(format!("redis://127.0.0.1:{}/", container.port)).map_err(|e| e.to_string())?;
    container.ready(|| {
        let mut connection = client.get_connection().map_err(|e| e.to_string())?;
        redis::cmd("PING")
            .query::<String>(&mut connection)
            .map(|_| ())
            .map_err(|e| e.to_string())
    })?;
    Ok(Box::new_in(
        RedisBackend {
            container,
            client,
            server,
            durability: args.durability,
            image,
            dragonfly_threads,
            data_model: args.data_model,
        },
        System,
    ))
}
impl Backend for RedisBackend {
    fn metadata(&self) -> BTreeMap<String, Value> {
        {
            let mut metadata = BTreeMap::new();
            metadata.extend([
                ("backend".into(), json!(format!("{:?}", self.server).to_lowercase())),
                ("durability".into(), json!(self.durability)),
                ("image".into(), json!(self.image)),
                ("dragonfly_threads".into(), json!(self.dragonfly_threads)),
                (
                    "flush".into(),
                    json!(if matches!(self.server, Server::Kvrocks) {
                        "FLUSHMEMTABLE"
                    } else {
                        "SAVE"
                    }),
                ),
                (
                    "storage".into(),
                    json!("native binary keys; no auxiliary ordered index"),
                ),
            ]);
            metadata
        }
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            data_models: &[DataModel::KeyValue, DataModel::Documents],
            ordered_ranges: false,
            transactions: false,
            batch_read: if self.data_model == DataModel::KeyValue || !matches!(self.server, Server::Garnet) {
                BatchMode::Native
            } else {
                BatchMode::Pipelined
            },
            batch_insert: BatchMode::Pipelined,
            batch_update: BatchMode::Pipelined,
            batch_delete: BatchMode::Native,
            bulk_load: BatchMode::Pipelined,
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_, System>> {
        Ok(Box::new_in(
            RedisSession {
                server: self.server,
                connection: self.client.get_connection().map_err(|e| e.to_string())?,
                data_model: self.data_model,
                pipeline: redis::pipe(),
                wire: Vec::new_in(System),
            },
            System,
        ))
    }
    fn flush(&self) -> Result<()> {
        if matches!(self.server, Server::Kvrocks) {
            let mut connection = self.client.get_connection().map_err(|e| e.to_string())?;
            return redis::cmd("FLUSHMEMTABLE")
                .query::<()>(&mut connection)
                .map_err(|e| e.to_string());
        }
        let mut connection = self.client.get_connection().map_err(|e| e.to_string())?;
        redis::cmd("SAVE")
            .query::<()>(&mut connection)
            .map_err(|e| e.to_string())
    }
    fn server_usage(&self) -> Result<Option<Value>> {
        self.container.stats().map(Some)
    }
    fn disk_bytes(&self) -> Result<u64> {
        self.container.disk_bytes()
    }
}
impl TransactionSession for RedisSession {}
impl BackendSession for RedisSession {
    fn documents(&mut self) -> Option<&mut dyn DocumentSession> {
        (self.data_model == DataModel::Documents).then_some(self)
    }
    fn insert(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        self.pipeline.clear();
        for (i, key) in keys.iter().enumerate() {
            self.pipeline
                .cmd("SET")
                .arg(key.as_bytes().as_slice())
                .arg(values.get(i).ok_or("Missing insert value")?)
                .arg("NX");
        }
        let results: Vec<Option<String>> = self.pipeline.query(&mut self.connection).map_err(|e| e.to_string())?;
        Ok(results.iter().filter(|r| r.is_some()).count())
    }
    fn read(&mut self, keys: &[Key], output: &mut RecordOutput<'_>) -> Result<usize> {
        output.clear();
        if keys.is_empty() {
            return Ok(0);
        }
        let mut cmd = redis::cmd("MGET");
        for key in keys {
            cmd.arg(key.as_bytes().as_slice());
        }
        let values: Vec<Option<Vec<u8>>> = cmd.query(&mut self.connection).map_err(|e| e.to_string())?;
        let mut found = 0;
        for value in values {
            found += usize::from(value.is_some());
            output.push(value.as_deref())?;
        }
        Ok(found)
    }
    fn update(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        self.pipeline.clear();
        for (i, key) in keys.iter().enumerate() {
            self.pipeline
                .cmd("SET")
                .arg(key.as_bytes().as_slice())
                .arg(values.get(i).ok_or("Missing update value")?)
                .arg("XX");
        }
        let results: Vec<Option<String>> = self.pipeline.query(&mut self.connection).map_err(|e| e.to_string())?;
        Ok(results.iter().filter(|r| r.is_some()).count())
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }
        let mut cmd = redis::cmd("DEL");
        for key in keys {
            cmd.arg(key.as_bytes().as_slice());
        }
        cmd.query(&mut self.connection).map_err(|e| e.to_string())
    }
    fn range_read(&mut self, _: Key, _: usize, _: &mut KeysOutput<'_>, _: &mut RecordOutput<'_>) -> Result<usize> {
        Err("Redis does not support ordered key ranges".into())
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
struct JsonDocument<'a> {
    score: u64,
    #[serde(borrow)]
    payload: &'a str,
}
impl DocumentSession for RedisSession {
    fn insert(&mut self, keys: &[Key], values: &DocumentInput<'_>) -> Result<usize> {
        self.pipeline.clear();
        for (i, key) in keys.iter().enumerate() {
            let value = values.get(i).ok_or("Missing document")?;
            self.wire.clear();
            serde_json::to_writer(
                &mut self.wire,
                &JsonDocument {
                    score: value.score,
                    payload: value.payload,
                },
            )
            .map_err(|e| e.to_string())?;
            self.pipeline
                .cmd("JSON.SET")
                .arg(key.as_bytes().as_slice())
                .arg("$")
                .arg(self.wire.as_slice());
            if !matches!(self.server, Server::Kvrocks) {
                self.pipeline.arg("NX");
            }
        }
        let results: Vec<Option<String>> = self.pipeline.query(&mut self.connection).map_err(|e| e.to_string())?;
        Ok(results.iter().filter(|v| v.is_some()).count())
    }
    fn read(&mut self, keys: &[Key], output: &mut DocumentOutput<'_>) -> Result<usize> {
        output.clear();
        self.pipeline.clear();
        if keys.is_empty() {
            return Ok(0);
        }
        let results: Vec<Option<Vec<u8>>> = if matches!(self.server, Server::Garnet) {
            for key in keys {
                self.pipeline.cmd("JSON.GET").arg(key.as_bytes().as_slice()).arg("$");
            }
            self.pipeline.query(&mut self.connection).map_err(|e| e.to_string())?
        } else {
            let mut command = redis::cmd("JSON.MGET");
            for key in keys {
                command.arg(key.as_bytes().as_slice());
            }
            command.arg("$");
            command.query(&mut self.connection).map_err(|e| e.to_string())?
        };
        let mut found = 0;
        for value in &results {
            if let Some(bytes) = value {
                let [doc]: [JsonDocument<'_>; 1] = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
                output.push(Some(DocumentRef {
                    score: doc.score,
                    payload: doc.payload,
                }))?;
                found += 1;
            } else {
                output.push(None)?;
            }
        }
        Ok(found)
    }
    fn update(&mut self, keys: &[Key], patches: &[DocumentPatch]) -> Result<usize> {
        self.pipeline.clear();
        for (key, patch) in keys.iter().zip(patches) {
            if matches!(
                self.server,
                Server::Kvrocks | Server::Valkey | Server::Dragonfly | Server::Garnet
            ) {
                self.pipeline.cmd("EVAL").arg("if redis.call('EXISTS',KEYS[1]) == 0 then return false end return redis.call('JSON.SET',KEYS[1],'$.score',ARGV[1])").arg(1).arg(key.as_bytes().as_slice()).arg(patch.score);
            } else {
                self.pipeline
                    .cmd("JSON.SET")
                    .arg(key.as_bytes().as_slice())
                    .arg("$.score")
                    .arg(patch.score)
                    .arg("XX");
            }
        }
        let results: Vec<Option<String>> = self.pipeline.query(&mut self.connection).map_err(|e| e.to_string())?;
        Ok(results.iter().filter(|v| v.is_some()).count())
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        BackendSession::delete(self, keys)
    }
    fn range_read(&mut self, _: Key, _: usize, _: &mut KeysOutput<'_>, _: &mut DocumentOutput<'_>) -> Result<usize> {
        Err("Redis does not support ordered document ranges".into())
    }
}
fn main() {
    let cli: Cli = crudeval::parse_cli();
    let settings = [
        ("Server", crudeval::spell_value(&cli.server)),
        (
            "Dragonfly threads",
            cli.dragonfly_threads
                .map_or_else(|| "auto".into(), |threads| threads.to_string()),
        ),
    ];
    if let Err(error) = run(
        cli.common,
        json!({"server":format!("{:?}",cli.server),"dragonfly_threads":cli.dragonfly_threads}),
        &settings,
        |args, path| open(args, path, cli.server, cli.dragonfly_threads),
    ) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
#[test]
#[ignore = "starts pinned Docker servers"]
fn native_protocol_contract() {
    for server in [
        Server::Redis,
        Server::Valkey,
        Server::Dragonfly,
        Server::Garnet,
        Server::Kvrocks,
    ] {
        eprintln!("native document contract: {server:?}");
        for data_model in [DataModel::Documents] {
            let path = tempfile::tempdir().unwrap();
            let mut args = Cli::parse_from(["contract"]).common;
            args.data_model = data_model;
            args.durability = Durability::None;
            let backend = open(
                &args,
                path.path(),
                server,
                matches!(server, Server::Dragonfly).then_some(NonZeroU16::new(4).unwrap()),
            )
            .unwrap();
            match data_model {
                DataModel::Documents => crudeval::assert_document_contract!(backend.as_ref()),
                DataModel::Graph => crudeval::assert_graph_contract!(backend.as_ref()),
                _ => unreachable!(),
            };
        }
    }
}

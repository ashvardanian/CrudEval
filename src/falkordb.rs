//! FalkorDB graph benchmark using Cypher over the Redis protocol.
//!
//! Requires Docker; the binary manages its own pinned FalkorDB container.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features falkordb-backend \
//!     --bin crud-eval-falkordb -- --records 1K --threads 4 --data-model graph
//! ```

mod cypher;

use std::{alloc::System, collections::BTreeMap, io::Write, path::Path};

use clap::Parser;
use redis::{Client, Connection as RedisConnection, Value as RedisValue};
use serde_json::{json, Value};

use crate::cypher::{CypherConnection, CypherOutput, CypherSession, GraphRow, Parameters, Shape};
use crudeval::{
    backend::{Backend, BackendCapabilities, BackendSession, BatchMode, DataModel, Durability, Key, Result},
    docker::ContainerHandle,
    run, BetweenWorkloads, CommonArgs,
};

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
}
struct FalkorDbBackend {
    container: ContainerHandle,
    client: Client,
    durability: Durability,
}
struct FalkorDbConnection {
    connection: RedisConnection,
    /// The `CYPHER ...` request, rebuilt in place for every query.
    wire: Vec<u8, System>,
}

impl FalkorDbConnection {
    fn new(connection: RedisConnection) -> Self {
        Self {
            connection,
            wire: Vec::new_in(System),
        }
    }
}

/// Writes `name=value ` into the request when the query names `$name`.
fn write_parameter(wire: &mut Vec<u8, System>, query: &str, name: &str, value: &impl serde::Serialize) -> Result<()> {
    if !query.split('$').skip(1).any(|rest| rest.starts_with(name)) {
        return Ok(());
    }
    wire.extend_from_slice(name.as_bytes());
    wire.push(b'=');
    serde_json::to_writer(&mut *wire, value).map_err(|e| e.to_string())?;
    wire.push(b' ');
    Ok(())
}
fn scalar_i64(value: &RedisValue) -> Result<Option<i64>> {
    match value {
        RedisValue::Nil => Ok(None),
        RedisValue::Int(n) => Ok(Some(*n)),
        _ => Err("Expected integer graph scalar".into()),
    }
}
fn scalar_key(value: &RedisValue) -> Result<Option<Key>> {
    match value {
        RedisValue::Nil => Ok(None),
        RedisValue::BulkString(bytes) => Key::parse_str(std::str::from_utf8(bytes).map_err(|e| e.to_string())?)
            .map(Some)
            .map_err(|e| e.to_string()),
        _ => Err("Expected UUID graph scalar".into()),
    }
}
impl CypherConnection for FalkorDbConnection {
    fn execute(&mut self, query: &str, params: &Parameters, output: &mut CypherOutput<'_, '_, '_>) -> Result<usize> {
        let wire = &mut self.wire;
        wire.clear();
        wire.extend_from_slice(b"CYPHER ");
        write_parameter(wire, query, "keys", &params.keys.as_slice())?;
        write_parameter(wire, query, "start", &params.start)?;
        write_parameter(wire, query, "limit", &params.limit)?;
        if query.contains("$vertices") {
            wire.extend_from_slice(b"vertices=[");
            for (i, row) in params.vertices.iter().enumerate() {
                if i > 0 {
                    wire.push(b',');
                }
                write!(wire, "{{id:'{}',version:{},edges:[", row.id, row.version).map_err(|e| e.to_string())?;
                for (j, edge) in row.edges.iter().enumerate() {
                    if j > 0 {
                        wire.push(b',');
                    }
                    write!(wire, "{{slot:{},target:'{}'}}", edge.slot, edge.target).map_err(|e| e.to_string())?;
                }
                wire.extend_from_slice(b"]}");
            }
            wire.extend_from_slice(b"] ");
        }
        if query.contains("$patches") {
            wire.extend_from_slice(b"patches=[");
            for (i, row) in params.patches.iter().enumerate() {
                if i > 0 {
                    wire.push(b',');
                }
                write!(wire, "{{id:'{}',version:{},neighbor:", row.id, row.version).map_err(|e| e.to_string())?;
                match row.neighbor {
                    Some(key) => write!(wire, "'{key}'").map_err(|e| e.to_string())?,
                    None => wire.extend_from_slice(b"null"),
                };
                wire.push(b'}');
            }
            wire.extend_from_slice(b"] ");
        }
        wire.extend_from_slice(query.as_bytes());
        let response: RedisValue = redis::cmd("GRAPH.QUERY")
            .arg("crudeval")
            .arg(wire.as_slice())
            .query(&mut self.connection)
            .map_err(|e| e.to_string())?;
        output.clear();
        let RedisValue::Array(parts) = response else {
            return Err("Invalid graph response".into());
        };
        if parts.len() == 1 {
            return Ok(0);
        }
        let Some(RedisValue::Array(rows)) = parts.get(1) else {
            return Err("Missing graph result rows".into());
        };
        for row in rows {
            let RedisValue::Array(cells) = row else {
                return Err("Invalid graph row".into());
            };
            match output.shape {
                Shape::Count => {
                    output.count = scalar_i64(cells.first().ok_or("Missing count")?)?.ok_or("Null count")? as usize
                }
                Shape::Keys => output.key(scalar_key(cells.first().ok_or("Missing key")?)?.ok_or("Null key")?)?,
                Shape::Vertices => {
                    if cells.len() != 5 {
                        return Err("Wrong graph column count".into());
                    }
                    output.row(GraphRow {
                        index: scalar_i64(&cells[0])?.ok_or("Missing row index")?,
                        key: scalar_key(&cells[1])?,
                        version: scalar_i64(&cells[2])?.map(|v| v as u64),
                        target: scalar_key(&cells[3])?,
                        slot: scalar_i64(&cells[4])?.map(|v| v as u32),
                    })?;
                }
            }
        }
        output.finish()
    }
}
fn open(args: &CommonArgs, path: &Path) -> Result<Box<dyn Backend, System>> {
    if args.between_workloads != BetweenWorkloads::Keep {
        return Err("Docker backends support only --between-workloads keep".into());
    }

    if args.data_model != DataModel::Graph {
        return Err("FalkorDB supports only graph data model".into());
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
    let redis_args = format!("--save '' --appendonly {append} --appendfsync {sync}");
    let container = ContainerHandle::start(
        "falkordb/falkordb:6.0.0",
        6379,
        path,
        "/data",
        &[("REDIS_ARGS", &redis_args)],
        &[],
    )?;
    let client = Client::open(format!("redis://127.0.0.1:{}/", container.port)).map_err(|e| e.to_string())?;
    container.ready(|| {
        let mut c = FalkorDbConnection::new(client.get_connection().map_err(|e| e.to_string())?);
        c.execute(
            "RETURN 1 AS count",
            &Parameters::default(),
            &mut CypherOutput::new(Shape::Count, None, None),
        )
        .map(|_| ())
    })?;
    let mut connection = FalkorDbConnection::new(client.get_connection().map_err(|e| e.to_string())?);
    if let Err(error) = connection.execute(
        "CREATE INDEX FOR (v:Vertex) ON (v.id)",
        &Parameters::default(),
        &mut CypherOutput::new(Shape::Count, None, None),
    ) {
        if !error.contains("'id' is already indexed") {
            return Err(error);
        }
    }
    Ok(Box::new_in(
        FalkorDbBackend {
            container,
            client,
            durability: args.durability,
        },
        System,
    ))
}
impl Backend for FalkorDbBackend {
    fn metadata(&self) -> BTreeMap<String, Value> {
        {
            let mut metadata = BTreeMap::new();
            metadata.extend([
                ("backend".into(), json!("falkordb")),
                ("image".into(), json!("falkordb/falkordb:6.0.0")),
                ("durability".into(), json!(self.durability)),
            ]);
            metadata
        }
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            data_models: &[DataModel::Graph],
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
            CypherSession::new(FalkorDbConnection::new(
                self.client.get_connection().map_err(|e| e.to_string())?,
            )),
            System,
        ))
    }
    fn flush(&self) -> Result<()> {
        redis::cmd("SAVE")
            .query::<()>(&mut self.client.get_connection().map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())
    }
    fn server_usage(&self) -> Result<Option<Value>> {
        self.container.stats().map(Some)
    }
    fn disk_bytes(&self) -> Result<u64> {
        self.container.disk_bytes()
    }
}
fn main() {
    let cli: Cli = crudeval::parse_cli();
    if let Err(error) = run(cli.common, "falkordb/falkordb:6.0.0", &[], open) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
#[test]
#[ignore = "starts pinned Docker servers"]
fn native_protocol_contract() {
    for data_model in [DataModel::Graph] {
        let path = tempfile::tempdir().unwrap();
        let mut args = Cli::parse_from(["contract"]).common;
        args.data_model = data_model;
        args.durability = Durability::None;
        let backend = open(&args, path.path()).unwrap();
        match data_model {
            DataModel::Documents => crudeval::assert_document_contract!(backend.as_ref()),
            DataModel::Graph => crudeval::assert_graph_contract!(backend.as_ref()),
            _ => unreachable!(),
        };
    }
}

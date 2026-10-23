//! Neo4j and Memgraph graph benchmark using Bolt and Cypher.
//!
//! Requires Docker; the binary manages its own pinned graph server.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features neo4j-backend \
//!     --bin crud-eval-neo4j -- --records 1K --threads 4 --data-model graph --durability flushed
//! ```

mod cypher;

use std::{alloc::System, collections::BTreeMap, path::Path, sync::Arc};

use clap::{Parser, ValueEnum};
use neo4rs::{query, BoltType, ConfigBuilder, Graph};
use serde_json::{json, Value};
use tokio::runtime::Runtime;

use crate::cypher::{CypherConnection, CypherOutput, CypherSession, GraphRow, Parameters, Shape};
use crudeval::{
    backend::{Backend, BackendCapabilities, BackendSession, BatchMode, DataModel, Durability, Key, Result},
    docker::ContainerHandle,
    run, BetweenWorkloads, CommonArgs,
};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Server {
    Neo4j,
    Memgraph,
}
#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long, env = "CRUDEVAL_SERVER", value_enum, default_value = "neo4j")]
    server: Server,
}
struct Neo4jBackend {
    container: ContainerHandle,
    runtime: Arc<Runtime>,
    graph: Graph,
    server: Server,
    image: &'static str,
}
struct Neo4jConnection {
    runtime: Arc<Runtime>,
    graph: Graph,
    optimistic_reads: bool,
    before: Vec<(Option<Key>, i64), System>,
    after: Vec<(Option<Key>, i64), System>,
}
fn bolt_map(fields: impl IntoIterator<Item = (&'static str, BoltType)>) -> BoltType {
    fields
        .into_iter()
        .map(|(key, value)| (key.into(), value))
        .collect::<std::collections::HashMap<neo4rs::BoltString, BoltType>>()
        .into()
}
fn bolt_query(text: &str, params: &Parameters, revisions: bool) -> neo4rs::Query {
    let rewritten;
    let query_text = if revisions && text.contains("v.version=row.version") {
        rewritten = text.replace(
            "v.version=row.version",
            "v.version=row.version,v.revision=coalesce(v.revision,0)+1",
        );
        rewritten.as_str()
    } else if revisions && text.contains("DETACH DELETE") {
        "UNWIND $keys AS key MATCH (v:Vertex {id:key,active:true}) OPTIONAL MATCH (source:Vertex)-[:Edge]->(v) SET source.revision=coalesce(source.revision,0)+1 WITH DISTINCT v DETACH DELETE v RETURN count(v) AS count"
    } else {
        text
    };
    let mut query = query(query_text);
    if text.contains("$keys") {
        query = query.param("keys", params.keys.iter().map(Key::to_string).collect::<Vec<_>>());
    }
    if text.contains("$start") {
        query = query
            .param("start", params.start.to_string())
            .param("limit", params.limit);
    }
    if text.contains("$vertices") {
        let rows: Vec<BoltType> = params
            .vertices
            .iter()
            .map(|row| {
                bolt_map([
                    ("id", row.id.to_string().into()),
                    ("version", (row.version as i64).into()),
                    (
                        "edges",
                        row.edges
                            .iter()
                            .map(|edge| {
                                bolt_map([
                                    ("slot", (edge.slot as i64).into()),
                                    ("target", edge.target.to_string().into()),
                                ])
                            })
                            .collect::<Vec<_>>()
                            .into(),
                    ),
                ])
            })
            .collect();
        query = query.param("vertices", rows);
    }
    if text.contains("$patches") {
        let rows: Vec<BoltType> = params
            .patches
            .iter()
            .map(|row| {
                bolt_map([
                    ("id", row.id.to_string().into()),
                    ("version", (row.version as i64).into()),
                    (
                        "neighbor",
                        row.neighbor
                            .map(|key| key.to_string().into())
                            .unwrap_or(BoltType::Null(neo4rs::BoltNull)),
                    ),
                ])
            })
            .collect();
        query = query.param("patches", rows);
    }
    query
}
fn row_key(row: &neo4rs::Row, name: &str) -> Result<Option<Key>> {
    row.get::<Option<String>>(name)
        .map_err(|e| e.to_string())?
        .map(|s| Key::parse_str(&s).map_err(|e| e.to_string()))
        .transpose()
}
async fn read_revisions(
    graph: &Graph,
    params: &Parameters,
    point: bool,
    output: &mut Vec<(Option<Key>, i64), System>,
) -> Result<()> {
    output.clear();
    let text = if point {
        "UNWIND range(0,size($keys)-1) AS n OPTIONAL MATCH (v:Vertex {id:$keys[n],active:true}) RETURN v.id AS id,coalesce(v.revision,0) AS revision ORDER BY n"
    } else {
        "MATCH (v:Vertex {active:true}) WHERE v.id >= $start RETURN v.id AS id,coalesce(v.revision,0) AS revision ORDER BY id LIMIT $limit"
    };
    let mut stream = graph
        .execute(bolt_query(text, params, false))
        .await
        .map_err(|e| e.to_string())?;
    let mut decoding = Ok(());
    while let Some(row) = stream.next().await.map_err(|e| e.to_string())? {
        if decoding.is_err() {
            continue;
        }
        decoding = (|| {
            output.push((row_key(&row, "id")?, row.get("revision").map_err(|e| e.to_string())?));
            Ok(())
        })();
    }
    decoding
}
impl CypherConnection for Neo4jConnection {
    fn execute(&mut self, text: &str, params: &Parameters, output: &mut CypherOutput<'_, '_, '_>) -> Result<usize> {
        self.runtime.block_on(async {
            for attempt in 0..8 {
                output.clear();
                let result: Result<usize> = async {
                    let checking = self.optimistic_reads && matches!(output.shape, Shape::Vertices);
                    if checking {
                        read_revisions(&self.graph, params, text.contains("$keys"), &mut self.before).await?;
                    }
                    let mut stream = self
                        .graph
                        .execute(bolt_query(text, params, self.optimistic_reads))
                        .await
                        .map_err(|e| e.to_string())?;
                    let mut decoding = Ok(());
                    while let Some(row) = stream.next().await.map_err(|e| e.to_string())? {
                        if decoding.is_err() {
                            continue;
                        }
                        decoding = (|| match output.shape {
                            Shape::Count => {
                                output.count = row.get::<i64>("count").map_err(|e| e.to_string())? as usize;
                                Ok(())
                            }
                            Shape::Keys => output.key(row_key(&row, "id")?.ok_or("Missing graph key")?),
                            Shape::Vertices => output.row(GraphRow {
                                index: row.get("n").map_err(|e| e.to_string())?,
                                key: row_key(&row, "id")?,
                                version: row
                                    .get::<Option<i64>>("version")
                                    .map_err(|e| e.to_string())?
                                    .map(|v| v as u64),
                                target: row_key(&row, "target")?,
                                slot: row
                                    .get::<Option<i64>>("slot")
                                    .map_err(|e| e.to_string())?
                                    .map(|v| v as u32),
                            }),
                        })();
                    }
                    if checking {
                        read_revisions(&self.graph, params, text.contains("$keys"), &mut self.after).await?;
                        if self.before != self.after {
                            return Err("graph revision changed during read".into());
                        }
                    }
                    decoding?;
                    output.finish()
                }
                .await;
                match result {
                    Err(ref error)
                        if attempt < 7
                            && (error == "graph revision changed during read"
                                || error.contains("TransientError")
                                || error.contains("SerializationError")
                                || error.contains("UniquenessConstraintViolation")
                                || error.contains("Unable to commit due to unique constraint violation")) =>
                    {
                        tokio::time::sleep(std::time::Duration::from_millis(5 << attempt)).await
                    }
                    _ => return result,
                }
            }
            unreachable!()
        })
    }
}

fn open(args: &CommonArgs, path: &Path, server: Server) -> Result<Box<dyn Backend, System>> {
    if args.between_workloads != BetweenWorkloads::Keep {
        return Err("Docker backends support only --between-workloads keep".into());
    }

    if args.data_model != DataModel::Graph {
        return Err("Neo4j and Memgraph support only graph data model".into());
    }
    if args.durability != Durability::Flushed {
        return Err("Neo4j/Memgraph require --durability flushed".into());
    }
    let (image, storage, env) = match server {
        Server::Neo4j => (
            "neo4j:2026.09.0-community",
            "/data",
            vec![("NEO4J_AUTH", "neo4j/crudeval-password")],
        ),
        Server::Memgraph => ("memgraph/memgraph:3.13.1", "/var/lib/memgraph", vec![]),
    };
    let command = match server {
        Server::Neo4j => vec![],
        Server::Memgraph => vec![
            "--storage-wal-enabled=true",
            "--storage-snapshot-interval-sec=300",
            "--storage-wal-file-flush-every-n-tx=1",
        ],
    };
    let container = ContainerHandle::start(image, 7687, path, storage, &env, &command)?;
    let runtime = Arc::new(Runtime::new().map_err(|e| e.to_string())?);
    let uri = format!("127.0.0.1:{}", container.port);
    let (user, password) = match server {
        Server::Neo4j => ("neo4j", "crudeval-password"),
        Server::Memgraph => ("", ""),
    };
    let database = match server {
        Server::Neo4j => "neo4j",
        Server::Memgraph => "memgraph",
    };
    let config = ConfigBuilder::new()
        .uri(&uri)
        .user(user)
        .password(password)
        .db(database)
        .build()
        .map_err(|e| e.to_string())?;
    container.ready(|| {
        runtime.block_on(async {
            let graph = Graph::connect(config.clone()).await.map_err(|e| e.to_string())?;
            graph.run(query("RETURN 1")).await.map_err(|e| e.to_string())
        })
    })?;
    let graph = runtime.block_on(Graph::connect(config)).map_err(|e| e.to_string())?;
    let index = match server {
        Server::Neo4j => "CREATE CONSTRAINT vertex_id IF NOT EXISTS FOR (v:Vertex) REQUIRE v.id IS UNIQUE",
        Server::Memgraph => "CREATE INDEX ON :Vertex(id)",
    };
    runtime.block_on(graph.run(query(index))).map_err(|e| e.to_string())?;
    if matches!(server, Server::Memgraph) {
        runtime
            .block_on(graph.run(query("CREATE CONSTRAINT ON (v:Vertex) ASSERT v.id IS UNIQUE")))
            .map_err(|e| e.to_string())?;
    }
    Ok(Box::new_in(
        Neo4jBackend {
            container,
            runtime,
            graph,
            server,
            image,
        },
        System,
    ))
}
impl Backend for Neo4jBackend {
    fn metadata(&self) -> BTreeMap<String, Value> {
        {
            let mut metadata = BTreeMap::new();
            metadata.extend([
                ("backend".into(), json!(format!("{:?}", self.server).to_lowercase())),
                ("image".into(), json!(self.image)),
                ("durability".into(), json!("flushed")),
            ("read_consistency".into(),json!(if matches!(self.server,Server::Neo4j) {"adjacency bracketed by native revision reads; up to 8 measured attempts, 3 round trips per attempt"}else{"native snapshot query"})),
            ("revision_storage".into(),json!(matches!(self.server,Server::Neo4j))),
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
            CypherSession::new(Neo4jConnection {
                runtime: self.runtime.clone(),
                graph: self.graph.clone(),
                optimistic_reads: matches!(self.server, Server::Neo4j),
                before: Vec::new_in(System),
                after: Vec::new_in(System),
            }),
            System,
        ))
    }
    fn flush(&self) -> Result<()> {
        Ok(())
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
    for server in [Server::Neo4j, Server::Memgraph] {
        for data_model in [DataModel::Graph] {
            let path = tempfile::tempdir().unwrap();
            let mut args = Cli::parse_from(["contract"]).common;
            args.data_model = data_model;
            args.durability = Durability::Flushed;
            let backend = open(&args, path.path(), server).unwrap();
            match data_model {
                DataModel::Documents => crudeval::assert_document_contract!(backend.as_ref()),
                DataModel::Graph => crudeval::assert_graph_contract!(backend.as_ref()),
                _ => unreachable!(),
            };
        }
    }
}

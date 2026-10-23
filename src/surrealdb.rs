//! SurrealDB document and native relation benchmarks over synchronous HTTP RPC.
//!
//! Requires Docker and a pinned SurrealDB 3.3.0 server.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features surrealdb-backend \
//!     --bin crud-eval-surrealdb -- --data-model documents --durability flushed
//! ```

use std::{
    alloc::System,
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use clap::Parser;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

use crudeval::{
    backend::{
        Backend, BackendCapabilities, BackendSession, BatchMode, DataModel, DocumentInput, DocumentOutput,
        DocumentPatch, DocumentRef, DocumentSession, Durability, GraphEdge, GraphInput, GraphOutput, GraphPatch,
        GraphSession, Key, KeysOutput, Result, TransactionSession, VertexRef,
    },
    docker::ContainerHandle,
    run, BetweenWorkloads, CommonArgs,
};

const IMAGE: &str = "surrealdb/surrealdb:v3.3.0";
const RPC_PAYLOAD_LIMIT: usize = (4 << 20) - (16 << 10);
#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
}
struct SurrealBackend {
    container: ContainerHandle,
    url: String,
    data_model: DataModel,
    path: PathBuf,
}
struct SurrealSession {
    agent: ureq::Agent,
    url: String,
    data_model: DataModel,
    edges: Vec<GraphEdge, System>,
}
#[derive(Serialize)]
struct RpcRequest<'a, P> {
    id: u64,
    method: &'static str,
    params: (&'a str, P),
}
#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<Vec<Statement<T>>>,
    error: Option<RpcError>,
}
#[derive(Deserialize)]
struct RpcError {
    message: String,
}
#[derive(Deserialize)]
struct Statement<T> {
    status: String,
    result: Option<WireResult<T>>,
}
#[derive(Deserialize)]
#[serde(untagged)]
enum WireResult<T> {
    Data(T),
    Error(String),
}
#[derive(Serialize)]
struct DocumentWrite<'a> {
    id: Key,
    score: u64,
    payload: &'a str,
}
#[derive(Deserialize)]
struct DocumentRow {
    key: Key,
    score: u64,
    payload: String,
}
#[derive(Serialize)]
struct ScoreWrite {
    id: Key,
    score: u64,
}
#[derive(Serialize)]
struct VertexWrite {
    id: Key,
    version: u64,
}
#[derive(Serialize)]
struct EdgeWrite {
    source: Key,
    slot: u32,
    target: Key,
}
#[derive(Serialize)]
struct GraphWrite {
    id: Key,
    version: u64,
    neighbor: Option<Key>,
}
#[derive(Deserialize)]
struct EdgeRow {
    slot: u32,
    target: Key,
}
#[derive(Deserialize)]
struct VertexRow {
    key: Key,
    version: u64,
    edges: Vec<EdgeRow>,
}
impl SurrealSession {
    fn new(url: String, data_model: DataModel) -> Self {
        Self {
            agent: ureq::Agent::new_with_defaults(),
            url,
            data_model,
            edges: Vec::new_in(System),
        }
    }
    fn query<T: DeserializeOwned, P: Serialize>(&self, sql: &str, params: P) -> Result<T> {
        for attempt in 0..8 {
            let result = self.query_once(sql, &params);
            match &result {
                Err(error)
                    if attempt < 7 && error.contains("Transaction conflict") && error.contains("can be retried") =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(2 << attempt))
                }
                _ => return result,
            }
        }
        unreachable!()
    }
    fn query_once<T: DeserializeOwned, P: Serialize>(&self, sql: &str, params: P) -> Result<T> {
        let mut response = self
            .agent
            .post(&self.url)
            .header("Authorization", "Basic Y3J1ZGV2YWw6Y3J1ZGV2YWw=")
            .header("Surreal-NS", "crudeval")
            .header("Surreal-DB", "crudeval")
            .send_json(RpcRequest {
                id: 1,
                method: "query",
                params: (sql, params),
            })
            .map_err(|e| e.to_string())?;
        let response: RpcResponse<T> = response.body_mut().read_json().map_err(|e| e.to_string())?;
        if let Some(error) = response.error {
            return Err(error.message);
        }
        let mut result = None;
        let mut errors = String::new();
        for statement in response.result.ok_or("Missing RPC result")? {
            if statement.status != "OK" {
                if let Some(WireResult::Error(message)) = statement.result {
                    errors.push_str(&message);
                    errors.push('\n');
                }
                continue;
            }
            if let Some(WireResult::Data(value)) = statement.result {
                result = Some(value);
            }
        }
        if !errors.is_empty() {
            return Err(errors);
        }
        result.ok_or_else(|| "Missing query result".into())
    }
    fn graph_row(&mut self, row: &VertexRow, out: &mut GraphOutput<'_>) -> Result<()> {
        self.edges.clear();
        self.edges.extend(row.edges.iter().map(|e| GraphEdge {
            slot: e.slot,
            target: e.target,
        }));
        out.push(Some(VertexRef {
            version: row.version,
            edges: &self.edges,
        }))
    }
}
fn open(args: &CommonArgs, path: &Path) -> Result<Box<dyn Backend, System>> {
    if args.between_workloads != BetweenWorkloads::Keep {
        return Err("Docker backends support only --between-workloads keep".into());
    }
    if args.data_model == DataModel::KeyValue {
        return Err("SurrealDB supports documents and native graphs".into());
    }
    if args.durability != Durability::Flushed {
        return Err("SurrealKV requires --durability flushed".into());
    }
    let container = ContainerHandle::start(
        IMAGE,
        8000,
        path,
        "/data",
        &[],
        &[
            "start",
            "--bind",
            "0.0.0.0:8000",
            "--user",
            "crudeval",
            "--pass",
            "crudeval",
            "surrealkv:/data/db?sync=every",
        ],
    )?;
    let url = format!("http://127.0.0.1:{}/rpc", container.port);
    let session = SurrealSession::new(url.clone(), args.data_model);
    container.ready(|| {
        session
            .query::<u64, _>(
                "DEFINE NAMESPACE IF NOT EXISTS crudeval; DEFINE DATABASE IF NOT EXISTS crudeval; RETURN 1;",
                (),
            )
            .map(|_| ())
    })?;
    Ok(Box::new_in(
        SurrealBackend {
            container,
            url,
            data_model: args.data_model,
            path: path.to_path_buf(),
        },
        System,
    ))
}
impl Backend for SurrealBackend {
    fn metadata(&self) -> BTreeMap<String, Value> {
        let mut result = BTreeMap::new();
        result.extend([
            ("backend".into(), json!("surrealdb")),
            ("image".into(), json!(IMAGE)),
            ("rpc_insert_payload_limit_bytes".into(),json!(RPC_PAYLOAD_LIMIT)),
            ("storage".into(), json!("SurrealKV")),
            ("durability".into(), json!("flushed")),
            (
                "graph_storage".into(),
                json!("native RELATE edges; incident edges removed on vertex deletion"),
            ),
            (
                "batch_execution".into(),
                json!("native inserts chunked below 4 MB RPC limit, one transaction per chunk; native document/vertex/relation inserts; scalar and graph updates execute in server FOR loops"),
            ),
        ]);
        result
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            data_models: &[DataModel::Documents, DataModel::Graph],
            ordered_ranges: true,
            batch_read: BatchMode::Native,
            batch_insert: BatchMode::Native,
            batch_update: BatchMode::Pipelined,
            batch_delete: BatchMode::Native,
            bulk_load: BatchMode::Native,
            ..Default::default()
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_, System>> {
        Ok(Box::new_in(
            SurrealSession::new(self.url.clone(), self.data_model),
            System,
        ))
    }
    fn flush(&self) -> Result<()> {
        Ok(())
    }
    fn disk_bytes(&self) -> Result<u64> {
        crudeval::backend::directory_bytes(&self.path)
    }
    fn server_usage(&self) -> Result<Option<Value>> {
        self.container.stats().map(Some)
    }
}
impl TransactionSession for SurrealSession {}
impl BackendSession for SurrealSession {
    fn documents(&mut self) -> Option<&mut dyn DocumentSession> {
        (self.data_model == DataModel::Documents).then_some(self)
    }
    fn graph(&mut self) -> Option<&mut dyn GraphSession> {
        (self.data_model == DataModel::Graph).then_some(self)
    }
}
impl DocumentSession for SurrealSession {
    fn insert(&mut self, keys: &[Key], values: &DocumentInput<'_>) -> Result<usize> {
        #[derive(Serialize)]
        struct Params<'a> {
            rows: &'a [DocumentWrite<'a>],
        }
        let mut rows = Vec::new_in(System);
        let mut total = 0;
        let mut start = 0;
        while start < keys.len() {
            rows.clear();
            let mut bytes = 0;
            while start + rows.len() < keys.len() {
                let index = start + rows.len();
                let value = values.get(index).ok_or("Missing document")?;
                let size = 128
                    + value
                        .payload
                        .bytes()
                        .map(|b| match b {
                            b'"' | b'\\' => 2,
                            0..=31 => 6,
                            _ => 1,
                        })
                        .sum::<usize>();
                if size > RPC_PAYLOAD_LIMIT {
                    return Err("Document exceeds the SurrealDB 4 MB RPC request limit".into());
                }
                if bytes + size > RPC_PAYLOAD_LIMIT {
                    break;
                }
                bytes += size;
                rows.push(DocumentWrite {
                    id: keys[index],
                    score: value.score,
                    payload: value.payload,
                });
            }
            total += self.query::<usize, _>(
                "RETURN count((INSERT IGNORE INTO document $rows RETURN id));",
                Params { rows: &rows },
            )?;
            start += rows.len();
        }
        Ok(total)
    }
    fn read(&mut self, keys: &[Key], out: &mut DocumentOutput<'_>) -> Result<usize> {
        out.clear();
        #[derive(Serialize)]
        struct Params<'a> {
            keys: &'a [Key],
        }
        let mut rows: Vec<DocumentRow> = self.query(
            "SELECT record::id(id) AS key,score,payload FROM $keys.map(|$key|type::record('document',$key));",
            Params { keys },
        )?;
        rows.sort_unstable_by_key(|row| row.key);
        let mut found = 0;
        for key in keys {
            if let Ok(i) = rows.binary_search_by_key(key, |row| row.key) {
                let row = &rows[i];
                out.push(Some(DocumentRef {
                    score: row.score,
                    payload: &row.payload,
                }))?;
                found += 1;
            } else {
                out.push(None)?;
            }
        }
        Ok(found)
    }
    fn update(&mut self, keys: &[Key], patches: &[DocumentPatch]) -> Result<usize> {
        let rows: Vec<_> = keys
            .iter()
            .zip(patches)
            .map(|(key, p)| ScoreWrite {
                id: *key,
                score: p.score,
            })
            .collect();
        #[derive(Serialize)]
        struct Params<T> {
            rows: T,
        }
        self.query("BEGIN TRANSACTION; LET $live=(SELECT VALUE record::id(id) FROM $rows.map(|$row|type::record('document',$row.id))); FOR $row IN $rows { UPDATE type::record('document',$row.id) SET score=$row.score RETURN NONE; }; RETURN count($live); COMMIT TRANSACTION;",Params {rows})
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        #[derive(Serialize)]
        struct Params<'a> {
            keys: &'a [Key],
        }
        self.query(
            "RETURN count((DELETE $keys.map(|$key|type::record('document',$key)) RETURN BEFORE));",
            Params { keys },
        )
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
        #[derive(Serialize)]
        struct Params {
            start: Key,
            limit: usize,
        }
        let rows:Vec<DocumentRow>=self.query("SELECT record::id(id) AS key,score,payload FROM document WHERE id >= type::record('document',$start) ORDER BY id LIMIT $limit;",Params {start,limit})?;
        for row in rows {
            keys.push(row.key)?;
            out.push(Some(DocumentRef {
                score: row.score,
                payload: &row.payload,
            }))?;
        }
        Ok(keys.len())
    }
}
impl GraphSession for SurrealSession {
    fn insert(&mut self, keys: &[Key], values: &GraphInput<'_>) -> Result<usize> {
        #[derive(Serialize)]
        struct Params<'a> {
            vertices: &'a [VertexWrite],
            edges: &'a [EdgeWrite],
        }
        let mut vertices = Vec::new_in(System);
        let mut edges = Vec::new_in(System);
        let mut start = 0;
        let mut total = 0;
        while start < keys.len() {
            vertices.clear();
            edges.clear();
            let mut bytes = 0;
            while start + vertices.len() < keys.len() {
                let index = start + vertices.len();
                let value = values.get(index).ok_or("Missing vertex")?;
                let size = 128 + value.edges.len() * 128;
                if size > RPC_PAYLOAD_LIMIT {
                    return Err("Vertex adjacency exceeds the SurrealDB 4 MB RPC request limit".into());
                }
                if bytes + size > RPC_PAYLOAD_LIMIT {
                    break;
                }
                bytes += size;
                let key = keys[index];
                vertices.push(VertexWrite {
                    id: key,
                    version: value.version,
                });
                edges.extend(value.edges.iter().map(|e| EdgeWrite {
                    source: key,
                    slot: e.slot,
                    target: e.target,
                }));
            }
            total+=self.query::<usize,_>("BEGIN TRANSACTION; LET $new=(INSERT IGNORE INTO vertex $vertices RETURN id); LET $relations=$edges.filter(|$edge| type::record('vertex',$edge.source) IN $new.id).map(|$edge| {id:type::record('edge',[$edge.source,$edge.slot]),in:type::record('vertex',$edge.source),out:type::record('vertex',$edge.target),slot:$edge.slot}); LET $inserted=(INSERT RELATION INTO edge $relations RETURN NONE); RETURN count($new); COMMIT TRANSACTION;",Params {vertices:&vertices,edges:&edges})?;
            start += vertices.len();
        }
        Ok(total)
    }
    fn read(&mut self, keys: &[Key], out: &mut GraphOutput<'_>) -> Result<usize> {
        out.clear();
        #[derive(Serialize)]
        struct Params<'a> {
            keys: &'a [Key],
        }
        let mut rows:Vec<VertexRow>=self.query("SELECT record::id(id) AS key,version,(SELECT slot,record::id(out) AS target FROM ->edge ORDER BY slot) AS edges FROM $keys.map(|$key|type::record('vertex',$key));",Params {keys})?;
        rows.sort_unstable_by_key(|row| row.key);
        let mut found = 0;
        for key in keys {
            if let Ok(i) = rows.binary_search_by_key(key, |row| row.key) {
                self.graph_row(&rows[i], out)?;
                found += 1;
            } else {
                out.push(None)?;
            }
        }
        Ok(found)
    }
    fn update(&mut self, keys: &[Key], patches: &[GraphPatch]) -> Result<usize> {
        let rows: Vec<_> = keys
            .iter()
            .zip(patches)
            .map(|(key, p)| GraphWrite {
                id: *key,
                version: p.version,
                neighbor: p.neighbor,
            })
            .collect();
        #[derive(Serialize)]
        struct Params<T> {
            rows: T,
        }
        self.query("BEGIN TRANSACTION; LET $live=(SELECT VALUE record::id(id) FROM $rows.map(|$row|type::record('vertex',$row.id))); FOR $row IN $rows { IF $row.id IN $live { UPDATE type::record('vertex',$row.id) SET version=$row.version RETURN NONE; DELETE type::record('edge',[$row.id,0]) RETURN NONE; IF $row.neighbor != NONE AND $row.neighbor != NULL { RELATE (type::record('vertex',$row.id))->(type::record('edge',[$row.id,0]))->(type::record('vertex',$row.neighbor)) SET slot=0 RETURN NONE; }; }; }; RETURN count($live); COMMIT TRANSACTION;",Params {rows})
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        #[derive(Serialize)]
        struct Params<'a> {
            keys: &'a [Key],
        }
        self.query(
            "RETURN count((DELETE $keys.map(|$key|type::record('vertex',$key)) RETURN BEFORE));",
            Params { keys },
        )
    }
    fn range_read(
        &mut self,
        start: Key,
        limit: usize,
        keys: &mut KeysOutput<'_>,
        out: &mut GraphOutput<'_>,
    ) -> Result<usize> {
        keys.clear();
        out.clear();
        #[derive(Serialize)]
        struct Params {
            start: Key,
            limit: usize,
        }
        let rows:Vec<VertexRow>=self.query("SELECT record::id(id) AS key,version,(SELECT slot,record::id(out) AS target FROM ->edge ORDER BY slot) AS edges FROM vertex WHERE id >= type::record('vertex',$start) ORDER BY id LIMIT $limit;",Params {start,limit})?;
        for row in rows {
            keys.push(row.key)?;
            self.graph_row(&row, out)?;
        }
        Ok(keys.len())
    }
    fn expand_neighbors(&mut self, start: Key, limit: usize, keys: &mut KeysOutput<'_>) -> Result<usize> {
        keys.clear();
        #[derive(Serialize)]
        struct Params {
            start: Key,
            limit: usize,
        }
        let rows:Vec<Key>=self.query("LET $targets=(SELECT VALUE record::id(id) FROM type::record('vertex',$start)->edge->vertex->edge->vertex); RETURN array::slice(array::sort(array::distinct($targets).filter(|$key|$key != $start)),0,$limit);",Params {start,limit})?;
        for key in rows {
            keys.push(key)?;
        }
        Ok(keys.len())
    }
}
fn main() {
    let cli: Cli = crudeval::parse_cli();
    if let Err(error) = run(cli.common, IMAGE, &[], open) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
#[test]
#[ignore = "starts pinned Docker servers"]
fn native_protocol_contract() {
    for data_model in [DataModel::Documents, DataModel::Graph] {
        let path = tempfile::tempdir().unwrap();
        let mut args = Cli::parse_from(["contract"]).common;
        args.data_model = data_model;
        args.durability = Durability::Flushed;
        let backend = open(&args, path.path()).unwrap();
        match data_model {
            DataModel::Documents => crudeval::assert_document_contract!(backend.as_ref()),
            DataModel::Graph => crudeval::assert_graph_contract!(backend.as_ref()),
            _ => unreachable!(),
        };
    }
}

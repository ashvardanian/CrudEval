//! PostgreSQL benchmark for key-value records, JSONB documents, and graphs.
//!
//! Requires Docker; the binary manages its own pinned PostgreSQL container.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features postgres-backend \
//!     --bin crud-eval-postgres -- --records 100K --threads 4
//! ```

use std::{alloc::System, collections::BTreeMap, path::Path};

use clap::Parser;
use postgres::{Client, NoTls};
use serde_json::{json, Value};

use crudeval::{
    backend::{
        Backend, BackendCapabilities, BackendSession, BatchMode, DataModel, DocumentInput, DocumentOutput,
        DocumentPatch, DocumentRef, DocumentSession, Durability, GraphEdge, GraphInput, GraphOutput, GraphPatch,
        GraphSession, Key, KeysOutput, RecordInput, RecordOutput, Result, TransactionSession, VertexRef,
    },
    docker::ContainerHandle,
    run, BetweenWorkloads, CommonArgs,
};

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
}
struct PostgresBackend {
    container: ContainerHandle,
    url: String,
    data_model: DataModel,
    durability: Durability,
}
struct PostgresSession {
    client: Client,
    data_model: DataModel,
    transaction: bool,
    statements: BTreeMap<&'static str, postgres::Statement>,
    edges: Vec<GraphEdge, System>,
}
fn open(args: &CommonArgs, path: &Path) -> Result<Box<dyn Backend, System>> {
    if args.between_workloads != BetweenWorkloads::Keep {
        return Err("Docker backends support only --between-workloads keep".into());
    }
    let sync = if args.durability == Durability::Flushed {
        "on"
    } else {
        "off"
    };
    let fsync = if args.durability == Durability::None {
        "off"
    } else {
        "on"
    };
    let container = ContainerHandle::start(
        "postgres:18.6",
        5432,
        path,
        "/var/lib/postgresql",
        &[("POSTGRES_PASSWORD", "crudeval"), ("POSTGRES_DB", "crudeval")],
        &[
            "postgres",
            "-c",
            &format!("fsync={fsync}"),
            "-c",
            &format!("synchronous_commit={sync}"),
        ],
    )?;
    let url = format!(
        "host=127.0.0.1 port={} user=postgres password=crudeval dbname=crudeval connect_timeout=2",
        container.port
    );
    container.ready(|| Client::connect(&url, NoTls).map(|_| ()).map_err(|e| format!("{e:?}")))?;
    let mut client = Client::connect(&url, NoTls).map_err(|e| format!("{e:?}"))?;
    let schema=match args.data_model { DataModel::KeyValue=>"CREATE TABLE IF NOT EXISTS records (id bytea PRIMARY KEY, value bytea NOT NULL)",DataModel::Documents=>"CREATE TABLE IF NOT EXISTS records (id bytea PRIMARY KEY, value jsonb NOT NULL)",DataModel::Graph=>"CREATE TABLE IF NOT EXISTS records (id bytea PRIMARY KEY, version bigint NOT NULL); CREATE TABLE IF NOT EXISTS edges (source bytea NOT NULL REFERENCES records(id) ON DELETE CASCADE, slot integer NOT NULL, target bytea NOT NULL, PRIMARY KEY(source,slot)); CREATE INDEX IF NOT EXISTS edges_target ON edges(target)"};
    client.batch_execute(schema).map_err(|e| format!("{e:?}"))?;
    Ok(Box::new_in(
        PostgresBackend {
            container,
            url,
            data_model: args.data_model,
            durability: args.durability,
        },
        System,
    ))
}
impl Backend for PostgresBackend {
    fn metadata(&self) -> BTreeMap<String, Value> {
        {
            let mut metadata = BTreeMap::new();
            metadata.extend([
                ("backend".into(), json!("postgres")),
                ("image".into(), json!("postgres:18.6")),
                ("durability".into(), json!(self.durability)),
                ("wal_enabled".into(), json!(true)),
                ("fsync".into(), json!(self.durability != Durability::None)),
                (
                    "synchronous_commit".into(),
                    json!(self.durability == Durability::Flushed),
                ),
            ]);
            metadata
        }
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            data_models: &[DataModel::KeyValue, DataModel::Documents, DataModel::Graph],
            ordered_ranges: true,
            transactions: true,
            batch_read: BatchMode::Native,
            batch_insert: BatchMode::Native,
            batch_update: BatchMode::Native,
            batch_delete: BatchMode::Native,
            bulk_load: BatchMode::Native,
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_, System>> {
        Ok(Box::new_in(
            PostgresSession {
                client: Client::connect(&self.url, NoTls).map_err(|e| format!("{e:?}"))?,
                data_model: self.data_model,
                transaction: false,
                statements: BTreeMap::new(),
                edges: Vec::new_in(System),
            },
            System,
        ))
    }
    fn flush(&self) -> Result<()> {
        Client::connect(&self.url, NoTls)
            .map_err(|e| format!("{e:?}"))?
            .batch_execute("CHECKPOINT")
            .map_err(|e| format!("{e:?}"))
    }
    fn server_usage(&self) -> Result<Option<Value>> {
        self.container.stats().map(Some)
    }
    fn disk_bytes(&self) -> Result<u64> {
        self.container.disk_bytes()
    }
}
impl PostgresSession {
    fn statement(&mut self, sql: &'static str) -> Result<postgres::Statement> {
        if let Some(statement) = self.statements.get(sql) {
            return Ok(statement.clone());
        }
        let statement = self.client.prepare(sql).map_err(|e| e.to_string())?;
        self.statements.insert(sql, statement.clone());
        Ok(statement)
    }
    fn execute(&mut self, sql: &'static str, params: &[&(dyn postgres::types::ToSql + Sync)]) -> Result<usize> {
        let statement = self.statement(sql)?;
        self.client
            .execute(&statement, params)
            .map(|n| n as usize)
            .map_err(|e| e.to_string())
    }
    fn query(
        &mut self,
        sql: &'static str,
        params: &[&(dyn postgres::types::ToSql + Sync)],
    ) -> Result<Vec<postgres::Row>> {
        let statement = self.statement(sql)?;
        self.client.query(&statement, params).map_err(|e| e.to_string())
    }
    fn graph_row(&mut self, row: &postgres::Row, out: &mut GraphOutput<'_>) -> Result<()> {
        let version: Option<i64> = row.get(1);
        if let Some(version) = version {
            self.edges.clear();
            let targets: Vec<Vec<u8>> = row.get(2);
            let slots: Vec<i32> = row.get(3);
            for (target, slot) in targets.iter().zip(slots) {
                self.edges.push(GraphEdge {
                    slot: slot as u32,
                    target: Key::from_slice(target).map_err(|e| e.to_string())?,
                });
            }
            out.push(Some(VertexRef {
                version: version as u64,
                edges: &self.edges,
            }))
        } else {
            out.push(None)
        }
    }
}
fn key_bytes(keys: &[Key]) -> Vec<&[u8], System> {
    let mut bytes = Vec::with_capacity_in(keys.len(), System);
    bytes.extend(keys.iter().map(|key| key.as_bytes().as_slice()));
    bytes
}
impl TransactionSession for PostgresSession {
    fn begin(&mut self) -> Result<()> {
        self.client.batch_execute("BEGIN").map_err(|e| e.to_string())?;
        self.transaction = true;
        Ok(())
    }
    fn commit(&mut self) -> Result<()> {
        let result = self.client.batch_execute("COMMIT").map_err(|e| e.to_string());
        self.transaction = false;
        result
    }
    fn rollback(&mut self) -> Result<()> {
        let result = self.client.batch_execute("ROLLBACK").map_err(|e| e.to_string());
        self.transaction = false;
        result
    }
}
impl BackendSession for PostgresSession {
    fn documents(&mut self) -> Option<&mut dyn DocumentSession> {
        (self.data_model == DataModel::Documents).then_some(self)
    }
    fn graph(&mut self) -> Option<&mut dyn GraphSession> {
        (self.data_model == DataModel::Graph).then_some(self)
    }
    fn insert(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        let ids = key_bytes(keys);
        let mut wire_values = Vec::with_capacity_in(keys.len(), System);
        for i in 0..keys.len() {
            wire_values.push(values.get(i).ok_or("Missing value")?);
        }
        self.execute(
            "INSERT INTO records SELECT * FROM unnest($1::bytea[],$2::bytea[]) ON CONFLICT DO NOTHING",
            &[&ids.as_slice(), &wire_values.as_slice()],
        )
    }
    fn read(&mut self, keys: &[Key], out: &mut RecordOutput<'_>) -> Result<usize> {
        out.clear();
        let ids = key_bytes(keys);
        let rows=self.query("SELECT r.value FROM unnest($1::bytea[]) WITH ORDINALITY q(id,n) LEFT JOIN records r ON r.id=q.id ORDER BY q.n",&[&ids.as_slice()])?;
        let mut found = 0;
        for row in rows {
            let value: Option<&[u8]> = row.get(0);
            found += usize::from(value.is_some());
            out.push(value)?;
        }
        Ok(found)
    }
    fn update(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        let ids = key_bytes(keys);
        let mut wire_values = Vec::with_capacity_in(keys.len(), System);
        for i in 0..keys.len() {
            wire_values.push(values.get(i).ok_or("Missing value")?);
        }
        self.execute(
            "UPDATE records r SET value=q.value FROM unnest($1::bytea[],$2::bytea[]) q(id,value) WHERE r.id=q.id",
            &[&ids.as_slice(), &wire_values.as_slice()],
        )
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        let ids = key_bytes(keys);
        self.execute(if self.data_model==DataModel::Graph {"WITH incoming AS (DELETE FROM edges WHERE target=ANY($1::bytea[])) DELETE FROM records WHERE id=ANY($1::bytea[])"}else{"DELETE FROM records WHERE id=ANY($1::bytea[])"},&[&ids.as_slice()])
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
        let rows = self.query(
            "SELECT id,value FROM records WHERE id >= $1 ORDER BY id LIMIT $2",
            &[&start.as_bytes().as_slice(), &(limit as i64)],
        )?;
        for row in rows {
            keys.push(Key::from_slice(row.get(0)).map_err(|e| e.to_string())?)?;
            out.push(Some(row.get(1)))?;
        }
        Ok(keys.len())
    }
}
impl DocumentSession for PostgresSession {
    fn insert(&mut self, keys: &[Key], values: &DocumentInput<'_>) -> Result<usize> {
        let ids = key_bytes(keys);
        let mut scores = Vec::with_capacity_in(keys.len(), System);
        let mut payloads = Vec::with_capacity_in(keys.len(), System);
        for i in 0..keys.len() {
            let value = values.get(i).ok_or("Missing document")?;
            scores.push(value.score as i64);
            payloads.push(value.payload);
        }
        self.execute("INSERT INTO records SELECT id,jsonb_build_object('score',score,'payload',payload) FROM unnest($1::bytea[],$2::bigint[],$3::text[]) q(id,score,payload) ON CONFLICT DO NOTHING",&[&ids.as_slice(),&scores.as_slice(),&payloads.as_slice()])
    }
    fn read(&mut self, keys: &[Key], out: &mut DocumentOutput<'_>) -> Result<usize> {
        out.clear();
        let ids = key_bytes(keys);
        let rows=self.query("SELECT (r.value->>'score')::bigint,r.value->>'payload' FROM unnest($1::bytea[]) WITH ORDINALITY q(id,n) LEFT JOIN records r ON r.id=q.id ORDER BY q.n",&[&ids.as_slice()])?;
        let mut found = 0;
        for row in rows {
            let score: Option<i64> = row.get(0);
            if let Some(score) = score {
                out.push(Some(DocumentRef {
                    score: score as u64,
                    payload: row.get(1),
                }))?;
                found += 1;
            } else {
                out.push(None)?;
            }
        }
        Ok(found)
    }
    fn update(&mut self, keys: &[Key], patches: &[DocumentPatch]) -> Result<usize> {
        let ids = key_bytes(keys);
        let mut scores = Vec::with_capacity_in(patches.len(), System);
        scores.extend(patches.iter().map(|p| p.score as i64));
        self.execute("UPDATE records r SET value=jsonb_set(r.value,'{score}',to_jsonb(q.score)) FROM unnest($1::bytea[],$2::bigint[]) q(id,score) WHERE r.id=q.id",&[&ids.as_slice(),&scores.as_slice()])
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
        let rows = self.query(
            "SELECT id,(value->>'score')::bigint,value->>'payload' FROM records WHERE id >= $1 ORDER BY id LIMIT $2",
            &[&start.as_bytes().as_slice(), &(limit as i64)],
        )?;
        for row in rows {
            keys.push(Key::from_slice(row.get(0)).map_err(|e| e.to_string())?)?;
            out.push(Some(DocumentRef {
                score: row.get::<_, i64>(1) as u64,
                payload: row.get(2),
            }))?;
        }
        Ok(keys.len())
    }
}
impl GraphSession for PostgresSession {
    fn insert(&mut self, keys: &[Key], values: &GraphInput<'_>) -> Result<usize> {
        let ids = key_bytes(keys);
        let mut versions = Vec::with_capacity_in(keys.len(), System);
        let mut sources = Vec::new_in(System);
        let mut slots = Vec::new_in(System);
        let mut targets = Vec::new_in(System);
        for (i, key) in keys.iter().enumerate() {
            let value = values.get(i).ok_or("Missing vertex")?;
            versions.push(value.version as i64);
            for edge in value.edges {
                sources.push(key.as_bytes().as_slice());
                slots.push(edge.slot as i32);
                targets.push(edge.target.as_bytes().as_slice());
            }
        }
        let rows=self.query("WITH vertices AS (INSERT INTO records SELECT * FROM unnest($1::bytea[],$2::bigint[]) ON CONFLICT DO NOTHING RETURNING id), links AS (INSERT INTO edges SELECT q.source,q.slot,q.target FROM unnest($3::bytea[],$4::int[],$5::bytea[]) q(source,slot,target) JOIN vertices v ON v.id=q.source) SELECT count(*) FROM vertices",&[&ids.as_slice(),&versions.as_slice(),&sources.as_slice(),&slots.as_slice(),&targets.as_slice()])?;
        Ok(rows[0].get::<_, i64>(0) as usize)
    }
    fn read(&mut self, keys: &[Key], out: &mut GraphOutput<'_>) -> Result<usize> {
        out.clear();
        let ids = key_bytes(keys);
        let rows=self.query("SELECT r.id,r.version,ARRAY(SELECT target FROM edges WHERE source=r.id ORDER BY slot),ARRAY(SELECT slot FROM edges WHERE source=r.id ORDER BY slot) FROM unnest($1::bytea[]) WITH ORDINALITY q(id,n) LEFT JOIN records r ON r.id=q.id ORDER BY q.n",&[&ids.as_slice()])?;
        let mut found = 0;
        for row in rows {
            found += usize::from(row.get::<_, Option<i64>>(1).is_some());
            self.graph_row(&row, out)?;
        }
        Ok(found)
    }
    fn update(&mut self, keys: &[Key], patches: &[GraphPatch]) -> Result<usize> {
        let ids = key_bytes(keys);
        let mut versions = Vec::with_capacity_in(patches.len(), System);
        versions.extend(patches.iter().map(|p| p.version as i64));
        let mut targets = Vec::with_capacity_in(patches.len(), System);
        targets.extend(
            patches
                .iter()
                .map(|p| p.neighbor.as_ref().map(|key| key.as_bytes().as_slice())),
        );
        let rows=self.query("WITH input AS (SELECT * FROM unnest($1::bytea[],$2::bigint[],$3::bytea[]) q(id,version,target)), vertices AS (UPDATE records r SET version=q.version FROM input q WHERE r.id=q.id RETURNING r.id), removed AS (DELETE FROM edges e USING input q,vertices v WHERE e.source=q.id AND v.id=q.id AND e.slot=0 AND q.target IS NULL), links AS (INSERT INTO edges SELECT q.id,0,q.target FROM input q JOIN vertices v ON v.id=q.id WHERE q.target IS NOT NULL ON CONFLICT(source,slot) DO UPDATE SET target=excluded.target) SELECT count(*) FROM vertices",&[&ids.as_slice(),&versions.as_slice(),&targets.as_slice()])?;
        Ok(rows[0].get::<_, i64>(0) as usize)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        BackendSession::delete(self, keys)
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
        let rows=self.query("SELECT id,version,ARRAY(SELECT target FROM edges WHERE source=r.id ORDER BY slot),ARRAY(SELECT slot FROM edges WHERE source=r.id ORDER BY slot) FROM records r WHERE id >= $1 ORDER BY id LIMIT $2",&[&start.as_bytes().as_slice(),&(limit as i64)])?;
        for row in rows {
            keys.push(Key::from_slice(row.get(0)).map_err(|e| e.to_string())?)?;
            self.graph_row(&row, out)?;
        }
        Ok(keys.len())
    }
    fn expand_neighbors(&mut self, start: Key, limit: usize, keys: &mut KeysOutput<'_>) -> Result<usize> {
        keys.clear();
        let rows=self.query("SELECT DISTINCT b.target FROM edges a JOIN edges b ON a.target=b.source WHERE a.source=$1 AND b.target<>$1 ORDER BY b.target LIMIT $2",&[&start.as_bytes().as_slice(),&(limit as i64)])?;
        for row in rows {
            keys.push(Key::from_slice(row.get(0)).map_err(|e| e.to_string())?)?;
        }
        Ok(keys.len())
    }
}
fn main() {
    let cli: Cli = crudeval::parse_cli();
    if let Err(error) = run(cli.common, "postgres:18.6", &[], open) {
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
        args.durability = Durability::None;
        let backend = open(&args, path.path()).unwrap();
        match data_model {
            DataModel::Documents => crudeval::assert_document_contract!(backend.as_ref()),
            DataModel::Graph => crudeval::assert_graph_contract!(backend.as_ref()),
            _ => unreachable!(),
        };
    }
}

//! Embedded Turso benchmark for UUID-keyed values and native JSONB documents.
//!
//! Run `cargo run --release --no-default-features --features turso-backend
//! --bin crud-eval-turso -- --records 100K --threads 4`.

use std::{
    alloc::System,
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

use clap::Parser;
use serde_json::json;
use tokio::runtime::Runtime;
use turso::{Connection, Database, Statement};

use crudeval::{
    backend::{
        directory_bytes, Backend, BackendCapabilities, BackendSession, DataModel, DocumentInput, DocumentOutput,
        DocumentPatch, DocumentRef, DocumentSession, Durability, Key, KeyWidth, KeysOutput, RecordInput, RecordOutput,
        Result, TransactionSession,
    },
    run, CommonArgs,
};

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
}

struct TursoBackend {
    database: Database,
    runtime: Runtime,
    path: PathBuf,
    durability: Durability,
    key_width: KeyWidth,
    documents: bool,
}

struct TursoSession<'a> {
    runtime: &'a Runtime,
    connection: Connection,
    insert: Statement,
    read: Statement,
    update: Statement,
    delete: Statement,
    range: Statement,
    key_width: KeyWidth,
    documents: bool,
}

impl TursoBackend {
    fn open(path: &Path, durability: Durability, key_width: KeyWidth, documents: bool) -> Result<Self> {
        if durability == Durability::Buffered {
            return Err("Turso supports none (OFF) and flushed (FULL) durability, not buffered".into());
        }
        std::fs::create_dir_all(path).map_err(|e| e.to_string())?;
        let path = path.join("turso.db");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let database = runtime
            .block_on(turso::Builder::new_local(&path.to_string_lossy()).build())
            .map_err(|e| e.to_string())?;
        let connection = database.connect().map_err(|e| e.to_string())?;
        runtime
            .block_on(connection.execute(
                "CREATE TABLE IF NOT EXISTS records (key BLOB PRIMARY KEY NOT NULL, value BLOB NOT NULL)",
                (),
            ))
            .map_err(|e| e.to_string())?;
        Ok(Self {
            database,
            runtime,
            path,
            durability,
            key_width,
            documents,
        })
    }
}

impl Backend for TursoBackend {
    fn metadata(&self) -> BTreeMap<String, serde_json::Value> {
        let mut metadata = BTreeMap::new();
        metadata.extend([
            ("backend".into(), json!("turso")),
            ("library_version".into(), json!("0.8.1")),
            ("durability".into(), json!(self.durability)),
            (
                "synchronous".into(),
                json!(if self.durability == Durability::None {
                    "OFF"
                } else {
                    "FULL"
                }),
            ),
            (
                "document_storage".into(),
                json!(if self.documents { "JSONB" } else { "none" }),
            ),
            ("table_layout".into(), json!("rowid_with_uuid_index")),
        ]);
        metadata
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            data_models: &[DataModel::KeyValue, DataModel::Documents],
            ordered_ranges: true,
            transactions: true,
            ..Default::default()
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_, System>> {
        let connection = self.database.connect().map_err(|e| e.to_string())?;
        connection
            .busy_timeout(Duration::from_secs(60))
            .map_err(|e| e.to_string())?;
        self.runtime.block_on(async {
            connection.execute(if self.durability == Durability::None { "PRAGMA synchronous=OFF" } else { "PRAGMA synchronous=FULL" }, ()).await.map_err(|e| e.to_string())?;
            let (insert, read, update, range) = if self.documents {
                (
                    "INSERT INTO records (key,value) VALUES (?1,jsonb_object('_id',?2,'score',?3,'payload',?4))",
                    "SELECT json_extract(value,'$.score'),json_extract(value,'$.payload') FROM records WHERE key=?1",
                    "UPDATE records SET value=jsonb_set(value,'$.score',?2) WHERE key=?1",
                    "SELECT key,json_extract(value,'$.score'),json_extract(value,'$.payload') FROM records WHERE key>=?1 ORDER BY key LIMIT ?2",
                )
            } else {
                (
                    "INSERT INTO records (key,value) VALUES (?1,?2)",
                    "SELECT value FROM records WHERE key=?1",
                    "UPDATE records SET value=?2 WHERE key=?1",
                    "SELECT key,value FROM records WHERE key>=?1 ORDER BY key LIMIT ?2",
                )
            };
            Ok(Box::new_in(TursoSession {
                runtime: &self.runtime,
                insert: connection.prepare(insert).await.map_err(|e| e.to_string())?,
                read: connection.prepare(read).await.map_err(|e| e.to_string())?,
                update: connection.prepare(update).await.map_err(|e| e.to_string())?,
                delete: connection.prepare("DELETE FROM records WHERE key=?1").await.map_err(|e| e.to_string())?,
                range: connection.prepare(range).await.map_err(|e| e.to_string())?,
                connection,
                key_width: self.key_width,
                documents: self.documents,
            }, System) as Box<dyn BackendSession, System>)
        })
    }
    fn flush(&self) -> Result<()> {
        let connection = self.database.connect().map_err(|e| e.to_string())?;
        self.runtime.block_on(async {
            let mut rows = connection
                .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
                .await
                .map_err(|e| e.to_string())?;
            while let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
                if row.get::<i64>(0).map_err(|e| e.to_string())? != 0 {
                    return Err("Turso checkpoint blocked by an active transaction".into());
                }
            }
            Ok(())
        })
    }
    fn disk_bytes(&self) -> Result<u64> {
        directory_bytes(self.path.parent().unwrap())
    }
}

impl TransactionSession for TursoSession<'_> {
    fn begin(&mut self) -> Result<()> {
        self.runtime
            .block_on(self.connection.execute("BEGIN IMMEDIATE", ()))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    fn commit(&mut self) -> Result<()> {
        self.runtime
            .block_on(self.connection.execute("COMMIT", ()))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    fn rollback(&mut self) -> Result<()> {
        self.runtime
            .block_on(self.connection.execute("ROLLBACK", ()))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

impl BackendSession for TursoSession<'_> {
    fn documents(&mut self) -> Option<&mut dyn DocumentSession> {
        self.documents.then_some(self)
    }
    fn insert(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        self.runtime.block_on(async {
            let local = keys.len() > 1 && self.connection.is_autocommit().map_err(|e| e.to_string())?;
            if local {
                self.connection
                    .execute("BEGIN IMMEDIATE", ())
                    .await
                    .map_err(|e| e.to_string())?;
            }
            let result: Result<usize> = async {
                let mut count = 0;
                for (index, key) in keys.iter().enumerate() {
                    count += self
                        .insert
                        .execute((
                            self.key_width.bytes(key),
                            values.get(index).ok_or("missing input value")?,
                        ))
                        .await
                        .map_err(|e| e.to_string())? as usize;
                }
                Ok(count)
            }
            .await;
            finish_batch(&self.connection, local, result).await
        })
    }
    fn update(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        self.runtime.block_on(async {
            let local = keys.len() > 1 && self.connection.is_autocommit().map_err(|e| e.to_string())?;
            if local {
                self.connection
                    .execute("BEGIN IMMEDIATE", ())
                    .await
                    .map_err(|e| e.to_string())?;
            }
            let result: Result<usize> = async {
                let mut count = 0;
                for (index, key) in keys.iter().enumerate() {
                    count += self
                        .update
                        .execute((
                            self.key_width.bytes(key),
                            values.get(index).ok_or("missing input value")?,
                        ))
                        .await
                        .map_err(|e| e.to_string())? as usize;
                }
                Ok(count)
            }
            .await;
            finish_batch(&self.connection, local, result).await
        })
    }
    fn read(&mut self, keys: &[Key], output: &mut RecordOutput<'_>) -> Result<usize> {
        output.clear();
        self.runtime.block_on(async {
            let mut count = 0;
            for key in keys {
                let mut rows = self
                    .read
                    .query([self.key_width.bytes(key)])
                    .await
                    .map_err(|e| e.to_string())?;
                if let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
                    let value = row.get::<Vec<u8>>(0).map_err(|e| e.to_string())?;
                    output.push(Some(&value))?;
                    count += 1;
                } else {
                    output.push(None)?;
                }
                while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
            }
            Ok(count)
        })
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        self.runtime.block_on(async {
            let local = keys.len() > 1 && self.connection.is_autocommit().map_err(|e| e.to_string())?;
            if local {
                self.connection
                    .execute("BEGIN IMMEDIATE", ())
                    .await
                    .map_err(|e| e.to_string())?;
            }
            let result: Result<usize> = async {
                let mut count = 0;
                for key in keys {
                    count += self
                        .delete
                        .execute([self.key_width.bytes(key)])
                        .await
                        .map_err(|e| e.to_string())? as usize;
                }
                Ok(count)
            }
            .await;
            finish_batch(&self.connection, local, result).await
        })
    }
    fn range_read(
        &mut self,
        start: Key,
        limit: usize,
        keys: &mut KeysOutput<'_>,
        output: &mut RecordOutput<'_>,
    ) -> Result<usize> {
        keys.clear();
        output.clear();
        self.runtime.block_on(async {
            let mut rows = self
                .range
                .query((
                    self.key_width.bytes(&start),
                    i64::try_from(limit).map_err(|e| e.to_string())?,
                ))
                .await
                .map_err(|e| e.to_string())?;
            while let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
                let key = row.get::<Vec<u8>>(0).map_err(|e| e.to_string())?;
                let value = row.get::<Vec<u8>>(1).map_err(|e| e.to_string())?;
                keys.push(self.key_width.from_bytes(&key)?)?;
                output.push(Some(&value))?;
            }
            Ok(keys.len())
        })
    }
}

impl DocumentSession for TursoSession<'_> {
    fn insert(&mut self, keys: &[Key], values: &DocumentInput<'_>) -> Result<usize> {
        self.runtime.block_on(async {
            let local = keys.len() > 1 && self.connection.is_autocommit().map_err(|e| e.to_string())?;
            if local {
                self.connection
                    .execute("BEGIN IMMEDIATE", ())
                    .await
                    .map_err(|e| e.to_string())?;
            }
            let result: Result<usize> = async {
                let mut count = 0;
                let mut uuid = [0; 36];
                for (index, key) in keys.iter().enumerate() {
                    let value = values.get(index).ok_or("missing input document")?;
                    count += self
                        .insert
                        .execute((
                            self.key_width.bytes(key),
                            key.hyphenated().encode_lower(&mut uuid) as &str,
                            i64::try_from(value.score).map_err(|e| e.to_string())?,
                            value.payload,
                        ))
                        .await
                        .map_err(|e| e.to_string())? as usize;
                }
                Ok(count)
            }
            .await;
            finish_batch(&self.connection, local, result).await
        })
    }
    fn read(&mut self, keys: &[Key], output: &mut DocumentOutput<'_>) -> Result<usize> {
        output.clear();
        self.runtime.block_on(async {
            let mut count = 0;
            for key in keys {
                let mut rows = self
                    .read
                    .query([self.key_width.bytes(key)])
                    .await
                    .map_err(|e| e.to_string())?;
                if let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
                    let score =
                        u64::try_from(row.get::<i64>(0).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
                    let payload = row.get::<String>(1).map_err(|e| e.to_string())?;
                    output.push(Some(DocumentRef {
                        score,
                        payload: &payload,
                    }))?;
                    count += 1;
                } else {
                    output.push(None)?;
                }
                while rows.next().await.map_err(|e| e.to_string())?.is_some() {}
            }
            Ok(count)
        })
    }
    fn update(&mut self, keys: &[Key], patches: &[DocumentPatch]) -> Result<usize> {
        if keys.len() != patches.len() {
            return Err("document patch count mismatch".into());
        }
        self.runtime.block_on(async {
            let local = keys.len() > 1 && self.connection.is_autocommit().map_err(|e| e.to_string())?;
            if local {
                self.connection
                    .execute("BEGIN IMMEDIATE", ())
                    .await
                    .map_err(|e| e.to_string())?;
            }
            let result: Result<usize> = async {
                let mut count = 0;
                for (key, patch) in keys.iter().zip(patches) {
                    count += self
                        .update
                        .execute((
                            self.key_width.bytes(key),
                            i64::try_from(patch.score).map_err(|e| e.to_string())?,
                        ))
                        .await
                        .map_err(|e| e.to_string())? as usize;
                }
                Ok(count)
            }
            .await;
            finish_batch(&self.connection, local, result).await
        })
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        BackendSession::delete(self, keys)
    }
    fn range_read(
        &mut self,
        start: Key,
        limit: usize,
        keys: &mut KeysOutput<'_>,
        output: &mut DocumentOutput<'_>,
    ) -> Result<usize> {
        keys.clear();
        output.clear();
        self.runtime.block_on(async {
            let mut rows = self
                .range
                .query((
                    self.key_width.bytes(&start),
                    i64::try_from(limit).map_err(|e| e.to_string())?,
                ))
                .await
                .map_err(|e| e.to_string())?;
            while let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
                let key = row.get::<Vec<u8>>(0).map_err(|e| e.to_string())?;
                let score = u64::try_from(row.get::<i64>(1).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
                let payload = row.get::<String>(2).map_err(|e| e.to_string())?;
                keys.push(self.key_width.from_bytes(&key)?)?;
                output.push(Some(DocumentRef {
                    score,
                    payload: &payload,
                }))?;
            }
            Ok(keys.len())
        })
    }
}

async fn finish_batch(connection: &Connection, local: bool, result: Result<usize>) -> Result<usize> {
    if !local {
        return result;
    }
    let result = match result {
        Ok(count) => connection
            .execute("COMMIT", ())
            .await
            .map(|_| count)
            .map_err(|e| e.to_string()),
        Err(error) => Err(error),
    };
    if result.is_err() && !connection.is_autocommit().map_err(|e| e.to_string())? {
        connection
            .execute("ROLLBACK", ())
            .await
            .map_err(|e| format!("{result:?}; rollback: {e}"))?;
    }
    result
}

fn main() -> Result<()> {
    let cli: Cli = crudeval::parse_cli();
    run(cli.common, (), &[], |args, path| {
        if args.data_model == DataModel::Graph {
            return Err("Turso graph workloads are unsupported".into());
        }
        Ok(Box::new_in(
            TursoBackend::open(
                path,
                args.durability,
                args.key_width()?,
                args.data_model == DataModel::Documents,
            )?,
            System,
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_contract() {
        let directory = tempfile::tempdir().unwrap();
        let db = TursoBackend::open(directory.path(), Durability::None, KeyWidth::U64, false).unwrap();
        crudeval::assert_backend_contract!(&db);
    }
    #[test]
    fn jsonb_patch_is_exact_and_rollback_preserves_payload() {
        let directory = tempfile::tempdir().unwrap();
        let db = TursoBackend::open(directory.path(), Durability::Flushed, KeyWidth::U64, true).unwrap();
        let mut session = db.session().unwrap();
        let session = session.documents().unwrap();
        let key = Key::from_u128(256);
        let missing = Key::from_u128(257);
        let mut input = crudeval::backend::DocumentBatch::new(1, 16);
        input
            .as_output()
            .push(Some(DocumentRef {
                score: 9_007_199_254_740_993,
                payload: "abcd",
            }))
            .unwrap();
        session.insert(&[key], &input.as_input()).unwrap();
        assert!(session.update(&[key, missing], &[DocumentPatch { score: 8 }]).is_err());
        session.begin().unwrap();
        assert_eq!(
            session
                .update(&[key, missing], &[DocumentPatch { score: 7 }; 2])
                .unwrap(),
            1
        );
        session.rollback().unwrap();
        let mut output = crudeval::backend::DocumentBatch::new(2, 16);
        assert_eq!(session.read(&[key, missing], &mut output.as_output()).unwrap(), 1);
        let values = output.as_input();
        let value = values.get(0).unwrap();
        assert_eq!((value.score, value.payload), (9_007_199_254_740_993, "abcd"));
        assert!(values.get(1).is_none());
        session.update(&[key], &[DocumentPatch { score: 7 }]).unwrap();
        session.read(&[key], &mut output.as_output()).unwrap();
        let values = output.as_input();
        let value = values.get(0).unwrap();
        assert_eq!((value.score, value.payload), (7, "abcd"));
        let fresh = Key::from_u128(999);
        let mut duplicate = crudeval::backend::DocumentBatch::new(2, 8);
        let mut rows = duplicate.as_output();
        for _ in 0..2 {
            rows.push(Some(DocumentRef {
                score: 3,
                payload: "same",
            }))
            .unwrap();
        }
        assert!(session.insert(&[fresh, key], &duplicate.as_input()).is_err());
        assert_eq!(session.read(&[fresh], &mut output.as_output()).unwrap(), 0);
        assert!(output.as_input().get(0).is_none());
    }
}

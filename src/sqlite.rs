//! SQLite benchmark for key-value records and JSONB documents.
//!
//! Requires a C compiler to build the bundled SQLite engine.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features sqlite-backend \
//!     --bin crud-eval-sqlite -- --records 100K --threads 4
//! ```

use std::{
    alloc::System,
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use clap::Parser;
use rusqlite::{params, Connection};
use serde_json::json;

use crudeval::{
    backend::{
        directory_bytes, Backend, BackendCapabilities, BackendSession, DataModel, DocumentInput, DocumentOutput,
        DocumentPatch, DocumentRef, DocumentSession, Durability, Key, KeysOutput, RecordInput, RecordOutput, Result,
        TransactionSession,
    },
    run, Bytes, CommonArgs,
};

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
    /// Page cache per session, like 64MB.
    #[arg(long, default_value = "64MB", value_parser = crudeval::workload::parse_size)]
    cache_size: Bytes,
}

struct SqliteBackend {
    path: PathBuf,
    durability: Durability,
    docs: bool,
    cache_size: u64,
    keeper: Mutex<Connection>,
}
struct SqliteSession {
    connection: Connection,
    docs: bool,
}
impl SqliteBackend {
    fn connect(&self) -> Result<Connection> {
        let connection = Connection::open(&self.path).map_err(|e| e.to_string())?;
        connection
            .busy_timeout(Duration::from_secs(60))
            .map_err(|e| e.to_string())?;
        connection
            .pragma_update(
                None,
                "synchronous",
                match self.durability {
                    Durability::None => "OFF",
                    Durability::Buffered => "NORMAL",
                    Durability::Flushed => "FULL",
                },
            )
            .map_err(|e| e.to_string())?;
        connection
            .pragma_update(None, "cache_size", -((self.cache_size >> 10) as i64))
            .map_err(|e| e.to_string())?;
        Ok(connection)
    }
    fn open(path: &Path, durability: Durability, docs: bool, cache_size: u64) -> Result<Self> {
        std::fs::create_dir_all(path).map_err(|e| e.to_string())?;
        let path = path.join("sqlite.db");
        let keeper = Connection::open(&path).map_err(|e| e.to_string())?;
        keeper
            .busy_timeout(Duration::from_secs(60))
            .map_err(|e| e.to_string())?;
        keeper.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS records (key BLOB PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID;").map_err(|e| e.to_string())?;
        Ok(Self {
            path,
            durability,
            docs,
            cache_size,
            keeper: Mutex::new(keeper),
        })
    }
}
impl Backend for SqliteBackend {
    fn metadata(&self) -> BTreeMap<String, serde_json::Value> {
        let mut metadata = BTreeMap::new();
        metadata.extend([
            ("backend".into(), json!("sqlite")),
            ("library_version".into(), json!(rusqlite::version())),
            ("durability".into(), json!(self.durability)),
            ("journal_mode".into(), json!("WAL")),
            (
                "synchronous".into(),
                json!(match self.durability {
                    Durability::None => "OFF",
                    Durability::Buffered => "NORMAL",
                    Durability::Flushed => "FULL",
                }),
            ),
            (
                "document_storage".into(),
                json!(if self.docs { "JSONB" } else { "none" }),
            ),
            ("cache_size_per_session".into(), json!(self.cache_size)),
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
        Ok(Box::new_in(
            SqliteSession {
                connection: self.connect()?,
                docs: self.docs,
            },
            System,
        ))
    }
    fn flush(&self) -> Result<()> {
        let connection = self.keeper.lock().map_err(|e| e.to_string())?;
        let (busy, _, _): (i64, i64, i64) = connection
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .map_err(|e| e.to_string())?;
        if busy != 0 {
            return Err("SQLite checkpoint blocked by an active transaction".into());
        }
        Ok(())
    }
    fn disk_bytes(&self) -> Result<u64> {
        directory_bytes(self.path.parent().unwrap())
    }
}
impl BackendSession for SqliteSession {
    fn documents(&mut self) -> Option<&mut dyn DocumentSession> {
        self.docs.then_some(self)
    }
    fn insert(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        let transaction = (keys.len() > 1 && self.connection.is_autocommit())
            .then(|| self.connection.unchecked_transaction())
            .transpose()
            .map_err(|e| e.to_string())?;
        let mut statement = self
            .connection
            .prepare_cached("INSERT INTO records (key,value) VALUES (?1,?2)")
            .map_err(|e| e.to_string())?;
        for (i, key) in keys.iter().enumerate() {
            statement
                .execute(params![key.as_bytes().as_slice(), values.get(i)])
                .map_err(|e| e.to_string())?;
        }
        drop(statement);
        if let Some(transaction) = transaction {
            transaction.commit().map_err(|e| e.to_string())?;
        }
        Ok(keys.len())
    }
    fn read(&mut self, keys: &[Key], output: &mut RecordOutput<'_>) -> Result<usize> {
        output.clear();
        let mut statement = self
            .connection
            .prepare_cached("SELECT value FROM records WHERE key=?1")
            .map_err(|e| e.to_string())?;
        let mut found = 0;
        for key in keys {
            let mut rows = statement
                .query([key.as_bytes().as_slice()])
                .map_err(|e| e.to_string())?;
            if let Some(row) = rows.next().map_err(|e| e.to_string())? {
                let value = row
                    .get_ref(0)
                    .map_err(|e| e.to_string())?
                    .as_blob()
                    .map_err(|e| e.to_string())?;
                output.push(Some(value))?;
                found += 1;
            } else {
                output.push(None)?;
            }
        }
        Ok(found)
    }
    fn update(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        let transaction = (keys.len() > 1 && self.connection.is_autocommit())
            .then(|| self.connection.unchecked_transaction())
            .transpose()
            .map_err(|e| e.to_string())?;
        let sql = "UPDATE records SET value=?2 WHERE key=?1";
        let mut statement = self.connection.prepare_cached(sql).map_err(|e| e.to_string())?;
        let mut count = 0;
        for (i, key) in keys.iter().enumerate() {
            count += statement
                .execute(params![key.as_bytes().as_slice(), values.get(i)])
                .map_err(|e| e.to_string())?;
        }
        drop(statement);
        if let Some(transaction) = transaction {
            transaction.commit().map_err(|e| e.to_string())?;
        }
        Ok(count)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        let transaction = (keys.len() > 1 && self.connection.is_autocommit())
            .then(|| self.connection.unchecked_transaction())
            .transpose()
            .map_err(|e| e.to_string())?;
        let mut statement = self
            .connection
            .prepare_cached("DELETE FROM records WHERE key=?1")
            .map_err(|e| e.to_string())?;
        let mut count = 0;
        for key in keys {
            count += statement
                .execute([key.as_bytes().as_slice()])
                .map_err(|e| e.to_string())?;
        }
        drop(statement);
        if let Some(transaction) = transaction {
            transaction.commit().map_err(|e| e.to_string())?;
        }
        Ok(count)
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
        let mut statement = self
            .connection
            .prepare_cached("SELECT key,value FROM records WHERE key>=?1 ORDER BY key LIMIT ?2")
            .map_err(|e| e.to_string())?;
        let mut rows = statement
            .query(params![
                start.as_bytes().as_slice(),
                i64::try_from(limit).unwrap_or(i64::MAX)
            ])
            .map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let key = row
                .get_ref(0)
                .map_err(|e| e.to_string())?
                .as_blob()
                .map_err(|e| e.to_string())?;
            let value = row
                .get_ref(1)
                .map_err(|e| e.to_string())?
                .as_bytes()
                .map_err(|e| e.to_string())?;
            keys.push(Key::from_slice(key).map_err(|e| e.to_string())?)?;
            output.push(Some(value))?;
        }
        Ok(keys.len())
    }
}

impl TransactionSession for SqliteSession {
    fn begin(&mut self) -> Result<()> {
        self.connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| e.to_string())
    }
    fn commit(&mut self) -> Result<()> {
        self.connection.execute_batch("COMMIT").map_err(|e| e.to_string())
    }
    fn rollback(&mut self) -> Result<()> {
        self.connection.execute_batch("ROLLBACK").map_err(|e| e.to_string())
    }
}
impl DocumentSession for SqliteSession {
    fn insert(&mut self, keys: &[Key], values: &DocumentInput<'_>) -> Result<usize> {
        let transaction = (keys.len() > 1 && self.connection.is_autocommit())
            .then(|| self.connection.unchecked_transaction())
            .transpose()
            .map_err(|e| e.to_string())?;
        let mut statement = self
            .connection
            .prepare_cached(
                "INSERT INTO records (key,value) VALUES (?1,jsonb_object('_id',?2,'score',?3,'payload',?4))",
            )
            .map_err(|e| e.to_string())?;
        let mut uuid = [0; 36];
        for (index, key) in keys.iter().enumerate() {
            let value = values.get(index).ok_or("missing input document")?;
            statement
                .execute(params![
                    key.as_bytes().as_slice(),
                    key.hyphenated().encode_lower(&mut uuid) as &str,
                    i64::try_from(value.score).map_err(|e| e.to_string())?,
                    value.payload
                ])
                .map_err(|e| e.to_string())?;
        }
        drop(statement);
        if let Some(transaction) = transaction {
            transaction.commit().map_err(|e| e.to_string())?;
        }
        Ok(keys.len())
    }
    fn read(&mut self, keys: &[Key], output: &mut DocumentOutput<'_>) -> Result<usize> {
        output.clear();
        let mut statement = self
            .connection
            .prepare_cached(
                "SELECT json_extract(value,'$.score'),json_extract(value,'$.payload') FROM records WHERE key=?1",
            )
            .map_err(|e| e.to_string())?;
        let mut count = 0;
        for key in keys {
            let mut rows = statement
                .query([key.as_bytes().as_slice()])
                .map_err(|e| e.to_string())?;
            if let Some(row) = rows.next().map_err(|e| e.to_string())? {
                let score =
                    u64::try_from(row.get::<_, i64>(0).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
                let payload = row
                    .get_ref(1)
                    .map_err(|e| e.to_string())?
                    .as_str()
                    .map_err(|e| e.to_string())?;
                output.push(Some(DocumentRef { score, payload }))?;
                count += 1;
            } else {
                output.push(None)?;
            }
        }
        Ok(count)
    }
    fn update(&mut self, keys: &[Key], patches: &[DocumentPatch]) -> Result<usize> {
        if keys.len() != patches.len() {
            return Err("document patch count mismatch".into());
        }
        let transaction = (keys.len() > 1 && self.connection.is_autocommit())
            .then(|| self.connection.unchecked_transaction())
            .transpose()
            .map_err(|e| e.to_string())?;
        let mut statement = self
            .connection
            .prepare_cached("UPDATE records SET value=jsonb_set(value,'$.score',?2) WHERE key=?1")
            .map_err(|e| e.to_string())?;
        let mut count = 0;
        for (key, patch) in keys.iter().zip(patches) {
            count += statement
                .execute(params![
                    key.as_bytes().as_slice(),
                    i64::try_from(patch.score).map_err(|e| e.to_string())?
                ])
                .map_err(|e| e.to_string())?;
        }
        drop(statement);
        if let Some(transaction) = transaction {
            transaction.commit().map_err(|e| e.to_string())?;
        }
        Ok(count)
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
        let mut statement = self.connection.prepare_cached(
            "SELECT key,json_extract(value,'$.score'),json_extract(value,'$.payload') FROM records WHERE key>=?1 ORDER BY key LIMIT ?2"
        ).map_err(|e| e.to_string())?;
        let mut rows = statement
            .query(params![
                start.as_bytes().as_slice(),
                i64::try_from(limit).map_err(|e| e.to_string())?
            ])
            .map_err(|e| e.to_string())?;
        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let key = row
                .get_ref(0)
                .map_err(|e| e.to_string())?
                .as_blob()
                .map_err(|e| e.to_string())?;
            let score = u64::try_from(row.get::<_, i64>(1).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
            let payload = row
                .get_ref(2)
                .map_err(|e| e.to_string())?
                .as_str()
                .map_err(|e| e.to_string())?;
            keys.push(Key::from_slice(key).map_err(|e| e.to_string())?)?;
            output.push(Some(DocumentRef { score, payload }))?;
        }
        Ok(keys.len())
    }
}

fn main() -> Result<()> {
    let cli: Cli = crudeval::parse_cli();
    let settings = [("Cache size", cli.cache_size.to_string())];
    run(
        cli.common,
        json!({"cache_size": cli.cache_size}),
        &settings,
        move |args, path| {
            if args.data_model == DataModel::Graph {
                return Err("SQLite graph data_model is unsupported".into());
            }
            Ok(Box::new_in(
                SqliteBackend::open(
                    path,
                    args.durability,
                    args.data_model == DataModel::Documents,
                    cli.cache_size.0,
                )?,
                System,
            ))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_contract() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        let db = SqliteBackend::open(path, Durability::None, false, 8).unwrap();
        crudeval::assert_backend_contract!(&db);
    }

    #[test]
    fn document_update_and_rollback() {
        let directory = tempfile::tempdir().unwrap();
        let db = SqliteBackend::open(directory.path(), Durability::Flushed, true, 8).unwrap();
        let mut session = db.session().unwrap();
        let session = session.documents().unwrap();
        let key = Key::from_u128(1);
        let mut values = crudeval::backend::DocumentBatch::new(1, 16);
        values
            .as_output()
            .push(Some(DocumentRef {
                score: 1,
                payload: "keep",
            }))
            .unwrap();
        session.insert(&[key], &values.as_input()).unwrap();
        session.begin().unwrap();
        session.update(&[key], &[DocumentPatch { score: 2 }]).unwrap();
        session.rollback().unwrap();
        let mut output = crudeval::backend::DocumentBatch::new(1, 16);
        session.read(&[key], &mut output.as_output()).unwrap();
        let input = output.as_input();
        let value = input.get(0).unwrap();
        assert_eq!((value.score, value.payload), (1, "keep"));
        session.begin().unwrap();
        session
            .update(
                &[key],
                &[DocumentPatch {
                    score: 9_007_199_254_740_993,
                }],
            )
            .unwrap();
        session.commit().unwrap();
        session.read(&[key], &mut output.as_output()).unwrap();
        let input = output.as_input();
        let value = input.get(0).unwrap();
        assert_eq!((value.score, value.payload), (9_007_199_254_740_993, "keep"));
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

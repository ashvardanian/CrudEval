//! redb key-value benchmark with grouped transactions.
//!
//! The engine is implemented in Rust; no external database installation is needed.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features redb-backend \
//!     --bin crud-eval-redb -- --records 100K --threads 4
//! ```

use std::{
    alloc::System,
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use clap::Parser;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde_json::json;

use crudeval::{
    backend::{
        directory_bytes, Backend, BackendCapabilities, BackendSession, DataModel, Durability, Key, KeyWidth,
        KeysOutput, RecordInput, RecordOutput, Result, TransactionSession,
    },
    run, CommonArgs,
};

const RECORDS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("records");
#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
}
struct RedbBackend {
    db: Database,
    key_width: KeyWidth,
    path: PathBuf,
    durability: Durability,
}
impl RedbBackend {
    fn open(path: &Path, durability: Durability, key_width: KeyWidth) -> Result<Self> {
        std::fs::create_dir_all(path).map_err(|e| e.to_string())?;
        let db = Database::create(path.join("redb.db")).map_err(|e| e.to_string())?;
        let tx = db.begin_write().map_err(|e| e.to_string())?;
        tx.open_table(RECORDS).map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        Ok(Self {
            db,
            key_width,
            path: path.into(),
            durability,
        })
    }
    fn write(&self) -> Result<redb::WriteTransaction> {
        let mut tx = self.db.begin_write().map_err(|e| e.to_string())?;
        tx.set_durability(match self.durability {
            Durability::None => redb::Durability::None,
            Durability::Buffered => redb::Durability::Immediate,
            Durability::Flushed => redb::Durability::Immediate,
        })
        .map_err(|e| e.to_string())?;
        Ok(tx)
    }
}
impl Backend for RedbBackend {
    fn metadata(&self) -> BTreeMap<String, serde_json::Value> {
        let mut metadata = BTreeMap::new();
        metadata.extend([
            ("backend".into(), json!("redb")),
            ("library_version".into(), json!("4.3.0")),
            ("durability".into(), json!(self.durability)),
            (
                "effective_durability".into(),
                json!(if self.durability == Durability::None {
                    "none"
                } else {
                    "immediate"
                }),
            ),
        ]);
        metadata
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            ordered_ranges: true,
            transactions: true,
            ..Default::default()
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_, System>> {
        Ok(Box::new_in(
            RedbSession {
                backend: self,
                transaction: None,
            },
            System,
        ))
    }
    fn flush(&self) -> Result<()> {
        let mut tx = self.db.begin_write().map_err(|e| e.to_string())?;
        tx.set_durability(redb::Durability::Immediate)
            .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())
    }
    fn disk_bytes(&self) -> Result<u64> {
        directory_bytes(&self.path)
    }
}
struct RedbSession<'a> {
    backend: &'a RedbBackend,
    transaction: Option<redb::WriteTransaction>,
}
impl RedbSession<'_> {
    fn write(&mut self, operation: impl FnOnce(&redb::WriteTransaction) -> Result<usize>) -> Result<usize> {
        if let Some(tx) = &self.transaction {
            return operation(tx);
        }
        let tx = self.backend.write()?;
        let count = operation(&tx)?;
        tx.commit().map_err(|e| e.to_string())?;
        Ok(count)
    }
}
fn read_rows(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    key_width: KeyWidth,
    keys: &[Key],
    output: &mut RecordOutput<'_>,
) -> Result<usize> {
    output.clear();
    let mut count = 0;
    for key in keys {
        let value = table.get(key_width.bytes(key)).map_err(|e| e.to_string())?;
        count += usize::from(value.is_some());
        output.push(value.as_ref().map(|v| v.value()))?;
    }
    Ok(count)
}
fn scan_rows(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    key_width: KeyWidth,
    start: Key,
    limit: usize,
    keys: &mut KeysOutput<'_>,
    output: &mut RecordOutput<'_>,
) -> Result<usize> {
    keys.clear();
    output.clear();
    for row in table
        .range(key_width.bytes(&start)..)
        .map_err(|e| e.to_string())?
        .take(limit)
    {
        let (key, value) = row.map_err(|e| e.to_string())?;
        keys.push(key_width.from_bytes(key.value())?)?;
        output.push(Some(value.value()))?;
    }
    Ok(keys.len())
}
impl BackendSession for RedbSession<'_> {
    fn insert(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        let key_width = self.backend.key_width;
        self.write(|tx| {
            let mut table = tx.open_table(RECORDS).map_err(|e| e.to_string())?;
            for (i, key) in keys.iter().enumerate() {
                table
                    .insert(key_width.bytes(key), values.get(i).ok_or("missing input value")?)
                    .map_err(|e| e.to_string())?;
            }
            Ok(keys.len())
        })
    }
    fn read(&mut self, keys: &[Key], output: &mut RecordOutput<'_>) -> Result<usize> {
        if let Some(tx) = &self.transaction {
            return read_rows(
                &tx.open_table(RECORDS).map_err(|e| e.to_string())?,
                self.backend.key_width,
                keys,
                output,
            );
        }
        let tx = self.backend.db.begin_read().map_err(|e| e.to_string())?;
        read_rows(
            &tx.open_table(RECORDS).map_err(|e| e.to_string())?,
            self.backend.key_width,
            keys,
            output,
        )
    }
    fn update(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        let key_width = self.backend.key_width;
        self.write(|tx| {
            let mut table = tx.open_table(RECORDS).map_err(|e| e.to_string())?;
            let mut count = 0;
            for (i, key) in keys.iter().enumerate() {
                let exists = table.get(key_width.bytes(key)).map_err(|e| e.to_string())?.is_some();
                if exists {
                    table
                        .insert(key_width.bytes(key), values.get(i).ok_or("missing input value")?)
                        .map_err(|e| e.to_string())?;
                    count += 1;
                }
            }
            Ok(count)
        })
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        let key_width = self.backend.key_width;
        self.write(|tx| {
            let mut table = tx.open_table(RECORDS).map_err(|e| e.to_string())?;
            let mut count = 0;
            for key in keys {
                count += usize::from(table.remove(key_width.bytes(key)).map_err(|e| e.to_string())?.is_some());
            }
            Ok(count)
        })
    }
    fn range_read(
        &mut self,
        start: Key,
        limit: usize,
        keys: &mut KeysOutput<'_>,
        output: &mut RecordOutput<'_>,
    ) -> Result<usize> {
        if let Some(tx) = &self.transaction {
            return scan_rows(
                &tx.open_table(RECORDS).map_err(|e| e.to_string())?,
                self.backend.key_width,
                start,
                limit,
                keys,
                output,
            );
        }
        let tx = self.backend.db.begin_read().map_err(|e| e.to_string())?;
        scan_rows(
            &tx.open_table(RECORDS).map_err(|e| e.to_string())?,
            self.backend.key_width,
            start,
            limit,
            keys,
            output,
        )
    }
}

impl TransactionSession for RedbSession<'_> {
    fn begin(&mut self) -> Result<()> {
        if self.transaction.is_some() {
            return Err("transaction already active".into());
        }
        self.transaction = Some(self.backend.write()?);
        Ok(())
    }
    fn commit(&mut self) -> Result<()> {
        self.transaction
            .take()
            .ok_or("no active transaction")?
            .commit()
            .map_err(|e| e.to_string())
    }
    fn rollback(&mut self) -> Result<()> {
        self.transaction
            .take()
            .ok_or("no active transaction")?
            .abort()
            .map_err(|e| e.to_string())
    }
}

fn main() -> Result<()> {
    let cli: Cli = crudeval::parse_cli();
    run(cli.common, (), &[], |args, path| {
        if args.data_model != DataModel::KeyValue {
            return Err("redb supports only kv data_model".into());
        }
        Ok(Box::new_in(
            RedbBackend::open(path, args.durability, args.key_width()?)?,
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
        let path = directory.path();
        let db = RedbBackend::open(path, Durability::None, KeyWidth::U64).unwrap();
        crudeval::assert_backend_contract!(&db);
    }
}

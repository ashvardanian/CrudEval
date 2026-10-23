//! fjall key-value benchmark with native write batches.
//!
//! The engine is implemented in Rust; no external database installation is needed.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features fjall-backend \
//!     --bin crud-eval-fjall -- --records 100K --threads 4
//! ```

use std::{
    alloc::System,
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use clap::Parser;
use fjall::{Database, Keyspace, PersistMode};
use serde_json::json;

use crudeval::{
    backend::{
        directory_bytes, Backend, BackendCapabilities, BackendSession, BatchMode, DataModel, Durability, Key,
        KeysOutput, RecordInput, RecordOutput, Result, TransactionSession,
    },
    run, CommonArgs,
};

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
}
struct FjallBackend {
    db: Database,
    records: Keyspace,
    path: PathBuf,
    durability: Durability,
}
impl FjallBackend {
    fn open(path: &Path, durability: Durability) -> Result<Self> {
        let db = Database::builder(path).open().map_err(|e| e.to_string())?;
        let records = db.keyspace("records", Default::default).map_err(|e| e.to_string())?;
        Ok(Self {
            db,
            records,
            path: path.into(),
            durability,
        })
    }
    fn batch(&self) -> fjall::OwnedWriteBatch {
        self.db.batch().durability(match self.durability {
            Durability::None => None,
            Durability::Buffered => Some(PersistMode::Buffer),
            Durability::Flushed => Some(PersistMode::SyncAll),
        })
    }
}
impl Backend for FjallBackend {
    fn metadata(&self) -> BTreeMap<String, serde_json::Value> {
        let mut metadata = BTreeMap::new();
        metadata.extend([
            ("backend".into(), json!("fjall")),
            ("library_version".into(), json!("3.1.10")),
            ("durability".into(), json!(self.durability)),
            ("conditional_writes".into(), json!("disjoint_deletes")),
        ]);
        metadata
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            ordered_ranges: true,
            batch_insert: BatchMode::Native,
            batch_update: BatchMode::Native,
            batch_delete: BatchMode::Native,
            bulk_load: BatchMode::Native,
            ..Default::default()
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_, System>> {
        Ok(Box::new_in(FjallSession(self), System))
    }
    fn flush(&self) -> Result<()> {
        self.db.persist(PersistMode::SyncAll).map_err(|e| e.to_string())
    }
    fn disk_bytes(&self) -> Result<u64> {
        directory_bytes(&self.path)
    }
}
struct FjallSession<'a>(&'a FjallBackend);
impl TransactionSession for FjallSession<'_> {}

impl BackendSession for FjallSession<'_> {
    fn insert(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        let mut batch = self.0.batch();
        for (i, key) in keys.iter().enumerate() {
            batch.insert(
                &self.0.records,
                key.as_bytes().as_slice(),
                values.get(i).ok_or("missing input value")?,
            );
        }
        batch.commit().map_err(|e| e.to_string())?;
        Ok(keys.len())
    }
    fn read(&mut self, keys: &[Key], output: &mut RecordOutput<'_>) -> Result<usize> {
        output.clear();
        let mut count = 0;
        for key in keys {
            let value = self.0.records.get(key.as_bytes()).map_err(|e| e.to_string())?;
            count += usize::from(value.is_some());
            output.push(value.as_deref())?;
        }
        Ok(count)
    }
    fn update(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        let mut batch = self.0.batch();
        let mut count = 0;
        for (i, key) in keys.iter().enumerate() {
            if self.0.records.contains_key(key.as_bytes()).map_err(|e| e.to_string())? {
                batch.insert(
                    &self.0.records,
                    key.as_bytes().as_slice(),
                    values.get(i).ok_or("missing input value")?,
                );
                count += 1;
            }
        }
        batch.commit().map_err(|e| e.to_string())?;
        Ok(count)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        let mut batch = self.0.batch();
        let mut count = 0;
        for key in keys {
            if self.0.records.contains_key(key.as_bytes()).map_err(|e| e.to_string())? {
                batch.remove(&self.0.records, key.as_bytes().as_slice());
                count += 1;
            }
        }
        batch.commit().map_err(|e| e.to_string())?;
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
        for row in self.0.records.range(start.as_bytes().as_slice()..).take(limit) {
            let (key, value) = row.into_inner().map_err(|e| e.to_string())?;
            keys.push(Key::from_slice(&key).map_err(|e| e.to_string())?)?;
            output.push(Some(&value))?;
        }
        Ok(keys.len())
    }
}
fn main() -> Result<()> {
    let cli: Cli = crudeval::parse_cli();
    run(cli.common, (), &[], |args, path| {
        if args.data_model != DataModel::KeyValue {
            return Err("fjall supports only kv data_model".into());
        }
        Ok(Box::new_in(FjallBackend::open(path, args.durability)?, System))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_contract() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        let db = FjallBackend::open(path, Durability::None).unwrap();
        crudeval::assert_backend_contract!(&db);
    }
}

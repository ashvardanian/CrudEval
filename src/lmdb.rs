//! LMDB key-value benchmark with per-worker transactions.
//!
//! Requires a C compiler; the engine is bundled through heed.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features lmdb-backend \
//!     --bin crud-eval-lmdb -- --records 100K --threads 4
//! ```

use std::{
    alloc::System,
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use clap::Parser;
use heed::{types::Bytes, Database, Env, EnvFlags, EnvOpenOptions};
use serde_json::json;

use crudeval::{
    backend::{
        directory_bytes, Backend, BackendCapabilities, BackendSession, DataModel, Durability, Key, KeyWidth,
        KeysOutput, RecordInput, RecordOutput, Result, TransactionSession,
    },
    run, CommonArgs,
};

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
    /// Largest database the memory map can hold, like 1TB.
    #[arg(long, env = "CRUDEVAL_MAP_SIZE", default_value = "1TB", value_parser = crudeval::workload::parse_size)]
    map_size: crudeval::Bytes,
}
struct LmdbBackend {
    env: Env,
    key_width: KeyWidth,
    db: Database<Bytes, Bytes>,
    path: PathBuf,
    durability: Durability,
    map_size: usize,
}
impl LmdbBackend {
    fn open(path: &Path, durability: Durability, key_width: KeyWidth, map_size: usize) -> Result<Self> {
        std::fs::create_dir_all(path).map_err(|e| e.to_string())?;
        let mut options = EnvOpenOptions::new();
        options.map_size(map_size).max_readers(4096);
        let flags = match durability {
            Durability::None => EnvFlags::NO_SYNC,
            Durability::Buffered => EnvFlags::NO_META_SYNC,
            Durability::Flushed => EnvFlags::empty(),
        };
        // Each sweep owns an isolated path and keeps its environment alive for every session.
        let env = unsafe { options.flags(flags).open(path) }.map_err(|e| e.to_string())?;
        let mut transaction = env.write_txn().map_err(|e| e.to_string())?;
        let db = env.create_database(&mut transaction, None).map_err(|e| e.to_string())?;
        transaction.commit().map_err(|e| e.to_string())?;
        Ok(Self {
            env,
            key_width,
            db,
            path: path.into(),
            durability,
            map_size,
        })
    }
}
impl Backend for LmdbBackend {
    fn metadata(&self) -> BTreeMap<String, serde_json::Value> {
        let mut metadata = BTreeMap::new();
        metadata.extend([
            ("backend".into(), json!("lmdb")),
            ("client_version".into(), json!("heed 0.22.1")),
            ("library_version".into(), json!(heed::lmdb_version().string)),
            (
                "sync_mode".into(),
                json!(match self.durability {
                    Durability::None => "NO_SYNC",
                    Durability::Buffered => "NO_META_SYNC",
                    Durability::Flushed => "sync",
                }),
            ),
            ("durability".into(), json!(self.durability)),
            ("map_size".into(), json!(self.map_size)),
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
            LmdbSession {
                backend: self,
                transaction: None,
            },
            System,
        ))
    }
    fn flush(&self) -> Result<()> {
        self.env.force_sync().map_err(|e| e.to_string())
    }
    fn disk_bytes(&self) -> Result<u64> {
        directory_bytes(&self.path)
    }
}
struct LmdbSession<'a> {
    backend: &'a LmdbBackend,
    transaction: Option<heed::RwTxn<'a>>,
}
impl LmdbSession<'_> {
    fn write(&mut self, operation: impl FnOnce(&mut heed::RwTxn<'_>) -> Result<usize>) -> Result<usize> {
        if let Some(transaction) = &mut self.transaction {
            return operation(transaction);
        }
        let mut transaction = self.backend.env.write_txn().map_err(|e| e.to_string())?;
        let count = operation(&mut transaction)?;
        transaction.commit().map_err(|e| e.to_string())?;
        Ok(count)
    }
}
impl BackendSession for LmdbSession<'_> {
    fn insert(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        let db = self.backend.db;
        self.write(|tx| {
            for (i, key) in keys.iter().enumerate() {
                db.put(
                    tx,
                    self.backend.key_width.bytes(key),
                    values.get(i).ok_or("missing input value")?,
                )
                .map_err(|e| e.to_string())?;
            }
            Ok(keys.len())
        })
    }
    fn read(&mut self, keys: &[Key], output: &mut RecordOutput<'_>) -> Result<usize> {
        output.clear();
        let read;
        let tx: &heed::RoTxn = if let Some(tx) = &self.transaction {
            tx
        } else {
            read = self.backend.env.read_txn().map_err(|e| e.to_string())?;
            &read
        };
        let mut found = 0;
        for key in keys {
            let value = self
                .backend
                .db
                .get(tx, self.backend.key_width.bytes(key))
                .map_err(|e| e.to_string())?;
            found += usize::from(value.is_some());
            output.push(value)?;
        }
        Ok(found)
    }
    fn update(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        let db = self.backend.db;
        self.write(|tx| {
            let mut count = 0;
            for (i, key) in keys.iter().enumerate() {
                if db
                    .get(tx, self.backend.key_width.bytes(key))
                    .map_err(|e| e.to_string())?
                    .is_some()
                {
                    db.put(
                        tx,
                        self.backend.key_width.bytes(key),
                        values.get(i).ok_or("missing input value")?,
                    )
                    .map_err(|e| e.to_string())?;
                    count += 1;
                }
            }
            Ok(count)
        })
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        let db = self.backend.db;
        self.write(|tx| {
            let mut count = 0;
            for key in keys {
                count += usize::from(
                    db.delete(tx, self.backend.key_width.bytes(key))
                        .map_err(|e| e.to_string())?,
                );
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
        keys.clear();
        output.clear();
        let read;
        let tx: &heed::RoTxn = if let Some(tx) = &self.transaction {
            tx
        } else {
            read = self.backend.env.read_txn().map_err(|e| e.to_string())?;
            &read
        };
        let range = (
            std::ops::Bound::Included(self.backend.key_width.bytes(&start)),
            std::ops::Bound::Unbounded,
        );
        for row in self
            .backend
            .db
            .range(tx, &range)
            .map_err(|e| e.to_string())?
            .take(limit)
        {
            let (key, value) = row.map_err(|e| e.to_string())?;
            keys.push(self.backend.key_width.from_bytes(key)?)?;
            output.push(Some(value))?;
        }
        Ok(keys.len())
    }
}

impl TransactionSession for LmdbSession<'_> {
    fn begin(&mut self) -> Result<()> {
        if self.transaction.is_some() {
            return Err("transaction already active".into());
        }
        self.transaction = Some(self.backend.env.write_txn().map_err(|e| e.to_string())?);
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
        self.transaction.take().ok_or("no active transaction")?.abort();
        Ok(())
    }
}

fn main() -> Result<()> {
    let cli: Cli = crudeval::parse_cli();
    let settings = [("Map size", cli.map_size.to_string())];
    run(
        cli.common,
        json!({"map_size": cli.map_size}),
        &settings,
        move |args, path| {
            if args.data_model != DataModel::KeyValue {
                return Err("LMDB supports only kv data_model".into());
            }
            Ok(Box::new_in(
                LmdbBackend::open(
                    path,
                    args.durability,
                    args.key_width()?,
                    usize::try_from(cli.map_size.0).map_err(|_| "map size exceeds platform limit")?,
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
        let db = LmdbBackend::open(path, Durability::None, KeyWidth::U64, 64 << 20).unwrap();
        crudeval::assert_backend_contract!(&db);
    }
}

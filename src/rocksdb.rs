//! RocksDB key-value benchmark with native batch reads and SST ingestion.
//!
//! Requires a C++ compiler, CMake, and libclang to build the bundled engine.
//!
//! ## Build and run
//!
//! ```sh
//! cargo run --release --no-default-features --features rocksdb-backend \
//!     --bin crud-eval-rocksdb -- --records 100K --threads 4
//! ```

use std::{
    alloc::System,
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use clap::Parser;
use rocksdb::{IngestExternalFileOptions, Options, SstFileWriter, WriteBatch, WriteOptions, DB};
use serde_json::json;

use crudeval::{
    backend::{
        directory_bytes, Backend, BackendCapabilities, BackendSession, BatchMode, DataModel, Durability, Key, KeyWidth,
        KeysOutput, RecordInput, RecordOutput, Result, TransactionSession,
    },
    run, Bytes, CommonArgs,
};

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,
    /// Memtable size before a flush, like 128MB.
    #[arg(long, env = "CRUDEVAL_WRITE_BUFFER_SIZE", default_value = "128MB", value_parser = crudeval::workload::parse_size)]
    write_buffer_size: Bytes,
}
struct RocksDbBackend {
    db: DB,
    key_width: KeyWidth,
    path: PathBuf,
    durability: Durability,
    write_buffer_size: usize,
    write_options: WriteOptions,
    sst_id: AtomicU64,
    ingested: AtomicBool,
}
impl RocksDbBackend {
    fn open(path: &Path, durability: Durability, key_width: KeyWidth, write_buffer_size: usize) -> Result<Self> {
        if write_buffer_size == 0 {
            return Err("write buffer size must be positive".into());
        }
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_write_buffer_size(write_buffer_size);
        let db = DB::open(&options, path).map_err(|e| e.to_string())?;
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(durability == Durability::None);
        write_options.set_sync(durability == Durability::Flushed);
        Ok(Self {
            db,
            key_width,
            write_options,
            path: path.into(),
            durability,
            write_buffer_size,
            sst_id: AtomicU64::new(0),
            ingested: AtomicBool::new(false),
        })
    }
}
impl Backend for RocksDbBackend {
    fn metadata(&self) -> BTreeMap<String, serde_json::Value> {
        let mut metadata = BTreeMap::new();
        metadata.extend([
            ("backend".into(), json!("rocksdb")),
            ("client_version".into(), json!("rocksdb 0.25.0")),
            ("library_version".into(), json!("11.8.1")),
            ("compact_after_ingestion".into(), json!(true)),
            ("durability".into(), json!(self.durability)),
            ("write_buffer_size".into(), json!(self.write_buffer_size)),
            ("conditional_writes".into(), json!("disjoint_deletes")),
        ]);
        metadata
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            ordered_ranges: true,
            batch_read: BatchMode::Native,
            batch_insert: BatchMode::Native,
            batch_update: BatchMode::Native,
            batch_delete: BatchMode::Native,
            bulk_load: BatchMode::Native,
            ..Default::default()
        }
    }
    fn session(&self) -> Result<Box<dyn BackendSession + '_, System>> {
        Ok(Box::new_in(RocksDbSession(self), System))
    }
    fn flush(&self) -> Result<()> {
        self.db.flush().map_err(|e| e.to_string())?;
        self.db.flush_wal(true).map_err(|e| e.to_string())?;
        if self.ingested.swap(false, Ordering::Relaxed) {
            self.db.compact_range::<&[u8], &[u8]>(None, None);
        }
        Ok(())
    }
    fn disk_bytes(&self) -> Result<u64> {
        directory_bytes(&self.path)
    }
}
struct RocksDbSession<'a>(&'a RocksDbBackend);
impl TransactionSession for RocksDbSession<'_> {}

impl BackendSession for RocksDbSession<'_> {
    fn insert(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        if let [key] = keys {
            self.0
                .db
                .put_opt(
                    self.0.key_width.bytes(key),
                    values.get(0).ok_or("missing input value")?,
                    &self.0.write_options,
                )
                .map_err(|e| e.to_string())?;
            return Ok(1);
        }
        let mut batch = WriteBatch::default();
        for (i, key) in keys.iter().enumerate() {
            batch.put(self.0.key_width.bytes(key), values.get(i).ok_or("missing input value")?);
        }
        self.0
            .db
            .write_opt(batch, &self.0.write_options)
            .map_err(|e| e.to_string())?;
        Ok(keys.len())
    }
    fn read(&mut self, keys: &[Key], output: &mut RecordOutput<'_>) -> Result<usize> {
        output.clear();
        if let [key] = keys {
            let value = self
                .0
                .db
                .get_pinned(self.0.key_width.bytes(key))
                .map_err(|e| e.to_string())?;
            output.push(value.as_deref())?;
            return Ok(usize::from(value.is_some()));
        }
        let mut count = 0;
        for value in self.0.db.multi_get(keys.iter().map(|key| self.0.key_width.bytes(key))) {
            let value = value.map_err(|e| e.to_string())?;
            count += usize::from(value.is_some());
            output.push(value.as_deref())?;
        }
        Ok(count)
    }
    fn update(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        if let [key] = keys {
            if self
                .0
                .db
                .get_pinned(self.0.key_width.bytes(key))
                .map_err(|e| e.to_string())?
                .is_none()
            {
                return Ok(0);
            }
            self.0
                .db
                .put_opt(
                    self.0.key_width.bytes(key),
                    values.get(0).ok_or("missing input value")?,
                    &self.0.write_options,
                )
                .map_err(|e| e.to_string())?;
            return Ok(1);
        }
        let mut batch = WriteBatch::default();
        let mut count = 0;
        for (i, key) in keys.iter().enumerate() {
            if self
                .0
                .db
                .get_pinned(self.0.key_width.bytes(key))
                .map_err(|e| e.to_string())?
                .is_some()
            {
                batch.put(self.0.key_width.bytes(key), values.get(i).ok_or("missing input value")?);
                count += 1;
            }
        }
        self.0
            .db
            .write_opt(batch, &self.0.write_options)
            .map_err(|e| e.to_string())?;
        Ok(count)
    }
    fn delete(&mut self, keys: &[Key]) -> Result<usize> {
        if let [key] = keys {
            if self
                .0
                .db
                .get_pinned(self.0.key_width.bytes(key))
                .map_err(|e| e.to_string())?
                .is_none()
            {
                return Ok(0);
            }
            self.0
                .db
                .delete_opt(self.0.key_width.bytes(key), &self.0.write_options)
                .map_err(|e| e.to_string())?;
            return Ok(1);
        }
        let mut batch = WriteBatch::default();
        let mut count = 0;
        for key in keys {
            if self
                .0
                .db
                .get_pinned(self.0.key_width.bytes(key))
                .map_err(|e| e.to_string())?
                .is_some()
            {
                batch.delete(self.0.key_width.bytes(key));
                count += 1;
            }
        }
        self.0
            .db
            .write_opt(batch, &self.0.write_options)
            .map_err(|e| e.to_string())?;
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
        let mut cursor = self.0.db.raw_iterator();
        cursor.seek(self.0.key_width.bytes(&start));
        while keys.len() < limit {
            let Some((key, value)) = cursor.item() else { break };
            keys.push(self.0.key_width.from_bytes(key)?)?;
            output.push(Some(value))?;
            cursor.next();
        }
        cursor.status().map_err(|e| e.to_string())?;
        Ok(keys.len())
    }
    fn bulk_load(&mut self, keys: &[Key], values: &RecordInput<'_>) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }
        let id = self.0.sst_id.fetch_add(1, Ordering::Relaxed);
        let path = self.0.path.join(format!("load-{id}.sst"));
        let options = Options::default();
        let mut writer = SstFileWriter::create(&options);
        writer.open(&path).map_err(|e| e.to_string())?;
        let result = (|| {
            for (i, key) in keys.iter().enumerate() {
                writer
                    .put(self.0.key_width.bytes(key), values.get(i).ok_or("missing input value")?)
                    .map_err(|e| e.to_string())?;
            }
            writer.finish().map_err(|e| e.to_string())?;
            let mut ingest = IngestExternalFileOptions::default();
            ingest.set_move_files(true);
            self.0
                .db
                .ingest_external_file_opts(&ingest, vec![&path])
                .map_err(|e| e.to_string())?;
            self.0.ingested.store(true, Ordering::Relaxed);
            Ok(keys.len())
        })();
        if path.exists() {
            let _ = std::fs::remove_file(path);
        }
        result
    }
}
fn main() -> Result<()> {
    let cli: Cli = crudeval::parse_cli();
    run(
        cli.common,
        json!({"write_buffer_size": cli.write_buffer_size}),
        &[("Write buffer size", cli.write_buffer_size.to_string())],
        move |args, path| {
            if args.data_model != DataModel::KeyValue {
                return Err("RocksDB supports only kv data_model".into());
            }
            Ok(Box::new_in(
                RocksDbBackend::open(
                    path,
                    args.durability,
                    args.key_width()?,
                    usize::try_from(cli.write_buffer_size.0).map_err(|_| "write buffer size exceeds platform limit")?,
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
        let db = RocksDbBackend::open(path, Durability::None, KeyWidth::U64, 8 << 20).unwrap();
        crudeval::assert_backend_contract!(&db);
    }
}

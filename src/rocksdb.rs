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
use rocksdb::{DBPath, IngestExternalFileOptions, Options, SstFileWriter, WriteBatch, WriteOptions, DB};
use serde_json::json;

use crudeval::{
    backend::{
        directory_bytes, Backend, BackendCapabilities, BackendSession, BatchMode, DataModel, Durability, Key, KeyWidth,
        KeysOutput, RecordInput, RecordOutput, Result, TransactionSession,
    },
    output, run, Bytes, CommonArgs,
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
    /// Every `db_paths` entry with its target size, or none when the database keeps to `path`.
    db_paths: Vec<(PathBuf, u64)>,
    durability: Durability,
    write_buffer_size: usize,
    write_options: WriteOptions,
    sst_id: AtomicU64,
    ingested: AtomicBool,
}
impl RocksDbBackend {
    fn open(
        path: &Path,
        further: &[PathBuf],
        durability: Durability,
        key_width: KeyWidth,
        write_buffer_size: usize,
    ) -> Result<Self> {
        if write_buffer_size == 0 {
            return Err("write buffer size must be positive".into());
        }
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_write_buffer_size(write_buffer_size);
        let mut db_paths = Vec::new();
        if !further.is_empty() {
            let disks = output::collect_disks();
            for directory in std::iter::once(path).chain(further.iter().map(PathBuf::as_path)) {
                let disk =
                    output::disk_of(&disks, directory).ok_or(format!("no disk holds {}", directory.display()))?;
                db_paths.push((directory.to_path_buf(), disk.total_bytes));
            }
            let entries = db_paths
                .iter()
                .map(|(directory, bytes)| DBPath::new(directory, *bytes))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            options.set_db_paths(&entries);
            // RocksDB turns dynamic level sizes off under several paths anyway; this states it.
            options.set_level_compaction_dynamic_level_bytes(false);
        }
        let db = DB::open(&options, path).map_err(|e| e.to_string())?;
        let mut write_options = WriteOptions::default();
        write_options.disable_wal(durability == Durability::None);
        write_options.set_sync(durability == Durability::Flushed);
        Ok(Self {
            db,
            key_width,
            write_options,
            path: path.into(),
            db_paths,
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
            (
                "data_layout".into(),
                json!(match self.db_paths.is_empty() {
                    true => "one directory",
                    false => "db_paths, filled in order, each up to its disk's size",
                }),
            ),
            ("db_paths".into(), json!(self.db_paths)),
            (
                "level_compaction_dynamic_level_bytes".into(),
                json!(self.db_paths.is_empty()),
            ),
        ]);
        metadata
    }
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            ordered_ranges: true,
            multiple_data_dirs: true,
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
        match self.db_paths.is_empty() {
            true => directory_bytes(&self.path),
            false => self
                .db_paths
                .iter()
                .map(|(directory, _)| directory_bytes(directory))
                .sum(),
        }
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
        // An SST file takes its keys in ascending order, which ordinals follow under uuid-v7 but not uuid-v4.
        let mut order = Vec::with_capacity_in(keys.len(), System);
        order.extend(0..keys.len());
        order.sort_unstable_by_key(|&i| keys[i]);
        let result = (|| {
            for i in order {
                writer
                    .put(
                        self.0.key_width.bytes(&keys[i]),
                        values.get(i).ok_or("missing input value")?,
                    )
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
                    &args.further_data_dirs(path),
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
        let db = RocksDbBackend::open(path, &[], Durability::None, KeyWidth::U64, 8 << 20).unwrap();
        crudeval::assert_backend_contract!(&db);
    }

    #[test]
    fn bulk_load_sorts_scattered_keys() {
        use crudeval::backend::RecordBatch;

        let directory = tempfile::tempdir().unwrap();
        let db = RocksDbBackend::open(directory.path(), &[], Durability::None, KeyWidth::U128, 8 << 20).unwrap();
        let keys = [Key::from_u128(9 << 64), Key::from_u128(3), Key::from_u128(5 << 32)];
        let mut values = RecordBatch::new(3, 64);
        {
            let mut out = values.as_output();
            for value in [b"nine", b"tri_", b"five"] {
                out.push(Some(value)).unwrap();
            }
        }
        let mut session = db.session().unwrap();
        assert_eq!(session.bulk_load(&keys, &values.as_input()).unwrap(), 3);
        let mut output = RecordBatch::new(3, 64);
        assert_eq!(session.read(&keys, &mut output.as_output()).unwrap(), 3);
        assert_eq!(output.as_input().get(0), Some(b"nine".as_slice()));
        assert_eq!(output.as_input().get(1), Some(b"tri_".as_slice()));
    }

    #[test]
    fn db_paths_contract() {
        let directories = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
        let further = [directories[1].path().to_path_buf()];
        let db = RocksDbBackend::open(
            directories[0].path(),
            &further,
            Durability::None,
            KeyWidth::U64,
            8 << 20,
        )
        .unwrap();
        assert_eq!(db.db_paths.len(), 2);
        assert!(db.db_paths.iter().all(|&(_, bytes)| bytes > 0));
        crudeval::assert_backend_contract!(&db);
    }
}

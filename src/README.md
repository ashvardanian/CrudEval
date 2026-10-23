# Source

`bench.rs` is the library root.
Each engine has one binary source file with its own typed command-line options and storage implementation.
The shared runner owns workload execution, validation, measurement, and reports.

## Responsibilities

The shared runner and data model live in:

- [`bench.rs`](bench.rs): `CommonArgs`, configuration sweeps, data manifests, worker coordination, entry budgets, and transaction boundaries.
- [`backend.rs`](backend.rs): backend/session traits, capabilities, typed batch buffers, keys, data models, and durability policies.
- [`workload.rs`](workload.rs): workload names, operation shares, batch sizes, and checked count/size parsing.
- [`data.rs`](data.rs): `KeySpace`, random distributions, and the deterministic value pool.
- [`model.rs`](model.rs): key-value, document, and graph generation and verification.

Measurement and infrastructure are split into:

- [`measure.rs`](measure.rs): workload measurements, latency summaries, and process resource sampling.
- [`output.rs`](output.rs): reports, machine information, configuration hashes, and atomic report writes.
- [`perf_counters.rs`](perf_counters.rs): optional Linux counters for benchmark worker threads.
- [`docker.rs`](docker.rs): container/network lifecycle, readiness, and server resource snapshots.
- [`cypher.rs`](cypher.rs): shared graph operations for Neo4j, Memgraph, and FalkorDB.

Each backend has its own binary:

- Embedded engines: [`rocksdb.rs`](rocksdb.rs), [`lmdb.rs`](lmdb.rs), [`redb.rs`](redb.rs), [`fjall.rs`](fjall.rs), [`sqlite.rs`](sqlite.rs), and [`turso.rs`](turso.rs).
- Database servers and variants: [`redis.rs`](redis.rs), [`mongodb.rs`](mongodb.rs), [`postgres.rs`](postgres.rs), [`neo4j.rs`](neo4j.rs), [`falkordb.rs`](falkordb.rs), [`surrealdb.rs`](surrealdb.rs), and [`scylladb.rs`](scylladb.rs).

## Vocabulary & Contracts

`Backend` owns the database and is shared across workers.
Each worker creates and uses its own `BackendSession`; sessions need not implement `Send`.
Storage operations are `insert`, `read`, `update`, `delete`, `bulk_load`, `range_read`, and `expand_neighbors`.
Methods return affected record counts; updates and deletes never create missing records.

`RecordBatch`, `DocumentBatch`, and `GraphBatch` own allocator-aware storage.
Their input views borrow typed slices; their output views append into bounded caller-provided storage and return an error on overflow.
Point reads retain request order and a position for every missing key; an empty value is present.
`range_read` uses an inclusive lower bound and returns ascending, unique keys with aligned values.
A full scan partitions the live key range into disjoint contiguous worker shards.

`Key` is a 16-byte UUID value; `KeyType` makes it from a record ordinal, decodes it back, and gives engines its big-endian bytes at the type's width, which sort as keys do.
`KeySpace` reserves globally unique insert ranges and publishes them only after successful writes or commits, tracking the earliest uncommitted reservation per worker.
Deletes claim the oldest live keys.
The data manifest records the live floor and next key after successful mutating workloads; interrupted mutations require a fresh bulk load.
Backend options participate in directory identity, preventing different server choices or engine settings from sharing a dataset.

`DataModel` selects `KeyValue`, `Documents`, or `Graph` through `--data-model key-value|documents|graph`.
Documents update `/score` while retaining their deterministic payload.
Graph updates change edge slot zero; slot identities survive deletion of incoming edges.
Read-modify-write derives the next value from the value actually read.

## Capabilities & Measurement

`BackendCapabilities` describes implemented modalities, ordered ranges, transactions, and per-operation `BatchMode` values: native, pipelined, or per-record.
Unsupported range workloads are reported as skipped and the remaining chain continues.
LMDB, redb, SQLite, Turso, and PostgreSQL expose explicit transactions; the other adapters reject `--calls-per-transaction`.
Redis-family and ScyllaDB adapters do not expose ordered ranges.
RocksDB uses SST ingestion for bulk loading.
SQLite, Turso, Redis-family servers, MongoDB, and ScyllaDB support key-value records and documents; PostgreSQL also supports graphs.
SurrealDB exposes documents and graphs.
Neo4j, Memgraph, and FalkorDB support graph workloads through typed, parameterized Cypher operations.

Throughput counts successful records, while latency distributions group storage calls by operation.
Transaction commits are timed and uncommitted records do not count as successful throughput.
Value generation and verification are outside individual storage-call latency; open-loop latency starts at the scheduled arrival.
Workload elapsed time excludes session teardown and histogram merging.
Flush time is separate, client resource sampling is process-wide, and optional hardware counters cover only benchmark worker threads.

## RetriEval Relationship

Both crates use `src/bench.rs` as their library root, feature-gated backend binaries, `CommonArgs`, `Backend`, `ConfigReport`, `MachineInfo`, and `ContainerHandle`.
Backend implementations use `<Engine>Backend` names, with `<Engine>Session` for storage worker state.
A configuration fixes engine settings, data generation, and worker count; a sweep runs several configurations.
A workload specifies an operation mix; a workload report describes one execution of it within a run.
CrudEval retains these storage-specific names instead of borrowing RetriEval's incremental-index `StepEntry` terminology.
Its shared backend is `Sync` and yields worker-local sessions, whereas RetriEval's engines generally manage their own query parallelism.

## Tests

Run commands from the repository root:

```sh
cargo test
cargo test --no-default-features --lib
cargo test --features lmdb-backend,redb-backend,fjall-backend
cargo test --no-default-features --features rocksdb-backend
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

The default feature runs SQLite tests alongside the shared library tests.
RocksDB requires its native build dependencies; see the root README.
Enabling server features compiles their adapters but does not start live server tests.

### Contracts

`backend.rs` defines `assert_backend_contract!`, invoked only by inline adapter tests.
The macro shares assertions across binary test targets without compiling a test helper into production binaries.
It checks missing-row alignment, present empty values, UUID ordering, inclusive range reads, zero-length ranges, update and delete counts, and flush persistence accounting.
Adapters advertising transactions also exercise read-your-writes, rollback, and commit.

Focused tests beside the shared implementation cover:

- Workload names, checked sizes, deterministic distributions, and large-key Zipf precision.
- Concurrent insert reservations, commit publication order, and payload corruption detection.
- Document payload preservation and graph adjacency, including stable edge slots after deletion.
- Small entry budgets, disjoint full scans, read-modify-write, commit accounting, and skipped workloads.
- Duration-limited arrival rates, persisted key ranges, backend configuration isolation, and interrupted mutation refusal.

`scripts/check-servers.sh` runs the ignored native server contracts and the supported server/modality workload chains.
The contracts check document integer precision and missing updates, plus exact two-hop graph results, stable edge slots, and incident-edge deletion.
The workload checks require zero failed or corrupted entries and permit explicit capability skips.
These checks require Docker and are separate from the default unit test suite.

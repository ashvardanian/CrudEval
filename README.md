# CrudEval

![CrudEval benchmarks thumbnail](https://github.com/ashvardanian/ashvardanian/raw/master/repositories/CrudEval.jpg?raw=true)

__CrudEval__ benchmarks Create, Read, Update, and Delete paths at crude hardware speeds.
It is the Rust successor to UCSB and a sibling of [RetriEval](https://github.com/ashvardanian/RetriEval).
One shared runner drives embedded storage engines and database servers, with opt-in document and graph workloads.
Runs use seeded data, verify returned values, and write latency distributions, throughput, resource usage, and configuration to JSON.

## Quickstart

```sh
sudo apt-get install build-essential clang libclang-dev cmake pkg-config
cargo run --release --bin crud-eval-sqlite -- \
    --records 100K --threads 4 --output results/

cargo run --release --no-default-features --features rocksdb-backend \
    --bin crud-eval-rocksdb -- --records 100K,1M --threads 1,4
```

SQLite is the default Cargo feature.
Every other binary has its own `<engine>-backend` feature; engine code is compiled only when selected.
Server binaries require a running Docker daemon and pull a pinned image on first use.
They publish a random port on localhost, or the `--port` that `crud-eval-scylladb` takes, wait for a successful database request, and remove their own containers when the run ends.
Database files remain in the run's data directory.

```sh
cargo run --release --no-default-features --features redis-backend \
    --bin crud-eval-redis -- --records 100K --threads 4
cargo run --release --no-default-features --features mongodb-backend \
    --bin crud-eval-mongodb -- --data-model documents --durability buffered \
    --records 10K --workloads bulk-load,read-95-update-5,read
cargo run --release --no-default-features --features postgres-backend \
    --bin crud-eval-postgres -- --data-model graph --records 1K --degree 8
cargo run --release --no-default-features --features neo4j-backend \
    --bin crud-eval-neo4j -- --data-model graph --durability flushed --records 1K
```

## Backends

| Backend                                                         | Key-value | Documents | Graphs |
| :-------------------------------------------------------------- | :-------: | :-------: | :----: |
| `crud-eval-rocksdb`, RocksDB 11.8.1                             |     ✓     |           |        |
| `crud-eval-lmdb`, through `heed`                                |     ✓     |           |        |
| `crud-eval-redis`, Redis, Valkey, Dragonfly, Garnet, or Kvrocks |     ✓     |     ✓     |        |
| `crud-eval-mongodb`, MongoDB or FerretDB                        |     ✓     |     ✓     |        |
| `crud-eval-redb`                                                |     ✓     |           |        |
| `crud-eval-fjall`                                               |     ✓     |           |        |
| `crud-eval-sqlite`                                              |     ✓     |     ✓     |        |
| `crud-eval-postgres`                                            |     ✓     |     ✓     |   ✓    |
| `crud-eval-neo4j`, Neo4j or Memgraph                            |           |           |   ✓    |
| `crud-eval-falkordb`                                            |           |           |   ✓    |
| `crud-eval-turso`, embedded Turso 0.8.1                         |     ✓     |     ✓     |        |
| `crud-eval-surrealdb`, SurrealDB 3.3.0                          |           |     ✓     |   ✓    |
| `crud-eval-scylladb`, ScyllaDB 2026.3.2                         |     ✓     |     ✓     |        |
| `crud-eval-ustore`, UStore's NVMe or RAM engine                 |     ✓     |     ✓     |   ✓    |

Every binary's flags, engine-specific ones included, are listed in the [CLI Reference](#cli-reference) below.
Reports include effective durability and distinguish native batches, protocol pipelines, and per-record loops for each operation.
Redis and ScyllaDB have no ordered range operation; range reads and full scans are reported as skipped.
A data model unsupported by the selected binary is rejected explicitly.
The matrix describes adapter coverage, not every upstream engine capability.
Redis-family documents use native JSON commands; Valkey needs its JSON bundle, and Garnet needs its JSON module.
FalkorDB supplies native graph operations over the Redis protocol; ordinary Redis-family adapters do not emulate graphs with hashes or sets.
Turso uses the embedded Rust engine, not libSQL or the hosted service; buffered durability is unsupported by this pinned adapter.
Its tables use a UUID index over an ordinary rowid table because the pinned engine's experimental `WITHOUT ROWID` support cannot execute the full mutation workload.
Neo4j checks internal vertex revisions around adjacency reads, with up to eight measured attempts; metadata records the extra round trips and revision storage.
SurrealDB splits large native insert batches to fit its RPC request limit and reports the transaction boundaries between chunks.
ScyllaDB runs one prepared CQL statement per record at `LOCAL_QUORUM` on a single replica, keeping up to 256 in flight per worker instead of sending multi-partition `BATCH` statements.
Its updates and deletes read the key first, so they never create or count a missing record; the check and the write are not atomic.
Documents are typed `score bigint` and `payload text` columns, and updates set the `score` column.
Durability `none` disables the keyspace's `durable_writes`, `buffered` uses periodic commit log sync, and `flushed` uses batch sync.
ScyllaDB needs direct I/O on its data directory, which Docker Desktop's file sharing on macOS does not provide; run it on Linux.
UStore opens its NVMe engine through `crudeval.ustore.json` in the run's first data directory, and its RAM engine keeps its image there.
Every UStore verb takes the whole batch on the engine's executor; updates and deletes first read which keys exist, without their values.
UStore documents are JSON, read back as `score` and `payload` columns; graph vertices keep their versions in a blobs collection and their edges in a graph collection, identified by slot, and change in transactions.
`--durability none` writes as `buffered`, since UStore has no log to disable.

## Workloads

Names spell out the operation and mix; there are no single-letter workload codes.
Sampling defaults to Zipfian; `--distribution uniform|zipf|latest` overrides it.
An explicitly named latest-read workload requires the latest distribution.
A run executes the comma-separated chain in order, preserving data between phases.
The default chain preserves UCSB's nine phases:

```text
bulk-load,read,batch-read-256,range-read-256,full-scan,read-50-update-50,read-latest-95-insert-5,batch-insert-1000,delete-oldest
```

| UCSB / YCSB    | Workload                       | Meaning                                                |
| :------------- | :----------------------------- | :----------------------------------------------------- |
| Init           | `bulk-load`                    | Load fresh sequential keys in batches of up to 100,000 |
|                | `ingest`                       | Load the same batches through the native bulk path     |
| Read / C       | `read`                         | Read one existing key                                  |
| BatchRead      | `batch-read-256`               | Read up to 256 distinct keys per call                  |
| RangeSelect    | `range-read-256`               | Inclusive ordered range, at most 256 entries           |
| Scan           | `full-scan`                    | Page through disjoint contiguous shards                |
| ReadUpdate / A | `read-50-update-50`            | Half reads, half updates                               |
| B              | `read-95-update-5`             | Read-mostly mix                                        |
| ReadUpsert / D | `read-latest-95-insert-5`      | Read recent keys and insert fresh keys                 |
| E              | `range-read-95-insert-5`       | Short ranges of 1–100 entries, with inserts            |
| F              | `read-50-read-modify-write-50` | Read, then modify the returned record                  |
| BatchUpsert    | `batch-insert-1000`            | Insert 1,000 fresh keys per call                       |
| Remove         | `delete-oldest`                | Delete oldest live keys up to the phase budget         |

The numeric suffixes on batch, range, bulk-load and ingest names are configurable: `batch-read-64`, `range-read-1K`, or `bulk-load-10K`.
`bulk-load` writes through each engine's ordinary batched inserts; `ingest` takes its native bulk path, such as RocksDB's sorted SST files, and is reported as its own row.
Mix percentages are per logical operation; throughput is successful entries per second.
Bulk-load keys ascend within each batch; concurrent workers may submit batches out of order.
Under `uuid-v4` they ascend by ordinal rather than by key, so RocksDB's `ingest` sorts each batch before writing its SST file, inside the measured load.
`--entries 10%` sets each ordinary phase's attempted entry budget relative to the initial record count.
A final partial batch uses the remaining budget.
Bulk load always loads `--records`; full scan covers the current live keyspace.
A load's throughput includes the flush after it, which every engine runs before the next phase.
Every other phase except the full scan first runs untimed for `--warmup`, then the timed run starts from that steady state.
`--time-limit 30s` replaces the ordinary phases' entry budget with a time limit, in whole `ms` or `s`.
The limit stops new work; in-flight operations and the final transaction commit may finish afterward.

```sh
cargo run --release --bin crud-eval-sqlite -- \
    --records 100K --threads 4 --entries 20K --value-size 100..1KB \
    --workloads bulk-load,read-95-update-5,read-50-read-modify-write-50 \
    --calls-per-transaction 32 --durability flushed
```

`--calls-per-second` sets aggregate scheduled logical operations per second across workers, with one operation in flight per worker.
It sets an arrival schedule, not a guaranteed achieved rate or an entry rate.
Rate-controlled latency starts at the intended arrival time, including preparation and queueing when workers fall behind.
Without a rate, latency measures adapter calls only; read-modify-write sums its read and write call times.
Phase throughput includes workload generation, verification, and transaction boundaries, but excludes setup and the separately reported flush.
`--verify structure` skips value verification, to measure that cost separately.
Transactions commit within the measured phase, every `--calls-per-transaction` logical operations per worker, including a final partial group.
Commit latency has its own histogram.
Backends without grouped transactions reject that option.

## Data Models

| Operation     | Key-value              | Documents                                                 | Graphs                                                      |
| :------------ | :--------------------- | :-------------------------------------------------------- | :---------------------------------------------------------- |
| Insert / load | UUID and binary value  | JSON document                                             | Vertex and outgoing edges                                   |
| Read          | Complete value         | Complete document                                         | Vertex version and outgoing adjacency                       |
| Update        | Replace existing value | Set `/score` using the database's JSON/document operation | Rewire the first outgoing edge                              |
| Delete        | Remove key             | Remove document                                           | Remove vertex and incident edges                            |
| Range read    | Ordered keys           | Ordered documents                                         | Distinct two-hop outgoing neighbors, capped by range length |
| Full scan     | Ordered keyspace       | Ordered document collection                               | Ordered vertex scan with adjacency                          |

### Keys & Order

Records are numbered by a sequential `u64` ordinal, and `--key-type` turns each ordinal into its key, with the same vocabulary as RetriEval's [Keys & Order](https://github.com/ashvardanian/RetriEval#keys--order) section.

- `uuid-v7`, the default, puts the ordinal in the RFC 9562 timestamp and counter, so keys sort as ordinals do and inserts stay append-ordered.
- `uuid-v4` mixes the ordinal through an invertible hash, so keys scatter over the whole key space, which is the random-insert case that costs ordered indexes most.
- `u32` and `u64` keep the ordinal itself, and `u32` caps a run at 2³² records.

Every type decodes back to its ordinal, so verification stays exact.
A full scan splits ordinals among workers and walks each slice in key order, so it is skipped under `uuid-v4`, whose key order is not ordinal order.

`--keys` replaces generated keys with a file: ordinal `n` takes the file's row `n`, so the file must hold ascending, unique keys, which the run checks before it starts.
A key's ordinal is then a binary search away, verification stays exact, and the run's capacity is the file's row count.
Like RetriEval's inputs, the path may be a quoted glob of shards, read in natural order, so `'keys.part_*.uuidbin'` reads `part_2` before `part_10`.

Engines store keys as follows:

- __RocksDB, LMDB, redb, fjall, SQLite, Turso and the Redis family__ store big-endian bytes, 4, 8 or 16 wide by key type; SQLite and Turso use a BLOB.
- __PostgreSQL__ stores the same bytes in `bytea`, rather than its native `integer` or `uuid` types.
- __MongoDB and FerretDB__ store them in a BSON Binary `_id` with the generic subtype.
- __ScyllaDB__ stores them in a `blob` partition key, rather than CQL's native types.
- __Neo4j, Memgraph and FalkorDB__ store canonical 36-character UUID strings in the vertex `id` property, integers zero-padded.
- __SurrealDB__ uses canonical UUID strings as native record identifiers.
- __UStore__ stores every key as its 16-byte UUID, integers big-endian.

Big-endian bytes and canonical strings both sort as keys do; Redis-family and ScyllaDB adapters do not expose ordered ranges.
Documents and graph payloads also render identifiers as canonical strings where their JSON representation requires them.
Equal key widths do not imply equal physical index size or serialization cost across engines.
`--key-type u64` matches UCSB's eight-byte keys for like-for-like comparisons with its historical results.

One shared keyspace allocates disjoint insert ranges and exposes only a contiguous prefix of completed commits to readers.
Each thread has a seeded generator; concurrency still makes mixed-workload interleavings nondeterministic.
Zipfian sampling retains UCSB's θ=0.99, fixed large domain, and FNV scramble using double precision.

Binary values have a 24-byte UUID/version header and a deterministic body drawn from a 64 MB pool built before measurement.
Reads verify the requested key, expected length, and every body byte.
Documents contain `_id`, mutable integer `score`, and an immutable hex payload derived from the binary value.
Generated scores and graph versions stay within the nonnegative signed 64-bit range shared by the engines.
Consequently `--value-size` describes the binary payload, not the larger serialized document size.
Reported processed bytes count logical values or patches, excluding keys, protocol framing, indexes, and physical storage writes.
Document updates currently target `/score` only.
Graph vertices carry a version and stable edge slots; their targets follow a seeded skewed distribution over the initial population.
`--degree` is capped at the initial population minus one, with self-loops and duplicate targets excluded.
Adjacency reads are checked against the deterministic graph model, accounting for deleted vertices.
Two-hop results are checked for duplicates, exclusion of the starting vertex, and the requested bound; the verifier does not independently replay a concurrent graph traversal.
Verification checks content consistency, not a complete concurrent history: a valid stale value can pass, and document reads do not prove that the latest score update was observed.

## CLI Reference

Every binary takes the shared flags; the engine flags apply only to the binary named.
A value spelled `<…,...>` takes comma-separated values and runs one configuration per combination.
Each run prints every setting at the start, as `- Name: value` in the same grammar the flag reads.
A bad value prints `--flag="value" does not parse, expected …` and exits with status 1.
Every flag also reads `CRUDEVAL_<FLAG>` as its default, upper-cased with dashes as underscores, so `--key-type` reads `CRUDEVAL_KEY_TYPE`.
The flag on the command line overrides the variable, and `--perf-counters` reads `1`, `0`, `true` or `false`.

<details>
<summary>Shared by every <code>crud-eval-*</code> binary</summary>

```text
crud-eval-*
  --records <N,...>                     100K
      Initial record counts, with decimal K, M, G or T; a sweep.
  --threads <N,...>                     0
      Worker counts, 0 for every core in the affinity mask; a sweep.
  --workloads <NAME,...>                the nine-phase chain
      Ordered workload names, run as one chain.
  --distribution <DISTRIBUTION>         per workload
      Key sampling override: uniform, zipf or latest.
  --entries <N|PERCENT>                 10%
      Attempted entries per ordinary phase, a count like 20K or a share of records.
  --time-limit <DURATION>               unset
      Time limit per ordinary phase instead of --entries, like 30s or 500ms.
  --warmup <DURATION>                   30s
      Untimed run of each ordinary phase before its timed run; 0s skips it.
  --value-size <SIZE|RANGE>             1KB
      Binary payload size or range like 100..1KB, at least 24 bytes.
  --data-dir <DIR,...>                  data
      Parent directories for marked benchmark databases, one per drive.
  --output <DIR>                        results
      Directory for JSON reports.
  --seed <SEED>                         42
      Seed for data and per-worker generators, or random.
  --durability <POLICY>                 none
      Requested write durability: none, buffered or flushed.
  --memory-limit <SIZE>                 the cgroup's limit
      Memory the engine may use, page cache included, at most the cgroup's limit; servers get it as their container's.
  --data-model <MODEL>                  key-value
      key-value, documents or graph.
  --key-type <TYPE>                     uuid-v7
      Keys made from record ordinals: u32, u64, uuid-v4 or uuid-v7.
  --keys <PATH|GLOB>                    generated
      Ascending, unique keys that ordinals index into: .i32bin, .u64bin or .uuidbin; excludes --key-type.
  --degree <N>                          8
      Outgoing graph degree, capped at the initial population minus one.
  --calls-per-transaction <N>           unset
      Logical operations per transaction; unset leaves boundaries to the adapter.
  --calls-per-second <RATE>             unset
      Aggregate scheduled operations per second across workers.
  --verify <LEVEL>                      values
      values checks payloads and structure, structure skips payload checks.
  --perf-counters                       off
      Record worker hardware counters; Linux and the perf-counters feature only.
  --between-runs <POLICY>               keep
      keep, reopen, or reopen-and-drop-caches on Linux; embedded engines only.
```

</details>

<details>
<summary>Engine flags</summary>

```text
crud-eval-redis
  --server <SERVER>                     redis
      redis, valkey, dragonfly, garnet or kvrocks.

crud-eval-mongodb, crud-eval-neo4j
  --server <SERVER>                     per binary
      mongodb or ferretdb; neo4j or memgraph.

crud-eval-scylladb
  --port <PORT>                         random
      Host port for CQL on localhost.
  --startup-time-limit <DURATION>       120s
      Time limit for container start and readiness.

crud-eval-ustore
  --engine <ENGINE>                     nvme
      nvme, or ram, which keeps its image in the data directory.
```

</details>

## Configuration & Reports

| Old interface                        | New interface                                        |
| :----------------------------------- | :--------------------------------------------------- |
| `ucsb_bench -db rocksdb`             | `crud-eval-rocksdb`                                  |
| CMake engine switches                | Cargo `<engine>-backend` features                    |
| `run.py -sz 100MB,1GB -th 1,8`       | `--records 100K,1M --threads 1,8`                    |
| Workload JSON files                  | `--workloads` and explicit workload names            |
| Engine `.cfg` files                  | Typed engine-specific CLI options                    |
| `operations_count`                   | `--entries` or `--time-limit`                        |
| `value_length`                       | `--value-size`                                       |
| `run.py -dp`                         | `--between-runs reopen-and-drop-caches`              |
| Nested merged Google Benchmark files | One `<backend>-<config-hash>.json` per configuration |

A chain beginning with bulk load replaces only its own marked benchmark directory.
Unmarked existing directories are never cleared.
A chain without bulk load reuses that configuration's data and restores the live key range saved after its last successful phase.
Interrupted or failed mutations leave the dataset marked dirty; reload it before another run.
Do not run two copies of the same configuration against the same data directory concurrently.
The server adapters accept only `--between-runs keep`; embedded adapters close and reopen between phases when requested.
Dropping the page cache requires Linux permissions and affects the whole host.
It does not provide a separate process or machine for each phase.

Reports are rewritten atomically after each phase.
They contain the machine, requested configuration, engine metadata, capabilities, completed/skipped/failed phases, successful entries, missing entries, errors, processed bytes, and disk size.
Latency is recorded in nanoseconds with p50, p90, p99, p99.9, and maximum per operation.
The timeline counts successful entries when published, after commit for grouped transactions.
Flush/checkpoint time is reported separately from workload time.
CPU, RSS, virtual memory, and process I/O are sampled every 100 ms; very short phases have limited sampling resolution.
Server reports additionally include a Docker resource snapshot after the phase, explicitly separate from client process measurements.
These snapshots are not interval-average server measurements.
Optional `--perf-counters` requires a Linux build with the `perf-counters` feature and kernel permission to open hardware counters.
Counters cover worker threads, excluding background engine threads and server processes.
Missing results, failed operations, and corrupted values are distinct; failed measured phases preserve their report and produce a nonzero exit status.
A short range near the end of the keyspace can legitimately contribute missing entries; full scans require complete coverage.

```sh
uv run scripts/plot.py results/ --output plots/
```

The plotting script reads the JSON reports and writes throughput, latency, memory, and disk charts as SVG.

### Several Drives

`--data-dir /mnt/nvme0,/mnt/nvme1` gives each configuration a directory under every parent, all with the same name, and the first holds the `.crudeval` marker.
A bulk load clears the others only when that marker vouches for them, and the number of parents is part of the configuration's identity.
Only `crud-eval-rocksdb` and `crud-eval-ustore --engine nvme` spread their files; other binaries refuse more than one directory.

- __UStore's NVMe engine__ keeps its manifest in the first directory and lands each sorted run in whichever directory has the most free bytes.
- __RocksDB__ gets one `db_paths` entry per directory, each targeting its disk's size, and keeps its write-ahead log and manifest in the first.
  RocksDB places whole levels on the paths in order, passing a level that does not fit the room left to a later path and the rest to the last one, while bulk-loaded SST files always land on the first; it is tiering, not striping.
  Several paths also turn `level_compaction_dynamic_level_bytes` off, and the targets are soft, so a first disk barely larger than the data can still fill up.
  A RAID0 volume passed as one `--data-dir` is RocksDB's striped layout.

Every report names the disk and file system under each directory in `data_disks`, so a RAID0 volume reads as one `/dev/md0` and separate drives as one device per directory.
RocksDB reports `data_layout` and `db_paths` with their target sizes, and UStore reports how many `data_directories` it opened.

### Twelve Drives on AWS

This runbook compares RocksDB with UStore's NVMe engine on an `i8g.48xlarge`: 192 Graviton4 cores, 1.5 TiB of RAM and 12 local 3.75 TB NVMe drives.
Each run loads 500M records of 1 KB, about 512 GB, then runs five phases of 60 s each.
Local drives vanish on a spot interruption, so every run copies its JSON reports to S3 as soon as it ends, and a background loop copies them every minute in between.
Build both binaries first, with the packages from the [Quickstart](#quickstart):

```sh
cargo build --release --no-default-features --features rocksdb-backend,ustore-backend \
    --bin crud-eval-rocksdb --bin crud-eval-ustore
export RESULTS_S3=s3://<bucket>/crudeval/i8g.48xlarge-$(date -u +%Y%m%dT%H%M)
drives=$(lsblk -d -n -o NAME,MODEL | awk '/Instance Storage/ {print "/dev/" $1}')
mkdir -p results && (while sleep 60; do aws s3 sync results/ "$RESULTS_S3/" --quiet; done) &
```

One function runs every configuration the same way.
Dropping the page cache needs root, the RAM cap keeps the 512 GB dataset well beyond what the page cache may hold, and io_uring needs locked memory:

```sh
crud() { # crud <rocksdb|ustore> <uuid-v7|uuid-v4> <data dirs, comma-separated>
    local extra=(); [ "$1" = ustore ] && extra=(--engine nvme)
    sudo systemd-run --scope --collect -p MemoryMax=128G -p MemorySwapMax=0 \
        prlimit --memlock=unlimited target/release/crud-eval-$1 "${extra[@]}" \
        --records 500M --value-size 1KB --threads 0 --key-type "$2" \
        --data-model key-value --durability buffered --between-runs reopen-and-drop-caches \
        --time-limit 60s --workloads bulk-load,read,read-95-update-5,read-50-read-modify-write-50,range-read-256,full-scan \
        --data-dir "$3" --output results/ 2>&1 | tee -a "results/$1-$2-$(date -u +%H%M).log"
    aws s3 sync results/ "$RESULTS_S3/"
}
```

First the native layout, one XFS file system per drive:

```sh
i=0; for drive in $drives; do
    sudo mkfs.xfs -f -K "$drive" && sudo mkdir -p /mnt/nvme$i && sudo mount -o noatime "$drive" /mnt/nvme$i
    i=$((i + 1))
done
sudo chown "$USER" /mnt/nvme*
native=$(ls -d /mnt/nvme* | sort -V | paste -sd,)
crud ustore uuid-v7 "$native"
crud ustore uuid-v4 "$native"
crud rocksdb uuid-v7 "$native"
sudo umount /mnt/nvme*
```

Then the RAID0 baseline, one XFS file system striped over all twelve:

```sh
sudo wipefs -a $drives
sudo mdadm --create /dev/md0 --run --level=0 --chunk=512 --raid-devices=12 $drives
sudo mkfs.xfs -f -K /dev/md0 && sudo mkdir -p /mnt/raid0 && sudo mount -o noatime /dev/md0 /mnt/raid0
sudo chown "$USER" /mnt/raid0
crud rocksdb uuid-v7 /mnt/raid0
crud rocksdb uuid-v4 /mnt/raid0
crud ustore uuid-v7 /mnt/raid0 # only with 16 minutes to spare
```

| Run               | Layout | Load     | Compaction   | Phases  | Estimate |
| :---------------- | :----- | :------- | :----------- | :------ | -------: |
| `ustore uuid-v7`  | native | 4–17 min | none         | 6–7 min |   16 min |
| `ustore uuid-v4`  | native | 4–17 min | none         | 5–6 min |   15 min |
| `rocksdb uuid-v7` | native | 2–4 min  | under 1 min  | 6–7 min |   10 min |
| `rocksdb uuid-v7` | RAID0  | 2–4 min  | under 1 min  | 6–7 min |   10 min |
| `rocksdb uuid-v4` | RAID0  | 2–4 min  | about 25 min | 5–6 min |   35 min |
| `ustore uuid-v7`  | RAID0  | 4–17 min | none         | 6–7 min |   16 min |

The first five runs and the two file system setups take about 1.5 hours; the last run adds 16 minutes.
The `rocksdb uuid-v4` load ends with a compaction of every overlapping SST file, which RocksDB runs on one thread and which grows with the data.
It runs only on RAID0, since its native layout would put the same files on the first drive.
Phase estimates include the reopens between phases and the full scan of 500M records, which `uuid-v4` skips.
The load and compaction ranges extrapolate small runs on RAM disks and are the least certain part of the estimate.


## Project Layout

```text
Cargo.toml                 Feature-gated backend binaries
src/bench.rs               Library root, CLI, sweeps, worker loop
src/backend.rs             Backend and per-worker session contracts
src/workload.rs            Explicit workload names and size parsing
src/data.rs                Seeded sampling, keyspace, binary values
src/model.rs               Key-value, document, and graph verification
src/measure.rs             Histograms and process resource sampling
src/output.rs              Machine/configuration reports and atomic output
src/perf_counters.rs       Optional Linux worker hardware counters
src/docker.rs              Owned container lifecycle
src/cypher.rs              Shared graph operations for Cypher servers
src/<engine>.rs            One binary per storage adapter
scripts/plot.py            Report visualization
```

Directory guides describe [source contracts](src/README.md), [test coverage](src/README.md#tests), [plotting](scripts/README.md), and [standalone server configurations](docker/README.md).

## API Ownership

The shared runner is the center of the backend design.
Embedded engines and server adapters implement the same lifecycle contract and provide worker-local sessions.
Key-value operations use borrowed flat byte batches.
`DocumentSession` receives typed scores and payloads; `GraphSession` receives UUIDs, versions, and flat edges with stable slots.
Read methods fill bounded caller-owned buffers and preserve positions for missing records.
Document updates pass only score patches, and graph updates pass only a version and slot-zero target.
JSON and BSON are database wire representations, not the shared record interface.

Calls are synchronous batches with one operation in flight per worker.
Async-only clients run behind the adapter boundary; they do not introduce boxed futures or task fan-out into the common API.
RetriEval follows the same ownership principles for vectors and search results, while retaining engine-owned query parallelism.
The projects share conventions, not a cross-repository framework dependency.

Project-owned collections use explicit `System` allocators, and reusable batch owners accept an allocator parameter.
Bounded buffers are reserved before timing and retain capacity across operations.
Database SDKs, serde values, operating-system APIs, and error strings may require their own standard containers; these allocations remain part of measured driver work where applicable.
They include RocksDB's owned `multi_get` values, MongoDB's BSON documents, Neo4j's Bolt maps, SurrealDB's document rows, and every server client's request and response buffers.
The pinned compiler does not expose an allocator parameter for `String`; hot formatting uses borrowed strings or byte buffers.
No dependency may silently select the benchmark's global allocator.

RocksDB and fjall rely on fresh insert keys and disjoint delete-only phases instead of an adapter-wide write mutex.
The key reservation lock protects publication of committed keys; an uncommitted reservation must never become visible to readers.
SQLite's coordinator lock covers checkpointing outside timed CRUD operations.

Schema 2 reports record attempted operation latency and successful committed throughput separately.
Plots separate schema versions so their different measurement semantics cannot be silently combined.

## Development

The repository pins Rust 1.100, as `beta-2026-10-18` until its stable release.
CI runs `cargo fmt --check`, Ruff on the plotting script, then `cargo clippy` and `cargo test` with every feature.
`src/` holds the shared library in `bench.rs` and one binary per engine, `scripts/` the plotting and server checks, `.githooks/` the Git hooks, and `docker/` the compose files for the servers.
Arm the hooks once per clone with `git config core.hooksPath .githooks`.
Run `scripts/check-servers.sh` for managed server integration checks.

## Remaining Scope

The implemented backend matrix above describes current support, not the full set of candidates considered during planning.
UStore's RocksDB and LevelDB engines are not benchmarked through `crud-eval-ustore`.
Native WiredTiger was deliberately excluded to avoid a separate C build and submodule; MongoDB exercises WiredTiger through a different interface and is not a substitute for a direct engine benchmark.
Cassandra, FoundationDB, Aerospike, SplinterDB, and Haura remain later candidates, with no adapters or placeholder binaries in this crate.

The following planned capabilities remain incomplete:

- Grouped transactions beyond LMDB, redb, SQLite, Turso, and PostgreSQL; UStore uses its own only inside graph mutations.
- Docker reopen/cache-drop support and continuous server resource sampling; current server statistics are post-phase snapshots.
- Document field updates beyond `/score`, incoming-neighbor graph reads, and an independent exact verifier for concurrent two-hop traversals.
- CUDA-equipped cross-project validation requires a suitable runner.

## Benchmark Pitfalls

### Durability vs Speed

Acknowledging a write in memory differs from acknowledging it after a durable log flush.
`--durability none`, `buffered`, and `flushed` make the requested policy explicit; metadata records the engine's actual settings.
Some engines cannot disable logging, and stronger behavior must not be presented as equivalent to a log-free write.
An engine that cannot honour a level rejects it rather than running a stronger or weaker one.
`flush` runs after every phase: engines with a log and store files checkpoint, while in-memory servers, whose only checkpoint rewrites the whole dataset, rely on their log.

| Engine        | `none`                   | `buffered`                        | `flushed`                        | `flush`                  |
| :------------ | :----------------------- | :-------------------------------- | :------------------------------- | :----------------------- |
| RocksDB       | WAL off                  | WAL, no sync                      | WAL synced per write             | memtables and WAL synced |
| LMDB          | `NO_SYNC`                | rejected                          | sync per commit                  | `force_sync`             |
| redb          | `Durability::None`       | rejected                          | `Immediate`                      | an `Immediate` commit    |
| fjall         | no journal persist       | `PersistMode::Buffer`             | `PersistMode::SyncAll`           | `SyncAll`                |
| SQLite        | WAL, `synchronous=OFF`   | WAL, `synchronous=NORMAL`         | WAL, `synchronous=FULL`          | WAL checkpoint           |
| Turso         | `synchronous=OFF`        | rejected                          | `synchronous=FULL`               | WAL checkpoint           |
| UStore        | as `buffered`            | log, no sync                      | log synced per commit            | a flushed commit         |
| PostgreSQL    | `fsync=off`              | `synchronous_commit=off`          | `synchronous_commit=on`          | `CHECKPOINT`             |
| MongoDB       | rejected                 | journal, no wait                  | `journal=true`                   | `fsync`                  |
| FerretDB      | Postgres `fsync=off`     | Postgres `synchronous_commit=off` | Postgres `synchronous_commit=on` | Postgres `CHECKPOINT`    |
| Redis, Valkey | no AOF                   | AOF, `everysec`                   | AOF, `always`                    | none                     |
| Dragonfly     | no snapshots             | rejected                          | rejected                         | none                     |
| Garnet        | no AOF                   | AOF, commit every 1,000 ms        | AOF, commit wait                 | `COMMITAOF` with an AOF  |
| Kvrocks       | WAL off                  | WAL, no sync                      | WAL synced                       | `FLUSHMEMTABLE`          |
| FalkorDB      | no AOF                   | AOF, `everysec`                   | AOF, `always`                    | none                     |
| ScyllaDB      | `durable_writes = false` | periodic commit log sync          | batch commit log sync            | `nodetool flush`         |
| Neo4j         | rejected                 | rejected                          | transaction log                  | `db.checkpoint()`        |
| Memgraph      | rejected                 | rejected                          | WAL flushed per commit           | none                     |
| SurrealDB     | rejected                 | rejected                          | SurrealKV `sync=every`           | none                     |

Device firmware and power-loss protection also affect what a successful flush guarantees.

LSM engines update a sorted memtable in RAM and, when logging is enabled, append to a write-ahead log (WAL) for recovery.
Freezing and flushing a memtable creates an immutable sorted-string-table (SST) file on disk; it does not merge all older versions away.
The WAL's durability policy and the later SST flush are separate concerns.

In leveled compaction, L0 files can cover overlapping key ranges.
Compaction reads selected files and their overlapping inputs from the next level, merges records by key and sequence number, and writes new SST files with disjoint ranges within that level.
The levels are logical file sets, not separate storage devices.
Once the new files are installed and old readers no longer need the inputs, obsolete files can be reclaimed.
An update can supersede older versions, but snapshots may still need them; a delete marker must remain while an older covered value could otherwise resurface.
Other policies make different tradeoffs; see the [RocksDB leveled-compaction guide](https://github.com/facebook/rocksdb/wiki/Leveled-Compaction).

Compaction rewrites existing data, increasing host writes relative to application writes, and can outlast the foreground workload.
The [RocksDB tuning guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide) explains this write amplification and its tradeoffs with read and space amplification.
RocksDB's `ingest` flush includes the compaction of its ingested files, and a load's throughput counts that flush.

### Engine & OS Caches

An engine's configured cache limit is not a process or machine memory limit.
Memory-mapped pages, the filesystem cache, client buffers, and database server memory all matter.
Measure the server as well as the client, and use operating-system limits when the experiment calls for a fixed memory budget.
A warm-cache read and a cold-cache read answer different questions.
Report the cache policy and working set rather than assuming one run establishes both.

### Dataset Size & NAND

An SSD's write performance can change after its SLC cache fills and as available capacity falls.
NAND cells encode bits in threshold-voltage windows; more bits per cell require more distinguishable windows.
Each extra bit doubles the number of states, reducing the margin between states within a fixed voltage window.

| NAND | Bits per cell | Voltage states |
| :--- | ------------: | -------------: |
| SLC  |             1 |              2 |
| MLC  |             2 |              4 |
| TLC  |             3 |              8 |
| QLC  |             4 |             16 |

An SLC-mode write cache can temporarily absorb writes faster than the drive sustains once that cache fills.
Cache capacity, free space, temperature, and workload affect that transition.
Folding cached data into denser storage shares NAND bandwidth with host writes and garbage collection.

Compare engines on similarly prepared drives, with enough data and runtime to reach the intended operating regime.
Small datasets can benchmark the device's cache rather than its sustained storage behavior.
Cell program/erase endurance is not drive endurance: error correction, spare capacity, and write amplification also affect rated terabytes written.
See [Kioxia's NAND endurance brief](https://americas.kioxia.com/content/dam/kioxia/en-us/business/memory/asset/KIOXIA-SSD-NAND-Endurance-Tech-Brief.pdf) for the distinction.

### Harness Overhead

Allocations, synchronization, key generation, value generation, and verification can bottleneck an otherwise fast engine.
CrudEval keeps value preparation outside the unthrottled engine-call latency interval and uses worker-local histograms.
Throughput still includes the harness cost, so inspect both metrics.
A batch's latency is the latency of the whole batch, not a fabricated per-key percentile.
A read-modify-write without a transaction is two calls and does not promise isolation against concurrent updates.
Resource samples are approximate, and a Docker snapshot does not replace continuous server profiling.
Match data models, key representations, batch semantics, durability, and cache policy before comparing engines.
Repeat runs and report their variation; a configured duration alone does not establish steady-state behavior.

## History

UCSB expanded the Yahoo Cloud Serving Benchmark with batch and range operations and described itself as the “Unbranded Cloud Serving Benchmark,” a grandchild of YCSB implemented in C++.
The original work was introduced in [Unbranding and Extending the Yahoo Cloud Serving Benchmark](https://www.unum.cloud/blog/2022-03-22-ucsb) on March 22, 2022, followed by [Beating RocksDB by up to 7x in almost every workload](https://www.unum.cloud/blog/2022-09-13-ucsb-10tb) on September 13, 2022.
Those historical results are not measurements of this Rust implementation.
The old single-precision Zipfian generator also restricted the effective sampled keyspace; the cleanup corrected it before this rewrite.
CrudEval preserves the workload lineage while changing key width, verification, key allocation, transaction timing, and reporting.

# Changelog

Runs of CrudEval's backends, one dated section per run day, newest first.
Every run uses the default nine-phase chain, 1 KB values, `uuid-v7` keys, Zipfian sampling, value verification and `--durability none` unless stated.
Throughput counts successful entries per second, and latency is per call, a whole batch for batched phases.

## November 5, 2026

### UStore Backend Check

`crud-eval-ustore` on its NVMe engine, on AWS `i8g.metal-24xl`: one Graviton4 socket of 96 Neoverse V2 cores, one NUMA domain, one local NVMe drive.
Each data model ran the default chain over 100K records with 8 threads, to check that the new backend loads, reads and verifies key-value records, documents and graphs.
100K records of 1 KB fit in memory, so these runs measure caches rather than the drive, and none of their throughput is kept as a result.

- No phase reported corrupted or failed entries.
- Graph range reads at the default degree of 8 report missing entries, because two hops reach at most 72 distinct neighbors, fewer than the 256 that `range-read-256` asks for.

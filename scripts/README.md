# Scripts

`plot.py` reads the JSON reports emitted by the benchmark binaries and produces four SVG charts: successful-entry throughput, operation p99 latency, client RSS, and database size.
They use RetriEval's palette, cream marks on a `#C44536` field, so one file reads the same on light and dark pages.
Each workload gets a separate panel; labels identify the engine, data model, record count, worker count, and configuration hash.
Only completed phases appear.

```sh
uv run scripts/plot.py results/
uv run scripts/plot.py results/ --output figures/
```

The script declares its Python and matplotlib dependencies inline for uv.
It uses a noninteractive rendering backend and accepts either a report directory or one JSON file.
The memory chart covers the client process; it does not imply that a server's memory is included.

## Checks

`./scripts/check-servers.sh` runs the supported server/modality combinations against managed containers.
It first runs native document and graph contracts, including integer precision, missing records, and exact two-hop results on a fixed graph.
It requires Docker, writes logs and reports into a fresh temporary directory, and fails on execution errors or corrupted records.

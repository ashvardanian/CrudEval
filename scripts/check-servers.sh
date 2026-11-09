#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
cargo build --no-default-features --features redis-backend,mongodb-backend,postgres-backend,neo4j-backend,falkordb-backend,surrealdb-backend,scylladb-backend
cargo test --no-default-features --features redis-backend,mongodb-backend,postgres-backend,neo4j-backend,falkordb-backend,surrealdb-backend,scylladb-backend --bins native_protocol_contract -- --ignored
python3 - <<'PY'
import json
import subprocess
import tempfile
from pathlib import Path

root = Path(tempfile.mkdtemp(prefix="crudeval-servers-"))
print(f"Validation reports: {root}", flush=True)
cases = []
for server in ("redis", "valkey", "dragonfly", "garnet", "kvrocks"):
    for model in ("key-value", "documents"):
        cases.append(("redis", ["--server", server], model, "none", f"{server}-{model}"))
for server in ("mongodb", "ferretdb"):
    for model in ("key-value", "documents"):
        cases.append(("mongodb", ["--server", server], model, "buffered", f"{server}-{model}"))
for model in ("key-value", "documents", "graph"):
    cases.append(("postgres", [], model, "none", f"postgres-{model}"))
for server in ("neo4j", "memgraph"):
    cases.append(("neo4j", ["--server", server], "graph", "flushed", server))
cases.append(("falkordb", [], "graph", "none", "falkordb"))
for model in ("documents", "graph"):
    cases.append(("surrealdb", [], model, "flushed", f"surrealdb-{model}"))
for model in ("key-value", "documents"):
    cases.append(("scylladb", [], model, "none", f"scylladb-{model}"))

for backend, extra, model, durability, label in cases:
    output = root / label
    command = [f"target/debug/crud-eval-{backend}", *extra, "--records", "64", "--threads", "2",
               "--entries", "32", "--warmup", "0s", "--value-size", "32", "--data-model", model,
               "--durability", durability, "--data-dir", str(output / "data"),
               "--output", str(output / "reports")]
    print(label, flush=True)
    output.mkdir()
    with (output / "run.log").open("w") as log:
        subprocess.run(command, check=True, stdout=log, stderr=subprocess.STDOUT, timeout=600)
    reports = list((output / "reports").glob("*.json"))
    if len(reports) != 1:
        raise RuntimeError(f"Expected one report for {label}")
    report = json.loads(reports[0].read_text())
    for phase in report["phases"]:
        if phase["status"] not in ("completed", "skipped") or phase["failed"] or phase["corrupted"]:
            raise RuntimeError(f"Invalid phase in {label}: {phase}")
print(f"Passed {len(cases)} server configurations. Reports: {root}")
PY

# Docker

These Compose files provide standalone equivalents of the pinned server images used by the Rust adapters.
The benchmark binaries manage their own containers directly and do not consume these files or attach to manually started servers.
Use the files for inspecting an engine's startup configuration or running it independently.

- [`redis.yml`](redis.yml): Redis, with optional `valkey`, `dragonfly`, `garnet`, and `kvrocks` profiles.
- [`mongodb.yml`](mongodb.yml): MongoDB, with an optional `ferretdb` profile backed by PostgreSQL/DocumentDB.
- [`postgres.yml`](postgres.yml): PostgreSQL.
- [`neo4j.yml`](neo4j.yml): Neo4j, with an optional `memgraph` profile.
- [`falkordb.yml`](falkordb.yml): FalkorDB.
- [`surrealdb.yml`](surrealdb.yml): SurrealDB.
- [`scylladb.yml`](scylladb.yml): ScyllaDB.

```sh
docker compose -f docker/redis.yml up -d redis
docker compose -f docker/redis.yml port redis 6379
docker compose -f docker/redis.yml down
```

Ports bind to localhost with dynamically assigned host ports.
Named volumes survive `down`; pass `--volumes` only when their data is no longer needed.
The sample configurations prioritize benchmark operation over durable write acknowledgment; the Rust binaries expose explicit durability policies and record their effective settings.

Garnet uses the native GarnetJSON module compiled from the same pinned 2.1.8 source release.
The Redis benchmark builds `crudeval-garnet-json:2.1.8` automatically when the image is absent.
To build it explicitly:

```sh
docker build -f docker/garnet-json.Dockerfile -t crudeval-garnet-json:2.1.8 docker
```

Valkey uses the bundle image with its native JSON module.
`surrealdb.yml` starts SurrealDB 3.3.0 with SurrealKV and `sync=every`; its document and graph adapter requires `--durability flushed`.

Dragonfly chooses its I/O thread count from the available CPUs by default.
On memory-constrained hosts, use `--server dragonfly --dragonfly-threads 4`; the selected count is recorded in the report and dataset identity.

# Quick start: the Docker lab

The Docker lab runs Kafka, MinIO, seeded telemetry and the VTOP engine on one
machine, so you can watch a batch go from source to verified object to source
commit. To build the binaries instead, see
[../install/from-source.md](../install/from-source.md).

```bash
docker compose up -d
docker compose logs -f vtop-engine
```

## What the lab runs

| Service | Purpose |
|---|---|
| `kafka` | test Kafka broker |
| `kafka-ui` | browser UI at `http://localhost:8080` |
| `minio` | S3-compatible object storage (built locally, see below) |
| `minio-init` | bucket initialization |
| `kafka-init` | seeded test events |
| `vtop-engine` | VTOP runtime |
| `rsyslog` | optional syslog collector profile |

MinIO endpoints (loopback only):

```text
API:     http://localhost:9000
Console: http://localhost:9001
Bucket:  telemetry-data
```

Host-side Kafka clients cannot use `127.0.0.1:9092`: the broker advertises its
in-network name `kafka:9092`. Run producers/consumers inside the compose
network (as `scripts/e2e-smoke.sh` does).

## Security of the lab

**Local-only by default (issue #81):** every published port — the lab's, the
observability stack's, and the benchmark backend's — binds to `127.0.0.1`, so
none of it is reachable from the network. The credentials are lab-grade
public defaults — the loopback bind, not the password, is the security
boundary. To deliberately re-expose the lab on a trusted network:
`VTOP_BIND_ADDR=0.0.0.0 docker compose up -d`. All lab credentials are
overridable via `.env` (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`,
`GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` — see `.env.example`).

Every container also runs against a hardening baseline: all capabilities
dropped, `no-new-privileges`, PID and memory limits, a read-only root
filesystem wherever the image tolerates one (the exceptions state their
reason in the compose file), and role-segmented networks — the broker plane,
the storage plane, and the observability plane are separate, and only the
services whose job spans planes join more than one. The optional
observability stack's Alloy mounts the host Docker socket read-only; treat
that as a privileged handle — it can read every container's logs, which is
why that stack stays lab-only.

## The MinIO image

MinIO no longer publishes container images (they were withdrawn from Docker
Hub and then quay.io), so the lab builds the pinned release from MinIO's
source (`docker/minio/Dockerfile`, checked out by commit). The first
`docker compose up` compiles it, which takes a minute or two. The upstream
repository is archived and receives no security fixes: this is a lab endpoint
on loopback-bound ports, never a store for real data. The image is built once
and reused; after changing `docker/minio/Dockerfile`, rebuild it with
`docker compose build minio`.

It runs as an unprivileged user, so a MinIO volume left by the older root-run
image is root-owned and the server cannot write to it. Recreate it once:
`docker compose down -v` for the lab stack, and
`docker compose -f benchmarks/docker-compose.benchmark.yml down -v` for the
benchmark stack. **`down -v` deletes the stack's volumes and everything in
them** (MinIO's objects included); copy out anything you still need first
(`mc mirror`).

## Kafka to MinIO

Start Kafka, MinIO, and seed data:

```bash
docker compose up -d kafka minio minio-init kafka-init
```

Start the engine:

```bash
docker compose up -d vtop-engine
docker compose logs -f vtop-engine
```

Expected lifecycle events:

```text
format_detected
object_uploaded
manifest_uploaded
verification_passed
source_committed
```

Open the MinIO console at `http://localhost:9001`, then inspect the
`telemetry-data` bucket.

## File to MinIO

Generate test input files:

```bash
docker/seed-events.sh cef    200 > ./data/input/auth.cef.log
docker/seed-events.sh json   200 > ./data/input/app.json.log
docker/seed-events.sh syslog 200 > ./data/input/sys.syslog.log
docker/seed-events.sh mixed  500 > ./data/input/mixed.log
```

Run the engine:

```bash
docker compose up -d vtop-engine
docker compose logs -f vtop-engine
```

Generate additional test data at any time:

```bash
docker/seed-events.sh <cef|json|jsonl|syslog|mixed> [count]
```

The same file flow runs without any infrastructure in
[`tests/integration_file_to_minio.rs`](../../tests/integration_file_to_minio.rs).

## Next

- [The CLI](../reference/cli.md): every `vtopctl` command and how to stop the engine.
- [The archive path](../reference/archive-path.md): the manifest, verification, recovery and metrics.

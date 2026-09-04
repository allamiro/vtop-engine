<div align="center">

<img src="docs/assets/vtop-logo.png" alt="VTOP Engine logo" width="220" />

# VTOP Engine

**Verified Telemetry Object Protocol Engine**

Replay-safe, manifest-driven telemetry transfer from Kafka, files, and syslog spools into object storage.

<br />

[![CI](https://github.com/allamiro/vtop-engine/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/allamiro/vtop-engine/actions/workflows/ci.yml?query=branch%3Amain)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2021-orange.svg)](https://www.rust-lang.org)
[![Status: Prototype](https://img.shields.io/badge/status-prototype-blue.svg)](#status)
[![Storage: S3 Compatible](https://img.shields.io/badge/storage-S3%20compatible-green.svg)](#upload-and-verification)
[![Safety: Verify Before Commit](https://img.shields.io/badge/safety-verify%20before%20commit-brightgreen.svg)](#core-rule)

</div>

---

## Overview

**VTOP Engine** moves telemetry into object storage while protecting the source commit point.

It ingests telemetry from:

- Kafka topics
- append-only log files
- syslog spool files

The longer-term cluster direction is a VTOP-owned native Rust broker and
control plane. Kafka remains supported as an optional edge/source adapter, not
as VTOP's coordinator or correctness dependency. See
[the native broker architecture](docs/NATIVE_BROKER_ARCHITECTURE.md).

For every batch, VTOP:

1. reads records from a source
2. forms a batch
3. compresses the batch
4. computes a checksum
5. creates a manifest
6. uploads the object and manifest
7. verifies the uploaded data
8. commits source progress only after verification succeeds

> [!IMPORTANT]
> VTOP does **not** commit Kafka offsets, file byte offsets, or syslog spool offsets until the destination object has been verified.

---

## Status

> [!NOTE]
> VTOP Engine is currently a **prototype / reference implementation** of a proposed protocol and proposed method.
>
> This repository is intended to support candidate-invention disclosure work. It is **not** patented or patent-pending.
>
> See [docs/INVENTION_DISCLOSURE_DRAFT.md](docs/INVENTION_DISCLOSURE_DRAFT.md).

---

## Table of contents

- [Overview](#overview)
- [Status](#status)
- [Why VTOP exists](#why-vtop-exists)
- [Core rule](#core-rule)
- [How it works](#how-it-works)
- [State machine](#state-machine)
- [Supported source modes](#supported-source-modes)
- [Format detection](#format-detection)
- [Architecture](#architecture)
- [Quick start](#quick-start)
- [Build and test](#build-and-test)
- [CLI usage](#cli-usage)
- [Docker lab](#docker-lab)
- [Example manifest](#example-manifest)
- [Metrics](#metrics)
- [Upload and verification](#upload-and-verification)
- [Replay and recovery](#replay-and-recovery)
- [Known limitations](#known-limitations)
- [Roadmap](#roadmap)
- [Documentation](#documentation)
- [License](#license)

---

## Why VTOP exists

Most log-to-object-storage pipelines can move bytes into a bucket.

The harder problem is knowing when it is safe to advance the source position.

```text
Did the object land intact,
and is it safe to commit the source offset now?
```

VTOP addresses this with a source-agnostic safety model:

| Capability | Purpose |
|---|---|
| **Manifest-bound transfer** | Binds source progress, object URI, checksum, format, compression, and verification state. |
| **Verify before commit** | Prevents source progress from advancing before destination verification. |
| **Replay-safe state store** | Allows recovery without silently losing unverified data. |
| **Explicit state machine** | Makes unsafe transitions visible and testable. |
| **Pluggable sources and backends** | Applies the same safety model to Kafka, files, syslog spools, and object storage backends. |

---

## Core rule

The protocol's **commit rule** ([docs/VTOP_PROTOCOL_DRAFT.md §13](docs/VTOP_PROTOCOL_DRAFT.md#13-commit-rule)):

```text
SOURCE_COMMITTED is forbidden until VERIFIED is true.
```

A source progress marker is never committed until the batch completes this lifecycle:

```text
DISCOVERED
  → BATCHING
  → SEALED
  → COMPRESSED
  → CHECKSUMMED
  → OBJECT_UPLOADED
  → MANIFEST_UPLOADED
  → VERIFIED
  → SOURCE_COMMITTED
```

Failure can happen from any state:

```text
ANY_STATE
  → FAILED
  → REPLAY_REQUIRED
  → BATCHING
```

> [!CAUTION]
> Transitions such as `SEALED → SOURCE_COMMITTED` or `OBJECT_UPLOADED → SOURCE_COMMITTED` are invalid.

---

## How it works

At a high level, VTOP separates source progress from destination durability.

```text
Source records
  → batch
  → compressed object
  → checksum
  → manifest
  → upload object
  → upload manifest
  → verify object and manifest
  → commit source progress
```

The manifest acts as the transfer evidence record.

It links:

- source type
- source name
- source progress marker
- object URI
- object checksum
- compression type
- detected format
- batch metadata
- reproducible manifest self-hash and optional keyed-BLAKE3 authentication

---

## State machine

![VTOP state machine](docs/assets/vtop-state-machine.png)

The state machine is the enforcement point for safe progress.

Only this final transition is valid:

```text
VERIFIED → SOURCE_COMMITTED
```

Invalid examples:

```text
SEALED → SOURCE_COMMITTED
COMPRESSED → SOURCE_COMMITTED
CHECKSUMMED → SOURCE_COMMITTED
OBJECT_UPLOADED → SOURCE_COMMITTED
MANIFEST_UPLOADED → SOURCE_COMMITTED
```

Relevant implementation:

- [crates/vtop-core/src/state_machine.rs](crates/vtop-core/src/state_machine.rs)
- [docs/VTOP_PROTOCOL_DRAFT.md §12](docs/VTOP_PROTOCOL_DRAFT.md#12-state-machine)

---

## Supported source modes

| Source mode | Progress marker | Behavior |
|---|---|---|
| **Kafka** | topic, partition, offset range | Uses a Kafka consumer with auto-commit disabled. Each batch contains records from one topic and one partition. Offsets are committed only after verification. |
| **File** | path, inode, byte range | Reads append-only files line-oriented, or whole-file for binary/compressed sources (`whole_file`). Partial trailing lines are not committed. Replay resumes from the last safe byte offset. |
| **Syslog spool** | spool ID, byte range | Treats rsyslog or syslog-ng spool files as append-only inputs. External collectors own syslog delivery; VTOP owns batching, upload, verification, replay, and commit safety. |

---

## Format detection

VTOP is not fixed to one telemetry format.

When a stream does not explicitly define a format in [examples/streams.yaml](examples/streams.yaml), the engine detects the format per batch.

Supported detected formats:

| Format | Example output extension |
|---|---|
| CEF | `.cef.gz` |
| LEEF | `.leef.gz` |
| JSON | `.json.gz` |
| JSON Lines | `.jsonl.gz` |
| Syslog | `.syslog.gz` |
| Raw | `.raw.gz` |

A single engine can process different formats across different streams.

For example:

```text
source A → CEF
source B → JSON Lines
source C → syslog
source D → mixed batches
```

The detected format is recorded in the manifest.

Relevant implementation:

- [crates/vtop-core/src/detect.rs](crates/vtop-core/src/detect.rs)

---

## Architecture

<div align="center">

<img src="https://cdn.simpleicons.org/rust/000000/FFFFFF" alt="Rust" height="34" />&nbsp;&nbsp;&nbsp;
<img src="https://cdn.simpleicons.org/apachekafka/231F20/FFFFFF" alt="Apache Kafka" height="34" />&nbsp;&nbsp;&nbsp;
<img src="https://cdn.simpleicons.org/minio/C72E49" alt="MinIO" height="34" />&nbsp;&nbsp;&nbsp;
<img src="https://cdn.simpleicons.org/sqlite/003B57/FFFFFF" alt="SQLite" height="34" />&nbsp;&nbsp;&nbsp;
<img src="https://cdn.simpleicons.org/docker/2496ED" alt="Docker" height="34" />

</div>

The engine reads from a source, writes a **compressed object plus a manifest** to
object storage, **verifies** what it wrote, and only then advances the source
commit point. Verification failure means the source is never committed, so the
data stays replayable.

```mermaid
flowchart LR
    S["Kafka · Files · Syslog spool"]
    E(["VTOP engine"])
    O[("S3 / MinIO")]
    D[("State store")]

    S -->|"read batch"| E
    E -->|"1 · upload object + manifest"| O
    O -->|"2 · verify size + checksum"| E
    E ==>|"3 · commit progress<br/>ONLY after VERIFIED"| S
    E <-.->|"batch state · replay ledger"| D

    classDef src fill:#eef4ff,stroke:#4a72b8,stroke-width:1px,color:#12263f
    classDef eng fill:#e8f7ee,stroke:#2e8b57,stroke-width:2px,color:#12263f
    classDef store fill:#fff6e5,stroke:#c07f19,stroke-width:1px,color:#12263f
    class S src
    class E eng
    class O,D store
```

Steps **1 → 2 → 3** are the whole protocol: the thick arrow is the one rule that
must never break — see [Core rule](#core-rule).

### Workspace layout

VTOP is organized as a Rust workspace.

```text
crates/
  vtop-core/       protocol-independent logic:
                   state machine, batching, manifests,
                   checksums, compression, partitioning,
                   config, replay

  vtop-log/        Kafka-independent native broker storage:
                   framed records, active/sealed segments,
                   crash recovery, sparse indexes, manifests

  vtop-adapters/   source adapters:
                   Kafka, file, syslog spool

  vtop-upload/     upload backends:
                   native S3, s3cmd, awscli, MinIO, mock

  vtop-state/      durable SQLite / feature-gated PostgreSQL state store

  vtop-cli/        vtopctl CLI and engine runtime

examples/          example config and sample streams
docs/              protocol, architecture, security, invention notes
docker/            container build files and seed scripts
tests/             integration tests
benchmarks/        benchmark and performance test support
```

> [!TIP]
> Keep detailed internals in `docs/`. The README should stay focused on orientation, quick start, and key guarantees.

Full architecture documentation:

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- [docs/ROADMAP.md](docs/ROADMAP.md) — what each release does, and what it
  deliberately does not do yet

---

## Quick start

Run the full Docker lab:

```bash
docker compose up -d
docker compose logs -f vtop-engine
```

Or build and run locally:

```bash
cargo build --release
cargo run -p vtop-cli -- discover --config examples/config.yaml
```

---

## Build and test

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release
```

CI runs formatting, linting, tests, and release build on push and pull request.

Kafka is enabled by default for compatibility with existing deployments, but
it is a feature-gated adapter. A native/file/syslog-only CLI build does not
compile or link `rdkafka`:

```bash
cargo build -p vtop-cli --no-default-features
```

Workflow file:

```text
.github/workflows/ci.yml
```

---

## CLI usage

The CLI binary is `vtopctl`.

```bash
cargo run -p vtop-cli -- run \
  --config examples/config.yaml

cargo run -p vtop-cli -- discover \
  --config examples/config.yaml

cargo run -p vtop-cli -- process-once \
  --source kafka \
  --config examples/config.yaml

cargo run -p vtop-cli -- process-once \
  --source file \
  --config examples/config.yaml

cargo run -p vtop-cli -- replay \
  --batch-id <batch_id> \
  --config examples/config.yaml

cargo run -p vtop-cli -- status \
  --config examples/config.yaml

cargo run -p vtop-cli -- list-batches \
  --config examples/config.yaml \
  --json

cargo run -p vtop-cli -- verify-manifest \
  --manifest s3://telemetry-data/.../batch.manifest.json \
  --config examples/config.yaml

# PostgreSQL only: run once with the privileged migration secret before the
# engine starts with its DML-only runtime secret.
cargo run -p vtop-cli --features postgres -- migrate \
  --config examples/config.yaml
```

Common CLI behavior:

| Option | Purpose |
|---|---|
| `--json` | machine-readable output |
| `--log-level` | runtime log level |
| non-zero exit | command failure |
| secret-safe output | commands should not print credentials |

PostgreSQL schema changes are never run by normal engine startup. Use a
separate migration identity for `vtopctl migrate`, then give the runtime role
only schema `USAGE` and `SELECT, INSERT, UPDATE` on `batches`. See
[PostgreSQL deployment](docs/POSTGRES_DEPLOYMENT.md).

---

## Docker lab

The Docker lab provides Kafka, MinIO, seeded telemetry, and the VTOP engine.

**Local-only by default (issue #81):** every published port — the lab's, the
observability stack's, and the benchmark backend's — binds to `127.0.0.1`, so
none of it is reachable from the network. The credentials below are lab-grade
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

| Service | Purpose |
|---|---|
| `kafka` | test Kafka broker |
| `kafka-ui` | browser UI at `http://localhost:8080` |
| `minio` | S3-compatible object storage |
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

Start the lab:

```bash
docker compose up -d
docker compose logs -f vtop-engine
```

---

## Kafka to MinIO example

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

Open the MinIO console:

```text
http://localhost:9001
```

Then inspect the `telemetry-data` bucket.

---

## File to MinIO example

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

Infrastructure-free file-flow test:

```text
tests/integration_file_to_minio.rs
```

---

## Example manifest

```json
{
  "protocol": "VTOP",
  "version": "0.2",
  "batch_id": "vtop-20260618T150000Z-app_events-p0-481000-482499-1a2b3c4d",
  "tenant": "default",
  "source_type": "kafka",
  "source_name": "app_events",
  "format": "cef",
  "compression": "gzip",
  "record_count": 1500,
  "source_progress": {
    "source_type": "kafka",
    "topic": "app_events",
    "partition": 0,
    "start_offset": 481000,
    "end_offset": 482499,
    "consumer_group": "vtop-engine"
  },
  "object": {
    "uri": "s3://telemetry-data/tenant=default/source=app_events/format=cef/year=2026/month=06/day=18/hour=15/vtop-....cef.gz",
    "size_bytes": 924822,
    "checksum_algorithm": "sha256",
    "checksum": "abc123..."
  },
  "manifest": {
    "uri": "s3://telemetry-data/.../vtop-....manifest.json",
    "sha256": "def456...",
    "mac": "0123abcd..."
  },
  "partitioning": {
    "path_template": "tenant={tenant}/source={source}/format={format}/year={yyyy}/month={mm}/day={dd}/hour={hh}",
    "resolved_prefix": "tenant=default/source=app_events/format=cef/year=2026/month=06/day=18/hour=15"
  },
  "upload_backend": "s3_native",
  "state": "manifest_uploaded",
  "verification_status": "not_verified",
  "created_at": "2026-06-18T15:00:02Z"
}
```

> [!NOTE]
> The manifest is written at `MANIFEST_UPLOADED`, before storage-side verification.
>
> The trailing `batch_id` token is a random suffix: replaying a batch currently writes a **new** object key rather than reproducing the same one — the open gap against the deterministic-naming rule of [docs/VTOP_PROTOCOL_DRAFT.md §15.1](docs/VTOP_PROTOCOL_DRAFT.md#151-object-key).
>
> The authoritative post-verification state lives in the state store and can be queried with `vtopctl status` or `vtopctl list-batches --json`.

The manifest self-hash is reproducible and detects accidental changes, but an
attacker able to rewrite the manifest can recompute it. Set
`manifest_mac_key_env` to the name of an environment variable containing a
32-byte hex key to add `manifest.mac`, a keyed BLAKE3 authenticator. Both
embedded values are blanked for canonicalization. The key itself is never
serialized. Enabling a key intentionally rejects older unsigned manifests;
verify the backlog before cutover. Key rotation is not implemented yet.

---

## Metrics

VTOP emits structured per-batch metrics.

Example:

```text
3 records, 114 B->80 B (1.43x, 29.8% saved) in 6 ms | 500 rec/s, 0.00 MiB/s up |
stages: compress=0ms checksum=0ms put_obj=0ms put_manifest=0ms verify=0ms commit=0ms
```

Each batch records:

| Metric area | Examples |
|---|---|
| **Size and transfer** | uncompressed bytes, compressed bytes, compression ratio, percentage saved |
| **Latency** | compression, checksum, object upload, manifest upload, verification, commit |
| **Throughput** | records/sec, uncompressed MiB/sec, effective upload MiB/sec |

`vtopctl process-once --json` includes the full metrics object per batch.

Prometheus metrics are exported by the engine at `/metrics` when `VTOP_METRICS_ADDR` is set. See [observability/](observability/) for the optional Grafana LGTM stack (Alloy + Mimir/Loki/Tempo) and dashboards.

Relevant implementation:

- [crates/vtop-core/src/metrics.rs](crates/vtop-core/src/metrics.rs)

---

## Upload and verification

VTOP supports multiple upload backends.

| Backend | Purpose | Verification level |
|---|---|---|
| native S3 | primary S3-compatible backend | service-computed SHA-256 or streamed BLAKE3 |
| AWS CLI | command-based backend | downloads and hashes stored content |
| s3cmd | command-based backend | downloads and hashes stored content |
| MinIO client | command-based backend | downloads and hashes stored content |
| LocalFS | local/air-gapped backend | streams stored files through the configured digest |
| mock | tests and local integration flow | hashes stored in-memory content |

> [!IMPORTANT]
> Strong verification is the default. A sidecar, ETag, or uploader-written user
> metadata is never accepted as proof of stored content.

The `awscli`, `s3cmd`, and `minio` compatibility backends are an explicit
opt-in. They require `upload.command_binary` to be an absolute path, verify the
tool's `--version` identity at startup, clear the child environment, and apply
wall-clock and captured-output limits. Add only the exact runtime variable
names the selected tool needs to `upload.command_env_allowlist`; values are
resolved at startup and are never serialized. Native `s3_native` does not
spawn an external process and does not use these settings.

---

## Replay and recovery

VTOP recovery is designed around one rule:

```text
Unverified data must remain replayable.
```

Recovery behavior:

| Crash point | Recovery action |
|---|---|
| before object upload | replay from source |
| after object upload but before verification | replay from source |
| after verification but before source commit | retry source commit |
| after source commit | batch is complete |

If verification fails:

```text
batch → FAILED
source progress → not committed
```

If commit fails after verification:

```text
batch → VERIFIED
recovery → retries source commit
```

Relevant tests:

```text
tests/integration_replay.rs
tests/integration_state_recovery.rs
```

---

## Known limitations

VTOP is currently a prototype. The following limits are known and intentional.

| Area | Current behavior | Planned direction |
|---|---|---|
| Deterministic object naming | not yet — `batch_id` embeds a timestamp + random suffix, so replay writes a **new** key (duplicate object, no data loss), against the MUST of [protocol §15.1](docs/VTOP_PROTOCOL_DRAFT.md#151-object-key)/§16 | deterministic / content-addressed keys ([PRODUCTION_HA.md Phase 4](docs/PRODUCTION_HA.md)) |
| Verification classes | strong (default) / backend-limited / disabled (size-only), per [protocol §17](docs/VTOP_PROTOCOL_DRAFT.md#17-verification-semantics); non-strong commit requires explicit opt-out | — |
| Large objects | native S3 + mock support resumable multipart with persisted sessions (`vtopctl tier copy --multipart-state-dir`); compatibility backends still single-shot `put_object` | wire remaining backends / streaming rehydrate |
| Large records / whole files | `max_bytes` is a hard per-source/per-batch ceiling; an oversized record is rejected without advancing source progress | raise the explicit budget only when the deployment has matching memory headroom |
| Partial upload recovery | replays from source instead of resuming half-written local objects | add resumable local staging |
| Command backend verification cost | `aws`, `s3cmd`, and `mc` download each stored object to hash it | prefer native S3 SHA-256 when read-back bandwidth is costly |
| Syslog timestamps | `received_time_*` is not yet extracted into the spool marker | add timestamp extraction |
| Manifest integrity | self-hash plus optional keyed-BLAKE3 authentication; key rotation not implemented | add multi-key rotation and public-key signatures if required |
| Object immutability | manifest version pinning + bucket-versioning validation implemented (hardened profile, `upload.require_object_versioning`); Object Lock retention itself is deployment policy | document an Object Lock deployment profile |
| Metrics export | **Prometheus `/metrics` implemented** (opt-in via `VTOP_METRICS_ADDR`); OpenTelemetry trace export not yet | add OTLP span export |
| Kafka integration test | requires live broker and is ignored by default | add optional CI service profile |
| Binary / pre-compressed inputs | **supported** via the file source `whole_file` mode (archived verbatim, byte-exact) | streaming for very large files |
| Local filesystem backend | **available** (`backend: localfs`, objects under `local_path/<bucket>/<key>`; sidecars are inventory hints only) | — |
| Checksums | **SHA-256 and BLAKE3**, or disabled (size-only); strong verification defaults on and `require_strong_verification: false` is an explicit weak-mode opt-out | — |

---

## Roadmap

Completed:

- [x] local filesystem upload backend (`backend: localfs`)
- [x] BLAKE3 checksum strategy (and checksum-disabled mode)
- [x] binary / pre-compressed input framing (file source `whole_file` mode)
- [x] strong-verification gate (`require_strong_verification`)
- [x] **Prometheus metrics exporter** — the engine serves `/metrics` (`/healthz`,
      `/readyz`) behind `VTOP_METRICS_ADDR`; see [observability/](observability/)
      for the optional Grafana LGTM stack and dashboards
- [x] **end-to-end smoke + live-broker Kafka CI** over the full compose lab
- [x] Kafka is isolated behind the optional `kafka` Cargo feature
- [x] first Kafka-independent native segment-log storage kernel
- [x] bounded native produce/fetch wire codec and TLS-1.3 mTLS local-broker
      library with durable producer-epoch fencing and committed-only fetch
- [x] bounded file/syslog/whole-file reads, pre-clone Kafka record checks, and
      streaming local compression; `max_bytes` is enforced before source
      progress advances
- [x] multipart resumable upload (native S3 + mock; persisted sessions, fencing, abandoned cleanup)
- [x] optional keyed-BLAKE3 manifest authentication via a named secret env var
- [x] **native three-node metadata/control plane** — an embedded Raft group
      with linearizable reads, membership change, and an mTLS admin endpoint
      whose commands are authorized by verified caller identity
- [x] **leader→follower quorum replication** — persistent mTLS streams,
      pipelined batches, per-follower flow control and memory budgets, and a
      committed high-water mark that fetch never reads past
- [x] **lease-driven range leadership with fencing epochs** — metadata grants
      the range, a grant always mints `epoch + 1`, and both leaders and
      followers refuse anything carrying an older one
- [x] **verified promotion** — a new leader establishes its committed boundary
      from a quorum of *fenced* replicas before serving, and refuses rather
      than guessing when a quorum cannot be reached
- [x] **epoch-qualified recovery (v0.2.0)** — each replica records which
      fencing epoch wrote each stretch of its log (KIP-101 style), reconciles
      against the new leader while fenced, and truncates a diverged tail
      instead of being stranded — bounded so it can never discard
      acknowledged records
- [x] **15-scenario live-chaos suite in CI** — real processes over real TLS:
      SIGKILL durability, disk-full and fsync failure, metadata partition,
      clock skew, membership change under load, and range-leader failover
- [x] **signed, attested releases** — multi-arch image and per-target binaries
      with keyless Sigstore signatures, SBOM, and provenance attestations

Not implemented yet:

- [ ] manifest MAC key rotation / optional public-key signatures
- [ ] S3 Object Lock profile
- [ ] OpenTelemetry trace export (the metrics endpoint exists; spans do not yet)
- [ ] million-file benchmark suite

---

## Documentation

| Document | Purpose |
|---|---|
| [docs/README.md](docs/README.md) | documentation index and reading order |
| [docs/VTOP_PROTOCOL_DRAFT.md](docs/VTOP_PROTOCOL_DRAFT.md) | protocol draft and conformance profiles |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | architecture, runtime flow, implementation status |
| [docs/PRODUCTION_HA.md](docs/PRODUCTION_HA.md) | engine HA: status, design, and phased roadmap |
| [docs/ROADMAP.md](docs/ROADMAP.md) | cluster-plane releases and per-release limitations |
| [docs/SECURITY_MODEL.md](docs/SECURITY_MODEL.md) | security model and normative rules |
| [docs/INVENTION_DISCLOSURE_DRAFT.md](docs/INVENTION_DISCLOSURE_DRAFT.md) | candidate-invention disclosure draft |
| [docs/PRIOR_ART_SEARCH_PLAN.md](docs/PRIOR_ART_SEARCH_PLAN.md) | prior-art search plan |


---

## License

**[MIT](LICENSE) © 2026 Tamir Suliman** — and today that covers the entire
engine.

The repository is set up as **dual-licensed** so the boundary is written down
before there is anything on the other side of it: directories named `ee/`, and
files carrying the header `VTOP Engine Enterprise Edition`, fall under
[LICENSE-EE](LICENSE-EE) instead, which permits evaluation, development and
testing but requires a commercial subscription for production use. **No `ee/`
directory exists yet**, so nothing in this repository is currently under those
terms.

Three rules bound it, stated in [COMMERCIAL.md](COMMERCIAL.md): code that is
MIT stays MIT, the core must build and pass CI with `ee/` deleted, and anything
whose absence could lose or corrupt acknowledged data — durability, fencing,
replication, failover, verification — stays in the core. Paying is for
operating at scale, not for not losing data.

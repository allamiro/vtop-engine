<div align="center">

<img src="docs/assets/vtop-logo.png" alt="VTOP Engine logo" width="220" />

# VTOP Engine

**Verified Telemetry Object Protocol Engine** — a replay-safe, manifest-driven telemetry object transfer engine.

[![CI](https://github.com/allamiro/vtop-engine/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/allamiro/vtop-engine/actions/workflows/ci.yml?query=branch%3Amain)
[![License: MIT](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2021-orange.svg)](https://www.rust-lang.org)
[![Status: prototype](https://img.shields.io/badge/status-prototype-blue.svg)](#status)

</div>

> [!NOTE]
> <a name="status"></a>**Status:** prototype / reference implementation of a *proposed protocol* and *proposed method*. This repository is a candidate-invention disclosure support package. It is **not** patented or patent-pending. See [docs/INVENTION_DISCLOSURE_DRAFT.md](docs/INVENTION_DISCLOSURE_DRAFT.md).

VTOP ingests telemetry from **Kafka topics**, **log files**, and **syslog spool files**; forms adaptive batches; compresses them; computes SHA‑256 checksums; generates a **manifest** for every object; uploads both the telemetry object and its manifest to **S3‑compatible object storage**; verifies the uploaded object and manifest; and commits source progress **only after verification succeeds**.

## Table of contents

- [Why it exists](#why-it-exists)
- [Core rule](#core-rule)
- [State machine](#state-machine)
- [Workspace layout](#workspace-layout)
- [Source modes](#source-modes)
- [Quick start](#quick-start)
- [Build and test](#build-and-test)
- [CLI usage](#cli-usage)
- [Docker lab](#docker-lab)
- [Example manifest](#example-manifest)
- [Metrics and efficiency](#metrics-and-efficiency)
- [Verification before commit](#verification-before-commit)
- [Replay after crash](#replay-after-crash)
- [Known limitations](#known-limitations)
- [Documentation](#documentation)
- [License](#license)

## Why it exists

Most log-to-object-storage tools move bytes into a bucket. They do **not** provide a single, source-agnostic *commit model* across Kafka, files, and syslog spools with **mandatory manifest verification before source progress is advanced**. VTOP makes the safety rule explicit and enforces it in code.

`gzip + upload` cannot answer: *did the object actually land intact, and is it safe to forget the source position now?* VTOP adds:

- a **manifest** that binds the source progress marker → object SHA‑256 → verification state (chain of custody);
- a **strongly-typed state machine** that makes premature commit impossible;
- a **replay-safe state store** so a crash never advances an unverified source;
- **verification before commit**, across pluggable storage backends.

## Core rule

```text
SOURCE_COMMITTED is forbidden until VERIFIED is true.
```

A Kafka offset, file byte offset, or syslog spool offset is **never** committed until, in order:

1. the batch is sealed
2. the compressed object is created
3. the object checksum is calculated
4. the object is uploaded
5. the manifest is generated
6. the manifest is uploaded
7. the uploaded object is verified
8. the manifest is verified
9. **only then** source progress is committed

This is enforced in [state_machine.rs](crates/vtop-core/src/state_machine.rs): the only legal predecessor of `SourceCommitted` is `Verified`, and the same guard is re-applied at the state-store layer.

## State machine

```text
DISCOVERED → BATCHING → SEALED → COMPRESSED → CHECKSUMMED
   → OBJECT_UPLOADED → MANIFEST_UPLOADED → VERIFIED → SOURCE_COMMITTED

ANY_STATE → FAILED        FAILED → REPLAY_REQUIRED → BATCHING
```

Illegal transitions (e.g. `SEALED → SOURCE_COMMITTED`) return `VtopError::IllegalStateTransition` / `CommitBeforeVerified`. See the `test_cannot_commit_from_*` tests in [state_machine.rs](crates/vtop-core/src/state_machine.rs). The full normative description is in [docs/VTOP_PROTOCOL_DRAFT.md](docs/VTOP_PROTOCOL_DRAFT.md).

## Workspace layout

```text
crates/
  vtop-core/       protocol-independent logic (state machine, batch, manifest,
                   checksum, compression, partitioning, config, replay)
  vtop-adapters/   source adapters: kafka_source, file_source, syslog_spool_source
  vtop-upload/     upload backends: s3_native (primary) + s3cmd/awscli/minio + mock
  vtop-state/      SQLite state store (sqlx) — the durable journal
  vtop-cli/        the `vtopctl` binary + the engine runtime
examples/          config.yaml, streams.yaml, sample logs
docs/              protocol draft, invention disclosure, prior-art plan, etc.
docker/            Dockerfile + entrypoint
tests/             integration tests (wired into vtop-cli via [[test]] paths)
```

`vtop-core` has **no** dependency on Kafka, S3, or the CLI. A deeper tour is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Source modes

| Mode | Progress marker | Behavior |
|------|-----------------|----------|
| **Kafka** | offset per topic+partition | `rdkafka` consumer with **auto-commit always disabled**; one batch = one topic + one partition + one offset range (partitions never mixed); offsets committed only after `VERIFIED`. |
| **File** | byte offset | Reads append-only files line by line, tracking `path`, `inode`, byte offsets, size, mtime; resumes from the last committed byte; a partial trailing line is never committed; replay rewinds to the start of the uncommitted range. |
| **Syslog spool** | spool byte offset | Treats rsyslog / syslog-ng spool files as append-only with a `spool_id` and byte range. External collectors own delivery; VTOP owns batching, checksum, manifest, upload, verification, replay state, and the commit rule. |

Every object gets a `*.manifest.json` written alongside it, binding the **source progress marker** to the object's **SHA‑256** plus a self-hash for tamper-evidence.

### Format auto-detection (mixed formats)

Format is **not** fixed to CEF. When a stream does not declare a `format` in
[`streams.yaml`](examples/streams.yaml), the engine **auto-detects it per batch**
from the content — CEF, JSON, JSON Lines, syslog (PRI header), or plain text.
Because detection runs per batch, **different formats can flow through one engine
at the same time**: source A can be CEF, source B JSON, source C syslog, and each
batch gets the correct object extension (`.cef.gz`, `.jsonl.gz`, …) and records
its detected `format` in the manifest. An explicit `format` in `streams.yaml`
always overrides detection. See [detect.rs](crates/vtop-core/src/detect.rs).

## Quick start

```bash
# Run the full lab (Kafka + MinIO + engine) in containers:
docker compose up -d
docker compose logs -f vtop-engine

# Or build and run locally against the example config:
cargo build --release
cargo run -p vtop-cli -- discover --config examples/config.yaml
```

## Build and test

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release
```

CI runs all four on every push and pull request — see [.github/workflows/ci.yml](.github/workflows/ci.yml).

## CLI usage

The binary is `vtopctl`:

```bash
cargo run -p vtop-cli -- run             --config examples/config.yaml
cargo run -p vtop-cli -- discover        --config examples/config.yaml
cargo run -p vtop-cli -- process-once --source kafka --config examples/config.yaml
cargo run -p vtop-cli -- process-once --source file  --config examples/config.yaml
cargo run -p vtop-cli -- replay --batch-id <batch_id> --config examples/config.yaml
cargo run -p vtop-cli -- status          --config examples/config.yaml
cargo run -p vtop-cli -- list-batches    --config examples/config.yaml --json
cargo run -p vtop-cli -- verify-manifest --manifest s3://telemetry-data/.../batch.manifest.json --config examples/config.yaml
```

Every command supports `--json` (machine-readable) and `--log-level`, exits non-zero on failure, and never prints secrets.

## Docker lab

```bash
docker compose up -d
docker compose logs -f vtop-engine
```

Services: `kafka`, `kafka-ui` (http://localhost:8080), `minio` (API `:9000`, console `:9001`, bucket `telemetry-data`), `minio-init`, `kafka-init` (seeds topic `app_events` with sample CEF events), `vtop-engine`, and an optional `rsyslog` collector (`--profile syslog`).

**Kafka → MinIO:**

```bash
docker compose up -d kafka minio minio-init kafka-init
docker compose up -d vtop-engine
docker compose logs -f vtop-engine   # object_uploaded → verification_passed → source_committed
# Browse results at http://localhost:9001 → bucket telemetry-data
```

**File → MinIO:**

```bash
cp examples/sample-cef.log ./data/input/auth.cef.log
docker compose up -d vtop-engine
```

The file flow is also covered without any infrastructure by [tests/integration_file_to_minio.rs](tests/integration_file_to_minio.rs) (in-memory `mock` backend).

## Example manifest

```json
{
  "protocol": "VTOP",
  "version": "0.1",
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
    "uri": "s3://telemetry-data/telemetry-data/tenant=default/source=app/format=cef/year=2026/month=06/day=18/hour=15/vtop-….cef.gz",
    "size_bytes": 924822,
    "sha256": "abc123…"
  },
  "manifest": {
    "uri": "s3://telemetry-data/…/vtop-….manifest.json",
    "sha256": "def456…"
  },
  "state": "manifest_uploaded",
  "verification_status": "not_verified"
}
```

> [!NOTE]
> The manifest is written at the `MANIFEST_UPLOADED` step — *before* the storage-side verification — so its embedded `state`/`verification_status` reflect that point in time and its hash stays stable. The **authoritative** post-verification state (`verified` → `source_committed`) lives in the state store, queryable via `vtopctl status` / `list-batches`. The `manifest.sha256` is computed over the manifest with that field blanked, so it is reproducible and tamper-evident (`verify_self_hash`).

## Metrics and efficiency

The engine measures every batch end-to-end and emits a structured `batch_metrics`
event (and a per-batch line under `vtopctl process-once`):

```text
3 records, 114 B->80 B (1.43x, 29.8% saved) in 6 ms | 500 rec/s, 0.00 MiB/s up |
stages: compress=0ms checksum=0ms put_obj=0ms put_manifest=0ms verify=0ms commit=0ms
```

Each batch records (see [metrics.rs](crates/vtop-core/src/metrics.rs)):

- **Size / transfer:** uncompressed vs compressed bytes, **compression ratio**, **% space saved** — i.e. how much smaller the object on the wire is than the source data.
- **Per-stage latency:** compress, checksum, object upload, manifest upload, verify, commit (ms). The `object_upload_ms` captures network cost (the "distance" to the bucket).
- **Throughput:** records/sec, uncompressed MiB/sec, and **effective upload MiB/sec** of the compressed object.

`vtopctl process-once --json` includes the full `metrics` object per batch. These
per-batch records are the raw input for the aggregate Prometheus-style counters
described under [Known limitations](#known-limitations) (e.g.
`bytes_uploaded_total`, `upload_latency_seconds`), which are designed but not yet
exported.

## Verification before commit

The rule is enforced at three layers:

1. The state machine permits `SourceCommitted` **only** from `Verified` (`transition()` returns `CommitBeforeVerified` otherwise).
2. `SqliteStateStore::update_batch_state` routes **every** state change through `transition()`, so the rule holds even at the persistence layer.
3. The engine pipeline ([engine.rs](crates/vtop-cli/src/engine.rs)) only calls `adapter.commit_progress(...)` *after* `mark_verified`. If verification fails, the batch is marked `FAILED` and `commit_progress` is never called. If the commit itself fails after verification, the batch stays `VERIFIED` (not lost) and recovery retries the commit.

Proven by the `state_machine.rs` unit tests and [tests/integration_replay.rs](tests/integration_replay.rs) (`verification_failure_never_commits`).

## Replay after crash

If the engine dies after `VERIFIED` but before `SOURCE_COMMITTED`, the source offset was never advanced. On restart, `Engine::recover()`:

- finds a `VERIFIED`-but-uncommitted batch and **retries the source commit** (the object is already durable and verified);
- marks any **earlier** incomplete batch `REPLAY_REQUIRED` and re-reads it from the source — source progress is never advanced for unverified data.

Proven by [tests/integration_replay.rs](tests/integration_replay.rs) (`crash_before_commit_is_replayable_then_recovers`) and [tests/integration_state_recovery.rs](tests/integration_state_recovery.rs).

## Known limitations

- **Single-part uploads.** The native S3 backend uses `put_object`; multipart upload for very large batches is a documented follow-up (`supports_multipart()` reports `false`).
- **Recovery of partial uploads.** Batches that crashed *before* `VERIFIED` are replayed from the source rather than resumed from a half-written local object (the prototype persists progress markers, not record payloads).
- **Command backends are size-limited verifiers.** `s3cmd` and `mc` verify size + existence only (reported as `backend_limited`); the native and `awscli` backends verify the stored SHA‑256.
- **Syslog timestamp parsing** is not yet extracted into the spool marker (`received_time_*` are `None`).
- **Manifest signing and S3 Object Lock** are designed for but not yet implemented (see [docs/SECURITY_MODEL.md](docs/SECURITY_MODEL.md)).
- **Metrics** are designed (Prometheus-style names) but not yet exported; the engine emits structured `tracing` events today.
- The Kafka integration test requires a live broker and is `#[ignore]` by default.

## Documentation

Full doc set in [docs/](docs/) (index: [docs/README.md](docs/README.md)):

| Document | Contents |
|----------|----------|
| [VTOP_PROTOCOL_DRAFT.md](docs/VTOP_PROTOCOL_DRAFT.md) | Normative protocol draft + conformance profiles |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | Architecture, runtime flow, and data-flow diagram |
| [SECURITY_MODEL.md](docs/SECURITY_MODEL.md) | Security model and normative rules |
| [INVENTION_DISCLOSURE_DRAFT.md](docs/INVENTION_DISCLOSURE_DRAFT.md) | Candidate-invention disclosure draft |
| [PRIOR_ART_SEARCH_PLAN.md](docs/PRIOR_ART_SEARCH_PLAN.md) | Prior-art search plan |

## License

[MIT](LICENSE) © 2026 Tamir Suliman.

# VTOP Engine

**Verified Telemetry Object Protocol Engine** — a replay-safe, manifest-driven
telemetry object transfer engine.

> **Status:** prototype / reference implementation of a *proposed protocol* and
> *proposed method*. This repository is a candidate-invention disclosure support
> package. It is **not** patented or patent-pending. See
> [docs/INVENTION_DISCLOSURE_DRAFT.md](docs/INVENTION_DISCLOSURE_DRAFT.md).

---

## What VTOP Engine is

VTOP ingests telemetry from **Kafka topics**, **log files**, and **syslog spool
files**; forms adaptive batches; compresses them; computes SHA-256 checksums;
generates a **manifest** for every object; uploads both the telemetry object and
its manifest to **S3-compatible object storage**; verifies the uploaded object
and manifest; and **commits source progress only after verification succeeds**.

## Why it exists

Most log-to-object-storage tools move bytes into a bucket. They do **not**
provide a single, source-agnostic *commit model* across Kafka, files, and syslog
spools with **mandatory manifest verification before source progress is
advanced**. VTOP makes the safety rule explicit and enforces it in code.

## Why it is not just `gzip + upload`

`gzip + upload` cannot answer: *did the object actually land intact, and is it
safe to forget the source position now?* VTOP adds:

- a **manifest** that binds the source progress marker → object SHA-256 →
  verification state (chain of custody);
- a **strongly-typed state machine** that makes premature commit impossible;
- a **replay-safe state store** so a crash never advances an unverified source;
- **verification before commit**, across pluggable storage backends.

## The core rule

```text
SOURCE_COMMITTED is forbidden until VERIFIED is true.
```

A Kafka offset, file byte offset, or syslog spool offset is **never** committed
until, in order:

1. the batch is sealed
2. the compressed object is created
3. the object checksum is calculated
4. the object is uploaded
5. the manifest is generated
6. the manifest is uploaded
7. the uploaded object is verified
8. the manifest is verified
9. **only then** source progress is committed

This is enforced in [`vtop-core/src/state_machine.rs`](crates/vtop-core/src/state_machine.rs):
the only legal predecessor of `SourceCommitted` is `Verified`, and the same
guard is re-applied at the state-store layer.

## State machine

```text
DISCOVERED → BATCHING → SEALED → COMPRESSED → CHECKSUMMED
   → OBJECT_UPLOADED → MANIFEST_UPLOADED → VERIFIED → SOURCE_COMMITTED

ANY_STATE → FAILED        FAILED → REPLAY_REQUIRED → BATCHING
```

Illegal transitions (e.g. `SEALED → SOURCE_COMMITTED`) return
`VtopError::IllegalStateTransition` / `CommitBeforeVerified`. See the tests in
`state_machine.rs` (`test_cannot_commit_from_*`).

---

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

`vtop-core` has **no** dependency on Kafka, S3, or the CLI.

---

## How each mode works

### Kafka mode
- `rdkafka` consumer with **auto-commit always disabled** (enforced in
  `build_client_config`).
- One batch = one topic + one partition + one offset range (partitions are not
  mixed by default).
- Offsets are committed via `commit_progress()` **only after** `VERIFIED`.

### File mode
- Reads append-only files line by line, tracking `path`, `inode`, byte offsets,
  file size, and mtime.
- Resumes from the last committed byte; a partial trailing line is never
  committed; replay rewinds to the start of the uncommitted range.

### Syslog spool mode
- Treats rsyslog / syslog-ng spool files as append-only files with a `spool_id`
  and byte range. External collectors own delivery; VTOP owns batching,
  checksum, manifest, upload, verification, replay state, and the commit rule.

### Manifests
- Every object has a `*.manifest.json` written alongside it. The manifest binds
  the **source progress marker** to the object's **SHA-256**, and carries its
  own self-hash (see [example below](#example-manifest-json)).

### Replay / crash recovery
- On startup the engine scans the state store for incomplete batches and maps
  each state to a recovery action (`vtop-core/src/replay.rs`):
  - `VERIFIED` but not committed → **retry the source commit**;
  - anything earlier → **mark `REPLAY_REQUIRED`** and rewind the source (source
    progress is never advanced for unverified data).

### S3 / MinIO upload
- The native backend (`aws-sdk-s3`) supports AWS S3, MinIO, and Ceph RGW via a
  custom `endpoint_url`, path-style addressing, and region. It stores the
  object SHA-256 as user metadata and verifies it with `head_object`.
- Compatibility backends shell out to `s3cmd`, `aws`, or `mc`.

---

## Build, test, run

```bash
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release
```

### CLI (`vtopctl`)

```bash
cargo run -p vtop-cli -- run            --config examples/config.yaml
cargo run -p vtop-cli -- discover       --config examples/config.yaml
cargo run -p vtop-cli -- process-once --source kafka --config examples/config.yaml
cargo run -p vtop-cli -- process-once --source file  --config examples/config.yaml
cargo run -p vtop-cli -- replay --batch-id <batch_id> --config examples/config.yaml
cargo run -p vtop-cli -- status         --config examples/config.yaml
cargo run -p vtop-cli -- list-batches   --config examples/config.yaml --json
cargo run -p vtop-cli -- verify-manifest --manifest s3://siem-data/.../batch.manifest.json --config examples/config.yaml
```

All commands support `--json` (machine-readable) and exit non-zero on failure.
Secrets are never printed.

---

## Docker lab

```bash
docker compose up -d
docker compose logs -f vtop-engine
```

Services: `kafka`, `kafka-ui` (http://localhost:8080), `minio`
(API :9000, console :9001, bucket `siem-data`), `minio-init`, `kafka-init`
(seeds topic `BLCT_1` with sample CEF events), `vtop-engine`, and an optional
`rsyslog` collector (`--profile syslog`).

### Example: Kafka → MinIO

```bash
docker compose up -d kafka minio minio-init kafka-init
docker compose up -d vtop-engine
docker compose logs -f vtop-engine     # watch object_uploaded / verification_passed / source_committed
# Browse the result at http://localhost:9001  ->  bucket siem-data
```

### Example: File → MinIO

```bash
cp examples/sample-cef.log ./data/input/BLCT.cef.log
docker compose up -d vtop-engine
docker compose logs -f vtop-engine
```

The same file flow is covered without any infrastructure by the integration
test `tests/integration_file_to_minio.rs` (using the in-memory `mock` backend).

---

## Example manifest JSON

```json
{
  "protocol": "VTOP",
  "version": "0.1",
  "batch_id": "vtop-20260618T150000Z-BLCT_1-p0-481000-482499-1a2b3c4d",
  "tenant": "default",
  "source_type": "kafka",
  "source_name": "BLCT_1",
  "format": "cef",
  "compression": "gzip",
  "record_count": 1500,
  "source_progress": {
    "source_type": "kafka",
    "topic": "BLCT_1",
    "partition": 0,
    "start_offset": 481000,
    "end_offset": 482499,
    "consumer_group": "vtop-engine"
  },
  "object": {
    "uri": "s3://siem-data/siem-data/tenant=default/source=BLCT/format=cef/year=2026/month=06/day=18/hour=15/vtop-...cef.gz",
    "size_bytes": 924822,
    "sha256": "abc123..."
  },
  "manifest": {
    "uri": "s3://siem-data/.../vtop-....manifest.json",
    "sha256": "def456..."
  },
  "state": "object_uploaded",
  "verification_status": "passed"
}
```

The manifest's `manifest.sha256` is computed over the manifest with that field
blanked, so it is reproducible and tamper-evident (`verify_self_hash`).

---

## Verification-before-commit enforcement

1. The state machine permits `SourceCommitted` **only** from `Verified`
   (`transition()` returns `CommitBeforeVerified` otherwise).
2. `SqliteStateStore::update_batch_state` routes **every** state change through
   `transition()`, so the rule holds even at the persistence layer.
3. The engine pipeline (`engine.rs`) only calls
   `adapter.commit_progress(...)` *after* `mark_verified`. If verification
   fails, the batch is marked `FAILED` and `commit_progress` is never called.
4. If the commit itself fails after verification, the batch stays `VERIFIED`
   (not lost) and recovery retries the commit.

Proven by `state_machine.rs` unit tests and
`tests/integration_replay.rs::verification_failure_never_commits`.

## Replay after crash

If the engine dies after `VERIFIED` but before `SOURCE_COMMITTED`, the source
offset was never advanced. On restart, `Engine::recover()` finds the `VERIFIED`
batch and commits it; earlier states are marked `REPLAY_REQUIRED` and re-read
from the source. Proven by
`tests/integration_replay.rs::crash_before_commit_is_replayable_then_recovers`
and `tests/integration_state_recovery.rs`.

---

## Known limitations

- **Single-part uploads.** The native S3 backend uses `put_object`; multipart
  upload for very large batches is a documented follow-up
  (`supports_multipart()` reports `false`).
- **Recovery of partial uploads.** Batches that crashed *before* `VERIFIED`
  are replayed from the source rather than resumed from a half-written local
  object (the prototype persists progress markers, not record payloads).
- **Command backends are size-limited verifiers.** `s3cmd` and `mc` verify size
  + existence only (reported as `backend_limited`); the native and `awscli`
  backends verify the stored SHA-256.
- **Syslog timestamp parsing** is not yet extracted into the spool marker
  (`received_time_*` are `None`).
- **Manifest signing and S3 Object Lock** are designed for but not yet
  implemented (see [docs/SECURITY_MODEL.md](docs/SECURITY_MODEL.md)).
- **Metrics** are designed (Prometheus-style names) but not yet exported; the
  engine emits structured `tracing` events today.
- The Kafka integration test requires a live broker and is `#[ignore]` by
  default.

---

## Documentation

- [docs/VTOP_PROTOCOL_DRAFT.md](docs/VTOP_PROTOCOL_DRAFT.md) — normative protocol draft + conformance profiles
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — architecture and data flow
- [docs/SECURITY_MODEL.md](docs/SECURITY_MODEL.md) — security model
- [docs/INVENTION_DISCLOSURE_DRAFT.md](docs/INVENTION_DISCLOSURE_DRAFT.md) — candidate-invention disclosure draft
- [docs/PRIOR_ART_SEARCH_PLAN.md](docs/PRIOR_ART_SEARCH_PLAN.md) — prior-art search plan

## License

MIT. See [LICENSE](LICENSE).

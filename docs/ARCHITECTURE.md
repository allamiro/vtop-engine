# Architecture

> Architecture of the **VTOP Engine reference implementation** (a prototype of the proposed VTOP protocol). Part of an **invention-disclosure support package**.

## Table of contents

1. [Overview](#1-overview)
2. [Design goals and non-goals](#2-design-goals-and-non-goals)
3. [Workspace layout](#3-workspace-layout)
4. [Core data types](#4-core-data-types)
5. [The pipeline](#5-the-pipeline) *(see [Engine runtime flow](#engine-runtime-flow))*
6. [Data-flow diagram](#6-data-flow-diagram)
7. [Source adapters](#7-source-adapters)
8. [Upload backends](#8-upload-backends)
9. [Checksum subsystem](#9-checksum-subsystem)
10. [Compression subsystem](#10-compression-subsystem)
11. [Partitioning and per-format buckets](#11-partitioning-and-per-format-buckets)
12. [State store schema](#12-state-store-schema)
13. [Crash recovery / replay algorithm](#13-crash-recovery--replay-algorithm)
14. [Observability and metrics](#14-observability-and-metrics)
15. [Concurrency and backpressure design](#15-concurrency-and-backpressure-design)
16. [Benchmark harness](#16-benchmark-harness)
17. [Extensibility](#17-extensibility)

---

## 1. Overview

VTOP Engine is a Rust workspace implementing a replay-safe, manifest-driven telemetry object transfer engine. It ingests telemetry from Kafka topics, files, and syslog spool files; forms adaptive batches; compresses them; computes a content checksum (SHA-256 or BLAKE3, or size-only when disabled); generates a bound manifest; uploads the telemetry object and manifest to a pluggable storage backend; verifies the uploaded object and manifest; and commits source progress only after verification succeeds.

The governing rule is: **`SOURCE_COMMITTED` is forbidden until `VERIFIED` is true.** It is enforced in three places — the state machine, the SQLite state store, and the engine pipeline.

---

## 2. Design goals and non-goals

### 2.1 Goals

| Goal | How the architecture serves it |
|------|--------------------------------|
| Replay safety | Durable state journal + verification-before-commit; no marker advances for unverified data. |
| Protocol/transport independence | `vtop-core` has no dependency on Kafka, S3, or the CLI; sources and backends sit behind traits. |
| Cross-source uniformity | One `SourceAdapter` trait and one progress-marker abstraction over Kafka/file/spool. |
| Determinism | Deterministic object naming so replay reproduces the same key. |
| Auditability | A bound, self-hashed manifest per object. |
| Backend independence | One `UploadBackend` trait with a verification contract; many implementations. |

### 2.2 Non-goals

- No fixed telemetry record schema (formats are detected/declared, not modeled).
- No transport-layer security implementation in core (delegated to source/storage clients).
- No resumption of half-written local objects (pre-`VERIFIED` crashes replay from source, not from local payload).

---

## 3. Workspace layout

The project is a Cargo workspace. `vtop-core` is protocol-independent and depends on none of the source-, backend-, or CLI-specific crates.

| Crate | Responsibility | Key dependencies |
|-------|----------------|------------------|
| `vtop-core` | Protocol-independent engine logic (state machine, batching, manifest, checksum, compression, partitioning, detection, config, replay classification, metrics). | none source/backend-specific |
| `vtop-adapters` | Source adapters behind the `SourceAdapter` trait: Kafka, file, syslog spool. | `rdkafka` |
| `vtop-upload` | Upload backends behind the `UploadBackend` trait. | `aws-sdk-s3`, external CLIs |
| `vtop-state` | Durable SQLite state journal (`sqlx`). | `sqlx` (sqlite) |
| `vtop-cli` | The `vtopctl` binary and the engine runtime that wires everything together. | all of the above |

### 3.1 `vtop-core` modules

| Module | Responsibility |
|--------|----------------|
| `batch` | Adaptive batch formation and sealing (records/bytes/age/partition-change/flush). |
| `detect` | Per-batch content-based format auto-detection (CEF, LEEF, JSON, JSONL, syslog, raw). |
| `compression` | Compression of sealed batches into immutable object bytes (gzip/zstd/none). |
| `checksum` | Content checksum (SHA-256 / BLAKE3 / disabled) over compressed bytes. |
| `manifest` | Manifest construction, serialization, validation, and self-hash. |
| `partitioning` | Object key / partition path and per-format bucket templating. |
| `state_machine` | State definitions and legal-transition enforcement. |
| `replay` | Recovery classification of incomplete batches. |
| `metrics` | Per-batch end-to-end and per-stage metric records. |
| `config` | Engine and pipeline configuration. |
| `errors` | Error types shared across the engine. |
| `types` | Shared domain types (batch id, source markers, manifest, etc.). |

---

## 4. Core data types

| Type | Role |
|------|------|
| `BatchId` | Unique, deterministic batch identifier embedding source/partition/offset range. |
| `SourceProgressMarker` | Source-agnostic position: Kafka offset, file byte offset, or syslog spool offset. |
| `Batch` | Sealed, immutable record set + covered marker range + record count + format + sizes. |
| `Manifest` | Bound document: source identity, marker range, object location, integrity metadata, self-hash. |
| `BatchState` | One of the state-machine states (see §12 / protocol §12). |
| `UploadResult` / `VerifyResult` | Backend outcomes, including verification strength (strong vs backend-limited). |
| `BatchMetrics` | Per-batch sizes, ratios, per-stage latencies, throughput. |

---

## 5. The pipeline
<a name="engine-runtime-flow"></a>

The engine drives each batch stage-by-stage; each stage advances the batch to a named state, and **each transition is persisted to `vtop-state` before proceeding**.

| # | Stage | Action | State reached |
|---|-------|--------|---------------|
| 1 | **Discover** | A source adapter discovers a source and its last committed marker. | `DISCOVERED` |
| 2 | **Batch** | Records are read forward and accumulated, then sealed adaptively. | `BATCHING` → `SEALED` |
| 3 | **Compress** | The sealed batch is compressed into immutable object bytes (gzip/zstd/none). | `COMPRESSED` |
| 4 | **Checksum** | A content checksum (SHA-256/BLAKE3, or size in disabled mode) is computed over the compressed bytes. | `CHECKSUMMED` |
| 5 | **Upload object** | The object is uploaded via the `UploadBackend`. | `OBJECT_UPLOADED` |
| 6 | **Manifest** | A manifest is generated binding object hash, integrity metadata, and covered markers; a self-hash is computed. | *(in `OBJECT_UPLOADED`)* |
| 7 | **Upload manifest** | The manifest is uploaded. | `MANIFEST_UPLOADED` |
| 8 | **Verify** | Both stored object and manifest are verified against expected checksums/metadata. | `VERIFIED` |
| 9 | **Commit** | Only after verification, the source marker is committed via the adapter. | `SOURCE_COMMITTED` |

If any stage fails, the batch transitions to `FAILED` and is later classified for replay (§13). If verification fails, `commit_progress(...)` is never called. If the commit itself fails after verification, the batch stays `VERIFIED` (not lost) and recovery retries the commit.

### 5.1 State machine

```
DISCOVERED -> BATCHING -> SEALED -> COMPRESSED -> CHECKSUMMED ->
OBJECT_UPLOADED -> MANIFEST_UPLOADED -> VERIFIED -> SOURCE_COMMITTED
```

Recovery states: `FAILED`, `REPLAY_REQUIRED`.

Legal transitions:

| From | To |
|------|-----|
| DISCOVERED | BATCHING |
| BATCHING | SEALED |
| SEALED | COMPRESSED |
| COMPRESSED | CHECKSUMMED |
| CHECKSUMMED | OBJECT_UPLOADED |
| OBJECT_UPLOADED | MANIFEST_UPLOADED |
| MANIFEST_UPLOADED | VERIFIED |
| VERIFIED | SOURCE_COMMITTED |
| ANY_STATE | FAILED |
| FAILED | REPLAY_REQUIRED |
| REPLAY_REQUIRED | BATCHING |

The `state_machine` module rejects any transition not in this table (`IllegalStateTransition` / `CommitBeforeVerified`).

---

## 6. Data-flow diagram

```
   +-----------+   +-----------+   +-------------------+
   |  Kafka    |   |   File    |   |  Syslog Spool     |   (vtop-adapters)
   | (offset)  |   | (byte off)|   |  (spool offset)   |
   +-----+-----+   +-----+-----+   +---------+---------+
         |               |                   |
         +---------------+-------------------+
                         |  records + progress markers
                         v
                 +---------------+
                 |    BATCH      |  adaptive seal (records/bytes/age/    (vtop-core: batch)
                 |               |  partition-change/flush) + detect fmt (vtop-core: detect)
                 +-------+-------+
                         v
                 +---------------+
                 |  COMPRESS     |  gzip / zstd / none                  (vtop-core: compression)
                 +-------+-------+
                         v
                 +---------------+
                 |  CHECKSUM     |  SHA-256 / BLAKE3 / disabled         (vtop-core: checksum)
                 +-------+-------+
                         v
                 +---------------+
                 | UPLOAD OBJECT |---------------------------+         (vtop-upload: UploadBackend)
                 +-------+-------+                           |
                         v                                   |
                 +---------------+                           v
                 |   MANIFEST    |                  +-----------------+
                 | (binds hash + |                  | object storage  |
                 |  markers,     |                  | (S3 / LocalFS / |
                 |  self-hash)   |                  |  CLI / mock)    |
                 +-------+-------+                  +--------+--------+
                         v                                   ^
                 +-----------------+                         |
                 | UPLOAD MANIFEST |-------------------------+
                 +-------+---------+
                         v
                 +---------------+
                 |    VERIFY     |  object + manifest verified         (vtop-upload + vtop-core)
                 |               |  (strong or backend-limited)
                 +-------+-------+
                         v
                 +-----------------+
                 | COMMIT SOURCE   |  ONLY AFTER VERIFIED                (vtop-adapters, engine-driven)
                 | PROGRESS        |
                 +-----------------+

   All state transitions persisted durably in SQLite        (vtop-state, sqlx)
```

---

## 7. Source adapters

All adapters implement `SourceAdapter`: expose records with progress markers, support forward reads from a marker, and **never self-commit** (commit is engine-driven after verification).

| Adapter | Marker | Behavior |
|---------|--------|----------|
| `kafka_source` | Kafka offset per topic+partition | `rdkafka` consumer with **auto-commit always disabled**. One batch = one topic + one partition + one offset range (partitions never mixed). Supports topic include/exclude regex and TLS/SASL config. Offsets committed only after `VERIFIED`. |
| `file_source` | File byte offset | Reads append-only files, tracking `path`, `inode`, byte offsets, size, and mtime. Line-oriented mode (a partial trailing line is never committed) **and** a whole-file mode for binary/compressed-source inputs. Resumes from the last committed byte; replay rewinds to the start of the uncommitted range. |
| `syslog_spool_source` | Spool byte offset | Treats rsyslog/syslog-ng spool files as append-only with a `spool_id` + byte range. External collectors own delivery; VTOP owns batching, checksum, manifest, upload, verification, replay state, and the commit rule. |

The marker carried by each adapter is a `SourceProgressMarker` — the unit bound into the manifest and gated by the commit rule.

---

## 8. Upload backends

All backends implement `UploadBackend` (`upload`, `verify_object`, `ensure_bucket`). The engine depends only on the trait, so it is backend-independent, and every backend is held to the same verification-before-commit contract.

| Backend | Target | Checksum verification | Multipart | Bucket create | Use |
|---------|--------|-----------------------|-----------|---------------|-----|
| `s3_native` | AWS S3 / MinIO / Ceph RGW (endpoint + path-style) via `aws-sdk-s3` | Strong (service-computed SHA-256; streamed stored-content BLAKE3) | No (`put_object`; `supports_multipart()` = false) | Yes (on-demand) | Primary production backend. |
| LocalFS | Local directory tree | Strong (streams the stored file; sidecar is not trusted) | N/A | Yes (mkdir tree) | Testing / air-gapped. |
| `awscli` | S3 via AWS CLI | Strong (downloads and hashes stored content) | Tool-dependent | Yes | Command-compatible. |
| `s3cmd` | S3 via s3cmd | Strong (downloads and hashes stored content) | Tool-dependent | Yes | Command-compatible. |
| `minio mc` | S3-compatible via `mc` | Strong (downloads and hashes stored content) | Tool-dependent | Yes | Command-compatible. |
| `mock` | In-memory | Configurable | N/A | Yes | Tests/benchmarks. |
| `mock_fail` / `mock_limited` | In-memory fault injection | Forced failure / size-only | N/A | Yes | Fault-injection tests. |

Checksums disabled by configuration (or a service unable to return a required
service checksum) produce **backend-limited** verification. Strong verification
is the default; accepting a limited result requires an explicit opt-out (see
protocol §17).

---

## 9. Checksum subsystem

- The `checksum` module computes a content checksum over the **compressed** object bytes — after compression, before upload (pipeline stage 4).
- Supported modes: **SHA-256**, **BLAKE3**, or **disabled** (size-only verification). The mode is configurable per run.
- The chosen algorithm and value (or the disabled indicator) are recorded in the manifest.
- The manifest additionally carries a reproducible **self-hash** (computed with the self-hash field blanked) for tamper-evidence (`verify_self_hash`).
- Verification compares a digest derived from the stored body (or S3's service-computed SHA-256) against the manifest before `VERIFIED` is reached. Uploader metadata and LocalFS sidecars are never strong evidence.

---

## 10. Compression subsystem

- The `compression` module compresses the sealed batch into immutable object bytes at pipeline stage 3.
- Supported algorithms: `gzip`, `zstd`, `none`. The algorithm and its extension are recorded in the manifest and determine the object key suffix (`.gz`, `.zst`, or none).
- Because checksums are computed over compressed bytes, the compressed object is the canonical artifact that is hashed, uploaded, and verified.

---

## 11. Partitioning and per-format buckets

The `partitioning` module derives a deterministic, telemetry-aware partition path from a consistent time policy. It is **general-purpose** (log analytics, observability, audit, compliance, SIEM), not tied to one domain.

```
s3://{bucket}/{prefix}/tenant={tenant}/source={source}/format={format}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{batch_id}.{format}.{compression_ext}
```

The bound manifest is stored at the same prefix as `{batch_id}.manifest.json`.

- **Extensible fields:** the path **may** be extended with `environment`, `facility`, `severity`, `retention_class`, `region`, `site`.
- **Per-format buckets:** bucket names may be templated, e.g. `bucket: "telemetry-{format}"`, with optional on-demand bucket creation via `ensure_bucket`. The fully resolved bucket and key are recorded in the manifest.

---

## 12. State store schema

`vtop-state` is a durable journal backed by **SQLite via `sqlx`**. It survives restart and is the source of truth for recovery. Conceptually it stores, per batch:

| Field group | Contents |
|-------------|----------|
| Identity | `batch_id`, tenant, source type/name, format. |
| Markers | Covered source progress marker range (start/end). |
| Object | Object/manifest URIs, sizes, checksum algorithm + value. |
| State | Current `BatchState`; transitions routed through the state machine. |
| Verification | Verification status and strength. |
| Timestamps | Discovery/seal/commit timestamps for audit and recovery. |

Crucially, `SqliteStateStore::update_batch_state` routes **every** state change through the state machine's `transition()`, so the commit rule holds even at the persistence layer (not only in the pipeline).

---

## 13. Crash recovery / replay algorithm

On startup, the `replay` module inspects `vtop-state` and maps each incomplete batch to a recovery action (`Engine::recover()`):

1. A batch in **`VERIFIED`** but not yet committed (object and manifest durably stored and verified) has its **source commit retried** (`VERIFIED → SOURCE_COMMITTED`). The verified object is never discarded.
2. A batch in any state **before `VERIFIED`** is transitioned `... → FAILED → REPLAY_REQUIRED → BATCHING` and re-driven from the last committed source progress marker.
3. Because object naming is deterministic (`{batch_id}` + partition path), re-uploading identical content yields the same object key — replay is **idempotent**.
4. A state/storage mismatch (e.g., an object present but unverified) is resolved by re-verifying or re-uploading **before** any commit.

This guarantees: no source progress marker is committed unless its object and manifest are durably stored and verified, and no committed marker is double-committed. Source progress is **never** advanced for unverified data.

---

## 14. Observability and metrics

The `metrics` module records per-batch, end-to-end measurements, emitted as structured `tracing` events (to stderr) and via `vtopctl process-once --json`:

| Category | Measurements |
|----------|--------------|
| Size / transfer | Uncompressed vs compressed bytes, compression ratio, % space saved. |
| Per-stage latency | compress, checksum, object upload, manifest upload, verify, commit (ms). |
| Throughput | records/s, uncompressed MiB/s, effective upload MiB/s of the compressed object. |

These per-batch records feed the aggregate Prometheus-style counters that are designed but not yet exported. All logs go to stderr; `--json` output is machine-readable.

---

## 15. Concurrency and backpressure design

- The pipeline advances one batch through ordered stages; **state is persisted before each transition**, so a crash at any point leaves a recoverable journal.
- Adaptive sealing (records/bytes/age) bounds in-flight batch size and provides natural backpressure: a slow upload stage delays sealing of the next batch rather than growing memory unboundedly.
- Source adapters **may** signal backpressure to the batching layer (protocol §6).
- The Kafka adapter's manual offset commit (auto-commit disabled) ensures the broker's notion of progress tracks the engine's verified-and-committed progress, not its in-memory read position.

---

## 16. Benchmark harness

A self-contained framework under `benchmarks/` drives the compiled `vtopctl` binary (it never imports engine code) to measure throughput, latency, compression, and replay across input volumes, file sizes, formats, batch settings, compression algorithms, checksum algorithms, and backends. It produces CSV plus `summary.json`/`summary.md` per run and a side-by-side matrix across runs. See `benchmarks/README.md` for usage.

---

## 17. Extensibility

The engine depends only on the `SourceAdapter` and `UploadBackend` traits, so new capabilities can be added without changing `vtop-core`.

| Add a… | Do this | Contract |
|--------|---------|----------|
| Source | Implement `SourceAdapter` (records + markers, forward reads, no self-commit, replayable). | Protocol §6. |
| Backend | Implement `UploadBackend` (`upload`, `verify_object`, `ensure_bucket`); declare verification strength. | Verification-before-commit, protocol §17. |
| Format | Extend `detect` and/or declare per stream; record format in the manifest. | Protocol §8.2. |
| Checksum | Add to `checksum`, computing over compressed bytes; record algorithm + value. | Protocol §10. |
| Partition field | Resolve deterministically from declared policy in `partitioning`. | Protocol §16. |

No extension may weaken the commit rule (protocol §13) or the replay rule (protocol §14).

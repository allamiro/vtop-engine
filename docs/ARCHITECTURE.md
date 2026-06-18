# Architecture

> Architecture of the **VTOP Engine reference implementation** (a prototype of the proposed VTOP protocol). Part of an **invention-disclosure support package**.

## Overview

VTOP Engine is a Rust workspace implementing a replay-safe, manifest-driven telemetry object transfer engine. It ingests telemetry from Kafka topics, files, and syslog spool files; forms adaptive batches; compresses them; computes SHA-256 checksums; generates manifest files; uploads telemetry objects and manifests to S3-compatible object storage; verifies the uploaded objects and manifests; and commits source progress only after verification succeeds.

The governing rule is: **`SOURCE_COMMITTED` is forbidden until `VERIFIED` is true.**

## Workspace Layout

The project is a Cargo workspace composed of the following crates:

### `vtop-core`

Protocol-independent engine logic. Contains no source-, backend-, or storage-specific code.

| Module | Responsibility |
|--------|----------------|
| `batch` | Adaptive batch formation and sealing (size/count/time thresholds). |
| `checksum` | SHA-256 computation over compressed object bytes. |
| `compression` | Compression of sealed batches into immutable object bytes. |
| `config` | Engine and pipeline configuration. |
| `errors` | Error types shared across the engine. |
| `manifest` | Manifest construction, serialization, and validation. |
| `partitioning` | Object key/partition path derivation (tenant/source/format/time). |
| `replay` | Recovery logic: classify and reconstruct incomplete batches. |
| `state_machine` | State definitions and legal-transition enforcement. |
| `types` | Shared domain types (batch id, source markers, manifest, etc.). |

### `vtop-adapters`

Source adapters implementing the `SourceAdapter` trait:

- `kafka_source` — Kafka topic consumption; progress marker = Kafka offset.
- `file_source` — File ingestion; progress marker = file byte offset.
- `syslog_spool_source` — Syslog spool ingestion; progress marker = spool offset.

The `SourceAdapter` trait exposes records with their progress markers and supports forward reads from a marker. Adapters never self-commit; commit is engine-driven.

### `vtop-upload`

Upload backends implementing the `UploadBackend` trait:

- `s3_native` — Primary backend, native S3-compatible client.
- `s3cmd` / `awscli` / `minio` — Compatibility backends shelling out to / wrapping external tools.

The `UploadBackend` trait abstracts upload and verification, so the engine is backend-independent. All backends must support verification of stored objects.

### `vtop-state`

Durable state store backed by **SQLite via `sqlx`**. Persists batch state and source progress markers so the engine can recover and replay safely after a crash.

### `vtop-cli`

The `vtopctl` binary. Drives the engine runtime, exposes configuration, and provides operational commands.

## Engine Runtime Flow

1. **Discover** — A source adapter discovers a telemetry source and its current (last committed) progress marker. Batch state begins at `DISCOVERED`.
2. **Batch** — Records are read forward and accumulated. State `BATCHING`. The batch is sealed adaptively (size/count/time) → `SEALED`.
3. **Compress** — The sealed batch is compressed into immutable object bytes → `COMPRESSED`.
4. **Checksum** — SHA-256 is computed over the compressed bytes → `CHECKSUMMED`.
5. **Upload object** — The object is uploaded via the `UploadBackend` → `OBJECT_UPLOADED`.
6. **Manifest** — A manifest is generated binding the object hash, integrity metadata, and covered source progress markers.
7. **Upload manifest** — The manifest is uploaded → `MANIFEST_UPLOADED`.
8. **Verify** — Both the uploaded object and the uploaded manifest are verified against expected checksums/metadata → `VERIFIED`.
9. **Commit** — Only after verification, the source progress marker is committed via the adapter → `SOURCE_COMMITTED`.

Each transition is persisted to `vtop-state` before proceeding.

## State Machine

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

The `state_machine` module rejects any transition not in this table.

## Crash Recovery / Replay

On startup, the `replay` module inspects `vtop-state`:

- Any batch **not** in `SOURCE_COMMITTED` is treated as incomplete.
- Incomplete batches are transitioned `... -> FAILED -> REPLAY_REQUIRED -> BATCHING` and re-driven from the last committed source progress marker.
- Because object naming is deterministic (`{batch_id}` and partition path), re-uploading identical content yields the same object key — replay is idempotent.
- A state/storage mismatch (e.g., an object present but unverified) is resolved by re-verifying or re-uploading **before** any commit.

This guarantees: no source progress marker is committed unless its object and manifest are durably stored and verified, and no committed marker is double-committed.

## Partitioning Scheme

Objects are written with a deterministic, SIEM-aware partition path:

```
s3://{bucket}/{prefix}/tenant={tenant}/source={source}/format={format}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{batch_id}.{format}.{compression_ext}
```

The bound manifest is stored at the same prefix:

```
s3://{bucket}/{prefix}/.../hour={hh}/{batch_id}.manifest.json
```

Partition components are derived by the `partitioning` module from a consistent time policy, enabling retention and downstream analytics.

## Data Flow Diagram

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
                 |    BATCH      |  adaptive seal (size/count/time)   (vtop-core: batch)
                 +-------+-------+
                         v
                 +---------------+
                 |  COMPRESS     |                                    (vtop-core: compression)
                 +-------+-------+
                         v
                 +---------------+
                 |  CHECKSUM     |  SHA-256 over compressed bytes     (vtop-core: checksum)
                 +-------+-------+
                         v
                 +---------------+
                 | UPLOAD OBJECT |---------------------------+        (vtop-upload: UploadBackend)
                 +-------+-------+                           |
                         v                                   |
                 +---------------+                           v
                 |   MANIFEST    |                  +-----------------+
                 | (binds hash + |                  | S3-compatible   |
                 |  markers)     |                  | object storage  |
                 +-------+-------+                  +--------+--------+
                         v                                   ^
                 +-----------------+                         |
                 | UPLOAD MANIFEST |-------------------------+
                 +-------+---------+
                         v
                 +---------------+
                 |    VERIFY     |  object + manifest verified        (vtop-upload + vtop-core)
                 +-------+-------+
                         v
                 +-----------------+
                 | COMMIT SOURCE   |  ONLY AFTER VERIFIED               (vtop-adapters, engine-driven)
                 | PROGRESS        |
                 +-----------------+

   All state transitions persisted durably in SQLite        (vtop-state, sqlx)
```

## Backend Independence

The engine depends only on the `SourceAdapter` and `UploadBackend` traits. New sources or storage backends can be added without changing `vtop-core`, and every backend is held to the same verification-before-commit contract.

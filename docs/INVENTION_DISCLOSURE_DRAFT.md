# Invention Disclosure Draft

> This document is part of an **invention-disclosure support package** and supports a **candidate invention**. It is an internal draft for technical and legal review.

## Table of contents

1. [Title](#title)
2. [Background and technical field](#background-and-technical-field)
3. [Problem](#problem)
4. [Problem statement (detailed)](#problem-statement-detailed)
5. [Invention](#invention)
6. [Summary of the proposed method](#summary-of-the-proposed-method)
7. [Detailed description](#detailed-description)
8. [Enumerated novel aspects](#enumerated-novel-aspects)
9. [Technical Advantages](#technical-advantages)
10. [Alternatives and variations considered](#alternatives-and-variations-considered)
11. [Reduction to practice](#reduction-to-practice)
12. [Main Claim Candidate](#main-claim-candidate)
13. [Potential Claim Families](#potential-claim-families)

## Title

**Replay-Safe Manifest-Driven Telemetry Object Transfer System**

## Background and technical field

The technical field is **telemetry/log data transfer and archival to object storage**, spanning streaming sources (e.g. Apache Kafka), file-based sources, and syslog spool files produced by collectors such as rsyslog and syslog-ng. Organizations move high volumes of logs into S3-compatible object storage for analytics, observability, audit, and compliance retention. The operational difficulty is not moving bytes — many tools do that — but **knowing when it is safe to forget the source position**, i.e. to advance a Kafka offset, a file byte offset, or a spool position, without risking data loss or duplicate transfer when failures occur mid-flight.

## Problem

Existing log-to-object-storage tools move telemetry into object storage, but they do not provide a unified source-agnostic commit model across Kafka, files, and syslog spools with mandatory manifest verification before source progress is advanced.

In current tooling:

- Source acknowledgment / offset commit is frequently decoupled from durable, verified storage of the uploaded data.
- There is no uniform abstraction that treats Kafka offsets, file byte offsets, and syslog spool positions as interchangeable source progress markers under a single commit discipline.
- Integrity manifests, when present, are not consistently bound to the uploaded object and used as a gating precondition for advancing source progress.

This leaves gaps in replay safety, auditability, and chain of custody when failures occur mid-transfer.

## Problem statement (detailed)

The following concrete failure modes motivate the proposed method:

| Failure mode | Consequence in conventional tooling |
|--------------|-------------------------------------|
| Acknowledge/commit before durable write | Data loss if the upload had not actually landed. |
| Acknowledge/commit before verifying integrity | Silent corruption archived as authoritative. |
| Crash between upload and acknowledgment | Either data loss (over-eager commit) or duplicates (re-upload without idempotent naming). |
| Per-source ad hoc commit logic | No single safety rule across Kafka, files, and spools; inconsistent guarantees. |
| No bound manifest | No record tying the stored object's hash to the exact source positions it covers; weak chain of custody. |

The unmet need is a **single commit discipline** that (a) abstracts heterogeneous source positions uniformly, (b) gates commit on a bound, verified manifest, and (c) recovers deterministically after a crash.

## Invention

A transfer engine that forms adaptive batches from multiple telemetry source types, writes compressed immutable objects and cryptographic manifests to S3-compatible storage, verifies both object and manifest integrity, and commits source progress only after verification.

The proposed method enforces a strict ordering in which a source progress marker (Kafka offset, file byte offset, or syslog spool offset) is never committed until the corresponding compressed telemetry object **and** its manifest have been durably written to object storage and independently verified.

## Summary of the proposed method

In operational terms, the proposed method performs, per batch:

1. Discover a telemetry source and its last committed progress marker.
2. Read records forward and accumulate them into a single-source adaptive batch; seal on size/count/age/partition-change/flush.
3. Compress the sealed batch into immutable object bytes.
4. Compute a content checksum over the compressed bytes.
5. Upload the object to a pluggable storage backend.
6. Generate a manifest binding the object hash, integrity metadata, and the covered source progress markers; compute a reproducible self-hash.
7. Upload the manifest.
8. Verify the stored object and the stored manifest.
9. **Only after verification**, commit the source progress marker.

A durable state journal records every transition so that a crash at any step is recoverable without data loss or double-commit.

## Detailed description

This section maps the proposed method onto the implemented reference system.

### Source adapters and the progress-marker abstraction

A uniform source-adapter interface exposes records together with a **source progress marker** and supports forward reads, while never self-committing. Three concrete adapters are implemented:

- **Kafka** — marker is the offset per topic+partition; the consumer runs with **auto-commit disabled**, and a batch maps to exactly one topic+partition+offset range.
- **File** — marker is a byte offset; the adapter tracks path, inode, size, and mtime, supports line-oriented and whole-file modes, and never commits a partial trailing line.
- **Syslog spool** — marker is a spool offset (`spool_id` + byte range) over rsyslog/syslog-ng spool files treated as append-only.

These three otherwise-incompatible positions are made **interchangeable** under one commit discipline.

### Adaptive batching

Records accumulate into a single-source batch sealed by configurable triggers: `max_records`, `max_bytes`, `max_batch_age_seconds`, partition change, or manual/shutdown flush. Each batch carries a unique, deterministic `batch_id`.

### Format detection

The payload format is detected **per batch** from content (CEF, LEEF — recognized even when syslog/timestamp-wrapped — JSON, JSONL, syslog by PRI header, or raw), or declared explicitly per stream (declaration overrides detection). Multiple formats may flow through one engine concurrently, each labeled in its manifest and object extension.

### Compression and checksum

The sealed batch is compressed (gzip, zstd, or none). A content checksum (**SHA-256**, **BLAKE3**, or an explicit size-only disabled mode) is computed over the **compressed** bytes — so the canonical hashed artifact is exactly the bytes that are stored and later verified.

### Manifest binding (chain of custody)

A manifest binds the **source progress marker range → object checksum → verification state**, plus object/manifest URIs, sizes, compression, format, and timestamps. The manifest carries a reproducible **self-hash** (computed with the self-hash field blanked) for tamper-evidence.

### State machine and verification-before-commit

A strongly-typed state machine (`DISCOVERED → BATCHING → SEALED → COMPRESSED → CHECKSUMMED → OBJECT_UPLOADED → MANIFEST_UPLOADED → VERIFIED → SOURCE_COMMITTED`, plus `FAILED` and `REPLAY_REQUIRED`) enforces that `SOURCE_COMMITTED` is reachable **only** from `VERIFIED`. The rule is enforced redundantly in the state machine, in the durable state store (every transition routed through the state machine), and in the engine pipeline.

### Replay and recovery

On restart, incomplete batches are classified: a `VERIFIED`-but-uncommitted batch retries its commit (the durable, verified object is never discarded); any pre-`VERIFIED` batch is marked `REPLAY_REQUIRED` and re-read from the source. Deterministic naming makes re-upload idempotent. Source progress is never advanced for unverified data.

### Per-format buckets and partitioning

Objects are written under a deterministic, telemetry-aware partition path (tenant/source/format/time, with extensible fields), optionally into **per-format buckets** via templated bucket names with optional on-demand bucket creation.

### Pluggable backends

A single upload/verification interface abstracts the storage target: native S3 (AWS S3 / MinIO / Ceph RGW), a local-filesystem backend (object tree on local storage, for testing/air-gapped use), command-compatible backends (s3cmd/awscli/minio mc), and in-memory mock and fault-injection backends. Backends declare verification strength (cryptographic vs. backend-limited size/existence).

## Enumerated novel aspects

The aspects below are presented as candidate distinguishing features; novelty/non-obviousness is for counsel to determine.

1. A **single source-progress-marker abstraction** unifying Kafka offsets, file byte offsets, and syslog spool offsets under one commit discipline.
2. A **bound, self-hashed manifest** tying the stored object's hash to the exact covered source positions, used as a **gating precondition** for advancing source progress.
3. **Commit strictly after dual verification** of both object and manifest, enforced redundantly at three layers (state machine, state store, pipeline).
4. A **deterministic, idempotent recovery model** distinguishing `VERIFIED`-retry-commit from pre-`VERIFIED` replay.
5. **Per-batch format detection** allowing heterogeneous formats through one engine, each labeled and bucketed by format.
6. **Backend-independent verification** with explicit declaration of verification strength.

## Technical Advantages

- **Deterministic replay** — uncommitted batches can be reconstructed and re-driven without data loss or double-commit.
- **Auditability** — every transferred object is described by a manifest recording its integrity metadata and covered source positions.
- **Object-level chain of custody** — the manifest binds the object hash to the source progress markers it covers.
- **Telemetry-aware archival partitioning** — objects are partitioned by tenant, source, format, and time for downstream analytics and retention (log analytics, observability, audit, compliance, SIEM, and similar; not tied to any single domain).
- **Multi-source progress abstraction** — Kafka offsets, file byte offsets, and syslog spool positions are handled under one uniform commit model.
- **Backend-independent object storage support** — a pluggable backend interface supports multiple S3-compatible implementations behind one verification contract.

## Alternatives and variations considered

| Variation | Description |
|-----------|-------------|
| Checksum algorithm | SHA-256, BLAKE3, or explicit size-only/disabled mode; the binding and gating are algorithm-independent. |
| Compression | gzip, zstd, or none; the hashed/verified artifact is the compressed object in every case. |
| Bucket strategy | Single bucket with partition prefix, or per-format templated buckets with optional on-demand creation. |
| Backend | Native S3, local filesystem (air-gapped), or command-compatible external tools; verification strength declared per backend. |
| Source mode | Line-oriented vs. whole-file ingestion for files; manual-offset Kafka; spool byte ranges. |
| Verification strength | Strong cryptographic verification vs. backend-limited size/existence, explicitly reported. |
| Future strengthening | Optional manifest signing and object-lock/WORM immutability (designed, not yet implemented). |

These variations are described so that the candidate invention is not read as limited to a single algorithm, backend, or bucket scheme.

## Reduction to practice

The proposed method is **reduced to practice** in the VTOP Engine reference implementation:

- A Rust workspace (`vtop-core`, `vtop-adapters`, `vtop-upload`, `vtop-state`, `vtop-cli`) implements the adapters, batching, compression, checksums (SHA-256/BLAKE3/disabled), bound manifests, the state machine, verification-before-commit, replay/recovery, per-format buckets, and pluggable backends described above.
- The commit rule and recovery behavior are exercised by automated tests (state-machine transition tests; replay/recovery integration tests confirming verification failure never commits and that a crash before commit is replayable and recovers).
- A separate benchmark harness drives the compiled binary to produce **CSV + JSON evidence** of throughput, latency, compression, and replay across input volumes, file sizes, formats, batch settings, compression and checksum algorithms, and backends — corroborating that the method operates across the claimed dimensions.

## Main Claim Candidate

A method comprising discovering telemetry sources, forming adaptive batches, generating a manifest containing source progress markers and object integrity metadata, uploading the compressed object and manifest to object storage, verifying integrity, and committing source progress only after successful verification.

## Potential Claim Families

1. **Manifest-bound telemetry object archival.**
   - 1a. Binding object hash to covered source positions in a stored manifest.
   - 1b. Reproducible manifest self-hash for tamper-evidence.
2. **Replay-safe source progress commit.**
   - 2a. Commit gated on dual (object + manifest) verification.
   - 2b. Redundant enforcement at state-machine, state-store, and pipeline layers.
3. **Multi-source progress abstraction for Kafka, file, and syslog spool sources.**
   - 3a. Uniform source-progress-marker type across heterogeneous sources.
4. **Telemetry-aware object partitioning with retention metadata** (applicable to log analytics, observability, audit, compliance, and SIEM).
   - 4a. Per-format bucket templating with optional on-demand creation.
5. **Recovery algorithm that reconstructs incomplete batches from manifest/state mismatch.**
   - 5a. `VERIFIED`-retry-commit vs. pre-`VERIFIED` replay distinction.
   - 5b. Deterministic, idempotent re-upload naming.
6. **Pluggable S3-compatible backend with integrity verification before source acknowledgment.**
   - 6a. Declared verification strength (cryptographic vs. backend-limited).
7. **Manifest/object hash binding for chain-of-custody verification.**
8. **Cross-source replay model using Kafka offsets, file byte offsets, and spool positions.**

---

*This document is an internal invention-disclosure draft for technical and legal review. It is not a patent application, does not establish patent rights, and should be reviewed by qualified patent counsel before filing or public disclosure.*

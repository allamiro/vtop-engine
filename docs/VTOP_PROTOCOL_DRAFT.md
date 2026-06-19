# VTOP Protocol Draft

**Verified Telemetry Object Protocol (VTOP)**

Status: Draft / proposed protocol
Version: 0.1 (reference implementation specification)

> This document describes a **proposed protocol** and accompanies a **reference implementation** ("VTOP Engine"). It is a draft for technical review and is part of an **invention-disclosure support package**. It does not describe a shipped standard.

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** in this document are to be interpreted as normative requirements describing conformant behavior of a VTOP implementation.

---

## Table of contents

1. [Abstract](#1-abstract)
2. [Scope and goals](#2-scope-and-goals)
3. [Terminology](#3-terminology)
4. [Conformance language](#4-conformance-language)
5. [Reference model and actors](#5-reference-model-and-actors)
6. [Source adapter contract](#6-source-adapter-contract)
7. [Progress markers](#7-progress-markers)
8. [Batch object](#8-batch-object)
9. [Compression](#9-compression)
10. [Checksum](#10-checksum)
11. [Manifest object](#11-manifest-object)
12. [State machine](#12-state-machine)
13. [Commit rule](#7-commit-rule) *(see §13)*
14. [Replay and recovery rule](#14-replay-and-recovery-rule)
15. [Object and bucket naming](#15-object-and-bucket-naming)
16. [Partitioning](#16-partitioning)
17. [Verification semantics](#17-verification-semantics)
18. [Security considerations](#18-security-considerations)
19. [Conformance profiles](#19-conformance-profiles)
20. [Extensibility](#20-extensibility)
21. [References](#21-references)

> Anchor compatibility: the canonical commit rule is normatively defined in [§13 Commit rule](#13-commit-rule). The historical anchor [`#7-commit-rule`](#7-commit-rule) is preserved below for external links.

---

## 1. Abstract

VTOP defines a **replay-safe, manifest-driven** method for transferring telemetry data from one or more heterogeneous telemetry sources into S3-compatible object storage. Its central guarantee is a strict commit discipline: a source's read position is never advanced until the data derived from that position has been compressed into an immutable object, described by a cryptographically bound manifest, durably written to object storage, and independently verified there.

The proposed protocol unifies three otherwise unrelated notions of "where am I in this source" — a Kafka offset, a file byte offset, and a syslog spool offset — behind a single **source progress marker** abstraction, and subjects all of them to one commit rule. This yields deterministic replay, object-level chain of custody, and auditable archival across mixed telemetry feeds.

---

## 2. Scope and goals

### 2.1 In scope

The proposed protocol specifies:

- How telemetry is ingested from heterogeneous sources (Kafka topic partitions, files, and syslog spool files).
- How a uniform **source progress marker** abstracts each source's read position.
- How telemetry records are grouped into **adaptive batches** sealed by configurable thresholds.
- How sealed batches are compressed into immutable **telemetry objects**.
- How cryptographic checksums and bound **manifest objects** are generated.
- How objects and manifests are uploaded to, and verified in, object storage.
- The strict ordering rule that source progress **MUST NOT** be committed until uploaded objects and manifests are durably written and verified.
- How an implementation recovers and replays after a crash without data loss or double-commit.

### 2.2 Goals

| Goal | Description |
|------|-------------|
| Replay safety | No source position is advanced for data that is not durably stored and verified. |
| Cross-source uniformity | One commit model spans Kafka offsets, file byte offsets, and syslog spool offsets. |
| Auditability | Every object is described by a bound manifest recording integrity metadata and covered positions. |
| Chain of custody | The manifest binds the object's hash to the source progress markers it covers. |
| Backend independence | The object-storage backend is pluggable behind one verification contract. |
| Determinism | Object naming is deterministic so replay reproduces the same object key. |

### 2.3 Out of scope (non-goals)

VTOP does **not** define:

- The internal record schema of telemetry payloads.
- The choice of compression algorithm beyond requiring that one be declared.
- The object storage backend implementation beyond the verification contract.
- Transport security mechanisms (delegated to the underlying source/storage transports and to [SECURITY_MODEL.md](SECURITY_MODEL.md)).

---

## 3. Terminology

| Term | Definition |
|------|------------|
| **Telemetry source** | An origin of telemetry records: a Kafka topic partition, a file, or a syslog spool file. |
| **Source progress marker** | A durable position into a source: a Kafka offset, a file byte offset, or a syslog spool offset. |
| **Batch** | An ordered, bounded collection of telemetry records selected from a single source for transfer. |
| **Adaptive batch** | A batch whose sealing is governed by size, record-count, and/or time thresholds, partition change, or explicit flush. |
| **Telemetry object** | A compressed, immutable representation of a sealed batch, written to object storage. |
| **Manifest** | A structured document describing a telemetry object, its integrity metadata, and the source progress markers it covers. |
| **Format** | The detected or declared payload encoding of a batch (e.g. CEF, LEEF, JSON, JSONL, syslog, raw). |
| **Verification** | Confirmation that a durably stored object/manifest matches its declared checksum and metadata. |
| **Commit** | Advancing a source progress marker so that covered records will not be re-read in normal operation. |
| **Replay** | Re-reading source records from a previously committed or uncommitted progress marker after a failure. |
| **Backend** | A pluggable implementation of the upload/verification contract for an object-storage target. |

---

## 4. Conformance language

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** are normative. A **conformant implementation** is one that satisfies every **MUST**/**MUST NOT** requirement of [VTOP-Core](#191-vtop-core-minimum) and of any conformance profile it advertises (§19). An implementation **MUST NOT** advertise a profile it does not fully satisfy.

---

## 5. Reference model and actors

```
   Telemetry sources            VTOP engine                Object storage
  +----------------+      +----------------------+      +-----------------+
  | Kafka / File / | ---> | adapter -> batch ->  | ---> | telemetry obj   |
  | Syslog spool   |      | compress -> checksum |      | + bound manifest|
  +----------------+      | -> upload -> verify  |      +-----------------+
        ^                 | -> COMMIT (gated)    |              ^
        |                 +----------+-----------+              |
        |  commit only after VERIFIED |   verify against stored |
        +-----------------------------+-------------------------+
```

Actors:

| Actor | Role |
|-------|------|
| **Source adapter** | Reads records and emits them with progress markers; never self-commits. |
| **Batching layer** | Accumulates records into a single-source batch and seals adaptively. |
| **Object pipeline** | Compresses, checksums, and uploads the object and manifest. |
| **Verifier** | Confirms the stored object and manifest match expected integrity metadata. |
| **Commit controller** | Advances the source progress marker only after verification. |
| **State store** | Durably journals state transitions for recovery. |
| **Upload backend** | Stores and verifies objects in an object-storage target. |

---

## 6. Source adapter contract

A VTOP source adapter abstracts a telemetry source behind a uniform interface. A conformant source adapter:

- **MUST** expose telemetry records together with an associated source progress marker.
- **MUST** support reading forward from a given source progress marker.
- **MUST NOT** advance (commit) a source progress marker on its own; commit is driven exclusively by the engine after verification (see §13).
- **MUST** support idempotent re-reads from an uncommitted marker (replay safety).
- **SHOULD** expose source identity metadata (tenant, source name, format) for object naming and partitioning.
- **MAY** support backpressure signaling to the batching layer.

Defined source adapter types:

| Adapter | Progress marker | Notes |
|---------|-----------------|-------|
| **Kafka source** | Kafka offset per topic partition | Auto-commit **MUST** be disabled; one batch = one topic + one partition + one offset range. **MAY** support topic include/exclude filtering and TLS/SASL. |
| **File source** | Byte offset into the file | **SHOULD** track path, inode, size, and mtime; **MAY** support line-oriented and whole-file (binary/compressed-source) reads. A partial trailing line **MUST NOT** be committed. |
| **Syslog spool source** | Spool offset (spool_id + byte range) | Treats rsyslog/syslog-ng spool files as append-only; external collectors own delivery, VTOP owns batching through commit. |

---

## 7. Progress markers

A **source progress marker** is the unit bound to objects and gated by the commit rule. It is source-agnostic at the protocol level and concrete at the adapter level:

| Source | Marker components |
|--------|-------------------|
| Kafka | topic, partition, start_offset, end_offset, consumer_group |
| File | path, inode, start byte offset, end byte offset (size/mtime as context) |
| Syslog spool | spool_id, start byte offset, end byte offset |

A conformant implementation:

- **MUST** treat the marker as the authoritative record of which source positions a batch covers.
- **MUST** record both the start and end marker of the range a batch covers.
- **MUST NOT** advance a marker except through the commit rule (§13).
- **MUST** preserve enough marker context to re-read an uncommitted range after a crash.

---

## 8. Batch object

A batch is the unit of transfer. A conformant implementation:

- **MUST** assign each batch a unique `batch_id`.
- **MUST** seal a batch before compression; a sealed batch is immutable.
- **MUST** record the source progress markers covered by the batch (start and end positions).
- **SHOULD** seal batches adaptively based on configurable thresholds.
- **MUST NOT** include records from more than one logical source in a single batch.

### 8.1 Adaptive sealing

A conformant implementation **SHOULD** seal a batch when **any** configured trigger fires:

| Trigger | Description |
|---------|-------------|
| `max_records` | Record count reaches the configured ceiling. |
| `max_bytes` | Accumulated uncompressed bytes reach the configured ceiling. |
| `max_batch_age_seconds` | The oldest buffered record reaches the configured age. |
| Partition change | The source partition/identity changes (forces a one-source-per-batch boundary). |
| Manual / shutdown flush | An operator or shutdown request forces a seal. |

A sealed batch carries at minimum: `batch_id`, source identity, covered progress markers, record count, format, and uncompressed byte size.

### 8.2 Format

The batch **format** **MAY** be declared explicitly per stream or **MAY** be auto-detected per batch from content (§17.3 and §20). Different formats **MAY** flow through one engine simultaneously; an explicit declaration **MUST** override detection.

---

## 9. Compression

- A conformant implementation **MUST** declare the compression algorithm applied to each object.
- A conformant implementation **MUST** support at least one of: `gzip`, `zstd`, or `none`.
- The declared algorithm and its file extension **MUST** be recorded in the manifest (§11).
- Compression **MUST** be applied to the sealed batch to produce the immutable object bytes; the object checksum (§10) **MUST** be computed over the compressed bytes.

| Algorithm | Extension | Notes |
|-----------|-----------|-------|
| `gzip` | `.gz` | Broadly compatible. |
| `zstd` | `.zst` | Higher ratio / throughput tradeoffs. |
| `none` | *(none)* | Raw object; declared explicitly. |

---

## 10. Checksum

- A conformant implementation **MUST** compute a content checksum over the **compressed** telemetry object bytes, using one of: **SHA-256**, **BLAKE3**, or a **disabled** mode in which size-only verification is used.
- The selected checksum algorithm and value (or the explicit "disabled" indication) **MUST** be recorded in the manifest.
- Verification **MUST** compare the checksum (or size, in disabled mode) of the durably stored object against the manifest.
- An implementation **MUST NOT** transition to `VERIFIED` if the stored object fails the configured check.
- An implementation **MAY** additionally record an uncompressed-content checksum.

| Mode | Strength | Verification basis |
|------|----------|--------------------|
| `SHA-256` | Cryptographic | Stored object hash vs. manifest hash. |
| `BLAKE3` | Cryptographic | Stored object hash vs. manifest hash. |
| disabled | Size-only | Stored object size vs. declared size (weaker; see §17). |

The manifest itself additionally carries a **self-hash** for tamper-evidence (§11.2).

---

## 11. Manifest object

A manifest is a structured JSON document bound to exactly one telemetry object.

### 11.1 Required and optional fields

A conformant manifest:

- **MUST** include the `batch_id`.
- **MUST** include source identity (tenant, source, format).
- **MUST** include the covered source progress markers (start and end).
- **MUST** include the telemetry object's storage location (bucket, key/URI).
- **MUST** include the telemetry object's integrity metadata: checksum algorithm, checksum value (or disabled indicator), compressed byte size, and uncompressed byte size.
- **MUST** include the compression algorithm and extension.
- **MUST** include a creation timestamp.
- **MUST NOT** include credentials, secrets, or authentication material (§18).
- **SHOULD** include retention/partitioning metadata to support archival policies.
- **MAY** include a manifest signature when manifest signing is enabled.

### 11.2 Field reference

| Field | Type | Requirement | Description |
|-------|------|-------------|-------------|
| `protocol` | string | MUST | Constant `"VTOP"`. |
| `version` | string | MUST | Protocol version (e.g. `"0.1"`). |
| `batch_id` | string | MUST | Unique batch identifier. |
| `tenant` | string | MUST | Tenant/source-owner identity. |
| `source_type` | enum | MUST | `kafka` \| `file` \| `syslog`. |
| `source_name` | string | MUST | Logical source name. |
| `format` | string | MUST | Detected/declared format (cef, leef, json, jsonl, syslog, raw, …). |
| `compression` | enum | MUST | `gzip` \| `zstd` \| `none`. |
| `record_count` | integer | MUST | Records in the batch. |
| `source_progress` | object | MUST | Marker range (start/end) for the source type. |
| `object.uri` | string | MUST | Object storage URI/key. |
| `object.size_bytes` | integer | MUST | Compressed object size. |
| `object.uncompressed_bytes` | integer | SHOULD | Uncompressed size. |
| `object.checksum_algorithm` | enum | MUST | `sha256` \| `blake3` \| `disabled`. |
| `object.sha256` / checksum value | string | MUST* | Hash value; required unless checksum disabled. |
| `manifest.uri` | string | MUST | Manifest storage URI/key. |
| `manifest.sha256` (self-hash) | string | MUST | Self-hash computed with this field blanked. |
| `state` | enum | SHOULD | Lifecycle state at manifest-write time. |
| `verification_status` | enum | SHOULD | Status at manifest-write time. |
| `partition` / retention metadata | object | SHOULD | Time/retention partitioning context. |
| `signature` | string | MAY | Manifest signature when signing enabled. |

The manifest binds the object's hash to the source progress markers, establishing an object-level **chain of custody**. The **self-hash** is computed over the manifest with the self-hash field blanked, so it is reproducible and tamper-evident.

> The manifest is written at the `MANIFEST_UPLOADED` step, before storage-side verification, so its embedded `state`/`verification_status` reflect that point in time and its hash stays stable. The authoritative post-verification state (`VERIFIED` → `SOURCE_COMMITTED`) lives in the state store.

---

## 12. State machine

Each batch progresses through a defined state machine. A conformant implementation **MUST** enforce these states and **MUST** reject illegal transitions.

States:

```
DISCOVERED → BATCHING → SEALED → COMPRESSED → CHECKSUMMED →
OBJECT_UPLOADED → MANIFEST_UPLOADED → VERIFIED → SOURCE_COMMITTED
```

plus terminal/recovery states `FAILED` and `REPLAY_REQUIRED`.

Legal transitions (and **only** these) are permitted:

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

```
                 +--------------------------------------------------+
                 |                                                  |
 DISCOVERED -> BATCHING -> SEALED -> COMPRESSED -> CHECKSUMMED ->    |
 OBJECT_UPLOADED -> MANIFEST_UPLOADED -> VERIFIED -> SOURCE_COMMITTED|
        |  |  |  |  |  |  |  |                                      |
        +--+--+--+--+--+--+--+--> FAILED -> REPLAY_REQUIRED --------+
                                   (from ANY_STATE)
```

A conformant implementation **MUST** persist state transitions durably so that recovery after a crash is possible (§14).

---

## 13. Commit rule

This is the **core normative rule** of VTOP:

> **A source progress marker MUST NOT be committed until the telemetry object and its manifest have been durably written and verified in object storage.**

Concretely, a source progress marker **MUST NOT** transition to `SOURCE_COMMITTED` until **all** of the following have completed in order:

1. The batch is **sealed**.
2. A **compressed** telemetry object has been created.
3. The object **checksum** has been calculated (or size recorded, in disabled mode).
4. The object has been **uploaded** to object storage.
5. The **manifest** has been generated.
6. The manifest has been **uploaded** to object storage.
7. The uploaded **object** has been **verified**.
8. The uploaded **manifest** has been **verified**.
9. **Only then** is the source progress marker committed.

A conformant implementation **MUST NOT** commit source progress in any other order. `SOURCE_COMMITTED` **MUST NOT** be reachable unless `VERIFIED` is true.

<a name="7-commit-rule"></a>
> The historical anchor `#7-commit-rule` resolves here for backward compatibility with external links.

---

## 14. Replay and recovery rule

A conformant implementation:

- **MUST** persist batch state durably such that, after a crash, every batch can be classified as committed or not-yet-committed.
- **MUST** treat any batch that did not reach `SOURCE_COMMITTED` as eligible for recovery.
- On recovery, a batch already in `VERIFIED` (object and manifest durably written and verified, but progress not yet committed) **MAY** complete by committing source progress (`VERIFIED → SOURCE_COMMITTED`); its object **MUST NOT** be discarded.
- On recovery, a batch in any state *before* `VERIFIED` **MUST** be transitioned to `FAILED` and then `REPLAY_REQUIRED`, and re-entered at `BATCHING`; its uncommitted source range **MUST** remain replayable.
- **MUST NOT** double-commit a source progress marker that was already committed.
- **MUST** be safe to re-run from the last committed source progress marker without data loss; re-uploaded objects with identical content **SHOULD** be idempotent (same `batch_id` and naming yields the same object key).

A mismatch between persisted state and the contents of object storage (e.g., an object exists but is not verified) **MUST** be resolved by re-verifying or re-uploading before any commit.

---

## 15. Object and bucket naming

### 15.1 Object key

Telemetry objects **MUST** be named using the following deterministic scheme:

```
s3://{bucket}/{prefix}/tenant={tenant}/source={source}/format={format}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{batch_id}.{format}.{compression_ext}
```

The corresponding manifest **MUST** be stored at the same prefix:

```
s3://{bucket}/{prefix}/tenant={tenant}/source={source}/format={format}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{batch_id}.manifest.json
```

- Time partition components (`year/month/day/hour`) **SHOULD** be derived from a consistent event-time or batch-seal-time policy declared by the implementation.
- `batch_id` **MUST** be unique within its partition prefix.
- The naming scheme **MUST** be deterministic so that replay produces the same object key for the same batch.

### 15.2 Per-format bucket templating

A conformant implementation **MAY** route objects to **per-format buckets** via a templated bucket name, for example `bucket: "telemetry-{format}"`. When bucket templating is used:

- The `{format}` (and other declared template fields) **MUST** be resolved deterministically.
- The implementation **MAY** create the target bucket on demand (see §18 for the least-privilege implications of `CreateBucket`).
- The manifest **MUST** record the fully resolved bucket and key.

---

## 16. Partitioning

The partition path is **observability/SIEM-aware but general-purpose** — it serves log analytics, observability, audit, and compliance archival, and is not tied to any single domain. The base partition fields are:

| Field | Source |
|-------|--------|
| `tenant` | Source identity. |
| `source` | Logical source name. |
| `format` | Detected/declared format. |
| `year` / `month` / `day` / `hour` | Consistent time policy. |

A conformant implementation **MAY** extend the partition path with additional declared fields such as `environment`, `facility`, `severity`, `retention_class`, `region`, or `site`. Partition components **MUST** be derived deterministically from declared policy so that replay reproduces the same path.

---

## 17. Verification semantics

### 17.1 Strong verification

When the configured checksum mode is cryptographic (`SHA-256` or `BLAKE3`) and the backend can read back the stored object's hash, verification is **strong**: the stored object's hash **MUST** equal the manifest hash before `VERIFIED`.

### 17.2 Backend-limited verification

Some backends can confirm only object **existence and size**, not a content hash. Such verification is **backend-limited**: a conformant implementation **MUST** report it as backend-limited and **MUST NOT** represent it as cryptographic verification. Backend-limited verification still gates the commit rule, but provides only size/existence assurance.

| Verification class | Basis | Strength |
|--------------------|-------|----------|
| Strong | Stored content hash == manifest hash (SHA-256/BLAKE3) | Cryptographic integrity. |
| Backend-limited | Stored object exists and size matches | Existence/size only. |
| Disabled checksum | Size-only by configuration | Weakest; explicit opt-in. |

### 17.3 Manifest verification

The stored **manifest** **MUST** also be verified (including its self-hash) before commit. The self-hash **MUST** be reproducible: computed over the manifest with the self-hash field blanked.

---

## 18. Security considerations

- Credentials and secrets **MUST NOT** be embedded in manifests or telemetry objects.
- Credentials **MUST NOT** be written to logs.
- Transport to Kafka and to S3-compatible endpoints **SHOULD** be protected with TLS; Kafka authentication **SHOULD** support SASL/SCRAM and mTLS.
- Object storage permissions **SHOULD** follow least privilege; on-demand bucket creation (§15.2) has `CreateBucket` implications addressed in the security model.
- Implementations **SHOULD** support object immutability (e.g., object lock / WORM) where the backend allows it.
- Implementations **SHOULD** support optional manifest signing to strengthen chain-of-custody guarantees.
- Verification (§10, §17) protects against silent corruption but is not a substitute for transport security.

See the accompanying [SECURITY_MODEL.md](SECURITY_MODEL.md) for the full security model.

---

## 19. Conformance profiles

| Profile | Requirement |
|---------|-------------|
| **VTOP-Core** | Mandatory baseline. See §19.1. |
| **VTOP-Kafka** | Implements the Kafka source adapter with offset-based progress markers. |
| **VTOP-File** | Implements the file source adapter with byte-offset progress markers. |
| **VTOP-Syslog-Spool** | Implements the syslog spool source adapter with spool-offset progress markers. |
| **VTOP-S3** | Implements an S3-compatible upload backend with verification. |
| **VTOP-LocalFS** | Implements a local-filesystem upload backend (object tree on local storage) with verification, for testing/air-gapped operation. |

### 19.1 VTOP-Core (minimum)

A minimum **VTOP-Core** implementation **MUST** provide:

1. **Manifest generation** (§11).
2. **Checksum generation** (§10) — at least one of SHA-256, BLAKE3, or an explicit size-only/disabled mode.
3. **State-machine enforcement** (§12) with rejection of illegal transitions.
4. **Verification-before-commit rule** (§13).
5. **Replay-safe state persistence** (§14).

An implementation claiming a source profile (Kafka/File/Syslog-Spool) **MUST** also satisfy VTOP-Core. An implementation claiming VTOP-S3 or VTOP-LocalFS **MUST** verify both object and manifest before any source commit. A conformant implementation **MUST NOT** advertise a profile it does not fully satisfy.

---

## 20. Extensibility

VTOP is designed to be extended without changing its core guarantees:

| Extension point | Contract to satisfy |
|-----------------|---------------------|
| New source adapter | Implement the §6 source adapter contract (records + markers, forward reads, no self-commit, replayable). |
| New upload backend | Implement upload + verification (object and manifest) so the §13 commit rule holds; declare verification strength (§17). |
| New format | Provide declaration and/or per-batch detection; record the format in the manifest. |
| New checksum algorithm | Compute over compressed object bytes; record algorithm + value; preserve §17 strength reporting. |
| New partition field | Resolve deterministically from declared policy (§16). |

Adding any extension **MUST NOT** weaken the commit rule (§13) or the replay rule (§14).

---

## 21. References

- [ARCHITECTURE.md](ARCHITECTURE.md) — reference-implementation architecture and data-flow.
- [SECURITY_MODEL.md](SECURITY_MODEL.md) — security model and normative security rules.
- [INVENTION_DISCLOSURE_DRAFT.md](INVENTION_DISCLOSURE_DRAFT.md) — candidate-invention disclosure draft.
- [PRIOR_ART_SEARCH_PLAN.md](PRIOR_ART_SEARCH_PLAN.md) — prior-art search plan.
- Project [README](../README.md) — setup, CLI, and Docker lab.

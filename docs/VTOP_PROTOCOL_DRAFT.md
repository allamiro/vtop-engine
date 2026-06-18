# VTOP Protocol Draft

**Verified Telemetry Object Protocol (VTOP)**

Status: Draft / proposed protocol
Version: 0.1 (reference implementation specification)

> This document describes a **proposed protocol** and accompanies a **reference implementation** ("VTOP Engine"). It is a draft for technical review and is part of an **invention-disclosure support package**. It does not describe a shipped standard.

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** in this document are to be interpreted as normative requirements describing conformant behavior of a VTOP implementation.

---

## 1. Scope

VTOP defines a **replay-safe, manifest-driven** method for transferring telemetry data from one or more telemetry sources into S3-compatible object storage.

The proposed protocol specifies:

- How telemetry is ingested from heterogeneous sources (Kafka topics, files, and syslog spool files).
- How telemetry records are grouped into **adaptive batches**.
- How batches are compressed into immutable **telemetry objects**.
- How cryptographic checksums and **manifest objects** are generated.
- How objects and manifests are uploaded to and verified in object storage.
- The strict ordering rule that source progress **MUST NOT** be committed until uploaded objects and manifests are durably written and verified.

VTOP does **not** define the internal record schema of telemetry payloads, the choice of compression algorithm (beyond requiring one be declared), or the object storage backend implementation.

---

## 2. Terminology

| Term | Definition |
|------|------------|
| **Telemetry source** | An origin of telemetry records: a Kafka topic partition, a file, or a syslog spool file. |
| **Source progress marker** | A durable position into a source: a Kafka offset, a file byte offset, or a syslog spool offset. |
| **Batch** | An ordered, bounded collection of telemetry records selected from a single source for transfer. |
| **Adaptive batch** | A batch whose sealing is governed by size, record-count, and/or time thresholds. |
| **Telemetry object** | A compressed, immutable representation of a sealed batch, written to object storage. |
| **Manifest** | A structured document describing a telemetry object, its integrity metadata, and the source progress markers it covers. |
| **Verification** | Confirmation that a durably stored object/manifest matches its declared checksum and metadata. |
| **Commit** | Advancing a source progress marker so that covered records will not be re-read in normal operation. |
| **Replay** | Re-reading source records from a previously committed or uncommitted progress marker after a failure. |

---

## 3. Source adapter contract

A VTOP source adapter abstracts a telemetry source behind a uniform interface. A conformant source adapter:

- **MUST** expose telemetry records together with an associated source progress marker.
- **MUST** support reading forward from a given source progress marker.
- **MUST NOT** advance (commit) a source progress marker on its own; commit is driven exclusively by the engine after verification (see §7).
- **MUST** support idempotent re-reads from an uncommitted marker (replay safety).
- **SHOULD** expose source identity metadata (tenant, source name, format) for object naming and partitioning.
- **MAY** support backpressure signaling to the batching layer.

Defined source adapter types:

- **Kafka source** — progress marker is the Kafka offset per topic partition.
- **File source** — progress marker is a byte offset into the file.
- **Syslog spool source** — progress marker is a spool offset into the spool file.

---

## 4. Batch object

A batch is the unit of transfer. A conformant implementation:

- **MUST** assign each batch a unique `batch_id`.
- **MUST** seal a batch before compression; a sealed batch is immutable.
- **MUST** record the source progress markers covered by the batch (start and end positions).
- **SHOULD** seal batches adaptively based on configurable size, record-count, and time thresholds.
- **MUST NOT** include records from more than one logical source in a single batch.

A sealed batch carries at minimum: `batch_id`, source identity, covered progress markers, record count, and uncompressed byte size.

---

## 5. Manifest object

A manifest is a structured JSON document bound to exactly one telemetry object. A conformant manifest:

- **MUST** include the `batch_id`.
- **MUST** include source identity (tenant, source, format).
- **MUST** include the covered source progress markers (start and end).
- **MUST** include the telemetry object's storage location (bucket, key).
- **MUST** include the telemetry object's integrity metadata: checksum algorithm (`SHA-256`), checksum value, compressed byte size, and uncompressed byte size.
- **MUST** include the compression algorithm and extension.
- **MUST** include a creation timestamp.
- **MUST NOT** include credentials, secrets, or authentication material (see §11).
- **SHOULD** include retention/partitioning metadata to support archival policies.
- **MAY** include a manifest signature when manifest signing is enabled.

The manifest binds the object's hash to the source progress markers, establishing an object-level **chain of custody**.

---

## 6. State machine

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

A conformant implementation **MUST** persist state transitions durably so that recovery after a crash is possible (see §8).

---

## 7. Commit rule

This is the **core normative rule** of VTOP:

> **A source progress marker MUST NOT be committed until the telemetry object and its manifest have been durably written and verified in object storage.**

Concretely, a source progress marker **MUST NOT** transition to `SOURCE_COMMITTED` until **all** of the following have completed in order:

1. The batch is **sealed**.
2. A **compressed** telemetry object has been created.
3. The object **checksum** (SHA-256) has been calculated.
4. The object has been **uploaded** to object storage.
5. The **manifest** has been generated.
6. The manifest has been **uploaded** to object storage.
7. The uploaded **object** has been **verified**.
8. The uploaded **manifest** has been **verified**.
9. **Only then** is the source progress marker committed.

A conformant implementation **MUST NOT** commit source progress in any other order. `SOURCE_COMMITTED` **MUST NOT** be reachable unless `VERIFIED` is true.

---

## 8. Replay rule

A conformant implementation:

- **MUST** persist batch state durably such that, after a crash, every batch can be classified as committed or not-yet-committed.
- **MUST** treat any batch that did not reach `SOURCE_COMMITTED` as eligible for replay.
- On recovery, a batch in any non-committed state **MUST** be transitioned to `FAILED` and then `REPLAY_REQUIRED`, and re-entered at `BATCHING`.
- **MUST NOT** double-commit a source progress marker that was already committed.
- **MUST** be safe to re-run from the last committed source progress marker without data loss; re-uploaded objects with identical content **SHOULD** be idempotent (same `batch_id` and naming yields the same object key).

A mismatch between persisted state and the contents of object storage (e.g., an object exists but is not verified) **MUST** be resolved by re-verifying or re-uploading before any commit.

---

## 9. Checksum rule

- A conformant implementation **MUST** compute a **SHA-256** checksum over the compressed telemetry object bytes.
- The checksum **MUST** be recorded in the manifest.
- Verification **MUST** compare the checksum of the durably stored object against the manifest checksum.
- An implementation **MUST NOT** transition to `VERIFIED` if the stored object's checksum does not match the manifest.
- An implementation **MAY** additionally record an uncompressed-content checksum.

---

## 10. Object naming rule

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

---

## 11. Security considerations

- Credentials and secrets **MUST NOT** be embedded in manifests or telemetry objects.
- Credentials **MUST NOT** be written to logs.
- Transport to Kafka and to S3-compatible endpoints **SHOULD** be protected with TLS.
- Object storage permissions **SHOULD** follow least privilege.
- Implementations **SHOULD** support object immutability (e.g., object lock / WORM) where the backend allows it.
- Implementations **SHOULD** support optional manifest signing to strengthen chain-of-custody guarantees.
- Verification (§9) protects against silent corruption but is not a substitute for transport security.

See the accompanying `SECURITY_MODEL.md` for the full security model.

---

## 12. Conformance requirements

### Conformance profiles

| Profile | Requirement |
|---------|-------------|
| **VTOP-Core** | Mandatory baseline. See below. |
| **VTOP-Kafka** | Implements the Kafka source adapter with offset-based progress markers. |
| **VTOP-File** | Implements the file source adapter with byte-offset progress markers. |
| **VTOP-Syslog-Spool** | Implements the syslog spool source adapter with spool-offset progress markers. |
| **VTOP-S3** | Implements an S3-compatible upload backend with verification. |

### VTOP-Core (minimum)

A minimum **VTOP-Core** implementation **MUST** provide:

1. **Manifest generation** (§5).
2. **Checksum generation** (§9, SHA-256).
3. **State-machine enforcement** (§6) with rejection of illegal transitions.
4. **Verification-before-commit rule** (§7).
5. **Replay-safe state persistence** (§8).

An implementation claiming a source profile (Kafka/File/Syslog-Spool) **MUST** also satisfy VTOP-Core. An implementation claiming VTOP-S3 **MUST** verify both object and manifest before any source commit.

A conformant implementation **MUST NOT** advertise a profile it does not fully satisfy.

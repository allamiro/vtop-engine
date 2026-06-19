# Prior-Art Search Plan

> Part of an **invention-disclosure support package** supporting a **candidate invention**. This document organizes a prior-art investigation. It identifies similarities, differences, and possible risk areas only. **It does not conclude patentability.** A qualified patent attorney must perform the formal analysis.

## Table of contents

1. [Purpose](#1-purpose)
2. [How to read this](#2-how-to-read-this)
3. [Methodology](#3-methodology)
4. [Dimensions of comparison](#4-dimensions-of-comparison)
5. [Comparison table](#5-comparison-table)
6. [Risk assessment framework](#6-risk-assessment-framework)
7. [Search execution checklist](#7-search-execution-checklist)
8. [Search strategy notes](#8-search-strategy-notes)
9. [Notes for patent counsel](#9-notes-for-patent-counsel)

---

## 1. Purpose

Catalog existing tools and systems whose behavior overlaps with the **proposed VTOP method** (replay-safe, manifest-driven telemetry object transfer with verification-before-commit). For each item, record what it does, how it is similar to and different from VTOP, the possible prior-art risk area, and notes for patent counsel.

## 2. How to read this

The dimension that matters most for VTOP is the **commit discipline**: whether the tool gates *source progress / acknowledgment* on *durable + verified* storage of both the object **and** a bound manifest, across heterogeneous source types. Many tools below archive to object storage and some acknowledge sources, but the combination of (a) cross-source progress abstraction, (b) bound manifest verification, and (c) commit-only-after-verification is the focus of comparison.

## 3. Methodology

### 3.1 Search sources

| Source class | Examples | What to look for |
|--------------|----------|------------------|
| Patent databases | USPTO full-text/PatFT-AppFT, Google Patents, Espacenet, WIPO PATENTSCOPE | Claims on offset/ack-after-durable-write, manifest-bound integrity, cross-source commit, WORM gating. |
| Academic literature | ACM DL, IEEE Xplore, USENIX (OSDI/NSDI/ATC/FAST), arXiv | Exactly-once delivery, end-to-end acknowledgment, durable log archival, tamper-evident logging. |
| Open-source projects | GitHub/GitLab repos + docs for the tools below | Precise commit/ack timing relative to durable write; presence/absence of a verified bound manifest. |
| Vendor documentation | Confluent, Elastic, Datadog, AWS, Cribl, Splunk, MinIO | Connector ack semantics, sink delivery guarantees, object-lock/WORM features. |
| Standards / specs | S3 API, S3 Object Lock, syslog (RFC 5424), Kafka protocol | Background art for primitives (multipart ETag, object lock, offsets). |

### 3.2 Example query terms

- `"offset commit" AND ("after upload" OR "after durable") AND object storage`
- `("end-to-end acknowledgement" OR "exactly once") AND (sink OR S3) AND telemetry`
- `manifest AND (checksum OR sha256 OR blake3) AND ("chain of custody" OR tamper-evident) AND log`
- `("source progress" OR "read position") AND (kafka OR syslog OR "byte offset") AND commit`
- `WORM OR "object lock" AND archival AND (verify OR integrity) AND commit`
- `"write once read many" AND telemetry AND retention`

### 3.3 Procedure

1. Run the query terms against each source class; capture date, identifier, and the precise commit/ack semantics.
2. Classify each hit against the dimensions in §4.
3. Score prior-art risk per §6.
4. Record open questions for counsel (§9).

## 4. Dimensions of comparison

The **distinguishing combination** to test against all prior art:

> **uniform cross-source progress marker abstraction + bound cryptographic manifest verified in storage + commit strictly after verification.**

| Dimension | Question to answer for each item |
|-----------|----------------------------------|
| Cross-source progress abstraction | Does it treat Kafka offsets, file byte offsets, and spool positions uniformly? |
| Bound manifest | Is there a stored manifest binding object hash to covered source positions? |
| Verified-before-commit | Is durable + verified storage a precondition for advancing source progress? |
| Dual verification | Are both object and manifest verified? |
| Integrity strength | Cryptographic hash vs. size/existence vs. none? |
| Recovery model | Deterministic, idempotent replay distinguishing verified-retry from pre-verified replay? |
| Partitioning/retention | Telemetry-aware partition + retention metadata, per-format buckets? |
| Immutability | WORM/object-lock gating? |

## 5. Comparison Table

| Tool/system | What it does | Similarity to VTOP | Difference from VTOP | Possible prior-art risk | Notes for patent attorney |
|-------------|--------------|--------------------|----------------------|-------------------------|---------------------------|
| **Kafka S3 Sink Connector** | Streams Kafka records to S3 objects, manages offset commit via Kafka Connect. | Kafka offset progress; partitioned S3 objects; commit tied to flush. | Kafka-only; no syslog/file abstraction; no bound cryptographic manifest verified before offset commit. | Medium-high on Kafka-offset-after-upload concept. | Examine exactly when offsets are committed relative to durable write; check whether any integrity manifest is verified pre-commit. |
| **Fluent Bit S3 output** | Buffers logs and uploads to S3 with optional compression. | Buffering/batching; compression; S3 upload. | No cross-source progress commit model; no manifest-verified-before-commit; file/spool offset commit not unified. | Medium on batch-then-upload. | Compare its retry/ack semantics and whether upload success gates any source position. |
| **Logstash S3 output** | Writes events to S3 in time/size-rolled files. | Time/size-based rollover (adaptive-ish batching); S3 upload. | At-least-once with limited verification; no bound manifest; no unified source progress commit. | Low-medium. | Clarify durability/ack guarantees; note absence of integrity manifest. |
| **Vector acknowledgements + S3 sink** | End-to-end acknowledgements; S3 sink with batching/compression. | End-to-end ack tied to sink delivery; batching; compression; multiple source types. | Ack model is delivery-based, not gated on a *verified bound manifest*; no SHA-256 manifest object as commit precondition. | High — closest on "ack only after delivery" across sources. | Most important to analyze. Map Vector's ack pipeline against VTOP's verification-before-commit; identify whether any manifest hash verification occurs. |
| **OpenSearch Data Prepper acknowledgements** | Acknowledgement framework gating source progress on sink completion. | Source progress gated on downstream completion; multi-source. | Geared to OpenSearch/streaming sinks; no bound manifest + object hash verification before commit. | Medium-high on ack-gating concept. | Compare ack-gating semantics; check for any integrity manifest. |
| **Apache Flume S3 sink** | Channel-based reliable delivery to sinks including S3. | Transactional channel; delivery before source removal. | Transaction model, not manifest-verified object archival; no cross-source uniform offset abstraction. | Medium. | Review transactional channel semantics vs. VTOP commit rule. |
| **Apache NiFi S3 processors** | Flow-based dataflow with PutS3Object and provenance. | Provenance/chain-of-custody flavor; S3 put; flowfile state. | Provenance is internal lineage, not a stored bound manifest verified before source ack; no unified offset model. | Medium on provenance/chain-of-custody. | Compare NiFi provenance to VTOP manifest binding; note storage location and verification differences. |
| **IBM Aspera FASP** | High-speed file transfer with integrity verification. | Integrity verification of transferred data. | Transport protocol, not telemetry batching/manifest archival; no source-offset commit model. | Low. | Relevant only to "verify before considering transfer complete." |
| **rclone** | Sync/copy to many backends with checksum verification. | Checksum verification post-transfer; many backends. | General file sync; no batching/manifest/source-offset commit; no telemetry semantics. | Low-medium on checksum-verify-after-upload. | Note generic checksum verification as background art. |
| **s3cmd** | CLI S3 transfer with MD5/checksum checks. | Checksum check on upload. | Pure CLI tool; no batching/manifest/commit model. | Low. | Background art for upload + integrity check. |
| **AWS CLI multipart upload** | Multipart upload with per-part ETag/checksum. | Per-part integrity; durable upload completion semantics. | No manifest, no source progress, no replay model. | Low. | Background art for upload integrity primitives. |
| **MinIO client (mc)** | CLI/SDK for S3-compatible storage with checksums and object lock. | S3-compatible backend; integrity; object lock/WORM. | Tooling, not a telemetry commit engine; no bound manifest gating source progress. | Low-medium. | Relevant to backend-independence and object lock claims. |
| **S3 object lock** | WORM retention/legal hold on objects. | Object immutability; retention metadata. | A storage feature, not a transfer/commit method. | Low (but relevant to immutability claim family). | Distinguish using object lock vs. *gating commit on verified manifest*. |
| **WORM archival systems** | Write-once-read-many compliance archival. | Immutability; retention; audit posture. | No telemetry source-offset commit or bound manifest verification step. | Low-medium on immutability/retention. | Useful to bound the immutability/retention claim family. |
| **Chain-of-custody logging systems** | Tamper-evident logging with hashes/signatures. | Hash binding; chain of custody; optional signing. | Focus on log tamper-evidence, not source-offset commit gated on verified object+manifest. | Medium-high on hash binding / chain of custody. | Closest on claim family 7 (manifest/object hash binding). Survey hash-chaining and signed-manifest prior art carefully. |
| **SIEM archival pipelines** | Archive security telemetry to object storage partitioned for analytics/retention. | SIEM-aware partitioning; retention metadata; telemetry archival. | Often per-vendor; lack unified cross-source verified-commit with bound manifest. | Medium on partitioning + retention metadata. | Relevant to claim family 4 (SIEM-aware partitioning). |
| **OpenTelemetry Collector (exporters)** | Pipeline of receivers/processors/exporters; file/object/queued-retry exporters. | Multi-source receivers; batching; retry/queue; pluggable exporters. | Retry/queue is delivery-based; no stored bound manifest verified in storage before advancing a source position; no unified offset/byte/spool commit. | Medium on multi-source pipeline + retry. | Examine queued-retry and any "persistent queue" ack timing; check whether any object hash is verified in storage pre-commit. |
| **Cribl Stream (S3 destination)** | Routes/transforms observability data to destinations incl. S3, with persistent queues. | Routing to S3; batching/compression; persistent queues; multi-source. | Delivery/queue-based durability, not commit gated on verified bound manifest; no uniform source-progress-marker across kafka/file/spool. | Medium. | Compare persistent-queue durability semantics to VTOP commit rule; note absence of stored verified manifest. |
| **Object-lock / WORM compliance systems (broad)** | Regulated retention (e.g. SEC 17a-4-style) with immutable storage. | Immutability + retention metadata; audit posture. | Storage-tier compliance, not a cross-source transfer/commit method with bound verified manifest. | Low-medium on immutability/retention claim family. | Bound the immutability claim family; distinguish from commit-after-verification. |

## 6. Risk assessment framework

Score each item along two axes and prioritize accordingly.

| Risk level | Meaning | Action |
|------------|---------|--------|
| High | Overlaps the *core* distinguishing combination (cross-source + bound manifest + verify-before-commit). | Deep-dive; map element-by-element for counsel. |
| Medium-high | Overlaps two of the three distinguishing elements. | Analyze the missing element carefully. |
| Medium | Overlaps one element or a related concept (provenance, ack-gating, partitioning). | Document the gap; treat as context. |
| Low-medium | Background art for a primitive (checksum verify, object lock). | Cite as background; bound the relevant claim family. |
| Low | Peripheral. | Note and move on. |

Per item, record: identifier/source, date, which dimensions (§4) it matches, the exact commit/ack timing, and whether a stored verified manifest exists.

## 7. Search execution checklist

- [ ] Run §3.2 query terms across each §3.1 source class; log identifiers + dates.
- [ ] For each high/medium-high item, document the **precise** commit/ack timing relative to durable write and verification.
- [ ] For each item, record whether a **stored bound manifest** exists and whether it is **verified before commit**.
- [ ] Classify each item along the §4 dimensions.
- [ ] Assign a §6 risk level.
- [ ] Separate **background-art primitives** (checksum verify, multipart ETag, object lock) from **method-level** prior art.
- [ ] Compile open questions and ambiguities for counsel (§9).
- [ ] Re-run for any newly discovered tools and on material dependency/feature changes.

## 8. Search strategy notes

- Prioritize **Vector acknowledgements + S3 sink** and **OpenSearch Data Prepper acknowledgements** for the "commit/ack only after delivery" angle.
- Prioritize **OpenTelemetry Collector** and **Cribl Stream** for the "multi-source pipeline with persistent-queue durability" angle and clarify whether any stored object hash is verified before a source position advances.
- Prioritize **chain-of-custody logging systems** and **NiFi provenance** for the "manifest/object hash binding" angle.
- Prioritize **Kafka S3 Sink Connector** for the "offset-commit-after-upload" angle.
- Treat CLI transfer tools (rclone, s3cmd, AWS CLI, mc) as **background art** for upload + checksum verification primitives.
- Treat object lock / WORM and broad WORM-compliance systems as **storage-feature** background art relevant to immutability claims.
- The distinguishing combination to test against all prior art: **uniform cross-source progress marker abstraction + bound cryptographic manifest verified in storage + commit strictly after verification.**

## 9. Notes for patent counsel

- The investigation above is for **technical triage only**; it deliberately does **not** assess novelty or non-obviousness.
- The key element-by-element comparison points are: (1) the source-progress-marker abstraction across Kafka/file/spool; (2) the stored, self-hashed manifest binding object hash to covered positions; (3) commit strictly after dual verification, enforced redundantly.
- Background-art primitives (checksum-verify-after-upload, multipart ETag, object lock/WORM) should be distinguished from the **method-level** combination.
- Open questions to resolve with counsel include the exact ack/commit timing in Vector, Data Prepper, OpenTelemetry Collector, and Cribl, and whether any of them verify a stored object hash (not merely confirm delivery) before advancing a source position.

> Reminder: this plan deliberately does **not** assess novelty or non-obviousness. Those determinations are reserved for qualified patent counsel.

# Prior-Art Search Plan

> Part of an **invention-disclosure support package** supporting a **candidate invention**. This document organizes a prior-art investigation. It identifies similarities, differences, and possible risk areas only. **It does not conclude patentability.** A qualified patent attorney must perform the formal analysis.

## Purpose

Catalog existing tools and systems whose behavior overlaps with the **proposed VTOP method** (replay-safe, manifest-driven telemetry object transfer with verification-before-commit). For each item, record what it does, how it is similar to and different from VTOP, the possible prior-art risk area, and notes for patent counsel.

## How to read this

The dimension that matters most for VTOP is the **commit discipline**: whether the tool gates *source progress / acknowledgment* on *durable + verified* storage of both the object **and** a bound manifest, across heterogeneous source types. Many tools below archive to object storage and some acknowledge sources, but the combination of (a) cross-source progress abstraction, (b) bound manifest verification, and (c) commit-only-after-verification is the focus of comparison.

---

## Comparison Table

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

---

## Search Strategy Notes

- Prioritize **Vector acknowledgements + S3 sink** and **OpenSearch Data Prepper acknowledgements** for the "commit/ack only after delivery" angle.
- Prioritize **chain-of-custody logging systems** and **NiFi provenance** for the "manifest/object hash binding" angle.
- Prioritize **Kafka S3 Sink Connector** for the "offset-commit-after-upload" angle.
- Treat CLI transfer tools (rclone, s3cmd, AWS CLI, mc) as **background art** for upload + checksum verification primitives.
- Treat object lock / WORM as **storage-feature** background art relevant to immutability claims.
- The distinguishing combination to test against all prior art: **uniform cross-source progress marker abstraction + bound cryptographic manifest verified in storage + commit strictly after verification.**

> Reminder: this plan deliberately does **not** assess novelty or non-obviousness. Those determinations are reserved for qualified patent counsel.

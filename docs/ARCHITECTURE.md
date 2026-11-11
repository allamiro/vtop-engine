# Architecture

> **Status:** living document, kept aligned with the code. **Audience:**
> engineers reading or extending the reference implementation.
>
> Architecture of the **VTOP Engine reference implementation** (a prototype of
> the proposed VTOP protocol). Part of an **invention-disclosure support
> package**. Normative behavior is defined in
> [VTOP_PROTOCOL_DRAFT.md](VTOP_PROTOCOL_DRAFT.md) (cited as "protocol §N");
> deployment/HA design and the achieved-vs-planned ledger live in
> [PRODUCTION_HA.md](PRODUCTION_HA.md).
>
> This document covers the **engine plane** (archive path). The distributed
> cluster plane (`vtop-meta`/`vtop-node`/`vtop-broker`/`vtop-log`) is designed
> in [NATIVE_BROKER_ARCHITECTURE.md](NATIVE_BROKER_ARCHITECTURE.md).

## Table of contents

1. [Overview](#1-overview)
2. [Design goals and non-goals](#2-design-goals-and-non-goals)
3. [Workspace layout](#3-workspace-layout)
4. [Core data types](#4-core-data-types)
5. [The pipeline](#5-the-pipeline) *(see [Engine runtime flow](#engine-runtime-flow))*
6. [Data-flow diagram](#6-data-flow-diagram)
7. [Source adapters](#7-source-adapters)
   - [7.1 What the commit rule does not cover](#71-what-the-commit-rule-does-not-cover)
   - [7.2 Rotation and truncation of file and spool sources](#72-rotation-and-truncation-of-file-and-spool-sources)
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
18. [Implementation status and direction](#18-implementation-status-and-direction)

---

## 1. Overview

VTOP Engine is a Rust workspace implementing a replay-safe, manifest-driven telemetry object transfer engine. It ingests telemetry from Kafka topics, files, and syslog spool files; forms adaptive batches; compresses them; computes a content checksum (SHA-256 or BLAKE3, or size-only when disabled); generates a bound manifest; uploads the telemetry object and manifest to a pluggable storage backend; verifies the uploaded object and manifest; and commits source progress only after verification succeeds.

The governing rule is the protocol's **commit rule** ([VTOP_PROTOCOL_DRAFT.md §13](VTOP_PROTOCOL_DRAFT.md#13-commit-rule)): **`SOURCE_COMMITTED` is forbidden until `VERIFIED` is true.** It is enforced in three places — the state machine, the durable state store, and the engine pipeline. Throughout this document, "protocol §N" cites [VTOP_PROTOCOL_DRAFT.md](VTOP_PROTOCOL_DRAFT.md).

---

## 2. Design goals and non-goals

### 2.1 Goals

| Goal | How the architecture serves it |
|------|--------------------------------|
| Replay safety | Durable state journal + verification-before-commit; no marker advances for unverified data. |
| Protocol/transport independence | `vtop-core` has no dependency on Kafka, S3, or the CLI; sources and backends sit behind traits. |
| Cross-source uniformity | One `SourceAdapter` trait and one progress-marker abstraction over Kafka/file/spool. |
| Determinism | Deterministic object naming so replay reproduces the same key (protocol §15.1 goal — **not yet met**: the `batch_id` embeds a timestamp and random suffix, so replay currently writes a new key; see §13). |
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
| `vtop-state` | Durable state journal behind the `StateStore` trait: SQLite by default, PostgreSQL behind `--features postgres`. | `sqlx` |
| `vtop-observe` | Opt-in metrics/readiness HTTP endpoint (`VTOP_METRICS_ADDR`). | — |
| `vtop-cli` | The `vtopctl` binary and the engine runtime that wires everything together. | all of the above |

(The workspace also contains the cluster-plane crates `vtop-meta`, `vtop-node`, `vtop-broker`, `vtop-log`, and `vtop-protocol`, documented in [NATIVE_BROKER_ARCHITECTURE.md](NATIVE_BROKER_ARCHITECTURE.md); they are outside this document's scope.)

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
| `BatchId` | Unique batch identifier embedding a UTC stamp, source/partition/offset range, and a random suffix (deterministic in shape, not in value — protocol §15.1 gap). |
| `SourceProgressMarker` | Source-agnostic position: Kafka offset, file byte offset, or syslog spool offset. |
| `Batch` | Sealed, immutable record set + covered marker range + record count + format + sizes. |
| `Manifest` | Bound document: source identity, marker range, object location, integrity metadata, self-hash. |
| `BatchState` | One of the state-machine states (see §5.1 / protocol §12). |
| `UploadResult` / `VerifyResult` | Backend outcomes, including verification strength (strong vs backend-limited vs disabled/size-only — protocol §17). |
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

If any stage fails, the batch transitions to `FAILED`, then `REPLAY_REQUIRED`, and its range is handed back to the source at the end of the cycle, whether or not those writes land (§13, item 5). If verification fails, `commit_progress(...)` is never called. If the commit itself fails after verification, the batch stays `VERIFIED` (not lost). The running engine retries the commit after each later pass of that source, and recovery retries it after a restart (#624, §13 item 1).

### 5.1 State machine

```
DISCOVERED -> BATCHING -> SEALED -> COMPRESSED -> CHECKSUMMED ->
OBJECT_UPLOADED -> MANIFEST_UPLOADED -> VERIFIED -> SOURCE_COMMITTED
```

Recovery states: `FAILED`, `REPLAY_REQUIRED`. Terminal states: `SOURCE_COMMITTED`, and `SUPERSEDED` for a handed-back range that its source has since committed past (#567, §13).

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
| FAILED | SUPERSEDED |
| REPLAY_REQUIRED | SUPERSEDED |

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
| `kafka_source` | Kafka offset per topic+partition | `rdkafka` consumer with **auto-commit always disabled**. Retention can overtake a committed cursor silently — see §7.1. One batch = one topic + one partition + one offset range (partitions never mixed). Supports topic include/exclude regex and TLS/SASL config. Offsets committed only after `VERIFIED`. |
| `file_source` | File byte offset | Reads append-only files, tracking `path`, `inode`, byte offsets, size, and mtime. Line-oriented mode (a partial trailing line is never committed) **and** a whole-file mode for binary/compressed-source inputs. Resumes from the last committed byte; replay rewinds to the start of the uncommitted range. |
| `syslog_spool_source` | Spool byte offset | Treats rsyslog/syslog-ng spool files as append-only with a `spool_id` + byte range. External collectors own delivery; VTOP owns batching, checksum, manifest, upload, verification, replay state, and the commit rule. |

The marker carried by each adapter is a `SourceProgressMarker` — the unit bound into the manifest and gated by the commit rule.

### 7.1 What the commit rule does not cover

The commit rule protects records the engine **read**. It says nothing about
records that stopped existing before the engine got to them, and for Kafka that
is a real and silent condition.

`auto.offset.reset` is passed through to `rdkafka` and defaults to `earliest`
(`kafka_source.rs`). Where a restart resumes a partition is decided first
(#614):

- **From the ledger** — when the ledger holds a committed batch for the
  partition (this topic, partition and consumer group) AND the broker reports
  a committed offset for the group on that partition. The partition resumes
  after the last offset of the newest committed batch, newest by
  `(created_at, batch_id)`, whatever the group's committed offset says: the
  offset commit carries no group generation, so Kafka accepts it from an
  engine whose claim on the batch has moved on, and only the ledger's commit
  is fenced. The group's offset is still committed, for lag monitoring, and a
  restart moves it up to the ledger's position when it is behind.
- **From `Offset::Stored`** — every other partition: a fresh store, no
  committed batch yet, a group with no committed offset for the partition
  (deleting a topic deletes its group offsets, and the ledger cannot tell a
  recreated topic from the old one), or a broker that could not be asked for
  the group's offsets at startup. librdkafka resolves it from the group's
  committed offset, or from `auto.offset.reset` when there is none. In the
  last case the ledger is not consulted for that run, and the protection
  against a stale engine's commit does not apply to it.

When retention has already removed records past that position, `auto.offset.reset`
applies by two different routes, and both end the same way:

- **After a restart** — the partition is assigned at the position above, which
  the broker answers out-of-range (or, at `Offset::Stored`, has no committed
  offset to resolve), and the reset applies.
- **While still running** — the assignment is sticky and ordinary lag does not
  seek at all; the pass seeks only where the desired offset diverges from what
  librdkafka would deliver next. A consumer that simply falls behind therefore
  keeps fetching forward until the broker answers out-of-range, and the reset
  applies there.

Either way a consumer overtaken by retention resumes at the **new** earliest offset. Nothing queries the
watermarks, nothing compares the resumed offset against the committed one, and
no metric fires. The gap between them is data that was never batched, never
manifested, never verified and never missed.

This is not a defect in the commit rule; it is the boundary of what a
commit-after-verification design can assert. Every guarantee in this document
about replay safety, manifests and verification applies to a record once the
engine has seen it. **Size retention above worst-case lag, and monitor consumer
lag independently of VTOP**, because VTOP will not tell you.

### 7.2 Rotation and truncation of file and spool sources

The file and syslog-spool adapters keep one read position per **path**, and a
position is only meaningful for the file it was taken from. Two things break
that, and the adapters now check for both on every read, against the open
descriptor (`fstat`) rather than a second path lookup:

- **Rotation** (rename or replace): the inode at the path differs from the one
  the cursor last read.
- **Truncation** (`copytruncate`, an in-place truncate): the same inode, now
  shorter than the read position.
- **Replacement at the same inode** (#578): a freed inode number handed to the
  next file created, or a file rewritten in place that has already regrown
  past the read position. Inode and length both agree with the old file;
  its first bytes do not. Every read fingerprints the file's first page
  (SHA-256 of its first `min(length, 4096)` bytes, read through the same
  descriptor), records it in its markers (`content_fingerprint`), and checks
  the fingerprint the previous read took against the file's first bytes. A
  file under one page fingerprints all of itself, so the fingerprint grows
  with the file; the check compares the old fingerprint's length of the new
  file, so an append is never taken for a replacement. A file that was empty
  at a read takes no fingerprint and proves nothing at the next one.

In either case the new content is read from **byte 0**, and the cursor's
committed point returns to 0 with it. Before this was handled, a read position
from the old file was applied to the new one: a new file shorter than the
position produced nothing until it outgrew it and then started mid-line, and a
longer one silently lost its first bytes (#558).

What follows from that, in the engine:

- **A buffered window is sealed at the discontinuity.** A read that does not
  start where the buffered window ended (a different inode, or a start that is
  not the previous end) is not merged into it; the old window becomes its own
  batch. A merged batch carried a start from one file and an end from the
  other, a range nothing could re-read or reconcile.
- **A late commit cannot move the restarted cursor.** A batch read before the
  rotation commits after the cursor has restarted; its offset names a place in
  a file that is no longer there. Its ledger row commits (the archive has it),
  but the cursor stays on the current file.
- **Replay refuses a range from a file that has changed.** The old bytes are
  not at those offsets any more, and rewinding the new file there would archive
  its middle as though it were the lost range. The adapters return an error
  when the inode differs from the marker's, when the file is shorter than the
  range's start, or (file sources, whose markers record the length at read
  time) when it is shorter than it was when the range was read. A spool marker
  without an inode keeps the older size-only check, and a spool truncated and
  regrown past the range's start cannot be told from the original. How the
  engine reports a refused rewind is described in section 13.
- **Commits are applied in the order the windows were read**, not the order
  their uploads finish. A rotation seals the old file's window and starts a new
  one in the same cycle, so both are in flight at once; committing the new
  window first would leave the old file as the most recent commit and make the
  restart seed name a file that is gone.
- **A late commit is settled against the generation its range was read in.**
  The adapter starts a new generation each time it restarts a cursor on a
  rotated or truncated file, and remembers which generation every range it
  handed out was read in. A range from an older generation never moves the
  cursor, however the file looks by the time its commit lands - a
  `copytruncate`d file that has regrown past the old range's end is otherwise
  indistinguishable, by inode and length, from the file the range was read
  from. The record is in-process: after a restart a commit falls back to the
  inode and length test.
- **A restart seeds the cursor with the inode** the offset came from
  (`StateStore::committed_cursors`: the most recent committed batch per path -
  the one created last, which is the window read last - with its own inode
  and offset, never a maximum: after a `copytruncate` the inode is unchanged
  and the old content's higher offset would skip the new content's first
  bytes). A file replaced while the engine was down is read from its first
  byte instead of from the old file's high-water mark. Ledger pruning keeps
  exactly this row for every path, so the seed is the same before and after
  any prune. The seed also carries the length the
  most recent committed batch saw its file at (`file_size` in the marker), so a
  `copytruncate` while the engine was down is caught even when the file is
  still longer than the committed offset, and the content fingerprint, so a
  replacement at the same inode is caught too. Syslog-spool markers record
  their length and fingerprint since #578; a spool seeded from an older row
  has the inode and the offset only. File markers also record, since #620,
  the fingerprint of the page that ends at their `end_byte`, so the seed
  catches a rewrite that kept the first page as well (below).
- **The bytes below the read position are evidence too** (#620). Every file
  read fingerprints the page (at most 4 KiB) that ends where it stopped,
  from the bytes it consumed, and the next read - and a seed after a
  restart - checks that the file still holds those bytes there, through the
  descriptor it reads from (one extra read of at most a page; two after a
  rewind moved the position off the page's end). A file rewritten in place
  with the same first page, or a reused inode whose new file starts like
  the old one, fails it - and is read from byte 0 - when its bytes in that
  page differ from the old file's; one that repeats them too is not caught
  (see the limits below). A late or recovered commit is judged the same way
  against its range's end, so one whose generation record was evicted (more
  than 256 ranges issued since) or lost to a restart cannot carry the
  cursor into rewritten content whose page below that end differs.
- **Late commits, rewinds and `delete_after_commit` judge content too**
  (#578, #620): a range whose marker fingerprinted other first bytes - or
  other bytes below its end - than the file at the path now holds does not
  move the cursor, is not replayed into the new file, and does not delete
  it. Before, a same-length replacement at the
  same inode satisfied every test of `delete_after_commit`
  (`end_byte >= size`) and was deleted unread.

Limits, stated plainly:

- The **unread tail of a rotated-away file** is not read through the old path.
  If the rotated name (`app.log.1`) also matches a configured pattern it is
  discovered as its own source, with a fresh cursor, and is archived again from
  byte 0 — a duplicate, never a hole. Archiving the unread tail without the
  duplicate would need cursors keyed by inode rather than path.
- `copytruncate` is racy by nature: lines written between the copy and the
  truncate are lost by the rotation tool before the engine can see them. If the
  file also regrows beyond the old read position before the next read, the
  truncation is invisible to the adapter.
- A file is identified by its **inode number, its first page, and the page
  below the read position**. An inode number can be reused: on a
  *different* filesystem by a replacement that happens to carry it (markers
  do not record the device), and on the *same* filesystem by the next file
  created after one is **unlinked**. Since #578 such a file is caught by its
  first bytes, and since #620 by the bytes below the read position, whatever
  its length. What is still not caught is a replacement that matches BOTH
  pages byte for byte - the same first 4 KiB and the same 4 KiB below the
  old position, such as a file restored from a copy of the old one, or a
  file under one page whose new content repeats all of the old - and a
  change confined to the bytes BETWEEN the two pages, which only a full
  re-hash of everything below the position would see. Those look like a
  continuation.
- Late commits are judged against the file as it is NOW (another inode, an
  end beyond its length, other first bytes, or other bytes below the range's
  end than the batch read). The in-process generation record still orders
  the discontinuities one process saw; a commit it has no record of (evicted,
  or after a restart) falls back to these tests, and passes only for content
  that matches both pages - the case above.
- **Rows written before #578 carry no fingerprint**, and a cursor seeded from
  one, or a range recovered from one, is judged by inode and length alone,
  exactly as before. The first read after an upgrade that finds the file
  non-empty takes a fingerprint, and from there the check applies. Rows
  written before #620 carry no tail fingerprint and are judged exactly as
  before, by the fields they do carry: inode and length, and the first page
  for a row written since #578. The first read after the upgrade takes
  one.
- History pruning keeps one row per path, so a file untouched for longer than
  the retention window can lose the rows that name its inode. The restart then
  reads it from byte 0 again: duplicates, bounded by the file.

Tracked as #361, which also carries the question of whether the adapter should
refuse rather than resume when it detects the jump — a refusal is only useful
if an operator can act on it, and a source that stops archiving because it fell
behind may be worse than one that archives what remains.

#### Ledger format note: content fingerprints (#578)

The change is to the JSON of the `progress_start` / `progress_end` markers the
ledger stores, on both backends; no table changes, so there is no DDL and
`vtopctl migrate` has nothing new to apply (PostgreSQL), and a SQLite ledger
needs nothing either.

- New, all optional and omitted when unknown: `content_fingerprint`
  (`{"len": <bytes covered>, "sha256": "<hex>"}`) on file and syslog-spool
  markers, and `file_size` on syslog-spool markers.
- Rows written before the change decode with the fields absent, and every
  reader judges them exactly as before: the adapters and the restart seed by
  inode and length (spool rows, which recorded no length, by inode and read
  position), the coverage audit by inode and size (files) or inode alone
  (spools). Rows from before and after the change at one path and inode are
  judged together: a fingerprint is compared only with another of the same
  length, so a row without one cannot cause a fingerprint split (the size
  test still applies to it as before).
- A marker with neither new field set (every file marker without a
  fingerprint; spool markers now always carry `file_size`) serializes byte
  for byte as before. The
  manifest's `source_progress` never carries the new fields: an older
  `verify-manifest` decodes a manifest and hashes it re-encoded, and would
  drop them and report a mismatch, so manifests stay in the format every
  released reader verifies.
- An older binary reading a newer ledger ignores the fields (serde skips
  unknown keys) and judges as it always did. Downgrade is still not
  supported (see the roadmap's versioning note); this is what it would do.

#### Ledger format note: tail fingerprints (#620)

The same kind of change, to file markers only, and again no DDL on either
backend: the restart seed reads the new field out of the stored
`progress_end_json` as it reads `content_fingerprint`.

- New, optional and omitted when unknown: `tail_fingerprint` on file markers,
  the same `{"len", "sha256"}` shape, covering the `len` bytes that END at the
  marker's `end_byte`. Absent for a marker at byte 0 and for a read the stat
  proved unchanged (it does not open the file).
- Rows written before it decode with the field absent and are judged exactly
  as before, by the fields they carry (inode and length, and the first page
  for a row written since #578). Leaving the field out changes nothing else
  about a marker: one without it serializes byte for byte as the code before
  #620 wrote the same marker - and as the code before #578 did only when it
  carries no `content_fingerprint` either (see the note above).
- A stored fingerprint no writer could produce - a zero length, a length
  over 4 KiB, a digest that is not 64 lowercase hex digits, or a value that
  is not a fingerprint at all - decodes as absent, for `tail_fingerprint`
  and `content_fingerprint` alike, both in the restart seed and in markers
  decoded whole (recovery's commits and rewinds). Absent only forgoes a
  check; a malformed value used to force a re-read, or a large read.
- The manifest's `source_progress` never carries it, for the reason above.
  The coverage audit does not use it.
- An older binary ignores it, as above.

---

## 8. Upload backends

All backends implement `UploadBackend` (`upload`, `verify_object`, `ensure_bucket`). The engine depends only on the trait, so it is backend-independent, and every backend is held to the same verification-before-commit contract.

| Backend | Target | Checksum verification | Resumable multipart (`supports_multipart`) | Bucket create | Use |
|---------|--------|-----------------------|---------------------------------------------|---------------|-----|
| `s3_native` | AWS S3 / MinIO / Ceph RGW (endpoint + path-style) via `aws-sdk-s3` | Strong (service-computed SHA-256; streamed stored-content BLAKE3) | Yes (create/upload_part/complete/abort + persisted session) | Yes (on-demand) | Primary production backend. |
| LocalFS | Local directory tree | Strong (streams the stored file; sidecar is not trusted) | No | Yes (mkdir tree) | Testing / air-gapped. |
| `awscli` | S3 via AWS CLI | Strong (downloads and hashes stored content) | No (CLI may multipart internally; no resume API) | Yes | Compatibility mode; explicit absolute binary, minimal environment, startup version check, timeout/output caps. |
| `s3cmd` | S3 via s3cmd | Strong (downloads and hashes stored content) | No | Yes | Compatibility mode; same hardened command policy. |
| `minio mc` | S3-compatible via `mc` | Strong (downloads and hashes stored content) | No | Yes | Compatibility mode; same hardened command policy. |
| `mock` | In-memory | Configurable | Yes (test double for resume/fencing) | Yes | Tests/benchmarks. |
| `mock_fail` / `mock_limited` | In-memory fault injection | Forced failure / size-only | Yes | Yes | Fault-injection tests. |

### Resumable multipart (#191)

Large sealed segments (and other objects above `upload.multipart_threshold_bytes`)
upload through a persisted multipart session when the backend advertises
`supports_multipart()`:

- Configurable `multipart_part_size_bytes`, `multipart_max_parallelism`, and
  `multipart_abandon_after_secs`.
- Session files under an operator-supplied state directory record upload id,
  completed parts, and per-part BLAKE3 digests so a process restart resumes
  without retransmitting finished parts.
- Sessions are fenced by destination URI + segment generation + fencing epoch
  + content digest; a mismatched fence refuses resume (stale generation).
- Multipart ETags are **not** integrity evidence. Evidence commit still requires
  strong whole-object `verify_object` and immutable version pins (#183/#184).
- `vtopctl tier cleanup-abandoned` aborts aged in-progress uploads.

Protocol §17 grades verification into three classes: **strong**
(stored-content or service-computed digest matches the manifest),
**backend-limited** (the service can confirm only existence and size), and
**disabled** (size-only by explicit configuration). A service unable to return
a required content checksum yields a backend-limited result; checksums
disabled by configuration are the separate disabled class. Strong verification
is the default; accepting a non-strong result requires an explicit opt-out
(`require_strong_verification: false`).

---

## 9. Checksum subsystem

- The `checksum` module computes a content checksum over the **compressed** object bytes — after compression, before upload (pipeline stage 4).
- Supported modes: **SHA-256**, **BLAKE3**, or **disabled** (size-only verification). The mode is configurable per run.
- The chosen algorithm and value (or the disabled indicator) are recorded in the manifest.
- The manifest additionally carries a reproducible **self-hash** (computed with both the embedded self-hash and MAC fields blanked) for corruption detection (`verify_self_hash`) — tamper detection requires the optional keyed BLAKE3 `manifest.mac` (protocol §11.2/§17.3).
- When `manifest_mac_key_env` is configured, stored manifests must carry a valid keyed MAC; a missing MAC never silently downgrades verification (protocol §17.3).
- Verification compares a digest derived from the stored body (or S3's service-computed SHA-256) against the manifest before `VERIFIED` is reached. Uploader metadata and LocalFS sidecars are never strong evidence.

---

## 10. Compression subsystem

- The `compression` module compresses the sealed batch into immutable object bytes at pipeline stage 3.
- Supported algorithms: `gzip`, `zstd`, `none`. The algorithm and its extension are recorded in the manifest and determine the object key suffix (`.gz`, `.zst`, or none).
- Because checksums are computed over compressed bytes, the compressed object is the canonical artifact that is hashed, uploaded, and verified.

---

## 11. Partitioning and per-format buckets

The `partitioning` module derives a telemetry-aware partition path from a consistent time policy (protocol §15.1, §15.2, §16). It is **general-purpose** (log analytics, observability, audit, compliance, SIEM), not tied to one domain.

```
s3://{bucket}/{prefix}/tenant={tenant}/source={source}/format={format}/year={yyyy}/month={mm}/day={dd}/hour={hh}/{batch_id}.{format}.{compression_ext}
```

The bound manifest is stored at the same prefix as `{batch_id}.manifest.json`.

**Which clock picks the time partition** (`partitioning.time_source`, #559):

- `seal` (default): the instant the batch was sealed — taken as it leaves the
  engine's buffer, before it waits for an upload slot, its ledger writes,
  compression, checksumming and upload, and recorded as its `sealed_at`. A
  saturated upload queue, a slow upload or a retry within the batch does not
  move it. Late data, a backlog drain and a replay land in the hour they were
  **sealed** in — a replay of yesterday's range is a new batch and is filed
  under today. Partitions used to come from the wall clock at upload, so a
  batch sealed at 12:59 and uploaded at 13:00 was filed under hour 13.
- `event`: the earliest record time the source reported. Only the Kafka source
  reports one (the message timestamp — producer `CreateTime` or broker
  `LogAppendTime`, whichever the topic uses); a batch from a source that reports
  none falls back to its seal time. Late data lands in the hour it **happened**
  in, which can be a partition a downstream reader has already processed:
  readers that need completeness must look back, or use the manifests.

Either way the manifest's `first_timestamp` / `last_timestamp` are the batch's
earliest and latest record times when the source reports them (Kafka), and
absent otherwise. They are a minimum and a maximum, not the first and last
record's times: producer timestamps arrive out of order.

- **Extensible fields:** the path **may** be extended with `environment`, `facility`, `severity`, `retention_class`, `region`, `site`.
- **Per-format buckets:** bucket names may be templated, e.g. `bucket: "telemetry-{format}"`, with optional on-demand bucket creation via `ensure_bucket`. The fully resolved bucket and key are recorded in the manifest.

---

## 12. State store schema

`vtop-state` is a durable journal behind the `StateStore` trait — **SQLite via `sqlx`** by default, **PostgreSQL** behind `--features postgres` (see [POSTGRES_DEPLOYMENT.md](POSTGRES_DEPLOYMENT.md)). It survives restart and is the source of truth for recovery. Conceptually it stores, per batch:

| Field group | Contents |
|-------------|----------|
| Identity | `batch_id`, tenant, source type/name, format. |
| Markers | Covered source progress marker range (start/end). |
| Object | Object/manifest URIs, sizes, checksum algorithm + value. |
| State | Current `BatchState`; transitions routed through the state machine. |
| Verification | Verification status and strength. |
| Timestamps | Discovery/seal/commit timestamps for audit and recovery. |

Crucially, every backend's `update_batch_state` routes **every** state change through the state machine's `transition()`, so the commit rule holds even at the persistence layer (not only in the pipeline); the PostgreSQL backend additionally enforces it with database constraints.

**What a power loss keeps (SQLite).** The SQLite ledger runs in WAL mode at `synchronous=NORMAL`, which syncs the WAL only at checkpoints, because the default rollback journal at FULL cost about half of the batch time under backlog (#87). `VERIFIED` and `SOURCE_COMMITTED` are the exception: they are written through a second connection at `synchronous=FULL`, so each one returns only after the WAL is synced, and that sync also covers every earlier write (#626). A power loss can therefore roll back only the writes after the last of them. No committed range is read and archived again, and a `VERIFIED` row, which a Kafka offset commit follows, is never lost. What remains is the batches in flight that had not reached `VERIFIED`, which are replayed as after a process crash (§13). The cost is two WAL syncs per batch instead of none. PostgreSQL commits durably by default and needs no change.

---

## 13. Crash recovery / replay algorithm

On startup, the `replay` module inspects `vtop-state` and maps each incomplete batch to a recovery action (`Engine::recover()`):

1. A batch in **`VERIFIED`** but not yet committed (object and manifest durably stored and verified) has its **source commit retried** (`VERIFIED → SOURCE_COMMITTED`). The verified object is never discarded.
2. A batch in any state **before `VERIFIED`** is transitioned `... → FAILED → REPLAY_REQUIRED` and its range is handed back to the source adapter (`replay_from_marker`) so it is read again. The replay decision is persisted first — if the `FAILED` or the `REPLAY_REQUIRED` write fails, no rewind is recorded for the row and it is counted as pending (a failure of the second write leaves the row `FAILED`, not untouched; the next recovery decides again). Recovery then collects the rewinds and applies the **lowest start per source unit** (one file or spool file at one inode, or one Kafka topic-partition) once, after the scan: each rewind overwrites the adapter's read position, so applying them row by row let the last row visited decide where the source resumed and skipped every earlier range. A rewind that fails, or that has no adapter to apply it to, is logged as an error and counted in the summary as still pending.

   A `VERIFIED` row is committed only after that, and not at all while a range of the same source unit that **starts below it** is about to be re-read: a commit moves the unit's committed point to the end of the committed range, the file adapters clamp a rewind to the committed point, and a `VERIFIED` range archives only its own bytes — so pending bytes below its start would be skipped. Such a row stays `VERIFIED` (its object is in the archive) until a later pass commits it.

   **That later pass is the running engine's, not the next restart's (#624).** The engine keeps the `VERIFIED` rows it left uncommitted: the rows its recovery held back, and the rows whose source commit failed, at recovery or in a pass. After each later pass of the same source type, it reads each row again. A row that is no longer `VERIFIED` is dropped. A held-back row is committed once the adapter's committed point is at or past its **end**. By then the re-read below it has archived and committed its bytes again, so committing it moves no source. A row whose commit failed is retried as the pipeline left it.

   Nothing is claimed. Every one of these rows is this process's own (created by its pipeline or claimed by its recovery), it still holds the fence token, and the pass that produced the row has finished with it. So no in-flight batch, of this engine or another, is ever touched. A row another engine has claimed since refuses the ledger write and is dropped; that engine's recovery commits it.

   Committing a range its source has already passed must not move the source back. The Kafka adapter skips a commit at or below its frontier (#617). The file and spool adapters skip one at or below a committed point that this process confirmed against the file now at the path. Before, they set the cursor to the stale end, and for a range read before a restart they moved the read head back too. The seed alone is not trusted for this: it can be a previous generation's (#564). Such a row is recorded `SOURCE_COMMITTED`: it is `VERIFIED`, and its source has committed through its end. A `VERIFIED` row that starts where the pending range starts — the object that replaced a stale row for the same bytes — commits normally. A marker with no inode (a legacy row) is compared against every inode at its path.

   **A rewind never goes below what the source has already committed.** The file and spool adapters have always clamped to their committed byte; the Kafka adapter clamps to the ledger's committed frontier for the partition (its newest committed batch, or what the process committed since it started) or, for a partition the ledger does not know, to where an earlier rewind in this process landed. It asks the broker for the consumer group's committed offset only for a partition it has none of these for (#614) — a stale engine's commit can move the group's offset past a range recovery is about to re-read, and clamping to it skipped that range for good. Either way, a stale `FAILED` row left behind by a failure that has since been repaired no longer rewinds the group to its start and re-archives everything since on each restart.

   **A range the source has committed past is retired, not handed back (#567).** Before the rewind is planned, recovery asks the row's adapter for the committed point of the row's unit (`SourceAdapter::committed_through`). At startup that is the ledger's newest committed row for the unit, newest by `(created_at, batch_id)` as in both stores and in pruning, the same row the restart cursor is seeded from, which retention never prunes. For Kafka it is the ledger frontier the adapter trusts (#614). For files and spools it is the cursor's committed byte, answered only when the row's marker is PROVEN to be the cursor's file: the same inode (a legacy marker without one is never retired), and the same content - equal fingerprints, or both matching the first bytes of the file now at the path - and, for a marker with a tail fingerprint, the same bytes below its end. Anything less keeps the replay path, because `SUPERSEDED` is terminal. If that point is at or past the end of the row's range, the rewind would land there and read nothing again. So the row moves to `SUPERSEDED` (from `FAILED` or `REPLAY_REQUIRED`; an in-flight row is written `FAILED` first), it is counted in the summary as superseded, not as a replay, and no rewind is planned for it. `SUPERSEDED` is terminal. Neither the claim nor the incomplete listing returns it, so later restarts never touch it, and retention deletes it with committed history, digesting it first. If a write fails, nothing is rewound: a row that was `FAILED` or `REPLAY_REQUIRED` stays as it was, and an in-flight row whose `FAILED` write landed but whose `SUPERSEDED` write did not is left `FAILED`. Nothing below the committed point would be read anyway, and the next recovery decides again. A row whose range spans two files, or whose adapter cannot say, keeps the replay path. Before this, every such row was claimed at every restart, written `FAILED` and `REPLAY_REQUIRED` again, counted in `replay_required_total`, and given a no-op rewind, and retention never removed it.

   `SUPERSEDED` claims only that the source committed past the range. It does not claim that a committed range covers the range's bytes. A ledger written before #552 can hold a committed range above a failed one, and that row is now retired like any other, because its rewind was already clamped to nothing. The coverage audit (#560) still judges a `SUPERSEDED` range exactly as a `FAILED` one and names it in any hole.
3. Replay currently produces a **new** object key: the `batch_id` is derived from wall-clock time plus a random suffix, and the `year/…/hour` path components from the replayed batch's own partition time (§11, `partitioning.time_source`) - under `seal`, the hour the replay was sealed in, so a replay of yesterday's range is filed under today; under `event`, the earliest record time, so a Kafka replay usually lands in the original hour but under a new key. Either way a replayed batch writes a duplicate object (at-least-once, no data loss). A power loss on the SQLite ledger no longer adds to these beyond what a process crash leaves (§12, #626). Protocol §14/§15.1/§16 specify deterministic naming so that replay is idempotent — this is the open gap tracked as [PRODUCTION_HA.md](PRODUCTION_HA.md) Phase 4.
4. A state/storage mismatch (e.g., an object present but unverified) is resolved by re-verifying or re-uploading **before** any commit.
5. **While the engine is running, a failed range is handed back at once, and the hand-back does not wait on the ledger.** Adapters advance their read position when they *read*, not when a batch commits, so a batch that fails (or errors before `VERIFIED`) is handed back to its source at the end of the cycle that failed it, not at the next restart. Otherwise the next cycle reads on from the end of the failed range and the first later range to verify commits past it, leaving those bytes unarchived (#552).

   The ledger records the decision as recovery does — `FAILED`, then `REPLAY_REQUIRED` — but, **unlike recovery (item 2), the rewind is unconditional**: it happens when the `FAILED` write fails, when the `REPLAY_REQUIRED` write fails (the row stays `FAILED`), and after any other error before `VERIFIED`, such as a ledger write that failed mid-pipeline, when the engine cannot even tell whether the ledger holds a row for the batch (#627). The two paths start from different read positions, and that is why one rule does not fit both:

   - At startup the read head is seeded at the committed point, which is at or below every uncommitted range. A rewind withheld there leaves the range ahead of the head.
   - While running, the head is already past the failed range. A rewind withheld there lets the next cycle read on and a later batch commit past the range; the restart that then marks the stale row for replay clamps its rewind to that commit, and the range is never archived.

   So a rewind on an unrecorded decision costs at most a duplicate, and withholding it costs the data. The duplicate is the object the failed attempt may have left in the store, with no `VERIFIED` row naming it; the range itself is archived once, by the batch that re-reads it. The stale row, if one was written, is visited by the next recovery, which records the replay decision and rewinds — clamped to the committed point, so nothing already archived is read again. The same holds for a write refused by the claim fence (#557): it means another engine claimed the row, and since sources are not divided between engines (two engines on one store are unsupported, [PRODUCTION_HA.md](PRODUCTION_HA.md) §19), this engine cannot know that the claimant will read the same bytes, so it hands its own range back too. A batch that reached `VERIFIED` is never handed back: its object is in the archive, and a failed source commit leaves it `VERIFIED` for recovery to retry (item 1).

   A cycle in which a batch failed waits before reading again — the idle interval, doubling per consecutive failing cycle up to a minute, plus jitter — because the failed range is now read again and a tight loop would hammer a store that is down.

   Limits, stated rather than implied: a ledger written by a build *before* #552 can already contain a committed range above a failed one. Replay does not repair that for file and spool sources — their rewind is clamped to the committed point so a superseded failed row cannot re-archive committed data on every restart — and the ledger cannot say whether the older range was later covered, because committed history may be pruned. Finding such holes is the job of an archive coverage audit (#560), not of recovery. A spool whose identity changed since the batch was read cannot be rewound; that is reported as an error and the range is not re-read — but only for a marker that recorded an inode: a legacy marker without one keeps the old size-only check and can rewind a replacement. The file adapter checks identity on replay the same way: a range whose file now has another inode (rotation, #558), is shorter than when the range was read, or keeps the inode but has different first bytes (a same-inode replacement, #578) is refused rather than re-read. The inode and content checks apply only to a marker that recorded them; a legacy marker keeps the size-only check.

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
- **Every call the engine makes to its object store and state store has a deadline** (#556). A store that accepts a connection and never answers would otherwise block the awaiting cycle forever, with the process alive and the health endpoint green. The native S3 backend sets the SDK's first-byte (read), per-attempt and whole-operation limits from `upload.request_timeout_seconds` (default 300 s per attempt; the whole operation is allowed the SDK's three attempts); its connect limit is a fixed 10 s, not configurable — the SDK's own defaults bound connecting but not waiting for a response, which the `request_deadline` test shows against a socket that accepts and stays silent. Those SDK limits end when the response headers arrive, so every response BODY the engine reads afterwards (verification, manifest recovery) is bounded separately, by the whole-operation allowance; a store that sends headers and then drips the body fails the read rather than holding it open. The compatibility backends use `upload.command_timeout_seconds`. The state store is wrapped so every request is bounded by `engine.state_store_timeout_seconds` (default 30 s; whole-ledger scans such as startup recovery get ten times it), and opening a Postgres store (connect plus schema check) is bounded by the same value. An expired request is an ordinary failure to the engine — a failed upload fails the batch, and recovery re-reads its range — but it is an **unknown outcome**, not a failed one: the store may have applied it. That is safe here because every ledger transition is checked against the state machine and recovery re-reads rows it finds incomplete; the worst case is a range archived twice, never one lost. Size `request_timeout_seconds` for the slowest link a part must cross (a 64 MiB part at 1 MiB/s needs about a minute). Not covered: `vtopctl migrate`, an operator-run one-shot command whose Postgres connection and schema statements carry no deadline (a schema change on a large ledger can legitimately be slow) — an operator who sees it hang interrupts it, and the migration's statements are idempotent, so running it again is safe.
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

---

## 18. Implementation status and direction

A one-screen map of how far this architecture is built and where it is going.
The authoritative, maintained version is
[PRODUCTION_HA.md](PRODUCTION_HA.md) Part I (status) and Part III (roadmap);
this table exists so a reader of the architecture does not mistake design for
reality.

| Architectural element | Status | Notes |
|---|---|---|
| Pipeline, state machine, commit rule (§5, §12–13) | ✅ shipped | Enforced in state machine + state store + pipeline; crash/replay tested in CI. |
| Source adapters: Kafka (`assign`), file (line + `whole_file`), syslog spool (§7) | ✅ shipped | Kafka consumer-group (`subscribe`) fleet mode is future work. |
| Upload backends incl. `localfs`, hardened command backends (§8) | ✅ shipped | Verification strength declared per backend (protocol §17). |
| `StateStore`: SQLite + PostgreSQL, shared battery, DB constraints (§12) | ✅ shipped | Retry-on-`40001`; `vtopctl migrate` privilege split; retention (#128). |
| Manifest self-hash + keyed BLAKE3 MAC, version pinning (§9) | ✅ shipped | Public-key signatures and key rotation are future work. |
| Metrics endpoint + readiness, dashboards-as-code (§14) | ✅ shipped | OTLP trace export and alerting rules are future work. |
| Deterministic object naming (§11, §13) | ⬜ planned | Protocol §15.1 MUST; the open conformance gap — [PRODUCTION_HA.md](PRODUCTION_HA.md) Phase 4. |
| Multi-instance fleet (consumer groups + multi-writer recovery) | ⬜ planned | Phase 5; single-instance lock (#66) until then. |
| Engine Kubernetes chart | ⬜ planned | Phase 7 — `helm/vtop` deploys the cluster plane, not the engine. |
| File/syslog HA (leases) | ⬜ optional | Phase 9, gated on an explicit product decision. |

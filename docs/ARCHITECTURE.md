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
| Determinism | Deterministic object naming so replay reproduces the same key (protocol §15.1). The object key is derived from the source, the range and the digest of the bytes, so a replay that produces the same bytes resolves to the object already stored (#626); the `batch_id`, and so the ledger row and the manifest key, stays unique per attempt. Limits in §13, item 3. |
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
| `BatchId` | Unique identifier of one attempt at a batch, embedding a UTC stamp, source/partition/offset range, and a random suffix. Names the ledger row and the manifest; the object is named by `build_object_name` (source, range, content digest), not by the batch id (#626). |
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

If any stage fails, the batch transitions to `FAILED`, then `REPLAY_REQUIRED`, and its range is handed back to the source at the end of the cycle, whether or not those writes land (§13, item 5). If verification fails, `commit_progress(...)` is never called. If the commit itself fails after verification, the batch stays `VERIFIED` (not lost). The running engine retries the commit after each later pass of the same source type, and recovery retries it after a restart (#624, §13 item 1).

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

While running, the partition list changes in ways a restart never sees, and
`latest` must not skip what arrived meanwhile (#675):

- **A partition added to a topic being read**, **a topic that listed no
  partitions when first asked for** (created moments later, or not yet known to
  the broker), or **a topic first read after the engine started** (discovery
  lists every topic each cycle, so it was created since) starts from the
  group's committed offset, else, under `latest`, from its **beginning**:
  every record in it is newer than the start `latest` promised when the engine
  began reading. Partition counts are re-checked every cycle from discovery's
  own listing (with no listing, one metadata request every 30s), so a new
  partition is assigned within a cycle. An answer with no partitions is never
  cached. When the start cannot be established, the partition waits, never
  assigned at `Offset::Stored`.
- **A whole re-assign** (the topic set or a partition count changed, a record
  was rejected, a poll failed) first pins every partition that stays assigned
  and is still waiting at `Offset::Stored` under `latest` where it started. A
  partition whose start cannot be established is left out and asked again
  10s later; every other partition is assigned and read. A partition whose
  records in a pass were all rejected or discarded is pinned at the first of
  them.
- **A failed partition lookup**, or a topic answer carrying an error other
  than "unknown topic", serves the topic's last known list.
- **A deleted and recreated topic** (fewer partitions than before, a topic that
  lists partitions again after listing none, or an end offset below the
  position held) is reported, not acted on: the ledger cannot tell the two
  topics apart, and offsets alone cannot tell a recreate from a truncated log.
  Where the new log is shorter than the old position, the broker answers out
  of range and `auto.offset.reset` applies; where it already reaches past it,
  the records below are skipped without an error, which the warning says.

Each of these logs what happened, what the engine did and what the operator
should check, with the topic, partition and offsets as fields, and counts in
`vtop_kafka_partition_events_total{event}`.

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
  regrown past the range's start cannot be told from the original. A file
  source then looks for the file the range WAS read from, and re-reads the
  range from it when it can prove it is that file (#621, below). How the
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
- **`delete_after_commit` deletes only a file that has stopped being
  written** (#619). A commit that covers a file to its end starts a wait
  instead of deleting it. It records what the file looked like (inode, first
  page, page below the end, length, mtime, ctime), and the file is deleted only once it has stayed
  exactly like that for `delete_quiet_seconds` (default 3600), timed on the
  engine's monotonic clock from that commit. With `delete_quiet_seconds: 0`
  there is no wait: the covering commit deletes the file at once, still
  through the lease, rename and check below, so never while a process has it
  open. The check runs again at the next commit or
  read pass. Until then the file is kept and read as usual. Before, the
  engine deleted the path the moment it caught up. Most appenders hold their
  log open with `O_APPEND`, so every later line went into the unlinked
  inode, which no path names, and was lost. That happened every time the
  engine caught up on a live file. The delete itself renames the file to a
  hidden name in the same directory (`.vtop-delete.<nanos>.<n>`, bounded
  and independent of the file's own name, so a name near the 255-byte limit
  still fits). The rename never replaces a file already at that name
  (`renameat2(RENAME_NOREPLACE)` on Linux, `renameatx_np(RENAME_EXCL)` on
  Apple platforms; elsewhere, or on a filesystem without the flag, an
  existence check first), and a taken name is skipped for the next. It then
  checks the renamed file again, and unlinks it only if it is still
  unchanged. A line appended between the decision and the rename is
  caught there, and the file is hard-linked back under its name and kept.
  Before, a check-then-unlink lost that line. A writer that opens the path
  for each write creates a new file once the rename has happened, and that
  file is read as a new source. A delete that fails for a reason that may
  pass (the open, the unlink) keeps the wait's result and is retried on the
  next pass. A rename aside that fails for a reason that will not pass by
  itself (`EXDEV` - a bind-mounted single file - `EACCES`, `EPERM`,
  `EROFS`, `EBUSY`) keeps the file, says so with the errno and what to fix,
  and is retried a quiet period later.
- **A configured path that is a symbolic link is never deleted.** A rename
  moves the link, not its target: #631's delete removed the link, left the
  target, and dropped the cursor, so a writer still writing to the target
  was never read again. The file is kept, and the warning says to point the
  pattern at the target.
- **...and only on proof that no process has the file open** (#619, after
  #631). A quiet file is not a closed one. With the quiet period alone, a
  writer that held the file open lost its lines three ways: it was silent
  for longer than the period and then wrote again; its write landed between
  the final check of the renamed file and the unlink; or it opened the file
  while the delete was under way and wrote after it. So the delete takes a
  Linux write lease (`fcntl(F_SETLEASE, F_WRLCK)`, `vtop-adapters`
  `write_lease.rs`) on the file before moving it aside, and holds it until
  after the unlink. The kernel grants it only when the engine's descriptor
  is the file's only open file description: no other process, and no other
  descriptor of this one, has it open, for reading or writing, mapped
  included. While it is held, any open of the file breaks it, and the
  opener waits until it is released. So:
  - a writer that still holds the file - silent however long - makes the
    lease fail, and the file is kept; the wait starts again;
  - a writer that opens the file after the lease is granted breaks it. The
    final check sees that, the file is put back under its name, and only
    then is the lease released, so the opener gets a file that still has
    its name (unless a new file has taken the name since the rename; see
    the limits);
  - an open of the path that starts after the rename finds a new file. One
    that started before it may have resolved the path to this file and not
    yet reached the lease, so the lease is held for a grace of 50 ms after
    the rename before the final check (review of #689). With writers that
    open, append and close in a tight loop and no grace, a harness that
    copies the delete's steps lost lines; with 20 or 50 ms it lost none.

  Leases are per inode in the kernel, so the check also sees a writer in
  another container sharing the volume, which a scan of `/proc/*/fd` cannot.
  The engine's own descriptors count too: the descriptor kept to re-read a
  file's outstanding ranges (#621) is the one the lease is taken through,
  and the descriptor the covering commit checks the file with is closed
  before the delete. The final checks run through the leased descriptor,
  since opening the renamed file again would break the lease. A break sends
  the holder a signal, SIGIO by default, whose default action ends the
  process, so the lease is set to signal SIGURG, ignored by default, and
  then to signal no one.

  This is the only `unsafe` in the engine's `src/` trees: `fcntl` with
  integer arguments on a descriptor a borrowed `File` keeps open for the
  call, behind a safe wrapper (`write_lease::take`, `WriteLease::intact`,
  and the release on drop), with a `SAFETY` note on the one block.
- **Where there is no such proof, the delete is refused.** The lease needs
  Linux, a local filesystem, the engine running as the file's owner or with
  `CAP_LEASE` in its effective set (otherwise `EACCES`), and
  `fs.leases-enable` on. "Local" is checked by `statfs` type against a list
  (ext2/3/4, xfs, btrfs, tmpfs, overlay, f2fs, zfs, bcachefs) before a lease
  is asked for: a lease counts only the openers its own kernel sees, so on
  NFS, CIFS, CephFS, GlusterFS, virtiofs, 9p or sshfs it could not see a
  writer on another machine or on the host of a shared folder, and kernels
  from before leases became opt-in per filesystem grant one on FUSE anyway.
  A lease error that may pass (anything but `EACCES`, `EPERM`, `EINVAL`,
  `ENOSYS`, `EOPNOTSUPP`) is a failed delete, tried again. Without the proof
  the fully archived file is kept, and a warning names the reason and the
  remedies; the candidate is dropped, so the refusal is said once per
  covering commit rather than retried every pass. On a platform other than
  Linux the engine also says so at startup. `delete_without_open_check:
  true` restores deletion on the quiet period alone there, and with it the
  losses above, by the operator's choice. Where the lease can be had, it is
  always taken, and a file some process holds is kept whatever that key
  says.
- **"Unchanged" is one identity test**, `continuity::proves` (#647), the
  test every identity decision shares: the same inode, the first page
  (#578) and the page below the committed end (#620's tail fingerprint),
  plus the exact length, the mtime the batch READ (the marker's, not the
  file's at commit time - review of #689: a same-length rewrite between
  the pages during the upload, or during the downtime before recovery
  commits, became the baseline and was deleted unread) and the ctime. The
  covering commit, the delete decision
  and the final check before the unlink all make it. Before #619 was
  finished, the delete had a test of its own, which passed a marker that
  named no first page on the inode and length alone, so a same-inode,
  same-length rewrite that landed before the commit was deleted unread; and
  on a platform without inode numbers it judged the size alone. Now
  anything unknown answers no: a marker with no inode or no first page
  never deletes a file. A file that is gone, or that fails the test in any
  way, is no longer a candidate, and is kept until a new commit covers it.
- **A moved mtime or ctime keeps the file** (#619). #631 restarted the wait
  when only the mtime had moved and the tail proved both pages unchanged,
  and deleted the file a quiet period later. But a same-length rewrite
  confined between the first page and the page below the end looks exactly
  like that - and with its mtime set back, like nothing at all - so it was
  deleted unread. Every write moves the ctime, and so does setting the
  mtime back; no process can set the ctime. So the candidate records it,
  and any change of mtime or ctime ends the candidate and keeps the file,
  with a warning to remove it by hand once it is known to hold nothing new.
  A touched drop now stays: keeping a touched file costs disk, deleting a
  rewritten one cost its lines. The ctime is checked up to and under the
  lease; after the delete's own rename, which moves it, the check is the
  lease, which nothing can write past without breaking. The deletes run on
  the blocking pool, as many at once as the reads.
- **The wait is in memory, so a restart starts it again.** The seed re-arms
  it with a fresh clock, but only from a row that names the inode and the
  first page, and whose batch saw the file end at the committed offset. A
  batch that read part of a longer file never arms a delete from the seed:
  a same-inode rewrite cut back to the committed offset would otherwise be
  deleted before the first read's continuity check could see it. The file
  must also still have the mtime the batch recorded
  (`CommittedCursor::observed_mtime`, the marker's `mtime`), tail
  fingerprint or not: a same-inode, same-length rewrite while the engine was
  down agrees with everything else the seed records. A row with no mtime,
  or a file whose mtime moved, leaves the delete unarmed, and the file is
  kept with a warning. The ledger has no ctime, so the candidate takes the
  file's ctime at the restart: a rewrite while the engine was down that
  also set the mtime back is the one change a restart cannot see. The same
  holds between a batch's read and its commit, for the same reason: the
  marker records the mtime the batch read, not the ctime.
- **Hard links.** Deleting one name of a file moves the ctime of the
  others (#378 makes each its own source), so another name's delete then
  finds the ctime moved and keeps its file, saying so. Nothing is lost; the
  remaining name is removed by hand.
- With `delete_without_open_check` and no lease, a delete whose unlink fails
  after the move aside puts the file back, which moves its ctime; the next
  attempt then keeps the file and says so, rather than trusting a ctime it
  cannot vouch for.

Limits, stated plainly:

- **Where the engine cannot check that no process holds a file open**, it
  keeps the file (above). With `delete_without_open_check: true` it deletes
  on the quiet period alone instead, and a writer that keeps the file open
  loses lines in two windows:
  - it is silent for longer than `delete_quiet_seconds` and then writes
    again: it writes into a deleted file;
  - after that same silence, its write lands between the delete's final
    check of the renamed file and the unlink.

  Scanning `/proc/*/fd` was considered and rejected as the check: it is
  Linux-only like the lease, it needs privileges to see other users'
  descriptors, and it cannot see writers in other containers' PID
  namespaces, which is exactly where a sidecar deployment puts them.
- **With the lease, these windows are left**:
  - an open of the path that resolved it to the file before the rename and
    then stalled in the kernel for longer than the 50 ms grace before
    reaching the lease;
  - a process that opens the file by its hidden name
    (`.vtop-delete.<nanos>.<n>`) between the final check and the unlink,
    which nothing has reason to do.

  Either open gets the unlinked file once the lease is released; if it
  broke the lease before then, the engine logs an error. A process that
  reaches the file through another hard link is not in this window: the
  file keeps that name, and the link is its own source (#378).
- **A file left under a delete's hidden name is archived as a source of
  its own.** A crash between the rename and the unlink leaves the file
  under its hidden name, never deleted unchecked; a put-back that finds a
  new file at the path (an `O_CREAT` writer recreated it during the grace)
  leaves it there too. Either can hold a late write: the crash releases the
  lease, and an opener that waited on it then writes into that file. So
  discovery, with `delete_after_commit` on, also lists `.vtop-delete.*`
  files in each pattern's directory, warns once per file, and archives them
  - the bytes already archived again, as a duplicate, with any late write -
  and then deletes them like any other file.
- While the lease is held - the grace and the checks around it - an open of
  the file waits (up to `fs.lease-break-time`, 45 s by default, if the
  engine were frozen), and an `O_NONBLOCK` open fails with `EWOULDBLOCK`.
- **Every decision is said** (#619): each delete, and each file kept, logs
  one line naming the path, what happened, what the engine did and what to
  do, with `path`, `inode` and `archived_bytes` as fields - for example
  "not deleting P: 6 bytes were appended after the commit that archived its
  first 8; they will be archived next pass", or "not deleting P: the rename
  aside failed (EXDEV: ...)". A refusal that will not change is a warning,
  said once per covering commit; one that repeats while its cause lasts (a
  process holds the file, the rename fails) is said once, then at debug.

- The **unread tail of a rotated-away file** is not read through the old path.
  If the rotated name (`app.log.1`) also matches a configured pattern it is
  discovered as its own source, with a fresh cursor, and is archived again from
  byte 0 — a duplicate, never a hole. Archiving the unread tail without the
  duplicate would need cursors keyed by inode rather than path. (A failed
  range re-read from the rotated file, below, reads on to its end, as a rewind
  of a file still at its path does: in that case the tail is archived.)
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

#### A failed range from a file rotated away since it was read (#621)

A batch that fails hands its range back to the source, and the file adapter
re-reads it from the path. When the file has been rotated in the meantime -
`app.log` renamed to `app.log.1`, a new `app.log` created - the file at the
path is not the range's, and the rewind used to be refused: an ERROR, the row
left FAILED, refused again at every restart, and the range never archived
unless the rotated name happened to match a source pattern. Worse, a rewind
that was *accepted* while the file was still at its path, followed by a
rotation before the re-read, restarted the cursor on the new file and never
re-read the range at all, with no error.

The adapter (`crates/vtop-adapters/src/rotated.rs`) now reaches the file the
range was read from, in this order:

1. **A held descriptor.** While a range read from a file is not committed, the
   adapter keeps a descriptor open on that file. A rotation by rename moves the
   descriptor to a list of rotated files instead of closing it, and a rename or
   an unlink does not affect what an open descriptor reads, so this works even
   when logrotate has already compressed and removed `app.log.1`. It does not
   survive a restart. A rewind accepted before the rotation is honoured the
   same way: the rewound range is re-read through the descriptor before the
   new file is read.
2. **The rotated-to path.** Across a restart there is no descriptor, so
   recovery's rewind looks for the file among the entries of the path's own
   directory and the files the source's patterns match, on the same
   filesystem, by inode.

Either way nothing is read from a file until it is **proven** to be the one
the range was read from: the inode the marker recorded, the first page its
fingerprint covers (#578), the page that ends where the marker's position is
when it recorded one (its tail fingerprint, #620 - so a rotated file rewritten
in place below an unchanged first page is not taken for the range's), and no
shorter than when the range was read. The descriptor is proven again before
every re-read, against the same first page, the page below where it was last
read, and the longest the file has been seen to be: a rotated file truncated above
its first page keeps its fingerprint, and reading on would archive a
shortened prefix as the range (or nothing, and call the re-read done). Such a
re-read is refused. For rotated-file recovery, a marker without an inode or a
fingerprint cannot prove the old file and is refused as before (its rewind of
the file at its own path keeps the inode-and-length check described above).

The re-read starts at the range's start and reads on to the end of the rotated
file, as a rewind of a file still at its path reads on to the path's end. A
rewind of it never goes below what is committed from that file, as a rewind of
a file at its path never goes below its committed point. Its markers carry the
source path and the rotated file's inode. Wherever the identity checks tell the
rotated file from the file now at the path - another inode, or other first
bytes - its commit does not move the current file's cursor, and
`delete_after_commit` (which judges the file at the path) deletes neither file.
A replacement those checks cannot tell apart (the blind spots below) is the
exception. A
re-read in progress keeps its path a source even if nothing is at the path
yet - and so does a rewind accepted before its file was renamed away with
nothing created in its place, whose range only a read of the path moves onto
the rotated list. A FAILED row stays in the ledger and is rewound again at every restart, so
recovery asks the ledger for the newest committed batch of each file that has
lived at that path (`StateStore::committed_cursors_by_file`) and starts the re-read
above what is already archived of the rotated file - counting only a row whose
first page, and page below its end, the rotated file still holds. A commit
in a running engine raises that floor by the same test: a window of a file's
old content, committed after the file was rewritten in place at the same
inode, does not count as archiving the new content. Without that, a range
re-read once was archived again at every restart while the rotated file
existed.

A rewind that still cannot be honoured stays loud: an ERROR naming the path,
and `vtop_source_replay_refused_total{tenant,source_type}` counts it, so it
can be alerted on. A re-read refused after the rewind was accepted (the
rotated file truncated, or no longer starting with its first page) is counted
there too, not only as a read error.

Limits:

- **`copytruncate` is not handled.** The copy has a new inode, and a matching
  fingerprint and length are not proof of identity, so a range read before a
  `copytruncate` is refused (and counted) unless the copy also matches a
  configured pattern and is archived as its own source.
- **A rotated file that is gone** - compressed (`app.log.1.gz` holds other
  bytes, so it never matches) or removed - can be re-read only through a held
  descriptor, so only within the process that read it. After a restart its
  ranges are refused and counted.
- **The descriptors are bounded.** At most 128 in all (`MAX_HELD_FILES`); past
  that a file is not held, and a failed range falls back to the search. A
  descriptor on a rotated file with no re-read in progress is closed one hour
  after it was last used (`HOLD_FOR`) - it keeps a removed file's disk space
  allocated until it is closed. A descriptor being re-read through is kept
  while that re-read is in progress, and is never closed to make room. The
  search looks at no more than 10,000 entries of the directory.
- **Identity is the inode, the first page and the page below the marker's
  position** (#620), with the same blind spots as every other check in this
  section (above): a replacement matching both sampled pages, or a change
  confined between them. A range starting at byte 0 has no page below its
  start, so its proof is the inode and the first page alone.
- **The restart seed is chosen per file, and needs proof.** A re-read batch
  is created after the current file's batches - a window of it read in the
  cycle whose rotation sealed the failed window, or batches committed before
  a restart whose recovery then re-reads the range - so it becomes the path's
  newest committed row. Seeded from that row, a restart took the current file
  for a rotation and archived it again from byte 0 (review of #621). The seed
  is therefore chosen from the newest committed row of EACH file that has
  lived at the path (`StateStore::committed_cursors_by_file`): the newest, as
  before, unless it names a file no longer at the path; then the row of the
  file that is, when that file is positively identified - the same inode,
  the first page and the page below the row's end (#620). A current file
  whose row lacks either fingerprint (written before #578/#620), or whose
  bytes no longer match them, is not proven, and the newest row stands: the
  current file is read from byte 0 again, a duplicate bounded by the file,
  never a skip. So does a current file whose rows pruning removed (pruning
  keeps the newest row per path, which can be the re-read's). Likewise, if
  pruning removes the re-read's rows while the rotated file still exists,
  the next restart re-reads it.
- Without inodes (non-Unix platforms) nothing can be proven, and the rewind is
  refused as before. Syslog-spool sources are unchanged: a spool rotated away
  is still refused.

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

#### Ledger format note: verification class (#664)

A table change, unlike the two notes above: one new column on both backends,
`verification_class TEXT`, nullable, no default.

- Written with the transition to VERIFIED, in the same statement:
  `strong` (the stored content matched a digest, by a store-computed
  checksum or a read-back) or `size_only` (the store confirmed only size and
  existence, `backend_limited`, accepted under
  `upload.require_strong_verification: false`). The store refuses a class on
  any other transition, and every other transition leaves it as it is, so a
  committed row keeps the class it was verified with. The method within
  `strong` (store checksum or read-back) is not recorded: the native S3
  backend falls back from one to the other per object without reporting
  which ran.
- Recovery counts a commit of a VERIFIED row, in `commit_recovered` and in
  the settle pass for a row it held back, by this class, not by its own
  storage re-check. The re-check still decides whether the row may commit.
- NULL is unknown: every row verified before #664, and a value this release
  does not know (a later release's). Both are counted as before, by the
  re-check. The column has no CHECK, so a later release can add a value
  without a constraint migration, and an older reader treats it as unknown.
- SQLite adds the column when it opens the file. PostgreSQL adds it in
  `vtopctl migrate` (`ADD COLUMN IF NOT EXISTS`); engine startup names it, so
  an engine of this release refuses a schema without it and asks for the
  migration, naming the missing column. Either way, adding it to a table
  that holds rows logs `state store: added the verification_class column; N
  existing rows left NULL (verification class unknown) ...` once, with
  `backend`, `existing_rows` and `column` as fields. The PostgreSQL schema
  marker stays at 3: see the migration notes in
  [`POSTGRES_DEPLOYMENT.md`](POSTGRES_DEPLOYMENT.md).
- SQLite has a schema version of its own from #664: the file's `PRAGMA
  user_version`, 1 for this schema, set on open (a file from before reads
  0). A file whose version is higher was opened by a newer release, and this
  release refuses it - `SQLite state-store schema version N (the ledger
  file's PRAGMA user_version) is newer than this release supports (newest
  1) ...` - and leaves it as it was. The check, the migration and the
  version write are one `BEGIN IMMEDIATE` transaction, so a newer release
  raising the version at the same moment is seen, never written back down.
  The PostgreSQL schema has the same rule
  for its marker. Releases before #664 do not read the version.
- The serialized record (a prune digest row) carries `verification_class`
  only when it is known, so a legacy row serializes as before; a value the
  reader does not know reads as unknown rather than failing the digest.
- An older binary ignores the column on both backends: it maps rows by
  column name, inserts by an explicit column list, and never writes it. A row
  it verifies has no class, and is counted by the re-check, as it always was.
  One caveat: releases before #664 read `SELECT *` / `RETURNING *` through
  statements cached per connection, and a connection open while the column
  is added fails those reads until it is replaced (PostgreSQL: `cached plan
  must not change result type`; SQLite: the driver panics on the extra
  column). A running engine makes such reads only in startup recovery and
  when it settles a VERIFIED row whose source commit failed; its batch
  writes name their columns. Restart older PostgreSQL engines right after `vtopctl migrate`,
  and stop any other process on a SQLite file before a newer release first
  opens it. This release names every column it reads (`batch_columns!` in
  vtop-state), so a column a later release adds does not affect it.

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

- Configurable `multipart_part_size_bytes`, `multipart_max_parallelism`,
  `multipart_part_attempts` and `multipart_abandon_after_secs`.
- Session files under an operator-supplied state directory (the engine's
  `work_dir`) record upload id, completed parts, and per-part BLAKE3 digests
  so a process restart resumes without retransmitting finished parts.
- Sessions are fenced by destination URI + segment generation + fencing epoch
  + content digest; a mismatched fence refuses resume (stale generation). A
  session is identified by that content, not by the local file's name (#668):
  every kept part is re-read from the current file and its digest compared
  before it is skipped. A session with no content digest (checksums off) is
  never resumed.
- A failed part keeps what already completed (#668). Up to
  `multipart_max_parallelism` parts are in flight and each success is written
  to the session as it arrives. A failed part is retried alone, up to
  `multipart_part_attempts` requests, with exponential backoff and full jitter
  (1 s doubling to at most 30 s); after the first part fails for good no new
  part starts, and the requests already on the wire are waited for and kept.
  Every request and every throttle is reported to the caller, so the engine's
  width controller still sees throttles the retries absorbed. An upload the
  store cannot continue (`NoSuchUpload`, or `InvalidPart` / `InvalidPartOrder`
  on complete) is aborted where it still exists and restarted once with a new
  upload; a second failure in the same call keeps nothing.
- **The telemetry path resumes across replays (#668).** When a batch's upload
  fails, the engine keeps the session if the object is under its derived key
  (#626): the replay of the same bytes names the same key and session and
  sends only the missing parts. Under a per-attempt key nothing will name the
  session again, so it is aborted as before. A replay whose bytes differ (the
  source grew) names another key and leaves the old session to the sweep.
- Abandoned sessions are swept at startup and, while the engine runs, every
  quarter of `multipart_abandon_after_secs` (at most every 15 minutes). A
  bucket lifecycle rule (`AbortIncompleteMultipartUpload`) is the backstop for
  an engine that never comes back; see PRODUCTION_HA §7.6.
- Multipart ETags are **not** integrity evidence. Evidence commit still requires
  strong whole-object `verify_object` and immutable version pins (#183/#184).
  A resumed object is read back and verified exactly like a fresh one.
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

   Nothing is claimed. Every one of these rows is this process's own (created by its pipeline or claimed by its recovery), it still holds the fence token, and the pass that produced the row has finished with it. So no in-flight batch, of this engine or another, is ever touched. A row can outlive its lease, though, and an engine that claims it then may be about to re-read a range below it. So before the source is touched, the engine renews its claim in one fenced write (`StateStore::renew_claim`, conditional on the claim generation it holds). After a renewal no other engine can claim the row until the new lease runs out. A refused renewal means another engine owns the row: it is dropped without moving the source, and that engine's recovery commits it. Detecting the lost claim at the ledger write after the source commit would come too late, since a source commit cannot be undone and with `delete_after_commit` it can delete the file.

   Committing a range its source has already passed must not move the source back. The Kafka adapter skips a commit at or below its frontier (#617). The file and spool adapters skip one at or below a committed point that this process confirmed, and only when `continuity::proves` (#647) shows that the file now at the path is both the confirmed file and the one the lower commit's marker was read from: the same inode, no shorter than any recorded length, both first-page fingerprints known and matching, and every recorded tail holding. Unknown content never proves (review of #659). Otherwise the commit moves the cursor as before. Before, they set the cursor to the stale end, and for a range read before a restart they moved the read head back too. The seed alone is not trusted for this: it can be a previous generation's (#564). Such a row is recorded `SOURCE_COMMITTED`: it is `VERIFIED`, and its source has committed through its end. A `VERIFIED` row that starts where the pending range starts — the object that replaced a stale row for the same bytes — commits normally. A marker with no inode (a legacy row) is compared against every inode at its path.

   **A rewind never goes below what the source has already committed.** The file and spool adapters have always clamped to their committed byte; the Kafka adapter clamps to the ledger's committed frontier for the partition (its newest committed batch, or what the process committed since it started) or, for a partition the ledger does not know, to where an earlier rewind in this process landed. It asks the broker for the consumer group's committed offset only for a partition it has none of these for (#614) — a stale engine's commit can move the group's offset past a range recovery is about to re-read, and clamping to it skipped that range for good. Either way, a stale `FAILED` row left behind by a failure that has since been repaired no longer rewinds the group to its start and re-archives everything since on each restart.

   **A range the source has committed past is retired, not handed back (#567).** Before the rewind is planned, recovery asks the row's adapter for the committed point of the row's unit (`SourceAdapter::committed_through`). At startup that is the ledger's newest committed row for the unit, newest by `(created_at, batch_id)` as in both stores and in pruning, the same row the restart cursor is seeded from, which retention never prunes. For Kafka it is the ledger frontier the adapter trusts (#614). For files and spools it is the cursor's committed byte, answered only when the row's marker is PROVEN to be the cursor's file: the same inode (a legacy marker without one is never retired), and the same content - both fingerprints matching the first bytes of the file now at the path, equal ones too, since equal fingerprints say only that the marker and the cursor read the same content, not that the file still holds it - and, for a marker with a tail fingerprint, the same bytes below its end, and for a cursor with one, the same bytes below its position; and the file no shorter than any length or position either recorded. This is one test (`continuity::proves`), shared with the choice of the restart seed and the re-read of a rotated file (#621, section 7.2). Anything less keeps the replay path, because `SUPERSEDED` is terminal. If that point is at or past the end of the row's range, the rewind would land there and read nothing again. So the row moves to `SUPERSEDED` (from `FAILED` or `REPLAY_REQUIRED`; an in-flight row is written `FAILED` first), it is counted in the summary as superseded, not as a replay, and no rewind is planned for it. `SUPERSEDED` is terminal. Neither the claim nor the incomplete listing returns it, so later restarts never touch it, and retention deletes it with committed history, digesting it first. If a write fails, nothing is rewound: a row that was `FAILED` or `REPLAY_REQUIRED` stays as it was, and an in-flight row whose `FAILED` write landed but whose `SUPERSEDED` write did not is left `FAILED`. Nothing below the committed point would be read anyway, and the next recovery decides again. A row whose range spans two files, or whose adapter cannot say, keeps the replay path. Before this, every such row was claimed at every restart, written `FAILED` and `REPLAY_REQUIRED` again, counted in `replay_required_total`, and given a no-op rewind, and retention never removed it.

   `SUPERSEDED` claims only that the source committed past the range. It does not claim that a committed range covers the range's bytes. A ledger written before #552 can hold a committed range above a failed one, and that row is now retired like any other, because its rewind was already clamped to nothing. The coverage audit (#560) still judges a `SUPERSEDED` range exactly as a `FAILED` one and names it in any hole.
3. **A replay that produces the same bytes resolves to the object already stored (#626).** The object key, under the partition prefix (`vtop_core::batch::build_object_name`), is `vtop-<engine>-<source>-<range>[-i<inode>]-<digest>.<format>[.<compression>]` for a file or spool range and `vtop-<source>-<range>-<digest>.<format>[.<compression>]` for a Kafka range: the source name, the batch's whole range (`ProgressMarker::spanning`) and the first 32 hex digits of the object's checksum, plus, for a file or spool file, the engine name and the inode. An object is shared only by attempts at one unit's range. A file or spool file is one host's unit, so two engines archiving byte-identical windows of their own files, or a new file at a path holding the old file's bytes, get separate objects. A Kafka topic-partition is one unit wherever it is read, so an engine that replays a range another engine sharing the ledger uploaded finds that object. The name does not carry the topic's ID: a topic deleted and recreated under the same name restarts its offsets at 0, and a new incarnation whose records are byte-identical at the same offsets would reuse the old incarnation's object, so a reader deduping by object URI would drop them. Only byte-identical records can do this, and the Kafka source reports a recreated topic as `recreated_or_truncated` in `vtop_kafka_partition_events_total` (#675); adding the topic ID to the name is left for later. Compression output is a function of the records and the level within one build (gzip writes no timestamp; a compression library upgrade may change the bytes and so the key, which costs one more object, nothing else), so the same range of the same source re-read with the same settings gives the same key. Before the put, the engine looks at that key:
   - **absent** (the backend's HEAD answers `NotFound`): the batch is put there;
   - **holds an object that verifies strongly** against the batch's size and full digest: the put is skipped and the object is reused (`object_key_reused` in the log). The pipeline then verifies it again like any other, so `VERIFIED` still rests on a read of the stored bytes;
   - **anything else** (the object there does not verify, verifies by size only, or the probe failed with anything but a throttle): the key is left alone and the batch is written under its **attempt key**, named by its `batch_id` as before, with the reason logged as `object_key_fallback`. Nothing that is already stored is ever overwritten, so a damaged object stays for the scrub to report, and a key that would need an overwrite under Object Lock is never written. A throttled probe fails the batch like any other stage.

   Two writers of one derived key write the same bytes by construction, so the race between the probe and the put changes no content; in a versioned bucket it adds an identical version at worst. The put must still replace the key whole: S3 puts are atomic, and `localfs` copies beside the key and renames onto it, so a crash mid-copy cannot tear an object that a committed row already verified. The probe is the only guard against writing over an object that does not verify; a store-enforced conditional put (`If-None-Match: *`) would close the probe-to-put window as well, and is not used yet.

   **What the operator is told.** Every outcome is one log event with `batch_id`, `source`, `range` and `key` (and `fallback_key`, `reason` where they apply) as fields, and a message that says what happened, what the engine did and what to do. Every outcome is also counted in `vtop_object_key_decisions_total{decision}`. The two notices a setting repeats on every batch (`object_key_per_attempt`, and `object_key_fallback` when the store refused or failed the check of a key) are logged at most once a minute per kind and source, with a `suppressed` count; the counter is never limited, and a fallback about one object (it differs, verifies by size only, or cannot be read) is always logged:

   | `event` | Level | Condition and action |
   |---|---|---|
   | `object_key_derived` | info | nothing stored at the derived key; the batch is uploaded under it |
   | `object_key_reused` | info | "object for range R of S already archived and verified; reusing key K, no upload" |
   | `object_key_fallback` | warn | the key holds an object that does not verify (`fallback_differs`), verifies by size only (`fallback_size_only`) or cannot be read back (`fallback_unreadable`), or the store refused or failed the check of the key (`fallback_unchecked`); the batch goes under its attempt key instead of overwriting, and the message says what to check (`vtopctl scrub`, a content-verifying backend, list permission when the store answered 403, the store's health) |
   | `object_key_probe_throttled` | warn | the store throttled the probe; nothing was written, the batch fails and its range is handed back |
   | `object_key_per_attempt` | info | no checksum, or a backend that cannot report an absent key; the batch is written under its attempt key and a replay will write another object |

   **What stays per attempt, and why.** The `batch_id` keeps its stamp and random suffix: it names the ledger row and the manifest (`{batch_id}.manifest.json`). A ledger row is one attempt: a FAILED or `REPLAY_REQUIRED` row and the row of the replay that succeeded are both kept, and a unique id per attempt keeps their transitions and claims apart (`batch_id` is the primary key of both stores). A manifest is per attempt for the same reason: it carries the `batch_id`, its own `created_at` and the self-hash and version id its row pinned (#135), so one attempt's manifest written over another's would break the pinned row's binding. Several manifests can therefore name one object, and they are always attempts at one unit's range with the same bytes, so a reader can dedupe them by the object URI. Batches of different units never share an object.

   **Where a replay still writes a second object.**
   - The batch boundaries differ. A re-read starts where the replayed range started but ends where the source and the read budget end now, not where the original batch ended. So a replay on a source that has grown since (a file still being written, a Kafka partition with traffic), and a re-read after a power loss that lost several committed rows (which reads what were several batches as one), produce other bytes and so another key. Reuse is reliable when the source has not grown past the original end, which is what a crash or a failed upload on an idle or caught-up source looks like. Reproducing the original boundaries needs a boundary record the replay can read, which this does not add. A power loss still archives again at most what was in flight (§12, `ledger_power_loss.rs`).
   - The partition prefix differs: under `partitioning.time_source = seal` the replay is filed under the hour it was sealed in, so a replay in a later hour gets a different key. `event` keeps the original hour for Kafka.
   - No digest names the object: with `checksum.algorithm = none`, or on a backend whose HEAD cannot tell an absent key from a refused request (the command backends: `awscli`, `minio`, `s3cmd`; native S3 answers `NotFound` only for a 404, and a 403 from a caller without list permission falls back), every attempt uses its attempt key, as before.
   - An archive written before #626 used attempt keys only; a replay of one of its ranges does not find that object by name and writes a derived one beside it. Recovery, scrub and the coverage audit read object locations from the ledger and the manifests, never from the shape of a key, so archives with either kind of key, or both, are recovered, scrubbed and audited as before.
4. A state/storage mismatch (e.g., an object present but unverified) is resolved by re-verifying or re-uploading **before** any commit.
5. **While the engine is running, a failed range is handed back at once, and the hand-back does not wait on the ledger.** Adapters advance their read position when they *read*, not when a batch commits, so a batch that fails (or errors before `VERIFIED`) is handed back to its source at the end of the cycle that failed it, not at the next restart. Otherwise the next cycle reads on from the end of the failed range and the first later range to verify commits past it, leaving those bytes unarchived (#552).

   The ledger records the decision as recovery does — `FAILED`, then `REPLAY_REQUIRED` — but, **unlike recovery (item 2), the rewind is unconditional**: it happens when the `FAILED` write fails, when the `REPLAY_REQUIRED` write fails (the row stays `FAILED`), and after any other error before `VERIFIED`, such as a ledger write that failed mid-pipeline, when the engine cannot even tell whether the ledger holds a row for the batch (#627). The two paths start from different read positions, and that is why one rule does not fit both:

   - At startup the read head is seeded at the committed point, which is at or below every uncommitted range. A rewind withheld there leaves the range ahead of the head.
   - While running, the head is already past the failed range. A rewind withheld there lets the next cycle read on and a later batch commit past the range; the restart that then marks the stale row for replay clamps its rewind to that commit, and the range is never archived.

   So a rewind on an unrecorded decision costs at most a duplicate, and withholding it costs the data. The duplicate is the object the failed attempt may have left in the store, with no `VERIFIED` row naming it; the range itself is archived once, by the batch that re-reads it. The stale row, if one was written, is visited by the next recovery, which records the replay decision and rewinds — clamped to the committed point, so nothing already archived is read again. The same holds for a write refused by the claim fence (#557): it means another engine claimed the row, and since sources are not divided between engines (two engines on one store are unsupported, [PRODUCTION_HA.md](PRODUCTION_HA.md) §19), this engine cannot know that the claimant will read the same bytes, so it hands its own range back too. A batch that reached `VERIFIED` is never handed back: its object is in the archive, and a failed source commit leaves it `VERIFIED` for recovery to retry (item 1).

   A cycle in which a batch failed waits before reading again — the idle interval, doubling per consecutive failing cycle up to a minute, plus jitter — because the failed range is now read again and a tight loop would hammer a store that is down.

   Limits, stated rather than implied: a ledger written by a build *before* #552 can already contain a committed range above a failed one. Replay does not repair that for file and spool sources — their rewind is clamped to the committed point so a superseded failed row cannot re-archive committed data on every restart — and the ledger cannot say whether the older range was later covered, because committed history may be pruned. Finding such holes is the job of an archive coverage audit (#560), not of recovery. A spool whose identity changed since the batch was read cannot be rewound; that is reported as an error and the range is not re-read — but only for a marker that recorded an inode: a legacy marker without one keeps the old size-only check and can rewind a replacement. The file adapter checks identity on replay the same way: a range whose file now has another inode (rotation, #558), is shorter than when the range was read, or keeps the inode but has different first bytes (a same-inode replacement, #578) is refused rather than re-read - unless the file it was read from can still be reached and is proven to be that file, in which case the range is re-read from it (section 7.2, #621): through a descriptor the adapter kept on it in the same process, or, after a restart, under whatever name it has by then, found by inode among the entries of the path's directory and the files the source's patterns match. A range read before a `copytruncate` is still refused - the copy has a new inode, so it cannot be proven to be the file - as is one whose rotated file was compressed or removed before a restart. The inode and content checks apply only to a marker that recorded them; a legacy marker keeps the size-only check against the file at its path, and is never re-read from a rotated file. Every refused rewind, in a cycle or in recovery, is counted in `vtop_source_replay_refused_total{tenant,source_type}` as well as logged.

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
- **Every call the engine makes to its object store and state store has a deadline** (#556). A store that accepts a connection and never answers would otherwise block the awaiting cycle forever, with the process alive and the health endpoint green. The native S3 backend sets the SDK's first-byte (read), per-attempt and whole-operation limits from `upload.request_timeout_seconds` (default 300 s per attempt; the whole operation is allowed the SDK's three attempts); its connect limit is a fixed 10 s, not configurable — the SDK's own defaults bound connecting but not waiting for a response, which the `request_deadline` test shows against a socket that accepts and stays silent. Those SDK limits end when the response headers arrive, so every response BODY the engine reads afterwards (verification, manifest recovery) is bounded separately, by the whole-operation allowance; a store that sends headers and then drips the body fails the read rather than holding it open. The compatibility backends use `upload.command_timeout_seconds`. The state store is wrapped so every request is bounded by `engine.state_store_timeout_seconds` (default 30 s; whole-ledger scans such as startup recovery get ten times it), and opening a Postgres store (connect plus schema check) is bounded by the same value. An expired request is an ordinary failure to the engine — a failed upload fails the batch, and recovery re-reads its range — but it is an **unknown outcome**, not a failed one: the store may have applied it. That is safe here because every ledger transition is checked against the state machine and recovery re-reads rows it finds incomplete; the worst case is a range archived twice, never one lost. A request that carries a body — an object, a multipart part, a manifest — is allowed `request_timeout_seconds` plus its size at `upload.request_min_bytes_per_second` (default 4 KiB/s) for each attempt, and its connector read timeout is disabled so the attempt timeout alone bounds it (#663). A fixed deadline on a body is a deadline on the request's share of the link: on a policed 10 Mbit/s uplink at 100 ms the shipped width put 24 part uploads on the link at once, each moved about 11 KB/s, and an 8 MiB part could not cross in 300 s, so every attempt was cut and re-sent from its first byte and the default configuration committed nothing. The floor is per request because the width times the parts in flight share the uplink. A store that stops reading a body is still caught within seconds by the SDK's stalled-upload protection; what the scaled allowance lengthens is how long a store that has read a body may take to answer. Not covered: `vtopctl migrate`, an operator-run one-shot command whose Postgres connection and schema statements carry no deadline (a schema change on a large ledger can legitimately be slow) — an operator who sees it hang interrupts it, and the migration's statements are idempotent, so running it again is safe.
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
| Deterministic object naming (§11, §13) | ✅ objects (#626) | Object keys derived from source, range and content digest; reused only after strong verification. Narrowed, not closed: manifests stay per attempt, and the limits in §13 item 3 still produce a second object. |
| Multi-instance fleet (consumer groups + multi-writer recovery) | ⬜ planned | Phase 5; single-instance lock (#66) until then. |
| Engine Kubernetes chart | ⬜ planned | Phase 7 — `helm/vtop` deploys the cluster plane, not the engine. |
| File/syslog HA (leases) | ⬜ optional | Phase 9, gated on an explicit product decision. |

# What VTOP is

VTOP Engine reads telemetry from **Kafka topics, log files and syslog spool
files**, packs it into compressed batches, and writes each batch to
**S3-compatible object storage** with a manifest describing it. It is written
in Rust and ships as two binaries: `vtopctl`, the archive engine and operator
CLI, and `vtop-node`, an optional replicated log.

## The problem

Moving bytes into a bucket is easy. Knowing when it is safe to let the source
forget them is not.

If a pipeline advances the Kafka offset, or the position in a log file, as
soon as it has *sent* a batch, then any failure after that point loses data
silently: an upload that died half-way, an object that landed corrupted, a
store that acknowledged and then lost the write. Nothing reads the source
again, because the source says that data is done.

## The one rule

!!! info "Source progress is committed only after the stored object is verified."

For every batch, VTOP:

1. reads records from the source and seals them into a batch;
2. compresses it and computes its checksum;
3. uploads the object, then a manifest that ties the object to the exact
   source range it came from;
4. **checks the copy in storage** against that checksum;
5. only then commits the source position: the Kafka offset, the file byte
   offset or the spool offset.

If anything fails before step 5, the source position has not moved, so the
data is read again. A crash produces a replay, never a gap. Every batch's
progress through these steps is recorded in a state ledger (SQLite or
PostgreSQL), which is how the engine recovers after a restart.

The rule is enforced in one place in the code: a commit from any state other
than `VERIFIED` is refused. The normative statement is
[protocol §13, the commit rule](../VTOP_PROTOCOL_DRAFT.md#13-commit-rule).

## What you get

- **No silent loss between source and bucket.** Verification failures stop
  the commit, and the data stays replayable.
- **A record you can audit.** Each object has a manifest naming its source
  range, checksum, format and compression, and the ledger records every
  batch. [`vtopctl audit-coverage`](../operate/scrub-and-audit.md) checks the
  archive for holes, and `vtopctl scrub` re-verifies stored objects later.
- **One engine for mixed sources.** Kafka, files and syslog spools, in CEF,
  LEEF, JSON, JSON Lines, syslog or raw, with the format detected per batch
  when you do not set it.
- **Any S3-compatible store,** through the native S3 client, or a local
  directory for air-gapped sites.

VTOP is a prototype and reference implementation, published under the MIT
license. The [known limitations](../reference/limitations.md) list what it
does not do yet.

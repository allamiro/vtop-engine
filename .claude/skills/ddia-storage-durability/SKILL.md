---
name: ddia-storage-durability
description: Review or design on-disk and storage-path code in VTOP — the append-only segment log (vtop-log), fsync/durability boundaries, torn writes, checksums, sealed segments, retention, tier copies, compaction-like cleanup, and the SQLite/Postgres state store. Use when touching vtop-log, vtop-meta/storage, vtop-state, group commit, or retention/transfer code.
---

# Storage and durability

Book lens: chapter 3 (storage engines: log-structured vs B-tree, write amplification, column/analytics storage), chapter 7 (transactions/isolation), chapter 12 (end-to-end correctness). Apply to `vtop-log` first; it is a log-structured design (append-only segments, sparse index, sealing, retention).

## Map

- `vtop-log`: `segment.rs` (active segment, index rebuild), `segment_set.rs` (roll, retention, reconfigure), `catalog.rs` (startup catalog, quarantine), `codec.rs`/`codec_v2.rs`, `proof.rs`, `transfer.rs` (verified replica transfer), `*_intent.rs` (truncate/retention intents), `env.rs` + `sim.rs` (storage seam + deterministic simulator).
- `vtop-meta/src/storage`: hard state, chunked checksummed raft log, atomic snapshots, applied/purged cursors.
- `vtop-state`: batch ledger behind the `StateStore` trait.
- Research docs: `docs/WRITE_AMP_PROOF_OVERHEAD.md`, `docs/FETCH_IO_RESEARCH.md`.

## Durability rules

1. **Acknowledge only what survives the fault you claim to tolerate.** `Durability::Buffered` means OS buffer only; `Fsync` flushes data *and* a checksummed commit boundary. Any ack path must say which one it provides and the docs must match.
2. **Data fsync is not directory fsync.** File contents become durable on `sync_data`; creates, renames and removals on `sync_dir`. The simulator models exactly this — every new file, rename, or delete needs the directory sync in the right order.
3. **Rename publishes atomically; fsyncs make the result survive a crash.** On one filesystem a rename is atomic regardless of fsync — readers see the old file or the new one. Durability is separate: fsync the file *before* the rename (so the new name never points at unwritten data) and the directory *after* (so the rename itself is not lost). Snapshots, hard state, and intent files follow this; new files must too.
4. **Tolerate torn and partial writes.** Recovery must accept any in-order prefix of the volatile op log with the last write cut at any byte. Records are length-prefixed and checksummed; recovery truncates the torn tail and quarantines what it cannot trust (never silently deletes).
5. **Never acknowledge past a failed fsync.** After a failed fsync the kernel's view of the dirty pages is unknown, so on the produce path fail the node's write path rather than retry-and-acknowledge (scenario 05b). A retry is acceptable only where nothing is acknowledged or installed until a sync actually succeeds: the transfer staging path (`StagedSegment::finish_artifact`) deliberately keeps its handle when `sync_data` fails so a later call can retry. Say which of the two a new path is.
6. **Checksums catch bit rot and bugs, not just crashes.** Verify on read paths that matter (transfer, tier copy, repair) and make verification strength explicit.
7. **Destructive steps are two-phase and intent-logged.** Truncate and retention write an intent, act, then clear it, so a crash in the middle resumes rather than corrupts. New destructive operations follow the same shape.
8. **Separate local retention from replica retirement.** Local log-prefix retention (`SegmentSet::retain`) reclaims segments against the **committed floor** and the configured bound; state which floor gates it. **Retiring an old replica, or deleting a tier object, must be evidence-gated**: do not do it until the replacement is proven (verified replica / verified tier copy recorded) — the v0.3.0 lesson in `docs/ROADMAP.md`.

## Trade-offs to state explicitly

- **Write amplification and group commit.** `group_commit.rs` shares one durability barrier across producers; ask what latency/throughput the batching window trades and whether ack still follows the shared barrier.
- **Log vs B-tree.** Sequential appends are cheap and recovery is a scan; random-update structures need a WAL anyway. Don't add in-place updates to segment files.
- **Background reclaimers need backpressure signals.** Retention, compaction, tier copy and repair each need a backlog metric and an alert; state what happens when one cannot keep up (default: nothing throttles writes and the disk fills). A retention pass that is blocked (floor) or failing must be observable — not silently `eprintln!`.
- **Retention vs slow consumers.** A log is a bounded buffer; a consumer whose cursor falls below the retention floor loses data, and only that consumer. State whether group cursors are consulted; export per-group lag and time-to-eviction and alert on it. Do not protect a slow consumer by growing the log without bound.
- **In-memory state has a cardinality bound.** For every map (producer windows, sequence maps, metadata KV) state what bounds it, what evicts, and the restart cost as a function of retained bytes and key count. Sealed-segment open that re-hashes every byte makes restart O(retained data).
- **Verify/repair/transfer reads are analytics-shaped.** Throttle or isolate them from the produce path and measure foreground p99 while they run. Opening an artifact for audit must not mutate it (no sidecar rewrites on a read-only mount).
- **Count durability barriers per ack** (data fsync, sidecar fsync, dir fsync), not only bytes written.
- **Read amplification of indexes.** Sparse index + chunk sidecar rebuild must be derivable from the segment alone (`rebuild_index`); an index that cannot be rebuilt is a second source of truth.
- **SQLite vs Postgres ledger.** Same trait, different failure modes (single-node file vs networked DB with its own replication lag and failover). Treat Postgres failover as a source of in-doubt commits; the batch state machine must tolerate a ledger that rolls back to an earlier state.

## Isolation notes for the ledger

If a change makes two engines (HA) touch the same batch row: lost updates, write skew and phantoms are the real risks.
- **Lost update:** any read-validate-write on a batch row loses a concurrent change unless the write is conditional on what was read (`UPDATE ... WHERE state = $expected`, check rows affected, re-read on zero). Check both backends — conditional on one is not conditional on the other. A serialization-failure retry must repeat the whole read-validate-write.
- **Write skew / phantoms** need a multi-row invariant ("no two live batches cover one byte range of a path", "progress equals a contiguous verified prefix"). Row CAS cannot protect these: use a unique constraint, a lock row per source, a single writer per source, or SERIALIZABLE with retry — and name which one holds.
- "Single engine per store" (instance lock) is the repo's serial-execution isolation. List every path that bypasses it (shared Postgres across hosts, duplicate engine names).
- **A lease column is a lock with a timeout, not a fence.** Add a claim generation to every later UPDATE of a claimed row.
- **The ledger is derived data.** It is an index over the manifest log. Can it be rebuilt from manifests, and is a rolled-back ledger (Postgres failover) detected by comparing against the object store?

## Checklist

- [ ] What is the exact byte state on disk after a crash at each operation? (Run it under `sim` crash sweep.)
- [ ] Which syscalls make each step durable, and in what order?
- [ ] Is every size read from disk bounded before allocation?
- [ ] Does recovery report (RecoveryReport / quarantine) what it dropped?
- [ ] Is any new on-disk field versioned? See `ddia-encoding-evolution`.
- [ ] Is there a regression test in `crates/vtop-log/tests/` (`sim_crash_sweep.rs`, `startup_crash_matrix.rs`) that fails on the old code?

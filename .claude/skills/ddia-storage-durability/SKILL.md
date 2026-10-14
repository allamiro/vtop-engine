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
3. **Write-then-rename is atomic only if you fsync before the rename and the dir after.** Snapshots, hard state, and intent files follow this; new files must too.
4. **Tolerate torn and partial writes.** Recovery must accept any in-order prefix of the volatile op log with the last write cut at any byte. Records are length-prefixed and checksummed; recovery truncates the torn tail and quarantines what it cannot trust (never silently deletes).
5. **fsync failure is not retryable.** After a failed fsync the page cache state is unknown; fail the node's write path rather than retry and acknowledge (scenario 05b).
6. **Checksums catch bit rot and bugs, not just crashes.** Verify on read paths that matter (transfer, tier copy, repair) and make verification strength explicit.
7. **Destructive steps are two-phase and intent-logged.** Truncate and retention write an intent, act, then clear it, so a crash in the middle resumes rather than corrupts. New destructive operations follow the same shape.
8. **Retention must be evidence-gated.** Do not retire a segment until the evidence (verified replicas / verified tier copy) is recorded — the v0.3.0 lesson in `docs/ROADMAP.md`.

## Trade-offs to state explicitly

- **Write amplification and group commit.** `group_commit.rs` shares one durability barrier across producers; ask what latency/throughput the batching window trades and whether ack still follows the shared barrier.
- **Log vs B-tree.** Sequential appends are cheap and recovery is a scan; random-update structures need a WAL anyway. Don't add in-place updates to segment files.
- **Read amplification of indexes.** Sparse index + chunk sidecar rebuild must be derivable from the segment alone (`rebuild_index`); an index that cannot be rebuilt is a second source of truth.
- **SQLite vs Postgres ledger.** Same trait, different failure modes (single-node file vs networked DB with its own replication lag and failover). Treat Postgres failover as a source of in-doubt commits; the batch state machine must tolerate a ledger that rolls back to an earlier state.

## Isolation notes for the ledger

If a change makes two engines (HA) touch the same batch row: lost updates, write skew and phantoms are the real risks. Use compare-and-set on state (the transition check already is one), not read-modify-write in application code; keep uniqueness constraints in the database.

## Checklist

- [ ] What is the exact byte state on disk after a crash at each operation? (Run it under `sim` crash sweep.)
- [ ] Which syscalls make each step durable, and in what order?
- [ ] Is every size read from disk bounded before allocation?
- [ ] Does recovery report (RecoveryReport / quarantine) what it dropped?
- [ ] Is any new on-disk field versioned? See `ddia-encoding-evolution`.
- [ ] Is there a regression test in `crates/vtop-log/tests/` (`sim_crash_sweep.rs`, `startup_crash_matrix.rs`) that fails on the old code?

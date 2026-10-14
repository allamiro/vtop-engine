---
name: ddia-replication-consensus
description: Reason about replication, leases, fencing epochs, quorums, clocks, linearizability, and consensus in the VTOP cluster plane (vtop-meta Raft, vtop-broker replication, vtop-node lease agent/watcher/promotion). Use when changing or reviewing leader election, range leadership, promotion, replica truncation/repair, membership changes, or anything that depends on time or on "who is the leader".
---

# Replication, consensus, and time (cluster plane)

Ideas from DDIA part II (replication, transactions, the trouble with distributed systems, consistency and consensus), applied to the code. Design source: `docs/NATIVE_BROKER_ARCHITECTURE.md`; shipped limits: `docs/ROADMAP.md`.

## Map

| Concern | Where |
|---|---|
| Replicated metadata (Raft, CAS generations, fencing epochs, exactly-once dedup table) | `crates/vtop-meta` (`state.rs`, `raft/`, `storage/`) |
| Placement over failure domains | `vtop-meta/src/placement.rs` (weighted rendezvous) |
| Range lease acquire/renew/lose | `vtop-node/src/lease_agent.rs`, `lease_watcher.rs`, `promotion.rs` |
| Broker refuses requests under a stale epoch | `vtop-broker/src/fencing_epochs.rs`, `replication/` |
| Commit boundary / quorum ack | `vtop-broker/src/committed_floor.rs`, `group_commit.rs` |

## Principles to apply

**Nodes cannot know; only a majority can decide.** A node that believes it is leader may have been paused, partitioned, or deposed. Truth is whatever a quorum agrees on, and a node must act on it, not on its own belief.

**Fencing beats timing.** A lease alone is unsafe: a process can stall (GC, VM pause, IO) past its expiry and then write. The defence is a monotonically increasing token (here the fencing epoch) that the *resource* checks and rejects when stale. `lease_agent.rs` is the model: safety rests on `fencing_epoch + 1` at acquisition, timers are only liveness tuning. Any new write path must check the epoch at the point of the write, not at the point of the decision.

**Offsets are not identities.** Two replicas reporting offset 90 need not hold the same record. Reconcile on `(epoch, start_offset)` history, not on high-water marks — the leader-epoch lesson `fencing_epochs.rs` already encodes. Truncation decisions need the first diverging epoch.

**Clocks lie.** Wall-clock time-of-day can jump or skew; only monotonic clocks measure durations. Never order events or decide expiry across machines by wall time. Scenario `scripts/live-chaos/scenarios/07-clock-skew.sh` is the regression guard; keep elections and lease expiry on monotonic time.

**Quorum intersection.** For `n` replicas, write quorum `w` and read quorum `r` give overlap only if `w + r > n`, and even then sloppy quorums, concurrent writes, and partial failures break the guarantee. Prefer Raft's single linearizable log for metadata; do not reintroduce leaderless semantics on top of it casually.

**Linearizable reads cost a round trip.** `ReadFence` exists for that reason. If a read need not be linearizable, say so; if it must, route it through the fence.

**Replication lag is user-visible.** Follower reads can return stale data, non-monotonic results, or out-of-order effects. Name which read guarantee (read-your-writes, monotonic, consistent prefix) each API offers.

**Exactly-once is idempotence plus dedup.** The metadata state machine's request-id dedup table (`DEDUP_CAPACITY`, travels in snapshots) and the producer sequence window in `vtop-log` are the pattern: retries must be safe by construction. Check the dedup window is bounded and its overflow behaviour is explicit.

**Atomic commit across nodes is expensive and blocking.** If a change needs cross-node atomicity, prefer a single-log decision (Raft proposal) over ad-hoc two-phase protocols; if a coordinator can fail between prepare and commit, in-doubt participants hold locks until it recovers.

## Review checklist

- [ ] For each state change: which node decides, who else can decide concurrently, and what stops the loser?
- [ ] Is every stale-leader write rejected by the **receiver** with an epoch check?
- [ ] What happens on: leader pause longer than the lease, partition isolating the leader, partition isolating a minority, both old and new leader alive, follower disk full or fsync failure (scenarios 05, 05b, 06, 09, 14)?
- [ ] Membership change: is a quorum of the old *and* new configuration required during transition (scenarios 01-03, 12)?
- [ ] Any duration computed from `SystemTime`? Replace with a monotonic source.
- [ ] Snapshot / purge / reopen: does recovery invent log ids or state it never persisted (`meta.applied`, `meta.purged`, `meta.membership_log_id`)?
- [ ] Is an unavailability trade-off acceptable and documented? Consistency vs availability under partition is a choice per operation, not a global label.

## Evidence to ask for

A new safety claim needs a scenario in `scripts/live-chaos/scenarios/` or a deterministic sim test (see `ddia-fault-testing`), plus a line in `docs/LIVE_CHAOS_VALIDATION.md`. "It worked in the lab" is not a proof of fencing.

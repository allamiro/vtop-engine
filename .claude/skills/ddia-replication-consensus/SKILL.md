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
| Placement over failure domains | `crates/vtop-meta/src/placement.rs` (weighted rendezvous) |
| Range lease acquire/renew/lose | `crates/vtop-node/src/lease_agent.rs`, `lease_watcher.rs`, `promotion.rs` |
| Broker refuses requests under a stale epoch | `crates/vtop-broker/src/fencing_epochs.rs`, `replication/` |
| Commit boundary / quorum ack | `crates/vtop-broker/src/committed_floor.rs`, `group_commit.rs` |

## Principles to apply

**Nodes cannot know; only a majority can decide.** A node that believes it is leader may have been paused, partitioned, or deposed. Truth is whatever a quorum agrees on, and a node must act on it, not on its own belief.

**Fencing beats timing.** A lease alone is unsafe: a process can stall (GC, VM pause, IO) past its expiry and then write. The defence is a monotonically increasing token (here the fencing epoch) that the *resource* checks and rejects when stale. `lease_agent.rs` is the model: safety rests on `fencing_epoch + 1` at acquisition, timers are only liveness tuning. Any new write path must check the epoch at the point of the write, not at the point of the decision.

**Offsets are not identities.** Two replicas reporting offset 90 need not hold the same record. Reconcile on `(epoch, start_offset)` history, not on high-water marks — the leader-epoch lesson `fencing_epochs.rs` already encodes. Truncation decisions need the first diverging epoch.

**Clocks lie.** Wall-clock time-of-day can jump or skew; only monotonic clocks measure durations on one machine, and Raft elections here are monotonic. But a deadline that travels *in the replicated log* cannot be monotonic — a reading from one machine means nothing on another. Range-lease deadlines are therefore proposer-stamped wall-clock instants (`issued_at_ms`, see the header of `crates/vtop-meta/src/command.rs`). Consequences to design for:
- Skew on a **proposer** has three failure directions. A fast-clocked candidate judges a live rival's lease expired and steals it repeatedly. A fast-clocked holder stamps its deadline in the future, so after it dies nobody can take over until the skew elapses. A slow-clocked holder stamps a deadline already in the past (or too short), so a correctly clocked contender acquires a higher epoch early while the holder still believes its lease is live. The epoch keeps all three *safe*; none is kept *live* or *prompt* by it.
- Treat `issued_at_ms` as an unbounded input: the metadata leader should stamp it, or reject values further than a stated bound (a fraction of the lease) from its own clock, before proposing. Apply stays deterministic because the stamped value is data in the log. That bound **is** the skew policy; write it down next to the lease duration.
- The replicated wall-clock deadline stays the authority for *when takeover is legal*; a holder must stand down no later than that deadline (minus margin) or it serves past the point a rival may legitimately take over. Use monotonic time only for **local elapsed budgets** (RPC deadlines, time since the last successful renew, how long a call has been blocked), and stand down at the earlier of the two views — never delay demotion because a local monotonic budget has not yet run out.
- `07-clock-skew.sh` skews a Raft voter only. A skewed **lease proposer** (data node) needs its own scenario; see `ddia-fault-testing` for the separate skew-before-renewal and skew-after-renewal cases.

**Quorum intersection.** For `n` replicas, write quorum `w` and read quorum `r` give overlap only if `w + r > n`, and even then sloppy quorums, concurrent writes, and partial failures break the guarantee. Prefer Raft's single linearizable log for metadata; do not reintroduce leaderless semantics on top of it casually.

**Linearizable reads cost a round trip.** `ReadFence` exists for that reason. If a read need not be linearizable, say so; if it must, route it through the fence.

**Offsets that leave the broker must come from the quorum-committed mark.** ListOffsets / high-water marks, committed cursors, receipts and lag metrics derived from the local `next_offset` or local commit can go backwards across failover (a promoted replica may reissue offsets the old leader handed out). Every path that reports a position needs the same gate fetch has while a promotion boundary is pending.

**The replica set must come from replicated state.** If each node derives its quorum size from its own config (leader + configured followers), a mixed-config window gives disjoint majorities and an acknowledged write can be overwritten. Carry the replica set (or a hash of it) in fence/append frames and refuse on mismatch, or store it in the range record and require old and new majorities during change. An even replica count adds a larger majority and no extra fault tolerance; RF=2 tolerates zero faults.

**A write that returned "quorum miss" may still commit later.** Retries must be idempotent (producer sequence window). A replica restored from an older copy under the same identity silently forgets acks it gave — treat a restore as a new, empty replica identity.

**Split-brain safety catches must be asymmetric.** Any "stand down on seeing a rival" path decides by epoch comparison (higher wins). A symmetric rule makes both nodes stop. Test both orderings.

**Failover timing is a load decision.** Too-short leases turn GC/fsync/metadata-commit stalls into spurious failovers, and each failover adds fence + probe + marker traffic. Required margin: `lease - renew` must exceed worst-case metadata commit latency plus stall. Today `LeaseAgentConfig::validate()` only requires `renew_interval < lease_duration` and a non-zero poll interval; a rule such as `renew_interval * 3 <= lease_duration` and a per-RPC deadline below the renew interval is a **validation to add**, not an existing guarantee. Lease renewals share one Raft FIFO with cursor/heartbeat/segment traffic — measure renew-commit p99 under a cursor storm.

When you write that validation, count what one lost round actually costs, from the START of the last good renewal round: the pause before the next round, plus the failed round's whole RPC budget, plus the retry pause - and make those true in the code, not just in the check. Three ways they were not: each call in a round was given the full renew interval (a slow read plus a hung renewal ran two intervals, so give a round one shared budget); the pause was measured from the END of the last round (a slow successful round pushed the next later, so start rounds a renew interval apart); and a skew allowance with a floor can exceed a short lease (cap it at half the lease).

**A pause looks like a partition from outside and like nothing from inside.** After any blocking call, assume the lease may be gone; check the fence at the point of effect, and again after the call. A timed-out call means *unknown outcome*, not *did not happen* — a cancelled proposal can still commit, so every such path needs an idempotent request id or a read-back.

**Replication lag is user-visible.** Follower reads can return stale data, non-monotonic results, or out-of-order effects. Name which read guarantee (read-your-writes, monotonic, consistent prefix) each API offers.

**Exactly-once is idempotence plus dedup.** The metadata state machine's request-id dedup table (`DEDUP_CAPACITY`, travels in snapshots) and the producer sequence window in `crates/vtop-log` are the pattern: retries must be safe by construction. Check the dedup window is bounded and its overflow behaviour is explicit.

**CAP is narrow.** Do not label a component CP or AP. For each operation write what it does on an isolated minority node (wait, error, serve stale) and what it costs when nothing is wrong — linearizability is slow even without faults. Linearizability is a single-object recency guarantee; an invariant spanning several metadata records must be one command applied atomically in the state machine (generation CAS), not read-then-propose from a client. When a decision travels over a second channel (a metadata grant observed by a CLI, then sent to a data node), the receiver must validate the epoch/generation/applied-index that rides with it.

**Uniqueness, locks, leases, membership and atomic commit are one problem.** If a design builds one outside Raft (a file lock, a DB-row lease, a clock- or counter-minted id used as an arbiter), treat it as a consensus reimplementation and require a fencing token on every write.

**Atomic commit across nodes is expensive and blocking.** If a change needs cross-node atomicity, prefer a single-log decision (Raft proposal) over ad-hoc two-phase protocols; if a coordinator can fail between prepare and commit, in-doubt participants hold locks until it recovers.

## Review checklist

- [ ] For each state change: which node decides, who else can decide concurrently, and what stops the loser?
- [ ] Is every stale-leader write rejected by the **receiver** with an epoch check?
- [ ] What happens on: leader pause longer than the lease, partition isolating the leader, partition isolating a minority, both old and new leader alive, follower disk full or fsync failure (scenarios 05, 05b, 06, 09, 14)?
- [ ] Membership change: is a quorum of the old *and* new configuration required during transition (scenarios 01-03, 12)?
- [ ] Any *local* duration computed from `SystemTime`? Use a monotonic source. (Deadlines carried in the log are the documented exception — see "Clocks lie".)
- [ ] Snapshot / purge / reopen: does recovery invent log ids or state it never persisted (`meta.applied`, `meta.purged`, `meta.membership_log_id`)?
- [ ] For each write target, who rejects a stale token? (replica append: epoch equality; metadata: CAS + holder+epoch; offset commit: holder+epoch; batch-ledger rows: check the current code; object store: no conditional writes, so unique key per attempt.)
- [ ] Is an unavailability trade-off acceptable and documented? Consistency vs availability under partition is a choice per operation, not a global label.

## Evidence to ask for

A new safety claim needs a scenario in `scripts/live-chaos/scenarios/` or a deterministic sim test (see `ddia-fault-testing`), plus a line in `docs/LIVE_CHAOS_VALIDATION.md`. "It worked in the lab" is not a proof of fencing.

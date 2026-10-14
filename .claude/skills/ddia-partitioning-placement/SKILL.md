---
name: ddia-partitioning-placement
description: Review partitioning, key ranges, range splits, replica placement, rebalancing, and request routing in VTOP (vtop-meta ranges/placement, vtop-log key ranges, the Kafka gateway's partition view, Helm replica counts). Use when touching SplitRange, routing hashes, topic/partition counts exposed to Kafka clients, placement, rebalance intents, or metadata indexes.
---

# Partitioning, placement, and routing

Book lens: chapter 6 (partitioning by key range vs hash, skew and hot spots, secondary indexes, rebalancing, request routing), plus the placement half of chapter 5. Where to look: `crates/vtop-meta/src/{placement,state,keys}.rs` (ranges, split, rebalance intents), `crates/vtop-log/src/types.rs` (`KeyRange`, `RangeLineage`), `crates/vtop-kafka` (partition view shown to Kafka clients), `docs/NATIVE_BROKER_ARCHITECTURE.md`, `helm/vtop/templates/_helpers.tpl` (replica-count validation).

## Principles

1. **Hot spots.** Time-first keys make one hot partition; leading with tenant/source and hashing the prefix spreads load. One hot *key* still lands in one range and splitting cannot spread it. App-level salting needs read fan-in and a record of which keys are salted — decide before it is needed.
2. **Hash mod N is a trap.** Kafka clients map key to partition by hash modulo the partition count. If a range split changes the partition count the gateway exposes, every key moves and per-key order breaks. Keep the Kafka-visible partition set stable (new topic epoch for splits), or never present a child range as a new partition index.
3. **Pin the routing hash.** Use a process- and version-stable hash (never a language builtin), with golden test vectors, and record its id on the topic. Changing it is a data migration. As of this writing no routing-hash function and no caller of `KeyRange::contains` exist — define them before the data plane honours a split.
4. **A split is only real when the data plane obeys the seal.** The barrier must equal the quorum-committed high-water mark *at the instant appends are fenced*, and the seal must stop produce atomically (epoch bump or an observed seal flag). Committing `SplitRange` in metadata while `check_range` keeps accepting produce on the parent is a metadata-only split.
5. **Secondary indexes.** Name-to-id indexes (`TopicByName`, `GroupByName`) are atomic and global only because one Raft apply writes both. Sharding the metadata plane turns them into term-partitioned indexes (async or 2PC). Decide that before sharding. A full scan inside `apply` (e.g. a cursor scan during split) signals a missing index keyed the other way.
6. **Rebalancing.** Many small units moved whole (segments) is the right model. Cap concurrent moves cluster-wide, not just per segment. Never couple automatic rebalance to automatic failure detection — an overloaded node judged dead triggers a cascade. Keep operator confirmation. Moves are add-before-retire with a failure-domain check.
7. **Replica placement is a safety input.** Quorum size must come from replicated state (see `ddia-replication-consensus`). Even replica counts add no fault tolerance; RF=2 tolerates zero faults. Helm validation should reject RF=2 / even data tiers the way it does for metadata voters, or require an explicit acknowledgement value.
8. **Request routing.** Every router must learn changes (readiness-gated Services + Kafka metadata). A "wrong node / range moved" answer must be retryable, distinct from "bad request", and carry a refresh hint — a non-retryable `Fenced` with no current epoch strands native clients after every failover.
9. **Scatter/gather.** Any cross-range read (topic enumeration, coordinator lookups) is tail-latency amplified. Bound the fan-out and the deadlines.
10. **Source-fleet rebalance.** For the Kafka *source* side, long uploads vs `max.poll.interval` cause spurious rebalances; revocation must abandon in-flight batches without committing.
11. **Cross-range order is not provided.** Per-range order only. Any feature relying on causal order across ranges (offsets vs data, group vs partition) must carry explicit references.

## Checklist

- [ ] Which key does this partition by, and what is the worst skew (one tenant, one source, one hot key)?
- [ ] Does the change alter any count or mapping a Kafka client can observe?
- [ ] Is the hash/ordering function versioned and golden-tested?
- [ ] What does the data plane do between "seal committed in metadata" and "children serve"?
- [ ] Who learns about the change and what does a stale router get back?
- [ ] Is there a cap on concurrent moves, and is automatic movement decoupled from failure detection?

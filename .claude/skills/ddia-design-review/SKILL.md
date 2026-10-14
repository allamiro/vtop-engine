---
name: ddia-design-review
description: Review a design, PR, or doc in vtop-engine through the reliability / scalability / maintainability framework from "Designing Data-Intensive Applications" (Kleppmann). Use when asked to evaluate an architecture change, a new component, a roadmap item, or "how can we improve this", before diving into the more specific ddia-* skills.
---

# DDIA design review for VTOP

Framework from chapter 1 of the book, mapped to this repo. Use it to structure a review; then drop into `ddia-replication-consensus`, `ddia-partitioning-placement`, `ddia-storage-durability`, `ddia-encoding-evolution`, or `ddia-fault-testing` for depth. Paraphrase — do not paste book text into repo docs.

First read: `docs/README.md` (two planes), `docs/ARCHITECTURE.md`, `docs/NATIVE_BROKER_ARCHITECTURE.md`, `docs/ROADMAP.md` (per-release known limitations). Review against what is actually shipped, not the aspiration.

## 1. Reliability — keep working correctly when things go wrong

Distinguish **fault** (a component deviates from spec) from **failure** (the service stops). The design goal is faults that do not become failures. Name the faults the change tolerates and the ones it explicitly does not.

| Fault class | Ask | VTOP anchor |
|---|---|---|
| Hardware (disk, node, power) | Does losing one node/disk lose acknowledged data? | `Durability::Fsync` vs `Buffered` in `vtop-log`; replica sets in `vtop-meta` placement |
| Software (correlated bugs, bad input, runaway resource, cascading) | Can one poison record/frame crash every node identically? | bounded codecs (`MAX_*` limits in `vtop-meta`), `memory_budget.rs`, group-commit `Overloaded` fail-closed |
| Human (config, operations) | Is the dangerous action hard, and the undo fast? | config key validation, `docs/PRODUCTION_HA.md`, rolling-upgrade + `scripts/upgrade-compat.sh` |

Also: is there monitoring that would reveal the violated assumption (`vtop-observe`, `docs/NODE_OBSERVABILITY.md`)? Can the system check its own invariants and alert (e.g. the `verify` tooling in `vtop-cli`)? Is the fault-tolerance path *exercised* (chaos scenarios, crash sweeps), or only believed in?

## 2. Scalability — a question, not a label

Never write "X is scalable". Write: *if load grows in dimension D, what breaks first and what are the options?*

1. Describe load with parameters (records/s, bytes/s, partitions/ranges, topics, consumer groups, replicas, metadata proposals/s).
2. Describe performance with **percentiles**, not averages (p50/p99/p999), and remember tail latency amplification when one request fans out to many backends. `benchmarks/` and `docs/THROUGHPUT_RESEARCH.md` are the evidence base; every number carries its conditions (see `vtop-contributing`).
3. Name the bottleneck resource: fsync rate, metadata Raft group throughput (`docs/METADATA_SATURATION_RESEARCH.md`), egress transport (`docs/EGRESS_TRANSPORT_RESEARCH.md`), object-store request rate.
4. Choose scale up vs scale out knowingly; stateful scale-out adds partitioning, rebalancing and failure-domain complexity — only pay it when measured need says so.

## 3. Maintainability

- **Operability:** can an on-call tell what is wrong from metrics and logs, and is there a safe default plus an override? Are admin surfaces authorized (`SECURITY_MODEL.md`)?
- **Simplicity:** remove accidental complexity with a good abstraction. Existing examples worth preserving: `StateStore` and `UploadBackend` traits, the `Env` seam in `vtop-log`, consensus-crate imports confined to `vtop-meta/src/raft`.
- **Evolvability:** can the next change land without a flag day? See `ddia-encoding-evolution`.

## 4. Derived data, time, and duties

- **Assumptions.** List the environmental assumptions each component makes (peer address resolved once, clock monotonic, disk never full, producer count small) and the signal that shows each still holds. Bugs sleep until an assumption stops being true.
- **Open-loop latency.** Measure latency at the client including queue time, with load at a fixed arrival rate. A harness that sends request N+1 after reply N is a service-time probe, not a latency measurement. Aggregate percentiles by summing histogram buckets; never average p95s. Name fan-out per operation *and its skew*, not the mean.
- **Integrity vs timeliness.** Timeliness violations (lag, stale reads) heal by waiting; integrity violations (loss, duplicates, wrong derived views) are permanent and need explicit checking. Spend rigor on integrity; the commit rule and manifests are integrity, visibility lag is timeliness.
- **System of record.** Name it. Everything else is derived through an ordered idempotent log, never written by a second writer. A dual-write migration is a temporary state and needs a reconciler for the one-sided outcome.
- **Hard vs loose constraints.** A hard invariant (uniqueness, fencing) needs a single decider; a loose one needs a recorded compensating action.
- **Backfill is a new output, not an edit.** Reprocess into a separate prefix/version, compare, then switch; keep source retention longer than the longest plausible reprocessing window.
- **Risky change checklist:** sandbox on real data, staged rollout, rollback, can derived data be recomputed?
- **Data duties.** Telemetry archives hold personal data. Per field: needed? retained how long? how is a subject's data erased given immutable segments, replicas, tier copies and WORM? Prefer cryptographic erasure / access control over policy alone; stored data is also a liability.

## Review procedure

1. State the claim of the change in one sentence and what it must never break.
2. Walk the three sections above; for each, write the concrete scenario, not the principle.
3. Prefer the smallest change that removes a failure mode. Reject premature scale work that locks in an inflexible design.
4. Output: **findings ranked by severity**, each with scenario, evidence (file:line or doc §), fix, and the test/measurement that would confirm it. End with "what I did not check".

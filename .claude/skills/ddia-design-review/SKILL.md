---
name: ddia-design-review
description: Review a design, PR, or doc in vtop-engine through the reliability / scalability / maintainability framework from "Designing Data-Intensive Applications" (Kleppmann). Use when asked to evaluate an architecture change, a new component, a roadmap item, or "how can we improve this", before diving into the more specific ddia-* skills.
---

# DDIA design review for VTOP

Framework from chapter 1 of the book, mapped to this repo. Use it to structure a review; then drop into `ddia-replication-consensus`, `ddia-storage-durability`, `ddia-encoding-evolution`, or `ddia-fault-testing` for depth. Paraphrase — do not paste book text into repo docs.

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

## Review procedure

1. State the claim of the change in one sentence and what it must never break.
2. Walk the three sections above; for each, write the concrete scenario, not the principle.
3. Prefer the smallest change that removes a failure mode. Reject premature scale work that locks in an inflexible design.
4. Output: **findings ranked by severity**, each with scenario, evidence (file:line or doc §), fix, and the test/measurement that would confirm it. End with "what I did not check".

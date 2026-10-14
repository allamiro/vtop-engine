---
name: ddia-fault-testing
description: Design tests that exercise distributed-systems and crash faults in VTOP — deterministic storage-simulator crash sweeps, startup crash matrices, live chaos scenarios (kill -9, partitions, clock skew, disk full), and benchmark fault scenarios. Use when adding or fixing anything about durability, failover, replay, fencing, or recovery, and when deciding what evidence a safety claim needs.
---

# Fault testing

Book lens: chapter 1 (deliberately inducing faults keeps the fault-tolerance path exercised), chapter 8 (partial failure, unreliable networks, clocks, pauses), chapter 9 (consistency models are only meaningful if you can check them). The repo already has three layers; pick the cheapest one that can fail for the right reason.

## The layers

| Layer | Use it for | Where |
|---|---|---|
| Deterministic simulation | Byte-level crash consistency: crash before op N, crash mid-write at byte K, op fails without crash | `vtop-log/src/sim.rs` (`FaultPlan`), `vtop-log/tests/sim_crash_sweep.rs`, `startup_crash_matrix.rs`; meta storage runs through the same `Env` seam |
| In-process integration | Cross-component logic, replica fencing, group commit, retention, replay | `crates/vtop-broker/tests/`, `tests/integration_*.rs`, `vtop-state` `test_battery.rs` + `pg_battery.rs` |
| Live chaos | Real processes, real sockets and signals: SIGKILL, partition, clock skew via LD_PRELOAD shim, disk full, fsync failure, membership change | `scripts/live-chaos/` (`lib.sh`, `run-all.sh`, `scenarios/NN-*.sh`), validated in `docs/LIVE_CHAOS_VALIDATION.md` |
| Benchmark faults | Behaviour and cost under network shape / replay | `benchmarks/scenarios/`, netem/toxiproxy |

Prefer the simulator for anything about *what is on disk*; prefer live chaos for anything about *who thinks they are leader*.

## Fault catalogue to draw from

Process: SIGKILL at each step, SIGTERM with in-flight work, long pause (SIGSTOP) longer than a lease. Storage: torn write, lost un-fsynced directory entry, fsync error, ENOSPC, silent corruption of a byte. Network: partition (symmetric and asymmetric), one-way loss, delay beyond timeout, duplicate and reordered delivery. Time: wall-clock skew and step, monotonic stall. Membership: grow, shrink, replace voter while leader is dead. Operator: wrong config, upgrade mid-flight, restart with the same dir.

## Writing the test

1. **State the invariant first**, in one line (e.g. "no acknowledged record is lost", "at most one broker accepts writes under epoch E", "source marker never ahead of VERIFIED").
2. **Make the fault land where the bug would be**: between two steps of the protocol, not around the whole operation. Sweep every operation index where cheap.
3. **Show it fails on the old code.** Revert the fix, run, record the failure output in the commit message. A test that cannot fail reports green and is worse than none.
4. **Check the invariant by reading back**, not by trusting the system's own success report (end-to-end check: re-read the records, re-verify the checksums, compare epochs).
5. **Failure messages name the cause:** what was observed, how many times, what was expected. Not "timed out".
6. **Never weaken the assertion to pass** — fix the assertion with a stated reason or fix the code.
7. **Control nondeterminism:** seed the RNG, use the injected `Clock`/`Env`, log the seed on failure. For live scenarios, poll for a named condition with a bounded timeout rather than `sleep`; report the last observed state when it expires. Flaky = unproven; investigate, don't retry-until-green.
8. **Sample, don't assume, for safety under concurrency**: simultaneous leaders is a violation; a legitimate win by a skewed node is not (scenario 07 draws this line).

## When adding a live scenario

Source `lib.sh`, validate env knobs with `require_integer_in_range`, keep shellcheck clean with the pinned CI invocation (see `vtop-contributing`), add it to `run-all.sh`, and document the invariant and result in `docs/LIVE_CHAOS_VALIDATION.md`.

## Claims

A statement like "survives leader loss" needs: the scenario that proves it, the conditions (topology, durability mode, timers), and the known gaps in `docs/ROADMAP.md`. Do not claim more than the sweep covers.

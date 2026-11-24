# AWS results, v0.11.1

The v0.11.1 release (`a7b4491e`) was tested on AWS from 2026-11-23 to 2026-11-24. The round includes:

- the failover, crash and cross-product benchmarks;
- the #596 throughput run;
- a stuck-leader check (#923);
- a Linux check of the #932 CPU fix.

The 72-hour soak (#726) is still running and is reported when it ends.

**No run in this round found VTOP losing, skipping or duplicating an acknowledged record** after a failover, a
whole-replica-set power loss, a `kill -9` of the archiver, or an upload over a policed link. The one duplication
VTOP showed is the at-least-once replay of a file that was rotated while VTOP was stopped (#748, below).

Every figure here is from one host per run. They are indicative, not a production benchmark and not a capacity
promise.

## Method

**Hosts.** All on-demand, region `us-east-1` (zone `us-east-1a`), Ubuntu 24.04 (AMI `ami-0a951420c495183dc`),
kernel `7.0.0-1018-aws`, Docker Engine 29.10.0. Each run's host file is in
[`benchmarks/measurements/aws-v0.11.1/hosts/`](https://github.com/allamiro/vtop-engine/tree/main/benchmarks/measurements/aws-v0.11.1/hosts).

| role | instance | disk | what ran on it |
|---|---|---|---|
| benchmarks | c6i.4xlarge (16 vCPU, 32 GiB) | 150 GiB gp3 data volume, 3,000 IOPS, 125 MiB/s, encrypted | #597, #599, #746, #747, #748, scenario 29, nocomp, one at a time |
| #596 | a second c6i.4xlarge, same disk | as above | #596 alone |
| soak | m6i.xlarge | 200 GiB gp3 data disk | #726 (running until about 2026-11-26) |

**Versions.**

| what | value |
|---|---|
| VTOP | `v0.11.1`, commit `a7b4491e5b605551554c13a02978fd6319612b25`, crates tree `14e8a13e`. #747, #748, scenario 29 and nocomp ran from a pristine checkout of the tag (scenario 29 and nocomp from the release binaries) |
| harness overlays | #597, #599 and #746 ran the tag's engine with the benchmark harness at `14edf8bc`; #596 with the harness at `8dbfd327`. Both overlays change only `benchmarks/`. The #596 and #746 rows record the overlay commit as `vtop_commit`, and #746's VTOP image is the same image id (`sha256:c68898cc…`) as the pristine #747 run |
| Apache Kafka | `apache/kafka:4.3.2@sha256:852a0d49…` |
| Vector, Kafka Connect S3 (Lenses), Fluent Bit | 0.61.0, 12.1.2, 5.2.0 (image digests in each run's CSV) |
| aws-cli, rclone, s5cmd | 2.40.3, 1.76.0, 2.3.0 |

**Rules the runs follow:**
- One benchmark runs at a time on a quiet host.
- Both systems get the same per-node limits: 2 CPUs and 2 GiB per node for the Kafka comparisons, and 2 CPUs and 2 GiB per tool for the cross-product runs.
- Every cell is reported as its median with the minimum and maximum.
- A cell that could not run is kept as a row saying so.
- A partial run is refused by the report generator.

## Results

### #597 failover under load

[Generated page](kafka-failover.md), run `failover-v0111-c6i4x`: 5 repetitions per fault, at 5,000 records/s offered
for 475,000 records. **All 30 failovers, 15 per system, read back every acknowledged record: 0 lost and 0 duplicated.**
VTOP's unavailability windows are longer than Kafka's:

| fault | Kafka | VTOP |
|---|---|---|
| `kill -9` of the leader | 10.2 s | 19.6 s |
| orderly stop of the leader | 1.03 s | 7.03 s |
| network partition of the leader | 30.0 s | 31.0 s |

All are medians; the page has the ranges. VTOP's time to a new leader, 13.2 s to 13.8 s after a kill or a
partition, sits close to its 15 s lease.

This run supersedes the laptop run `failover-20261111T032549Z`, which stays committed. The README's failover chart
now draws this run.

**Limits:**
- VTOP is driven through its native client and Kafka through its JVM client, so the two systems are measured through different clients.
- Only the F1, F3 and F4 faults were run, five times each.
- Profile `defaults` only.

### #599 whole-replica-set crash

[Generated page](kafka-crash.md), run `crash-v0111-c6i4x-30-r200`. Each configuration was crashed 30 times by a
simulated power loss of all three nodes (dm-flakey), at 200 records/s of 1 KiB. Every crash was ok in all four
configurations:

- **Kafka as shipped** lost acknowledged records in 30 of 30 crashes: 102,663 in all, and 6,895 in the worst crash.
- **Kafka with an fsync on every message**, **VTOP as shipped** and **VTOP with group commit on** each lost none in 30.
  Zero in 30 bounds the chance that a crash loses something to 9.5 % at 95 % confidence. That is evidence, not proof.
- VTOP needed no manual repair after any crash, and recovered in 9.6 s to 9.9 s.

Finding: after a whole-set crash, VTOP served none of the acknowledged records until the next write committed a new
quorum entry. In all 60 VTOP crashes, the harness's sentinel write then made every one of them readable. This is
when the answer arrives, not a loss.

A first run at 1,000 records/s is kept in the measurements directory, not drawn. Neither VTOP nor Kafka with fsync
could hold that rate with this client: 29 to 51 % of the records were acknowledged before the crash, and 90 of 120
rows were refused.

### #923 stuck leader (live-chaos scenario 29)

Scenario 29 ran three times on the v0.11.1 release binaries ([`s29/`](https://github.com/allamiro/vtop-engine/tree/main/benchmarks/measurements/aws-v0.11.1/s29)),
and all three passed:
- A leader left alone refused 100 records for want of a quorum.
- Quorum writes resumed 0 to 1 s after the full restart, inside the 60 s bound.
- 2,100 of 2,100 records were verified with no gap.
- Every sealed segment passes `vtopctl segment verify`.

The #599 crashes add the second half of #923's evidence: none of the 60 VTOP restarts there needed repair.

### #746 Kafka topic to object storage: VTOP, Vector and Kafka Connect S3

[Store kills](archive-comparison-store-kills.md) (run `archive-store-kills-v0111`) and
[time kills and verify fault](archive-comparison-time-kills-fault.md) (run `archive-time-kills-fault-v0111`): the
correctness cells only, against the lab MinIO.

**VTOP:** 0 gaps, 0 duplicates, 0 torn and 0 orphan objects after every `kill -9` (K1, K2 and K3, three runs each).

**The other tools:**
- Vector with a disk buffer lost 934 records in one K2 run.
- Vector duplicated records after kills.
- Connect left its 18 staging and index objects per cell. These are a floor of its design.

**The verify-fault cell does not test VTOP.** The page scores VTOP "behind", with 2 corrupt objects committed past,
but the fault corrupts only the body of a GET. VTOP's shipped profile verifies with the store's SHA-256 checksum
over HEAD and issues no GET: the tap saw 0 GETs from VTOP. So VTOP's verification was never exercised, and nothing
in the bucket was corrupt. The cell needs a fault that the checksum path sees.

### #747 upload over a policed link

[Generated page](link-comparison.md), run `link-hosted-v0111`.

| tool | objects committed of 48 | time |
|---|---|---|
| VTOP | 48 | 1,638.7 s |
| aws-cli | 48 | 2,618.7 s |
| s5cmd | 48 | 1,084.0 s |
| rclone | 34 (failed) | timed out at 3,600 s |

No tool had a mismatched object. This is the policed cell only, run once.

### #748 files and a syslog spool to object storage

[Crash](spool-comparison-crash.md) (run `spool-hosted-crash-v0111`) and
[rotation](spool-comparison-rotation.md) (run `spool-hosted-rotation-v0111`).

**VTOP:** 0 lines lost, 0 torn and 0 orphan objects in every counted row, and no duplicates under any kill.

**Finding (at-least-once, not a loss).** In the stopped-while-rotating cell, VTOP re-archived 2,031 already-committed
lines per run (6,093 over three runs), with both rename and copytruncate rotation. Those bytes had moved to a
rotated name while VTOP was down. Its object key includes the file path, so it archived them again rather than
recognising them. The fix would be file identity that follows a rotation.

**Harness gap.** VTOP's crash-K2 cell did not run: in two of three runs no K2 kill fired, and in the third only one
of three did. The S3 tap counts VTOP's bucket-create request, which gets a 409, as the first PUT, so a kill timed on
the first successful PUT never lands. The cell needs a harness fix and a re-run.

**The other tools:**
- Fluent Bit lost 44,825 lines across 9 crash runs.
- Vector lost lines under rotation kills: 10,000 per run with acks on copytruncate, and up to 14,645 per run with its disk buffer.
- No tool wrote a torn object.

**Limits.** The rows are labelled `dedicated`: 2 CPUs per tool, not the hosted class's 1 CPU.

### #932 uncompressed CPU fix, on Linux

[`nocomp/summary.txt`](https://github.com/allamiro/vtop-engine/tree/main/benchmarks/measurements/aws-v0.11.1/nocomp)
compares v0.11.0 with v0.11.1, uncompressed, 3 repetitions per cell:

- **NVMe data volume:** v0.11.1 spends 8 to 12 % less system CPU per GiB in all four uncompressed cells, for example 2.93 to 2.58 s/GiB with SHA-256 at the default width. Throughput is unchanged at the disk's limit, about 143 MiB/s.
- **tmpfs:** the two releases are within -6 to +4 %.

The 4 to 5x macOS cost #932 fixed never appeared on Linux, in either release. So the fix regresses nothing here.

### #596 throughput, latency and end to end: not drawn

Run `v0111-596-c6i4x` was stopped by SIGINT after the 100 B cells and the 1 KiB Kafka cells, before most of the 1 KiB
VTOP cells, by agreement, when its time ran over. The report generator refuses an interrupted run, so it has no
generated page.

Its raw data is in
[`benchmarks/measurements/aws-v0.11.1/v0111-596-c6i4x/`](https://github.com/allamiro/vtop-engine/tree/main/benchmarks/measurements/aws-v0.11.1/v0111-596-c6i4x).
The figures below are copied from its `results.csv`:

- **Saturation, 100 B, one producer on one partition:** Kafka 368,664 records/s; VTOP 6,235 (group commit off) and 6,247 (on). Kafka is 59.1x faster. At 1 KiB the ratio is 52.9x, from only two VTOP repetitions.
- **End to end at a common 3,117 records/s, 100 B, p50 and p99:**
  - Kafka as shipped: 5 ms and 13 to 28 ms;
  - Kafka with an fsync on every message: about 48 ms and 67 to 107 ms;
  - VTOP: 140 ms and 174 ms, with group commit off and on alike.

  All 12 rows sustained the rate.

**Why the gap is wider than on the laptop (12.0x to 13.9x):**
- One idempotent producer on one partition has one durable append in flight through VTOP's Kafka gateway.
- Each append waits for a quorum fsync, which took about 24 ms on this gp3 volume, against about 6 ms on the laptop.
- Group commit has nothing to merge with one producer, so on and off match.
- The #752 overlap needs several producers or partitions to show anything.

The saturation "latency" in the CSV is the client's buffer draining. Never read it as a server latency.

### #722 and #726

- **#722**, the live S3 suite against AWS S3, was closed by #934 before this round.
- **#726**, the 72-hour soak on v0.11.1, started 2026-11-23 06:57 UTC and ends about 2026-11-26 07:00 UTC. Its verdict and evidence are added when it finishes.

## Harness findings from this round

These do not change any result above, but each one needs fixing before the next round:

- The #746 verify-fault cell cannot exercise VTOP's checksum verification (above).
- In #597, the "p99 30 s before" column for VTOP tracks the fault's outage, about 12 s to 39 s, while its steady
  gap is about 30 ms. That column appears to include records acknowledged after the fault.
- In #748, crash-K2 never fires on VTOP: the bucket-create 409 is counted as a PUT.
- The #596 and #597 `environment.json` label says "Docker Desktop" on a native Linux Docker Engine.
- The overlay runs record the harness commit as `vtop_commit`.
- The #599 run is marked `worktree_dirty` by an untracked results directory.
- In #747, `host_disk` is not probed, and the smoke rows are labelled `laptop`.
- The tarballs carry no `SHA256SUMS`.

## What this closes

| issue | result |
|---|---|
| [#597](https://github.com/allamiro/vtop-engine/issues/597) failover | Open: zero loss in all 30 failovers, but only three of the issue's faults, five repetitions, `defaults` only |
| [#599](https://github.com/allamiro/vtop-engine/issues/599) whole-replica-set crash | Open: 30 ok crashes per configuration and the generated page and chart exist. Whether this meets the issue is the maintainer's call |
| [#596](https://github.com/allamiro/vtop-engine/issues/596) throughput and latency | Open: the run was interrupted, and the issue's full matrix was not run |
| [#746](https://github.com/allamiro/vtop-engine/issues/746), [#747](https://github.com/allamiro/vtop-engine/issues/747), [#748](https://github.com/allamiro/vtop-engine/issues/748), [#744](https://github.com/allamiro/vtop-engine/issues/744) | Open: correctness plans only; `drain`, `live` and the full plans were not run |
| [#600](https://github.com/allamiro/vtop-engine/issues/600) charts and report | Open: this round's crash, failover and cross-product pages are generated and drift-checked; #596's run is not drawn |
| [#726](https://github.com/allamiro/vtop-engine/issues/726) soak | Open until the soak ends |
| #923 stuck leader | Confirmed on v0.11.1 (already merged) |
| #932 CPU fix | Confirmed on Linux: no regression |

## Reproducing

The commands each run used are in its `environment.json` and in the measurements directory. The ingest steps
(copy a run under its kind's `results/`, add a `report.json`, commit, then run `regenerate.py`) are in the
[benchmarks README](https://github.com/allamiro/vtop-engine/blob/main/benchmarks/README.md#ingesting-a-results-directory-the-v0110-aws-round).

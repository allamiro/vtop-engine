# AWS results, v0.11.0

> [!IMPORTANT]
> **The round ran on v0.11.1, and its results are in [AWS results, v0.11.1](aws-v0.11.1.md).** This page is the
> plan as it was written before the round, kept for its method and its limits; its **FILL** markers are answered on
> the v0.11.1 page, not here.

The v0.11.0 release is tested on AWS in one round: the engine soak, the live S3 suite, and the benchmark harnesses
that need a dedicated Linux host. Each benchmark runs once on its host (twice where its section says so), its raw
results directory is committed under `benchmarks/`, and the report and charts are regenerated from it. Where a
benchmark runs more than once, every run is a page of its own.

## Method

**Hosts.** All on-demand, region `us-east-1`, Ubuntu 24.04, every resource tagged `project=vtop-bench`.

| role | instance | disk | what ran on it |
|---|---|---|---|
| benchmarks | c6i.4xlarge (16 vCPU, 32 GiB) | 100 GiB gp3, default IOPS and throughput | #596, #597, #599, #746, #747, #748, one at a time |
| soak | m6i.xlarge (4 vCPU, 16 GiB) | 50 GiB root and a 200 GiB gp3 data disk | #726 |
| live S3 suite | GitHub-hosted runner, AWS S3 in `us-east-1` | not applicable | #722 |

**FILL:** the instance ids, AMI ids and kernel of each host, copied from the notes file each host wrote
(`notes-<instance type>.txt`) and committed beside the results as `benchmarks/measurements/aws-v0.11.0/`. No harness
records the EC2 instance type except the crash harness (from the DMI), which is why the notes file exists.

**Versions.** Each run records what it ran in its `environment.json`; the generated pages print it in the footer of
every image and in an Environment block. This table is copied from those files and from nothing else.

| what | value |
|---|---|
| VTOP | **FILL:** the `v0.11.0` commit (`vtop_commit`), and its engine tree (`vtop_engine_tree`) |
| harness for #599 and #746 | **FILL:** the harness commit those two runs record as `vtop_commit`, and the check that its engine tree is the tag's |
| Apache Kafka (brokers and perf tools) | **FILL:** `kafka_image` with its digest |
| Vector, Kafka Connect S3 (Lenses), Fluent Bit | **FILL:** `system_version` and `image_digest` from the comparison runs' CSVs |
| Docker Engine, kernel | **FILL:** `docker_server_version` and `docker_vm_kernel` |

**Rules the runs follow.** One benchmark at a time on a quiet host; the same client and the same per-node limits
for both systems; every cell is repeated and reported as the median with the minimum and maximum, never the best
run; a cell that could not run is a row saying so (`failed` or `refused`), never a gap; a run that is partial or
mixes two environments is refused by the report and draws nothing. Each system keeps one colour in every chart, and
no chart tells two systems apart by red against green. The fairness rules are those of
[#595](https://github.com/allamiro/vtop-engine/issues/595) and, for the cross-product comparisons, of
[#744](https://github.com/allamiro/vtop-engine/issues/744).

**What the numbers are.** One host per run, so these are indicative, not a production benchmark and not a capacity
promise. Every image says so in its footer.

## What was run, and what it can and cannot show

Each section has the same shape: the command, the page the generator wrote, a headline to fill from that page, and
the limits stated before the result was known.

### #596 throughput and latency, VTOP gateway against Apache Kafka

- **Command:** `python3 benchmarks/kafka_compare/run.py --run-id v0110-596-c6i4x`, which records, besides the
  throughput and latency runs, the end-to-end latency distribution and the CPU and bytes written of every measured
  run (`--no-e2e` and `--no-resources` leave them out). **FILL:** the command as run, if it differs, and the run's
  duration.
- **Generated page:** **FILL:** link `kafka-comparison-aws-v0.11.0.md` once it exists. It holds produce throughput at
  saturation, produce latency at 25, 50 and 75 percent of Kafka's saturation, and consume throughput, each with the
  minimum and maximum across the repetitions; the end-to-end latency distribution (a CDF per record size, for VTOP,
  Kafka as shipped and Kafka with an fsync on every message, all offered one rate: half of the slower system's
  saturation); CPU seconds per GiB delivered and bytes written per byte delivered; and what each durability
  profile costs in latency, as bars.
- **Headline:** **FILL:** where Kafka is faster, where VTOP is, and the reason where the code supports one (the page
  prints its own list). Quote only what the generated page prints.
- **Limits.** One partition, one producer, no compression, 100 B and 1 KiB records: the issue's full matrix (10 KiB,
  six partitions, four producers, `zstd`) is not run. The throughput and fixed-rate cells run under profile
  `defaults` only; Kafka's `fsync-parity` is measured in the end-to-end stage alone, at one offered load, so the
  profile's latency cost is read at that load and its throughput cost is not measured. End-to-end latency has 1 ms
  resolution and is read at one load. The CPU and bytes-written figures are the three nodes' summed cgroup counters
  over each run, with no per-node breakdown and no memory figure; the windows add about 6 s of wall time per run,
  outside the timed part. Under `defaults` a Kafka acknowledgement does not wait for an fsync and a VTOP
  quorum acknowledgement does, so a speed difference is also a difference in the guarantee. VTOP runs as a static
  leader with two followers (no metadata plane). The perf tools report latency in whole milliseconds.

### #597 failover under load

- **Command:** `python3 benchmarks/kafka_compare/failover.py --reps 5 --run-id failover-v0110-c6i4x`. **FILL:** as
  run, and the duration.
- **Generated page:** **FILL:** link `kafka-failover-aws-v0.11.0.md`. A correctness table (acknowledged records lost
  and duplicated, per fault) comes first, then the unavailability window and the time to a new leader per fault, and
  the acknowledgement-rate timeline around the fault.
- **Headline:** **FILL:** the correctness table's result for both systems, then the windows.
- **Limits.** A Kafka client cannot follow a VTOP failover through the gateway (the gateway is refused on the
  candidate nodes that fail over in place), so VTOP's failover is measured through its native client and the two
  clients are different programs: the figures compare two client-and-cluster paths. Five repetitions per fault.

### #599 whole-replica-set crash

- **Command:** `python3 benchmarks/kafka_compare/crash.py --setup-flakey 10240 --teardown-flakey --crashes 12 --modes
  flakey-drop --max-hours 1.5 --run-id crash-v0110-c6i4x`. **FILL:** as run.
- **Generated page:** **FILL:** link `kafka-crash.md`. Per configuration (Kafka as shipped, Kafka with an fsync on
  every message, VTOP as shipped): acknowledged records, records lost, crashes that lost records, and, beside every
  cell that lost nothing, the exact binomial upper bound on the chance that a crash loses something. Charts: records
  lost per crash, the gap between the kill and the drop, and, beside the loss, each configuration's end-to-end
  latency at the common rate from the #596 run on the same host.
- **Headline:** **FILL:** from the loss table. A configuration with no loss is stated with its bound, never as proof.
- **Limits.** Twelve crashes per configuration, in power-loss mode only. With none lost in twelve, the 95 percent
  upper bound on the chance that a crash loses something is 22.1 percent; twenty crashes would give 13.9 percent and
  thirty 9.5 percent, so this run can show that a configuration loses records, and can only bound one that does not.
  The power loss is simulated with a dm-flakey device that drops the writes the page cache had not flushed, on one
  host, for all three replicas at once; it is not the `sysrq-b` of a virtual machine that the issue preferred, and it
  does not model a drive that lies about flushes, a torn write, or independent failures. The kernel may flush a little
  between the kill and the drop (the page prints the gap). The client is `kafka-verifiable-producer.sh`, so records
  are a few bytes, not 1 KiB. There is no process-kill control in this run.

### #746 Kafka topic to object storage, VTOP against Vector and Kafka Connect S3

- **Commands:** `python3 benchmarks/archive_compare/run.py --plan hosted-store-kills --host-class dedicated --max-hours
  5 --run-id archive-store-kills-v0110` and the same with `--plan hosted-time-kills-fault --run-id
  archive-time-kills-fault-v0110`. **FILL:** as run.
- **Generated pages:** **FILL:** link `archive-comparison-store-kills.md` and `archive-comparison-time-kills-fault.md`.
  Each has a scoreboard (VTOP's row present even where it is last), and tables of gaps, duplicates, torn and orphan
  objects after `kill -9`, and of corrupted objects committed past a bit-flipped read-back.
- **Headline:** **FILL:** from each page's "where VTOP is behind, level and ahead" list.
- **Limits.** The store is the lab MinIO, always: the harness has no option that points it at AWS S3, so this is not a
  measurement against S3. The two plans run the correctness cells only (kills and the verification fault): no
  throughput, cost or latency cell was run, so nothing here ranks the tools on speed. Each tool's "committed" means
  something different, so time to store-visible and time to source-committed are different clocks. One Kafka node, one
  host.

### #747 upload over a lossy and policed link

- **Command:** `python3 benchmarks/link_compare/run.py --plan hosted --run-id link-hosted-v0110`. **FILL:** as run.
- **Generated page:** **FILL:** link `link-comparison.md`.
- **Headline:** **FILL:** from the page's scoreboard.
- **Limits.** The `hosted` plan is the policed-link cell only; the link is shaped by the middlebox of the lab (netem
  and a policer), not a real WAN, and the store is the lab MinIO. The two calibrations either side of the run must
  agree within ten percent or the run is refused. One host.

### #748 files and syslog spool to object storage

- **Commands:** `python3 benchmarks/spool_compare/run.py --plan hosted-crash --run-id spool-hosted-crash-v0110` and
  `--plan hosted-rotation --run-id spool-hosted-rotation-v0110`. **FILL:** as run.
- **Generated pages:** **FILL:** link `spool-comparison-crash.md` and `spool-comparison-rotation.md`.
- **Headline:** **FILL:** from each page's scoreboard.
- **Limits.** The crash and rotation plans measure correctness (what a kill and a rotation cost in lost and duplicated
  records), not drain or live throughput. The store is the lab MinIO. One host.

### #722 live S3 suite, #726 72-hour soak

- **#722:** the `s3-aws` workflow runs the live S3 tests twice (both checksum settings) against AWS S3 from a
  GitHub-hosted runner, building the code at the dispatched ref. **FILL:** the workflow run, the two result files, and
  the cell of the S3 compatibility matrix they fill.
- **#726:** the 72-hour soak, with its fault schedule, judged by its twenty criteria. **FILL:** the run id, the
  verdict, and the line "passed" or the criteria that failed, as recorded in the evidence tarball attached to the
  release. How the soak is built and judged is in [Engine soak validation](../SOAK_VALIDATION.md).

## What this closes

A row closes its issue only when every acceptance item of the issue is met by what the round produced; the maintainer
decides, with the generated page open. Until a row's **Result** is filled, nothing here says the issue is closed.

| issue | what the round provides | closes when | Result |
|---|---|---|---|
| [#596](https://github.com/allamiro/vtop-engine/issues/596) throughput and latency | the run above, raw CSV, generated charts, an honest write-up | one command runs both clusters, a full run's CSVs are committed, the charts are generated from them, and the write-up includes the cells where Kafka is faster. The issue's full matrix is not covered (see the limits): the maintainer decides whether the run is "at least one full run" or what is carried over | **FILL** |
| [#597](https://github.com/allamiro/vtop-engine/issues/597) failover | the failover run | per-kill rows, the timeline and the unavailability box plot generated from them, and zero acknowledged-record loss asserted for both systems (the harness exits non-zero on a loss) | **FILL** |
| [#599](https://github.com/allamiro/vtop-engine/issues/599) whole-replica-set crash | the crash run | a repeatable script, documented hardware, per-crash rows, and a chart of records lost per profile generated from them. The issue asks for at least 20 crashes and this run has 12: the maintainer decides whether 12 and its stated bound are enough or whether to extend the run | **FILL** |
| [#600](https://github.com/allamiro/vtop-engine/issues/600) charts and report | every generated page above, produced by `report.py` including the new crash report | the report regenerates every image from the committed CSVs with no drift in CI, and the refusals are covered by tests. The issue's end-to-end latency CDF, CPU and bytes written, and the profile's latency cost beside the crash chart are drawn from the #596 and #599 runs, the last only when the crash run's `report.json` names the #596 run (see the ingest steps) | **FILL** |
| [#722](https://github.com/allamiro/vtop-engine/issues/722) S3 servers and AWS S3 | the `s3-aws` results | the AWS leg of the compatibility matrix is filled from the result files | **FILL** |
| [#726](https://github.com/allamiro/vtop-engine/issues/726) 72-hour soak | the soak evidence | the soak passed its criteria, with the evidence attached to the release | **FILL** |
| [#736](https://github.com/allamiro/vtop-engine/issues/736) sizing guidance | the section below, derived from these numbers | each figure in the guidance is a number printed on a generated page above, with the tested limit beside it | **FILL** |
| [#744](https://github.com/allamiro/vtop-engine/issues/744) cross-product comparisons | the archive, link and spool runs through the shared generator | the umbrella: its sub-issues are #746, #747 and #748, and the plans not run in this round (below) stay open | **FILL** |
| [#746](https://github.com/allamiro/vtop-engine/issues/746) archive comparison | two hosted plans, local MinIO | the issue's acceptance, item by item. The `drain` and `live` cells and the full plan are not in this round, and nothing was measured against AWS S3 | **FILL** |
| [#747](https://github.com/allamiro/vtop-engine/issues/747) lossy link | the `hosted` plan | the issue's acceptance, item by item. The `full` plan (defaults, equal connections, tuned, neighbour) is not in this round | **FILL** |
| [#748](https://github.com/allamiro/vtop-engine/issues/748) spool | `hosted-crash` and `hosted-rotation` | the issue's acceptance, item by item. The `full` plan (drain and live) is not in this round | **FILL** |

Anything a run shows that the acceptance list does not cover is a finding: it is stated on the generated page and
becomes its own issue with the measurement cited, not a footnote here.

## Sizing guidance from these numbers (#736)

**FILL:** written after the results exist, and only from them. The rule for this section: every figure is a number
that a generated page above prints, quoted with the page it comes from, the host class it was measured on, and the
limit it was measured within; no extrapolation beyond the tested range, and no figure from the laptop runs.

| question | measured here | from | tested on | the limit of the claim |
|---|---|---|---|---|
| **FILL** | **FILL** | **FILL** | **FILL** | **FILL** |

What this page deliberately does not do: size a cluster for a workload it did not run (more partitions, more
producers, compression, larger records, several hosts), or turn a single-host figure into a capacity promise.

## Reproducing a run

Each command above is run from a checkout at the `v0.11.0` tag (the harnesses build the engine image from the tree they
are run in, and record its commit and engine tree in `environment.json`). The host set-up the harnesses need (Docker,
the `dm-flakey` and `ifb`/`netem` kernel modules, passwordless `sudo` for the crash harness) is in each harness's
README under `benchmarks/`. How the results directories were placed and the pages regenerated is in the
[benchmarks README](https://github.com/allamiro/vtop-engine/blob/main/benchmarks/README.md#ingesting-a-results-directory-the-v0110-aws-round).

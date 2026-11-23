# VTOP vs Apache Kafka, head to head (phase 1)

A same-client comparison of VTOP, through its Kafka wire gateway, against Apache Kafka: produce throughput
at saturation, produce latency at fixed offered rates, and consume throughput. Tracking issue
[#595](https://github.com/allamiro/vtop-engine/issues/595) (its ten fairness rules are binding here),
measurements [#596](https://github.com/allamiro/vtop-engine/issues/596), charts and report
[#600](https://github.com/allamiro/vtop-engine/issues/600). The published result is
[`docs/benchmarks/kafka-comparison.md`](../../docs/benchmarks/kafka-comparison.md).

## Run it

Needs Docker (with Compose v2) and Python 3.10+; the charts need 3.11+ (the plotting stack pinned in
[`requirements-report.txt`](requirements-report.txt) requires it).
`run.py` refuses to start when `Cargo.toml`, `Cargo.lock`, `.cargo/`, `crates/` or `docker/` differ from
`HEAD`: the image it benchmarks is tagged and recorded as `HEAD`, so it must be built from `HEAD`.

```bash
# The whole matrix, both systems, one at a time (about 1.5 to 2 hours on a laptop; VTOP runs are the long ones).
# Builds vtop-bench:<HEAD sha> from docker/Dockerfile if it is not there yet.
python3 benchmarks/kafka_compare/run.py

# Charts (PNG + SVG in docs/benchmarks/img/) and docs/benchmarks/kafka-comparison.md, with the exact
# plotting stack the committed output was drawn with.
python3 -m pip install --require-hashes -r benchmarks/kafka_compare/requirements-report.txt
python3 benchmarks/kafka_compare/report.py benchmarks/kafka_compare/results/<run-id>

# Every committed run at once, as CI's drift check does (then `git diff -- docs/benchmarks/` must be empty).
python3 benchmarks/kafka_compare/regenerate.py
```

Useful knobs: `--sizes 100,1024`, `--reps 3`, `--sat-records 100:3000000,1024:1000000` (records per
saturation run AND per consume run, per record size), `--fixed-seconds 30` (a fixed-rate run sends rate x this many records),
`--smoke-records`, `--no-consume`, `--rebuild`, `--run-id` (one path component), `--max-hours 30`,
`--producers 1` (producer counts; see the ablation below).
Every one is checked before any Docker work, and so is the matrix's projected run time (`plan.py`):
a matrix projected past `--max-hours` is refused before the first container starts.

### Group commit on and off (#752)

`--group-commit off,on` runs VTOP once per setting of the node's `group_commit.enabled` (`off`, `on`, or
both), so the official run can report group commit measured instead of argued. It is VTOP's setting alone:
Kafka has no counterpart, runs once, and its rows leave the `group_commit` column blank.

```bash
python3 benchmarks/kafka_compare/run.py --group-commit off,on
```

- **Only what the node accepts.** `run.py` reads `crates/vtop-node/src/config.rs` at `HEAD` (the commit the
  image is built from) for the keys of the data node's `group_commit:` section, and writes only `enabled`. If
  the engine at `HEAD` has no such section (it lands with #832), the request is refused before any Docker work,
  saying so: the node would refuse the key at startup, and a run that left it out would label rows with a
  setting that was never applied.
- **Written, not defaulted.** Each setting is appended to the leader's `vtop/leader.yaml` as an explicit
  `group_commit: {enabled: false}` or `{enabled: true}` (to `results/<run-id>/configs/`, and verbatim into
  `environment.json` under `configs.vtop_leader_group_commit_<setting>`), so the day the node's own default
  changes, `off` still means off. `compose.vtop.yml` takes the file from `VTOP_LEADER_CONFIG`. Followers are
  not given the section: a follower with it enabled is refused by the node.
- **Confirmed, not assumed.** After each VTOP cluster comes up, the leader's own
  `vtop_broker_group_commit_enabled` gauge must read 1 for `on` and 0 for `off` (or the node does not export it)
  or the run stops. It is not written as a `refused` cell: the cause would repeat in every cell.
- **Owed, and checked.** `group_commits` is an axis of the matrix. A run that asked for both settings owes
  VTOP's rows twice, and `run.py --check` and the report's completeness check list the setting that is missing.
  A matrix without the axis leaves VTOP at the node's own default and writes a blank setting, as every earlier
  run did. `--max-hours` counts VTOP once per setting.
- **Shown as its own dimension.** The report draws VTOP once per setting, in VTOP's one colour (the palette
  has two validated slots): `off` hatched or dashed with diamond markers, `on` solid with square markers. It adds
  a *Group commit ablation* table (off, on, and the ratio of medians per record size), names the settings in
  the Method, the footer and the Environment table, and replaces the explanation of VTOP's per-request barrier
  with a pointer to the ablation.

#### Several producers on one partition, and the leader's counters

```bash
python3 benchmarks/kafka_compare/run.py --group-commit off,on --producers 1,2,4,16 --sizes 100,1024
```

The 1-producer cell cannot show group commit, so the ablation has a producer axis:

- **Why one producer shows nothing.** A commit group can only hold requests that are in the broker at the same
  time, and one producer never has two there. The gateway's session loop answers a Produce request before it
  reads the next one from that connection, and the gateway's bridge to the broker queues an idempotent
  producer's record sets one after another (the producer turnstile). So with one producer every group is one
  request, and on and off should measure the same, as they did at 1 producer and 1 partition in the first AWS
  round on v0.11.1. That is the expected result, not evidence that group commit does nothing.
- **What can show it.** Several producers on ONE partition: separate client containers, so separate
  connections and producer ids, all writing one range. `--producers 1,2,4,16` runs every cell once per count;
  each timed produce run starts that many `kafka-producer-perf-test` clients at once, sharing the run's records
  and offered rate evenly. A row's records/s and MiB/s are what all the producers delivered over one shared
  window: their steady-state records (and the MiB those carried) over the span from the earliest steady-state
  start to the latest end, each producer's window being its records over its own rate, placed where its run
  ended; the span is never shorter than the longest single window. Adding the producers' own rates would
  overstate a row whenever they finish at different times (100,000 records in 1 s and 100,000 in 2 s add up
  to 150,000 records/s, where 200,000 were delivered in 2 s). Its percentiles are the
  worst of the N producers' (`lat_basis` `worst-of-N`), since percentiles of different producers cannot be
  merged; its average is weighted by records. The smoke and the warm-up use one producer. Kafka runs the same
  producer counts, so every cell has its Kafka reference.
- **Partitions do not merge.** A group is formed per range: each partition is a range with its own leader log
  and its own coordinator, and fsyncs of different logs cannot be shared. More partitions are not evidence for
  or against group commit, and the ablation keeps one.
- **Pipelining one producer's requests is not built.** Letting one connection have several record sets in
  flight before the earlier is durable (answered in order) is the only change that could move the 1-producer
  cell. It is not group commit and does not exist yet.
- **The leader's counters.** In a run with `--group-commit`, `run.py` reads the leader's `/metrics` before and
  after every VTOP saturation repetition and records what moved, under the row's key, in `environment.json`
  under `observed.vtop_produce_counters` (not part of the fingerprint): `vtop_broker_group_commit_requests_total`,
  `vtop_broker_group_commits_total`, `vtop_broker_group_commit_overlapped_total` and
  `vtop_broker_produce_phase_seconds_total{phase}` for `local_fsync`, `replicate` and `commit`. The window is
  the client's whole run, warm-up records included. A scrape that fails is an event and leaves that
  repetition's counters out; it does not stop the run. The report's ablation table then gives, per record size
  and producer count, the mean group size (requests over commits), the share of groups whose local append ran
  while the previous group replicated, and the milliseconds per request spent in each phase, all for `on`: with
  group commit off the node exports no request count, so the off runs' phase totals stay in
  `environment.json`.
- **Drawn at one count, tabled at all.** The report's charts and main tables are drawn at the run's smallest
  producer count; the ablation table has a row per record size and producer count. A run that varies producers
  without both group commit settings is still refused, as before.

The only knob exposed for the ablation is `enabled`. `group_commit.max_delay_ms`, `max_records`, `max_bytes` and
`max_pending_requests` stay at the node's defaults (natural batching, the plan's decision D1), so the run
measures what ships; varying them is not part of this switch.

```bash
# What a run directory still lacks, whatever its state: exit 0 when every expected row is there,
# 2 with the missing row keys listed otherwise.
python3 benchmarks/kafka_compare/run.py --check benchmarks/kafka_compare/results/<run-id>
# The cells, row count and projected hours of a matrix (or an environment.json).
python3 benchmarks/kafka_compare/plan.py matrix.json --max-hours 30
```

A run writes `results/<run-id>/` and never overwrites another. Unlike
`benchmarks/results/` (git-ignored by `benchmarks/.gitignore`, whose `results/*` is anchored to that
directory), `kafka_compare/results/<run-id>/` is committed on purpose: the published reports are generated
from it, and a reader can check every number against it.

## What it does

For each record size, Kafka first and then VTOP:

1. **Fresh cluster.** `docker compose up` on empty named volumes: three Kafka brokers in KRaft mode
   ([`compose.kafka.yml`](compose.kafka.yml)), or three VTOP nodes ([`compose.vtop.yml`](compose.vtop.yml),
   configs in [`vtop/`](vtop/)): a static range leader serving the Kafka gateway plus two followers, the shape
   `scripts/live-chaos` scenario 17 drives with a stock client, in containers. Kafka's topic is created with
   RF=3 and `min.insync.replicas=2`; VTOP's is the range in the node config, acknowledged at a quorum of 2 of 3.
2. **Smoke.** `--smoke-records` records of the cell's size are produced with `acks=all` by
   `kafka-console-producer` and read back by `kafka-console-consumer` from offset 0; every record must come
   back byte-exact and in order, or the cluster is refused and its rows say so. No timing before this passes.
3. **Warm-up.** One unrecorded producer run. Its client logs at INFO, and the resolved `ProducerConfig` is
   saved in `environment.json` per system, so "same client settings" is the client's word.
4. **Saturation.** `kafka-producer-perf-test --throughput -1`, `--reps` times. The first 20% of each run's
   records are `--warmup-records` and excluded from its steady-state line, which is the one recorded.
5. **Fixed rate.** 25 / 50 / 75 % of **Kafka's** median saturation for the cell, offered to both systems with
   `--throughput` (open loop, rule 6), each sending about `rate x --fixed-seconds` records. That is a target
   length, not a time limit: a system that cannot keep up takes longer to send them, and the CSV's
   `wall_seconds` says how long each run took. The three rates run in turn, and that round `--reps` times.
   The systems are not interleaved: Kafka's whole cell runs before VTOP's. A run whose achieved rate is more
   than 5% under its offer is recorded with `sustained=false`.
6. **Consume.** `kafka-consumer-perf-test` reads `--sat-records` records from the start of the log with a
   fresh group, `--reps` times; the recorded rates are its `fetch.*` columns, which exclude the group join.
7. **Down.** The leader's log is saved under `logs/`, the cluster is removed with its volumes.

## Output

`results/<run-id>/results.csv`, schema 3 (#752, on schema 2 from #596): one row per key
(`system`, `mode`, `record_size`, `partitions`, `producers`, `compression`, `acks`, `profile`, `group_commit`,
`repetition`, `load_basis`, `load_pct`), with the run-constant columns (`run_id`, `env_fingerprint`, `vtop_commit`,
`kafka_image`, `client`). `mode` is `saturation`, `fixed_rate`, `consume` or `e2e` (end-to-end latency);
`compression` is the producer's codec; `profile` is `defaults` or `fsync-parity`; `group_commit` is VTOP's
`group_commit.enabled` for the row, `off` or `on`, blank on every Kafka row and on every row of a run that did
not vary it; `load_basis` says what a
rated row's percentage is of: `kafka` (Kafka's median saturation for the cell) or `common` (the slower
system's), blank on unrated rows. `lat_basis` is `single`, or `worst-of-N` when N producers ran and the row
carries the worst producer's percentiles. `records_produced` is set on `e2e` rows, whose `records` is the
count consumed; `window_start_ms` and `window_end_ms` bound the measurement window that the resource
counters cover. `same_config_as` names the profile a row was measured under when it repeats that
measurement: VTOP's node configuration is the same under both profiles, so it runs once per cell and its rows
are recorded under each. Which rows a matrix owes is defined once, in `plan.py`, for the runner, `--check`
and the report.

Schema 2 (no `group_commit` column) still loads, read with a blank setting. Schema 1, the layout of the
committed phase-1 run (`20261109T232328Z`), still loads. It had no
`compression`, `same_config_as`, `load_basis`, `records_produced`, `lat_basis` or window columns, and held
`profile` constant across the run. Its rows are read as schema 2 with compression `none`, load basis `kafka`
on fixed-rate rows and latency basis `single`, which is what that run did, and its report is unchanged
byte for byte.

`status` is `ok`, `failed` (the run ran and did not produce a usable result) or `refused` (it could not run,
e.g. the smoke failed); a row that is not `ok` says why in `note`. Latencies are the perf tool's own
percentiles in whole milliseconds; MiB/s is the tool's "MB/sec" (2^20 bytes).

`results/<run-id>/environment.json`: host CPU/model/RAM and OS, Docker Desktop version, the VM's CPUs,
memory, kernel and storage driver, the volume filesystem, per-node and client limits, both image references
with digests, the VTOP commit and engine tree hash, the client and its JVM, the producer and consumer
properties passed, the client's resolved `ProducerConfig` per system, selected settings from Kafka's broker
configuration as Kafka reports it (`kafka-configs --describe --all`, filtered to the flush, replication,
timeout, thread, socket, fetch, segment and group settings `run.py` names) and the topic layout, the derived
offered rates, the two Compose files and three VTOP node configs verbatim, and the COUNT of other containers
running on the Docker host at start (not their names). From `schema_version` 3 it also records
`comparable.vtop_group_commit.settings`, the group-commit settings the run measured and the `enabled` each was
written as (empty when the run did not vary it), which is part of what the fingerprint covers; the report
refuses a run whose recorded settings disagree with its matrix's `group_commits`. From `schema_version` 2 it also records
`comparable.host_disk`, the host disk behind Docker's data (model, SSD or HDD on macOS, the kernel's rotational
flag on Linux, bus, filesystem), which every image's footer names; `run.py` and `failover.py` refuse to start,
before pulling or building anything, when the disk cannot be named, and say what the probe saw. A file without
`schema_version` is schema 1 (the runs committed before #600). The `comparable` block is hashed into
`env_fingerprint`; `run_state` is `complete` only when the run finished.

`logs/`: every perf tool's output and each cluster's node-1 log.

## Report

`report.py` refuses (exit 2, the reason on stderr, nothing written) when the CSV or `environment.json` is
missing, `environment.json`'s `schema_version` is malformed or newer than the report reads, an
`environment.json` of schema 2 records no `host_disk`, the run did not finish, a row is malformed or a `failed`/`refused` row gives no reason, any expected
row is missing or duplicated, rows come from more than one environment, commit, image or client (or, in a run whose
CSV is schema 1, profile), the run varies along a dimension this report does not draw yet (partitions,
producers outside a group commit ablation at one partition, compression, acks, profile, or a load basis other
than `kafka`; the grouped charts are #600's), `observed.vtop_produce_counters` holds an entry `run.py` would not
write,
or holds a value its text does not say (a codec other than none, acks other than all, a profile other
than the environment's, end-to-end rows), a row that repeats another profile's measurement does not match
a measured row under that profile, the CSV's run id, commit, image, client or `acks` (and, in a run whose CSV is
schema 1, profile) differ from what `environment.json` records, or
the CSV's fingerprint does not match `environment.json` (rule 9). A cell that did not run is a row with
status `refused` (or `failed`, if it ran and gave no usable result); the report accepts such a row, does not
refuse the run over it, and the chart shows "no result" in its place.

Charts use one colour per system everywhere, from the registry in [`colours.py`](colours.py) (Kafka blue
`#2a78d6`, VTOP orange `#eb6834`, with a dark step of each hue for the README; validated for
colour-vision-deficiency separation and contrast, never red vs green), marker shape as a second channel, and a
footer from [`footer.py`](footer.py), also written into each image's metadata, with
the environment, profile, repetitions and run date. Measured numbers come from the CSV; the conditions,
run id, dates and matrix come from `environment.json`. Output is deterministic for a given CSV,
`environment.json` and matplotlib version (3.10.3, with the rest of the plotting stack pinned beside it in
[`requirements-report.txt`](requirements-report.txt); fixed rcParams, fixed SVG hash salt, no timestamps in
metadata, matplotlib's bundled DejaVu Sans). The report's Environment table names the matplotlib version it
was drawn with.

CI's `benchmark charts (regenerate + no drift)` job runs [`regenerate.py`](regenerate.py), which hands every
committed run under `results/` to `report.py` and fails on any difference in `docs/benchmarks/`. It refuses
another plotting stack, a font from outside matplotlib, no committed run, a committed run whose files on
disk differ from `HEAD` (it vouches for what `HEAD` says; the refusal names the run directory and the
`git restore --staged --worktree -- <run dir>` and `git clean -n`/`-f -- <run dir>` commands that put it
back), two runs writing one file, and committed output
that no run writes. SVG and Markdown must match byte for byte. PNGs must match pixel for
pixel, not byte for byte: Pillow's macOS and Linux wheels compress identical pixels to different bytes, so a
PNG whose decoded image is unchanged keeps its committed bytes.

The same job then runs [`../readme_charts.py`](../readme_charts.py), which draws the project README's
evidence charts (light and dark SVGs under `docs/assets/readme/`) from these runs and from
`benchmarks/measurements/`, with the same pinned stack, and fails on any difference there too.
`tests/test_readme_charts.py` fails when the README stops quoting a figure the data gives.

## Fairness rules applied (#595)

| rule | how |
|---|---|
| 1 same machine, same limits | both clusters in the same Docker VM, one at a time; every node `cpus: 2`, `mem_limit: 2g`, `memswap_limit: 2g`, named volumes; `run.py` refuses to start if the two compose files' limits differ |
| 2 same client | the JVM perf tools from the pinned `apache/kafka` image, in one container shape (4 CPUs, 4 GiB, 1 GiB heap) for both |
| 3 same replication contract | Kafka RF=3, `min.insync.replicas=2`; VTOP one range on three nodes, quorum 2 of 3; `acks=all` |
| 4 durability parity stated | profile `defaults` only, labelled on every chart; the report states that Kafka acknowledges without fsync and VTOP after a quorum fsync |
| 5 timeouts | Kafka's broker timeouts are recorded as Kafka reports them; failover is not measured in phase 1 (#597) |
| 6 open loop | latency only from `--throughput` runs at fixed fractions of Kafka's saturation; unsustained runs flagged |
| 7 repetition | 3 repetitions, warm-up run plus `--warmup-records` excluded, median with min-max |
| 8 pinned and recorded | image digests, commit, client, host and every config in `environment.json` |
| 9 no hand-entered numbers | `report.py` generates every chart and table from the CSV (conditions from `environment.json`) and refuses partial or mixed data |
| 10 gateway gaps listed | in the report and below |

## Gateway gaps met

| gap | exact error | what was done |
|---|---|---|
| CreateTopics | `UnsupportedVersionException: The node does not support CREATE_TOPICS` (`kafka-topics.sh --create`) | VTOP's topic is the range in the node config |
| DescribeConfigs | `UnsupportedVersionException: The node does not support DESCRIBE_CONFIGS` (`kafka-topics.sh --describe`) | not needed by the perf tools |
| OffsetCommit without a metadata plane | client: `Offset commit failed on partition bench-0 at offset <n>: The message format version on the broker does not support the request`; gateway: `kafka OffsetCommit refused: this gateway has no durable offset store` | `enable.auto.commit=false` for both systems |
| compressed batches | `UnsupportedCompressionTypeException: batch 0: record batch is compressed (codec 4), which this gateway does not decompress` (zstd) | `compression.type=none` for both systems |
| gateway on followers | the node refuses `kafka` on any role but leader or standalone | one gateway, on the range's leader |
| gateway on a candidate (failover, #597) | ``error: `kafka` is configured on a Candidate node: the gateway is served by a leader or standalone only — it holds one broker, and a candidate's changes with the lease (#225)`` | when #597 ran, no Kafka client could follow a VTOP failover (#751 closed that gap, and the run was not repeated); `failover.py` measures VTOP through its native client, labelled as such |

Everything the producer and consumer perf tools use was served: the 4.3 JVM client found a common version
for ApiVersions, Metadata, Produce (idempotent, the client's default, via InitProducerId), Fetch, ListOffsets
and the classic group protocol, all within the gateway's non-flexible ranges. Advertised listeners worked
with `kafka.advertised_host`.

## Failover under load (#597)

```bash
# 3 faults x 5 repetitions x 2 systems, about 75 minutes; one system at a time.
python3 benchmarks/kafka_compare/failover.py --reps 5
# Charts and docs/benchmarks/kafka-failover.md (report.py sees failover.csv and hands it on).
python3 benchmarks/kafka_compare/report.py benchmarks/kafka_compare/results/failover-<stamp>
```

**The client paths differ, and that is the first finding.** The only VTOP shape that fails over by itself
is every node a *candidate* (the Helm replicated topology), and in the build this run measured a candidate
refused the Kafka gateway (row above; #751 has since lifted that, and live-chaos scenarios 27 and 28 show a
Kafka client following a failover on one host, with debug binaries and a 6 s lease). A leased leader can carry the gateway, but its followers never take the range over themselves. So the
Kafka side is driven by the JVM client (`kafka-verifiable-producer.sh`, idempotent, retries at the client
default) and the VTOP side by its native client (`vtop-node produce` via
[`vtop/failover-driver.sh`](vtop/failover-driver.sh)); every chart and table says so.

- **Clusters.** Kafka as in phase 1 ([`compose.kafka.yml`](compose.kafka.yml)). VTOP:
  [`compose.vtop-ha.yml`](compose.vtop-ha.yml), three co-located nodes ([`vtop/ha-node-*.yaml`](vtop/)),
  each a metadata Raft voter and a candidate replica, TLS on every plane (promotion is refused on a plaintext
  replica plane; certificates are minted per run with `scripts/live-chaos/gen-certs.sh` and never committed).
  The CA key is deleted once the leaves are minted, private keys are owner-only (on Docker Desktop; a native
  Linux daemon needs them readable by the container's uid 10001), and each node mounts only `ca.pem` and its
  own two key pairs. Kafka's listeners are PLAINTEXT. Both stacks drop every capability and set
  `no-new-privileges`.
  Same per-node limits, enforced as in phase 1. Shipped timeouts on both (profile `defaults`).
- **Per repetition.** Fresh cluster; producer at `--rate` (default 5,000 records/s), `acks=all`; at 35 s the
  partition leader gets `kill9_leader`, `stop_leader` (`docker stop -t 30`) or `partition_leader`
  (`docker network disconnect`, reconnected after 30 s); 60 s more load; then the whole partition is read
  back (Kafka: `kafka-console-consumer.sh`, compared as text; VTOP: `vtop-node verify`, byte-exact, in
  order). Leaders are observed every 100 ms ([`metadata_probe.py`](metadata_probe.py) for Kafka, the range
  lease for VTOP).
- **Output.** `results/failover-<stamp>/failover.csv` (one row per system, fault and repetition: unavailability
  window, time to new leader, p99 latency before and after measured from the fixed-rate schedule, client
  errors and retries, acknowledged records lost, duplicates, the read-back verdict), `timeline.csv` (records
  acknowledged per 100 ms around the fault), `environment.json`, and `logs/` per repetition. A read-back that
  finds a lost or duplicated acknowledged record keeps the run's raw data under `logs/<rep>/evidence/` and
  makes `failover.py` exit 1. A repetition whose producer did not acknowledge every record (stopped at the
  drain deadline, or exited non-zero) is a `failed` row: its gaps and latencies would leave out the unresolved
  end of the outage. Its read-back verdict is still recorded.
- **Expected noise in `logs/`.** Every Kafka `consumer.err` holds `ERROR Error processing message, terminating
  consumer process: ... TimeoutException`: that is how `kafka-console-consumer.sh` ends a read with
  `--timeout-ms` once no record arrives for 20 s, not a failed read-back. The next line,
  `Processed a total of N messages`, is the read; the verdict is the `verify` column.
- **Host contention.** The host is shared and not quieted. `host_noise.csv` samples it every 30 s (load
  average, CPU idle, Docker containers including `rust:` builds, Lima VMs running), and `environment.json`
  `host_noise` summarises it for the run and per repetition. The report names the repetitions it affected.
- **Committed run, and the one it superseded.** `results/failover-20261111T032549Z` is the committed run.
  It replaced `results/failover-20261110T185855Z`, whose VTOP producer sent a batch when its first record
  was due and stamped acknowledgements after fsyncing its floor file, so its VTOP 30 s-before p99 was not
  comparable with Kafka's (#613 review). That run was added in e0c3d35 and is last present, unchanged, in
  190037b on the #613 branch (`git show 190037b:benchmarks/kafka_compare/results/failover-20261110T185855Z/failover.csv`).

## Known gaps

- Matrix: 100 B and 1 KiB, 1 partition, 1 producer, no compression, `defaults` only (no `fsync-parity`,
  `matched-timeouts`, 10 KiB, 6 partitions, 4 producers).
- VTOP runs without its metadata plane (static leadership): no lease, no failover, no durable offsets.
- No per-node CPU/RSS or bytes-written sampling, no end-to-end latency.
- Laptop numbers: Docker Desktop on one host, sharing the VM with whatever else runs there.

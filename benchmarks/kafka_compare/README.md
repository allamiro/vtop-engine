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
saturation run AND per consume run, per record size), `--fixed-seconds 30` (the target length of a fixed-rate leg; see step 5),
`--smoke-records`, `--no-consume`, `--rebuild`, `--run-id` (one path component), `--max-hours 30`, and the
schema-4 switches `--no-e2e` (no end-to-end stage), `--no-fsync-parity` (the end-to-end stage for both systems as
shipped only) and `--no-resources` (no cgroup windows).
Every one is checked before any Docker work, and so is the matrix's projected run time (`plan.py`):
a matrix projected past `--max-hours` is refused before the first container starts. The projection is a bound:
each fixed-rate leg is charged its wall limit (`3 x --fixed-seconds + 60 s`), and each saturation or consume run
the longer of its bytes at 2 MiB/s and its records at 6,000 records/s (VTOP's rate with 100 B records in the
v0.11.1 AWS round, rounded down). `plan.py` and the runner's first line also print the figure if every leg is on time.

### More than one producer or partition (#596, #752)

`--producers N` and `--partitions N` set the run's shape, one value of each per run (a sweep is several runs, as
for `--group-commit`); `environment.json` records them in `matrix.producers` and `matrix.partitions`, the keys
every run already had, so a run of either shape is still schema 4.

- **Producers.** Every measured run is N `kafka-producer-perf-test` containers at once against the same topic,
  each its own idempotent producer, offered `rate / N` and sent `records / N` (unthrottled for each at
  saturation). A row sums their records and carries the worst of their percentiles, with `lat_basis`
  `worst-of-N`; a percentile of their latencies merged cannot exceed the worst producer's, so that is an upper
  bound. Its records/s and MiB/s are what all of them delivered over one shared window: their steady-state
  records (and the MiB those carried) over the window from their common start to the last one's end, the
  longest producer's own steady-state window (its records over its rate). The containers start a second or so
  apart at most, against minutes of run, and that skew is not corrected for. Adding the producers' own rates would overstate a row whose
  producers finish at different times (100,000 records in 1 s and 100,000 in 2 s add up to 150,000 records/s,
  where 200,000 were delivered in 2 s). A producer that fails fails the row, naming it. One producer's appends are serialised in VTOP's
  gateway, so group commit (#752) has something to merge only at more than one.
- **Partitions.** Kafka's topic is created with N partitions; the smoke reads every partition from its start and
  requires every record back once and each partition's records in the order produced. VTOP is not measured at
  N > 1: its Kafka gateway serves one partition per node process, one range each, and refuses the others by name
  (`kafka.partitions` in `crates/vtop-node/src/config.rs`). N partitions replicated three ways are N ranges with
  a leader and two followers each, 3N node processes (live-chaos scenario 24 runs that shape with 2), and this
  harness runs VTOP as three single-process containers at the per-node limits Kafka's brokers get (fairness
  rule 1). Building a cluster that packs 3N processes into those three limits is not done here, so VTOP's rows
  at N > 1 are `refused` rows saying why, and the report says so.
- **The end-to-end stage** is measured at 1 partition and 1 producer only (`plan.axes` gives no other cell the
  mode); a run of another shape has none, and its end-to-end figures are a 1-partition, 1-producer run's.

### An interrupted run, and continuing it

A run writes each row as it is measured and each event (a group-commit setting confirmed, a smoke passed, a cell
refused) to `environment.json` when it happens. Ctrl-C and a `SIGTERM` (a stopped instance, a CI timeout) are
handled alike: `run_state` becomes `interrupted`, the signal is recorded in `events`, and both clusters are taken
down. `--check <run-dir>` lists the rows it still owes.

```bash
python3 benchmarks/kafka_compare/run.py --resume <run-id> <the arguments the run was started with>
```

`--resume` reopens `results/<run-id>/` and refuses, before any cluster starts, unless the matrix the arguments give,
the environment fingerprint, the VTOP commit and the images (`vtop_image_id`, `kafka_image_id`, the end-to-end
client's) and the cluster configuration files equal what `environment.json` recorded. It then skips every unit of
work whose rows are all present: a unit is one system's cell of one record size at one group-commit setting, or the
end-to-end stage of one size. A unit with only some of its rows is run again whole: those rows are moved to
`logs/partial-<unit>.csv` (an end-to-end stage's histogram rows to `logs/partial-<unit>-e2e_hist.csv`) and an event
says how many, so no row key is ever written twice and nothing measured is deleted. A skipped Kafka cell's offered
rates come from `observed.offered_rates`, and a skipped cell's saturation rows still set the end-to-end stage's
common rate.

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
- **The leader's counters.** Around every VTOP saturation repetition of a `--group-commit` run, `run.py` reads
  the leader's `/metrics` and records what moved, under the row's key, in `environment.json` under
  `observed.vtop_produce_counters` (not part of the fingerprint): `vtop_broker_group_commit_requests_total`,
  `vtop_broker_group_commits_total`, `vtop_broker_group_commit_overlapped_total` and
  `vtop_broker_produce_phase_seconds_total{phase}` for `local_fsync`, `replicate` and `commit`. The window is the
  client's whole run, warm-up records included. A scrape that fails is an event and leaves that repetition's
  counters out; it does not stop the run. The ablation table then gives, per record size, the mean group size
  (requests over commits), the share of groups whose local append ran while the previous group replicated,
  and the milliseconds per request in each phase, all for `on`: with group commit off the node exports no
  request count, so the off runs' phase totals stay in `environment.json`. The report refuses an entry `run.py`
  would not write.
- **Where it can show.** One producer never has two requests in the broker (the gateway answers a Produce
  request before reading the next from that connection, and its bridge queues an idempotent producer's record
  sets one after another), so at `--producers 1` every group is one request and on and off should measure the
  same, as they did at 1 producer and 1 partition in the first AWS round on v0.11.1. Run the ablation at
  several producers on one partition (`--producers 2`, `4`, `16`, one run each). Partitions do not merge: each
  is its own range with its own coordinator. Pipelining one producer's requests, the only change that could
  move the 1-producer cell, is not built.

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
   `--throughput` (open loop, rule 6). A leg sends `min(rate, the system's own saturation median) x
   --fixed-seconds` records, still throttled at the offered rate, so it lasts about `--fixed-seconds` whether
   or not the system can hold the rate. (Until the v0.11.1 AWS round a leg sent `rate x --fixed-seconds`
   records, and a system below the rate ran for as long as its shortfall: VTOP's "30 s" legs at 100 B took 7
   to 20 minutes.) A leg sized from the system's saturation is `sustained=false` with a note naming the
   offered rate, the achieved rate and the sizing. Each leg's wall limit is `3 x --fixed-seconds + 60 s`; a
   leg that reaches it is a `failed` row with `sustained=false`, its client container removed, and its note
   gives the achieved rate of the last progress line the tool printed, when it printed one. The CSV's
   `wall_seconds` says how long each run took. The three rates run in turn, and that round `--reps` times.
   The systems are not interleaved: Kafka's whole cell runs before VTOP's. A run whose achieved rate is more
   than 5% under its offer is recorded with `sustained=false`.
6. **Consume.** `kafka-consumer-perf-test` reads `--sat-records` records from the start of the log with a
   fresh group, `--reps` times; the recorded rates are its `fetch.*` columns, which exclude the group join.
7. **Down.** The leader's log is saved under `logs/`, the cluster is removed with its volumes.

Then, once per record size and after both systems' cells (schema 4, #600), **the end-to-end stage**: the common rate
is `E2E_LOAD_PCT` (50 %) of the SLOWER system's median saturation for the cell, and both systems, and Kafka under
`fsync-parity` as well (`compose.kafka.fsync-parity.yml` layered on the base file, plus `flush.messages=1` on the
topic: the setting `crash.py`'s fsync-parity configuration uses), are brought up fresh, smoke-checked, warmed up for
10 s at that rate and measured `--reps` times. In each repetition [`client/E2eLatency.java`](client/E2eLatency.java),
compiled once by [`client/Dockerfile`](client/Dockerfile) (the pinned `eclipse-temurin` JDK, against the pinned Kafka
image's own kafka-clients jars, onto that image: the image's JRE has no compiler, so it cannot launch the source
file) and run by the image's own JVM in a second client container (2 CPUs, 2 GiB), assigns the partition and seeks to the end,
says `READY`, and counts each record's latency (its `poll()` return time minus the producer's `CreateTime`, both
the client host's clock, 1 ms resolution) into a histogram while the perf producer offers `rate x --fixed-seconds`
records at that rate. VTOP has one configuration, so it owes no `fsync-parity` row. With `--group-commit off,on`, VTOP is
measured end-to-end once per setting, and the common rate's slower system is the slower of Kafka and VTOP's
slowest setting, so every end-to-end series is offered the same rate. A run that consumed a
different number of records than it produced is a `failed` row saying how many are missing; a negative latency
fails the row (a clock or timestamp fault); a rate not held is flagged `sustained=false`.

**Resources (metric 4).** Around every measured run (saturation, each fixed rate, consume, end-to-end), the
runner `sync`s each of the system's three nodes, reads `cpu.stat` and `io.stat` from inside each container
(`docker exec`, [`resources.py`](resources.py)), runs the producer or consumer, `sync`s again, waits 2 s for
write-back, and reads again. The row records the three nodes' summed deltas (`cpu_seconds`, `disk_write_bytes`,
`disk_read_bytes`), the bytes the run delivered (`delivered_bytes`: records sent, or read, times the record
size) and the window's start and end. The report divides: CPU seconds per GiB delivered, and bytes written per
byte delivered (write amplification: replication, logs, indexes and metadata included). cgroup v2 with the `cpu`
and `io` controllers is required and checked before the run starts (cgroup v1 is refused, not approximated).

**Overhead, stated.** The counters are read outside the producer's run, never during it, so they add no load to
what is timed; each window adds about 6 s of wall time (the syncs, the settle and the reads), about 6 minutes
over a default run's 36 measured runs per system. The end-to-end stage adds two fresh clusters per record size
(one for Kafka under `fsync-parity`) with 40 s of runs per repetition, about 15 minutes in all. The consumer
runs beside the producer, which is the point of an end-to-end measurement, but means the client host carries two
JVMs at once (6 CPUs of client limits beside the nodes' 6): on a host with fewer than 12 CPUs, say so in the
result. `plan.py` charges both to the projected hours.

**Unverified until a run on the target host:** before the VTOP image is built or any cluster starts, `run.py` builds
the consumer's image (`kc-e2e-client:<hash>`, which pulls the pinned JDK image: outbound internet) and runs the class
once with `--check`, refusing and naming `--no-e2e` when either fails. Whether VTOP's gateway returns a record's `CreateTime` unchanged (the
latency would otherwise be nonsense, and the run refuses a negative one) and whether the node containers'
`io.stat` accounts the data volume's writes are the two things the first run shows.

## Output

`results/<run-id>/results.csv`, schema 4 (#600, on schema 3 from #752 and schema 2 from #596): one row per key
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

**Schema 4** (#600) adds four columns after the window, `delivered_bytes`, `cpu_seconds`, `disk_write_bytes` and
`disk_read_bytes` (the three nodes summed; all four blank on a row without a resource window and on every failed or
refused row), a sidecar `e2e_hist.csv` (one line per ok `e2e` row and 1 ms bucket: the row's key columns, including
`group_commit`, then `bucket_ms` with 60000 meaning 60 s or more, and `count`; the counts must sum to the row's
`records`, and the row's percentiles must be the histogram's), and two matrix keys, `resources` (every ok measured
row carries its resources) and `e2e.extra_profiles` (profiles Kafka alone is also measured under, end-to-end only,
at the common rate). It is written with `environment.json` `schema_version` 4, and the two are refused when
one is schema 4 and the other is not. Schemas 1 to 3 still load, and their reports are unchanged byte for byte: the columns
a row predates read as blank.

Schema 3 (#752, the `group_commit` column) and schema 2 (no `group_commit` column, read with a blank setting)
still load. Schema 1, the layout of the
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
before pulling or building anything, when the disk cannot be named, and say what the probe saw. On a host whose
probe cannot name it (an EBS NVMe volume that reports no model), `--host-disk "EBS gp3 100 GiB, SSD, NVMe, ext4"`
names it instead; it is recorded as given and must be one line without `|` (the footer's separator). A file without
`schema_version` is schema 1 (the runs committed before #600). The `comparable` block is hashed into
`env_fingerprint`; `run_state` is `complete` only when the run finished.

Beside `comparable`, and outside the fingerprint, `host_metadata` records `instance_type` (the EC2 instance type from
IMDSv2, `unknown` anywhere else or when the metadata service does not answer in a second), `cpu_model` and `kernel`
(`compare_common/hostmeta.py`; the archive, link and spool comparisons record the same block). A run committed
without it loads and regenerates as before. `VTOP_BENCH_NO_IMDS=1` skips the instance-type lookup.

`logs/`: every perf tool's output and each cluster's node-1 log.

## Report

`report.py` refuses (exit 2, the reason on stderr, nothing written) when the CSV or `environment.json` is
missing, `environment.json`'s `schema_version` is malformed or newer than the report reads, an
`environment.json` of schema 2 records no `host_disk`, the run did not finish, a row is malformed or a `failed`/`refused` row gives no reason, any expected
row is missing or duplicated, rows come from more than one environment, commit, image or client (or, in a run whose
CSV is schema 1, profile), the run varies along a dimension this report does not draw yet (partitions,
producers, compression, acks, profile other than Kafka's `fsync-parity` end-to-end rows, or a load basis other
than `kafka` on fixed-rate rows and `common` on end-to-end rows; the grouped charts are #600's)
or holds a value its text does not say (a codec other than none, acks other than all, a profile other
than the environment's), a run whose matrix has the end-to-end mode but whose `e2e_hist.csv` is missing, short,
or disagrees with its rows, a run whose matrix asks for resources but has an ok row without them, a row that repeats another profile's measurement does not match
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

**A second run of a kind, and the crash report.** A run directory may hold a [`report.json`](run_outputs.py),
`{"doc": "docs/benchmarks/<page>.md", "img_dir": "docs/benchmarks/img/<dir>"}`, which sends that run's page and
images beside the first run's instead of onto the same files (without it two committed runs of one kind are
refused, as above). The file is part of the run, committed with it. A newer run that REPLACES an earlier one of its
kind (throughput, failover or crash, by the file the directory holds) says `{"supersedes": "<run-id>"}` instead,
with `doc` and `img_dir` both or neither (neither: the kind's default paths). The superseded run stays committed
and checked, but `regenerate.py` does not draw it, so the newer run takes over its page and images; the target
must be a committed run in the same results directory, of the same kind, that started earlier (`started_at`),
with no cycle. `readme_charts.py` draws the one current throughput run and the one current failover run, refusing
when there is not exactly one of each (`regenerate.current_run`). `report.py` hands a crash run (its directory
holds `crash.csv`, #599) to [`crash_report.py`](crash_report.py), which reads it through the harness's own
`crash_schema.load_crash_run` with a floor of ten ok crashes per configuration and mode, writes
`docs/benchmarks/kafka-crash.md` and `kafka-crash-*.{png,svg}`, and prints the exact binomial upper bound beside every
cell that lost nothing. The steps for placing a run, in order, are in
[`../README.md`](../README.md#ingesting-a-results-directory-the-v0110-aws-round).

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
refused the Kafka gateway (row above; #751 has since lifted that, and live-chaos scenarios 27, 28 and 30 to 33
show a Kafka client following a failover on one host, with the chart's lease and release binaries). A leased leader can carry the gateway, but its followers never take the range over themselves. So the
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

## Whole-replica-set crash (#599)

`acks=all` means *replicated*, not *on disk*: Kafka's default (`flush.messages` unset) acknowledges a write once
the in-sync replicas have it in their page cache. `crash.py` kills the whole replica set at once and counts the
acknowledged records that are missing afterwards, for Kafka as shipped, Kafka made to fsync, and VTOP.

```bash
# The short profile: 2 crashes per cell, both modes, SIGKILL and power-loss, about 20 minutes on a 4-vCPU VM.
python3 benchmarks/kafka_compare/crash.py --smoke --setup-flakey 8192

# The full run on a dedicated Linux host (see below): 4 configurations x 12 power-loss crashes, projected at 1.3 h.
python3 benchmarks/kafka_compare/crash.py --setup-flakey 10240 --teardown-flakey \
    --crashes 12 --modes flakey-drop --max-hours 2

# What a run directory still owes, whatever its state (exit 0 when complete, 2 listing the missing crashes), and
# the way to continue an interrupted one.
python3 benchmarks/kafka_compare/crash.py --check benchmarks/kafka_compare/results/<run-id>
python3 benchmarks/kafka_compare/crash.py --resume <run-id> --crashes 12 --modes flakey-drop
```

The projection (`crash_schema.projected_seconds`, checked against `--max-hours` before any container starts, and again
before every crash) uses per-crash timings measured on the smoke run: a Kafka crash takes about 46 s and a VTOP crash
about 21 s beyond its trigger (mean 40 s in the default window), so a crash index of the four default configurations takes
about 4 minutes and 12 of them about an hour; the full length was not run. The issue's 30 crashes per configuration
are `--crashes 30` (3.2 h with the four default configurations), and a `sigkill` control for them doubles that.

**The host.** One Linux machine with Docker Engine (Compose v2), a user in the `docker` group with passwordless
`sudo` (the harness runs only `crash/powercut.sh` through `sudo -n`; run it as root and git will want
`safe.directory` for the checkout), and a kernel that has `loop` and `dm-flakey`
(`sudo modprobe dm-flakey` must succeed; on Ubuntu the `linux-modules-extra-$(uname -r)` package carries it).
Nothing in the harness is macOS-specific, and it does not use Docker Desktop: Docker Desktop's VM is not one this
script can crash. The reference shape for the full run is one EC2 instance of 16 vCPUs and 32 GiB with Ubuntu
24.04 and a gp3 or instance-store volume (a `c7i.4xlarge` or `m7i.4xlarge`): three 2-CPU nodes, the 4-CPU client and
headroom, with nothing else running. `environment.json` records the instance type (`host_metadata`, from IMDSv2, and the DMI's word in `comparable.host_instance`), the CPU model, kernel,
filesystem and mount options, the `vm.dirty_*` sysctls, and the disk behind Docker's data. The first run builds
`vtop-bench:<commit>` from `docker/Dockerfile` if it is not there (the release build took 7 minutes with 2 CPUs). Docker installs with
`curl -fsSL https://get.docker.com | sudo sh`; the harness needs only the Python standard library.

`--setup-flakey MIB` makes `--data-root` (default `/var/lib/kc599/data`) a sparse image file, a loop device, a
dm-flakey device and an ext4 filesystem (`crash/powercut.sh setup`); `--teardown-flakey` removes it at the end. A host
whose disk the probe cannot name (a virtio VM) is told with `--host-disk TEXT`. The CPUs per node default to 2, as the
Compose files say; the smoke run on a small VM lowers them (`--node-cpus 1 --client-cpus 1 --client-memory 2g`) for
Kafka and VTOP alike, and `environment.json` records what ran.

**Per crash** (one row of `crash.csv`): wipe the three data directories, bring a fresh three-node cluster up, create
the topic, `sync` the data filesystem, write the fsynced canary, start the producer, and at `t_i` seconds after it
started (`t_i` is uniform in the trigger window, drawn from the seed and `i` only, so every
configuration and mode sees the same schedule) write the unsynced canary and:

1. `docker kill --signal KILL` on all three nodes, one command;
2. in `flakey-drop` mode, immediately `dmsetup` reload of the data device with `drop_writes`, with the filesystem
   neither frozen nor synced; no sync runs between the kill and the drop;
3. stop the client (its last acknowledgements are already on disk), remove the dead containers, and in `flakey-drop`
   unmount, write again, and mount (journal recovery);
4. read the canaries: the fsynced one must be whole; the unsynced one must be absent or short in `flakey-drop` and whole
   in `sigkill`, or the crash is a `refused` row (kept, with the reason) and not an `ok` one;
5. `docker compose up`; wait until ListOffsets is served (the first served read); read the whole partition
   (`read_missing_before_sentinel`); produce a **sentinel** record with `acks=all` and wait until the partition's end
   offset has moved past what it was; read the whole partition again: that read decides the loss, `acked_lost`.
   `sentinel_needed` says the two reads differ, that is, that records acknowledged before the crash were not served
   until a new write was committed (a restarted leader serves what is cluster-committed, so a read straight after a
   whole-set restart can be short without anything being lost on disk). `served_end_before_sentinel` and
   `served_end_after_sentinel` are the end offsets ListOffsets reported.

The client is the same for both systems: `kafka-verifiable-producer.sh` from the pinned apache/kafka image,
`acks=all`, idempotence on, at a fixed offered rate (default 1,000 records/s), bootstrapped at all three Kafka brokers
and at VTOP's range leader (the Kafka gateway). It prints one line per acknowledgement with its offset, so
"acknowledged" is what the client saw before the kill. Records are 1 KiB (`--record-size`, the issue's size): the stock tool has no size option (its `--value-prefix` is an integer and its value is `<prefix>.<n>`), so for a size the client is `crash/client/AckProducer.java`, the same `KafkaProducer` from the same pinned image (compiled in a multi-stage build with a pinned JDK image, since the Kafka image ships a JRE only; `ensure_client_image` builds it once, named for its source), paced the same way and printing the same lines, with the value padded to the size (`<crash>.<n>~xxx...`). It is used for both systems, so nothing differs but the system. `--record-size 0` runs the stock `kafka-verifiable-producer` and a few bytes a record, which needs no build. The size is part of the run's matrix (a resume cannot change it) and a column of every row. A bigger record means more dirty page cache per second, which is why the size matters to the loss window.

**Configurations** (`config`): `kafka-defaults`, `kafka-fsync-parity` (`log.flush.interval.messages=1` on every broker
through `crash/compose.kafka.fsync-parity.yml`, and `flush.messages=1` on the topic; the brokers' own report of the
setting is checked and a mismatch refuses the cell), and VTOP in three configurations that differ in the node's
`group_commit:` section only (#752, `crates/vtop-node/src/config.rs`): `vtop-shipped` (the section left out: the node as
shipped; an append is acknowledged after 2 of 3 replicas have fsynced it, and it answers both Kafka profiles),
`vtop-group-commit-on` and `vtop-group-commit-off` (the shipped leader file plus the section, in
`crash/leader-group-commit-{on,off}.yaml`; the followers never get one). The node ships with group commit **off**
(`GROUP_COMMIT_ON_BY_DEFAULT = false`), so `vtop-group-commit-off` would repeat `vtop-shipped`; by default a run takes
both Kafka profiles, `vtop-shipped` and the state that differs from the shipped one (today `vtop-group-commit-on`), read
from the source, and says so when `--configs` names two that coincide. At every bring-up the leader's own
`vtop_broker_group_commit_enabled` gauge is read and a mismatch refuses the cell, as the Kafka flush setting is.

**What `flakey-drop` simulates, and what it does not.** The three data directories are on one ext4 filesystem on one
dm-flakey device over a loop device. After the SIGKILL, `drop_writes` discards every write the page cache has not yet
sent to the device, and the filesystem is unmounted and mounted again. What is left is what had reached the device:
data an fsync returned for, and whatever the kernel's own writeback had already flushed. That is the loss of one host's
page cache for all three replicas at once: a kernel panic, or power loss with a disk that honours flushes. It does
**not** model a drive whose volatile write cache lies about flushes, a torn write inside a block, a host with a hardware
write-back cache, or independent crashes (the replicas share one failure domain, on purpose). Between the kill and the
drop there is a gap (`kill_to_drop_ms`, about 0.15 s here) in which the kernel may flush a little more; that can only
make a system look better. `sigkill` is the control: killing processes leaves the page cache intact, so no
acknowledged record may be lost in either system, and the canary says so.

The method is not the issue's first choice (`echo b > /proc/sysrq-trigger` in a VM that holds all three replicas, with the
client outside it): that needs a second machine for the client, and the harness here is one process on one host. The
two agree on what they simulate (the guest's page cache is lost and what an fsync returned for is kept); the issue's
calibration of one against the other is not done here.

**A restart that never serves is a row, not a stop.** If the cluster does not serve a write within 120 s of the
restart (the sentinel's deadline), the crash is a `failed` row with `needed_manual_repair` true and the reason in
`note`; the harness never repairs a cluster, and its node logs are kept under `logs/`. The run goes on.

**A run that looks wrong stops.** If Kafka's defaults lose nothing in `--suspect-after` (5) `flakey-drop` crashes, the
method is probably not dropping the page cache, and the run stops (exit 3) with that said, before any chart.

**Output.** `results/<run-id>/crash.csv` (columns in [`crash_schema.py`](crash_schema.py)) and `environment.json`
(`kind: "crash"`, schema 2; `matrix`, `comparable` and the verbatim Compose files and `powercut.sh`); `logs/<cell>/`
holds each crash's three node logs (as they died and after the restart) and a summary. The report generator reads a
run through `crash_schema.load_crash_run(run_dir, min_ok=N)`, which refuses a partial, mixed-environment or
unfinished run, a row that is `ok` while its canary did not behave, and fewer than `N` ok crashes in any cell, by name.
`crash_analysis.upper_bound_95(0, n)` gives the exact binomial bound to print beside every "0 of n": 13.9 % at 20
crashes, 9.5 % at 30. The issue's plan asks for 30 crashes per configuration (3.2 h projected with four configurations: `--crashes 30`).

## Known gaps

- Matrix: 100 B and 1 KiB, 1 partition, 1 producer, no compression, `defaults` only (no `fsync-parity`,
  `matched-timeouts`, 10 KiB, 6 partitions, 4 producers).
- VTOP runs without its metadata plane (static leadership): no lease, no failover, no durable offsets.
- Resources are the three nodes' summed CPU seconds and bytes read and written (no per-node breakdown, no RSS),
  and end-to-end latency is at one offered load, the common rate, at 1 ms resolution. A run of the schema-1 shape
  (`--no-e2e --no-resources`) has neither.
- Laptop numbers: Docker Desktop on one host, sharing the VM with whatever else runs there.

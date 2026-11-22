# Engine soak validation

The engine's other long tests run for minutes. This one runs the archive path,
Kafka to S3, for **72 hours at a stated rate with a fault on a schedule**, and
decides pass or fail from what the run recorded. It is the engine's answer to
"does it stay up, stay flat and stay correct for as long as a release
candidate would be deployed before anyone looks at it".

It is run by a maintainer before each release candidate. A 72 hour run needs a
dedicated host for three days, so no pull request and no scheduled job starts
one. A short profile, `ci-smoke`, runs the same machinery in about ten
minutes and proves the harness works.

The harness lives in `benchmarks/soak/` and the workflow is
`.github/workflows/soak.yml`. Issue #726 is the record of the requirement; it
closes when a maintainer attaches the evidence of a passing 72 hour run
([What to attach to a release](#what-to-attach-to-a-release)).

## What a run does

| | |
|---|---|
| Load | 1,000 records per second of about 700 bytes (random ASCII payload from Kafka's own producer performance tool, so the records are not numbered), over 3 partitions, for 72 hours. `acks=all`; the producer is restarted if a broker restart ends it. |
| Path under test | Kafka (single node, KRaft, plaintext) to the engine (`vtopctl run`, SQLite ledger, gzip level 1, 20,000 records or 30 s a batch) to the lab's MinIO. |
| Faults | One every 30 minutes from minute 30, cycling through six kinds, none in the last hour. 142 faults in all: 24 each of `kill9`, `broker_restart`, `partition` and `store_outage`, and 23 each of `clock_step` and `enospc`. |
| Sampled | Every 30 s: the engine process's resident memory and open files, the work directory's size, the ledger's size (the state database and its log), the engine's own counters, and the host's free disk. Every 60 s: the topic's end offset and the consumer group's committed offset. Hourly: a full copy of the engine's `/metrics`. |
| On a schedule | `prune-ledger` every hour. A `scrub` over 200 batches chosen at random every 6 hours. `audit-coverage` after every fault, once the engine has caught up. |
| At the end | The producer stops, the engine drains, and then: `audit-coverage --check-storage`, a full `scrub` of every committed batch, the group's committed offset against the topic's end on every partition, and `SIGTERM` to the engine, which must exit 0. |

The lab is `benchmarks/soak/docker-compose.soak.yml`: Kafka, MinIO, a
[toxiproxy](https://github.com/Shopify/toxiproxy) and the producer, as
containers, and the engine as a container too. The engine reaches Kafka and
MinIO only through the proxy; the producer reaches Kafka directly, so the load
goes on while the engine is cut off, as it would in production. Only the
proxy's admin port is published, on loopback.

## The faults

Each injector proves the fault landed before it counts. A partition that did
not partition is the run that looks healthiest, so a fault that cannot show
its effect fails the run and says what it saw.

| Fault | What is done | How it is proven | Live-chaos counterpart |
|---|---|---|---|
| `kill9` | `docker kill --signal=KILL` on the engine, then start it again | the container's exit status is 137 | scenario 04, kill -9 |
| `broker_restart` | `docker restart` of the Kafka broker (a graceful stop, then a start) | the container's start time changed and it is healthy again | |
| `partition` | both toxiproxy proxies disabled for 180 s: the engine can reach neither the broker nor the store | the engine container cannot open a connection through either proxy while it is cut, and can before and after | scenario 06, partition |
| `store_outage` | the store's proxy disabled for 300 s | the same, for the store only; the broker stays reachable | |
| `clock_step` | the engine is stopped and started again with its wall clock 3,600 s ahead of the host's, held there for 180 s, then stopped and started on the real clock. Successive clock steps alternate the direction: the next one starts 3,600 s behind | the shim records the time it loaded at, which must be the host's plus the step, and the real-clock start must record none | scenario 07, clock skew |
| `enospc` | a 256 KiB tmpfs is laid over the engine's work directory, in the engine's mount namespace, and filled with `dd` until a write fails; held full for 300 s; then lifted | a write to the directory works before; after the fill a write fails with `No space left on device`; the engine's own log shows that text while the directory is full; after the lift the directory has its original mount and a write works | the engine crash sweep's `enospc` leg (`ENGINE_SWEEP_FAULT=enospc`: a size-limited tmpfs at the work directory) |

The disk-full fault is the sweep's `enospc` leg made in a running engine. The
engine's container is not changed for it: it keeps no capabilities and its
read-only root. A throwaway container, started from the engine's own image and
privileged, joins the engine's PID namespace and runs `mount -t tmpfs` and
`umount` in the engine's mount namespace (`nsenter -t 1 -m`), so the tmpfs
exists for the engine's processes and nobody else, and goes with the container
if the engine dies. What the work directory's volume holds is hidden while the
tmpfs is there, not touched, and is back when it is lifted, with the batches
the engine had staged. The fill is 256 KiB, so the engine meets the full disk
on the first file it creates, not after a gigabyte of staging. The engine is
meant to survive the hold: one that exits during it is a finding, as in the
clock step. The space is freed by lifting the mount, and the fault is not
over until a write to the directory works again and the engine has caught up
(the recovery wait below, then `audit-coverage`). Because it needs Docker to
run a privileged helper, the driver lays the tmpfs and lifts it once, on the
idle engine, before the load starts, and refuses to run if it cannot: a host
that would fail the first disk-full fault, hours in, fails at the start.

The clock step uses the shim that scenario 07 embeds. The Dockerfile layer
(`Dockerfile.engine`) compiles it from that scenario's own source, which
`soak_lab.extract_skew_shim_source` reads out of it, so there is one copy and
a test pins that the extraction still works. A process's clock cannot be
moved from outside it, so the step is taken the way a host's clock step meets
a daemon: the engine is started into a clock that is no longer where it was.
The engine's upload requests are signed with its own clock, and the store
refuses a request that is more than 15 minutes from its own, so while the
clock is an hour off the engine cannot upload; that is part of what the fault
tests.

System model: a crash-recovery engine process, a partially synchronous
network, honest nodes. The invariants are the engine's: it never commits past
what it verified (safety), the archive has no hole (safety), and once a fault
is lifted the engine catches up within the recovery timeout (liveness). Timers
may affect liveness only; the clock step must leave safety intact.

After a fault the run waits for recovery before it goes on: the engine runs and
serves `/metrics`, the group's committed offset has moved, and the archive lag
is back under the profile's bound. The time that takes is recorded, and a
fault the engine does not recover from within the timeout fails the run.

## Pass and fail

`soak_eval.py` reads `samples.jsonl`, `events.jsonl` and `finals.json` from a
run directory and writes `verdict.json`. It asks the engine nothing, so a
maintainer can re-run it over an archived evidence tarball and get the same
answer. It exits 0 when every criterion passes, 1 when one fails and 2 when an
input cannot be read.

A criterion with nothing to judge fails: a run that injected no fault, or
sampled nothing, or ran no scrub does not pass.

| Criterion | Passes when |
|---|---|
| `run_completed` | the run ended without being aborted, and sampled to the profile's duration |
| `samples` | the resource samples (every 30 s) and the lag samples (every 60 s) are each counted on their own over the run's duration: at least 90% of each kind's expected number exist and observed something (a resource record the engine could not be probed for, or a lag record whose query failed, is not a sample), and none is more than 5 minutes after the one before |
| `faults_executed` | every fault in the plan was injected, landed, and the engine recovered from it |
| `recovery_time` | the slowest recovery was within the timeout (15 minutes) |
| `enospc_recovered` | for every disk-full fault in the plan (a profile without one is not asked): a write failed with `ENOSPC` and then worked again, the engine's log showed the condition, the engine's counters showed no commit beyond a verification from the fault's start until the engine had recovered, and the `audit-coverage` after the fault found no hole |
| `no_unplanned_exit` | the engine never exited except when a fault stopped it |
| `commit_rule` | `vtop_commits_total` never exceeded `vtop_verified_total` (two samples in a row, within one engine process, since a scrape reads the two a moment apart), and `vtop_verification_failures_total` never rose |
| `trend_rss_bytes`, `trend_open_fds`, `trend_ledger_bytes`, `trend_work_dir_bytes`, `trend_batch_seconds`, `trend_lag_seconds` | no growth trend over the last 80% of the run: see below |
| `lag_steady` | the archive lag, outside fault windows, has a 95th percentile of at most 180 s |
| `rate` | the topic's end offset advanced by at least 90% of the stated rate while the load ran (up to the producer's stop; the drain and the final checks after it are not production) |
| `ledger_prune` | `prune-ledger` ran on its schedule, at least 90% of the runs it should have, and none failed |
| `zero_coverage_holes` | `audit-coverage` was clean after every fault, and `audit-coverage --check-storage` was clean at the end. A Kafka partition's gap (a stretch nothing archived) does not fail the command, because compaction and transaction markers can explain one on a production topic; this lab has neither, so the soak reads the report and fails on any gap. The command also leaves what lies below a partition's first committed range unjudged, so the soak requires each partition's judged span to start at offset 0 (the topic is new) and, at the end, to stop at the partition's end offset |
| `scrub_clean` | every sampled `scrub` and the final full `scrub` were clean, and the final one read at least 1,000 batches |
| `tail_drained` | after the load stopped, the group's committed offset equals the end offset on every partition |
| `clean_shutdown` | the drained engine exits 0 on `SIGTERM` |
| `disk_headroom` | the host had at least 5 GiB free throughout |

### How a trend is judged

For each series the evaluator takes the last 80% of the run, reduces the
samples to the median of each time bucket (at most 240 per engine process),
and takes the Theil-Sen slope: the median of the slopes between pairs of
buckets. Pairs are only taken within one engine process. The engine restarts
on every `kill9` and `clock_step`, and a restart drops memory and open files
back to where they began; that is not a trend, and fitting across it would
hide a real one. A burst while a fault is active barely moves the median of the
slopes, where it would pull a least-squares line far off.

The growth over the window is the slope times the window's length. A series
fails when that growth exceeds both an absolute floor and a percentage of the
series' level, so a series that is tiny and noisy does not fail on a handful
of units:

| Series | Fails above | Samples left out |
|---|---|---|
| resident memory | 5% and 32 MiB | none: a leak during a fault is a leak |
| open files | 5% and 16 | none |
| ledger size | 5% and 8 MiB | none |
| work directory | 10% and 128 MiB | those inside a fault window, until the engine has recovered: a store outage legitimately fills it |
| mean batch duration (`vtop_batch_duration_seconds`, from the histogram's sum and count) | 10% and 2 s | those inside a fault window |
| archive lag (topic end minus committed offset, in seconds of load) | 10% and 10 s | those inside a fault window |

These limits are the profile's (`criteria.trend`), committed with the profile,
so a run is judged against what the file said when it ran. They were chosen
before any 72 hour run existed, from what the synthetic runs in the unit tests
separate (a steady leak from a restart sawtooth, a burst in a fault window, a
slow ramp before the window). The first real run is what tells whether they
are right, and they are changed in the profile, with the reason, if it does not.

### The ledger stays flat because `prune-ledger` runs on a schedule

The ledger holds a row for every batch the engine has committed, and nothing
deletes them unless an operator runs `prune-ledger`. Left alone, 72 hours at
this rate writes thousands of rows, so a ledger that grows says nothing about
a leak. The soak therefore runs `prune-ledger` every hour and judges the
ledger's size flat against that schedule. This is what an operator is
expected to do, and what the soak measures is that the engine's ledger, with
that done, does not grow.

The soak runs

```
vtopctl prune-ledger --config <config> --older-than-days 0 --limit 500
```

`--older-than-days 0` is accepted (the option is an unsigned integer with no
lower bound; only `--limit 0` is refused). It sets the cutoff to the present
moment, so every committed row older than now is deleted, **except the newest
committed row of each source path, which recovery reads its cursor from**, and
rows in any other state, which are untouched. The rows are written to a digest
before they are deleted, so `audit-coverage` and `scrub` still account for
them. The digest directory grows, by design and slowly; it is the audit trail,
not part of the ledger.

The ledger's size is the state database file plus its write-ahead log. A
SQLite file does not shrink when rows are deleted; its freed pages are reused,
so the file reaches a plateau, and that plateau is what a flat series is.

## Host requirements

| | |
|---|---|
| Operating system | Linux, x86_64 |
| Software | Docker with the Compose plugin, `python3` (3.9 or later), `bash`. Nothing is built on the host except Docker images. The disk-full fault runs a privileged throwaway container (see [The faults](#the-faults)), so Docker must allow one; the driver checks this before the load starts. |
| Disk | about 180 GB free on the disk that holds the lab's data (`--data-dir`, or Docker's data root) |
| Time | the host dedicated for 72 hours, the hour of quiet tail, the drain, the final checks and the teardown: plan for 80 hours |
| Runner | a GitHub Actions self-hosted runner registered with the label `vtop-soak` |
| Network | outbound, to pull the images and the Python packages the unit tests use |

The disk figure is arithmetic, not a measurement: 1,000 records a second of
700 bytes is 181 GB of record bytes over 72 hours. The archive's size depends
on what gzip level 1 makes of them, so it is below that, and the topic keeps
6 hours of records (about 15 GB), with MinIO's own overhead and the engine's
work directory and ledger on top. 180 GB leaves room for the case that the
payload does not compress at all. The driver refuses to start with less free
than the profile says (`min_free_gib`), and the run fails if free space drops
below 5 GiB at any sample.

### Registering the runner

Register the host as a self-hosted runner of this repository with the label
`vtop-soak`, as the runner software's own instructions say, and run it as a
service. Give it a user that can use Docker. Set the repository variable
`VTOP_SOAK_DATA_DIR` to a directory on the disk with the free space (the
default is `/var/lib/vtop-soak`). The workflow runs only on `workflow_dispatch`,
never on a pull request, so a change from outside the repository cannot reach
the runner.

## Running it

From the Actions tab, run the **soak** workflow and choose the profile `72h`.
The job `72 hour soak (self-hosted, vtop-soak)` builds the engine image,
runs the unit tests, runs the soak, writes the verdict to the job summary and
uploads the evidence tarball as the artifact `soak-72h-evidence`.

By hand, on the same host:

```bash
docker build -f docker/Dockerfile -t vtop-engine:local .
docker compose build minio
python3 benchmarks/soak/soak_driver.py --profile 72h --out /srv/soak \
    --data-dir /srv/soak/data --build-engine-from vtop-engine:local
```

The driver exits 0 when the run passed, 1 when it did not, and 2 when it could
not start. `--keep-lab` leaves the containers up afterwards for inspection;
`--project` changes the container-name prefix when more than one lab shares a
host. `SOAK_CPUS_KAFKA`, `SOAK_CPUS_MINIO`, `SOAK_CPUS_PRODUCER` and
`SOAK_CPUS_ENGINE` in the environment cap a container's CPUs, for a shared
machine; leave them unset on the host that produces the evidence. `--subnet`
gives the lab's network an explicit subnet, for a host whose Docker address
pools are used up.

### The `ci-smoke` profile

`ci-smoke` runs the same driver in about ten minutes: 300 records a
second over 2 partitions for 9 minutes, a fault every 75 seconds (each kind
once, and a second `kill9`), `prune-ledger` every minute and a sampled `scrub`
every 150 seconds. Its trend limits are loose (200%), because a nine minute run
is dominated by warm-up. It proves the lab starts, the load flows, every fault
lands and is lifted, the engine recovers, the checks run, the evaluator
reaches a verdict and the evidence tarball is written. **It is not evidence
that the engine does not leak.**

It runs on a GitHub-hosted runner when dispatched with the profile
`ci-smoke`, and on a pull request that changes only the soak's own files (the
soak directory, the workflow, this page, and the docs navigation, limitations
table, benchmark test paths and actionlint label file that a soak change also edits).

### What the smoke run showed

The harness was run end to end on a developer machine (Docker Desktop on
Apple silicon, so an arm64 Linux VM; Kafka and MinIO capped at one CPU each,
the producer at half a CPU and the engine at two; a release build of the
engine from commit `bc4c2856` in a stand-in Debian 13 image, not the image
`docker/Dockerfile` builds). The run took 9 minutes 27 seconds from start to
teardown, with the images already built, and passed all 20 criteria:

- all six faults landed and the engine recovered from each, the slowest in 18 s
  and the median in 10 s;
- seven `audit-coverage` runs were clean (one after each fault and the final
  `--check-storage`), and the final full `scrub` read 93 batches clean;
- both partitions' committed offsets reached the topic's end, and the engine
  exited 0 on `SIGTERM`;
- `prune-ledger --older-than-days 0` ran eight times without failing and
  deleted 109 ledger rows between them.

Two things the run showed about the engine:

- While the clock was an hour ahead, MinIO answered the engine's requests with
  `RequestTimeTooSkewed` (HTTP 403). The engine logged `batch failed`, then
  `batch did not reach VERIFIED; its range was handed back to the source and
  will be re-read`, and committed nothing from those batches; once the clock
  was back it archived them, and the coverage audit found no hole. That is the
  commit rule holding under a clock fault, in a nine minute run on one machine,
  which is all it is evidence for.
- The ledger's size (the state database and its write-ahead log) climbs from
  the engine's start to about 4 MiB in the first few minutes, then stays
  there, and starts again after a restart. This is why a trend is fitted
  within one engine process and why the ledger limit has an absolute floor.
  Whether the plateau holds for 72 hours is what the real run is for.

After the `enospc` fault was added the smoke run was repeated on the same
machine (the same caps; a release build of the engine on a stand-in Ubuntu
24.04 image). It passed every criterion, the new `enospc_recovered` among them,
over six faults, one of each kind: the disk-full fault held a 256 KiB tmpfs full
for 25 s, the engine logged `batch failed ... compression error: No space left
on device (os error 28)` ten times and carried on, and 12 s after the space was
freed it had committed past where it was; the audit after the fault was clean.

The `ci-smoke` job in `soak.yml` has not run on a GitHub-hosted runner yet:
it first runs when this workflow is on the default branch and someone
dispatches it, or on the next pull request that changes only the soak's files.

## Evidence

The driver writes a run directory, `soak-<profile>-<time>/`, and a tarball of
it. Everything the verdict rests on is in it:

| File | What |
|---|---|
| `profile.json` | the profile the run used, which holds the thresholds |
| `config/config.yaml` | the engine's configuration |
| `docker-compose.soak.yml` | the lab |
| `host.json` | the host: Docker version, operating system, CPUs and memory, the engine image |
| `samples.jsonl` | one record per sample: the raw series |
| `events.jsonl` | every engine start and exit, fault, recovery, prune, audit and scrub, with times |
| `raw/metrics-*.txt.gz` | the engine's full `/metrics`, hourly |
| `finals.json` | the checks after the load stopped |
| `verdict.json` | the evaluator's verdict, with the hashes of the three input files |
| `logs/*.log.gz` | the engine's, the producer's, Kafka's, MinIO's and the proxy's container logs |
| `SHA256SUMS` | the checksum of every other file |

To re-judge a tarball:

```bash
tar -xzf soak-72h-*.tar.gz
python3 benchmarks/soak/soak_eval.py --profile 72h --run-dir soak-72h-*/ \
    --out /tmp/verdict-again.json
```

### What to attach to a release

When a 72 hour run passes on a release candidate, attach to the release:

1. the evidence tarball (`soak-72h-evidence`, from the workflow run);
2. its `verdict.json`, and the line "72 hour soak: passed, run `<id>`, engine
   image built from `<commit>`" in the release notes' verification section.

Issue #726 closes at that point: its acceptance is a 72 hour run's host,
config, raw metrics and verdict attached to a release. A run that fails is
not attached; its tarball is the report of the bug it found.

## What this does not cover

Written down so the evidence is not read as more than it is.

- **The full disk is the work directory's, and small.** The `enospc` fault fills
  the engine's work directory, where it stages batches. It does not fill the
  state database's volume or the store's: a full ledger is a different failure
  (the engine cannot record a commit) and is not injected here. Live-chaos
  scenarios 05 and 05b cover a full or failing disk on the native cluster's
  nodes. The directory is filled to 256 KiB, not to the size of a production
  disk, so what the fault shows is the engine's reaction to the error, not how
  it behaves with a large backlog of staged data stuck behind it.
- **One engine, one broker, one store.** The Kafka broker is a single KRaft
  node with plaintext listeners; the store is the lab's MinIO. Nothing here
  speaks to a replicated Kafka cluster, to TLS, to another S3
  implementation, or to the PostgreSQL ledger (the ledger is SQLite).
- **The clock step is taken at a restart.** The engine's clock is moved by
  starting it into a skewed one, not by moving it under a running process. A
  clock that steps while the engine runs is not tested.
- **The payload is not numbered.** Kafka's producer tool generates random
  records, so the soak does not count each produced record in the archive. The
  hole check is the engine's own: `audit-coverage --check-storage` (the
  committed ranges tile every partition, and every object and manifest is in
  the store at its recorded size), `scrub` (every committed object is read back
  and verified against its manifest), and the committed offset reaching the
  topic's end. The failpoint sweep (`scripts/live-chaos/engine/run-sweep.sh`)
  is what counts every record.
- **The limits are untested against a real run.** See the paragraph under
  [How a trend is judged](#how-a-trend-is-judged).

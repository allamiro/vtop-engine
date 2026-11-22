# A Kafka topic to object storage: VTOP, Vector and Kafka Connect S3 (#746)

Use cases (a), (d) and (e) of [#744](https://github.com/allamiro/vtop-engine/issues/744): a Kafka topic archived
to an S3-compatible bucket by VTOP and by the two tools people use for the job, with the crash safety of each
measured by an independent checker. #595's rules 1 to 10 and #744's rules 11 to 21 apply. The shared code
(registry, schema, plan, resumable runner, checker, S3 tap, kill schedule, cgroup sampler, report) is
[`benchmarks/compare_common/`](../compare_common/); this package adds the producer, the offset sampler, the
bucket checker's key rules, the three tools' configurations and the lab.

| system | profile | what "committed" means, from its docs |
|---|---|---|
| VTOP (commit under test, MIT) Kafka source, `s3_native` | `strong` (default); `verify-off` (non-default, labelled: isolates the cost of verification, never run in a crash or fault cell) | the consumer offset is committed only after the batch is VERIFIED against the stored object; auto-commit is refused |
| Vector 0.61.0 (MPL-2.0) `kafka` source, `aws_s3` sink | `acks` (sink acknowledgements, memory buffer, `when_full: block`) and `disk-buffer` | with acknowledgements the offset advances after the sink delivered the event; with a disk buffer once the event is in the buffer (synced every 500 ms) |
| Kafka Connect S3 sink, Lenses Stream Reactor 12.1.2 (Apache-2.0), on the Connect runtime of `apache/kafka` 4.3.2 | `exactly-once` | `connect.s3.exactly.once.enable` (the documented default): the object is staged under `.temp-upload/`, copied to its final key under an ETag fence, and the offset moves only after the `.indexes/` entry is updated |

The source is one Apache Kafka 4.3.2 node (KRaft, RF=1, 3 partitions), identical for every tool and not under
test; the store is the lab MinIO build (`docker/minio`, upstream archived). Pins (by tag and digest, or zip sha256) are
in [`products.toml`](../compare_common/products.toml) and the compose file; a test fails if they differ.

Configurations: [`tools/`](tools/), one file per system and profile, with a comment per setting naming the
documentation page and the telemetry setting stated. [`tools.json`](tools.json) is the list of tools and
profiles. `run.py` refuses to start when one differs from `HEAD`, and the CSV carries each file's sha256.

## What a run does

Each system run is its own Compose project ([`compose.yml`](compose.yml)) on fresh volumes:

1. The source, the store and the **offset sampler** start. The sampler is a console consumer on `__consumer_offsets`
   that writes every offset commit, as the broker stamps it, to `commits.jsonl`; [`commits.py`](commits.py) turns it
   into the group's lag at any moment and into the time each record was committed, with no polling gap.
2. The **seeded producer** ([`producer.py`](producer.py)) writes records, each one compact JSON object of exactly the
   cell's size (100 B or 1 KiB) with `id` (`vtb-<run tag>-<sequence>`), `p` (partition) and `s` (sequence). A record
   is a pure function of the seed, the run tag, its sequence and its size; its partition is the murmur2 hash of its
   key (the broker's default partitioner), and `run.py` compares the partitions' end offsets with the counts the
   producer computed before a tool starts. A backlog is written before the tool starts, with `acks=all`. The ids,
   not Kafka offsets, are the ground truth: Vector does not preserve offsets.
3. The tool starts in its limit class (2 CPUs and 2 GiB, no swap; 1 CPU on a hosted runner; Connect's JVM at
   `-Xmx1g`). A tool that exits at start is a `refused` row with its own error; a configuration that cannot be
   written is a `refused` row, never a weaker one. Nothing is timed or crashed for a tool whose smoke did not pass.
4. The cell runs (below), then the run waits for **quiescence**: the group's lag is 0 and no new object has appeared
   for 75 s (one 60 s batch age and margin).
5. The **checker** ([`check_bucket.py`](check_bucket.py)) mirrors the whole bucket, gunzips every data object, parses
   its JSON lines and compares the embedded ids with the seeded set: `gaps`, `duplicates` (extra copies),
   `torn_objects` (cannot decompress, or a partial last line), `orphan_objects` (Connect's `.temp-upload/` and
   `.indexes/` keys and incomplete multipart uploads, listed by name) and `unexpected`. VTOP's manifest and receipt
   objects are metadata. It never reads a tool's state.

| cell | class | what |
|---|---|---|
| `smoke` | correctness | records across the 3 partitions, every one read back against the seeded value (byte-exact where a tool stores the value as it came, JSON-equal where Connect rewrites it); anything else and the tool is `failed` and nothing is timed |
| `drain` | performance | 2,000,000 x 100 B and 1,000,000 x 1 KiB pre-loaded; the window runs from the tool's start to the later of its last object and the group's lag reaching 0: records/s and MiB/s of source bytes, CPU-seconds per GiB and sampled peak RSS over that window (the tool's cgroup), store requests per GiB by method (MinIO's own counters, never the tap), object count and size p50 and p99 |
| `live` | performance | a producer at 50% of the slowest tool's median drain rate for the size; 2 min warm-up excluded, 10 min measured; per record the time to store-visible (MinIO's LastModified) and the time to source-committed (the broker's stamp on the group's first commit past it), p50 and p99; a tool that cannot hold the offered rate, or loses records, is `sustained = false` and not ranked |
| `crash-K1`, `crash-K2` | correctness | through the S3 tap: `kill -9` at the start of the Nth PUT (K1), or right after the store answered it (K2); restart at once; three kills per run; after the last, run unfaulted to quiescence, then check. A run whose tool went idle before every trigger fired keeps its observed counts and says so in `note` |
| `crash-K3` | correctness | `kill -9` at a seeded time after each (re)start |
| `verify-fault` | correctness | the tap flips one bit in the body of a read-back for one object in ten (a seeded hash of the key). Per run: objects selected, read back, noticed, and committed past ([`faults.py`](faults.py)) |

The kill schedule (the Nth PUT, the seconds) comes from a recorded seed and the run number, never from the tool, so
every tool meets the same schedule. `kills.csv` records each kill, its trigger and the time to the first object after
the restart.

## Run it

Needs Docker with Compose v2 and Python 3.9+. Plans are in [`plans.json`](plans.json); every flag is checked before any
Docker work. **Durations are plan settings**: a plan lists each cell's size, repetitions and (for `live`)
`warmup_seconds` and `measure_seconds`, and `minutes_per_run` is the estimate the projection adds up.

| plan | what | projected | where |
|---|---|---|---|
| `smoke` | every tool, 2,000 records read back, then one K2 kill at 20,000 | about 0.4 hours | any host; CI's nightly |
| `hosted-store-kills` | smoke, then K1 and K2 (100,000 x 1 KiB, 3 kills x 3 runs per kind) | about 3.4 hours | a hosted runner (correctness only) |
| `hosted-time-kills-fault` | smoke, then K3 and `verify-fault` | about 3 hours | a hosted runner (correctness only) |
| `hosted` | the two above as one plan | about 6.2 hours | one machine; over a hosted job's limit |
| `full` | every cell at the sizes #746 names, Vector in both profiles, VTOP in `strong` and `verify-off` for `drain` and `live` | about 16 hours | the dedicated Linux host, in resumable pieces |

The projections are estimates from per-run costs, not measurements. `python3 benchmarks/archive_compare/run.py --plan
full --dry-run` prints the cells and the sum; `--max-hours` refuses a plan over it.

```bash
# What a plan owes and how long it is projected to take.
python3 benchmarks/archive_compare/run.py --plan full --dry-run

# The smoke plan: every tool, tiny data, minutes (laptop or CI). Builds vtop-compare:<engine tree> from
# docker/Dockerfile and vtop-connect-lenses:12.1.2 (the connector zip, checked against its sha256) if absent.
python3 benchmarks/archive_compare/run.py --plan smoke --host-class laptop

# The full plan on the dedicated Linux host, in resumable pieces.
python3 benchmarks/archive_compare/run.py --plan full --budget-minutes 180 --cooldown-seconds 60
python3 benchmarks/archive_compare/run.py --resume archive-<stamp> --budget-minutes 180
# (a resume reads the plan, the --tools selection and the host class the run recorded; repeating one that
# disagrees is refused)

# What a run directory still lacks: exit 0 when complete, 2 with the missing keys listed.
python3 benchmarks/archive_compare/run.py --check benchmarks/archive_compare/results/archive-<stamp>

# The checker alone, on a mirror of a bucket.
python3 benchmarks/archive_compare/check_bucket.py --objects <dir> --tag <run tag> --expected <N>

# Charts and docs/benchmarks/archive-comparison.md, with the pinned plotting stack.
python3 -m pip install --require-hashes -r benchmarks/kafka_compare/requirements-report.txt
python3 benchmarks/archive_compare/report.py benchmarks/archive_compare/results/archive-<stamp>
python3 benchmarks/compare_common/regenerate.py      # every committed run, as CI's drift check does
```

On a host whose Docker address pools are used up by other projects' networks ("all predefined address pools have been
fully subnetted"), set `VTOP_LAB_SUBNET` (for example `10.207.146.0/24`, one not in use) and each run's lab network
takes that subnet. `VTOP_SOURCE_CPUS` and `VTOP_SOURCE_MEMORY` set the source's limits (default 2 CPUs, 3g; recorded in
`environment.json`), for a laptop that cannot spare them.

The store's Compose service is not named `minio`: tools dial the alias `minio`, and in tap cells the S3 tap takes
that alias while the store keeps `minio-real`. A service name is a DNS name too, so a service called `minio` would
answer beside the tap and let a request skip it.

The harness's own listings (it polls the bucket to know when a tool is quiet) are requests to the store whose
counters give each tool's request cost. They are measured once at an idle moment and taken out of the counts, at the
price of one listing per poll; that price is an upper bound for the early polls, so a tool's LIST count can be
understated but does not grow with the length of the run.

The correctness plans also run on a GitHub-hosted runner:
[`archive-compare.yml`](../../.github/workflows/archive-compare.yml) (`workflow_dispatch` with `plan` = `smoke`,
`hosted-store-kills` or `hosted-time-kills-fault`; `smoke` nightly, never on a pull request). The run directory is
uploaded as an artifact; a data PR commits it under `results/` by hand.

## Adding a tool or a profile

[`tools.json`](tools.json) is the one list. A new profile of an existing tool is an entry under its `profiles`, its
configuration file, and (if the compose service needs a file it does not mount yet) a volume line; `kind.PROFILES`
reads the list, so `run.py`, the plan check and the report pick it up. A new tool is also a Compose service of the
same name, a product in [`products.toml`](../compare_common/products.toml) with its licence evidence, and a slot in
the `archive` family's order (a colour). The Aiven S3 sink 3.4.4, if the maintainer wants it as a second Connect
entry (#746 comment, "optional second Connect profile in `smoke` and the crash cells"), is exactly that, with two
deviations to state: it flushes by `offset.flush.interval.ms` and `file.max.records` (no byte threshold), and its
`format.output.type=jsonl` wraps the value under `format.output.fields`. It is **not** shipped here: the open
question is the maintainer's.

## Output

`results/<run-id>/archive.csv` (one row per system, profile, cell, record size and repetition; the columns are
`compare_common.schema`'s plus [`kind.py`](kind.py)'s), `kills.csv`, `environment.json` (host, Docker VM, limits, image
digests, every configuration verbatim with its sha256 and the Connect worker's, the JVM flags, the telemetry
statement of each tool, the plan, the deviations), and `logs/<key>/` (the tool's log, the tap trace in tap cells, the
offset commits, the producer's log, the bucket snapshot the checker read).

## Deviations (rule 15), recorded in every run's `environment.json`

- VTOP and Vector store the Kafka value as it came, newline-framed; Connect (Lenses) parses it with its JSON converter
  and writes it again. The checker reads the id token in either form; MiB/s is of source bytes.
- Each tool's documented threshold nearest to "about 32 MiB uncompressed or 60 s": VTOP `batching.max_bytes` and
  Vector `batch.max_bytes` count uncompressed payload, Connect's `flush.size` counts the staged file. The measured
  object sizes are reported.
- Connect's `.indexes/` entries are permanent bookkeeping and are counted as orphans, as the issue specifies; its
  orphan count has a floor.
- Connect's offsets are committed by the worker at `offset.flush.interval.ms` (60 s), Vector's source every 5 s,
  VTOP's after VERIFIED: the time to source-committed carries that, and is a result.
- The producer is `kafka-console-producer.sh` (acks=all, linger.ms=5) fed by the seeded generator, one process, so a
  record's offset is its partition's count; its produce time is the generator's write time.
- Time to store-visible, time to source-committed and the produce time share the Docker VM's clock; the window start
  and the kill times are the harness host's clock, the same clock on a Linux host.
- Connect runs standalone (one worker, no internal topics).

## Known limits

- **The nightly smoke's job result.** `run.py` exits 0 when the run directory is schema-valid and complete, and a
  `failed` or `refused` row is complete. A tool that loses records or whose configuration is rejected in the smoke is
  therefore a recorded row (every later cell of that tool is `refused` behind it), not a red job: whoever reads the
  nightly artifact (`archive.csv`, or `--check`'s "problem" lines) sees it, the job itself does not turn red.
- **A kill run that took fewer kills than planned** keeps its observed counts and a `note`; the report does not yet
  leave it out of the crash totals (the shared schema has no hook for that yet).
- **The S3 tap's K2 gate** holds the requests of the other connections while a kill is pending; the response of a
  request already in flight on another connection is not held (the shared tap, not this harness).

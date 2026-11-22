# Files and a syslog spool to object storage: VTOP, Vector and Fluent Bit (#748)

Use cases (b) and (d) of [#744](https://github.com/allamiro/vtop-engine/issues/744): append-only files
and a syslog spool, delivered into object storage, with the crash safety of each tool measured by an
independent checker. #595's rules 1 to 10 and #744's rules 11 to 21 apply. The shared code (registry,
schema, plan, resumable runner, checker, S3 tap, kill schedule, cgroup sampler, report) is
[`benchmarks/compare_common/`](../compare_common/).

| system | profile | what "committed" means, from its docs |
|---|---|---|
| Fluent Bit 5.2.0 `tail` -> `s3` | `durable`: `storage.type filesystem`, `db.sync full`, `storage.sync full` | offsets in the `db` SQLite file; after an unexpected stop "at-least-once", the offset may trail the processed position; the S3 output re-sends what is left in `store_dir` and states no formal guarantee |
| VTOP (commit under test) File and Syslog-spool sources, `s3_native` | `strong` (default) | the cursor moves only after the object and its manifest are verified; a partial trailing line is never committed |
| Vector 0.61.0 `file` -> `aws_s3` | `acks` (sink acknowledgements, memory buffer, `when_full: block`) and `disk-buffer` | with acknowledgements and a memory buffer the checkpoint advances after delivery; with a disk buffer, once the event is in the buffer (synced every 500 ms) |
| OpenTelemetry Collector 0.165.0 (optional, off in the shipped plans) | `file-storage` | per the receiver's stored offset and the exporter's persistent queue; the exporter is alpha |

Configurations: [`tools/`](tools/), one file per system and profile, with a comment per setting naming
the documentation page and the telemetry setting stated. `run.py` refuses to start when one differs from
`HEAD`, and the CSV carries each file's sha256.

## What a run does

Each system run is its own Compose project ([`compose.yml`](compose.yml)) on fresh volumes:

1. The lab MinIO (`docker/minio`, built from source; upstream archived) and the **generator**
   ([`generator.py`](generator.py)) start. The generator is the only writer of the spool and owns every
   rotation: seeded RFC 5424 lines of 200 bytes, each with a unique `id=vtb-<run tag>-<seq>`, into
   `/spool/<source>/app.log`, rotated by size. `rename` moves `app.log` to `app.log.<k>` and creates a
   new one; `copytruncate` copies it to `app.log.<k>` and truncates it in place. Rotated files are never
   deleted: they are the source of truth.
2. The tool starts in its limit class (2 CPUs and 2 GiB, no swap; 1 CPU on a hosted runner), reading the
   spool volume read-only, its state on its own volume. Every tool is given the same pattern, the active
   file and its rotated names (`app.log*`). A tool that exits at start is a `refused` row with its own
   error; a configuration that cannot be written is a `refused` row, never a weaker one.
3. The cell runs (below), then the run waits for **quiescence**: at least one object and no new object
   for 75 s (one 60 s batch age and margin).
4. The **checker** mirrors the tool's prefix and counts gaps, duplicates, torn and orphan objects
   against the generator's sequence numbers. It never reads a tool's state.

| cell | class | what |
|---|---|---|
| `smoke` | correctness | 10,000 lines across rotated files; every line present exactly once, or the tool is `failed` and nothing is timed |
| `drain` | performance | a 4 GiB backlog across rotated files; lines/s and MiB/s of source bytes, CPU-seconds per GiB and sampled peak RSS (the tool's cgroup), store requests per GiB (MinIO's own counters) |
| `live` | performance | the generator at 50% of the slowest tool's median drain rate for the variant; 2 min warm-up, 10 min measured; write-to-visible p50 and p99 per line |
| `crash-K1`, `crash-K2` | correctness | through the S3 tap: `kill -9` at the start of the Nth PUT (K1) or right after the store answered it (K2); restart; three kills per run |
| `crash-K3` | correctness | `kill -9` at a seeded time after each (re)start |
| `crash-K4-rotation` | correctness | the harness rotates the active file at a seeded moment and the kill follows inside a seeded window (0 to 500 ms); then lines are written to the new file and the tool restarts |
| `down-while-rotating` | correctness | the tool drains a third, is stopped, the file is rotated twice with lines written after each, the tool starts; a run where the tool was running at a rotation is `refused` |

Variants: `source` (`file`: VTOP's File source; `syslog-spool`: VTOP's Syslog-spool source; the other
tools read both with the same input) and `rotation` (`rename`, `copytruncate`).

## Run it

Needs Docker with Compose v2 and Python 3.9+. Plans are in [`plans.json`](plans.json); every flag is
checked before any Docker work.

```bash
# What a plan owes and how long it is projected to take.
python3 benchmarks/spool_compare/run.py --plan full --dry-run

# The smoke plan: every tool, tiny data, minutes (laptop or CI). Builds vtop-compare:<engine tree> from
# docker/Dockerfile if it is not there yet.
python3 benchmarks/spool_compare/run.py --plan smoke --host-class laptop

# The full plan on the dedicated Linux host, in resumable pieces.
python3 benchmarks/spool_compare/run.py --plan full --budget-minutes 180 --cooldown-seconds 60
python3 benchmarks/spool_compare/run.py --plan full --resume spool-<stamp> --budget-minutes 180

# What a run directory still lacks: exit 0 when complete, 2 with the missing keys listed.
python3 benchmarks/spool_compare/run.py --check benchmarks/spool_compare/results/spool-<stamp>

# Charts and docs/benchmarks/spool-comparison.md, with the pinned plotting stack.
python3 -m pip install --require-hashes -r benchmarks/kafka_compare/requirements-report.txt
python3 benchmarks/spool_compare/report.py benchmarks/spool_compare/results/spool-<stamp>
python3 benchmarks/compare_common/regenerate.py      # every committed run, as CI's drift check does
```

On a host whose Docker address pools are used up by other projects' networks ("all predefined address
pools have been fully subnetted"), set `VTOP_LAB_SUBNET` (for example `10.207.48.0/24`, one not in use)
and each run's lab network takes that subnet.

The store's Compose service is not named `minio`: tools dial the alias `minio`, and in crash cells the S3 tap
takes that alias while the store keeps `minio-real`. A service name is a DNS name too, so a service called
`minio` would answer beside the tap and let a request skip it.

The harness's own listings (it polls the bucket to know when a tool is quiet) are requests to the store whose
counters give each tool's request cost. They are measured once at an idle moment and taken out of the counts, at
the price of one listing per poll; that price is an upper bound for the early polls, so a tool's LIST count can be
understated but does not grow with the length of the run.

The correctness plans also run on a GitHub-hosted runner:
[`spool-compare.yml`](../../.github/workflows/spool-compare.yml) (`workflow_dispatch` with
`plan` = `smoke`, `hosted-crash` or `hosted-rotation`; `smoke` nightly). The run directory is uploaded as
an artifact; a data PR commits it under `results/` by hand.

## Output

`results/<run-id>/spool.csv` (one row per system, profile, cell, variant and repetition; the columns are
`compare_common.schema`'s plus [`kind.py`](kind.py)'s), `kills.csv` (one row per kill: kind, trigger, the
Nth PUT or the seeded time, kill and restart times, the tool's last log line), `environment.json` (host,
Docker VM, limits, image digests, every configuration verbatim with its sha256, the telemetry statement
and the durability settings of each tool (`durability`, e.g. Fluent Bit's `storage.type` and `db.sync`),
the plan, the deviations), and `logs/<key>/` (the tool's log, the tap trace in K1 and K2, the
generator's state, the bucket snapshot the checker read).

## Deviations (rule 15), recorded in every run's `environment.json`

- VTOP stores each line as read; Vector and Fluent Bit wrap each line in JSON (their documented JSON
  output). The checker reads the id token in either form; MiB/s is of source bytes.
- Fluent Bit's closest settings to "about 32 MiB or 60 s" are `total_file_size 32M` and
  `upload_timeout 60s`.
- Rotated names are `app.log.<k>` with k never reused (no shifting of `.1` to `.2`), so no file is
  renamed twice.
- Time to store-visible uses the generator's write time and MinIO's LastModified (ms), both on the Docker
  VM's clock.

## Where it runs

- **Laptop**: tests, `--plan smoke`, charts. No `drain`, no `live`.
- **Hosted runner**: `smoke`, `hosted-crash`, `hosted-rotation` (correctness only).
- **Dedicated Linux host**: `--plan full` (projected 18.5 hours by `plan.py`'s per-run estimates).

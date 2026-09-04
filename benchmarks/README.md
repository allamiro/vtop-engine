# VTOP Benchmark Framework

A self-contained harness for measuring the VTOP engine under many realistic
conditions — different input volumes, file sizes, formats, batch settings,
compression, checksums, upload backends, fault injection, and long-running
workloads. It **drives the compiled `vtopctl` binary** and never imports engine
code, so benchmark logic stays fully separate from the engine.

Output is plain **CSV + JSON** under `results/<run_id>/` for later analysis, and
runs **never overwrite** prior results.

## Requirements

- The built engine binary (`target/release/vtopctl`). The runner builds it
  automatically if missing, or set `VTOPCTL_BIN=/path/to/vtopctl`.
- Python 3.9+. Optional but recommended:
  ```bash
  pip install -r benchmarks/requirements.txt   # PyYAML + psutil
  ```
  Without them the framework still runs (a minimal YAML parser handles the flat
  scenario files, and system metrics fall back to `ps`).

## 1. Start the benchmark stack (only for the MinIO backend)

In-memory scenarios (`backend: mock` / `mock_fail`) need **no** services. For
the real-upload scenario (`backend: minio`):

```bash
docker compose -f benchmarks/docker-compose.benchmark.yml up -d
# MinIO console: http://localhost:9001  (minioadmin / minioadmin)
```

Buckets are provisioned by the one-shot `minio-init` service; on the very
first boot of a fresh volume, let it exit before starting a run
(`docker compose -f benchmarks/docker-compose.benchmark.yml ps -a` shows it
`Exited (0)`) or the first upload can fail with `NoSuchBucket`. The buckets
live in the named volume from then on, so this is a first-boot wait only.

## 2. Generate seed data

Seed data is generated automatically per scenario, but you can also produce it
standalone:

```bash
python3 -c "import sys; sys.path.insert(0,'benchmarks'); \
from lib import seed; print(seed.generate_dataset('/tmp/seed','jsonl',1000,'small'))"
```

Size classes: `small` (1 KB–64 KB), `medium` (1 MB–10 MB), `large`
(100 MB–1 GB), `mixed`. Formats: `jsonl`, `csv`, `txt`/`log`, `cef`, `leef`,
`syslog`, `mixed`, `binary`.

## 3. Run one scenario

```bash
python3 benchmarks/run_benchmark.py benchmarks/scenarios/01-small-jsonl-gzip.yaml
```

Results land in `benchmarks/results/<run_id>/` (unique per run).

## 4. Run the full matrix

```bash
python3 benchmarks/run_matrix.py --all
# or a subset:
python3 benchmarks/run_matrix.py benchmarks/scenarios/01-small-jsonl-gzip.yaml \
                                 benchmarks/scenarios/02-medium-cef-zstd.yaml
```

This runs each scenario and writes `results/matrix-<stamp>/matrix.csv` +
`matrix.md` comparing them side by side.

## 5. Read the metrics files

Each `results/<run_id>/` contains:

| File | Granularity | Key columns |
|------|-------------|-------------|
| `metrics.csv` | one row per run | throughput, latency p50/p95/p99, compression, cpu/mem |
| `batch_metrics.csv` | one row per batch | per-stage durations, compression ratio, status |
| `state_transition_metrics.csv` | one row per state transition | `from_state→to_state`, duration |
| `upload_metrics.csv` | one row per object | object size, upload duration + speed, retries |
| `replay_metrics.csv` | one row per replay | failed state, replay duration, success |
| `backlog_metrics.csv` | one row per sustained-load cycle | bytes seeded vs archived, and the deficit between them |
| `system_metrics.csv` | one row per sample | cpu%, memory, disk, network |
| `summary.json` / `summary.md` | run rollup | everything above, aggregated + bottleneck notes |

All timestamps are ISO 8601 (UTC).

## 6. Compare results

- Across runs: open `matrix.csv` (from `run_matrix.py`) in any spreadsheet / pandas.
- Within a run: `summary.md` for a human view; CSVs for analysis.
- Example (pandas):
  ```python
  import pandas as pd, glob
  df = pd.concat(pd.read_csv(f) for f in glob.glob("benchmarks/results/*/metrics.csv"))
  df.groupby("scenario_name")[["throughput_mb_per_sec","p95_latency_ms"]].mean()
  ```

## 7. Long-duration tests

Set `duration_seconds` in a scenario (e.g. `06-longrun-5min.yaml` = 300 s,
also 1800 / 3600 for 30 min / 1 h). The runner re-seeds fresh files each cycle
to sustain load and samples system metrics throughout. Each run gets its own
`run_id` directory — long runs never clobber earlier ones.

### Sustained backpressure (#98)

Sustained load is not the same as a **backlog**. `process-once` drains
everything it can see before returning, so work added between cycles is work
the engine was never behind on: serially the deficit is zero at the end of
every cycle, at any volume. That is the property the original "1M records/sec
for 5 minutes" framing was really after — Kafka's producers do not wait — and
it comes from the *source*, not from the record rate.

`seed_concurrently: true` runs the seeder beside the engine, adding
`volume x backlog_multiplier` files every `seed_interval_seconds`. Above what
a cycle can drain, a real deficit accumulates — at whatever scale the disk can
afford, which is why `11-backpressure-soak.yaml` reaches the condition on an
ordinary machine instead of needing 45 GB and a producer rig.

Three outputs answer the questions the issue asks:

| output | reads on |
|--------|----------|
| `backlog_metrics.csv` | the deficit per cycle, in bytes seeded vs bytes archived. Whether it **plateaus or climbs** is the whole hypothesis; a shape needs samples, not an end-of-run total |
| `ledger_bytes` / `ledger_rows` / `ledger_bytes_per_batch` | ledger growth. Rows scale with BATCH count, so a low `batch_max_records` grows the ledger far faster per byte archived than a flood does — this is tested *better* small than large |
| `recovery_ms` | what it costs to open that ledger afterwards, with no work left to do (#77 loads the whole thing into memory at startup) |

## 8. Clean benchmark data

```bash
rm -rf benchmarks/results/*        # results/ is git-ignored
```

MinIO objects live in the named volume `bench-minio-data`, which survives
`docker compose ... down`. Drop them wholesale by removing the volume:

```bash
docker compose -f benchmarks/docker-compose.benchmark.yml down -v
```

or delete a single run — every run namespaces its objects under its `run_id`.
The compose stack's `mc` alias lives only inside the ephemeral init
container, so point one at the published port first, with the same
`MINIO_ROOT_*` overrides the stack itself honors (#81). `${VAR:-default}`
below reads only the shell — if your overrides live in `benchmarks/.env`,
read the two values out and plug them in (`grep -E
'^MINIO_ROOT_(USER|PASSWORD)=' benchmarks/.env`); `.env` is compose DATA,
not shell code, so it is parsed, never sourced — the same rule the smoke
scripts follow, and sourcing would also let a filed value clobber an
exported one, inverting compose's shell-wins precedence. The bucket is
whatever the scenario filed (`vtop-bench-soak` for the soak):

```bash
mc alias set local http://localhost:9000 \
  "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-minioadmin}"   # once per host
mc rm -r --force "local/<bucket>/<run_id>/"
```

The prefix makes runs separable; only the volume or prefix deletion above
bounds growth — nothing expires benchmark objects automatically.

## 9. Add a new scenario

Copy any file in `scenarios/`, change the knobs, drop it in `scenarios/`.
Every parameter is configurable (see `lib/scenario.py` `DEFAULTS`):
volume, file_size, format, batch_max_records/bytes/age, compression(+level),
checksum, backend, duration_seconds, fault, sys_sample_interval, bucket,
endpoint_url. `run_matrix.py --all` automatically picks it up.

## Benchmark matrix coverage

| Dimension | Supported now | Notes |
|-----------|---------------|-------|
| File volume | ✅ 1k–1M (configurable) | very large volumes need disk + time |
| File sizes | ✅ small / medium / large / mixed | |
| Batch size | ✅ by count, by bytes, by time window | `batch_max_records/bytes/age` |
| Formats | ✅ jsonl, csv, txt, cef, leef, syslog, mixed, binary | binary/compressed sources via whole-file (`whole_file`) mode — scenario `10-binary-localfs` |
| Compression | ✅ none / gzip / zstd | |
| Checksum | ✅ sha256 / blake3 / disabled | all three engine modes (protocol §10) — scenarios `08-blake3-jsonl`, `09-checksum-disabled` |
| Upload backend | ✅ MinIO, in-memory mock, localfs; AWS S3 via endpoint+creds | `localfs` (VTOP-LocalFS profile) driven by scenario `10-binary-localfs` |
| Failure conditions | ✅ verification failure, replay/recovery | `backend: mock_fail`, `fault: replay` |
| Runtime duration | ✅ any (`duration_seconds`) | 5 min / 30 min / 1 h presets easy to add |
| Sustained backpressure | ✅ `seed_concurrently` + `backlog_multiplier` | a real deficit, not just sustained load — scenario `11-backpressure-soak` (#98) |

## Native segment write amp / proof overhead (#189)

The Python scenarios above drive the **archive** `vtopctl` path. Native
segment write amplification and proof-carrying overhead are measured in-process
by the Rust harness (see
[`docs/WRITE_AMP_PROOF_OVERHEAD.md`](../docs/WRITE_AMP_PROOF_OVERHEAD.md)):

```bash
mkdir -p benchmarks/results/native-write-amp
VTOP_WRITE_AMP_JSON=benchmarks/results/native-write-amp/summary.json \
  cargo test -p vtop-log --test write_amp_proof_harness --locked -- --nocapture
```

That report complements matrix issues #92 / #98 / #130; it does not claim
Kafka superiority.

## Native fetch I/O research (#190)

Native fetch I/O strategy research (buffered page cache vs Linux
`sendfile`/`splice` vs experimental `O_DIRECT`, with an explicit gate before
`io_uring`) is measured in-process by the Rust harness (see
[`docs/FETCH_IO_RESEARCH.md`](../docs/FETCH_IO_RESEARCH.md)):

```bash
mkdir -p benchmarks/results/native-fetch-io
VTOP_FETCH_IO_JSON=benchmarks/results/native-fetch-io/summary.json \
  cargo test -p vtop-log --test fetch_io_research_harness --locked -- --nocapture
```

This is a research harness — it does not ship three production fetch engines.
It complements matrix issues #92 / #98 / #130 and does not claim Kafka
superiority.

## Native metadata saturation research (#192)

Single three-node metadata Raft saturation and sharding-trigger criteria are
measured in-process by the Rust harness (see
[`docs/METADATA_SATURATION_RESEARCH.md`](../docs/METADATA_SATURATION_RESEARCH.md)):

```bash
mkdir -p benchmarks/results/native-meta-saturation
VTOP_META_SATURATION_JSON=benchmarks/results/native-meta-saturation/summary.json \
  cargo test -p vtop-meta --test metadata_saturation_harness --locked -- --nocapture
```

This is a research harness — it does **not** implement multi-group metadata
sharding (epic #93). Multi-hour dedicated soaks remain deferred.

## Design principles

- Benchmark logic is **separate** from engine logic (drives the binary only).
- **No hardcoded paths** — output dirs, seed dirs, and the binary are configurable.
- **Every parameter** is scenario-configurable.
- Results are **reproducible** (seedable generators) and **never overwritten**.
- Simple **CSV/JSON** output for later analysis.
- Local **Docker Compose** first; structure is extensible toward Kubernetes.

## Known limitations

- **System metrics** are best with `psutil`; the `ps` fallback reports CPU%/RSS
  of the process tree only (disk/network show 0).
- **Mid-flight restart** is approximated via the fault/replay path
  (`mock_fail` → failed batches → recovery), not a hard kill at a random instant.
- Very large volumes (100k–1M files) are supported but bounded by local disk and
  time; start small and scale up.

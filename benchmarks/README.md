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

## 8. Clean benchmark data

```bash
rm -rf benchmarks/results/*        # results/ is git-ignored
```

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
| Formats | ✅ jsonl, csv, txt, cef, leef, syslog, mixed; ⚠️ binary | engine is line-oriented; binary archives as raw |
| Compression | ✅ none / gzip / zstd | |
| Checksum | ✅ sha256; ⚠️ blake3 / disabled | engine currently SHA-256 only (recorded as requested) |
| Upload backend | ✅ MinIO, in-memory mock; AWS S3 via endpoint+creds | a local-fs backend is a planned follow-up |
| Failure conditions | ✅ verification failure, replay/recovery | `backend: mock_fail`, `fault: replay` |
| Runtime duration | ✅ any (`duration_seconds`) | 5 min / 30 min / 1 h presets easy to add |

## Design principles

- Benchmark logic is **separate** from engine logic (drives the binary only).
- **No hardcoded paths** — output dirs, seed dirs, and the binary are configurable.
- **Every parameter** is scenario-configurable.
- Results are **reproducible** (seedable generators) and **never overwritten**.
- Simple **CSV/JSON** output for later analysis.
- Local **Docker Compose** first; structure is extensible toward Kubernetes.

## Known limitations

- **BLAKE3 / checksum-disabled** are matrix dimensions the engine does not yet
  implement; scenarios record the requested value but the engine uses SHA-256.
- **Binary / compressed-source files**: the engine is line-oriented, so binary
  inputs archive as `raw` and yield few records. A whole-object source mode is a
  follow-up.
- **A local-filesystem upload backend** is not yet implemented; use `mock`
  (in-memory) or `minio` for backend comparisons.
- **System metrics** are best with `psutil`; the `ps` fallback reports CPU%/RSS
  of the process tree only (disk/network show 0).
- **Mid-flight restart** is approximated via the fault/replay path
  (`mock_fail` → failed batches → recovery), not a hard kill at a random instant.
- Very large volumes (100k–1M files) are supported but bounded by local disk and
  time; start small and scale up.

# Long-link pipelining: measurements

The runs behind the pipelined archive cycles and the long-link tuning note,
taken with `benchmarks/long_link/` (its README describes the rig). One row per
run in `runs.csv`, as `analyze.py --csv` prints it.

## Conditions

- **Host:** Apple M5 Max, Docker Desktop. The Linux VM had 18 CPUs and
  55 GiB, kernel 7.0.14-linuxkit. It was shared with other work throughout:
  the host's one-minute load average was between 37 and 97 during the runs.
- **Link:** 200 ms round trip added on MinIO's path back to the engine (a
  `netem` delay); no bandwidth cap; no loss.
- **Broker:** Kafka 3.9.1, one KRaft node, `--cpus=1`, beside the engine, not
  delayed.
- **Store:** MinIO, the lab image `vtop-minio:RELEASE.2025-09-07T16-13-09Z`,
  `--cpus=1`.
- **Engine:** `--cpus=2`, `debian:stable-slim`.
- **Workload:** 1 GiB (3,957,651 records) of `gen.py` JSONL in topic `p1` (one
  partition) and `p4` (four), drained from offset 0 by a fresh consumer group
  each run.
- **Engine configuration:** `config.yaml.in`: `max_bytes` 16 MiB unless the
  run name says `mb64` (64 MiB), `max_records` 1M, `max_batch_age_seconds`
  10, eight batches wide, zstd level 3, SHA-256 (store-computed) unless it
  says `blake3` (read-back), strong verification.
- **Builds:**
  - `base-*`: v0.10.3 (8c834942);
  - `pr3-*`: the pipelined engine at 8a8104dc (this change's first commit;
    later commits change tests, docs and the review fix to throttle
    counting, not the batch path).
- **Slow start:** `ssai0` runs set the engine's network namespace's
  `net.ipv4.tcp_slow_start_after_idle=0`; the others leave Linux's default
  (1).
- **Order:** base and pipelined runs were interleaved (p1, then p4, three
  rounds) so host load fell on both.

## Results (medians)

| Build and setting | Partitions | Runs | MiB/s end to end, median (range) | MiB/s to 90% | CPU cores | object upload ms | manifest PUT ms | verify ms |
|---|---|---|---|---|---|---|---|---|
| v0.10.3 | 1 | 3 | 8.4 (8.4–8.5) | 9.3 | 0.08 | 639 | 207 | 405 |
| v0.10.3 | 4 | 3 | 8.4 (8.4–9.3) | 9.5 | 0.10 | 909 | 216 | 405 |
| v0.10.3, slow start after idle off | 1 | 2 | 10.2 (9.9–10.4) | 11.2 | 0.11 | 238 | 207 | 405 |
| v0.10.3, slow start after idle off | 4 | 2 | 12.7 (12.1–13.4) | 14.3 | 0.13 | 292 | 208 | 404 |
| this PR | 1 | 3 | 33.5 (33.3–34.6) | 54.6 | 0.40 | 452 | 211 | 406 |
| this PR | 4 | 3 | 33.9 (33.3–34.2) | 53.7 | 0.42 | 509 | 204 | 405 |
| this PR, slow start after idle off | 1 | 2 | 35.7 (35.3–36.0) | 56.6 | 0.38 | 398 | 204 | 406 |
| this PR, slow start after idle off | 4 | 2 | 36.2 (34.9–37.6) | 57.6 | 0.38 | 372 | 204 | 407 |
| v0.10.3, max_bytes 64 MiB | 1 | 1 | 18.8 | 23.6 | 0.13 | 950 | 203 | 404 |
| v0.10.3, max_bytes 64 MiB | 4 | 1 | 22.8 | 24.5 | 0.16 | 1215 | 204 | 405 |
| this PR, max_bytes 64 MiB | 1 | 2 | 22.8 (20.0–25.6) | 29.1 | 0.19 | 1050 | 204 | 406 |
| this PR, max_bytes 64 MiB | 4 | 2 | 26.8 (23.6–30.0) | 25.3 | 0.20 | 1757 | 204 | 404 |
| v0.10.3, BLAKE3 read-back | 4 | 1 | 4.9 | 5.1 | 0.04 | 1294 | 204 | 1782 |
| this PR, BLAKE3 read-back | 4 | 1 | 27.7 | 40.5 | 0.25 | 623 | 203 | 1281 |

"End to end" runs from the first productive read to the last committed batch,
including the final partial batches sealed by `max_batch_age_seconds` about
10 s after the backlog ran out. "To 90%" leaves that tail out. Stage timings
are means of the `batch_metrics` lines. For the pipelined build, `verify_ms`
counts the object check's whole duration, which now overlaps
`manifest_upload_ms`.

## What may be said

Comparisons between these rows, on this rig. Not what any real link does:
the link is emulated on one shared host and was not calibrated against a real
one.

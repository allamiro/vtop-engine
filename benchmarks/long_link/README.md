# Long-link rig

Measures how fast the engine drains a Kafka backlog to an object store that is
far away, on one Docker host. It was built to find out why an external
benchmark over a 196 ms link measured the engine at a fraction of a sink that
keeps several uploads in flight, while it used a tenth of a core.

## What it is

- **Kafka** (3.9.1, one KRaft node) beside the engine, as a sink runs beside
  its broker. Two topics hold the same backlog: `p1` (one partition) and `p4`
  (four).
- **MinIO** (the lab image from `docker/minio`; build it with
  `docker compose build minio`) behind a round trip of `DELAY_MS` (default
  200 ms) and no bandwidth cap, so per-stream TCP, not the pipe, is the limit.
  The delay is a `netem` qdisc in MinIO's network namespace on packets to the
  engine only. On MinIO's side it delays the answers and ACKs and leaves the
  engine's send queue alone, so the engine's own
  `net.ipv4.tcp_slow_start_after_idle` is the one that matters. Kafka traffic
  is not delayed.
- **The backlog**: `DATA_BYTES` (default 1 GiB) of synthetic JSONL web-service
  logs (`gen.py`). zstd level 3 compresses a 16 MiB batch of it about 10.7
  times, to a 1.5 MiB object: one PUT each, as in the benchmark this was built
  to reproduce.
- **The engine** runs in a container (`--cpus=2`) on its own address, built
  from the tree under test. Each run drains one topic under a fresh consumer
  group and bucket, with the configuration in `config.yaml.in`: `max_bytes`
  16 MiB (or as given), `max_records` 1M, `max_batch_age_seconds` 10, eight
  batches wide, zstd level 3, SHA-256 (store-computed) and strong
  verification.

## Run it

```bash
docker compose build minio                    # the lab's MinIO image, once
benchmarks/long_link/rig.sh up                # network, broker, store, delay, data
# a Linux vtopctl for this host's architecture, built in a container:
#   cargo build --release --locked --bin vtopctl -p vtop-cli
benchmarks/long_link/run.sh base-p4-1 /path/to/vtopctl p4 16777216 1
benchmarks/long_link/run.sh ssai0-p4-1 /path/to/vtopctl p4 16777216 0   # slow start after idle off
python3 benchmarks/long_link/analyze.py benchmarks/long_link/work base-p4-1 ssai0-p4-1
benchmarks/long_link/crash.sh crash-p4 /path/to/vtopctl p4 8          # kills, checks, audit, scrub
benchmarks/long_link/rig.sh down
```

Run names become bucket names: lower case, digits and dashes. `LONGLINK_WORK`
moves the work directory (runs, logs, ledgers) out of the tree. `SUBNET`
chooses the Docker network's address range when Docker's own pools are used
up.

## What it reports

`analyze.py` gives, per run:

- throughput over the whole drain, from the first productive read to the last
  committed batch. This includes the final partial batches, sealed by
  `max_batch_age_seconds`, about 10 s after the backlog ran out;
- throughput until 90% of the bytes were committed, which leaves that tail out;
- the engine's CPU over the run's wall clock;
- the mean of every stage timing in the `batch_metrics` lines.

`crash.sh` kills the engine with SIGKILL at random points of a drain. At each
kill it checks the commit rule: the group's committed offset never passes the
ranges the ledger holds as VERIFIED or SOURCE_COMMITTED. After the restarts it
checks that the drain completes, that `audit-coverage --check-storage` finds
no hole, and that `scrub` reads every committed object back clean.

## Limits

An emulated link on one shared host: the broker, the store, the delay and the
engine share a machine and a clock, and nothing here was calibrated against a
real long link. Use it to compare configurations and builds on the same rig,
not as what a given real link will do. The measurements made with it are in
`benchmarks/measurements/long-link-pipelining/`.

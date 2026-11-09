# VTOP vs Apache Kafka, head to head (phase 1)

A same-client comparison of VTOP, through its Kafka wire gateway, against Apache Kafka: produce throughput
at saturation, produce latency at fixed offered rates, and consume throughput. Tracking issue
[#595](https://github.com/allamiro/vtop-engine/issues/595) (its ten fairness rules are binding here),
measurements [#596](https://github.com/allamiro/vtop-engine/issues/596), charts and report
[#600](https://github.com/allamiro/vtop-engine/issues/600). The published result is
[`docs/benchmarks/kafka-comparison.md`](../../docs/benchmarks/kafka-comparison.md).

## Run it

Needs Docker (with Compose v2) and Python 3.9+. `report.py` also needs matplotlib.

```bash
# The whole matrix, both systems, one at a time (about an hour on a laptop).
# Builds vtop-bench:<HEAD sha> from docker/Dockerfile if it is not there yet.
python3 benchmarks/kafka_compare/run.py

# Charts (PNG + SVG in docs/benchmarks/img/) and docs/benchmarks/kafka-comparison.md.
python3 -m pip install 'matplotlib==3.10.*'
python3 benchmarks/kafka_compare/report.py benchmarks/kafka_compare/results/<run-id>
```

Useful knobs: `--sizes 100,1024`, `--reps 3`, `--sat-records 100:3000000,1024:1000000` (records per
saturation run, per record size), `--fixed-seconds 30` (length of a fixed-rate run), `--smoke-records`,
`--no-consume`, `--rebuild`. A run writes `results/<run-id>/` and never overwrites another.

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
   `--throughput` (open loop, rule 6), about `--fixed-seconds` long each, `--reps` times, interleaved. A run
   whose achieved rate is more than 5% under its offer is recorded with `sustained=false`.
6. **Consume.** `kafka-consumer-perf-test` reads `--sat-records` records from the start of the log with a
   fresh group, `--reps` times; the recorded rates are its `fetch.*` columns, which exclude the group join.
7. **Down.** The leader's log is saved under `logs/`, the cluster is removed with its volumes.

## Output

`results/<run-id>/results.csv`: one row per (system, mode, record size, repetition, offered load), with
the run-constant columns (`run_id`, `env_fingerprint`, `vtop_commit`, `kafka_image`, `client`, `profile`).
`status` is `ok`, `failed` (the run ran and did not produce a usable result) or `refused` (it could not run,
e.g. the smoke failed); a row that is not `ok` says why in `note`. Latencies are the perf tool's own
percentiles in whole milliseconds; MiB/s is the tool's "MB/sec" (2^20 bytes).

`results/<run-id>/environment.json`: host CPU/model/RAM and OS, Docker Desktop version, the VM's CPUs,
memory, kernel and storage driver, the volume filesystem, per-node and client limits, both image references
with digests, the VTOP commit and engine tree hash, the client and its JVM, the producer and consumer
properties passed, the client's resolved `ProducerConfig` per system, Kafka's broker configuration as Kafka
reports it (`kafka-configs --describe --all`) and topic layout, the derived offered rates, every config file
verbatim, and the other containers running on the Docker host at start. The `comparable` block is hashed into
`env_fingerprint`; `run_state` is `complete` only when the run finished.

`logs/`: every perf tool's output and each cluster's node-1 log.

## Report

`report.py` refuses (exit 2, the reason on stderr, nothing written) when the CSV or `environment.json` is
missing, the run did not finish, a row is malformed or a `failed`/`refused` row gives no reason, any expected
row is missing or duplicated, rows come from more than one environment, commit, image, client or profile, or
the CSV's fingerprint does not match `environment.json` (rule 9). A cell that did not run is not a refusal:
it is a row, and the chart shows "no result" in its place.

Charts use one colour per system everywhere (Kafka blue `#2a78d6`, VTOP orange `#eb6834`; validated for
colour-vision-deficiency separation, never red vs green), marker shape as a second channel, and a footer with
the environment, profile, repetitions and run date. Output is deterministic for a given CSV and matplotlib
version (fixed rcParams, fixed SVG hash salt, no timestamps in metadata).

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
| 9 no hand-entered numbers | `report.py` generates every chart and table from the CSV and refuses partial or mixed data |
| 10 gateway gaps listed | in the report and below |

## Gateway gaps met

| gap | exact error | what was done |
|---|---|---|
| CreateTopics | `UnsupportedVersionException: The node does not support CREATE_TOPICS` (`kafka-topics.sh --create`) | VTOP's topic is the range in the node config |
| DescribeConfigs | `UnsupportedVersionException: The node does not support DESCRIBE_CONFIGS` (`kafka-topics.sh --describe`) | not needed by the perf tools |
| OffsetCommit without a metadata plane | client: `Offset commit failed on partition bench-0 at offset 150447: The message format version on the broker does not support the request`; gateway: `kafka OffsetCommit refused: this gateway has no durable offset store` | `enable.auto.commit=false` for both systems |
| compressed batches | `UnsupportedCompressionTypeException: batch 0: record batch is compressed (codec 4), which this gateway does not decompress` (zstd) | `compression.type=none` for both systems |
| gateway on followers | the node refuses `kafka` on any role but leader or standalone | one gateway, on the range's leader |

Everything the producer and consumer perf tools use was served: the 4.3 JVM client found a common version
for ApiVersions, Metadata, Produce (idempotent, the client's default, via InitProducerId), Fetch, ListOffsets
and the classic group protocol, all within the gateway's non-flexible ranges. Advertised listeners worked
with `kafka.advertised_host`.

## Known gaps

- Matrix: 100 B and 1 KiB, 1 partition, 1 producer, no compression, `defaults` only (no `fsync-parity`,
  `matched-timeouts`, 10 KiB, 6 partitions, 4 producers).
- VTOP runs without its metadata plane (static leadership): no lease, no failover, no durable offsets.
- No per-node CPU/RSS or bytes-written sampling, no end-to-end latency.
- Laptop numbers: Docker Desktop on one host, sharing the VM with whatever else runs there.
- CI does not yet regenerate the images and check for drift.

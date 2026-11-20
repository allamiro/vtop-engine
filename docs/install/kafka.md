# Supported Kafka brokers

The Kafka source (`sources.kafka`) reads through librdkafka, which the
binaries compile in. This page says which brokers that path is tested
against, what "tested" covers, and what is not tested.

## Tested releases

"Supported" means the release below is run every night by the
[kafka matrix](https://github.com/allamiro/vtop-engine/blob/main/.github/workflows/kafka-matrix.yml)
workflow: a single-node KRaft broker from the official `apache/kafka` image,
pinned by digest, and the whole live-broker test suite
(`tests/integration_kafka_to_minio.rs`, every test marked `#[ignore]`)
against it; the leg fails unless all of them pass. That suite includes the
read and commit tests, the stale-commit tests of #614 and the
stall-recovery tests of #623. A leg first checks that the broker reports the
release it is named for.

| Kafka release line | Patch tested | When it runs |
|---|---|---|
| 3.7 | 3.7.2 | nightly, and on every pull request that changes the Kafka path |
| 3.8 | 3.8.1 | nightly |
| 3.9 | 3.9.2 | nightly |
| 4.0 | 4.0.2 | nightly |
| 4.1 | 4.1.2 | nightly |
| 4.2 | 4.2.2 | nightly |
| 4.3 | 4.3.2 | nightly; the per-pull-request `kafka integration (live broker)` job runs 4.3.1 |
| 4.4 | 4.4.0 | nightly |

Other patch releases of these lines are expected to behave the same and are
not run. A pull request that changes the workflow or `scripts/kafka-broker.sh`
runs every release in the table before it merges.

## Not tested

- **ZooKeeper-mode brokers and releases before 3.7.** Expected to work, since
  the engine uses only the consumer, offset-commit and topic-metadata APIs,
  whose versions librdkafka negotiates with each broker, but untested. 3.7 is
  the first release with an official `apache/kafka` image.
- **Brokers other than Apache Kafka.** Not tested.
- **Multi-broker clusters.** Every leg is one broker that is also its own
  controller, so a partition leader moving to another broker is not
  exercised.
- **Authentication and encryption.** The legs use a plaintext listener.
- **Mutual TLS.** Not configurable: `sources.kafka` has `ssl_ca_location` to
  verify the broker, and no client certificate or key setting.
- **Kerberos (SASL GSSAPI).** Needs a build with `--features kafka-gssapi`
  ([from-source.md](from-source.md)) and is not tested.

## Run the suite against one release

The CI legs and a local run start the broker with the same script:

```bash
scripts/kafka-broker.sh up apache/kafka:3.7.2
scripts/kafka-broker.sh seed
VTOP_TEST_KAFKA=localhost:9092 \
  cargo test -p vtop-cli --test integration_kafka_to_minio -- --ignored
scripts/kafka-broker.sh down
```

The script publishes port 9092 and advertises `localhost`. To run the tests
from another container, set `KAFKA_ADVERTISED_HOST` and
`KAFKA_DOCKER_NETWORK`, or start the test container with
`--network container:vtop-kafka-broker` and keep the defaults; the script's
header lists every setting.

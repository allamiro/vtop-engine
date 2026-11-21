# Supported Kafka brokers

The Kafka source (`sources.kafka`) reads through librdkafka, which the
binaries compile in. This page says which brokers that path is tested
against, what "tested" covers, and what is not tested.

## Supported range

- **Apache Kafka 3.7 to 4.4**, KRaft, every release line in the table below.
  The oldest, 3.7, is the first release the official `apache/kafka` image
  exists for.
- **Listeners:** PLAINTEXT, TLS (`security_protocol: SSL`) and SASL over
  either, with the mechanisms PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512.
- **One broker that is not Apache Kafka:** Redpanda, smoke-tested (see
  below).

Anything outside this is either listed under "Not tested" or not claimed.

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

## Authentication and encryption

The same suite runs through each of these listeners, on the release named in
the second column, and each leg also runs the refusal tests of
`tests/integration_kafka_auth.rs`. A leg is one broker with one listener; the
broker is started by `scripts/kafka-broker.sh`, which also creates the user
(`vtop`) and, for TLS, a CA and a server certificate for `localhost`.

| Listener (`security_protocol`) | `sasl_mechanism` | Releases | Refusal tests |
|---|---|---|---|
| `PLAINTEXT` | none | every release | none: there is nothing to refuse |
| `SSL` | none | every release | no CA, another CA, a plaintext client |
| `SASL_SSL` | `SCRAM-SHA-512` | every release | the SASL ones of the rows below, and the TLS ones |
| `SASL_PLAINTEXT` | `PLAIN` | 3.7.2 and 4.4.0 | wrong password, unknown user, no credentials, a client with no SASL |
| `SASL_PLAINTEXT` | `SCRAM-SHA-256` | 3.7.2 and 4.4.0 | the same |
| `SASL_PLAINTEXT` | `SCRAM-SHA-512` | 3.7.2 and 4.4.0 | the same |

The refusal tests first read the topic with the right credentials, so a
refusal is the credential's doing and not a broker that is down, and then
present the wrong one: the source must fail within a bounded time, with an
error that does not contain the password, and read no record. They run nightly
and on dispatch, never on an ordinary pull request.

The legs' client settings are exactly the `sources.kafka` settings
(`security_protocol`, `sasl_mechanism`, `sasl_username`, `sasl_password_env`,
`ssl_ca_location`), set through `VTOP_TEST_KAFKA_*` environment variables read
by the test config (`tests/kafka_broker_env.rs`).

## Mutual TLS is not supported

`sources.kafka` can verify the broker (`ssl_ca_location`) and can log in with
SASL, but it has no setting for a client certificate or a client key, so the
engine cannot present one, so a listener that requires client certificates
(`ssl.client.auth=required`) cannot be used with it. This is a documented
limitation and is not tested: the legs use `ssl.client.auth=none`. Use a
listener that does not require them, with SASL for the client's identity.

## A three-broker cluster

The [kafka cluster](https://github.com/allamiro/vtop-engine/blob/main/.github/workflows/kafka-cluster.yml)
workflow starts three Apache Kafka 4.4.0 nodes in KRaft, each broker and
controller (`scripts/kafka-cluster.sh`). The restart tests' topics are at
replication 3 with `min.insync.replicas` 2; the live suite's own topics keep
the replication 1 it creates them with. It runs the whole live suite with the three
nodes as `bootstrap_servers`, and then `tests/integration_kafka_cluster.rs`:

- the leader of the partition the engine is reading is stopped between two
  batches, records keep arriving at the other two nodes, and the node is
  started again and catches up while the engine reads on;
- a follower is stopped the same way.

Each requires every record committed exactly once, in order, without a
restart of the engine, and the consumer group's offset at the end of the
partition. It runs nightly and on dispatch, and on a pull request that
changes the workflow, the script or the test.

## A broker that is not Apache Kafka

**Redpanda v26.3.1** (`redpandadata/redpanda`, a single node in
`dev-container` mode, pinned by digest) runs a smoke of the live suite on its
plaintext listener, nightly and on dispatch: nine of the suite's tests, covering a read, a commit that moves progress and nothing else does, the stale-commit tests of #614 and the stall-recovery test of #623. The leg fails unless every one passes, and a leg first checks that the broker reports `v26.3.1`. The whole suite also passed on this release when it was run by hand; it is not in the nightly. Redpanda with
authentication or TLS is not tested. Other Kafka-compatible brokers are not
tested and nothing here is claimed for them.

## Not tested

- **ZooKeeper-mode brokers and releases before 3.7.** Expected to work, since
  the engine uses only the consumer, offset-commit and topic-metadata APIs,
  whose versions librdkafka negotiates with each broker, but untested. 3.7 is
  the first release with an official `apache/kafka` image.
- **Brokers other than Apache Kafka and the Redpanda smoke above.**
- **Clusters larger than three brokers, and other failures.** The cluster
  job below stops one node at a time, in an orderly way; a crash, a network
  partition and a rolling upgrade are not exercised.
- **Mutual TLS.** See above.
- **SASL OAUTHBEARER and delegation tokens.** Not tested.
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

The script's other listeners, and the refusal tests, are run the same way:

```bash
scripts/kafka-broker.sh up apache/kafka:3.7.2 sasl-ssl     # or tls, sasl-plaintext
scripts/kafka-broker.sh seed
# client-env prints the VTOP_TEST_KAFKA_* lines for the running broker; its
# argument is the SASL mechanism (PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512).
set -a; eval "$(scripts/kafka-broker.sh client-env SCRAM-SHA-512)"; set +a
cargo test -p vtop-cli --test integration_kafka_to_minio -- --ignored
# sasl_ tests need a SASL listener, tls_ tests a TLS one (sasl-ssl has both)
cargo test -p vtop-cli --test integration_kafka_auth -- --ignored sasl_
cargo test -p vtop-cli --test integration_kafka_auth -- --ignored tls_
scripts/kafka-broker.sh down
```

A Redpanda image takes the plain mode only:
`scripts/kafka-broker.sh up redpandadata/redpanda:v26.3.1`.

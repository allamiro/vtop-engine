//! Integration test: the Kafka source against a three-broker cluster whose
//! partition leader is stopped and started while the engine reads (#721, B2).
//!
//! System model: three Apache Kafka nodes in KRaft (each broker and
//! controller), a topic with replication 3 and `min.insync.replicas` 2,
//! records produced with `acks=all`; an honest network; a fault that is one
//! node stopping (`docker stop`, an orderly shutdown) and starting again. The
//! engine runs with the mock object store and a SQLite ledger.
//!
//! The invariants, checked when the engine has drained the topic: the
//! committed ranges in the ledger tile `[0, N)` with no gap (nothing lost) and
//! no overlap (nothing committed twice), and the consumer group's committed
//! offset is `N`, the next offset to read. A leader change makes the engine's
//! reads and its offset commits fail for a while; the test allows passes to
//! fail during that window and requires the engine to come through it on its
//! own, without a restart.
//!
//! The cluster is started by `scripts/kafka-cluster.sh`, which prints the two
//! variables this file reads:
//!
//! - `VTOP_TEST_KAFKA`: the three nodes' client addresses
//! - `VTOP_TEST_KAFKA_CLUSTER_NODES`: the nodes' container names, in node-id
//!   order, so the test can stop the one that leads the partition
//!
//! ```bash
//! scripts/kafka-cluster.sh up apache/kafka:4.3.2
//! scripts/kafka-cluster.sh seed
//! set -a; eval "$(scripts/kafka-cluster.sh client-env)"; set +a
//! cargo test -p vtop-cli --test integration_kafka_cluster -- --ignored
//! ```
//!
//! `integration_kafka_to_minio.rs` runs against the same cluster in the same
//! job, so every live test also runs on three brokers.

use std::time::{Duration, Instant};

use vtop_adapters::kafka_source::build_client_config;
use vtop_core::config::KafkaSourceConfig;
use vtop_core::types::ProgressMarker;

#[path = "kafka_broker_env.rs"]
mod broker_env;
use broker_env::bootstrap;

/// Records per batch, so the topic is archived in many batches and the
/// leader can be stopped between them.
const BATCH_RECORDS: usize = 50;

/// How long passes may fail around a leader change before the test fails:
/// the cluster's own failover (a controller noticing the stopped node and
/// electing a new leader) plus the client's metadata refresh.
const FAILOVER_BUDGET: Duration = Duration::from_secs(120);

fn kafka_cfg(group: &str) -> KafkaSourceConfig {
    let mut cfg = KafkaSourceConfig {
        enabled: true,
        bootstrap_servers: vec![bootstrap()],
        consumer_group: group.to_string(),
        topic_include_regex: ".*".into(),
        topic_exclude_regex: "^__.*".into(),
        auto_offset_reset: "earliest".into(),
        enable_auto_commit: false,
        security_protocol: None,
        sasl_mechanism: None,
        sasl_username: None,
        sasl_password_env: None,
        ssl_ca_location: None,
    };
    broker_env::apply(&mut cfg);
    cfg
}

/// The nodes' container names, in node-id order (node 1 first).
fn nodes() -> Vec<String> {
    let names = std::env::var("VTOP_TEST_KAFKA_CLUSTER_NODES").unwrap_or_else(|_| {
        panic!(
            "VTOP_TEST_KAFKA_CLUSTER_NODES is not set: this test needs a cluster started by \
             scripts/kafka-cluster.sh and the variables `scripts/kafka-cluster.sh client-env` \
             prints"
        )
    });
    let nodes: Vec<String> = names.split(',').map(str::to_owned).collect();
    assert_eq!(nodes.len(), 3, "three nodes, got {nodes:?}");
    nodes
}

fn unique(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("it_cluster_{tag}_{nanos}")
}

/// Create `topic`: one partition, replication 3, `min.insync.replicas` 2.
async fn create_replicated_topic(topic: &str) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    let admin: AdminClient<_> = build_client_config(&kafka_cfg("vtop-it-admin"))
        .unwrap()
        .create()
        .expect("admin client");
    admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(3)).set("min.insync.replicas", "2")],
            &AdminOptions::new(),
        )
        .await
        .expect("create_topics call failed")
        .pop()
        .expect("one topic result")
        .expect("topic creation failed");
}

/// Append records `from..to` to `topic` with `acks=all`, and wait until the
/// cluster has acknowledged every one.
fn produce(topic: &str, from: usize, to: usize) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let mut config = build_client_config(&kafka_cfg("vtop-it-producer")).unwrap();
    config.set("acks", "all");
    config.set("enable.idempotence", "true");
    let producer: BaseProducer = config.create().expect("producer");
    for i in from..to {
        let payload = format!("CEF:0|VTOP|IT|1.0|{i}|Cluster|3|src=10.0.0.{}", i % 250);
        while let Err((e, _)) = producer.send(BaseRecord::<(), str>::to(topic).payload(&payload)) {
            assert!(
                matches!(
                    e,
                    rdkafka::error::KafkaError::MessageProduction(
                        rdkafka::types::RDKafkaErrorCode::QueueFull
                    )
                ),
                "enqueue produce: {e}"
            );
            producer.poll(Duration::from_millis(10));
        }
    }
    producer
        .flush(Duration::from_secs(60))
        .expect("the cluster acknowledged every record");
}

/// The node id that leads partition 0 of `topic`.
fn leader_of(topic: &str) -> i32 {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    let probe: BaseConsumer = build_client_config(&kafka_cfg("vtop-it-metadata"))
        .unwrap()
        .create()
        .unwrap();
    let deadline = Instant::now() + FAILOVER_BUDGET;
    loop {
        if let Ok(metadata) = probe.fetch_metadata(Some(topic), Duration::from_secs(10)) {
            if let Some(leader) = metadata
                .topics()
                .first()
                .and_then(|t| t.partitions().first())
                .map(|p| p.leader())
                .filter(|leader| *leader >= 0)
            {
                return leader;
            }
        }
        assert!(
            Instant::now() < deadline,
            "partition 0 of {topic} has no leader"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Partition 0 of `topic` has all three replicas in sync again.
fn wait_in_sync(topic: &str) {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    let probe: BaseConsumer = build_client_config(&kafka_cfg("vtop-it-metadata"))
        .unwrap()
        .create()
        .unwrap();
    let deadline = Instant::now() + FAILOVER_BUDGET;
    loop {
        let isr = probe
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .ok()
            .and_then(|m| {
                m.topics()
                    .first()
                    .and_then(|t| t.partitions().first())
                    .map(|p| p.isr().len())
            });
        if isr == Some(3) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "partition 0 of {topic} did not get three replicas back in sync (in sync: {isr:?})"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn docker(args: &[&str]) {
    let status = std::process::Command::new("docker")
        .args(args)
        .status()
        .expect("run docker");
    assert!(status.success(), "docker {args:?} failed: {status}");
}

/// A stopped node, started again when the guard is dropped, so a failing
/// test does not leave the cluster at two nodes for the tests after it.
struct Stopped(String);

impl Stopped {
    fn node(name: &str) -> Self {
        docker(&["stop", "--time", "10", name]);
        Self(name.to_string())
    }

    fn start(self) {
        docker(&["start", &self.0]);
        std::mem::forget(self);
    }
}

impl Drop for Stopped {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["start", &self.0])
            .status();
    }
}

/// An engine on `db` that reads only `topic`, as `group`, into the mock
/// store, in batches of [`BATCH_RECORDS`].
async fn engine_on(db: &str, work: &std::path::Path, topic: &str, group: &str) -> vtop_cli::Engine {
    let mut cfg = vtop_cli::testkit::file_config(work.to_str().unwrap(), db, vec![], "mock");
    cfg.sources.file = None;
    let mut kafka = kafka_cfg(group);
    kafka.topic_include_regex = format!("^{topic}$");
    cfg.sources.kafka = Some(kafka);
    cfg.batching.source_poll_wait_ms = 1_000;
    cfg.batching.max_records = BATCH_RECORDS;
    vtop_cli::Engine::new(cfg, vtop_core::config::StreamsConfig { streams: vec![] })
        .await
        .expect("the engine starts")
}

/// The committed ranges in the ledger, `(start, end)` inclusive, lowest first.
async fn committed(engine: &vtop_cli::Engine) -> Vec<(i64, i64)> {
    use vtop_core::state_machine::BatchState;
    let mut out: Vec<_> = engine
        .store
        .list_batches()
        .await
        .unwrap()
        .into_iter()
        .filter(|b| b.state == BatchState::SourceCommitted)
        .map(|b| match (&b.progress_start, &b.progress_end) {
            (
                ProgressMarker::Kafka { start_offset, .. },
                ProgressMarker::Kafka { end_offset, .. },
            ) => (*start_offset, *end_offset),
            other => panic!("Kafka markers: {other:?}"),
        })
        .collect();
    out.sort_unstable();
    out
}

/// Run passes until the committed ranges reach `last` (inclusive). A pass
/// may fail (a leader that is gone, a coordinator that moved) for up to
/// [`FAILOVER_BUDGET`]; the engine must come through on its own.
async fn commit_through(engine: &mut vtop_cli::Engine, last: i64) -> Vec<(i64, i64)> {
    let deadline = Instant::now() + FAILOVER_BUDGET;
    let mut failures = 0;
    loop {
        if let Err(e) = engine
            .process_once(vtop_core::types::SourceType::Kafka)
            .await
        {
            failures += 1;
            eprintln!("a pass failed ({failures} so far): {e}");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let ranges = committed(engine).await;
        if ranges.last().is_some_and(|(_, end)| *end >= last) {
            return ranges;
        }
        assert!(
            Instant::now() < deadline,
            "the engine did not commit through offset {last} within {FAILOVER_BUDGET:?} \
             ({failures} failed passes); committed: {ranges:?}"
        );
    }
}

/// The group's committed offset for partition 0 of `topic`.
fn group_offset(topic: &str, group: &str) -> Option<i64> {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::topic_partition_list::TopicPartitionList;
    let probe: BaseConsumer = build_client_config(&kafka_cfg(group))
        .unwrap()
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, 0);
    let deadline = Instant::now() + FAILOVER_BUDGET;
    loop {
        match probe.committed_offsets(tpl.clone(), Duration::from_secs(10)) {
            Ok(c) => {
                return match c.find_partition(topic, 0).map(|e| e.offset()) {
                    Some(rdkafka::Offset::Offset(at)) => Some(at),
                    _ => None,
                }
            }
            Err(e) => {
                assert!(Instant::now() < deadline, "read the group's offset: {e}");
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

/// `ranges` tile `[0, records)`: no gap, no overlap.
fn assert_tiles(ranges: &[(i64, i64)], records: i64) {
    let mut next = 0;
    for (start, end) in ranges {
        assert_eq!(
            *start, next,
            "the committed ranges do not tile the partition at offset {next}: {ranges:?}"
        );
        assert!(end >= start, "an empty range: {ranges:?}");
        next = end + 1;
    }
    assert_eq!(
        next,
        records,
        "committed through {}, of {records}: {ranges:?}",
        next - 1
    );
}

/// The leader of the partition the engine is reading is stopped between two
/// batches, records keep arriving at the two nodes left, and the node is
/// started again and catches up while the engine reads on. Every record is
/// committed exactly once, in order, and the group's offset ends at the end
/// of the partition, through a leader change and a new leader for the group
/// coordinator when the stopped node held it.
#[tokio::test]
#[ignore = "requires a three-broker Kafka cluster (scripts/kafka-cluster.sh)"]
async fn a_leader_stopped_and_started_mid_read_loses_nothing_and_commits_nothing_twice() {
    let nodes = nodes();
    let topic = unique("leader");
    let group = format!("vtop-it-{topic}");
    create_replicated_topic(&topic).await;
    produce(&topic, 0, 200);

    let dir = tempfile::tempdir().unwrap();
    let db = format!("sqlite://{}", dir.path().join("state.db").display());
    let mut engine = engine_on(&db, &dir.path().join("work"), &topic, &group).await;

    // Part of the partition is archived and committed.
    commit_through(&mut engine, (BATCH_RECORDS - 1) as i64).await;

    // Its leader stops; the two nodes left take the next records.
    let leader = leader_of(&topic);
    let stopped = Stopped::node(&nodes[(leader - 1) as usize]);
    eprintln!(
        "stopped node {leader} ({}), the leader of {topic}",
        stopped.0
    );
    produce(&topic, 200, 400);
    assert_ne!(leader_of(&topic), leader, "the stopped node still leads");
    commit_through(&mut engine, 399).await;

    // It starts again, catches up, and more records arrive.
    stopped.start();
    produce(&topic, 400, 500);
    wait_in_sync(&topic);
    let ranges = commit_through(&mut engine, 499).await;

    assert_tiles(&ranges, 500);
    assert_eq!(
        group_offset(&topic, &group),
        Some(500),
        "the group's offset is the next offset to read"
    );
}

/// A follower that stops while the engine reads changes nothing the engine
/// can see: it reads on from the same leader, and every record is committed
/// once. With the follower gone, `acks=all` still succeeds on two replicas.
#[tokio::test]
#[ignore = "requires a three-broker Kafka cluster (scripts/kafka-cluster.sh)"]
async fn a_follower_stopped_mid_read_changes_nothing() {
    let nodes = nodes();
    let topic = unique("follower");
    let group = format!("vtop-it-{topic}");
    create_replicated_topic(&topic).await;
    produce(&topic, 0, 100);

    let dir = tempfile::tempdir().unwrap();
    let db = format!("sqlite://{}", dir.path().join("state.db").display());
    let mut engine = engine_on(&db, &dir.path().join("work"), &topic, &group).await;
    commit_through(&mut engine, (BATCH_RECORDS - 1) as i64).await;

    let leader = leader_of(&topic);
    let follower = (1..=3).find(|n| *n != leader).unwrap();
    let stopped = Stopped::node(&nodes[(follower - 1) as usize]);
    produce(&topic, 100, 200);
    let ranges = commit_through(&mut engine, 199).await;
    stopped.start();
    wait_in_sync(&topic);

    assert_tiles(&ranges, 200);
    assert_eq!(group_offset(&topic, &group), Some(200));
}

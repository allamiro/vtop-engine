//! Integration test: Kafka source semantics against a live broker.
//!
//! Runs in CI against a real broker (see the `kafka integration` job), and
//! locally against the docker-compose lab:
//!
//! ```bash
//! docker compose up -d kafka
//! echo "127.0.0.1 kafka" | sudo tee -a /etc/hosts   # advertised listener is kafka:9092
//! VTOP_TEST_KAFKA=kafka:9092 \
//!   cargo test -p vtop-cli --test integration_kafka_to_minio -- --ignored
//! ```
//!
//! Why this file matters: the Kafka path had a P0 where every read returned zero
//! records (a `subscribe()` per read forced a rebalance that reseeked to earliest
//! and consumed the whole poll window), so no batch ever sealed and no offset
//! ever committed. CI stayed green throughout, because nothing here talked to a
//! broker — and the pre-existing test only asserted the *shape* of the progress
//! marker, which an empty read also satisfies. It would not have caught the bug
//! even if it had run.
//!
//! So these tests assert the two things that actually failed:
//!   1. a read of a non-empty topic returns records;
//!   2. progress advances ONLY on an explicit post-verification commit.

use vtop_adapters::kafka_source::build_client_config;
use vtop_core::config::KafkaSourceConfig;

fn kafka_cfg(bootstrap: &str, group: &str) -> KafkaSourceConfig {
    KafkaSourceConfig {
        enabled: true,
        bootstrap_servers: vec![bootstrap.to_string()],
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
    }
}

/// Always-on guard: the Kafka client config must never enable auto-commit,
/// independent of any broker. Runs even without Kafka.
#[test]
fn kafka_client_never_auto_commits() {
    let cc = build_client_config(&kafka_cfg("localhost:9092", "vtop-engine-it"))
        .expect("config without secrets builds");
    assert_eq!(cc.get("enable.auto.commit"), Some("false"));
}

// ---------------------------------------------------------------------------
// Live-broker tests. `#[ignore]` so a plain `cargo test` stays hermetic; CI runs
// them explicitly with `-- --ignored` against a seeded broker.
// ---------------------------------------------------------------------------

use vtop_adapters::base::{DiscoveredSource, SourceAdapter};
use vtop_adapters::KafkaSource;
use vtop_core::types::{ProgressMarker, TelemetryFormat};

fn bootstrap() -> String {
    std::env::var("VTOP_TEST_KAFKA").unwrap_or_else(|_| "kafka:9092".into())
}

/// A unique consumer group per test, so a re-run never inherits offsets from a
/// previous run — which would silently mask a broken read.
fn unique_group(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("vtop-it-{tag}-{nanos}")
}

/// The topic CI seeds with records.
const SEEDED_TOPIC: &str = "it_events";

async fn seeded_topic(adapter: &mut KafkaSource) -> DiscoveredSource {
    let sources = adapter
        .discover_sources()
        .await
        .expect("kafka discovery failed (is the broker up?)");
    sources
        .into_iter()
        .find(|s| s.source_name == SEEDED_TOPIC)
        .unwrap_or_else(|| panic!("expected the CI-seeded topic '{SEEDED_TOPIC}'"))
}

/// THE regression test for the P0: reading a topic that definitely has records
/// must return records. The old test asserted only the marker shape, which an
/// empty read also satisfies — so it would have stayed green through the stall.
#[tokio::test]
#[ignore = "requires a running Kafka broker"]
async fn read_returns_records_from_a_non_empty_topic() {
    let mut adapter = KafkaSource::new(
        kafka_cfg(&bootstrap(), &unique_group("read")),
        TelemetryFormat::Cef,
    )
    .unwrap();
    let src = seeded_topic(&mut adapter).await;

    // One read yields one ReadResult per partition it saw. CI seeds a
    // single-partition topic, but a local lab topic may have more, so assert on
    // the AGGREGATE: the stall this test guards against is "no records at all",
    // which is partition-count independent. Testing only reads[0] would let a
    // regression that starves every partition but the first slip through.
    let reads = adapter
        .read_batch_candidates(&src, 100, 1 << 20, std::time::Duration::from_secs(10))
        .await
        .expect("read failed");

    let total: usize = reads.iter().map(|r| r.records.len()).sum();
    assert!(
        total > 0,
        "read returned ZERO records from a seeded topic - this is exactly the stall \
         the assign()/subscribe() fix addressed"
    );

    // Every returned unit must carry a well-formed Kafka marker, not just the
    // first: each one is independently committed by the engine.
    for read in &reads {
        match &read.progress_end {
            ProgressMarker::Kafka {
                partition,
                start_offset,
                end_offset,
                ..
            } => {
                assert!(*partition >= 0);
                assert!(
                    end_offset >= start_offset,
                    "end_offset {end_offset} must not precede start_offset {start_offset}"
                );
            }
            other => panic!("expected a Kafka marker, got {other:?}"),
        }
    }
}

/// The core invariant on the Kafka path: a fresh consumer group must NOT advance
/// past data merely by reading it. Simulates a crash before VERIFIED.
#[tokio::test]
#[ignore = "requires a running Kafka broker"]
async fn reading_alone_does_not_advance_progress() {
    let group = unique_group("nocommit");
    let mut a = KafkaSource::new(kafka_cfg(&bootstrap(), &group), TelemetryFormat::Cef).unwrap();
    let src = seeded_topic(&mut a).await;

    let first = a
        .read_batch_candidates(&src, 100, 1 << 20, std::time::Duration::from_secs(10))
        .await
        .unwrap();
    // Aggregate over partitions: the invariant is about data being re-readable
    // anywhere in the topic, not about which partition it landed in.
    let first_total: usize = first.iter().map(|r| r.records.len()).sum();
    assert!(first_total > 0, "precondition: topic has records");
    // Deliberately do NOT commit - the batch never reached VERIFIED.
    drop(a);

    // A new consumer in the same group must re-read the same data, because
    // nothing was ever committed.
    let mut b = KafkaSource::new(kafka_cfg(&bootstrap(), &group), TelemetryFormat::Cef).unwrap();
    let replay = b
        .read_batch_candidates(&src, 100, 1 << 20, std::time::Duration::from_secs(10))
        .await
        .unwrap();
    let replay_total: usize = replay.iter().map(|r| r.records.len()).sum();
    assert!(
        replay_total > 0,
        "uncommitted data MUST remain replayable - progress advanced without a commit"
    );
}

/// #96 A1: ONE multiplexed pass over MANY topics must return every topic's
/// records, demuxed so each unit's marker names the topic it actually came
/// from. This is the behaviour the serial per-topic loop was replaced with;
/// mis-demuxing here means topic B's records get archived (and B's offsets
/// committed) under topic A.
#[tokio::test]
#[ignore = "requires a running Kafka broker"]
async fn one_pass_reads_many_topics_and_demuxes_by_marker_topic() {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

    let mut adapter = KafkaSource::new(
        kafka_cfg(&bootstrap(), &unique_group("multiplex")),
        TelemetryFormat::Cef,
    )
    .unwrap();
    let seeded = seeded_topic(&mut adapter).await;

    // A second topic, unique per run so stale offsets from a previous run can
    // never mask a broken demux. Created explicitly — auto-creation is broker
    // configuration this test must not depend on.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let topic_b = format!("it_multiplex_{nanos}");
    let admin: AdminClient<_> = build_client_config(&kafka_cfg(&bootstrap(), "vtop-it-admin"))
        .unwrap()
        .create()
        .expect("admin client");
    admin
        .create_topics(
            &[NewTopic::new(&topic_b, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create_topics call failed")
        .pop()
        .expect("one topic result")
        .expect("topic creation failed");

    let producer: BaseProducer = build_client_config(&kafka_cfg(&bootstrap(), "vtop-it-producer"))
        .unwrap()
        .create()
        .expect("producer");
    for i in 0..10 {
        let payload = format!("CEF:0|VTOP|IT|1.0|{i}|Multiplex|3|src=10.0.0.{i}");
        producer
            .send(BaseRecord::<(), str>::to(&topic_b).payload(&payload))
            .expect("enqueue produce");
    }
    producer
        .flush(std::time::Duration::from_secs(10))
        .expect("flush produce");

    let sources = vec![
        seeded.clone(),
        DiscoveredSource {
            source_type: seeded.source_type.clone(),
            source_name: topic_b.clone(),
            format: seeded.format.clone(),
        },
    ];
    let report = adapter
        .read_all_batch_candidates(&sources, 1000, 1 << 20, std::time::Duration::from_secs(10))
        .await
        .expect("multiplexed read failed");

    assert_eq!(
        report.outcomes.len(),
        sources.len(),
        "one outcome per source"
    );
    assert_eq!(report.failed_ms, 0, "no failure bucket on a healthy pass");
    let mut totals = vec![0usize; sources.len()];
    for outcome in &report.outcomes {
        let src = &sources[outcome.source_index];
        let reads = outcome.result.as_ref().expect("per-source result ok");
        for read in reads {
            let ProgressMarker::Kafka { topic, .. } = &read.progress_end else {
                panic!("expected a Kafka marker");
            };
            // THE demux invariant: every unit routed to a source carries a
            // marker for that source's topic, never another topic's.
            assert_eq!(topic, &src.source_name, "unit demuxed to the wrong source");
            totals[outcome.source_index] += read.records.len();
        }
    }
    // Both topics have data; ONE pass must have fetched from BOTH — a pass
    // that only serves one topic is the serial starvation this replaces.
    assert!(
        totals.iter().all(|&t| t > 0),
        "every seeded topic must yield records in a single pass, got {totals:?}"
    );

    // STICKY-ASSIGNMENT path: a SECOND pass on the same adapter (unchanged
    // topic set) must not re-assign — and must still deliver new data
    // produced after the first pass. This is the pass shape the engine runs
    // in steady state, and the shape that starved when every pass re-assigned
    // (fetcher restart ate the poll window).
    for i in 10..20 {
        let payload = format!("CEF:0|VTOP|IT|1.0|{i}|Multiplex2|3|src=10.0.1.{i}");
        producer
            .send(BaseRecord::<(), str>::to(&topic_b).payload(&payload))
            .expect("enqueue produce (second round)");
    }
    producer
        .flush(std::time::Duration::from_secs(10))
        .expect("flush second round");
    let report2 = adapter
        .read_all_batch_candidates(&sources, 1000, 1 << 20, std::time::Duration::from_secs(10))
        .await
        .expect("second multiplexed read failed");
    let b_idx = sources
        .iter()
        .position(|s| s.source_name == topic_b)
        .unwrap();
    let second_b: usize = report2.outcomes[b_idx]
        .result
        .as_ref()
        .expect("second-pass outcome ok")
        .iter()
        .map(|r| r.records.len())
        .sum();
    assert!(
        second_b >= 10,
        "second pass on an unchanged assignment must deliver newly produced \
         records (sticky assignment), got {second_b}"
    );

    // Best-effort cleanup; a leaked topic only clutters a long-lived local lab.
    let _ = admin
        .delete_topics(&[topic_b.as_str()], &AdminOptions::new())
        .await;
}

/// Review regression for #140: an oversized record is a failure of its own
/// topic, not of the shared Kafka pass, and it must be delivered again after
/// the operator raises the budget. This exercises both per-source isolation
/// and invalidation of a sticky Offset::Stored assignment.
#[tokio::test]
#[ignore = "requires a running Kafka broker"]
async fn oversized_topic_is_isolated_and_retried_after_budget_change() {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

    let bootstrap = bootstrap();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let oversized_topic = format!("it_oversized_{nanos}");
    let healthy_topic = format!("it_healthy_{nanos}");
    let admin: AdminClient<_> = build_client_config(&kafka_cfg(&bootstrap, "vtop-it-admin"))
        .unwrap()
        .create()
        .expect("admin client");
    admin
        .create_topics(
            &[
                NewTopic::new(&oversized_topic, 1, TopicReplication::Fixed(1)),
                NewTopic::new(&healthy_topic, 1, TopicReplication::Fixed(1)),
            ],
            &AdminOptions::new(),
        )
        .await
        .expect("create_topics call failed")
        .into_iter()
        .for_each(|result| {
            result.expect("topic creation failed");
        });

    let producer: BaseProducer = build_client_config(&kafka_cfg(&bootstrap, "vtop-it-producer"))
        .unwrap()
        .create()
        .expect("producer");
    producer
        .send(BaseRecord::<(), [u8]>::to(&oversized_topic).payload(&[b'x'; 32]))
        .expect("enqueue oversized record");
    producer
        .send(BaseRecord::<(), [u8]>::to(&healthy_topic).payload(b"ok"))
        .expect("enqueue healthy record");
    producer
        .flush(std::time::Duration::from_secs(10))
        .expect("flush produce");

    let sources = vec![
        DiscoveredSource {
            source_type: vtop_core::types::SourceType::Kafka,
            source_name: oversized_topic.clone(),
            format: TelemetryFormat::Cef,
        },
        DiscoveredSource {
            source_type: vtop_core::types::SourceType::Kafka,
            source_name: healthy_topic.clone(),
            format: TelemetryFormat::Cef,
        },
    ];
    let mut adapter = KafkaSource::new(
        kafka_cfg(&bootstrap, &unique_group("oversized-isolation")),
        TelemetryFormat::Cef,
    )
    .unwrap();

    let first = adapter
        .read_all_batch_candidates(&sources, 100, 16, std::time::Duration::from_secs(10))
        .await
        .expect("one bad topic must not fail the shared pass");
    let oversized_error = first.outcomes[0]
        .result
        .as_ref()
        .expect_err("oversized topic must report a source-local failure");
    assert!(oversized_error
        .to_string()
        .contains("exceeding max_bytes=16"));
    let healthy_records: usize = first.outcomes[1]
        .result
        .as_ref()
        .expect("healthy topic remains returnable")
        .iter()
        .map(|read| read.records.len())
        .sum();
    assert!(
        healthy_records > 0,
        "healthy topic was discarded with its peer"
    );

    let retry = adapter
        .read_all_batch_candidates(&sources, 100, 64, std::time::Duration::from_secs(10))
        .await
        .expect("retry pass");
    let retried_records: usize = retry.outcomes[0]
        .result
        .as_ref()
        .expect("raised budget accepts the previously rejected topic")
        .iter()
        .map(|read| read.records.len())
        .sum();
    assert_eq!(
        retried_records, 1,
        "oversized record was skipped instead of reassigned from its unadvanced cursor"
    );

    let _ = admin
        .delete_topics(
            &[oversized_topic.as_str(), healthy_topic.as_str()],
            &AdminOptions::new(),
        )
        .await;
}

/// After an explicit commit (what the engine does only once VERIFIED), a new
/// consumer in the same group must resume past the committed offsets rather than
/// re-reading them, or verified batches would be archived twice.
#[tokio::test]
#[ignore = "requires a running Kafka broker"]
async fn commit_advances_progress_for_a_new_consumer() {
    let group = unique_group("commit");
    let mut a = KafkaSource::new(kafka_cfg(&bootstrap(), &group), TelemetryFormat::Cef).unwrap();
    let src = seeded_topic(&mut a).await;

    let read = a
        .read_batch_candidates(&src, 1000, 1 << 20, std::time::Duration::from_secs(10))
        .await
        .unwrap();
    let read_total: usize = read.iter().map(|r| r.records.len()).sum();
    assert!(read_total > 0, "precondition: topic has records");

    // The step the engine performs ONLY after VERIFIED — once per independently
    // committable unit, since each partition carries its own marker. Committing
    // only the first would leave the rest replayable and the assertion below
    // would (correctly) fail.
    for r in &read {
        a.commit_progress(&r.progress_end)
            .await
            .expect("commit_progress failed");
    }
    drop(a);

    let mut b = KafkaSource::new(kafka_cfg(&bootstrap(), &group), TelemetryFormat::Cef).unwrap();
    let after = b
        .read_batch_candidates(&src, 1000, 1 << 20, std::time::Duration::from_secs(5))
        .await
        .unwrap();
    let after_total: usize = after.iter().map(|r| r.records.len()).sum();
    assert!(
        after_total == 0,
        "committed records were re-delivered ({after_total} records) - the commit did not \
         take effect, which would mean duplicate archiving"
    );
}

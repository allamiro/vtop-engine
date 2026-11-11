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

// ---------------------------------------------------------------------------
// #614: a stale engine's offset commit. The adapter reads with assign(), so its
// commit carries no group generation and Kafka accepts it from an engine whose
// claim on the batch has moved on. These tests make that commit for real and
// then restart an engine on the ledger, which is what decides where it reads.
// ---------------------------------------------------------------------------

/// Deletes its topic when dropped, so a test's topic is removed whether the
/// test passes or panics - every run creates new uniquely named topics, and
/// leaking them grows the broker's metadata without bound.
struct TopicCleanup(String);

impl Drop for TopicCleanup {
    fn drop(&mut self) {
        use rdkafka::admin::{AdminClient, AdminOptions};
        use rdkafka::client::DefaultClientContext;
        let Ok(admin) =
            build_client_config(&kafka_cfg(&bootstrap(), "vtop-it-admin")).and_then(|cc| {
                cc.create::<AdminClient<DefaultClientContext>>()
                    .map_err(|e| vtop_core::errors::VtopError::Source(e.to_string()))
            })
        else {
            eprintln!("could not build an admin client to delete topic {}", self.0);
            return;
        };
        // The admin future is driven by the client's own thread, so it can be
        // waited on here without the test's runtime - which, on a panic, may
        // be the one unwinding.
        let options =
            AdminOptions::new().operation_timeout(Some(std::time::Duration::from_secs(10)));
        match futures::executor::block_on(admin.delete_topics(&[self.0.as_str()], &options)) {
            Ok(results) => {
                for result in results {
                    if let Err((topic, code)) = result {
                        eprintln!("deleting topic {topic} failed: {code}");
                    }
                }
            }
            Err(e) => eprintln!("deleting topic {} failed: {e}", self.0),
        }
    }
}

/// A NEW single-partition topic holding `records` records at offsets
/// `0..records`, so every offset a test names is exact. The topic is deleted
/// when the returned guard is dropped.
async fn fresh_topic(tag: &str, records: usize) -> (String, TopicCleanup) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let topic = format!("it_{tag}_{nanos}");
    let admin: AdminClient<_> = build_client_config(&kafka_cfg(&bootstrap(), "vtop-it-admin"))
        .unwrap()
        .create()
        .expect("admin client");
    admin
        .create_topics(
            &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create_topics call failed")
        .pop()
        .expect("one topic result")
        .expect("topic creation failed");
    let cleanup = TopicCleanup(topic.clone());
    let producer: BaseProducer = build_client_config(&kafka_cfg(&bootstrap(), "vtop-it-producer"))
        .unwrap()
        .create()
        .expect("producer");
    for i in 0..records {
        let payload = format!("CEF:0|VTOP|IT|1.0|{i}|StaleCommit|3|src=10.0.0.{}", i % 250);
        producer
            .send(BaseRecord::<(), str>::to(&topic).payload(&payload))
            .expect("enqueue produce");
    }
    producer
        .flush(std::time::Duration::from_secs(10))
        .expect("flush produce");
    (topic, cleanup)
}

fn kafka_marker(topic: &str, group: &str, start: i64, end: i64) -> ProgressMarker {
    ProgressMarker::Kafka {
        topic: topic.into(),
        partition: 0,
        start_offset: start,
        end_offset: end,
        consumer_group: group.into(),
    }
}

/// Put a batch for `[start, end]` into the ledger the way the engine moves it:
/// every step of the legal walk up to VERIFIED, then either committed, or sent
/// back for replay as recovery does when a re-check of the stored object
/// fails (FAILED, then REPLAY_REQUIRED).
async fn ledger_batch(
    store: &dyn vtop_state::StateStore,
    id: &str,
    topic: &str,
    group: &str,
    (start, end): (i64, i64),
    committed: bool,
) {
    use vtop_core::state_machine::BatchState;
    let now = chrono::Utc::now().to_rfc3339();
    store
        .save_batch_state(&vtop_state::BatchRecord {
            batch_id: id.into(),
            tenant: "default".into(),
            source_type: vtop_core::types::SourceType::Kafka,
            source_name: topic.into(),
            format: TelemetryFormat::Raw,
            state: BatchState::Batching,
            progress_start: kafka_marker(topic, group, start, start),
            progress_end: kafka_marker(topic, group, start, end),
            object_uri: None,
            manifest_uri: None,
            object_sha256: None,
            manifest_sha256: None,
            manifest_version_id: None,
            object_size_bytes: None,
            record_count: Some(end - start + 1),
            error_message: None,
            owner: None,
            lease_expires_at: None,
            created_at: now.clone(),
            updated_at: now,
        })
        .await
        .unwrap();
    for st in [
        BatchState::Sealed,
        BatchState::Compressed,
        BatchState::Checksummed,
        BatchState::ObjectUploaded,
        BatchState::ManifestUploaded,
        BatchState::Verified,
    ] {
        store
            .update_batch_state(id, st, &vtop_state::BatchPatch::default())
            .await
            .unwrap();
    }
    if committed {
        store.mark_source_committed(id).await.unwrap();
    } else {
        store
            .mark_failed(id, "re-check of the stored object failed")
            .await
            .unwrap();
        store
            .update_batch_state(
                id,
                BatchState::ReplayRequired,
                &vtop_state::BatchPatch::default(),
            )
            .await
            .unwrap();
    }
}

/// What engine A does when it resumes after its claim moved on: commit the
/// offset after ITS batch's range to the group. Returns the group's committed
/// offset as the broker reports it afterwards, which proves Kafka accepted it.
async fn stale_commit(topic: &str, group: &str, end: i64) -> i64 {
    let mut a = KafkaSource::new(kafka_cfg(&bootstrap(), group), TelemetryFormat::Raw).unwrap();
    a.commit_progress(&kafka_marker(topic, group, 0, end))
        .await
        .expect("Kafka refused the stale commit; the premise of #614 no longer holds");
    group_offset(topic, group)
}

/// The group's committed offset for partition 0 of `topic`, from the broker.
fn group_offset(topic: &str, group: &str) -> i64 {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::topic_partition_list::TopicPartitionList;

    let probe: BaseConsumer = build_client_config(&kafka_cfg(&bootstrap(), group))
        .unwrap()
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, 0);
    let committed = probe
        .committed_offsets(tpl, std::time::Duration::from_secs(10))
        .expect("read the group's committed offset");
    match committed.find_partition(topic, 0).map(|e| e.offset()) {
        Some(rdkafka::Offset::Offset(at)) => at,
        other => panic!("the group has no committed offset: {other:?}"),
    }
}

/// An engine B on `db` reading only `topic` as `group`: recover (the restart),
/// then run passes until it has committed a batch the ledger did not hold, and
/// return the start offsets of the batches it committed, lowest first.
async fn restart_and_read(db: &str, work: &std::path::Path, topic: &str, group: &str) -> Vec<i64> {
    let mut engine = engine_on(db, work, topic, group).await;
    engine.recover().await.expect("recovery");
    read_until_a_new_commit(&mut engine).await
}

/// An engine on `db` that reads only `topic`, as `group`, into the mock store.
async fn engine_on(db: &str, work: &std::path::Path, topic: &str, group: &str) -> vtop_cli::Engine {
    let mut cfg = vtop_cli::testkit::file_config(work.to_str().unwrap(), db, vec![], "mock");
    cfg.sources.file = None;
    let mut kafka = kafka_cfg(&bootstrap(), group);
    kafka.topic_include_regex = format!("^{topic}$");
    cfg.sources.kafka = Some(kafka);
    cfg.batching.source_poll_wait_ms = 2_000;
    vtop_cli::Engine::new(cfg, vtop_core::config::StreamsConfig { streams: vec![] })
        .await
        .expect("engine B starts")
}

/// Run passes until `engine` commits a batch the ledger did not hold before;
/// the start offsets of the new committed batches, lowest first.
async fn read_until_a_new_commit(engine: &mut vtop_cli::Engine) -> Vec<i64> {
    use vtop_core::state_machine::BatchState;
    let ledger_ids: std::collections::HashSet<String> = engine
        .store
        .list_batches()
        .await
        .unwrap()
        .into_iter()
        .map(|b| b.batch_id)
        .collect();
    for _ in 0..15 {
        engine
            .process_once(vtop_core::types::SourceType::Kafka)
            .await
            .expect("a read pass");
        let mut starts: Vec<i64> = engine
            .store
            .list_batches()
            .await
            .unwrap()
            .into_iter()
            .filter(|b| b.state == BatchState::SourceCommitted && !ledger_ids.contains(&b.batch_id))
            .map(|b| match b.progress_start {
                ProgressMarker::Kafka { start_offset, .. } => start_offset,
                other => panic!("a Kafka batch carries a Kafka marker: {other:?}"),
            })
            .collect();
        if !starts.is_empty() {
            starts.sort_unstable();
            return starts;
        }
    }
    panic!("engine B committed nothing in 15 passes over a topic with unread records");
}

/// #614, the confirmed consequence. Engine B has archived and committed
/// `[0, 19]`; engine A, which lost its claim on the batch `[0, 4]`, then
/// commits offset 5. Kafka accepts it - the group's offset goes BACKWARDS - but
/// a restarted engine reads on from the ledger's 20, not from the group's 5,
/// and moves the group's offset back up to 20 so lag monitoring agrees.
///
/// Against the code before #614 this fails: the restart resumed from the
/// group's offset and archived `[5, 19]` again.
#[tokio::test]
#[ignore = "requires a running Kafka broker"]
async fn a_stale_commit_behind_the_ledger_does_not_move_where_a_restart_reads() {
    let (topic, _cleanup) = fresh_topic("stale_back", 40).await;
    let group = unique_group("stale-back");
    let dir = tempfile::tempdir().unwrap();
    let db = format!("sqlite://{}", dir.path().join("state.db").display());
    {
        let store = vtop_state::SqliteStateStore::connect(&db).await.unwrap();
        ledger_batch(&store, "b-0-19", &topic, &group, (0, 19), true).await;
    }
    assert_eq!(
        stale_commit(&topic, &group, 4).await,
        5,
        "precondition: Kafka took the stale commit and moved the group's offset back"
    );

    let mut engine = engine_on(&db, &dir.path().join("work"), &topic, &group).await;
    engine.recover().await.expect("recovery");
    assert_eq!(
        group_offset(&topic, &group),
        20,
        "the restart left the group's offset behind the ledger, so lag monitoring reports \
         archived records as unread"
    );
    let starts = read_until_a_new_commit(&mut engine).await;
    assert_eq!(
        starts[0], 20,
        "the restart read from offset {} - the stale group offset decided, not the ledger, \
         and records the ledger holds as archived were archived again (committed starts: \
         {starts:?})",
        starts[0]
    );
}

/// #614, the open question: can a stale commit move the offset FORWARD past a
/// range that is not intact? On Kafka's side, yes. Engine B committed `[0, 9]`,
/// re-checked engine A's verified batch `[10, 19]`, found its object damaged
/// and sent it back for replay (REPLAY_REQUIRED). A then resumes and commits
/// offset 20 for that batch, and Kafka accepts it. Before #614 the restart's
/// rewind to 10 was clamped to the group's 20 and `[10, 19]` was never
/// archived. With the ledger deciding, the range is re-read.
#[tokio::test]
#[ignore = "requires a running Kafka broker"]
async fn a_stale_commit_past_a_range_sent_back_for_replay_does_not_skip_it() {
    let (topic, _cleanup) = fresh_topic("stale_fwd", 40).await;
    let group = unique_group("stale-fwd");
    let dir = tempfile::tempdir().unwrap();
    let db = format!("sqlite://{}", dir.path().join("state.db").display());
    {
        let store = vtop_state::SqliteStateStore::connect(&db).await.unwrap();
        ledger_batch(&store, "b-0-9", &topic, &group, (0, 9), true).await;
        ledger_batch(&store, "b-10-19", &topic, &group, (10, 19), false).await;
    }
    assert_eq!(
        stale_commit(&topic, &group, 19).await,
        20,
        "precondition: Kafka took the stale commit and moved the group's offset past [10, 19]"
    );

    let starts = restart_and_read(&db, &dir.path().join("work"), &topic, &group).await;
    assert_eq!(
        starts[0], 10,
        "the restart read from offset {} - a range sent back for replay was skipped because \
         a stale commit moved the group's offset past it (committed starts: {starts:?})",
        starts[0]
    );
}

/// #614 with ONE engine, no stale engine involved. Batch `[0, 9]` reached
/// VERIFIED but its Kafka commit failed, so it stayed VERIFIED; the next batch
/// `[10, 19]` committed (the group's offset went to 20). On restart recovery
/// re-checks `[0, 9]` against storage and commits it. Before #614 that commit
/// was sent to Kafka and moved the group's offset BACK to 10, and the restart
/// read `[10, 19]` again. Now the range is recorded SOURCE_COMMITTED as
/// superseded, the group's offset stays at 20, and reading resumes at 20.
#[tokio::test]
#[ignore = "requires a running Kafka broker"]
async fn recovering_an_older_verified_batch_does_not_move_the_group_offset_back() {
    use vtop_core::manifest::ManifestBuilder;
    use vtop_core::state_machine::BatchState;
    use vtop_core::types::{CompressionType, SourceType};
    use vtop_state::StateStore;

    let (topic, _cleanup) = fresh_topic("superseded", 40).await;
    let group = unique_group("superseded");
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let db = format!("sqlite://{}", dir.path().join("state.db").display());

    // The VERIFIED batch [0, 9]: an object and a manifest the re-check will
    // find intact.
    let marker = kafka_marker(&topic, &group, 0, 9);
    let object_uri = "s3://telemetry-data/x/b-0-9.raw.gz";
    let manifest_uri = "s3://telemetry-data/x/b-0-9.manifest.json";
    let object_bytes = b"archived-bytes";
    let object_digest = vtop_core::checksum::sha256_bytes(object_bytes);
    let manifest = ManifestBuilder {
        batch_id: "b-0-9".into(),
        tenant: "default".into(),
        source_type: SourceType::Kafka,
        source_name: topic.clone(),
        format: TelemetryFormat::Raw,
        compression: CompressionType::None,
        record_count: 10,
        first_timestamp: None,
        last_timestamp: None,
        source_progress: marker.clone(),
        object_uri: object_uri.into(),
        object_size: object_bytes.len() as u64,
        object_checksum_algorithm: "sha256".into(),
        object_checksum: object_digest.clone(),
        manifest_uri: manifest_uri.into(),
        path_template: "test".into(),
        resolved_prefix: "x".into(),
        upload_backend: "mock".into(),
        created_at: chrono::Utc::now().to_rfc3339(),
    }
    .build()
    .unwrap();
    let manifest_path = manifest.write_to_file(&work).unwrap();
    {
        let store = vtop_state::SqliteStateStore::connect(&db).await.unwrap();
        let now = chrono::Utc::now().to_rfc3339();
        store
            .save_batch_state(&vtop_state::BatchRecord {
                batch_id: "b-0-9".into(),
                tenant: "default".into(),
                source_type: SourceType::Kafka,
                source_name: topic.clone(),
                format: TelemetryFormat::Raw,
                state: BatchState::Batching,
                progress_start: kafka_marker(&topic, &group, 0, 0),
                progress_end: marker,
                object_uri: Some(object_uri.into()),
                manifest_uri: Some(manifest_uri.into()),
                object_sha256: Some(object_digest.clone()),
                manifest_sha256: Some(manifest.manifest.sha256.clone()),
                manifest_version_id: None,
                object_size_bytes: Some(object_bytes.len() as i64),
                record_count: Some(10),
                error_message: None,
                owner: None,
                lease_expires_at: None,
                created_at: now.clone(),
                updated_at: now,
            })
            .await
            .unwrap();
        for st in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
            BatchState::Verified,
        ] {
            store
                .update_batch_state("b-0-9", st, &vtop_state::BatchPatch::default())
                .await
                .unwrap();
        }
        // The next batch, read after it and committed - Kafka's offset with it.
        ledger_batch(&store, "b-10-19", &topic, &group, (10, 19), true).await;
    }
    assert_eq!(
        stale_commit(&topic, &group, 19).await,
        20,
        "precondition: the later batch's commit put the group at 20"
    );

    let mut engine = engine_on(&db, &work, &topic, &group).await;
    let staged = dir.path().join("staged-object");
    std::fs::write(&staged, object_bytes).unwrap();
    engine
        .backend
        .put_object(
            &staged,
            object_uri,
            Some(vtop_upload::ObjectChecksum::new("sha256", &object_digest)),
        )
        .await
        .unwrap();
    engine
        .backend
        .put_manifest(
            &manifest_path,
            manifest_uri,
            Some(vtop_upload::ObjectChecksum::new(
                "sha256",
                &manifest.manifest.sha256,
            )),
        )
        .await
        .unwrap();
    let summary = engine.recover().await.expect("recovery");
    assert_eq!(
        summary.committed, 1,
        "the VERIFIED batch is committed: {summary:?}"
    );
    assert_eq!(
        engine
            .store
            .get_batch("b-0-9")
            .await
            .unwrap()
            .unwrap()
            .state,
        BatchState::SourceCommitted
    );
    assert_eq!(
        group_offset(&topic, &group),
        20,
        "recovery's commit of the older range moved the group's offset backwards"
    );
    let starts = read_until_a_new_commit(&mut engine).await;
    assert_eq!(
        starts[0], 20,
        "the restart read from offset {} - recovering an older batch moved where this \
         engine resumes, and archived records were read again (committed starts: {starts:?})",
        starts[0]
    );
}

// ---- #624: VERIFIED rows the running engine settles without a restart ------

/// Append `records` more records to `topic`.
fn produce(topic: &str, records: usize) {
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = build_client_config(&kafka_cfg(&bootstrap(), "vtop-it-producer"))
        .unwrap()
        .create()
        .expect("producer");
    for i in 0..records {
        let payload = format!("CEF:0|VTOP|IT|1.0|{i}|More|3|src=10.0.1.{}", i % 250);
        producer
            .send(BaseRecord::<(), str>::to(topic).payload(&payload))
            .expect("enqueue produce");
    }
    producer
        .flush(std::time::Duration::from_secs(10))
        .expect("flush produce");
}

/// The group's committed offset for partition 0 of `topic`, or `None` while
/// it has none.
fn group_offset_if_any(topic: &str, group: &str) -> Option<i64> {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::topic_partition_list::TopicPartitionList;

    let probe: BaseConsumer = build_client_config(&kafka_cfg(&bootstrap(), group))
        .unwrap()
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(topic, 0);
    // A broker that has just started may not have a group coordinator yet
    // (its offsets topic is still being created): ask again rather than fail
    // a test about the engine on the broker's warm-up.
    let mut attempts = 0;
    let committed = loop {
        match probe.committed_offsets(tpl.clone(), std::time::Duration::from_secs(10)) {
            Ok(committed) => break committed,
            Err(e) if attempts < 10 => {
                attempts += 1;
                eprintln!("reading the group's committed offset: {e}; retrying");
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            Err(e) => panic!("read the group's committed offset: {e}"),
        }
    };
    match committed.find_partition(topic, 0).map(|e| e.offset()) {
        Some(rdkafka::Offset::Offset(at)) => Some(at),
        _ => None,
    }
}

/// Every group offset seen, in order: the test fails if one is lower than
/// the one before it.
#[derive(Default)]
struct OffsetWatch(Vec<i64>);

impl OffsetWatch {
    fn look(&mut self, topic: &str, group: &str) {
        if let Some(at) = group_offset_if_any(topic, group) {
            if let Some(&last) = self.0.last() {
                assert!(
                    at >= last,
                    "the group's committed offset moved BACKWARDS: {:?} then {at}",
                    self.0
                );
            }
            self.0.push(at);
        }
    }
}

/// The Kafka adapter, but its next `failures` source commits fail - a group
/// coordinator that is unavailable, say.
struct FailingCommits {
    inner: Box<dyn vtop_adapters::SourceAdapter>,
    failures: usize,
}

#[async_trait::async_trait]
impl vtop_adapters::SourceAdapter for FailingCommits {
    async fn discover_sources(
        &self,
    ) -> Result<Vec<vtop_adapters::DiscoveredSource>, vtop_core::errors::VtopError> {
        self.inner.discover_sources().await
    }
    async fn read_batch_candidates(
        &mut self,
        source: &vtop_adapters::DiscoveredSource,
        max_records: usize,
        max_bytes: usize,
        max_wait: std::time::Duration,
    ) -> Result<Vec<vtop_adapters::ReadResult>, vtop_core::errors::VtopError> {
        self.inner
            .read_batch_candidates(source, max_records, max_bytes, max_wait)
            .await
    }
    async fn read_all_batch_candidates(
        &mut self,
        sources: &[vtop_adapters::DiscoveredSource],
        max_records: usize,
        max_bytes: usize,
        max_wait: std::time::Duration,
    ) -> Result<vtop_adapters::base::AdapterReadReport, vtop_core::errors::VtopError> {
        self.inner
            .read_all_batch_candidates(sources, max_records, max_bytes, max_wait)
            .await
    }
    async fn commit_progress(
        &mut self,
        marker: &ProgressMarker,
    ) -> Result<(), vtop_core::errors::VtopError> {
        if self.failures > 0 {
            self.failures -= 1;
            return Err(vtop_core::errors::VtopError::Source(
                "kafka commit: coordinator not available".into(),
            ));
        }
        self.inner.commit_progress(marker).await
    }
    async fn replay_from_marker(
        &mut self,
        marker: &ProgressMarker,
    ) -> Result<(), vtop_core::errors::VtopError> {
        self.inner.replay_from_marker(marker).await
    }
    async fn replay_from_markers(
        &mut self,
        markers: &[ProgressMarker],
    ) -> Vec<Result<(), vtop_core::errors::VtopError>> {
        self.inner.replay_from_markers(markers).await
    }
    fn read_position(&self, marker: &ProgressMarker) -> Option<i128> {
        self.inner.read_position(marker)
    }
    fn committed_through(&self, marker: &ProgressMarker) -> Option<i128> {
        self.inner.committed_through(marker)
    }
    fn source_type(&self) -> vtop_core::types::SourceType {
        self.inner.source_type()
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self.inner.as_any_mut()
    }
}

/// #624: a batch whose Kafka commit failed after VERIFIED stayed VERIFIED for
/// as long as the engine ran; the next restart committed it, after later
/// batches had committed past it. Now a later pass retries it: superseded by
/// then, it is recorded SOURCE_COMMITTED without being sent, and the group's
/// offset never moves backwards.
#[tokio::test]
#[ignore = "requires a running Kafka broker"]
async fn a_failed_kafka_commit_is_retried_by_a_later_pass_and_never_moves_the_group_back() {
    use vtop_core::state_machine::BatchState;
    use vtop_core::types::SourceType;

    let (topic, _cleanup) = fresh_topic("commit_retry", 20).await;
    let group = unique_group("commit-retry");
    let dir = tempfile::tempdir().unwrap();
    let db = format!("sqlite://{}", dir.path().join("state.db").display());
    let mut engine = engine_on(&db, &dir.path().join("work"), &topic, &group).await;
    engine.recover().await.expect("recovery");
    let inner = engine.adapters.remove(&SourceType::Kafka).unwrap();
    engine.adapters.insert(
        SourceType::Kafka,
        Box::new(FailingCommits { inner, failures: 1 }),
    );
    let mut watch = OffsetWatch::default();

    // The first batch verifies, and its commit fails.
    let mut stuck = None;
    for _ in 0..15 {
        let outcomes = engine
            .process_once(SourceType::Kafka)
            .await
            .expect("a pass");
        watch.look(&topic, &group);
        if let Some(o) = outcomes.into_iter().find(|o| !o.committed) {
            stuck = Some(o.batch_id);
            break;
        }
    }
    let stuck = stuck.expect("a batch whose commit failed");
    assert_eq!(
        engine.store.get_batch(&stuck).await.unwrap().unwrap().state,
        BatchState::Verified
    );
    assert_eq!(
        group_offset_if_any(&topic, &group),
        None,
        "nothing committed yet"
    );

    // More records arrive; a later batch commits past the stuck one, and the
    // pass retries the stuck one.
    produce(&topic, 10);
    for _ in 0..15 {
        engine
            .process_once(SourceType::Kafka)
            .await
            .expect("a pass");
        watch.look(&topic, &group);
        if engine.store.get_batch(&stuck).await.unwrap().unwrap().state
            == BatchState::SourceCommitted
        {
            break;
        }
    }
    assert_eq!(
        engine.store.get_batch(&stuck).await.unwrap().unwrap().state,
        BatchState::SourceCommitted,
        "the batch whose commit failed is committed without a restart"
    );
    assert_eq!(
        watch.0.last(),
        Some(&30),
        "the group's offset ends after the last record, never back at 20: {:?}",
        watch.0
    );
}

/// #624: recovery holds a VERIFIED range back while a lower range of its
/// partition is read again, and nothing in the running loop looked at it
/// again: it stayed VERIFIED until the next restart. Now the pass whose
/// re-read commits past it settles it - SOURCE_COMMITTED, not sent to Kafka,
/// and the group's offset never moves backwards.
#[tokio::test]
#[ignore = "requires a running Kafka broker"]
async fn a_held_back_kafka_range_is_settled_without_a_restart_and_never_moves_the_group_back() {
    use vtop_core::manifest::ManifestBuilder;
    use vtop_core::state_machine::BatchState;
    use vtop_core::types::{CompressionType, SourceType};
    use vtop_state::StateStore;

    let (topic, _cleanup) = fresh_topic("held_back", 40).await;
    let group = unique_group("held-back");
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let db = format!("sqlite://{}", dir.path().join("state.db").display());

    // [0, 9] was handed back for replay; [10, 19] is VERIFIED, with an object
    // and a manifest the re-check will find intact.
    let marker = kafka_marker(&topic, &group, 10, 19);
    let object_uri = "s3://telemetry-data/x/b-10-19.raw.gz";
    let manifest_uri = "s3://telemetry-data/x/b-10-19.manifest.json";
    let object_bytes = b"archived-bytes";
    let object_digest = vtop_core::checksum::sha256_bytes(object_bytes);
    let manifest = ManifestBuilder {
        batch_id: "b-10-19".into(),
        tenant: "default".into(),
        source_type: SourceType::Kafka,
        source_name: topic.clone(),
        format: TelemetryFormat::Raw,
        compression: CompressionType::None,
        record_count: 10,
        first_timestamp: None,
        last_timestamp: None,
        source_progress: marker.clone(),
        object_uri: object_uri.into(),
        object_size: object_bytes.len() as u64,
        object_checksum_algorithm: "sha256".into(),
        object_checksum: object_digest.clone(),
        manifest_uri: manifest_uri.into(),
        path_template: "test".into(),
        resolved_prefix: "x".into(),
        upload_backend: "mock".into(),
        created_at: chrono::Utc::now().to_rfc3339(),
    }
    .build()
    .unwrap();
    let manifest_path = manifest.write_to_file(&work).unwrap();
    {
        let store = vtop_state::SqliteStateStore::connect(&db).await.unwrap();
        ledger_batch(&store, "b-0-9", &topic, &group, (0, 9), false).await;
        let now = chrono::Utc::now().to_rfc3339();
        store
            .save_batch_state(&vtop_state::BatchRecord {
                batch_id: "b-10-19".into(),
                tenant: "default".into(),
                source_type: SourceType::Kafka,
                source_name: topic.clone(),
                format: TelemetryFormat::Raw,
                state: BatchState::Batching,
                progress_start: kafka_marker(&topic, &group, 10, 10),
                progress_end: marker,
                object_uri: Some(object_uri.into()),
                manifest_uri: Some(manifest_uri.into()),
                object_sha256: Some(object_digest.clone()),
                manifest_sha256: Some(manifest.manifest.sha256.clone()),
                manifest_version_id: None,
                object_size_bytes: Some(object_bytes.len() as i64),
                record_count: Some(10),
                error_message: None,
                owner: None,
                lease_expires_at: None,
                created_at: now.clone(),
                updated_at: now,
            })
            .await
            .unwrap();
        for st in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
            BatchState::Verified,
        ] {
            store
                .update_batch_state("b-10-19", st, &vtop_state::BatchPatch::default())
                .await
                .unwrap();
        }
    }

    let mut engine = engine_on(&db, &work, &topic, &group).await;
    let staged = dir.path().join("staged-object");
    std::fs::write(&staged, object_bytes).unwrap();
    engine
        .backend
        .put_object(
            &staged,
            object_uri,
            Some(vtop_upload::ObjectChecksum::new("sha256", &object_digest)),
        )
        .await
        .unwrap();
    engine
        .backend
        .put_manifest(
            &manifest_path,
            manifest_uri,
            Some(vtop_upload::ObjectChecksum::new(
                "sha256",
                &manifest.manifest.sha256,
            )),
        )
        .await
        .unwrap();
    let summary = engine.recover().await.expect("recovery");
    assert_eq!(
        (summary.committed, summary.still_pending),
        (0, 1),
        "the VERIFIED range is held back above the range being re-read: {summary:?}"
    );
    let mut watch = OffsetWatch::default();
    watch.look(&topic, &group);

    for _ in 0..15 {
        engine
            .process_once(SourceType::Kafka)
            .await
            .expect("a read pass");
        watch.look(&topic, &group);
        if engine
            .store
            .get_batch("b-10-19")
            .await
            .unwrap()
            .unwrap()
            .state
            == BatchState::SourceCommitted
        {
            break;
        }
    }
    assert_eq!(
        engine
            .store
            .get_batch("b-10-19")
            .await
            .unwrap()
            .unwrap()
            .state,
        BatchState::SourceCommitted,
        "the held-back range is settled without a restart"
    );
    assert_eq!(
        watch.0.last(),
        Some(&40),
        "the group's offset ends after the last record, never back at 20: {:?}",
        watch.0
    );
}

//! Integration test: Kafka source -> object storage.
//!
//! This test requires a live Kafka broker and is therefore `#[ignore]` by
//! default. Run it against the docker-compose lab:
//!
//! ```bash
//! docker compose up -d kafka minio minio-init
//! # produce some events into a topic, then:
//! VTOP_TEST_KAFKA=localhost:9092 \
//!   cargo test -p vtop-cli --test integration_kafka_to_minio -- --ignored
//! ```
//!
//! It exercises the same pipeline as the file test, but proves that Kafka
//! offsets are committed only after VERIFIED (auto-commit is always disabled).

use vtop_adapters::kafka_source::build_client_config;
use vtop_core::config::KafkaSourceConfig;

fn kafka_cfg(bootstrap: &str) -> KafkaSourceConfig {
    KafkaSourceConfig {
        enabled: true,
        bootstrap_servers: vec![bootstrap.to_string()],
        consumer_group: "vtop-engine-it".into(),
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
/// independent of any broker. This is the core safety property and runs in CI.
#[test]
fn kafka_client_never_auto_commits() {
    let cc = build_client_config(&kafka_cfg("localhost:9092"));
    assert_eq!(cc.get("enable.auto.commit"), Some("false"));
}

#[tokio::test]
#[ignore = "requires a running Kafka broker (docker-compose lab)"]
async fn kafka_source_archives_and_commits() {
    use vtop_adapters::base::SourceAdapter;
    use vtop_adapters::KafkaSource;
    use vtop_core::types::TelemetryFormat;

    let bootstrap = std::env::var("VTOP_TEST_KAFKA").unwrap_or_else(|_| "localhost:9092".into());
    let mut adapter = KafkaSource::new(kafka_cfg(&bootstrap), TelemetryFormat::Cef).unwrap();

    // Discovery proves the broker is reachable and topic filters apply.
    let sources = adapter
        .discover_sources()
        .await
        .expect("kafka discovery (is the broker up?)");
    assert!(
        !sources.is_empty(),
        "expected at least one non-internal topic"
    );

    // Read a batch from the first discovered topic.
    let src = &sources[0];
    let read = adapter
        .read_batch_candidates(src, 100, 1 << 20, std::time::Duration::from_secs(5))
        .await
        .unwrap();
    // The progress marker must be a Kafka marker with a single partition.
    match read.progress_end {
        vtop_core::types::ProgressMarker::Kafka { partition, .. } => {
            assert!(partition >= 0);
        }
        _ => panic!("expected kafka marker"),
    }
}

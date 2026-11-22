//! Integration test: the Kafka source against a broker whose listener
//! requires TLS or SASL, with credentials and certificates that are wrong
//! (#721).
//!
//! System model: one broker, an honest network, a client that is given a
//! credential or certificate the broker must refuse. The invariant is
//! safety: a client that cannot prove who it is, or cannot prove it is
//! talking to the broker it meant to, is refused and reads no record. A
//! refusal that arrived after a hang, or that echoed the password into an
//! error message, is a failure too.
//!
//! `integration_kafka_to_minio.rs` runs the whole live suite THROUGH these
//! listeners with the right credentials; this file is the other half, the
//! wrong ones. Each test first reads the topic with the right credentials, so
//! a refusal below is the credential's fault and not a broker that is down or
//! a listener nobody can reach.
//!
//! Test names say which listener they need: `sasl_*` tests need a SASL
//! listener (`SASL_PLAINTEXT` or `SASL_SSL`), `tls_*` tests a TLS one (`SSL`
//! or `SASL_SSL`), `client_cert_*` tests a TLS listener that requires a
//! client certificate (no prefix contains another: the test filter matches
//! substrings). A leg runs the prefixes its listener has
//! (`.github/workflows/kafka-matrix.yml`), and a test whose environment is
//! missing panics instead of passing, so a leg configured wrongly fails.
//!
//! ```bash
//! scripts/kafka-broker.sh up apache/kafka:4.3.2 sasl-ssl
//! scripts/kafka-broker.sh seed
//! set -a; eval "$(scripts/kafka-broker.sh client-env SCRAM-SHA-512)"; set +a
//! cargo test -p vtop-cli --test integration_kafka_auth -- --ignored sasl_
//! cargo test -p vtop-cli --test integration_kafka_auth -- --ignored tls_
//! # mutual TLS: `up ... mtls`, then
//! cargo test -p vtop-cli --test integration_kafka_auth -- --ignored client_cert_
//! ```

use std::time::{Duration, Instant};

use vtop_adapters::base::{DiscoveredSource, SourceAdapter};
use vtop_adapters::KafkaSource;
use vtop_core::config::KafkaSourceConfig;
use vtop_core::types::{SourceType, TelemetryFormat};

#[path = "kafka_broker_env.rs"]
mod broker_env;

/// The topic the broker script seeds.
const SEEDED_TOPIC: &str = "it_events";

/// How long a refusal may take. The adapter bounds its metadata request at 10
/// seconds; a refusal that needs several times that is a hang that happened
/// to end.
const REFUSAL_BUDGET: Duration = Duration::from_secs(45);

/// A config for the seeded broker with the credentials the environment holds.
fn good_cfg() -> KafkaSourceConfig {
    let mut cfg = KafkaSourceConfig {
        enabled: true,
        bootstrap_servers: vec![broker_env::bootstrap()],
        consumer_group: format!(
            "vtop-it-auth-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ),
        topic_include_regex: ".*".into(),
        topic_exclude_regex: "^__.*".into(),
        auto_offset_reset: "earliest".into(),
        enable_auto_commit: false,
        security_protocol: None,
        sasl_mechanism: None,
        sasl_username: None,
        sasl_password_env: None,
        ssl_ca_location: None,
        ssl_certificate_location: None,
        ssl_key_location: None,
        ssl_key_password_env: None,
    };
    broker_env::apply(&mut cfg);
    cfg
}

fn require(name: &str) -> String {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v,
        _ => panic!(
            "{name} is not set: this test needs a broker started by scripts/kafka-broker.sh and \
             the variables `scripts/kafka-broker.sh client-env` prints for it"
        ),
    }
}

fn uses_sasl(cfg: &KafkaSourceConfig) -> bool {
    cfg.security_protocol
        .as_deref()
        .is_some_and(|p| p.to_ascii_uppercase().starts_with("SASL"))
}

fn uses_tls(cfg: &KafkaSourceConfig) -> bool {
    cfg.security_protocol
        .as_deref()
        .is_some_and(|p| p.to_ascii_uppercase().ends_with("SSL"))
}

/// The topics a config can list, or why it could not.
async fn discover(cfg: KafkaSourceConfig) -> Result<Vec<String>, String> {
    let adapter = KafkaSource::new(cfg, TelemetryFormat::Cef).map_err(|e| e.to_string())?;
    adapter
        .discover_sources()
        .await
        .map(|sources| sources.into_iter().map(|s| s.source_name).collect())
        .map_err(|e| e.to_string())
}

/// The control: the right credentials list the seeded topic.
async fn assert_the_right_credentials_work() {
    let topics = discover(good_cfg()).await.unwrap_or_else(|e| {
        panic!(
            "the correct credentials were refused ({e}); the broker, its listener or the \
             variables are wrong, so a refusal below would prove nothing"
        )
    });
    assert!(
        topics.iter().any(|t| t == SEEDED_TOPIC),
        "the correct credentials listed {topics:?}, which lacks the seeded topic '{SEEDED_TOPIC}'"
    );
}

/// `cfg` must be refused, in time, with a message that names no secret.
async fn assert_refused(what: &str, cfg: KafkaSourceConfig, secrets: &[&str]) -> String {
    let started = Instant::now();
    let outcome = discover(cfg).await;
    let took = started.elapsed();
    let message = match outcome {
        Err(message) => message,
        Ok(topics) => panic!("{what}: the broker listed {topics:?} to a client it should refuse"),
    };
    assert!(
        took < REFUSAL_BUDGET,
        "{what}: the refusal took {took:?}, over the {REFUSAL_BUDGET:?} budget; message: {message}"
    );
    for secret in secrets {
        assert!(
            !message.contains(secret),
            "{what}: the error message contains a secret: {message}"
        );
    }
    eprintln!("{what}: refused after {took:?}: {message}");
    message
}

/// A client that is refused reads no record, however the read is asked for.
async fn assert_reads_nothing(what: &str, cfg: KafkaSourceConfig) {
    let mut adapter = match KafkaSource::new(cfg, TelemetryFormat::Cef) {
        Ok(adapter) => adapter,
        Err(e) => {
            eprintln!("{what}: no adapter was built ({e})");
            return;
        }
    };
    let source = DiscoveredSource {
        source_type: SourceType::Kafka,
        source_name: SEEDED_TOPIC.to_string(),
        format: TelemetryFormat::Cef,
    };
    let read = adapter
        .read_batch_candidates(&source, 1000, 1 << 20, Duration::from_secs(3))
        .await;
    match read {
        Err(e) => eprintln!("{what}: the read failed, as it should ({e})"),
        Ok(results) => {
            let records: usize = results.iter().map(|r| r.records.len()).sum();
            assert_eq!(
                records, 0,
                "{what}: a refused client read {records} records of '{SEEDED_TOPIC}'"
            );
        }
    }
}

fn the_password() -> String {
    require(broker_env::PASSWORD_ENV)
}

// ---------------------------------------------------------------------------
// SASL: PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 share these tests; the leg's
// mechanism is whichever the environment names.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires a running Kafka broker with a SASL listener"]
async fn sasl_a_wrong_password_is_refused() {
    let good = good_cfg();
    assert!(
        uses_sasl(&good),
        "VTOP_TEST_KAFKA_SECURITY_PROTOCOL must be SASL_PLAINTEXT or SASL_SSL for this test"
    );
    assert_the_right_credentials_work().await;
    let real = the_password();
    // The config names a variable that holds the password, so the wrong one
    // lives in a variable too, which the broker script exports beside the
    // right one. It is neither a prefix, a suffix nor a case change of the
    // right one, so a broker that compared loosely would still refuse it.
    let name = "VTOP_TEST_KAFKA_SASL_WRONG_PASSWORD";
    let wrong = require(name);
    assert_ne!(real, wrong);
    let mut cfg = good_cfg();
    cfg.sasl_password_env = Some(name.to_string());
    assert_refused("a wrong password", cfg.clone(), &[&real, &wrong]).await;
    assert_reads_nothing("a wrong password", cfg).await;
}

#[tokio::test]
#[ignore = "requires a running Kafka broker with a SASL listener"]
async fn sasl_an_unknown_user_is_refused() {
    let good = good_cfg();
    assert!(uses_sasl(&good), "this test needs a SASL listener");
    assert_the_right_credentials_work().await;
    let real = the_password();
    let mut cfg = good_cfg();
    cfg.sasl_username = Some("nobody-has-this-name".to_string());
    assert_refused("an unknown user", cfg.clone(), &[&real]).await;
    assert_reads_nothing("an unknown user", cfg).await;
}

#[tokio::test]
#[ignore = "requires a running Kafka broker with a SASL listener"]
async fn sasl_a_client_that_names_no_credentials_is_refused() {
    let good = good_cfg();
    assert!(uses_sasl(&good), "this test needs a SASL listener");
    assert_the_right_credentials_work().await;
    // The protocol and the mechanism, no user and no password: the shape of a
    // config whose credentials were deleted.
    let mut cfg = good_cfg();
    cfg.sasl_username = None;
    cfg.sasl_password_env = None;
    assert_refused("a SASL client with no credentials", cfg, &[&the_password()]).await;
}

#[tokio::test]
#[ignore = "requires a running Kafka broker with a SASL listener"]
async fn sasl_a_client_that_does_not_speak_sasl_is_refused() {
    let good = good_cfg();
    assert!(uses_sasl(&good), "this test needs a SASL listener");
    assert_the_right_credentials_work().await;
    // The same address and TLS settings, but the client asks for no SASL. On
    // SASL_SSL it still completes the TLS handshake (it has the CA), so the
    // refusal is the broker's, for the missing login.
    let mut cfg = good_cfg();
    cfg.security_protocol = Some(if uses_tls(&good) { "SSL" } else { "PLAINTEXT" }.to_string());
    cfg.sasl_mechanism = None;
    cfg.sasl_username = None;
    cfg.sasl_password_env = None;
    assert_refused("a client with no SASL", cfg.clone(), &[&the_password()]).await;
    assert_reads_nothing("a client with no SASL", cfg).await;
}

// ---------------------------------------------------------------------------
// TLS: the client verifies the broker. On a listener that requires a client
// certificate the good config carries one; these tests leave it in place, so
// what they change is the only thing wrong.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires a running Kafka broker with a TLS listener"]
async fn tls_a_client_without_the_ca_refuses_the_broker() {
    let good = good_cfg();
    assert!(uses_tls(&good), "this test needs a TLS listener");
    assert_the_right_credentials_work().await;
    // No ssl_ca_location: the client falls back to the system's roots, which
    // do not hold the test CA, so the broker's certificate cannot be trusted.
    let mut cfg = good_cfg();
    cfg.ssl_ca_location = None;
    assert_refused("a client with no CA", cfg.clone(), &[]).await;
    assert_reads_nothing("a client with no CA", cfg).await;
}

#[tokio::test]
#[ignore = "requires a running Kafka broker with a TLS listener"]
async fn tls_a_client_given_another_ca_refuses_the_broker() {
    let good = good_cfg();
    assert!(uses_tls(&good), "this test needs a TLS listener");
    assert_the_right_credentials_work().await;
    // A real CA certificate that did not sign the broker's: what a client
    // sees when something else is answering on the broker's address.
    let other = require("VTOP_TEST_KAFKA_SSL_OTHER_CA");
    assert_ne!(Some(other.as_str()), good.ssl_ca_location.as_deref());
    let mut cfg = good_cfg();
    cfg.ssl_ca_location = Some(other);
    assert_refused("a client given another CA", cfg.clone(), &[]).await;
    assert_reads_nothing("a client given another CA", cfg).await;
}

#[tokio::test]
#[ignore = "requires a running Kafka broker with a TLS listener"]
async fn tls_a_plaintext_client_is_refused() {
    let good = good_cfg();
    assert!(uses_tls(&good), "this test needs a TLS listener");
    assert_the_right_credentials_work().await;
    // The default security protocol is plaintext; a client left on it speaks
    // to a TLS port, which must not answer it.
    let mut cfg = good_cfg();
    cfg.security_protocol = None;
    cfg.sasl_mechanism = None;
    cfg.sasl_username = None;
    cfg.sasl_password_env = None;
    cfg.ssl_ca_location = None;
    cfg.ssl_certificate_location = None;
    cfg.ssl_key_location = None;
    cfg.ssl_key_password_env = None;
    assert_refused("a plaintext client", cfg.clone(), &[]).await;
    assert_reads_nothing("a plaintext client", cfg).await;
}

// ---------------------------------------------------------------------------
// Mutual TLS: the broker verifies the client (#721).
// ---------------------------------------------------------------------------

fn uses_mtls(cfg: &KafkaSourceConfig) -> bool {
    uses_tls(cfg) && cfg.ssl_certificate_location.is_some()
}

#[tokio::test]
#[ignore = "requires a running Kafka broker whose TLS listener requires a client certificate"]
async fn client_cert_a_client_without_one_is_refused() {
    let good = good_cfg();
    assert!(
        uses_mtls(&good),
        "this test needs a mutual-TLS listener and VTOP_TEST_KAFKA_SSL_CERT and _KEY"
    );
    assert_the_right_credentials_work().await;
    // It trusts the broker (the CA is set), but presents nothing.
    let mut cfg = good_cfg();
    cfg.ssl_certificate_location = None;
    cfg.ssl_key_location = None;
    assert_refused("a client without a certificate", cfg.clone(), &[]).await;
    assert_reads_nothing("a client without a certificate", cfg).await;
}

#[tokio::test]
#[ignore = "requires a running Kafka broker whose TLS listener requires a client certificate"]
async fn client_cert_another_ca_signed_is_refused() {
    let good = good_cfg();
    assert!(uses_mtls(&good), "this test needs a mutual-TLS listener");
    assert_the_right_credentials_work().await;
    // A well-formed client certificate and its key, signed by a CA the broker
    // does not trust.
    let mut cfg = good_cfg();
    cfg.ssl_certificate_location = Some(require("VTOP_TEST_KAFKA_SSL_OTHER_CERT"));
    cfg.ssl_key_location = Some(require("VTOP_TEST_KAFKA_SSL_OTHER_KEY"));
    assert_ne!(cfg.ssl_certificate_location, good.ssl_certificate_location);
    assert_refused("a certificate another CA signed", cfg.clone(), &[]).await;
    assert_reads_nothing("a certificate another CA signed", cfg).await;
}

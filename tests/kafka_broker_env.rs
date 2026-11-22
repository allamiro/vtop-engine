//! The client side of a live-broker Kafka test: where the broker is, and how
//! the client is to authenticate to it (#721).
//!
//! Shared by `integration_kafka_to_minio.rs` (the whole live suite) and
//! `integration_kafka_auth.rs` (the wrong-credential and wrong-certificate
//! tests) with `#[path]`, so the two read the same settings the same way.
//!
//! The environment, all optional except that a SASL protocol needs the
//! credential variables it uses. `scripts/kafka-broker.sh client-env` prints
//! the values that match the listener it started, so a leg never repeats them:
//!
//! - `VTOP_TEST_KAFKA`: the bootstrap address (default `kafka:9092`)
//! - `VTOP_TEST_KAFKA_SECURITY_PROTOCOL`: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`
//!   or `SASL_SSL` (default: the config's own default, plaintext)
//! - `VTOP_TEST_KAFKA_SASL_MECHANISM`: `PLAIN`, `SCRAM-SHA-256` or
//!   `SCRAM-SHA-512`
//! - `VTOP_TEST_KAFKA_SASL_USERNAME`
//! - `VTOP_TEST_KAFKA_SASL_PASSWORD`: the password. The config never holds it:
//!   it names this variable in `sasl_password_env`, as a deployment does.
//! - `VTOP_TEST_KAFKA_SSL_CA`: the CA certificate that signed the broker's
//!   certificate (`ssl_ca_location`)
//! - `VTOP_TEST_KAFKA_SSL_CERT`, `VTOP_TEST_KAFKA_SSL_KEY`: the client
//!   certificate and key for a listener that requires one (mutual TLS:
//!   `ssl_certificate_location`, `ssl_key_location`)

use vtop_core::config::KafkaSourceConfig;

/// The variable the test config names as `sasl_password_env`.
pub const PASSWORD_ENV: &str = "VTOP_TEST_KAFKA_SASL_PASSWORD";

pub fn bootstrap() -> String {
    std::env::var("VTOP_TEST_KAFKA").unwrap_or_else(|_| "kafka:9092".into())
}

/// A variable that is set and not empty: a workflow that exports an empty
/// value means "not set".
fn present(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

/// Give `cfg` the security settings the environment names.
pub fn apply(cfg: &mut KafkaSourceConfig) {
    cfg.security_protocol = present("VTOP_TEST_KAFKA_SECURITY_PROTOCOL");
    cfg.sasl_mechanism = present("VTOP_TEST_KAFKA_SASL_MECHANISM");
    cfg.sasl_username = present("VTOP_TEST_KAFKA_SASL_USERNAME");
    cfg.sasl_password_env = present(PASSWORD_ENV).map(|_| PASSWORD_ENV.to_string());
    cfg.ssl_ca_location = present("VTOP_TEST_KAFKA_SSL_CA");
    cfg.ssl_certificate_location = present("VTOP_TEST_KAFKA_SSL_CERT");
    cfg.ssl_key_location = present("VTOP_TEST_KAFKA_SSL_KEY");
}

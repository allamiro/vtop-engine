//! The operator message for a verification receipt that could not be
//! written (#671 release B), in a binary with one test: `tracing` caches a
//! callsite's interest globally, and a concurrent test that reached it first
//! with no subscriber would hide the event from this capture.
//!
//! The message has three parts (what happened, what the engine did, what to
//! do), says batches are unaffected, is rate-limited with the suppressed
//! count, and carries no key, no MAC value and no URL query string, even
//! when the store's error quotes a signed URL.
#![cfg(unix)]

use std::io::Write;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use vtop_cli::Engine;
use vtop_core::config::StreamsConfig;
use vtop_core::types::SourceType;
use vtop_upload::{MockBackend, UploadBackend};

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);
struct LogWriter(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
    type Writer = LogWriter;
    fn make_writer(&'a self) -> LogWriter {
        LogWriter(self.0.clone())
    }
}

#[tokio::test]
async fn a_receipt_that_cannot_be_written_is_a_rate_limited_warning_that_leaks_nothing() {
    let logs = Logs::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_writer(logs.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let key_hex = "5d".repeat(32);
    let env = format!("VTOP_TEST_RECEIPT_MESSAGE_MAC_{}", std::process::id());
    std::env::set_var(&env, &key_hex);
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    let write = |tag: &str| {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&input)
            .unwrap();
        for i in 0..3 {
            writeln!(f, "{tag} record {i}").unwrap();
        }
    };
    write("app");
    let mut cfg = vtop_cli::testkit::file_config(
        dir.path().join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.path().join("state.db").display()),
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    cfg.manifest_mac_key_env = Some(env.clone());
    cfg.manifest.verification_receipts = true;
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    std::env::remove_var(&env);
    // The store's error quotes a signed URL, as an SDK error can.
    let backend = Arc::new(MockBackend::new().with_failing_put_suffix(".receipt.json"));
    engine.backend = backend.clone() as Arc<dyn UploadBackend>;

    let mut batch_ids = Vec::new();
    for round in 0..2 {
        if round == 1 {
            write("more");
        }
        let out = engine.process_once(SourceType::File).await.unwrap();
        assert!(out[0].committed, "{out:?}");
        batch_ids.push(out[0].batch_id.clone());
    }

    let raw = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    let events: Vec<Value> = raw
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|v| v["fields"]["event"] == "verification_receipt_failed")
        .collect();
    assert_eq!(
        events.len(),
        1,
        "two failures in a row are one warning, the second suppressed: {raw}"
    );
    let event = &events[0];
    assert_eq!(event["level"], "WARN");
    assert_eq!(event["fields"]["batch_id"], batch_ids[0].as_str());
    assert_eq!(event["fields"]["suppressed"], 0);
    let message = event["fields"]["message"].as_str().unwrap();
    for part in [
        // what happened
        &format!("the verification receipt for batch {} could not be written", batch_ids[0]),
        "InternalError",
        // what the engine did
        "the batch is verified and was committed regardless; batches are unaffected",
        // what to do
        "recovery writes the receipt if the batch is still VERIFIED at the next start",
        "vtopctl scrub reports it missing",
        "check the store's health and the engine's permission to write beside the manifests",
    ] {
        assert!(message.contains(part), "missing {part:?} in: {message}");
    }
    // Redaction: no key, no MAC value, no URL query, anywhere in the log.
    assert!(!raw.contains(&key_hex), "the key leaked: {raw}");
    assert!(!raw.contains("X-Amz-Signature=") && !raw.contains("X-Amz-Credential="), "{raw}");
    assert!(message.contains("?[redacted]"), "{message}");
    let hex_run = message
        .split(|c: char| !c.is_ascii_hexdigit())
        .map(str::len)
        .max()
        .unwrap_or(0);
    assert!(hex_run < 64, "a digest or MAC value in the warning: {message}");
}

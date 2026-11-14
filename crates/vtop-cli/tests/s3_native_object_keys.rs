//! #626 against a real S3 API (the native backend on a versioned bucket).
//!
//! The mock and localfs tests pin every key decision; this one checks the
//! decisions the native backend takes on a real store, where the evidence is
//! the store's own: a 404 HEAD is a free key, a reuse adds no object version,
//! and a key holding other bytes is never written over. The bucket must have
//! versioning enabled (the hardened profile, `require_object_versioning`).
//!
//! Its own test binary (it sets credentials in the process environment), and
//! `#[ignore]`d because it needs a store:
//!
//! ```bash
//! VTOP_TEST_S3_ENDPOINT=http://localhost:9000 VTOP_TEST_S3_BUCKET=vtop-versioned \
//! AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//!   cargo test -p vtop-cli --test s3_native_object_keys -- --ignored
//! ```
#![cfg(unix)]

use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use vtop_cli::scrub::{self, ScrubOptions};
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::{StreamsConfig, VtopConfig};
use vtop_core::types::SourceType;

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

impl Logs {
    fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        let logs = Logs::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::INFO)
            .with_writer(logs.clone())
            .finish();
        (logs.clone(), tracing::subscriber::set_default(subscriber))
    }

    fn one(&self, event: &str, batch_id: &str) -> Value {
        let bytes = self.0.lock().unwrap().clone();
        let found: Vec<Value> = String::from_utf8(bytes)
            .unwrap()
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter_map(|v| v.get("fields").cloned())
            .filter(|f| {
                f.get("event").and_then(Value::as_str) == Some(event)
                    && f.get("batch_id").and_then(Value::as_str) == Some(batch_id)
            })
            .collect();
        assert_eq!(found.len(), 1, "one {event} for {batch_id}: {found:?}");
        found[0].clone()
    }
}

fn field(v: &Value, name: &str) -> String {
    v.get(name)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for this test"))
}

fn config(dir: &Path, db: &str, input: &Path, prefix: &str) -> VtopConfig {
    let mut cfg = file_config(
        dir.join(format!("work-{db}")).to_str().unwrap(),
        &format!("sqlite://{}", dir.join(db).display()),
        vec![input.to_string_lossy().into_owned()],
        "s3_native",
    );
    cfg.upload.bucket = env("VTOP_TEST_S3_BUCKET");
    cfg.upload.prefix = prefix.into();
    cfg.upload.endpoint_url = Some(env("VTOP_TEST_S3_ENDPOINT"));
    cfg.upload.region = "us-east-1".into();
    cfg.upload.force_path_style = true;
    cfg.upload.require_object_versioning = true;
    cfg.upload.require_strong_verification = true;
    cfg.partitioning.template = "tenant={tenant}/format={format}/".into();
    cfg
}

async fn s3() -> aws_sdk_s3::Client {
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(env("VTOP_TEST_S3_ENDPOINT"))
        .force_path_style(true)
        .credentials_provider(Credentials::new(
            env("AWS_ACCESS_KEY_ID"),
            env("AWS_SECRET_ACCESS_KEY"),
            None,
            None,
            "test",
        ))
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}

/// The stored versions of exactly `uri`.
async fn versions(client: &aws_sdk_s3::Client, uri: &str) -> usize {
    let (bucket, key) = uri.strip_prefix("s3://").unwrap().split_once('/').unwrap();
    client
        .list_object_versions()
        .bucket(bucket)
        .prefix(key)
        .send()
        .await
        .unwrap()
        .versions()
        .iter()
        .filter(|v| v.key() == Some(key))
        .count()
}

async fn run(cfg: VtopConfig) -> (String, String, Engine) {
    let mut e = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .expect("engine on the native backend");
    assert_eq!(e.backend.backend_name(), "s3_native");
    assert!(e.backend.reports_absent_objects());
    let out = e.process_once(SourceType::File).await.unwrap();
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(out[0].committed, "{out:?}");
    (
        out[0].batch_id.clone(),
        out[0].object_uri.clone().unwrap(),
        e,
    )
}

#[tokio::test]
#[ignore = "requires an S3 API endpoint with a versioned bucket"]
async fn the_native_backend_reuses_a_verified_object_and_never_overwrites_another() {
    let client = s3().await;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    {
        let mut f = std::fs::File::create(&input).unwrap();
        for i in 0..20 {
            writeln!(f, "s3 record {i}").unwrap();
        }
    }
    let prefix = format!(
        "it-626-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let (logs, _guard) = Logs::capture();

    // A: the key is free (a 404 HEAD), the batch is put under it, and its
    // row pins the manifest's stored version.
    let (a_id, archived, a) = run(config(dir.path(), "a.db", &input, &prefix)).await;
    assert_eq!(
        field(&logs.one("object_key_derived", &a_id), "key"),
        archived
    );
    let a_row = a.store.get_batch(&a_id).await.unwrap().unwrap();
    assert!(a_row.manifest_version_id.is_some(), "a pinned manifest");
    assert_eq!(versions(&client, &archived).await, 1);

    // B: the ledger is lost; the same bytes are read again and the stored
    // object is reused, adding no version.
    let (b_id, reused, b) = run(config(dir.path(), "b.db", &input, &prefix)).await;
    assert_eq!(reused, archived);
    assert_eq!(
        field(&logs.one("object_key_reused", &b_id), "key"),
        archived
    );
    assert_eq!(
        versions(&client, &archived).await,
        1,
        "a reuse adds no version"
    );
    let report = b
        .scrub(
            scrub::committed_rows(b.store.list_batches().await.unwrap(), Vec::new()),
            &ScrubOptions::default(),
        )
        .await
        .unwrap();
    assert!(report.passed, "{report:#?}");

    // Another writer replaces the object with other bytes. C reads the same
    // range again: the key holds bytes that do not verify, so C writes under
    // its attempt key and adds nothing at the derived key.
    let (bucket, key) = archived
        .strip_prefix("s3://")
        .unwrap()
        .split_once('/')
        .unwrap();
    let size = a_row.object_size_bytes.unwrap() as usize;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(vec![0x42u8; size].into())
        .send()
        .await
        .unwrap();
    assert_eq!(versions(&client, &archived).await, 2);
    let (c_id, written, _c) = run(config(dir.path(), "c.db", &input, &prefix)).await;
    assert_ne!(written, archived);
    let fallback = logs.one("object_key_fallback", &c_id);
    assert_eq!(field(&fallback, "key"), archived);
    assert_eq!(field(&fallback, "fallback_key"), written);
    assert!(
        field(&fallback, "message").contains("instead of overwriting it"),
        "{fallback}"
    );
    assert_eq!(
        versions(&client, &archived).await,
        2,
        "the engine wrote nothing over the other writer's object"
    );
}

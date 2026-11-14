//! Helpers shared by the Object Lock engine tests (#684): `object_lock.rs`
//! and `object_lock_messages.rs`, which asserts log events from a binary
//! with one test, so no concurrent test hits a callsite first without a
//! subscriber and caches its interest as "never".
#![allow(dead_code)]

use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::{ObjectLockConfig, ObjectLockMode, StreamsConfig, VtopConfig};
use vtop_core::state_machine::BatchState;
use vtop_state::BatchRecord;
use vtop_upload::{MockBackend, ObjectLockPolicy, UploadBackend};

pub const TEMPLATE: &str = "tenant={tenant}/source={source}/format={format}/";

pub fn write_lines(path: &Path, tag: &str, lines: usize) {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    for i in 0..lines {
        writeln!(f, "{tag} record {i}").unwrap();
    }
}

pub fn lock(mode: ObjectLockMode, legal_hold: bool) -> ObjectLockConfig {
    ObjectLockConfig {
        mode,
        retain_days: 30,
        legal_hold,
    }
}

pub fn governance() -> ObjectLockConfig {
    lock(ObjectLockMode::Governance, false)
}

/// A file-source config on the mock backend, with its own tenant so the
/// counters a test reads are its own.
pub fn config(dir: &Path, tenant: &str, bucket: &str, input: &Path) -> VtopConfig {
    let mut cfg = file_config(
        dir.join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.join("state.db").display()),
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    cfg.partitioning.template = TEMPLATE.into();
    cfg.upload.bucket = bucket.into();
    cfg.engine.tenant = tenant.into();
    cfg
}

/// An engine with the profile set, over `backend`. Built WITHOUT the block
/// first, because `build_backend` refuses it for the mock backend (as it does
/// for every backend but s3_native), then given the block and the mock.
pub async fn engine(
    mut cfg: VtopConfig,
    lock: Option<ObjectLockConfig>,
    backend: &Arc<MockBackend>,
) -> Engine {
    // The registry, before the engine counts anything: without one the
    // engine counts nothing, by design.
    vtop_core::telemetry::init().unwrap();
    cfg.upload.object_lock = None;
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    if let Some(lock) = lock {
        engine.config.upload.require_object_versioning = true;
        engine.config.upload.object_lock = Some(lock);
    }
    engine.backend = backend.clone() as Arc<dyn UploadBackend>;
    engine
}

pub fn locked_mock(lock: &ObjectLockConfig) -> MockBackend {
    MockBackend::new().with_object_lock(ObjectLockPolicy::from_config(lock))
}

/// The operator log, captured as JSON lines.
#[derive(Clone, Default)]
pub struct Logs(Arc<Mutex<Vec<u8>>>);
pub struct LogWriter(Arc<Mutex<Vec<u8>>>);
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
    pub fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        let logs = Logs::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::INFO)
            .with_writer(logs.clone())
            .finish();
        (logs.clone(), tracing::subscriber::set_default(subscriber))
    }
    pub fn raw(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
    pub fn events(&self, event: &str) -> Vec<Value> {
        self.raw()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter_map(|v| v.get("fields").cloned())
            .filter(|f| f.get("event").and_then(Value::as_str) == Some(event))
            .collect()
    }
}

pub fn text(fields: &Value, name: &str) -> String {
    fields
        .get(name)
        .map(|v| {
            v.as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| v.to_string())
        })
        .unwrap_or_default()
}

pub fn checks(tenant: &str, result: &str) -> u64 {
    vtop_core::telemetry::init()
        .unwrap()
        .object_lock_checks_total
        .with_label_values(&[tenant, result])
        .get()
}

pub fn verification_failures(tenant: &str) -> u64 {
    vtop_core::telemetry::init()
        .unwrap()
        .verification_failures_total
        .with_label_values(&[tenant, "file", "raw"])
        .get()
}

pub async fn row(engine: &Engine, batch_id: &str) -> BatchRecord {
    engine.store.get_batch(batch_id).await.unwrap().unwrap()
}

pub async fn committed(engine: &Engine) -> Vec<BatchRecord> {
    let mut rows: Vec<BatchRecord> = engine
        .store
        .list_batches()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.state == BatchState::SourceCommitted)
        .collect();
    rows.sort_by(|a, b| a.batch_id.cmp(&b.batch_id));
    rows
}

/// The three parts of an operator message, each asserted.
pub fn assert_says(message: &str, parts: &[&str]) {
    for part in parts {
        assert!(message.contains(part), "{message:?} should say {part:?}");
    }
}

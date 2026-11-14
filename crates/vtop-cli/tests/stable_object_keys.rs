//! #626: a replayed range resolves to the object already archived for it.
//!
//! Every attempt at a batch used to name its object after the attempt's own
//! batch id, which embeds the wall clock and a random suffix, so a replay of a
//! range that was already uploaded wrote a second object. The object key is
//! now derived from the source, the range and the digest of the bytes, and an
//! object already at that key is reused only after it verifies against this
//! batch's digest. These tests pin:
//!
//! - a replay of an uploaded range writes no second object;
//! - a replay whose bytes differ, or that finds the key holding bytes that do
//!   not verify, never overwrites what is there;
//! - an archive written with the old per-attempt keys still recovers, scrubs
//!   and audits, next to objects written with the new ones.
//!
//! The partition template here has no time fields, so a test that crosses an
//! hour boundary does not see two partitions (the time partition is a
//! separate, documented limit of the scheme).
#![cfg(unix)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;
use vtop_core::errors::VtopError;
use vtop_upload::{HeadFault, ObjectHead, StoredManifest, StoredObject, VerificationResult};

use vtop_cli::scrub::{self, ScrubOptions, ScrubResult};
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::{StreamsConfig, VtopConfig};
use vtop_core::manifest::ManifestBuilder;
use vtop_core::state_machine::BatchState;
use vtop_core::types::{CompressionType, ProgressMarker, SourceType, TelemetryFormat};
use vtop_state::{BatchPatch, BatchRecord};
use vtop_upload::{MockBackend, ObjectChecksum, UploadBackend};

const TEMPLATE: &str = "tenant={tenant}/source={source}/format={format}/";

fn write_lines(path: &Path, tag: &str, lines: usize) {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    for i in 0..lines {
        writeln!(f, "{tag} record {i}").unwrap();
    }
}

fn config(dir: &Path, db: &str, inputs: &[PathBuf]) -> VtopConfig {
    let mut cfg = file_config(
        dir.join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.join(db).display()),
        inputs
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect(),
        "mock",
    );
    cfg.partitioning.template = TEMPLATE.into();
    cfg
}

async fn engine(cfg: VtopConfig, backend: &Arc<MockBackend>) -> Engine {
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    engine.backend = backend.clone() as Arc<dyn UploadBackend>;
    engine
}

/// The operator log, captured as JSON lines (#626): every key decision is an
/// event with an `event` name and the batch id, key and range as fields.
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
    /// Capture this thread's events until the guard drops. The tests run on
    /// the current-thread runtime, where the key decisions are logged.
    fn capture() -> (Self, tracing::subscriber::DefaultGuard) {
        let logs = Logs::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::INFO)
            .with_writer(logs.clone())
            .finish();
        (logs.clone(), tracing::subscriber::set_default(subscriber))
    }

    /// The fields of every event named `event`, oldest first.
    fn events(&self, event: &str) -> Vec<Value> {
        let bytes = self.0.lock().unwrap().clone();
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter_map(|v| v.get("fields").cloned())
            .filter(|f| f.get("event").and_then(Value::as_str) == Some(event))
            .collect()
    }

    /// The one event named `event` for `batch_id`.
    fn one(&self, event: &str, batch_id: &str) -> Value {
        let found: Vec<Value> = self
            .events(event)
            .into_iter()
            .filter(|f| text(f, "batch_id") == batch_id)
            .collect();
        assert_eq!(
            found.len(),
            1,
            "one {event} for {batch_id}, found {found:?}; all events: {}",
            String::from_utf8_lossy(&self.0.lock().unwrap())
        );
        found[0].clone()
    }
}

fn text(fields: &Value, name: &str) -> String {
    fields
        .get(name)
        .map(|v| {
            v.as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| v.to_string())
        })
        .unwrap_or_default()
}

/// Assert an event names its batch, key and range, and says `says`.
fn assert_event(fields: &Value, key: &str, range: &str, says: &[&str]) {
    assert_eq!(text(fields, "key"), key, "{fields}");
    assert_eq!(text(fields, "range"), range, "{fields}");
    assert!(!text(fields, "source").is_empty(), "{fields}");
    let message = text(fields, "message");
    for part in says {
        assert!(message.contains(part), "{message:?} should say {part:?}");
    }
}

fn file_range(input: &Path) -> String {
    format!("b0-{}", std::fs::metadata(input).unwrap().len())
}

/// Whether the store holds a second version at `uri`: a second put to it.
async fn put_twice(backend: &MockBackend, uri: &str) -> bool {
    backend.get_object_pinned(uri, "v2", 1 << 20).await.is_ok()
}

async fn committed(engine: &Engine) -> Vec<BatchRecord> {
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

/// The in-process replay: the object put lands, the manifest put is
/// throttled, the batch fails and its range is handed back. The re-read holds
/// the same bytes, so it must find the object the failed attempt left and
/// write no second one.
#[tokio::test]
async fn a_replay_of_an_uploaded_range_writes_no_second_object() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let mut engine = engine(
        config(dir.path(), "state.db", std::slice::from_ref(&input)),
        &backend,
    )
    .await;
    let (logs, _guard) = Logs::capture();
    let range = file_range(&input);

    let first = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(first.len(), 1, "{first:?}");
    assert!(
        !first[0].committed,
        "the throttled manifest fails the batch"
    );
    let failed = engine
        .store
        .get_batch(&first[0].batch_id)
        .await
        .unwrap()
        .unwrap();
    let uploaded = failed
        .object_uri
        .clone()
        .expect("the failed attempt recorded the object it put");
    assert!(backend.contains(&uploaded));

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(second.len(), 1, "{second:?}");
    assert!(second[0].committed, "{second:?}");
    assert_ne!(
        second[0].batch_id, first[0].batch_id,
        "each attempt keeps its own ledger row"
    );
    assert_eq!(
        second[0].object_uri.as_deref(),
        Some(uploaded.as_str()),
        "the replay resolved to the object the failed attempt uploaded"
    );
    assert!(
        !put_twice(&backend, &uploaded).await,
        "the replay reused {uploaded} without putting it again"
    );
    assert_event(
        &logs.one("object_key_derived", &first[0].batch_id),
        &uploaded,
        &range,
        &["no object for range", "uploading it under its derived key"],
    );
    assert_event(
        &logs.one("object_key_reused", &second[0].batch_id),
        &uploaded,
        &range,
        &["already archived and verified; reusing key", "no upload"],
    );

    // The committed row scrubs clean against the reused object.
    let rows = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
    let report = engine.scrub(rows, &ScrubOptions::default()).await.unwrap();
    assert!(report.passed, "{report:#?}");
    assert_eq!(
        report.results.get(ScrubResult::Ok.as_str()).copied(),
        Some(1),
        "{report:#?}"
    );
}

/// An object may only be shared by attempts at ONE unit and range of ONE
/// engine (review of #626): the object URI is what a reader dedupes on, so two
/// engines that archive byte-identical windows of their own files, or a new
/// file at the same path holding the same bytes, must not share an object, or
/// a reader that dedupes by object drops one of them.
#[tokio::test]
async fn identical_bytes_from_another_engine_or_another_file_get_their_own_object() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "banner", 3);
    let backend = Arc::new(MockBackend::new());

    let mut host_a = config(dir.path(), "a.db", std::slice::from_ref(&input));
    host_a.engine.name = "host-a".into();
    let mut a = engine(host_a, &backend).await;
    let out = a.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    let from_a = out[0].object_uri.clone().unwrap();

    // Another engine, its own ledger, the same bytes at the same path.
    let mut host_b = config(dir.path(), "b.db", std::slice::from_ref(&input));
    host_b.engine.name = "host-b".into();
    let mut b = engine(host_b, &backend).await;
    let out = b.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    let from_b = out[0].object_uri.clone().unwrap();
    assert_ne!(from_b, from_a, "another engine's batch is another object");

    // The same engine after its ledger is lost, with the path now naming a new
    // file (another inode) that holds the same bytes.
    let bytes = std::fs::read(&input).unwrap();
    let moved = dir.path().join("app.log.1");
    std::fs::rename(&input, &moved).unwrap();
    std::fs::write(&input, &bytes).unwrap();
    let mut host_a_again = config(dir.path(), "a2.db", std::slice::from_ref(&input));
    host_a_again.engine.name = "host-a".into();
    let mut a2 = engine(host_a_again, &backend).await;
    let out = a2.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    let from_new_file = out[0].object_uri.clone().unwrap();
    assert_ne!(from_new_file, from_a, "another file is another object");
    assert!(!put_twice(&backend, &from_a).await);
}

/// The same range read again with different bytes (the file was replaced by
/// one of the same length) is a different object: it gets its own key, and
/// the archived object keeps its bytes and its single version.
#[tokio::test]
async fn a_replay_with_different_bytes_gets_its_own_key_and_leaves_the_archived_object() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "aaa", 4);
    let backend = Arc::new(MockBackend::new());

    let mut a = engine(
        config(dir.path(), "a.db", std::slice::from_ref(&input)),
        &backend,
    )
    .await;
    let out = a.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    let archived = out[0].object_uri.clone().unwrap();
    let archived_bytes = backend.get_object(&archived).await.unwrap();

    // The ledger is lost (a fresh one) and the file now holds other bytes of
    // the same length at the same offsets.
    std::fs::remove_file(&input).unwrap();
    write_lines(&input, "bbb", 4);
    let mut b = engine(
        config(dir.path(), "b.db", std::slice::from_ref(&input)),
        &backend,
    )
    .await;
    let (logs, _guard) = Logs::capture();
    b.recover().await.unwrap();
    let out = b.process_once(SourceType::File).await.unwrap();
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(out[0].committed, "{out:?}");
    let replayed = out[0].object_uri.clone().unwrap();
    assert_event(
        &logs.one("object_key_derived", &out[0].batch_id),
        &replayed,
        &file_range(&input),
        &["uploading it under its derived key"],
    );
    assert_ne!(replayed, archived, "different bytes, a different key");
    assert_eq!(
        backend.get_object(&archived).await.unwrap(),
        archived_bytes,
        "the archived object keeps its bytes"
    );
    assert!(!put_twice(&backend, &archived).await);
}

/// The replay's key is occupied by bytes that do not verify against this
/// batch's digest (the archived object was damaged). Reusing it would commit
/// a range over bytes that are not its own, and overwriting it would destroy
/// the evidence a scrub reports. The batch is written under a distinct key and
/// the damaged object is left exactly as it was.
#[tokio::test]
async fn a_key_holding_bytes_that_do_not_verify_is_neither_reused_nor_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(MockBackend::new());

    let mut a = engine(
        config(dir.path(), "a.db", std::slice::from_ref(&input)),
        &backend,
    )
    .await;
    let out = a.process_once(SourceType::File).await.unwrap();
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    let archived = out[0].object_uri.clone().unwrap();
    backend.corrupt(&archived);
    let damaged = backend.get_object(&archived).await.unwrap();

    // The ledger is lost and the same bytes are read again: the derived key
    // is the damaged object's.
    let mut b = engine(
        config(dir.path(), "b.db", std::slice::from_ref(&input)),
        &backend,
    )
    .await;
    let (logs, _guard) = Logs::capture();
    b.recover().await.unwrap();
    let out = b.process_once(SourceType::File).await.unwrap();
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(out[0].committed, "{out:?}");
    let written = out[0].object_uri.clone().unwrap();
    let fallback = logs.one("object_key_fallback", &out[0].batch_id);
    assert_event(
        &fallback,
        &archived,
        &file_range(&input),
        &[
            "exists but its bytes differ from this batch",
            "instead of overwriting it",
            "vtopctl scrub",
        ],
    );
    assert_eq!(text(&fallback, "fallback_key"), written);
    assert!(!text(&fallback, "reason").is_empty());
    assert_ne!(
        written, archived,
        "an object that does not verify is not adopted"
    );
    assert_eq!(
        backend.get_object(&archived).await.unwrap(),
        damaged,
        "the damaged object is left for the scrub to report"
    );
    assert!(
        !put_twice(&backend, &archived).await,
        "nothing was written over {archived}"
    );
    let report = b
        .scrub(
            scrub::committed_rows(b.store.list_batches().await.unwrap(), Vec::new()),
            &ScrubOptions::default(),
        )
        .await
        .unwrap();
    assert!(report.passed, "B's own batch verifies: {report:#?}");
}

/// An archive written before #626, with a per-attempt object key
/// (`vtop-<stamp>-<source>-<range>-<random>`), is still recovered, scrubbed
/// and audited, and later batches of the same file are written beside it
/// under the derived keys without touching it.
#[tokio::test]
async fn an_archive_with_per_attempt_keys_still_recovers_scrubs_and_audits() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "old", 4);
    let len = std::fs::metadata(&input).unwrap().len();
    let path = input.to_string_lossy().into_owned();
    let backend = Arc::new(MockBackend::new());
    let mut engine = engine(
        config(dir.path(), "state.db", std::slice::from_ref(&input)),
        &backend,
    )
    .await;

    // The legacy batch: a VERIFIED row whose source commit was lost, its
    // object and manifest under the old per-attempt name.
    let batch_id = format!(
        "vtop-20250101T000000Z-{}-b0-{len}-1a2b3c4d",
        vtop_core::batch::sanitize_token(&path)
    );
    let prefix = format!("s3://telemetry-data/{TEMPLATE}");
    let object_uri = format!("{prefix}{batch_id}.raw");
    let manifest_uri = format!("{prefix}{batch_id}.manifest.json");
    let bytes = std::fs::read(&input).unwrap();
    let digest = vtop_core::checksum::sha256_bytes(&bytes);
    let marker = ProgressMarker::File {
        path: path.clone(),
        inode: Some(std::os::unix::fs::MetadataExt::ino(
            &std::fs::metadata(&input).unwrap(),
        )),
        start_byte: 0,
        end_byte: len,
        file_size: len,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    let manifest = ManifestBuilder {
        batch_id: batch_id.clone(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: path.clone(),
        format: TelemetryFormat::Raw,
        compression: CompressionType::None,
        record_count: 4,
        first_timestamp: None,
        last_timestamp: None,
        source_progress: marker.clone(),
        object_uri: object_uri.clone(),
        object_size: bytes.len() as u64,
        object_checksum_algorithm: "sha256".into(),
        object_checksum: digest.clone(),
        manifest_uri: manifest_uri.clone(),
        path_template: TEMPLATE.into(),
        resolved_prefix: "x".into(),
        upload_backend: "mock".into(),
        created_at: chrono::Utc::now().to_rfc3339(),
    }
    .build()
    .unwrap();
    let manifest_path = manifest.write_to_file(&work).unwrap();
    let staged = dir.path().join("legacy-object");
    std::fs::write(&staged, &bytes).unwrap();
    backend
        .put_object(
            &staged,
            &object_uri,
            Some(ObjectChecksum::new("sha256", &digest)),
        )
        .await
        .unwrap();
    let stored_manifest = backend
        .put_manifest(&manifest_path, &manifest_uri, None)
        .await
        .unwrap();
    let now = chrono::Utc::now().to_rfc3339();
    let row = BatchRecord {
        batch_id: batch_id.clone(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: path.clone(),
        format: TelemetryFormat::Raw,
        state: BatchState::Batching,
        progress_start: marker.clone(),
        progress_end: marker,
        object_uri: Some(object_uri.clone()),
        manifest_uri: Some(manifest_uri.clone()),
        object_sha256: Some(digest.clone()),
        manifest_sha256: Some(manifest.manifest.sha256.clone()),
        manifest_version_id: stored_manifest.version_id.clone(),
        object_size_bytes: Some(bytes.len() as i64),
        record_count: Some(4),
        error_message: None,
        owner: None,
        lease_expires_at: None,
        created_at: now.clone(),
        updated_at: now,
    };
    engine.store.save_batch_state(&row).await.unwrap();
    for st in [
        BatchState::Sealed,
        BatchState::Compressed,
        BatchState::Checksummed,
        BatchState::ObjectUploaded,
        BatchState::ManifestUploaded,
        BatchState::Verified,
    ] {
        engine
            .store
            .update_batch_state(&batch_id, st, &BatchPatch::default())
            .await
            .unwrap();
    }

    // Recovery re-verifies the legacy object and commits it.
    let summary = engine.recover().await.unwrap();
    assert_eq!(summary.committed, 1, "{summary:?}");

    // A later range of the same file is archived under a derived key.
    write_lines(&input, "new", 3);
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(out[0].committed, "{out:?}");
    let derived = out[0].object_uri.clone().unwrap();
    assert_ne!(derived, object_uri);
    assert!(!put_twice(&backend, &object_uri).await);

    let rows = committed(&engine).await;
    assert_eq!(rows.len(), 2, "{rows:?}");
    let report = engine
        .scrub(
            scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new()),
            &ScrubOptions::default(),
        )
        .await
        .unwrap();
    assert!(report.passed, "{report:#?}");
    assert_eq!(
        report.results.get(ScrubResult::Ok.as_str()).copied(),
        Some(2),
        "both the legacy and the derived object verify: {report:#?}"
    );

    let units = vtop_cli::coverage::audit(engine.store.list_batches().await.unwrap());
    assert_eq!(units.len(), 1, "{units:#?}");
    let unit = &units[0];
    assert!(!unit.has_findings(), "{unit:#?}");
    assert_eq!(
        unit.committed_through,
        std::fs::metadata(&input).unwrap().len()
    );
    let storage = vtop_cli::coverage::check_storage(backend.as_ref(), &rows).await;
    assert!(storage.is_empty(), "{storage:?}");
}

// ---- Every condition the key scheme touches (#626) ------------------------

/// Archive `input` once with `cfg` into `backend` and return the committed
/// batch's object.
async fn archive_once(cfg: VtopConfig, backend: &Arc<MockBackend>) -> String {
    let mut e = engine(cfg, backend).await;
    let out = e.process_once(SourceType::File).await.unwrap();
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(out[0].committed, "{out:?}");
    out[0].object_uri.clone().unwrap()
}

/// Different bytes from a different compression setting: the same records
/// re-read under another level are another object. The archived one is left
/// alone and the replay says it uploaded under a new derived key.
#[tokio::test]
async fn a_replay_compressed_differently_is_another_object() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 50);
    let backend = Arc::new(MockBackend::new());
    let mut a_cfg = config(dir.path(), "a.db", std::slice::from_ref(&input));
    a_cfg.compression.kind = CompressionType::Gzip;
    a_cfg.compression.level = 1;
    let archived = archive_once(a_cfg, &backend).await;

    let mut b_cfg = config(dir.path(), "b.db", std::slice::from_ref(&input));
    b_cfg.compression.kind = CompressionType::Zstd;
    b_cfg.compression.level = 19;
    let (logs, _guard) = Logs::capture();
    let mut b = engine(b_cfg, &backend).await;
    let out = b.process_once(SourceType::File).await.unwrap();
    assert!(out[0].committed, "{out:?}");
    let replayed = out[0].object_uri.clone().unwrap();
    assert_ne!(replayed, archived);
    assert!(!put_twice(&backend, &archived).await);
    assert_event(
        &logs.one("object_key_derived", &out[0].batch_id),
        &replayed,
        &file_range(&input),
        &["uploading it under its derived key"],
    );
}

/// Different bytes from a different batching boundary: the failed attempt
/// uploaded `[0, n)`, the file grew before the re-read, and the replay reads
/// `[0, m)`. That is another object (the documented limit); the failed
/// attempt's object is not touched.
#[tokio::test]
async fn a_replay_over_a_grown_source_is_another_object_and_overwrites_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let mut e = engine(
        config(dir.path(), "state.db", std::slice::from_ref(&input)),
        &backend,
    )
    .await;
    let (logs, _guard) = Logs::capture();
    let first = e.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed);
    let orphan = e
        .store
        .get_batch(&first[0].batch_id)
        .await
        .unwrap()
        .unwrap()
        .object_uri
        .unwrap();

    write_lines(&input, "more", 3);
    let second = e.process_once(SourceType::File).await.unwrap();
    assert_eq!(second.len(), 1, "{second:?}");
    assert!(second[0].committed, "{second:?}");
    let replayed = second[0].object_uri.clone().unwrap();
    assert_ne!(replayed, orphan, "another range, another key");
    assert!(backend.contains(&orphan) && !put_twice(&backend, &orphan).await);
    assert_event(
        &logs.one("object_key_derived", &second[0].batch_id),
        &replayed,
        &file_range(&input),
        &["uploading it under its derived key"],
    );
}

/// An object at the key that the backend can verify only by size is not
/// proven to hold this batch: neither reused nor overwritten.
#[tokio::test]
async fn a_size_only_object_at_the_key_is_neither_reused_nor_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(MockBackend::limited());
    let archived = archive_once(
        config(dir.path(), "a.db", std::slice::from_ref(&input)),
        &backend,
    )
    .await;

    let (logs, _guard) = Logs::capture();
    let mut b = engine(
        config(dir.path(), "b.db", std::slice::from_ref(&input)),
        &backend,
    )
    .await;
    let out = b.process_once(SourceType::File).await.unwrap();
    assert!(out[0].committed, "{out:?}");
    let written = out[0].object_uri.clone().unwrap();
    assert_ne!(written, archived);
    assert!(!put_twice(&backend, &archived).await);
    let fallback = logs.one("object_key_fallback", &out[0].batch_id);
    assert_event(
        &fallback,
        &archived,
        &file_range(&input),
        &["could confirm only its size", "instead of overwriting it"],
    );
    assert_eq!(text(&fallback, "fallback_key"), written);
}

/// A probe the store refuses (S3's 403 without list permission) says nothing
/// about the key: the batch goes under its attempt key, and the operator is
/// told what to check.
#[tokio::test]
async fn a_refused_probe_falls_back_to_the_attempt_key_and_says_why() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(MockBackend::new().with_head_fault(HeadFault::Refused));
    let (logs, _guard) = Logs::capture();
    let mut e = engine(
        config(dir.path(), "state.db", std::slice::from_ref(&input)),
        &backend,
    )
    .await;
    let out = e.process_once(SourceType::File).await.unwrap();
    assert!(out[0].committed, "{out:?}");
    let written = out[0].object_uri.clone().unwrap();
    assert!(
        written.contains(&out[0].batch_id),
        "the attempt key: {written}"
    );
    let fallback = logs.one("object_key_fallback", &out[0].batch_id);
    assert_eq!(text(&fallback, "fallback_key"), written);
    assert!(
        !backend.contains(&text(&fallback, "key")),
        "nothing at the derived key"
    );
    assert_event(
        &fallback,
        &text(&fallback, "key"),
        &file_range(&input),
        &["could not be checked", "403", "permissions"],
    );
}

/// A throttled probe fails the batch before anything is written; its range
/// is handed back, and the operator is told so.
#[tokio::test]
async fn a_throttled_probe_fails_the_batch_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(MockBackend::new().with_head_fault(HeadFault::Throttled));
    let (logs, _guard) = Logs::capture();
    let mut e = engine(
        config(dir.path(), "state.db", std::slice::from_ref(&input)),
        &backend,
    )
    .await;
    let out = e.process_once(SourceType::File).await.unwrap();
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(!out[0].committed, "{out:?}");
    let row = e.store.get_batch(&out[0].batch_id).await.unwrap().unwrap();
    assert!(row.object_uri.is_none(), "nothing was put: {row:?}");
    let throttled = logs.one("object_key_probe_throttled", &out[0].batch_id);
    assert!(!backend.contains(&text(&throttled, "key")));
    assert_event(
        &throttled,
        &text(&throttled, "key"),
        &file_range(&input),
        &["throttled", "nothing was written", "handed back for retry"],
    );
}

/// No checksum, no content to name the object by: per-attempt key, said so.
#[tokio::test]
async fn without_a_checksum_the_object_is_named_per_attempt_and_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(MockBackend::new());
    let mut cfg = config(dir.path(), "state.db", std::slice::from_ref(&input));
    cfg.checksum.algorithm = vtop_core::types::ChecksumAlgorithm::None;
    let (logs, _guard) = Logs::capture();
    let mut e = engine(cfg, &backend).await;
    let out = e.process_once(SourceType::File).await.unwrap();
    assert!(out[0].committed, "{out:?}");
    let written = out[0].object_uri.clone().unwrap();
    assert!(written.contains(&out[0].batch_id), "{written}");
    assert_event(
        &logs.one("object_key_per_attempt", &out[0].batch_id),
        &written,
        &file_range(&input),
        &[
            "per-attempt key",
            "checksums are disabled",
            "enable a checksum",
        ],
    );
}

/// A backend that cannot tell an absent key from a refused request (the
/// command backends: awscli, minio, s3cmd relay a tool's words) is never
/// probed and never written under a derived key.
struct NoAbsentAnswer {
    inner: Arc<MockBackend>,
    heads: AtomicUsize,
}

#[async_trait]
impl UploadBackend for NoAbsentAnswer {
    async fn put_object(
        &self,
        local_path: &Path,
        object_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredObject, VtopError> {
        self.inner
            .put_object(local_path, object_uri, checksum)
            .await
    }
    async fn put_manifest(
        &self,
        local_path: &Path,
        manifest_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredManifest, VtopError> {
        self.inner
            .put_manifest(local_path, manifest_uri, checksum)
            .await
    }
    async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError> {
        self.heads.fetch_add(1, Ordering::SeqCst);
        self.inner.head_object(object_uri).await
    }
    async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError> {
        self.inner.get_object(object_uri).await
    }
    async fn get_object_bounded(
        &self,
        object_uri: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, VtopError> {
        self.inner.get_object_bounded(object_uri, max_bytes).await
    }
    async fn verify_object(
        &self,
        object_uri: &str,
        expected_size: u64,
        expected: Option<ObjectChecksum<'_>>,
    ) -> Result<VerificationResult, VtopError> {
        self.inner
            .verify_object(object_uri, expected_size, expected)
            .await
    }
    async fn delete_object(&self, object_uri: &str) -> Result<(), VtopError> {
        self.inner.delete_object(object_uri).await
    }
    fn backend_name(&self) -> &'static str {
        "command"
    }
    fn supports_checksum_verification(&self) -> bool {
        true
    }
    fn supports_multipart(&self) -> bool {
        false
    }
}

#[tokio::test]
async fn a_backend_without_an_absent_answer_is_not_probed_and_names_per_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(NoAbsentAnswer {
        inner: Arc::new(MockBackend::new()),
        heads: AtomicUsize::new(0),
    });
    assert!(!backend.reports_absent_objects(), "the trait's default");
    let (logs, _guard) = Logs::capture();
    let mut e = Engine::new(
        config(dir.path(), "state.db", std::slice::from_ref(&input)),
        StreamsConfig { streams: vec![] },
    )
    .await
    .unwrap();
    e.backend = backend.clone() as Arc<dyn UploadBackend>;
    let out = e.process_once(SourceType::File).await.unwrap();
    assert!(out[0].committed, "{out:?}");
    let written = out[0].object_uri.clone().unwrap();
    assert!(written.contains(&out[0].batch_id), "{written}");
    assert_eq!(backend.heads.load(Ordering::SeqCst), 0, "no probe");
    assert_event(
        &logs.one("object_key_per_attempt", &out[0].batch_id),
        &written,
        &file_range(&input),
        &[
            "per-attempt key",
            "backend command cannot tell a missing key from a refused request",
        ],
    );
}

/// The hardened profile: a versioned bucket with every manifest pinned to its
/// stored version. A reused object adds no version, and the replay's row pins
/// its own manifest's version, which recovery's re-check and the scrub read.
#[tokio::test]
async fn under_versioning_a_reuse_adds_no_version_and_pins_its_own_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let mut cfg = config(dir.path(), "state.db", std::slice::from_ref(&input));
    cfg.upload.require_object_versioning = true;
    let (logs, _guard) = Logs::capture();
    let mut e = engine(cfg, &backend).await;
    let first = e.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed);
    let second = e.process_once(SourceType::File).await.unwrap();
    assert!(second[0].committed, "{second:?}");
    let row = e
        .store
        .get_batch(&second[0].batch_id)
        .await
        .unwrap()
        .unwrap();
    let object = row.object_uri.clone().unwrap();
    assert_event(
        &logs.one("object_key_reused", &second[0].batch_id),
        &object,
        &file_range(&input),
        &["no upload"],
    );
    assert!(!put_twice(&backend, &object).await, "no second version");
    let pinned = row.manifest_version_id.clone().expect("a pinned manifest");
    let manifest = backend
        .get_manifest_pinned(row.manifest_uri.as_deref().unwrap(), &pinned, 1 << 20)
        .await
        .unwrap();
    let manifest: vtop_core::manifest::VtopManifest = serde_json::from_slice(&manifest).unwrap();
    assert_eq!(manifest.batch_id, second[0].batch_id, "its own manifest");
    assert_eq!(manifest.object.uri, object);
    let report = e
        .scrub(
            scrub::committed_rows(e.store.list_batches().await.unwrap(), Vec::new()),
            &ScrubOptions::default(),
        )
        .await
        .unwrap();
    assert!(report.passed, "{report:#?}");
}

/// Multipart and a single PUT name the object the same way: an attempt
/// uploaded through multipart is reused by the replay, which uploads no part.
#[tokio::test]
async fn a_multipart_upload_is_reused_like_a_single_put() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 200);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let mut cfg = config(dir.path(), "state.db", std::slice::from_ref(&input));
    cfg.compression.kind = CompressionType::None;
    cfg.upload.multipart_threshold_bytes = 1;
    cfg.upload.multipart_part_size_bytes = 1024;
    let (logs, _guard) = Logs::capture();
    let mut e = engine(cfg, &backend).await;
    let first = e.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed);
    let parts = backend.multipart_parts_uploaded();
    assert!(
        parts > 1,
        "the first attempt went through multipart: {parts}"
    );
    let second = e.process_once(SourceType::File).await.unwrap();
    assert!(second[0].committed, "{second:?}");
    assert_eq!(
        backend.multipart_parts_uploaded(),
        parts,
        "the replay uploaded no part"
    );
    let object = second[0].object_uri.clone().unwrap();
    assert_event(
        &logs.one("object_key_derived", &first[0].batch_id),
        &object,
        &file_range(&input),
        &["uploading it under its derived key"],
    );
    assert_event(
        &logs.one("object_key_reused", &second[0].batch_id),
        &object,
        &file_range(&input),
        &["no upload"],
    );
}

/// The localfs backend: the same conditions on disk. A replay reuses the
/// stored file; a file at the key holding other bytes is neither reused nor
/// overwritten.
#[tokio::test]
async fn localfs_reuses_a_verified_file_and_never_overwrites_a_damaged_one() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let localfs = |db: &str| {
        let mut cfg = config(dir.path(), db, std::slice::from_ref(&input));
        cfg.upload.backend = "localfs".into();
        cfg.upload.local_path = Some(root.to_string_lossy().into_owned());
        cfg
    };
    let (logs, _guard) = Logs::capture();
    let run = |db: &'static str| {
        let cfg = localfs(db);
        async move {
            let mut e = Engine::new(cfg, StreamsConfig { streams: vec![] })
                .await
                .unwrap();
            assert_eq!(e.backend.backend_name(), "localfs");
            let out = e.process_once(SourceType::File).await.unwrap();
            assert!(out[0].committed, "{out:?}");
            (out[0].batch_id.clone(), out[0].object_uri.clone().unwrap())
        }
    };
    let (_, archived) = run("a.db").await;
    let (reuse_id, reused) = run("b.db").await;
    assert_eq!(reused, archived, "the ledger was lost; the file is reused");
    assert_event(
        &logs.one("object_key_reused", &reuse_id),
        &archived,
        &file_range(&input),
        &["no upload"],
    );

    // Damage the stored file (same size, other bytes) and replay again.
    let (bucket, key) = archived
        .strip_prefix("s3://")
        .unwrap()
        .split_once('/')
        .unwrap();
    let on_disk = root.join(bucket).join(key);
    let mut bytes = std::fs::read(&on_disk).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(&on_disk, &bytes).unwrap();
    let (fallback_id, written) = run("c.db").await;
    assert_ne!(written, archived);
    assert_eq!(std::fs::read(&on_disk).unwrap(), bytes, "left as it was");
    let fallback = logs.one("object_key_fallback", &fallback_id);
    assert_eq!(text(&fallback, "fallback_key"), written);
    assert_event(
        &fallback,
        &archived,
        &file_range(&input),
        &["bytes differ from this batch", "instead of overwriting it"],
    );
    let leftovers: Vec<_> = std::fs::read_dir(on_disk.parent().unwrap())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".partial"))
        .collect();
    assert!(leftovers.is_empty(), "no staging file left: {leftovers:?}");
}

/// Two engines sharing one ledger, each reading its own copy of a file at the
/// same path (a file is one host's unit). Neither adopts nor overwrites the
/// other's object, even with identical bytes; each says what it did.
#[tokio::test]
async fn two_engines_sharing_a_ledger_keep_their_own_file_objects() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let shared = |name: &str, work: &str| {
        let mut cfg = config(dir.path(), "shared.db", std::slice::from_ref(&input));
        cfg.engine.name = name.into();
        cfg.engine.work_dir = dir.path().join(work).to_string_lossy().into_owned();
        cfg
    };
    let (logs, _guard) = Logs::capture();
    let mut a = engine(shared("host-a", "work-a"), &backend).await;
    let first = a.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed, "A's manifest put was throttled");
    let a_object = a
        .store
        .get_batch(&first[0].batch_id)
        .await
        .unwrap()
        .unwrap()
        .object_uri
        .unwrap();

    let mut b = engine(shared("host-b", "work-b"), &backend).await;
    let out = b.process_once(SourceType::File).await.unwrap();
    assert!(out[0].committed, "{out:?}");
    let b_object = out[0].object_uri.clone().unwrap();
    assert_ne!(b_object, a_object, "another engine's file, another object");
    assert!(!put_twice(&backend, &a_object).await);
    assert_event(
        &logs.one("object_key_derived", &out[0].batch_id),
        &b_object,
        &file_range(&input),
        &["uploading it under its derived key"],
    );
    let rows = b.store.list_batches().await.unwrap();
    assert!(
        rows.iter().any(|r| r.batch_id == first[0].batch_id),
        "one ledger"
    );
}

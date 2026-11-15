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
            .filter_map(|v| {
                let mut fields = v.get("fields").cloned()?;
                fields["level"] = v["level"].clone();
                Some(fields)
            })
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

/// The count of `decision` for file batches of `tenant` (#626). A test that
/// asserts on it uses a tenant of its own, so the parallel tests of this
/// binary never add to the series it reads and it can assert exact deltas.
fn decisions(tenant: &str, decision: &str) -> u64 {
    vtop_core::telemetry::init()
        .unwrap()
        .object_key_decisions_total
        .with_label_values(&[tenant, "file", "raw", decision])
        .get()
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
    let tenant = "counted-626";
    let mut cfg = config(dir.path(), "state.db", std::slice::from_ref(&input));
    cfg.engine.tenant = tenant.into();
    let mut engine = engine(cfg, &backend).await;
    let (logs, _guard) = Logs::capture();
    let range = file_range(&input);
    let count = |decision| decisions(tenant, decision);
    let (derived_before, reused_before) = (count("derived"), count("reused"));

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
        &[
            "already archived and verified; reusing key",
            "no upload",
            "nothing to do",
        ],
    );
    assert_eq!(
        count("derived"),
        derived_before + 1,
        "the first attempt is counted once"
    );
    assert_eq!(
        count("reused"),
        reused_before + 1,
        "the reuse is counted once"
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
        // A row written before #664 recorded no verification class.
        verification_class: None,
        object_size_bytes: Some(bytes.len() as i64),
        sealed_at: None,
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

/// #626: the failed attempt uploaded `[0, n)`, the manifest put was
/// throttled, and the file grew before the re-read. The re-read stops at `n`
/// (the cut the failed window recorded), so it rebuilds exactly the bytes the
/// failed attempt uploaded and resolves to that object; the appended lines
/// are the next window. Before, the replay read `[0, m)`, named another
/// object, and the failed attempt's object stayed as an orphan.
#[tokio::test]
async fn a_replay_over_a_grown_source_resolves_to_the_archived_object() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let n = std::fs::metadata(&input).unwrap().len();
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let tenant = "grown-626";
    let mut cfg = config(dir.path(), "state.db", std::slice::from_ref(&input));
    cfg.engine.tenant = tenant.into();
    let mut e = engine(cfg, &backend).await;
    let (logs, _guard) = Logs::capture();
    let exact_before = rebuilds(tenant, "exact");
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
    assert_eq!(
        second.len(),
        2,
        "the rebuilt window, then the new one: {second:?}"
    );
    assert!(second.iter().all(|o| o.committed), "{second:?}");
    assert_eq!(
        second[0].object_uri.as_deref(),
        Some(orphan.as_str()),
        "the replay resolves to the object the failed attempt uploaded"
    );
    assert!(
        !put_twice(&backend, &orphan).await,
        "and does not put it again"
    );
    assert_ne!(second[1].object_uri.as_deref(), Some(orphan.as_str()));
    assert_eq!(rebuilds(tenant, "exact"), exact_before + 1);
    let len = std::fs::metadata(&input).unwrap().len();
    let mut ranges: Vec<String> = committed(&e)
        .await
        .iter()
        .map(|r| {
            vtop_core::types::ProgressMarker::spanning(&r.progress_start, &r.progress_end)
                .range_token()
        })
        .collect();
    ranges.sort();
    assert_eq!(
        ranges,
        vec![format!("b0-{n}"), format!("b{n}-{len}")],
        "the ranges tile"
    );
    let rebuilt = logs.one("replay_rebuilt", &second[0].batch_id);
    assert_event(
        &rebuilt,
        &orphan,
        &format!("b0-{n}"),
        &[
            "rebuilt exactly as first cut (4 records)",
            "names the object the earlier attempt uploaded",
        ],
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
        &["could not be checked", "403", "grant list permission"],
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

// ---- A replay rebuilds the window an earlier attempt sealed (#626) --------

/// The count of `result` on `vtop_replay_rebuilds_total` for file batches of
/// `tenant`; a test reading it uses a tenant of its own, as for `decisions`.
fn rebuilds(tenant: &str, result: &str) -> u64 {
    vtop_core::telemetry::init()
        .unwrap()
        .replay_rebuilds_total
        .with_label_values(&[tenant, "file", "raw", result])
        .get()
}

/// An instant, for a seal clock.
fn at(rfc3339: &str) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(rfc3339)
        .unwrap()
        .with_timezone(&chrono::Utc)
}

/// The first attempt is sealed in this hour; its replay in the next.
const SEALED: &str = "2026-03-04T05:59:59.900+00:00";
const REPLAYED: &str = "2026-03-04T06:00:30+00:00";

/// A config whose object keys carry the hour the batch was sealed in.
fn hourly_config(dir: &Path, db: &str, input: &Path, tenant: &str) -> VtopConfig {
    let mut cfg = config(dir, db, &[input.to_path_buf()]);
    cfg.partitioning.template = vtop_core::partitioning::DEFAULT_TEMPLATE.into();
    cfg.engine.tenant = tenant.into();
    cfg
}

/// #626: the object put lands at 05:59:59, the manifest put is throttled,
/// the batch fails, and its range is read again at 06:00:30. The replay is
/// sealed at the instant the first attempt was, so it is filed under hour 05
/// and names the key the failed attempt wrote: the object is reused, not
/// written again under hour 06. The replay says so, and is counted `exact`.
#[tokio::test]
async fn a_replay_sealed_in_a_later_hour_resolves_to_the_recorded_key() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let tenant = "later-hour-626";
    let mut engine = engine(
        hourly_config(dir.path(), "state.db", &input, tenant),
        &backend,
    )
    .await;
    let (logs, _guard) = Logs::capture();
    let (reused_before, exact_before) = (decisions(tenant, "reused"), rebuilds(tenant, "exact"));

    engine.set_seal_clock(|| at(SEALED));
    let first = engine.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed, "{first:?}");
    let failed = engine
        .store
        .get_batch(&first[0].batch_id)
        .await
        .unwrap()
        .unwrap();
    let orphan = failed.object_uri.clone().unwrap();
    assert!(orphan.contains("hour=05"), "{orphan}");
    assert_eq!(
        failed.sealed_at.as_deref(),
        Some(at(SEALED).to_rfc3339().as_str()),
        "the row records the instant the window was sealed"
    );

    engine.set_seal_clock(|| at(REPLAYED));
    let second = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(second.len(), 1, "{second:?}");
    assert!(second[0].committed, "{second:?}");
    assert_eq!(
        second[0].object_uri.as_deref(),
        Some(orphan.as_str()),
        "the replay in hour 06 names the key the failed attempt wrote in hour 05"
    );
    assert!(
        !put_twice(&backend, &orphan).await,
        "and does not put it again"
    );
    assert_eq!(decisions(tenant, "reused"), reused_before + 1);
    assert_eq!(rebuilds(tenant, "exact"), exact_before + 1);
    let replay = engine
        .store
        .get_batch(&second[0].batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        replay.sealed_at, failed.sealed_at,
        "the replay's own row keeps the first attempt's instant"
    );

    let rebuilt = logs.one("replay_rebuilt", &second[0].batch_id);
    assert_eq!(text(&rebuilt, "level"), "INFO");
    assert_eq!(text(&rebuilt, "earlier_batch_id"), first[0].batch_id);
    assert_event(
        &rebuilt,
        &orphan,
        &file_range(&input),
        &[
            &format!("failed earlier (batch {})", first[0].batch_id),
            "rebuilt exactly as first cut (4 records)",
            "sealed at the instant first recorded",
            "filed under the same hour",
            "names the object the earlier attempt uploaded, with the same bytes",
            "nothing to do",
        ],
    );
}

/// #626, after a restart: the window's seal instant is read back from its
/// ledger row, so the replay a restarted engine makes in a later hour also
/// names the key the failed attempt wrote.
#[tokio::test]
async fn a_replay_after_a_restart_in_a_later_hour_resolves_to_the_recorded_key() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let tenant = "later-hour-restart-626";
    let cfg = hourly_config(dir.path(), "state.db", &input, tenant);
    let orphan = {
        let mut first = engine(cfg.clone(), &backend).await;
        first.set_seal_clock(|| at(SEALED));
        let out = first.process_once(SourceType::File).await.unwrap();
        assert!(!out[0].committed, "{out:?}");
        first
            .store
            .get_batch(&out[0].batch_id)
            .await
            .unwrap()
            .unwrap()
            .object_uri
            .unwrap()
    };
    let exact_before = rebuilds(tenant, "exact");

    let mut restarted = engine(cfg, &backend).await;
    restarted.set_seal_clock(|| at(REPLAYED));
    restarted.recover().await.unwrap();
    let out = restarted.process_once(SourceType::File).await.unwrap();
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(out[0].committed, "{out:?}");
    assert_eq!(
        out[0].object_uri.as_deref(),
        Some(orphan.as_str()),
        "the restarted engine's replay in hour 06 names the hour-05 key"
    );
    assert!(!put_twice(&backend, &orphan).await);
    assert_eq!(rebuilds(tenant, "exact"), exact_before + 1);
}

/// #626 (D5): the range is rebuilt as first cut, but compression changed
/// between the attempts, so its bytes differ from the object the failed
/// attempt uploaded. The replay is written under its own key, the earlier
/// object is left as it is, and the operator is told what happened, what was
/// done and what to do; it is counted `bytes_differ`.
#[tokio::test]
async fn a_replay_whose_bytes_differ_from_the_uploaded_object_says_so_and_overwrites_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 50);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let tenant = "bytes-differ-626";
    let mut a_cfg = hourly_config(dir.path(), "state.db", &input, tenant);
    a_cfg.compression.kind = CompressionType::Gzip;
    a_cfg.compression.level = 1;
    let (first_id, orphan, recorded) = {
        let mut first = engine(a_cfg.clone(), &backend).await;
        let out = first.process_once(SourceType::File).await.unwrap();
        assert!(!out[0].committed, "{out:?}");
        let row = first
            .store
            .get_batch(&out[0].batch_id)
            .await
            .unwrap()
            .unwrap();
        (
            out[0].batch_id.clone(),
            row.object_uri.unwrap(),
            row.object_sha256.unwrap(),
        )
    };
    let differ_before = rebuilds(tenant, "bytes_differ");

    let mut b_cfg = a_cfg;
    b_cfg.compression.kind = CompressionType::Zstd;
    b_cfg.compression.level = 19;
    let (logs, _guard) = Logs::capture();
    let mut restarted = engine(b_cfg, &backend).await;
    restarted.recover().await.unwrap();
    let out = restarted.process_once(SourceType::File).await.unwrap();
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(out[0].committed, "{out:?}");
    let written = out[0].object_uri.clone().unwrap();
    assert_ne!(written, orphan, "other bytes, another key");
    assert!(
        backend.contains(&orphan),
        "the earlier object is left as it is"
    );
    assert!(!put_twice(&backend, &orphan).await, "and not written over");
    assert_eq!(rebuilds(tenant, "bytes_differ"), differ_before + 1);

    let differs = logs.one("replay_bytes_differ", &out[0].batch_id);
    assert_eq!(text(&differs, "level"), "WARN");
    assert_eq!(text(&differs, "earlier_batch_id"), first_id);
    assert_eq!(text(&differs, "recorded_digest"), recorded);
    assert_ne!(text(&differs, "digest"), recorded);
    assert_event(
        &differs,
        &written,
        &file_range(&input),
        &[
            "was rebuilt as first cut, but its bytes differ from the object the earlier attempt",
            "the earlier object is left untouched",
            "nothing is lost",
            "if compression, format or checksum settings changed since, nothing to do",
            "run `vtopctl scrub`",
        ],
    );
    assert!(
        logs.events("replay_rebuilt").is_empty(),
        "a rebuild whose bytes differ is not reported as exact"
    );
}

/// #626: the window is rebuilt exactly and sealed in its first hour, so it
/// names the failed attempt's key, but the object there no longer verifies
/// (damaged while the batch waited). The key decision falls back to the
/// attempt key, as it always has. The rebuild must say that, and be counted
/// `fallback`: reporting it `exact`, "names the same key", would claim a reuse
/// that did not happen.
#[tokio::test]
async fn a_rebuilt_window_whose_key_falls_back_is_counted_as_a_fallback_not_exact() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let tenant = "rebuilt-fallback-626";
    let mut engine = engine(
        hourly_config(dir.path(), "state.db", &input, tenant),
        &backend,
    )
    .await;
    let (logs, _guard) = Logs::capture();
    let (exact_before, fallback_before) = (rebuilds(tenant, "exact"), rebuilds(tenant, "fallback"));

    let first = engine.process_once(SourceType::File).await.unwrap();
    assert!(!first[0].committed, "{first:?}");
    let orphan = engine
        .store
        .get_batch(&first[0].batch_id)
        .await
        .unwrap()
        .unwrap()
        .object_uri
        .unwrap();
    backend.corrupt(&orphan);

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(second[0].committed, "{second:?}");
    let written = second[0].object_uri.clone().unwrap();
    assert_ne!(written, orphan, "the damaged object is not reused");
    assert!(
        written.contains(&second[0].batch_id),
        "the attempt key: {written}"
    );
    assert_eq!(
        rebuilds(tenant, "exact"),
        exact_before,
        "not an exact rebuild"
    );
    assert_eq!(rebuilds(tenant, "fallback"), fallback_before + 1);
    assert!(
        logs.events("replay_rebuilt").is_empty(),
        "no claim that the same key was named"
    );
    let fallback = logs.one("replay_key_fallback", &second[0].batch_id);
    assert_eq!(text(&fallback, "level"), "INFO");
    assert_event(
        &fallback,
        &written,
        &file_range(&input),
        &[
            "was rebuilt exactly as first cut",
            "its derived key could not be used",
            &format!("written under {written}"),
            "object_key_fallback",
            "nothing is lost",
        ],
    );
}

// ---- Bounded replay for file sources (#626) --------------------------------

/// The committed ranges of a file source, as `(start, end)`, in file order.
async fn committed_ranges(engine: &Engine) -> Vec<(u64, u64)> {
    let mut ranges: Vec<(u64, u64)> = committed(engine)
        .await
        .iter()
        .map(|r| match (&r.progress_start, &r.progress_end) {
            (ProgressMarker::File { start_byte, .. }, ProgressMarker::File { end_byte, .. }) => {
                (*start_byte, *end_byte)
            }
            other => panic!("file markers: {other:?}"),
        })
        .collect();
    ranges.sort_unstable();
    ranges
}

/// #626, after a restart: two adjacent windows failed after their objects
/// landed, and the file grew while the engine was down. Recovery hands both
/// back and each re-read stops at its window's recorded end, so both are
/// rebuilt exactly and reuse their objects; only the appended lines are new.
#[tokio::test]
async fn two_failed_windows_are_each_rebuilt_after_a_restart_and_reuse_their_objects() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    // Three manifest puts fail: window A, A's rebuild, and window B.
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(3));
    let tenant = "two-windows-626";
    let mut cfg = config(dir.path(), "state.db", std::slice::from_ref(&input));
    cfg.engine.tenant = tenant.into();
    cfg.batching.max_records = 2;
    let orphans: Vec<String> = {
        let mut first = engine(cfg.clone(), &backend).await;
        // A: two lines (the pass budget), fails.
        let a = first.process_once(SourceType::File).await.unwrap();
        assert_eq!(a.len(), 1, "{a:?}");
        // A pass of four lines rebuilds A up to its cut and reads B on after
        // it, as its own window; both fail.
        first.config.batching.max_records = 4;
        let out = first.process_once(SourceType::File).await.unwrap();
        assert_eq!(out.len(), 2, "A rebuilt, then B: {out:?}");
        assert!(out.iter().all(|o| !o.committed), "{out:?}");
        let mut uris = Vec::new();
        for o in &out {
            let row = first.store.get_batch(&o.batch_id).await.unwrap().unwrap();
            uris.push(row.object_uri.unwrap());
        }
        uris
    };
    cfg.batching.max_records = 4;
    write_lines(&input, "more", 1);
    let exact_before = rebuilds(tenant, "exact");

    let mut restarted = engine(cfg, &backend).await;
    let summary = restarted.recover().await.unwrap();
    assert_eq!(
        summary.replay_required, 3,
        "A, its rebuild, and B: {summary:?}"
    );
    let mut out = restarted.process_once(SourceType::File).await.unwrap();
    out.extend(restarted.process_once(SourceType::File).await.unwrap());
    assert!(out.iter().all(|o| o.committed), "{out:?}");
    let reused: Vec<_> = out.iter().filter_map(|o| o.object_uri.clone()).collect();
    for orphan in &orphans {
        assert!(reused.contains(orphan), "{orphan} reused: {reused:?}");
        assert!(!put_twice(&backend, orphan).await, "{orphan} not put again");
    }
    assert_eq!(rebuilds(tenant, "exact"), exact_before + 2);
    let ranges = committed_ranges(&restarted).await;
    let len = std::fs::metadata(&input).unwrap().len();
    let mut next = 0;
    for (start, end) in &ranges {
        assert_eq!(*start, next, "a gap or a repeat: {ranges:?}");
        next = *end;
    }
    assert_eq!(next, len, "{ranges:?}");
    assert_eq!(
        ranges.len(),
        3,
        "the two rebuilt windows and the new line: {ranges:?}"
    );
}

/// #626: the file was rewritten under the failed range at the same inode, so
/// the re-read cannot be cut where the failed window ended. It is read as
/// before, the bound is reported refused (three parts, counted), nothing is
/// overwritten, and the archive has no gap.
#[tokio::test]
async fn a_range_rewritten_in_place_is_refused_as_a_bound_and_read_as_before() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let tenant = "rewritten-626";
    let mut cfg = config(dir.path(), "state.db", std::slice::from_ref(&input));
    cfg.engine.tenant = tenant.into();
    let mut e = engine(cfg, &backend).await;
    let (logs, _guard) = Logs::capture();
    let refused_before = rebuilds(tenant, "refused");
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

    // Same inode, other bytes, longer: a writer that rewrote the file.
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&input)
        .unwrap();
    for i in 0..6 {
        writeln!(f, "REWRITTEN line {i}").unwrap();
    }
    drop(f);
    let second = e.process_once(SourceType::File).await.unwrap();
    assert!(second.iter().all(|o| o.committed), "{second:?}");
    assert!(
        !put_twice(&backend, &orphan).await,
        "nothing is overwritten"
    );
    assert_eq!(rebuilds(tenant, "refused"), refused_before + 1);
    let refused = logs.events("replay_bound_refused");
    assert_eq!(refused.len(), 1, "{refused:?}");
    let message = text(&refused[0], "message");
    for part in [
        "cannot be rebuilt as first cut",
        "it is read from its start to the current end as before",
        "nothing is lost",
        "run `vtopctl audit-coverage`",
    ] {
        assert!(message.contains(part), "{message:?} should say {part:?}");
    }
    assert_eq!(text(&refused[0], "level"), "WARN");
    assert_eq!(text(&refused[0], "earlier_batch_id"), first[0].batch_id);
}

/// #626: the limits were lowered while the engine was down, so the window
/// recovery hands back is over 4x them. It is not held whole (`too_large`,
/// three parts), and its re-read runs as before, with no gap.
#[tokio::test]
async fn a_window_over_four_times_the_limits_is_not_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 50);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let tenant = "too-large-626";
    let mut cfg = config(dir.path(), "state.db", std::slice::from_ref(&input));
    cfg.engine.tenant = tenant.into();
    cfg.batching.max_records = 100;
    {
        let mut first = engine(cfg.clone(), &backend).await;
        let out = first.process_once(SourceType::File).await.unwrap();
        assert!(!out[0].committed);
    }
    let before = rebuilds(tenant, "too_large");
    cfg.batching.max_records = 10;
    let (logs, _guard) = Logs::capture();
    let mut e = engine(cfg, &backend).await;
    e.recover().await.unwrap();
    assert_eq!(rebuilds(tenant, "too_large"), before + 1);
    let event = logs.events("replay_bound_too_large");
    assert_eq!(event.len(), 1, "{event:?}");
    assert_eq!(text(&event[0], "level"), "WARN");
    let message = text(&event[0], "message");
    for part in [
        "50 records",
        "over 4x the current batching.max_records (10)",
        "memory stays bounded",
        "nothing is lost",
        "raise the limits back",
    ] {
        assert!(message.contains(part), "{message:?} should say {part:?}");
    }
    for _ in 0..6 {
        e.process_once(SourceType::File).await.unwrap();
    }
    let ranges = committed_ranges(&e).await;
    let len = std::fs::metadata(&input).unwrap().len();
    let mut next = 0;
    for (start, end) in &ranges {
        assert_eq!(*start, next, "{ranges:?}");
        next = *end;
    }
    assert_eq!(next, len, "{ranges:?}");
}

/// #626: a window larger than one read pass's budget is rebuilt across two
/// passes. A forced flush between them (`process_once`) must not seal it
/// short of its recorded end, which would name another object: it is sealed
/// once, at the end, and resolves to the failed attempt's object.
#[tokio::test]
async fn a_rebuild_that_takes_two_passes_is_sealed_once_at_its_recorded_end() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 20);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let tenant = "two-passes-626";
    let mut cfg = config(dir.path(), "state.db", std::slice::from_ref(&input));
    cfg.engine.tenant = tenant.into();
    cfg.batching.max_records = 20;
    let mut e = engine(cfg, &backend).await;
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
    // Each pass now reads ten lines: the rebuild needs two.
    e.config.batching.max_records = 10;
    let pass_1 = e.process_once(SourceType::File).await.unwrap();
    assert!(
        pass_1.is_empty(),
        "half a window is held, not flushed: {pass_1:?}"
    );
    let pass_2 = e.process_once(SourceType::File).await.unwrap();
    assert_eq!(pass_2.len(), 1, "{pass_2:?}");
    assert!(pass_2[0].committed, "{pass_2:?}");
    assert_eq!(pass_2[0].object_uri.as_deref(), Some(orphan.as_str()));
    assert!(!put_twice(&backend, &orphan).await);
    let len = std::fs::metadata(&input).unwrap().len();
    assert_eq!(committed_ranges(&e).await, vec![(0, len)]);
}

/// Review of #755: a rebuild is reported once it is VERIFIED, not when its key
/// is decided. During an outage every attempt decides a key and fails; counting
/// each as a `fallback` made the alert on second objects fire although nothing
/// was written.
#[tokio::test]
async fn a_rebuild_that_fails_before_verified_is_not_counted() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    // The probe is refused (fallback to the attempt key), and the first two
    // puts are throttled: the first attempt and the first rebuild fail.
    let backend = Arc::new(
        MockBackend::new()
            .with_head_fault(HeadFault::Refused)
            .with_throttled_puts(2),
    );
    let tenant = "rebuild-outage-626";
    let mut cfg = config(dir.path(), "state.db", std::slice::from_ref(&input));
    cfg.engine.tenant = tenant.into();
    let mut e = engine(cfg, &backend).await;
    let before = rebuilds(tenant, "fallback");
    for attempt in 0..2 {
        let out = e.process_once(SourceType::File).await.unwrap();
        assert!(!out[0].committed, "attempt {attempt}: {out:?}");
    }
    assert_eq!(
        rebuilds(tenant, "fallback"),
        before,
        "a rebuild that did not reach VERIFIED wrote nothing and is not counted"
    );
    let out = e.process_once(SourceType::File).await.unwrap();
    assert!(out[0].committed, "{out:?}");
    assert_eq!(rebuilds(tenant, "fallback"), before + 1);
}

/// Review of #755: a row that recorded no seal instant (one a v0.9.0 engine
/// wrote) is replayed in the hour it is read again, as before, and is not
/// reported as rebuilt in its first hour.
#[tokio::test]
async fn a_row_without_a_seal_instant_is_not_reported_as_rebuilt() {
    use sqlx::ConnectOptions;
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 4);
    let backend = Arc::new(MockBackend::new().with_throttled_manifest_puts(1));
    let tenant = "no-instant-626";
    let cfg = hourly_config(dir.path(), "state.db", &input, tenant);
    let orphan = {
        let mut first = engine(cfg.clone(), &backend).await;
        first.set_seal_clock(|| at(SEALED));
        let out = first.process_once(SourceType::File).await.unwrap();
        assert!(!out[0].committed, "{out:?}");
        first
            .store
            .get_batch(&out[0].batch_id)
            .await
            .unwrap()
            .unwrap()
            .object_uri
            .unwrap()
    };
    {
        let mut conn = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(dir.path().join("state.db"))
            .connect()
            .await
            .unwrap();
        sqlx::query("UPDATE batches SET sealed_at = NULL")
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let (logs, _guard) = Logs::capture();
    let exact_before = rebuilds(tenant, "exact");
    let mut restarted = engine(cfg, &backend).await;
    restarted.set_seal_clock(|| at(REPLAYED));
    restarted.recover().await.unwrap();
    let out = restarted.process_once(SourceType::File).await.unwrap();
    assert!(out[0].committed, "{out:?}");
    let written = out[0].object_uri.clone().unwrap();
    assert!(
        written.contains("hour=06"),
        "filed in the hour it is replayed in: {written}"
    );
    assert_ne!(written, orphan);
    assert_eq!(
        rebuilds(tenant, "exact"),
        exact_before,
        "no instant, no claim"
    );
    assert!(logs.events("replay_rebuilt").is_empty());
}

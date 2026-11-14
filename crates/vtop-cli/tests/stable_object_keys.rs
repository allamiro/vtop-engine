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
use std::sync::Arc;

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
    b.recover().await.unwrap();
    let out = b.process_once(SourceType::File).await.unwrap();
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(out[0].committed, "{out:?}");
    let replayed = out[0].object_uri.clone().unwrap();
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
    b.recover().await.unwrap();
    let out = b.process_once(SourceType::File).await.unwrap();
    assert_eq!(out.len(), 1, "{out:?}");
    assert!(out[0].committed, "{out:?}");
    let written = out[0].object_uri.clone().unwrap();
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

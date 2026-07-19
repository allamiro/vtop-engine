//! Integration test: crash before commit -> replay-safe recovery.
//!
//! Proves the core rule: when the engine crashes after VERIFIED but before
//! SOURCE_COMMITTED, source progress is NOT advanced and the batch remains
//! committable on recovery. Also proves a verification failure never commits.

use std::io::Write;
use std::sync::Arc;
use vtop_adapters::base::SourceAdapter;
use vtop_adapters::FileSource;
use vtop_cli::testkit::{file_config, pipeline, FailCommitAdapter};
use vtop_core::state_machine::BatchState;
use vtop_core::types::TelemetryFormat;
use vtop_state::{SqliteStateStore, StateStore};
use vtop_upload::MockBackend;

fn sample(dir: &std::path::Path) -> String {
    let input = dir.join("input.log");
    let mut f = std::fs::File::create(&input).unwrap();
    for i in 0..5 {
        writeln!(f, "record-{i}").unwrap();
    }
    input.to_string_lossy().into_owned()
}

#[tokio::test]
async fn crash_before_commit_is_replayable_then_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let path = sample(dir.path());
    let cfg = file_config(
        work.to_str().unwrap(),
        "sqlite::memory:",
        vec![path.clone()],
        "mock",
    );

    let store = SqliteStateStore::connect(&cfg.engine.state_store)
        .await
        .unwrap();
    let backend: Arc<dyn vtop_upload::UploadBackend> = Arc::new(MockBackend::new());

    // Adapter that fails the first commit (simulated crash before commit).
    let inner = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
    let mut adapter = FailCommitAdapter::new(inner, 1);

    let source = adapter.discover_sources().await.unwrap().pop().unwrap();
    let mut reads = adapter
        .read_batch_candidates(&source, 1000, 1 << 20, std::time::Duration::ZERO)
        .await
        .unwrap();
    // A file source is a single committable unit, so the Vec is always length 1
    // (only Kafka splits a read per partition). Assert it before indexing so a
    // regression that returns 0 or 2 fails loudly here.
    assert_eq!(reads.len(), 1);
    let read = reads.remove(0);
    assert_eq!(read.records.len(), 5);

    let outcome = pipeline(&store, backend.clone(), &cfg)
        .process(&mut adapter, &source, read, None)
        .await
        .unwrap();

    // Verified, but NOT committed — the commit was simulated to fail.
    assert_eq!(outcome.final_state, BatchState::Verified);
    assert!(
        !outcome.committed,
        "must not commit when source commit fails"
    );

    let rec = store.get_batch(&outcome.batch_id).await.unwrap().unwrap();
    assert_eq!(rec.state, BatchState::Verified);

    // ---- Recovery: a fresh read still sees the uncommitted data ----------
    // (source progress was never advanced).
    let read2 = adapter
        .read_batch_candidates(&source, 1000, 1 << 20, std::time::Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(read2.len(), 1);
    assert_eq!(
        read2[0].records.len(),
        0,
        "read head advanced, but commit point did not"
    );
    // Rewinding to the uncommitted start replays the same 5 records.
    adapter
        .replay_from_marker(&rec.progress_start)
        .await
        .unwrap();
    let replayed = adapter
        .read_batch_candidates(&source, 1000, 1 << 20, std::time::Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(replayed.len(), 1);
    assert_eq!(
        replayed[0].records.len(),
        5,
        "uncommitted data is replayable"
    );

    // Recovery action for VERIFIED is to retry the source commit; the second
    // commit attempt now succeeds.
    adapter.commit_progress(&rec.progress_end).await.unwrap();
    store.mark_source_committed(&rec.batch_id).await.unwrap();
    let rec2 = store.get_batch(&outcome.batch_id).await.unwrap().unwrap();
    assert_eq!(rec2.state, BatchState::SourceCommitted);
}

#[tokio::test]
async fn verification_failure_never_commits() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let path = sample(dir.path());
    let cfg = file_config(
        work.to_str().unwrap(),
        "sqlite::memory:",
        vec![path.clone()],
        "mock",
    );

    let store = SqliteStateStore::connect(&cfg.engine.state_store)
        .await
        .unwrap();
    // Backend that always fails verification.
    let backend: Arc<dyn vtop_upload::UploadBackend> = Arc::new(MockBackend::failing());

    let mut adapter = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
    let source = adapter.discover_sources().await.unwrap().pop().unwrap();
    let mut reads = adapter
        .read_batch_candidates(&source, 1000, 1 << 20, std::time::Duration::ZERO)
        .await
        .unwrap();
    // File source: exactly one committable unit per read.
    assert_eq!(reads.len(), 1);
    let read = reads.remove(0);

    let outcome = pipeline(&store, backend, &cfg)
        .process(&mut adapter, &source, read, None)
        .await
        .unwrap();

    assert_eq!(outcome.final_state, BatchState::Failed);
    assert!(!outcome.committed, "failed verification must never commit");

    // The source offset was never committed: the data is fully replayable.
    adapter
        .replay_from_marker(&vtop_core::types::ProgressMarker::File {
            path: path.clone(),
            inode: None,
            start_byte: 0,
            end_byte: 0,
            file_size: 0,
            mtime: String::new(),
        })
        .await
        .unwrap();
    let replay = adapter
        .read_batch_candidates(&source, 1000, 1 << 20, std::time::Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].records.len(), 5, "data replayable after failure");
}

/// #64: a same-size replacement must fail in the normal pipeline even when
/// uploader-controlled checksum metadata still advertises the expected hash.
#[tokio::test]
async fn metadata_preserving_content_replacement_never_commits() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work-content-attack");
    let path = sample(dir.path());
    let cfg = file_config(
        work.to_str().unwrap(),
        "sqlite::memory:",
        vec![path.clone()],
        "mock",
    );
    let store = SqliteStateStore::connect(&cfg.engine.state_store)
        .await
        .unwrap();
    let concrete = Arc::new(MockBackend::corrupting());
    let backend: Arc<dyn vtop_upload::UploadBackend> = concrete.clone();

    let mut adapter = FileSource::new(vec![path], TelemetryFormat::Raw, false);
    let source = adapter.discover_sources().await.unwrap().pop().unwrap();
    let mut reads = adapter
        .read_batch_candidates(&source, 1000, 1 << 20, std::time::Duration::ZERO)
        .await
        .unwrap();
    let outcome = pipeline(&store, backend, &cfg)
        .process(&mut adapter, &source, reads.remove(0), None)
        .await
        .unwrap();

    assert_eq!(outcome.final_state, BatchState::Failed);
    assert!(!outcome.committed);
    let row = store
        .get_batch(&outcome.batch_id)
        .await
        .unwrap()
        .expect("failed batch remains in ledger");
    let object_uri = row.object_uri.expect("object was uploaded before attack");
    let head = vtop_upload::UploadBackend::head_object(concrete.as_ref(), &object_uri)
        .await
        .unwrap();
    let stored = vtop_upload::UploadBackend::get_object(concrete.as_ref(), &object_uri)
        .await
        .unwrap();
    assert_eq!(head.size_bytes, Some(stored.len() as u64));
    assert!(
        head.etag.is_some(),
        "uploader checksum metadata remains present"
    );
    assert!(
        head.checksum_sha256.is_none(),
        "uploader metadata must not be exposed as a service-computed checksum"
    );
}

/// #64 migration contract: strong verification is the default behavior, while
/// an explicit false value preserves a compatibility path for size-only stores.
#[tokio::test]
async fn backend_limited_verification_requires_explicit_opt_out() {
    for (require_strong, expected_state) in [
        (true, BatchState::Failed),
        (false, BatchState::SourceCommitted),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work-limited");
        let path = sample(dir.path());
        let mut cfg = file_config(
            work.to_str().unwrap(),
            "sqlite::memory:",
            vec![path.clone()],
            "mock",
        );
        cfg.upload.require_strong_verification = require_strong;
        let store = SqliteStateStore::connect(&cfg.engine.state_store)
            .await
            .unwrap();
        let backend: Arc<dyn vtop_upload::UploadBackend> = Arc::new(MockBackend::limited());
        let mut adapter = FileSource::new(vec![path], TelemetryFormat::Raw, false);
        let source = adapter.discover_sources().await.unwrap().pop().unwrap();
        let mut reads = adapter
            .read_batch_candidates(&source, 1000, 1 << 20, std::time::Duration::ZERO)
            .await
            .unwrap();

        let outcome = pipeline(&store, backend, &cfg)
            .process(&mut adapter, &source, reads.remove(0), None)
            .await
            .unwrap();
        assert_eq!(outcome.final_state, expected_state);
        assert_eq!(outcome.committed, !require_strong);
    }
}

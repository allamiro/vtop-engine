//! Integration test: file source -> object storage (mock S3 / MinIO stand-in).
//!
//! Demonstrates the happy path:
//!   read file -> seal -> compress -> checksum -> upload object -> manifest ->
//!   verify -> commit. Each object gets a manifest that binds the source
//!   progress marker to the object SHA-256.
//!
//! The `mock` backend is an in-memory S3 stand-in so the test runs with no
//! external services. The same flow runs against real MinIO via docker-compose
//! (see README and docker-compose.yml).

use std::io::Write;
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::StreamsConfig;
use vtop_core::manifest::VtopManifest;
use vtop_core::state_machine::BatchState;
use vtop_core::types::SourceType;

#[tokio::test]
async fn file_source_archives_and_commits() {
    let dir = tempfile::tempdir().unwrap();
    let work_dir = dir.path().join("work");
    let input = dir.path().join("auth.cef.log");
    let state_db = dir.path().join("state.db");

    // Write a sample CEF-style log with 3 records.
    {
        let mut f = std::fs::File::create(&input).unwrap();
        for i in 0..3 {
            writeln!(
                f,
                "CEF:0|VTOP|Engine|1.0|100|Test Event {i}|3|src=10.0.0.{i}"
            )
            .unwrap();
        }
    }

    let cfg = file_config(
        work_dir.to_str().unwrap(),
        &format!("sqlite://{}", state_db.display()),
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );

    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();

    let outcomes = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(outcomes.len(), 1, "one batch expected");
    let o = &outcomes[0];
    assert!(o.committed, "batch must be committed after verification");
    assert_eq!(o.final_state, BatchState::SourceCommitted);
    assert_eq!(o.record_count, 3);
    let object_uri = o.object_uri.clone().expect("object uri set");
    assert!(object_uri.ends_with(".cef.gz") || object_uri.ends_with(".raw.gz"));

    // A manifest must exist on disk and bind the source progress marker.
    let manifest_path = work_dir.join(format!("{}.manifest.json", o.batch_id));
    let bytes = std::fs::read(&manifest_path).expect("manifest written");
    let manifest: VtopManifest = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(manifest.protocol, "VTOP");
    assert_eq!(manifest.record_count, 3);
    assert!(
        !manifest.object.checksum.is_empty(),
        "object checksum present"
    );
    assert_eq!(manifest.object.checksum_algorithm, "sha256");
    manifest
        .verify_self_hash()
        .expect("manifest self-hash verifies");
    // The manifest binds source progress (file byte range) to the object.
    match manifest.source_progress {
        vtop_core::types::ProgressMarker::File { end_byte, .. } => {
            assert!(end_byte > 0, "file end byte recorded in manifest");
        }
        _ => panic!("expected a file progress marker"),
    }

    // State store reflects exactly one committed batch.
    let batches = engine.store.list_batches().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].state, BatchState::SourceCommitted);
    assert!(batches[0].object_uri.is_some());
    assert!(batches[0].manifest_uri.is_some());

    // Re-running finds nothing new (offset was committed).
    let again = engine.process_once(SourceType::File).await.unwrap();
    assert!(again.is_empty(), "committed data must not be reprocessed");
}

/// #68: `verify-manifest` must verify CONTENT, not existence. A corrupted
/// object whose size and metadata still match is exactly what a HEAD-only
/// check waves through — deep verification must catch it.
#[tokio::test]
async fn verify_manifest_checks_content_not_just_existence() {
    use std::sync::Arc;
    use vtop_adapters::base::SourceAdapter;
    use vtop_adapters::FileSource;
    use vtop_cli::commands::verify_manifest_deep;
    use vtop_cli::testkit::pipeline;
    use vtop_core::types::TelemetryFormat;
    use vtop_state::{SqliteStateStore, StateStore};
    use vtop_upload::MockBackend;

    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("app.log");
    {
        let mut f = std::fs::File::create(&input).unwrap();
        for i in 0..4 {
            writeln!(f, "record-{i}").unwrap();
        }
    }
    let cfg = file_config(
        work.to_str().unwrap(),
        "sqlite::memory:",
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    let store = SqliteStateStore::connect(&cfg.engine.state_store)
        .await
        .unwrap();
    // Keep a concrete handle so the test can corrupt stored content later.
    let mock = Arc::new(MockBackend::new());
    let backend: Arc<dyn vtop_upload::UploadBackend> = mock.clone();

    let mut adapter = FileSource::new(
        vec![input.to_string_lossy().into_owned()],
        TelemetryFormat::Raw,
        false,
    );
    let source = adapter.discover_sources().await.unwrap().pop().unwrap();
    let mut reads = adapter
        .read_batch_candidates(&source, 1000, 1 << 20, std::time::Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(reads.len(), 1);
    let outcome = pipeline(&store, backend.clone(), &cfg)
        .process(&mut adapter, &source, reads.remove(0), None)
        .await
        .unwrap();
    assert!(outcome.committed, "precondition: batch committed");

    let row = store.get_batch(&outcome.batch_id).await.unwrap().unwrap();
    let manifest_uri = row.manifest_uri.clone().expect("manifest uri recorded");
    let object_uri = row.object_uri.clone().expect("object uri recorded");

    // Intact store: every check passes.
    let report = verify_manifest_deep(&store, backend.as_ref(), &manifest_uri)
        .await
        .expect("verification runs");
    assert!(
        report.passed,
        "intact object must verify: {:?}",
        report.lines
    );

    // Corrupt ONE byte of the stored object, leaving size and recorded
    // metadata identical. A HEAD-based check still passes; content must fail.
    mock.corrupt(&object_uri);
    let report = verify_manifest_deep(&store, backend.as_ref(), &manifest_uri)
        .await
        .expect("verification still runs");
    assert!(
        !report.passed,
        "corrupted content MUST fail deep verification: {:?}",
        report.lines
    );
    assert!(
        report
            .lines
            .iter()
            .any(|l| l.contains("object content") && l.contains("FAILED")),
        "the failure must be attributed to object content: {:?}",
        report.lines
    );
}

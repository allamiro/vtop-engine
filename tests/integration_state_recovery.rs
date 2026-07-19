//! Integration test: state survives "restart" and recovery commits a
//! verified-but-uncommitted batch.
//!
//! The state store is the durable journal. After a simulated restart (reopen
//! the same SQLite file) the engine's recovery scan finds the VERIFIED batch
//! and commits it — never advancing source progress for unverified batches.

use std::io::Write;
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::StreamsConfig;
use vtop_core::state_machine::BatchState;
use vtop_core::types::SourceType;
use vtop_state::{BatchPatch, BatchRecord, SqliteStateStore, StateStore};

#[tokio::test]
async fn state_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("state.db");
    let url = format!("sqlite://{}", db.display());

    let marker = vtop_core::types::ProgressMarker::Kafka {
        topic: "app_events".into(),
        partition: 0,
        start_offset: 0,
        end_offset: 9,
        consumer_group: "vtop-engine".into(),
    };
    let now = chrono::Utc::now().to_rfc3339();
    let rec = BatchRecord {
        batch_id: "b-survive".into(),
        tenant: "default".into(),
        source_type: SourceType::Kafka,
        source_name: "app_events".into(),
        format: vtop_core::types::TelemetryFormat::Cef,
        state: BatchState::Batching,
        progress_start: marker.clone(),
        progress_end: marker,
        object_uri: None,
        manifest_uri: None,
        object_sha256: None,
        manifest_sha256: None,
        record_count: Some(10),
        error_message: None,
        owner: None,
        lease_expires_at: None,
        created_at: now.clone(),
        updated_at: now,
    };

    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        store.save_batch_state(&rec).await.unwrap();
        // Walk to VERIFIED but stop before committing (simulated crash).
        for st in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
            BatchState::Verified,
        ] {
            store
                .update_batch_state("b-survive", st, &BatchPatch::default())
                .await
                .unwrap();
        }
    } // store (and its pool) dropped — simulates process exit

    // Reopen the SAME database file.
    let store2 = SqliteStateStore::connect(&url).await.unwrap();
    let got = store2.get_batch("b-survive").await.unwrap().unwrap();
    assert_eq!(got.state, BatchState::Verified, "state survived restart");

    let incomplete = store2.list_incomplete_batches().await.unwrap();
    assert_eq!(incomplete.len(), 1);
}

#[tokio::test]
async fn recovery_commits_verified_but_uncommitted_batch() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    {
        let mut f = std::fs::File::create(&input).unwrap();
        for i in 0..4 {
            writeln!(f, "line-{i}").unwrap();
        }
    }
    let db = dir.path().join("state.db");
    let url = format!("sqlite://{}", db.display());

    // Seed a VERIFIED file batch directly into the store (as if a crash
    // happened right before the source commit).
    let marker = vtop_core::types::ProgressMarker::File {
        path: input.to_string_lossy().into_owned(),
        inode: None,
        start_byte: 0,
        end_byte: 28, // "line-0\n".. (7 bytes * 4)
        file_size: 28,
        mtime: String::new(),
    };
    let now = chrono::Utc::now().to_rfc3339();
    let rec = BatchRecord {
        batch_id: "b-verified".into(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: input.to_string_lossy().into_owned(),
        format: vtop_core::types::TelemetryFormat::Raw,
        state: BatchState::Batching,
        progress_start: marker.clone(),
        progress_end: marker,
        object_uri: Some("s3://telemetry-data/x/b-verified.raw.gz".into()),
        manifest_uri: Some("s3://telemetry-data/x/b-verified.manifest.json".into()),
        object_sha256: Some("deadbeef".into()),
        manifest_sha256: Some("feedface".into()),
        record_count: Some(4),
        error_message: None,
        owner: None,
        lease_expires_at: None,
        created_at: now.clone(),
        updated_at: now,
    };
    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        store.save_batch_state(&rec).await.unwrap();
        for st in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
            BatchState::Verified,
        ] {
            store
                .update_batch_state("b-verified", st, &BatchPatch::default())
                .await
                .unwrap();
        }
    }

    // Build an engine pointed at the same DB and run recovery.
    let cfg = file_config(
        work.to_str().unwrap(),
        &url,
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let summary = engine.recover().await.unwrap();
    assert_eq!(summary.committed, 1, "verified batch committed on recovery");

    let got = engine.store.get_batch("b-verified").await.unwrap().unwrap();
    assert_eq!(got.state, BatchState::SourceCommitted);
}

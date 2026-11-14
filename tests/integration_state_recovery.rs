//! Integration test: state survives "restart" and recovery commits a
//! verified-but-uncommitted batch.
//!
//! The state store is the durable journal. After a simulated restart (reopen
//! the same SQLite file) the engine's recovery scan finds the VERIFIED batch
//! and commits it — never advancing source progress for unverified batches.

use std::io::Write;
use std::path::{Path, PathBuf};
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::StreamsConfig;
use vtop_core::manifest::{ManifestBuilder, VtopManifest};
use vtop_core::state_machine::BatchState;
use vtop_core::types::{CompressionType, ProgressMarker, SourceType, TelemetryFormat};
use vtop_state::{BatchPatch, BatchRecord, SqliteStateStore, StateStore, VerificationClass};

#[allow(clippy::too_many_arguments)]
fn write_recovery_manifest_with_records(
    work_dir: &Path,
    batch_id: &str,
    source_name: &str,
    progress: ProgressMarker,
    object_uri: &str,
    manifest_uri: &str,
    object_size: u64,
    checksum_algorithm: &str,
    checksum: &str,
    record_count: usize,
) -> (VtopManifest, PathBuf) {
    let manifest = ManifestBuilder {
        batch_id: batch_id.into(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: source_name.into(),
        format: TelemetryFormat::Raw,
        compression: CompressionType::None,
        record_count,
        first_timestamp: None,
        last_timestamp: None,
        source_progress: progress,
        object_uri: object_uri.into(),
        object_size,
        object_checksum_algorithm: checksum_algorithm.into(),
        object_checksum: checksum.into(),
        manifest_uri: manifest_uri.into(),
        path_template: "test".into(),
        resolved_prefix: "x".into(),
        upload_backend: "mock".into(),
        created_at: chrono::Utc::now().to_rfc3339(),
    }
    .build()
    .unwrap();
    let path = manifest.write_to_file(work_dir).unwrap();
    (manifest, path)
}

#[allow(clippy::too_many_arguments)]
fn write_recovery_manifest(
    work_dir: &Path,
    batch_id: &str,
    source_name: &str,
    progress: ProgressMarker,
    object_uri: &str,
    manifest_uri: &str,
    object_size: u64,
    checksum_algorithm: &str,
    checksum: &str,
) -> (VtopManifest, PathBuf) {
    write_recovery_manifest_with_records(
        work_dir,
        batch_id,
        source_name,
        progress,
        object_uri,
        manifest_uri,
        object_size,
        checksum_algorithm,
        checksum,
        1,
    )
}

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
        manifest_version_id: None,
        object_size_bytes: None,
        verification_class: None,
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
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    let object_uri = "s3://telemetry-data/x/b-verified.raw.gz";
    let manifest_uri = "s3://telemetry-data/x/b-verified.manifest.json";
    let object_bytes = b"archived-bytes";
    let object_digest = vtop_core::checksum::sha256_bytes(object_bytes);
    let (manifest, manifest_path) = write_recovery_manifest(
        &work,
        "b-verified",
        &input.to_string_lossy(),
        marker.clone(),
        object_uri,
        manifest_uri,
        object_bytes.len() as u64,
        "sha256",
        &object_digest,
    );
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
        object_uri: Some(object_uri.into()),
        manifest_uri: Some(manifest_uri.into()),
        object_sha256: Some(object_digest.clone()),
        manifest_sha256: Some(manifest.manifest.sha256.clone()),
        manifest_version_id: None,
        object_size_bytes: Some(object_bytes.len() as i64),
        verification_class: None,
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

    // Recovery re-verifies storage before committing (#69), so the object the
    // ledger points at must actually EXIST in the backend with the recorded
    // digest — exactly what a real crash-restart would find.
    let staged = dir.path().join("staged-object");
    std::fs::write(&staged, object_bytes).unwrap();
    engine
        .backend
        .put_object(
            &staged,
            object_uri,
            Some(vtop_upload::ObjectChecksum::new("sha256", &object_digest)),
        )
        .await
        .unwrap();
    // The re-check requires the MANIFEST too — verified means both halves.
    engine
        .backend
        .put_manifest(
            &manifest_path,
            manifest_uri,
            Some(vtop_upload::ObjectChecksum::new(
                "sha256",
                &manifest.manifest.sha256,
            )),
        )
        .await
        .unwrap();

    let summary = engine.recover().await.unwrap();
    assert_eq!(summary.committed, 1, "verified batch committed on recovery");

    let got = engine.store.get_batch("b-verified").await.unwrap().unwrap();
    assert_eq!(got.state, BatchState::SourceCommitted);
}

/// #64: recovery must hash the stored body, not accept the checksum value that
/// accompanied the upload. This simulates a same-size object replacement while
/// the manifest, ledger, and uploader metadata all retain the original digest.
#[tokio::test]
async fn recovery_rejects_same_size_replacement_with_matching_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work-content-recovery");
    let input = dir.path().join("content-recovery.log");
    std::fs::write(&input, b"line-0\n").unwrap();
    let db = dir.path().join("content-recovery.db");
    let url = format!("sqlite://{}", db.display());
    let batch_id = "b-content-replaced";
    let object_uri = "s3://telemetry-data/x/b-content-replaced.raw.gz";
    let manifest_uri = "s3://telemetry-data/x/b-content-replaced.manifest.json";
    let expected_bytes = b"payload-A";
    let replacement_bytes = b"payload-B";
    assert_eq!(expected_bytes.len(), replacement_bytes.len());
    let expected_digest = vtop_core::checksum::sha256_bytes(expected_bytes);
    let marker = ProgressMarker::File {
        path: input.to_string_lossy().into_owned(),
        inode: None,
        start_byte: 0,
        end_byte: 7,
        file_size: 7,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    let (manifest, manifest_path) = write_recovery_manifest(
        &work,
        batch_id,
        &input.to_string_lossy(),
        marker.clone(),
        object_uri,
        manifest_uri,
        expected_bytes.len() as u64,
        "sha256",
        &expected_digest,
    );
    let now = chrono::Utc::now().to_rfc3339();
    let rec = BatchRecord {
        batch_id: batch_id.into(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: input.to_string_lossy().into_owned(),
        format: TelemetryFormat::Raw,
        state: BatchState::Batching,
        progress_start: marker.clone(),
        progress_end: marker,
        object_uri: Some(object_uri.into()),
        manifest_uri: Some(manifest_uri.into()),
        object_sha256: Some(expected_digest.clone()),
        manifest_sha256: Some(manifest.manifest.sha256.clone()),
        manifest_version_id: None,
        object_size_bytes: Some(expected_bytes.len() as i64),
        verification_class: None,
        record_count: Some(1),
        error_message: None,
        owner: None,
        lease_expires_at: None,
        created_at: now.clone(),
        updated_at: now,
    };
    let store = SqliteStateStore::connect(&url).await.unwrap();
    store.save_batch_state(&rec).await.unwrap();
    for state in [
        BatchState::Sealed,
        BatchState::Compressed,
        BatchState::Checksummed,
        BatchState::ObjectUploaded,
        BatchState::ManifestUploaded,
        BatchState::Verified,
    ] {
        store
            .update_batch_state(batch_id, state, &BatchPatch::default())
            .await
            .unwrap();
    }
    drop(store);

    let cfg = file_config(
        work.to_str().unwrap(),
        &url,
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let staged = dir.path().join("replacement-object");
    std::fs::write(&staged, replacement_bytes).unwrap();
    engine
        .backend
        .put_object(
            &staged,
            object_uri,
            Some(vtop_upload::ObjectChecksum::new("sha256", &expected_digest)),
        )
        .await
        .unwrap();
    engine
        .backend
        .put_manifest(
            &manifest_path,
            manifest_uri,
            Some(vtop_upload::ObjectChecksum::new(
                "sha256",
                &manifest.manifest.sha256,
            )),
        )
        .await
        .unwrap();

    let summary = engine.recover().await.unwrap();
    assert_eq!(summary.committed, 0);
    assert_eq!(summary.replay_required, 1);
    let got = engine.store.get_batch(batch_id).await.unwrap().unwrap();
    assert_eq!(got.state, BatchState::ReplayRequired);
}

/// #64 review regression: an unsigned manifest can be edited and rehashed, so
/// recovery must also bind its embedded self-hash to the durable ledger value.
#[tokio::test]
async fn recovery_rejects_rehashed_manifest_not_bound_to_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work-manifest-replacement");
    let input = dir.path().join("manifest-replacement.log");
    std::fs::write(&input, b"line-0\n").unwrap();
    let db = dir.path().join("manifest-replacement.db");
    let url = format!("sqlite://{}", db.display());
    let batch_id = "b-manifest-replaced";
    let object_uri = "s3://telemetry-data/x/b-manifest-replaced.raw.gz";
    let manifest_uri = "s3://telemetry-data/x/b-manifest-replaced.manifest.json";
    let original_bytes = b"payload-A";
    let replacement_bytes = b"payload-B";
    let object_digest = vtop_core::checksum::sha256_bytes(original_bytes);
    let marker = ProgressMarker::File {
        path: input.to_string_lossy().into_owned(),
        inode: None,
        start_byte: 0,
        end_byte: 7,
        file_size: 7,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    let (original_manifest, _) = write_recovery_manifest(
        &work,
        batch_id,
        &input.to_string_lossy(),
        marker.clone(),
        object_uri,
        manifest_uri,
        original_bytes.len() as u64,
        "sha256",
        &object_digest,
    );
    // Same binding strings and checksum value, but a rehashed replacement
    // downgrades the algorithm to size-only. Under the explicit weak policy it
    // would pass unless the ledger's original manifest hash is checked.
    let (replacement_manifest, replacement_path) = write_recovery_manifest(
        &work,
        batch_id,
        &input.to_string_lossy(),
        marker.clone(),
        object_uri,
        manifest_uri,
        replacement_bytes.len() as u64,
        "none",
        &object_digest,
    );
    assert_ne!(
        original_manifest.manifest.sha256,
        replacement_manifest.manifest.sha256
    );

    let now = chrono::Utc::now().to_rfc3339();
    let rec = BatchRecord {
        batch_id: batch_id.into(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: input.to_string_lossy().into_owned(),
        format: TelemetryFormat::Raw,
        state: BatchState::Batching,
        progress_start: marker.clone(),
        progress_end: marker,
        object_uri: Some(object_uri.into()),
        manifest_uri: Some(manifest_uri.into()),
        object_sha256: Some(object_digest.clone()),
        manifest_sha256: Some(original_manifest.manifest.sha256.clone()),
        manifest_version_id: None,
        object_size_bytes: Some(original_bytes.len() as i64),
        verification_class: None,
        record_count: Some(1),
        error_message: None,
        owner: None,
        lease_expires_at: None,
        created_at: now.clone(),
        updated_at: now,
    };
    let store = SqliteStateStore::connect(&url).await.unwrap();
    store.save_batch_state(&rec).await.unwrap();
    for state in [
        BatchState::Sealed,
        BatchState::Compressed,
        BatchState::Checksummed,
        BatchState::ObjectUploaded,
        BatchState::ManifestUploaded,
        BatchState::Verified,
    ] {
        store
            .update_batch_state(batch_id, state, &BatchPatch::default())
            .await
            .unwrap();
    }
    drop(store);

    let cfg = file_config(
        work.to_str().unwrap(),
        &url,
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    assert!(!cfg.upload.require_strong_verification);
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let staged = dir.path().join("same-size-replacement");
    std::fs::write(&staged, replacement_bytes).unwrap();
    engine
        .backend
        .put_object(
            &staged,
            object_uri,
            Some(vtop_upload::ObjectChecksum::new("sha256", &object_digest)),
        )
        .await
        .unwrap();
    engine
        .backend
        .put_manifest(&replacement_path, manifest_uri, None)
        .await
        .unwrap();

    let summary = engine.recover().await.unwrap();
    assert_eq!(summary.committed, 0);
    assert_eq!(summary.replay_required, 1);
    assert_eq!(
        engine
            .store
            .get_batch(batch_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        BatchState::ReplayRequired
    );
}

/// #69: a VERIFIED ledger row whose object is GONE (deleted/modified while the
/// engine was down) must NOT get its source progress committed — recovery must
/// route it to replay. Committing would advance the source past data that no
/// longer exists as verified.
#[tokio::test]
async fn recovery_refuses_to_commit_when_storage_no_longer_verifies() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    std::fs::write(&input, "line-0\nline-1\n").unwrap();
    let db = dir.path().join("state.db");
    let url = format!("sqlite://{}", db.display());

    let marker = vtop_core::types::ProgressMarker::File {
        path: input.to_string_lossy().into_owned(),
        inode: None,
        start_byte: 0,
        end_byte: 14,
        file_size: 14,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    let now = chrono::Utc::now().to_rfc3339();
    let rec = BatchRecord {
        batch_id: "b-stale".into(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: input.to_string_lossy().into_owned(),
        format: vtop_core::types::TelemetryFormat::Raw,
        state: BatchState::Batching,
        progress_start: marker.clone(),
        progress_end: marker,
        // The ledger CLAIMS this object exists — the mock backend is empty, so
        // the claim is stale, exactly as after an out-of-band deletion.
        object_uri: Some("s3://telemetry-data/x/b-stale.raw.gz".into()),
        manifest_uri: Some("s3://telemetry-data/x/b-stale.manifest.json".into()),
        object_sha256: Some("deadbeef".into()),
        manifest_sha256: Some("feedface".into()),
        manifest_version_id: None,
        object_size_bytes: None,
        verification_class: None,
        record_count: Some(2),
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
                .update_batch_state("b-stale", st, &BatchPatch::default())
                .await
                .unwrap();
        }
    }

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

    assert_eq!(summary.committed, 0, "a stale VERIFIED must never commit");
    assert_eq!(summary.replay_required, 1, "it must be routed to replay");
    let got = engine.store.get_batch("b-stale").await.unwrap().unwrap();
    assert_eq!(
        got.state,
        BatchState::ReplayRequired,
        "source progress stays unadvanced; the uncommitted range will be re-read"
    );
}

/// #67: once a manifest MAC key is enabled, recovery must download and
/// authenticate the stored manifest. A legacy/unsigned manifest with a valid
/// self-hash is not grandfathered into a keyed deployment.
#[tokio::test]
async fn recovery_rejects_unsigned_manifest_after_mac_cutover() {
    use vtop_core::manifest::ManifestBuilder;
    use vtop_core::types::{CompressionType, ProgressMarker, TelemetryFormat};

    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work-mac-cutover");
    let input = dir.path().join("mac-cutover.log");
    std::fs::write(&input, b"line-0\n").unwrap();
    let db = dir.path().join("mac-cutover.db");
    let url = format!("sqlite://{}", db.display());
    let object_uri = "s3://telemetry-data/x/b-unsigned.raw.gz";
    let manifest_uri = "s3://telemetry-data/x/b-unsigned.manifest.json";
    let marker = ProgressMarker::File {
        path: input.to_string_lossy().into_owned(),
        inode: None,
        start_byte: 0,
        end_byte: 7,
        file_size: 7,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    let now = chrono::Utc::now().to_rfc3339();
    let unsigned = ManifestBuilder {
        batch_id: "b-unsigned".into(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: input.to_string_lossy().into_owned(),
        format: TelemetryFormat::Raw,
        compression: CompressionType::None,
        record_count: 1,
        first_timestamp: None,
        last_timestamp: None,
        source_progress: marker.clone(),
        object_uri: object_uri.into(),
        object_size: 14,
        object_checksum_algorithm: "sha256".into(),
        object_checksum: "deadbeef".into(),
        manifest_uri: manifest_uri.into(),
        path_template: "test".into(),
        resolved_prefix: "x".into(),
        upload_backend: "mock".into(),
        created_at: now.clone(),
    }
    .build()
    .unwrap();
    unsigned.verify_self_hash().unwrap();
    assert!(unsigned.manifest.mac.is_none());
    let manifest_path = unsigned.write_to_file(&work).unwrap();

    let rec = BatchRecord {
        batch_id: "b-unsigned".into(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: input.to_string_lossy().into_owned(),
        format: TelemetryFormat::Raw,
        state: BatchState::Batching,
        progress_start: marker.clone(),
        progress_end: marker,
        object_uri: Some(object_uri.into()),
        manifest_uri: Some(manifest_uri.into()),
        object_sha256: Some("deadbeef".into()),
        manifest_sha256: Some(unsigned.manifest.sha256.clone()),
        manifest_version_id: None,
        object_size_bytes: Some(14),
        verification_class: None,
        record_count: Some(1),
        error_message: None,
        owner: None,
        lease_expires_at: None,
        created_at: now.clone(),
        updated_at: now,
    };
    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        store.save_batch_state(&rec).await.unwrap();
        for state in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
            BatchState::Verified,
        ] {
            store
                .update_batch_state("b-unsigned", state, &BatchPatch::default())
                .await
                .unwrap();
        }
    }

    let env_name = format!("VTOP_TEST_RECOVERY_MAC_{}", std::process::id());
    std::env::set_var(&env_name, "6b".repeat(32));
    let mut cfg = file_config(
        work.to_str().unwrap(),
        &url,
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    cfg.manifest_mac_key_env = Some(env_name.clone());
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    std::env::remove_var(&env_name);

    let staged = dir.path().join("staged-object-mac-cutover");
    std::fs::write(&staged, b"archived-bytes").unwrap();
    engine
        .backend
        .put_object(
            &staged,
            object_uri,
            Some(vtop_upload::ObjectChecksum::new("sha256", "deadbeef")),
        )
        .await
        .unwrap();
    engine
        .backend
        .put_manifest(
            &manifest_path,
            manifest_uri,
            Some(vtop_upload::ObjectChecksum::new(
                "sha256",
                &unsigned.manifest.sha256,
            )),
        )
        .await
        .unwrap();

    let summary = engine.recover().await.unwrap();
    assert_eq!(summary.committed, 0);
    assert_eq!(summary.replay_required, 1);
    let got = engine.store.get_batch("b-unsigned").await.unwrap().unwrap();
    assert_eq!(got.state, BatchState::ReplayRequired);
}

/// #125: with no digest to compare (checksums disabled) and
/// `require_strong_verification: false`, the recovery re-check must still
/// compare the RECORDED size — a same-URI replacement of a different size
/// cannot pass on existence alone.
#[tokio::test]
async fn recovery_size_check_rejects_replaced_object_without_checksums() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    std::fs::write(&input, "line-0\n").unwrap();
    let db = dir.path().join("state.db");
    let url = format!("sqlite://{}", db.display());

    let marker = vtop_core::types::ProgressMarker::File {
        path: input.to_string_lossy().into_owned(),
        inode: None,
        start_byte: 0,
        end_byte: 7,
        file_size: 7,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    let object_uri = "s3://telemetry-data/x/b-size.raw.gz";
    let manifest_uri = "s3://telemetry-data/x/b-size.manifest.json";
    let (manifest, manifest_path) = write_recovery_manifest(
        &work,
        "b-size",
        &input.to_string_lossy(),
        marker.clone(),
        object_uri,
        manifest_uri,
        100,
        "none",
        "",
    );
    let now = chrono::Utc::now().to_rfc3339();
    let mut rec = BatchRecord {
        batch_id: "b-size".into(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: input.to_string_lossy().into_owned(),
        format: vtop_core::types::TelemetryFormat::Raw,
        state: BatchState::Batching,
        progress_start: marker.clone(),
        progress_end: marker,
        object_uri: Some(object_uri.into()),
        manifest_uri: Some(manifest_uri.into()),
        // Checksums disabled: the ledger recorded an empty digest…
        object_sha256: Some(String::new()),
        manifest_sha256: Some(manifest.manifest.sha256.clone()),
        manifest_version_id: None,
        // …but it DID record the uploaded size.
        object_size_bytes: Some(100),
        verification_class: None,
        record_count: Some(1),
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
                .update_batch_state("b-size", st, &BatchPatch::default())
                .await
                .unwrap();
        }
    }

    let cfg = file_config(
        work.to_str().unwrap(),
        &url,
        vec![input.to_string_lossy().into_owned()],
        "mock", // testkit config has require_strong_verification: false
    );
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    // Stage object + manifest, but the object is 14 bytes, not the recorded 100
    // — a replaced object that existence-only checking would wave through.
    let staged = dir.path().join("staged");
    std::fs::write(&staged, b"replaced-bytes").unwrap();
    engine
        .backend
        .put_object(&staged, object_uri, None)
        .await
        .unwrap();
    engine
        .backend
        .put_manifest(&manifest_path, manifest_uri, None)
        .await
        .unwrap();

    let summary = engine.recover().await.unwrap();
    assert_eq!(summary.committed, 0, "size mismatch must refuse the commit");
    assert_eq!(summary.replay_required, 1);

    // Drop the size expectation and it becomes the accepted backend-limited
    // case again (existence-only, require_strong=false), proving the refusal
    // above was the SIZE gate.
    rec.batch_id = "b-size-ok".into();
    rec.object_size_bytes = Some(14);
    let object_uri_ok = "s3://telemetry-data/x/b-size-ok.raw.gz";
    let manifest_uri_ok = "s3://telemetry-data/x/b-size-ok.manifest.json";
    let (manifest_ok, manifest_path_ok) = write_recovery_manifest(
        &work,
        "b-size-ok",
        &input.to_string_lossy(),
        rec.progress_end.clone(),
        object_uri_ok,
        manifest_uri_ok,
        14,
        "none",
        "",
    );
    rec.object_uri = Some(object_uri_ok.into());
    rec.manifest_uri = Some(manifest_uri_ok.into());
    rec.manifest_sha256 = Some(manifest_ok.manifest.sha256.clone());
    engine.store.save_batch_state(&rec).await.unwrap();
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
            .update_batch_state("b-size-ok", st, &BatchPatch::default())
            .await
            .unwrap();
    }
    engine
        .backend
        .put_object(&staged, object_uri_ok, None)
        .await
        .unwrap();
    engine
        .backend
        .put_manifest(&manifest_path_ok, manifest_uri_ok, None)
        .await
        .unwrap();
    let summary = engine.recover().await.unwrap();
    assert_eq!(summary.committed, 1, "matching size commits");
}

/// #552: with several incomplete ranges of one source, recovery applied each
/// rewind in turn, and `replay_from_marker` overwrites the adapter's read
/// position — so whichever row it visited last decided where the source
/// resumed, and every earlier range was skipped. It must resume from the
/// LOWEST incomplete start.
#[tokio::test]
async fn recovery_resumes_from_the_lowest_incomplete_range_not_the_last_one_visited() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    // Three 7-byte lines: [0,7) [7,14) [14,21).
    std::fs::write(&input, "line-0\nline-1\nline-2\n").unwrap();
    let db = dir.path().join("state.db");
    let url = format!("sqlite://{}", db.display());
    let path = input.to_string_lossy().into_owned();
    let range = |start: u64, end: u64| ProgressMarker::File {
        path: path.clone(),
        inode: None,
        start_byte: start,
        end_byte: end,
        file_size: 21,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };

    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        // Two FAILED ranges, the later-created one starting HIGHER, so a
        // last-row-wins rewind lands at byte 7 and the first line is lost.
        for (id, start, end, at) in [
            ("b-low", 0, 7, "2026-01-01T00:00:00+00:00"),
            ("b-high", 7, 14, "2026-01-01T00:00:01+00:00"),
        ] {
            let rec = BatchRecord {
                batch_id: id.into(),
                tenant: "default".into(),
                source_type: SourceType::File,
                source_name: path.clone(),
                format: TelemetryFormat::Raw,
                state: BatchState::Batching,
                progress_start: range(start, end),
                progress_end: range(start, end),
                object_uri: None,
                manifest_uri: None,
                object_sha256: None,
                manifest_sha256: None,
                manifest_version_id: None,
                object_size_bytes: None,
                verification_class: None,
                record_count: Some(1),
                error_message: None,
                owner: None,
                lease_expires_at: None,
                created_at: at.into(),
                updated_at: at.into(),
            };
            store.save_batch_state(&rec).await.unwrap();
            store.mark_failed(id, "upload failed").await.unwrap();
        }
    }

    let cfg = file_config(work.to_str().unwrap(), &url, vec![path.clone()], "mock");
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let summary = engine.recover().await.unwrap();
    assert_eq!(summary.replay_required, 2, "both ranges are handed back");

    let outcomes = engine.process_once(SourceType::File).await.unwrap();
    assert!(outcomes.iter().all(|o| o.committed), "{outcomes:?}");

    let committed: Vec<_> = engine
        .store
        .list_batches()
        .await
        .unwrap()
        .into_iter()
        .filter(|b| b.state == BatchState::SourceCommitted)
        .collect();
    assert_eq!(committed.len(), 1, "{committed:?}");
    let ProgressMarker::File { start_byte, .. } = &committed[0].progress_start else {
        panic!("a file batch carries a file marker");
    };
    assert_eq!(
        *start_byte, 0,
        "the source must resume at the first byte that is not archived, not at \
         the start of whichever incomplete row recovery happened to visit last"
    );
    assert_eq!(
        committed[0].record_count,
        Some(3),
        "all three lines archived"
    );
}

/// #552 (review): a VERIFIED range sitting ABOVE a failed one of the same
/// file. Committing it first moves the adapter's committed point past the
/// failed range, and the rewind that follows is clamped to that point — the
/// failed range would be skipped. Recovery must leave the VERIFIED row alone
/// until the lower range has been re-read.
#[tokio::test]
async fn recovery_does_not_commit_a_verified_range_above_a_failed_one() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    // Four 7-byte lines: the failed range is [0,14), the verified one [14,28).
    std::fs::write(&input, "line-0\nline-1\nline-2\nline-3\n").unwrap();
    let db = dir.path().join("state.db");
    let url = format!("sqlite://{}", db.display());
    let path = input.to_string_lossy().into_owned();
    let range = |start: u64, end: u64| ProgressMarker::File {
        path: path.clone(),
        inode: None,
        start_byte: start,
        end_byte: end,
        file_size: 28,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };

    let object_uri = "s3://telemetry-data/x/b-above.raw.gz";
    let manifest_uri = "s3://telemetry-data/x/b-above.manifest.json";
    let object_bytes = b"archived-bytes";
    let object_digest = vtop_core::checksum::sha256_bytes(object_bytes);
    // The ledger row below says two records; the manifest must agree.
    let (manifest, manifest_path) = write_recovery_manifest_with_records(
        &work,
        "b-above",
        &path,
        range(14, 28),
        object_uri,
        manifest_uri,
        object_bytes.len() as u64,
        "sha256",
        &object_digest,
        2,
    );
    let row = |id: &str, start: u64, end: u64, at: &str| BatchRecord {
        batch_id: id.into(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: path.clone(),
        format: vtop_core::types::TelemetryFormat::Raw,
        state: BatchState::Batching,
        progress_start: range(start, end),
        progress_end: range(start, end),
        object_uri: None,
        manifest_uri: None,
        object_sha256: None,
        manifest_sha256: None,
        manifest_version_id: None,
        object_size_bytes: None,
        verification_class: None,
        record_count: Some(2),
        error_message: None,
        owner: None,
        lease_expires_at: None,
        created_at: at.into(),
        updated_at: at.into(),
    };
    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        // The failed LOWER range, created first.
        store
            .save_batch_state(&row("b-failed", 0, 14, "2026-01-01T00:00:00+00:00"))
            .await
            .unwrap();
        store
            .mark_failed("b-failed", "upload failed")
            .await
            .unwrap();
        // The verified HIGHER range.
        let mut above = row("b-above", 14, 28, "2026-01-01T00:00:01+00:00");
        above.object_uri = Some(object_uri.into());
        above.manifest_uri = Some(manifest_uri.into());
        above.object_sha256 = Some(object_digest.clone());
        above.manifest_sha256 = Some(manifest.manifest.sha256.clone());
        above.object_size_bytes = Some(object_bytes.len() as i64);
        store.save_batch_state(&above).await.unwrap();
        for st in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
            BatchState::Verified,
        ] {
            store
                .update_batch_state("b-above", st, &BatchPatch::default())
                .await
                .unwrap();
        }
    }

    let cfg = file_config(work.to_str().unwrap(), &url, vec![path.clone()], "mock");
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    // The VERIFIED row's object and manifest exist, so it passes the re-check.
    let staged = dir.path().join("staged-object");
    std::fs::write(&staged, object_bytes).unwrap();
    engine
        .backend
        .put_object(
            &staged,
            object_uri,
            Some(vtop_upload::ObjectChecksum::new("sha256", &object_digest)),
        )
        .await
        .unwrap();
    engine
        .backend
        .put_manifest(
            &manifest_path,
            manifest_uri,
            Some(vtop_upload::ObjectChecksum::new(
                "sha256",
                &manifest.manifest.sha256,
            )),
        )
        .await
        .unwrap();

    let summary = engine.recover().await.unwrap();
    assert_eq!(
        summary.committed, 0,
        "the higher range must not commit first"
    );
    assert_eq!(summary.still_pending, 1, "it is reported as still pending");
    assert_eq!(
        summary.replay_required, 1,
        "the failed range is handed back"
    );
    let above = engine.store.get_batch("b-above").await.unwrap().unwrap();
    assert_eq!(
        above.state,
        BatchState::Verified,
        "left VERIFIED: its object is in the archive and a later pass commits it"
    );

    // The source resumes at the failed range, not above it.
    let outcomes = engine.process_once(SourceType::File).await.unwrap();
    assert!(outcomes.iter().all(|o| o.committed), "{outcomes:?}");
    let reread: Vec<_> = engine
        .store
        .list_batches()
        .await
        .unwrap()
        .into_iter()
        .filter(|b| b.state == BatchState::SourceCommitted && b.batch_id != "b-above")
        .collect();
    assert_eq!(reread.len(), 1, "{reread:?}");
    let ProgressMarker::File { start_byte, .. } = &reread[0].progress_start else {
        panic!("a file batch carries a file marker");
    };
    assert_eq!(
        *start_byte, 0,
        "the failed range [0,14) is read again; committing the VERIFIED range first \
         would have clamped the rewind to byte 28 and skipped it"
    );
    assert_eq!(reread[0].record_count, Some(4));
    // #624: the held-back row is settled by the running engine only when its
    // marker is proven to be this file (review of #567); these legacy markers
    // name no inode, so it stays VERIFIED for the next recovery, as before.
    // `a_held_back_verified_range_is_settled_without_a_restart_...` covers
    // the settle with markers that carry their identity.
    assert_eq!(
        engine
            .store
            .get_batch("b-above")
            .await
            .unwrap()
            .unwrap()
            .state,
        BatchState::Verified
    );
}

/// #563 (review): a FAILED row whose bytes a later retry already archived and
/// committed is stale. Its rewind is clamped to the committed point, so nothing
/// is re-read for it, and it must not hold back the VERIFIED range above it
/// for ever.
#[tokio::test]
async fn recovery_commits_a_verified_range_above_a_failed_row_that_a_retry_already_covered() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    // Four 7-byte lines: the failed range is [0,14), the verified one [14,28).
    std::fs::write(&input, "line-0\nline-1\nline-2\nline-3\n").unwrap();
    let db = dir.path().join("state.db");
    let url = format!("sqlite://{}", db.display());
    let path = input.to_string_lossy().into_owned();
    let range = |start: u64, end: u64| ProgressMarker::File {
        path: path.clone(),
        inode: None,
        start_byte: start,
        end_byte: end,
        file_size: 28,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };

    let object_uri = "s3://telemetry-data/x/b-above.raw.gz";
    let manifest_uri = "s3://telemetry-data/x/b-above.manifest.json";
    let object_bytes = b"archived-bytes";
    let object_digest = vtop_core::checksum::sha256_bytes(object_bytes);
    // The ledger row below says two records; the manifest must agree.
    let (manifest, manifest_path) = write_recovery_manifest_with_records(
        &work,
        "b-above",
        &path,
        range(14, 28),
        object_uri,
        manifest_uri,
        object_bytes.len() as u64,
        "sha256",
        &object_digest,
        2,
    );
    let row = |id: &str, start: u64, end: u64, at: &str| BatchRecord {
        batch_id: id.into(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: path.clone(),
        format: vtop_core::types::TelemetryFormat::Raw,
        state: BatchState::Batching,
        progress_start: range(start, end),
        progress_end: range(start, end),
        object_uri: None,
        manifest_uri: None,
        object_sha256: None,
        manifest_sha256: None,
        manifest_version_id: None,
        object_size_bytes: None,
        verification_class: None,
        record_count: Some(2),
        error_message: None,
        owner: None,
        lease_expires_at: None,
        created_at: at.into(),
        updated_at: at.into(),
    };
    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        // The failed LOWER range, created first.
        store
            .save_batch_state(&row("b-failed", 0, 14, "2026-01-01T00:00:00+00:00"))
            .await
            .unwrap();
        store
            .mark_failed("b-failed", "upload failed")
            .await
            .unwrap();
        // The retry that re-archived those bytes and committed them.
        let mut retry = row("b-retry", 0, 14, "2026-01-01T00:00:00+00:30");
        retry.object_uri = Some("s3://telemetry-data/x/b-retry.raw.gz".into());
        retry.manifest_uri = Some("s3://telemetry-data/x/b-retry.manifest.json".into());
        store.save_batch_state(&retry).await.unwrap();
        for st in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
            BatchState::Verified,
            BatchState::SourceCommitted,
        ] {
            store
                .update_batch_state("b-retry", st, &BatchPatch::default())
                .await
                .unwrap();
        }
        // The verified HIGHER range.
        let mut above = row("b-above", 14, 28, "2026-01-01T00:00:01+00:00");
        above.object_uri = Some(object_uri.into());
        above.manifest_uri = Some(manifest_uri.into());
        above.object_sha256 = Some(object_digest.clone());
        above.manifest_sha256 = Some(manifest.manifest.sha256.clone());
        above.object_size_bytes = Some(object_bytes.len() as i64);
        store.save_batch_state(&above).await.unwrap();
        for st in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
            BatchState::Verified,
        ] {
            store
                .update_batch_state("b-above", st, &BatchPatch::default())
                .await
                .unwrap();
        }
    }

    let cfg = file_config(work.to_str().unwrap(), &url, vec![path.clone()], "mock");
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    // The VERIFIED row's object and manifest exist, so it passes the re-check.
    let staged = dir.path().join("staged-object");
    std::fs::write(&staged, object_bytes).unwrap();
    engine
        .backend
        .put_object(
            &staged,
            object_uri,
            Some(vtop_upload::ObjectChecksum::new("sha256", &object_digest)),
        )
        .await
        .unwrap();
    engine
        .backend
        .put_manifest(
            &manifest_path,
            manifest_uri,
            Some(vtop_upload::ObjectChecksum::new(
                "sha256",
                &manifest.manifest.sha256,
            )),
        )
        .await
        .unwrap();

    let summary = engine.recover().await.unwrap();
    assert_eq!(
        summary.committed, 1,
        "the VERIFIED range commits: the failed row below it is covered by the retry"
    );
    assert_eq!(summary.still_pending, 0, "nothing is held back");
    let above = engine.store.get_batch("b-above").await.unwrap().unwrap();
    assert_eq!(above.state, BatchState::SourceCommitted);

    // The source resumes after the committed ranges, so nothing is archived twice.
    let outcomes = engine.process_once(SourceType::File).await.unwrap();
    assert!(
        outcomes.is_empty(),
        "[0,28) is committed; a re-read here would duplicate it: {outcomes:?}"
    );
}

/// A ledger whose only VERIFIED row, `b-629` over `[7,14)`, can be re-checked
/// by size alone (checksums disabled, testkit's `require_strong_verification:
/// false`), with its object and manifest staged in the engine's backend. With
/// `failed_below`, a FAILED row over `[0,7)` of the same file sits under it,
/// so recovery must hold `b-629` back (#552). The row has no verification
/// class, as every release before #664 wrote it.
async fn size_only_verified_ledger(dir: &Path, tenant: &str, failed_below: bool) -> Engine {
    let url = format!("sqlite://{}", dir.join("state.db").display());
    verified_ledger(
        dir,
        &url,
        "b-629",
        tenant,
        StoredBy::SizeOnly,
        None,
        failed_below,
        false,
    )
    .await
}

/// What recovery's re-check of `b-629` will find in the store.
#[derive(Clone, Copy, Debug)]
enum StoredBy {
    /// Checksums disabled: the re-check can confirm size only.
    SizeOnly,
    /// A SHA-256 the mock backend checks the stored bytes against.
    Digest,
}

/// [`size_only_verified_ledger`] on the state store at `url`, with the
/// re-check finding `stored`, and `b-629` made VERIFIED by `mark_verified`
/// with `class` - or, for `None`, with no class, as a release before #664
/// did. The ledger is written through a store that is closed again before
/// the engine opens it: the engine is the process after a restart.
#[allow(clippy::too_many_arguments)]
async fn verified_ledger(
    dir: &Path,
    url: &str,
    id: &str,
    tenant: &str,
    stored: StoredBy,
    class: Option<VerificationClass>,
    failed_below: bool,
    strict: bool,
) -> Engine {
    let work = dir.join("work");
    let input = dir.join("in.log");
    std::fs::write(&input, "line-0\nline-1\n").unwrap();
    let path = input.to_string_lossy().into_owned();
    let range = |start: u64, end: u64| ProgressMarker::File {
        path: path.clone(),
        inode: None,
        start_byte: start,
        end_byte: end,
        file_size: 14,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    let object_uri = &format!("s3://telemetry-data/x/{id}.raw.gz");
    let manifest_uri = &format!("s3://telemetry-data/x/{id}.manifest.json");
    let object_bytes = b"fourteen-bytes";
    let digest = vtop_core::checksum::sha256_bytes(object_bytes);
    let (algorithm, checksum) = match stored {
        StoredBy::SizeOnly => ("none", ""),
        StoredBy::Digest => ("sha256", digest.as_str()),
    };
    let (manifest, manifest_path) = write_recovery_manifest(
        &work,
        id,
        &path,
        range(7, 14),
        object_uri,
        manifest_uri,
        14,
        algorithm,
        checksum,
    );
    let row = |id: &str, start: u64, end: u64, at: &str| BatchRecord {
        batch_id: id.into(),
        tenant: tenant.into(),
        source_type: SourceType::File,
        source_name: path.clone(),
        format: TelemetryFormat::Raw,
        state: BatchState::Batching,
        progress_start: range(start, end),
        progress_end: range(start, end),
        object_uri: None,
        manifest_uri: None,
        object_sha256: None,
        manifest_sha256: None,
        manifest_version_id: None,
        object_size_bytes: None,
        verification_class: None,
        record_count: Some(1),
        error_message: None,
        owner: None,
        lease_expires_at: None,
        created_at: at.into(),
        updated_at: at.into(),
    };
    {
        let store = vtop_state::connect_state_store(url).await.unwrap();
        if failed_below {
            store
                .save_batch_state(&row(
                    &format!("{id}-failed"),
                    0,
                    7,
                    "2026-01-01T00:00:00+00:00",
                ))
                .await
                .unwrap();
            store
                .mark_failed(&format!("{id}-failed"), "upload failed")
                .await
                .unwrap();
        }
        let mut verified = row(id, 7, 14, "2026-01-01T00:00:01+00:00");
        verified.object_uri = Some(object_uri.into());
        verified.manifest_uri = Some(manifest_uri.into());
        verified.object_sha256 = Some(checksum.to_owned());
        verified.manifest_sha256 = Some(manifest.manifest.sha256.clone());
        verified.object_size_bytes = Some(14);
        store.save_batch_state(&verified).await.unwrap();
        for st in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
        ] {
            store
                .update_batch_state(id, st, &BatchPatch::default())
                .await
                .unwrap();
        }
        match class {
            Some(class) => store.mark_verified(id, class).await.unwrap(),
            None => store
                .update_batch_state(id, BatchState::Verified, &BatchPatch::default())
                .await
                .unwrap(),
        }
    }

    let mut cfg = file_config(
        work.to_str().unwrap(),
        url,
        vec![path.clone()],
        "mock", // testkit config has require_strong_verification: false
    );
    cfg.engine.tenant = tenant.into();
    cfg.upload.require_strong_verification = strict;
    if url.starts_with("postgres") {
        // The engine takes a PostgreSQL URL only from a secret reference:
        // the variable the Postgres test reads `url` from.
        cfg.engine.state_store =
            vtop_core::config::StateStoreConfig::Env(vtop_core::config::StateStoreEnvRef {
                env: "VTOP_TEST_POSTGRES".into(),
            });
    }
    let engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let staged = dir.join("staged");
    std::fs::write(&staged, object_bytes).unwrap();
    let (object_checksum, manifest_checksum) = match stored {
        StoredBy::SizeOnly => (None, None),
        StoredBy::Digest => (
            Some(vtop_upload::ObjectChecksum::new("sha256", &digest)),
            Some(vtop_upload::ObjectChecksum::new(
                "sha256",
                &manifest.manifest.sha256,
            )),
        ),
    };
    engine
        .backend
        .put_object(&staged, object_uri, object_checksum)
        .await
        .unwrap();
    engine
        .backend
        .put_manifest(&manifest_path, manifest_uri, manifest_checksum)
        .await
        .unwrap();
    engine
}

/// How much one recovery of `b-629`, from the ledger [`verified_ledger`]
/// writes, moves the size-only counter of `tenant`. Asserts the recovery
/// committed it.
async fn size_only_count_of_recovery(
    dir: &Path,
    url: &str,
    tenant: &str,
    stored: StoredBy,
    class: Option<VerificationClass>,
) -> u64 {
    let before = size_only_count(tenant);
    let mut engine = verified_ledger(dir, url, "b-629", tenant, stored, class, false, false).await;
    let summary = engine.recover().await.unwrap();
    assert_eq!(
        summary.committed, 1,
        "recovery commits b-629 (re-check {stored:?}, class {class:?}): {summary:?}"
    );
    let row = engine.store.get_batch("b-629").await.unwrap().unwrap();
    assert_eq!(
        (row.state, row.verification_class),
        (BatchState::SourceCommitted, class)
    );
    size_only_count(tenant) - before
}

/// #664: a row verified on size alone before a restart is counted as
/// size-only when recovery commits it, though recovery's own re-check finds
/// a digest match. Recovery re-derived the class from that re-check, so the
/// commit was counted as strong.
async fn assert_size_only_row_is_counted_size_only(dir: &Path, url: &str, tenant: &str) {
    let moved = size_only_count_of_recovery(
        dir,
        url,
        tenant,
        StoredBy::Digest,
        Some(VerificationClass::SizeOnly),
    )
    .await;
    assert_eq!(
        moved, 1,
        "b-629 was verified size-only before the restart, so its recovered commit must \
         count in vtop_verification_backend_limited_total{{tenant={tenant:?}}} once; it \
         moved by {moved}"
    );
}

/// #664: a row verified strongly is never counted as size-only, though
/// recovery's re-check can confirm only its size (and the opt-out lets it
/// commit on that).
async fn assert_strong_row_is_never_counted_size_only(dir: &Path, url: &str, tenant: &str) {
    let moved = size_only_count_of_recovery(
        dir,
        url,
        tenant,
        StoredBy::SizeOnly,
        Some(VerificationClass::Strong),
    )
    .await;
    assert_eq!(
        moved, 0,
        "b-629 was verified strongly before the restart, so its recovered commit must not \
         count in vtop_verification_backend_limited_total{{tenant={tenant:?}}}; it moved \
         by {moved}"
    );
}

#[tokio::test]
async fn a_row_verified_size_only_before_a_restart_is_counted_size_only_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("state.db").display());
    assert_size_only_row_is_counted_size_only(dir.path(), &url, "tenant-664-size-only").await;
}

#[tokio::test]
async fn a_row_verified_strong_is_never_counted_size_only_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("state.db").display());
    assert_strong_row_is_never_counted_size_only(dir.path(), &url, "tenant-664-strong").await;
}

/// #664: a row with no class - verified by a release before it - is counted
/// as before: by what recovery's re-check finds. Size-only is counted (also
/// `recovery_counts_a_size_only_acceptance_like_the_running_engine`), a digest
/// match is not.
#[tokio::test]
async fn a_legacy_row_without_a_class_is_counted_by_its_recheck_as_before() {
    for (stored, expected) in [(StoredBy::SizeOnly, 1), (StoredBy::Digest, 0)] {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("state.db").display());
        let tenant = format!("tenant-664-legacy-{stored:?}");
        let moved = size_only_count_of_recovery(dir.path(), &url, &tenant, stored, None).await;
        assert_eq!(
            moved, expected,
            "a legacy row whose re-check finds {stored:?} counts {expected} size-only commit(s), \
             as before #664; it counted {moved}"
        );
    }
}

/// The same two recoveries on a PostgreSQL ledger. Runs when
/// `VTOP_TEST_POSTGRES` names a migration identity on a throwaway database,
/// as for the state store's battery (whose tables it truncates: do not run
/// the two at once), in a build with the `postgres` feature.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn on_postgres_a_recovered_commit_is_counted_by_its_stored_class() {
    let Ok(url) = std::env::var("VTOP_TEST_POSTGRES") else {
        eprintln!("VTOP_TEST_POSTGRES not set; skipping the Postgres recovery test");
        return;
    };
    vtop_state::migrate_state_store(&url).await.unwrap();
    let admin = vtop_state::PgStateStore::connect(&url).await.unwrap();
    for (scenario, tenant) in [
        ("size-only", "tenant-664-pg-size-only"),
        ("strong", "tenant-664-pg-strong"),
    ] {
        admin.execute_raw("TRUNCATE batches").await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        match scenario {
            "size-only" => {
                assert_size_only_row_is_counted_size_only(dir.path(), &url, tenant).await
            }
            _ => assert_strong_row_is_never_counted_size_only(dir.path(), &url, tenant).await,
        }
    }
    // And every combination the SQLite matrix covers, on Postgres.
    recovery_matrix(&url, "pg", || {
        Box::pin(async { admin.execute_raw("TRUNCATE batches").await.unwrap() })
    })
    .await;
    admin.execute_raw("TRUNCATE batches").await.unwrap();
}

/// `vtop_verification_backend_limited_total` for one tenant's raw file batches.
fn size_only_count(tenant: &str) -> u64 {
    vtop_core::telemetry::init()
        .expect("metrics registry")
        .verification_backend_limited_total
        .with_label_values(&[tenant, "file", TelemetryFormat::Raw.extension()])
        .get()
}

/// #629: recovery's storage re-check accepted a size-only (backend-limited)
/// result for a VERIFIED row without a word, so a commit made on size alone
/// left no trace in the counter the running engine uses for exactly that. It
/// must count it under the same labels (tenant, source type, format).
#[tokio::test]
async fn recovery_counts_a_size_only_acceptance_like_the_running_engine() {
    // A tenant no other test uses: the registry is process-wide.
    let tenant = "tenant-629-recovery";
    let before = size_only_count(tenant);
    let dir = tempfile::tempdir().unwrap();
    let mut engine = size_only_verified_ledger(dir.path(), tenant, false).await;

    let summary = engine.recover().await.unwrap();
    assert_eq!(summary.committed, 1, "the size-only re-check commits");
    let after = size_only_count(tenant);
    assert_eq!(
        after - before,
        1,
        "recovery committed b-629 on size alone and must count it in \
         vtop_verification_backend_limited_total{{tenant={tenant:?}}}: \
         before {before}, after {after}"
    );
}

/// #629 (review): the count is of size-only COMMITS, as in the running
/// engine, where it moves with `verified_total`. A VERIFIED row that passes
/// the re-check but is held back under a failed lower range is not committed,
/// and stays VERIFIED to be re-checked by every later recovery; counting it
/// at the re-check counted a commit that never happened, once per restart.
#[tokio::test]
async fn recovery_does_not_count_a_size_only_row_it_holds_back() {
    let tenant = "tenant-629-held-back";
    let before = size_only_count(tenant);
    let dir = tempfile::tempdir().unwrap();
    let mut engine = size_only_verified_ledger(dir.path(), tenant, true).await;

    for pass in 1..=2 {
        let summary = engine.recover().await.unwrap();
        assert_eq!(summary.committed, 0, "pass {pass}: b-629 is held back");
        let row = engine.store.get_batch("b-629").await.unwrap().unwrap();
        assert_eq!(row.state, BatchState::Verified, "pass {pass}");
        let after = size_only_count(tenant);
        assert_eq!(
            after - before,
            0,
            "pass {pass}: b-629 was re-checked by size but not committed, so \
             vtop_verification_backend_limited_total{{tenant={tenant:?}}} must not \
             move: before {before}, after {after}"
        );
    }
}

// ---- #567: a handed-back range its source has since committed past ----------

/// The inode the file adapter records for `path`, so the ledger rows below
/// name the file as a running engine would.
fn inode_of(path: &Path) -> Option<u64> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Some(std::fs::metadata(path).unwrap().ino())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

/// A file ledger row in BATCHING over `[start, end)` of `path`, created at
/// `at`, read when the file was 28 bytes long - with the fingerprint of
/// those bytes, as the file adapter records it (#578): recovery retires a
/// range only for a marker proven to be the cursor's file.
fn file_row(id: &str, path: &str, inode: Option<u64>, range: (u64, u64), at: &str) -> BatchRecord {
    let head = std::fs::read(path).unwrap();
    let marker = ProgressMarker::File {
        path: path.into(),
        inode,
        start_byte: range.0,
        end_byte: range.1,
        file_size: 28,
        mtime: String::new(),
        content_fingerprint: vtop_core::types::ContentFingerprint::of(&head[..28]),
        tail_fingerprint: None,
    };
    BatchRecord {
        batch_id: id.into(),
        tenant: "default".into(),
        source_type: SourceType::File,
        source_name: path.into(),
        format: TelemetryFormat::Raw,
        state: BatchState::Batching,
        progress_start: marker.clone(),
        progress_end: marker,
        object_uri: None,
        manifest_uri: None,
        object_sha256: None,
        manifest_sha256: None,
        manifest_version_id: None,
        object_size_bytes: None,
        verification_class: None,
        record_count: Some(2),
        error_message: None,
        owner: None,
        lease_expires_at: None,
        created_at: at.into(),
        updated_at: at.into(),
    }
}

async fn commit_row(store: &SqliteStateStore, rec: &BatchRecord) {
    store.save_batch_state(rec).await.unwrap();
    for st in [
        BatchState::Sealed,
        BatchState::Compressed,
        BatchState::Checksummed,
        BatchState::ObjectUploaded,
        BatchState::ManifestUploaded,
        BatchState::Verified,
        BatchState::SourceCommitted,
    ] {
        store
            .update_batch_state(&rec.batch_id, st, &BatchPatch::default())
            .await
            .unwrap();
    }
}

/// A range handed back as an engine leaves it: FAILED, then REPLAY_REQUIRED.
async fn hand_back_row(store: &SqliteStateStore, rec: &BatchRecord) {
    store.save_batch_state(rec).await.unwrap();
    store
        .mark_failed(&rec.batch_id, "upload failed")
        .await
        .unwrap();
    store
        .update_batch_state(
            &rec.batch_id,
            BatchState::ReplayRequired,
            &BatchPatch::default(),
        )
        .await
        .unwrap();
}

/// #567: a REPLAY_REQUIRED row whose bytes a later batch re-archived and
/// committed never reached a terminal state. Every restart claimed it, wrote
/// FAILED and REPLAY_REQUIRED over it, counted it as a replay and planned a
/// rewind that the committed point clamped to nothing; and retention, which
/// deletes only committed rows, kept it for ever.
///
/// Now recovery retires it - SUPERSEDED, one write, once - and so for an
/// in-flight row a crashed engine left under the same commit. The next
/// restart does not touch either, nothing is read again, and retention
/// deletes them with the committed history (after digesting them).
#[tokio::test]
async fn a_stale_replay_row_a_later_commit_covers_is_superseded_once_and_pruned() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    // Four 7-byte lines, [0,28).
    std::fs::write(&input, "line-0\nline-1\nline-2\nline-3\n").unwrap();
    let inode = inode_of(&input);
    let db = dir.path().join("state.db");
    let url = format!("sqlite://{}", db.display());
    let path = input.to_string_lossy().into_owned();
    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        // The range that failed and was handed back...
        hand_back_row(
            &store,
            &file_row("b-stale", &path, inode, (0, 14), "2026-01-01T00:00:00Z"),
        )
        .await;
        // ...a range a crashed engine had in flight...
        let inflight = file_row("b-sealed", &path, inode, (14, 28), "2026-01-01T00:00:01Z");
        store.save_batch_state(&inflight).await.unwrap();
        store
            .update_batch_state("b-sealed", BatchState::Sealed, &BatchPatch::default())
            .await
            .unwrap();
        // ...and the re-read that archived both and committed them, read later.
        commit_row(
            &store,
            &file_row("b-retry", &path, inode, (0, 28), "2026-01-01T00:00:02Z"),
        )
        .await;
    }

    let cfg = file_config(work.to_str().unwrap(), &url, vec![path.clone()], "mock");
    let streams = || StreamsConfig { streams: vec![] };
    let mut engine = Engine::new(cfg.clone(), streams()).await.unwrap();
    let summary = engine.recover().await.unwrap();
    assert_eq!(
        (
            summary.superseded,
            summary.replay_required,
            summary.still_pending
        ),
        (2, 0, 0),
        "both rows lie below the committed point: retired, not handed back ({summary:?})"
    );
    let mut stamps = Vec::new();
    for id in ["b-stale", "b-sealed"] {
        let row = engine.store.get_batch(id).await.unwrap().unwrap();
        assert_eq!(row.state, BatchState::Superseded, "{id}");
        stamps.push(row.updated_at);
    }
    let outcomes = engine.process_once(SourceType::File).await.unwrap();
    assert!(
        outcomes.is_empty(),
        "[0,28) is committed; nothing is read again: {outcomes:?}"
    );
    drop(engine);

    // The next restart leaves them alone: not claimed (a claim rewrites
    // `updated_at`), not rewritten, not counted.
    let mut engine = Engine::new(cfg, streams()).await.unwrap();
    let summary = engine.recover().await.unwrap();
    assert_eq!(
        (
            summary.superseded,
            summary.replay_required,
            summary.still_pending
        ),
        (0, 0, 0),
        "a retired row is no recovery work: {summary:?}"
    );
    for (id, stamp) in ["b-stale", "b-sealed"].into_iter().zip(&stamps) {
        let row = engine.store.get_batch(id).await.unwrap().unwrap();
        assert_eq!(row.state, BatchState::Superseded, "{id}");
        assert_eq!(&row.updated_at, stamp, "{id} was written again at restart");
    }

    // Retention deletes them, as it deletes committed history - digested
    // first - and keeps the committed row the restart cursor is read from.
    let digests = dir.path().join("digests");
    let cutoff = (chrono::Utc::now() + chrono::Duration::days(1)).to_rfc3339();
    let (deleted, digest) =
        vtop_cli::coverage::prune_with_digest(engine.store.as_ref(), &cutoff, 500, &digests)
            .await
            .unwrap();
    assert_eq!(deleted, 2, "both superseded rows are pruned");
    assert!(digest.is_some(), "and digested before they were deleted");
    for id in ["b-stale", "b-sealed"] {
        assert!(engine.store.get_batch(id).await.unwrap().is_none(), "{id}");
    }
    assert_eq!(
        engine
            .store
            .get_batch("b-retry")
            .await
            .unwrap()
            .map(|r| r.state),
        Some(BatchState::SourceCommitted)
    );
    // The coverage audit still reads them, from the digest, and finds the
    // ranges covered.
    let (digested, files) = vtop_cli::coverage::read_digests(&digests).unwrap();
    assert_eq!(files, 1);
    assert!(digested
        .iter()
        .all(|row| row.state == BatchState::Superseded));
    let mut rows = engine.store.list_batches().await.unwrap();
    rows.extend(digested);
    for report in vtop_cli::coverage::audit(rows) {
        assert!(
            !report.has_findings(),
            "the ranges are covered by the commit: {report:?}"
        );
    }
}

/// #567: only a row its source has committed past is retired. One above the
/// committed point is handed back exactly as before - rewound and read again -
/// and retired at the restart after its range has been committed.
#[tokio::test]
async fn a_replay_row_above_the_committed_point_is_still_rewound_and_retired_once_covered() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    std::fs::write(&input, "line-0\nline-1\nline-2\nline-3\n").unwrap();
    let inode = inode_of(&input);
    let db = dir.path().join("state.db");
    let url = format!("sqlite://{}", db.display());
    let path = input.to_string_lossy().into_owned();
    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        commit_row(
            &store,
            &file_row("b-first", &path, inode, (0, 14), "2026-01-01T00:00:00Z"),
        )
        .await;
        // Read after the commit, and never archived: [14,28) is lost unless
        // it is read again.
        hand_back_row(
            &store,
            &file_row("b-lost", &path, inode, (14, 28), "2026-01-01T00:00:01Z"),
        )
        .await;
    }

    let cfg = file_config(work.to_str().unwrap(), &url, vec![path.clone()], "mock");
    let streams = || StreamsConfig { streams: vec![] };
    let mut engine = Engine::new(cfg.clone(), streams()).await.unwrap();
    let summary = engine.recover().await.unwrap();
    assert_eq!(
        (summary.superseded, summary.replay_required),
        (0, 1),
        "the committed point is below the row's end: it is handed back ({summary:?})"
    );
    assert_eq!(
        engine
            .store
            .get_batch("b-lost")
            .await
            .unwrap()
            .unwrap()
            .state,
        BatchState::ReplayRequired
    );
    let outcomes = engine.process_once(SourceType::File).await.unwrap();
    assert!(outcomes.iter().all(|o| o.committed), "{outcomes:?}");
    let reread: Vec<_> = engine
        .store
        .list_batches()
        .await
        .unwrap()
        .into_iter()
        .filter(|b| b.state == BatchState::SourceCommitted && b.batch_id != "b-first")
        .collect();
    assert_eq!(reread.len(), 1, "{reread:?}");
    let ProgressMarker::File {
        start_byte,
        end_byte,
        ..
    } = &reread[0].progress_end
    else {
        panic!("a file batch carries a file marker");
    };
    assert_eq!(
        (*start_byte, *end_byte),
        (14, 28),
        "the handed-back range is read again from its start"
    );
    drop(engine);

    // Now a commit covers it, and the next restart retires it.
    let mut engine = Engine::new(cfg, streams()).await.unwrap();
    let summary = engine.recover().await.unwrap();
    assert_eq!(
        (summary.superseded, summary.replay_required),
        (1, 0),
        "{summary:?}"
    );
    assert_eq!(
        engine
            .store
            .get_batch("b-lost")
            .await
            .unwrap()
            .unwrap()
            .state,
        BatchState::Superseded
    );
    let outcomes = engine.process_once(SourceType::File).await.unwrap();
    assert!(outcomes.is_empty(), "{outcomes:?}");
}

/// #567 (review): SUPERSEDED is terminal, so a range is retired only when its
/// marker is proven to be the file the committed point belongs to. A legacy
/// marker names no inode and cannot be told from another file that lived at
/// the path - a rotated-away file's failed range, say - so it keeps the
/// replay path, exactly as before, even under a later commit at the path.
#[tokio::test]
async fn a_legacy_marker_without_an_inode_is_never_retired() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    std::fs::write(&input, "line-0\nline-1\nline-2\nline-3\n").unwrap();
    let inode = inode_of(&input);
    let db = dir.path().join("state.db");
    let url = format!("sqlite://{}", db.display());
    let path = input.to_string_lossy().into_owned();
    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        hand_back_row(
            &store,
            &file_row("b-legacy", &path, None, (0, 14), "2026-01-01T00:00:00Z"),
        )
        .await;
        commit_row(
            &store,
            &file_row("b-retry", &path, inode, (0, 28), "2026-01-01T00:00:02Z"),
        )
        .await;
    }
    let cfg = file_config(work.to_str().unwrap(), &url, vec![path.clone()], "mock");
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let summary = engine.recover().await.unwrap();
    assert_eq!(
        (summary.superseded, summary.replay_required),
        (0, 1),
        "a marker with no inode is not proven to be this file: {summary:?}"
    );
    assert_eq!(
        engine
            .store
            .get_batch("b-legacy")
            .await
            .unwrap()
            .unwrap()
            .state,
        BatchState::ReplayRequired
    );
}

/// #567 with #651: retiring a range as SUPERSEDED verifies nothing and
/// commits nothing, so it counts neither - not `verified_total`, not
/// `commits_total`, and not the size-only acceptance counter. Only the
/// retirement itself is counted, under `batches_total{state="superseded"}`.
#[tokio::test]
async fn a_retired_range_counts_no_verification_and_no_commit() {
    // A tenant no other test uses: the registry is process-wide.
    let tenant = "tenant-567-retired";
    let count = |family: &str| -> u64 {
        let mx = vtop_core::telemetry::init().expect("metrics registry");
        let l = [tenant, "file", TelemetryFormat::Raw.extension()];
        match family {
            "verified" => mx.verified_total.with_label_values(&l).get(),
            "commits" => mx.commits_total.with_label_values(&l).get(),
            "size_only" => mx
                .verification_backend_limited_total
                .with_label_values(&l)
                .get(),
            _ => mx
                .batches_total
                .with_label_values(&[l[0], l[1], l[2], "superseded"])
                .get(),
        }
    };
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    std::fs::write(&input, "line-0\nline-1\nline-2\nline-3\n").unwrap();
    let inode = inode_of(&input);
    let db = dir.path().join("state.db");
    let url = format!("sqlite://{}", db.display());
    let path = input.to_string_lossy().into_owned();
    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        let mut stale = file_row("b-stale", &path, inode, (0, 14), "2026-01-01T00:00:00Z");
        stale.tenant = tenant.into();
        hand_back_row(&store, &stale).await;
        let mut retry = file_row("b-retry", &path, inode, (0, 28), "2026-01-01T00:00:02Z");
        retry.tenant = tenant.into();
        commit_row(&store, &retry).await;
    }
    let families = ["verified", "commits", "size_only", "superseded"];
    let before = families.map(count);
    let mut cfg = file_config(work.to_str().unwrap(), &url, vec![path.clone()], "mock");
    cfg.engine.tenant = tenant.into();
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let summary = engine.recover().await.unwrap();
    assert_eq!(summary.superseded, 1, "{summary:?}");
    let after = families.map(count);
    let delta: Vec<u64> = after
        .iter()
        .zip(before.iter())
        .map(|(a, b)| a - b)
        .collect();
    assert_eq!(
        delta,
        [0, 0, 0, 1],
        "(verified, commits, size-only, superseded) counted for a retirement"
    );
}

/// #567 (review, Codex): a file rewritten in place, keeping its inode and its
/// first page but SHORTER, is a new generation, and the newest committed row -
/// the old content's, committed through byte 10000 - says nothing about it.
/// A handed-back range of the new content below that offset must keep the
/// replay path: retired as SUPERSEDED, it would be terminal with no commit
/// covering its bytes.
#[tokio::test]
async fn a_range_of_a_truncated_rewrite_is_not_retired_against_the_old_offset() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    // The new content: the old first page, then a shorter rest, 5000 bytes
    // of 10-byte lines.
    let line = |i: usize| format!("line-{:04}\n", i % 10_000);
    let content: String = (0..500).map(line).collect();
    assert_eq!(content.len(), 5_000);
    std::fs::write(&input, &content).unwrap();
    let inode = inode_of(&input);
    let first_page = vtop_core::types::ContentFingerprint::of(&content.as_bytes()[..4096]);
    let url = format!("sqlite://{}", dir.path().join("state.db").display());
    let path = input.to_string_lossy().into_owned();
    let marker = |start: u64, end: u64, size: u64| ProgressMarker::File {
        path: path.clone(),
        inode,
        start_byte: start,
        end_byte: end,
        file_size: size,
        mtime: String::new(),
        content_fingerprint: first_page.clone(),
        tail_fingerprint: None,
    };
    let row = |id: &str, m: ProgressMarker, at: &str| {
        let mut rec = file_row(id, &path, inode, (0, 0), at);
        rec.progress_start = m.clone();
        rec.progress_end = m;
        rec
    };
    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        // The old content, 10000 bytes, committed through its end...
        commit_row(
            &store,
            &row("b-old", marker(0, 10_000, 10_000), "2026-01-01T00:00:00Z"),
        )
        .await;
        // ...and a range of the new content, read after the rewrite, that
        // failed: [4000, 5000) of a 5000-byte file.
        hand_back_row(
            &store,
            &row("b-new", marker(4_000, 5_000, 5_000), "2026-01-01T00:00:01Z"),
        )
        .await;
    }
    let cfg = file_config(work.to_str().unwrap(), &url, vec![path.clone()], "mock");
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let summary = engine.recover().await.unwrap();
    assert_eq!(
        (summary.superseded, summary.replay_required),
        (0, 1),
        "the truncated rewrite's range was retired against the old content's offset: {summary:?}"
    );
    assert_eq!(
        engine
            .store
            .get_batch("b-new")
            .await
            .unwrap()
            .unwrap()
            .state,
        BatchState::ReplayRequired
    );
}

// ---- #624: a held-back VERIFIED range is settled while the engine runs -------

/// #624: recovery holds a VERIFIED range back while a lower range of its file
/// is re-read, and nothing in the running loop looked at it again: it stayed
/// VERIFIED until the next restart, which then committed it - moving the
/// committed point, and with it the read head, back below everything the
/// re-read had archived since.
///
/// Now the pass whose re-read commits past it settles it: SOURCE_COMMITTED,
/// without a restart, and without moving the source back - the next pass
/// reads nothing again.
#[tokio::test]
async fn a_held_back_verified_range_is_settled_without_a_restart_and_without_moving_back() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    // Six 7-byte lines: the failed range is [0,14), the held-back VERIFIED one
    // [14,28), and [28,42) has not been read yet.
    std::fs::write(&input, "line-0\nline-1\nline-2\nline-3\nline-4\nline-5\n").unwrap();
    let inode = inode_of(&input);
    let db = dir.path().join("state.db");
    let url = format!("sqlite://{}", db.display());
    let path = input.to_string_lossy().into_owned();

    let above = file_row("b-above", &path, inode, (14, 28), "2026-01-01T00:00:01Z");
    let object_uri = "s3://telemetry-data/x/b-above.raw.gz";
    let manifest_uri = "s3://telemetry-data/x/b-above.manifest.json";
    let object_bytes = b"archived-bytes";
    let object_digest = vtop_core::checksum::sha256_bytes(object_bytes);
    let (manifest, manifest_path) = write_recovery_manifest_with_records(
        &work,
        "b-above",
        &path,
        above.progress_end.clone(),
        object_uri,
        manifest_uri,
        object_bytes.len() as u64,
        "sha256",
        &object_digest,
        2,
    );
    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        hand_back_row(
            &store,
            &file_row("b-failed", &path, inode, (0, 14), "2026-01-01T00:00:00Z"),
        )
        .await;
        let mut above = above.clone();
        above.object_uri = Some(object_uri.into());
        above.manifest_uri = Some(manifest_uri.into());
        above.object_sha256 = Some(object_digest.clone());
        above.manifest_sha256 = Some(manifest.manifest.sha256.clone());
        above.object_size_bytes = Some(object_bytes.len() as i64);
        store.save_batch_state(&above).await.unwrap();
        for st in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
            BatchState::Verified,
        ] {
            store
                .update_batch_state("b-above", st, &BatchPatch::default())
                .await
                .unwrap();
        }
    }

    let cfg = file_config(work.to_str().unwrap(), &url, vec![path.clone()], "mock");
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let staged = dir.path().join("staged-object");
    std::fs::write(&staged, object_bytes).unwrap();
    engine
        .backend
        .put_object(
            &staged,
            object_uri,
            Some(vtop_upload::ObjectChecksum::new("sha256", &object_digest)),
        )
        .await
        .unwrap();
    engine
        .backend
        .put_manifest(
            &manifest_path,
            manifest_uri,
            Some(vtop_upload::ObjectChecksum::new(
                "sha256",
                &manifest.manifest.sha256,
            )),
        )
        .await
        .unwrap();

    let summary = engine.recover().await.unwrap();
    assert_eq!(
        (
            summary.committed,
            summary.still_pending,
            summary.replay_required
        ),
        (0, 1, 1),
        "the VERIFIED range is held back above the failed one ({summary:?})"
    );

    // The re-read from byte 0 archives the whole file and commits it; the
    // held-back range is settled in the same pass.
    let outcomes = engine.process_once(SourceType::File).await.unwrap();
    assert!(outcomes.iter().all(|o| o.committed), "{outcomes:?}");
    let held = engine.store.get_batch("b-above").await.unwrap().unwrap();
    assert_eq!(
        held.state,
        BatchState::SourceCommitted,
        "settled without a restart"
    );
    let file = &engine.adapters[&SourceType::File];
    assert_eq!(
        file.committed_through(&held.progress_end),
        Some(42),
        "settling the older range moved the committed point back"
    );
    let again = engine.process_once(SourceType::File).await.unwrap();
    assert!(
        again.is_empty(),
        "settling the older range moved the read head back: {again:?}"
    );
}

/// #624 with #651: the settle pass is a commit path for a row recovery left,
/// so a size-only acceptance at recovery's re-check is counted when the row
/// settles - once - and never at the hold, as `commit_recovered` counts it.
///
/// A FAILED `[0,7)` holds back a VERIFIED `[7,14)` whose re-check can confirm
/// only size (`MockBackend::limited`). Both rows carry the inode and content
/// fingerprint the file adapter records, so #567's identity rule lets the
/// held row settle once the re-read has committed past it. The held row's
/// tenant is its own: the re-read's pipeline batch is counted under the
/// engine's tenant, so the counter below moves for the settle alone.
#[tokio::test]
async fn a_held_back_size_only_row_counts_its_size_only_acceptance_when_it_settles() {
    let moved =
        held_back_row_settles("tenant-624-settled-size-only", StoredBy::SizeOnly, None).await;
    assert_eq!(moved, 1);
}

/// #664 on the settle path: a held-back row is counted by the class it was
/// verified with before the restart, as `commit_recovered` counts one, not
/// by what recovery's re-check found. Its re-check here can confirm only
/// size; verified strongly, it is not counted as size-only when it settles.
#[tokio::test]
async fn a_held_back_row_verified_strong_is_not_counted_size_only_when_it_settles() {
    let moved = held_back_row_settles(
        "tenant-664-settled-strong",
        StoredBy::SizeOnly,
        Some(VerificationClass::Strong),
    )
    .await;
    assert_eq!(
        moved, 0,
        "b-held was verified strongly; settling it must not count a size-only commit"
    );
}

/// And verified size-only, it is, though its re-check finds a digest match
/// (review of #664: with a size-only re-check this passed before the fix too).
#[tokio::test]
async fn a_held_back_row_verified_size_only_is_counted_size_only_when_it_settles() {
    let moved = held_back_row_settles(
        "tenant-664-settled-size-only",
        StoredBy::Digest,
        Some(VerificationClass::SizeOnly),
    )
    .await;
    assert_eq!(
        moved, 1,
        "b-held was verified size-only; settling it must count one size-only commit"
    );
}

/// The ledger and passes of the settle tests above: `b-held`, verified with
/// `class` (`None`: by a release before #664) and re-checked by size alone,
/// is held back by recovery and settled by the next pass. Asserts nothing is
/// counted at the hold and nothing after the settle, and returns how far
/// the settle moved the size-only counter of `row_tenant`.
async fn held_back_row_settles(
    row_tenant: &str,
    stored: StoredBy,
    class: Option<VerificationClass>,
) -> u64 {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    std::fs::write(&input, "line-0\nline-1\n").unwrap();
    let inode = inode_of(&input);
    let head = std::fs::read(&input).unwrap();
    let url = format!("sqlite://{}", dir.path().join("state.db").display());
    let path = input.to_string_lossy().into_owned();
    let range = |start: u64, end: u64| ProgressMarker::File {
        path: path.clone(),
        inode,
        start_byte: start,
        end_byte: end,
        file_size: 14,
        mtime: String::new(),
        content_fingerprint: vtop_core::types::ContentFingerprint::of(&head),
        tail_fingerprint: None,
    };
    let object_uri = "s3://telemetry-data/x/b-held.raw.gz";
    let manifest_uri = "s3://telemetry-data/x/b-held.manifest.json";
    // Checksums disabled, and a backend that can only confirm size - or a
    // SHA-256 the mock backend checks the stored bytes against.
    let object_bytes = b"fourteen-bytes";
    let digest = vtop_core::checksum::sha256_bytes(object_bytes);
    let (algorithm, checksum) = match stored {
        StoredBy::SizeOnly => ("none", ""),
        StoredBy::Digest => ("sha256", digest.as_str()),
    };
    let (manifest, manifest_path) = write_recovery_manifest(
        &work,
        "b-held",
        &path,
        range(7, 14),
        object_uri,
        manifest_uri,
        14,
        algorithm,
        checksum,
    );
    let row = |id: &str, start: u64, end: u64, at: &str| BatchRecord {
        batch_id: id.into(),
        tenant: row_tenant.into(),
        source_type: SourceType::File,
        source_name: path.clone(),
        format: TelemetryFormat::Raw,
        state: BatchState::Batching,
        progress_start: range(start, end),
        progress_end: range(start, end),
        object_uri: None,
        manifest_uri: None,
        object_sha256: None,
        manifest_sha256: None,
        manifest_version_id: None,
        object_size_bytes: None,
        verification_class: None,
        record_count: Some(1),
        error_message: None,
        owner: None,
        lease_expires_at: None,
        created_at: at.into(),
        updated_at: at.into(),
    };
    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        hand_back_row(&store, &row("b-below", 0, 7, "2026-01-01T00:00:00Z")).await;
        let mut held = row("b-held", 7, 14, "2026-01-01T00:00:01Z");
        held.object_uri = Some(object_uri.into());
        held.manifest_uri = Some(manifest_uri.into());
        held.object_sha256 = Some(checksum.to_owned());
        held.manifest_sha256 = Some(manifest.manifest.sha256.clone());
        held.object_size_bytes = Some(14);
        store.save_batch_state(&held).await.unwrap();
        for st in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
        ] {
            store
                .update_batch_state("b-held", st, &BatchPatch::default())
                .await
                .unwrap();
        }
        match class {
            Some(class) => store.mark_verified("b-held", class).await.unwrap(),
            None => store
                .update_batch_state("b-held", BatchState::Verified, &BatchPatch::default())
                .await
                .unwrap(),
        }
    }

    // testkit's config has require_strong_verification: false.
    let mut cfg = file_config(work.to_str().unwrap(), &url, vec![path.clone()], "mock");
    cfg.engine.tenant = "tenant-624-settle-engine".into();
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let (object_checksum, manifest_checksum) = match stored {
        StoredBy::SizeOnly => {
            engine.backend = std::sync::Arc::new(vtop_upload::MockBackend::limited());
            (None, None)
        }
        StoredBy::Digest => (
            Some(vtop_upload::ObjectChecksum::new("sha256", &digest)),
            Some(vtop_upload::ObjectChecksum::new(
                "sha256",
                &manifest.manifest.sha256,
            )),
        ),
    };
    let staged = dir.path().join("staged");
    std::fs::write(&staged, object_bytes).unwrap();
    engine
        .backend
        .put_object(&staged, object_uri, object_checksum)
        .await
        .unwrap();
    engine
        .backend
        .put_manifest(&manifest_path, manifest_uri, manifest_checksum)
        .await
        .unwrap();

    let before = size_only_count(row_tenant);
    let summary = engine.recover().await.unwrap();
    assert_eq!(
        (
            summary.committed,
            summary.still_pending,
            summary.replay_required
        ),
        (0, 1, 1),
        "b-held passes its re-check ({stored:?}) and is held back: {summary:?}"
    );
    assert_eq!(
        size_only_count(row_tenant) - before,
        0,
        "nothing is counted at the hold"
    );

    // The re-read from byte 0 commits past b-held, and the settle pass
    // commits it in the same pass.
    let outcomes = engine.process_once(SourceType::File).await.unwrap();
    assert!(outcomes.iter().all(|o| o.committed), "{outcomes:?}");
    assert_eq!(
        engine
            .store
            .get_batch("b-held")
            .await
            .unwrap()
            .unwrap()
            .state,
        BatchState::SourceCommitted,
        "the held-back row settled without a restart"
    );
    let moved = size_only_count(row_tenant) - before;
    // And nothing more on a later pass.
    engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(
        size_only_count(row_tenant) - before,
        moved,
        "a settled row is counted once, at its commit"
    );
    moved
}

/// #659 review (Codex P1): a skipped commit must have content evidence.
///
/// Two VERIFIED rows of one inode from two generations: an older one through
/// byte 60 recorded with no fingerprints (a legacy marker), and one read after
/// a copytruncate, through byte 21. The file then regrows to 84 bytes before
/// recovery. Recovery commits the older row first: that sets the committed
/// point to 60 for this inode, with no content on record. The newer row's
/// commit was then skipped as "superseded" - `None` content cannot be shown
/// to differ - and the read head stayed at 60, so bytes 21..60 of the new
/// generation were never archived. Unknown content must never suppress a
/// commit: the newer row's commit moves the cursor back to 21, as before
/// #624, and the next pass reads 21..84.
#[tokio::test]
async fn an_unfingerprinted_older_commit_never_suppresses_a_newer_generations() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    // The new generation, regrown to 84 bytes: twelve 7-byte lines.
    let content: String = (0..12).map(|i| format!("new-{i:02}\n")).collect();
    assert_eq!(content.len(), 84);
    std::fs::write(&input, &content).unwrap();
    let inode = inode_of(&input);
    let url = format!("sqlite://{}", dir.path().join("state.db").display());
    let path = input.to_string_lossy().into_owned();
    // The old generation's row: [0,60), read when the file was 60 bytes, by
    // a build that recorded no fingerprints.
    let old = ProgressMarker::File {
        path: path.clone(),
        inode,
        start_byte: 0,
        end_byte: 60,
        file_size: 60,
        mtime: String::new(),
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    // The new generation's row: [0,21), read when the file was 21 bytes.
    let new = ProgressMarker::File {
        path: path.clone(),
        inode,
        start_byte: 0,
        end_byte: 21,
        file_size: 21,
        mtime: String::new(),
        content_fingerprint: vtop_core::types::ContentFingerprint::of(&content.as_bytes()[..21]),
        tail_fingerprint: vtop_core::types::ContentFingerprint::of(&content.as_bytes()[..21]),
    };
    let staged = dir.path().join("staged");
    std::fs::write(&staged, b"archived-bytes").unwrap();
    let digest = vtop_core::checksum::sha256_bytes(b"archived-bytes");
    let mut cfg = file_config(work.to_str().unwrap(), &url, vec![path.clone()], "mock");
    cfg.batching.max_batch_age_seconds = 3_600;
    let mut verified_rows = Vec::new();
    for (id, marker, at) in [
        ("b-old-gen", old, "2026-01-01T00:00:00Z"),
        ("b-new-gen", new, "2026-01-01T00:00:01Z"),
    ] {
        let object_uri = format!("s3://telemetry-data/x/{id}.raw.gz");
        let manifest_uri = format!("s3://telemetry-data/x/{id}.manifest.json");
        let (manifest, manifest_path) = write_recovery_manifest(
            &work,
            id,
            &path,
            marker.clone(),
            &object_uri,
            &manifest_uri,
            14,
            "sha256",
            &digest,
        );
        let mut rec = file_row(id, &path, inode, (0, 0), at);
        rec.progress_start = marker.clone();
        rec.progress_end = marker;
        rec.object_uri = Some(object_uri.clone());
        rec.manifest_uri = Some(manifest_uri.clone());
        rec.object_sha256 = Some(digest.clone());
        rec.manifest_sha256 = Some(manifest.manifest.sha256.clone());
        rec.object_size_bytes = Some(14);
        verified_rows.push((rec, object_uri, manifest_uri, manifest, manifest_path));
    }
    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        for (rec, ..) in &verified_rows {
            store.save_batch_state(rec).await.unwrap();
            for st in [
                BatchState::Sealed,
                BatchState::Compressed,
                BatchState::Checksummed,
                BatchState::ObjectUploaded,
                BatchState::ManifestUploaded,
                BatchState::Verified,
            ] {
                store
                    .update_batch_state(&rec.batch_id, st, &BatchPatch::default())
                    .await
                    .unwrap();
            }
        }
    }
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    for (_, object_uri, manifest_uri, manifest, manifest_path) in &verified_rows {
        engine
            .backend
            .put_object(
                &staged,
                object_uri,
                Some(vtop_upload::ObjectChecksum::new("sha256", &digest)),
            )
            .await
            .unwrap();
        engine
            .backend
            .put_manifest(
                manifest_path,
                manifest_uri,
                Some(vtop_upload::ObjectChecksum::new(
                    "sha256",
                    &manifest.manifest.sha256,
                )),
            )
            .await
            .unwrap();
    }
    let summary = engine.recover().await.unwrap();
    assert_eq!(summary.committed, 2, "both rows commit: {summary:?}");

    // The next pass must read the new generation on from byte 21.
    engine.process_once(SourceType::File).await.unwrap();
    let mut covered: Vec<(u64, u64)> = engine
        .store
        .list_batches()
        .await
        .unwrap()
        .into_iter()
        .filter(|b| b.state == BatchState::SourceCommitted && b.batch_id != "b-old-gen")
        .map(|b| match (&b.progress_start, &b.progress_end) {
            (ProgressMarker::File { start_byte, .. }, ProgressMarker::File { end_byte, .. }) => {
                (*start_byte, *end_byte)
            }
            other => panic!("a file batch carries file markers: {other:?}"),
        })
        .collect();
    covered.sort_unstable();
    let mut through = 0;
    for &(start, end) in &covered {
        assert!(
            start <= through,
            "bytes {through}..{start} of the new generation were never archived: {covered:?}"
        );
        through = through.max(end);
    }
    assert_eq!(
        through, 84,
        "the new generation is archived only through {through}: {covered:?}"
    );
}

// ---- #664: every combination recovery meets, and what it says ---------------

/// Every log line this test process writes, as JSON. One process-wide capture:
/// a capture scoped to one test thread misses events when other tests in the
/// process race it for the tracing callsite cache. Lines are told apart by
/// batch id, which each test below makes unique.
fn logs() -> &'static Captured {
    static LOGS: std::sync::OnceLock<Captured> = std::sync::OnceLock::new();
    LOGS.get_or_init(|| {
        let captured = Captured::default();
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .json()
                .with_max_level(tracing::Level::INFO)
                .with_writer(captured.clone())
                .finish(),
        )
        .expect("the only global subscriber in this binary");
        captured
    })
}

#[derive(Clone, Default)]
struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl Captured {
    /// The captured lines about batch `id`.
    fn about(&self, id: &str) -> Vec<String> {
        let field = format!(r#""batch_id":"{id}""#);
        String::from_utf8_lossy(&self.0.lock().unwrap())
            .lines()
            .filter(|line| line.contains(&field))
            .map(str::to_owned)
            .collect()
    }
}

impl std::io::Write for Captured {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Asserts one captured line about `id` carries every part.
fn assert_said(id: &str, parts: &[&str]) {
    let lines = logs().about(id);
    assert!(
        lines
            .iter()
            .any(|line| parts.iter().all(|part| line.contains(part))),
        "no line about {id} carries all of {parts:?}; lines about it:\n{}",
        lines.join("\n")
    );
}

/// Asserts no captured line about `id` contains `part`.
fn assert_not_said(id: &str, part: &str) {
    let lines = logs().about(id);
    assert!(
        lines.iter().all(|line| !line.contains(part)),
        "a line about {id} says {part:?}:\n{}",
        lines.join("\n")
    );
}

/// What recovery of one VERIFIED row must do, for each class it may have
/// been verified with, each result its re-check may find, and each setting of
/// `upload.require_strong_verification` (#664):
///
/// - strict, re-check size-only: not committed, replayed, nothing counted;
///   the class is kept on the handed-back row;
/// - otherwise committed, and counted as size-only exactly when the row was
///   verified size-only - or, with no class, when the re-check was;
/// - each says what happened in the log: an unknown class, or a class the
///   re-check disagrees with, by name, with the batch id, class and re-check
///   result as fields.
async fn recovery_matrix<'a, F>(url: &str, label: &str, reset: F)
where
    F: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>>,
{
    let _ = logs();
    let classes = [
        None,
        Some(VerificationClass::Strong),
        Some(VerificationClass::SizeOnly),
    ];
    for strict in [false, true] {
        for class in classes {
            for stored in [StoredBy::SizeOnly, StoredBy::Digest] {
                reset().await;
                let class_name = class.map_or("unknown", VerificationClass::as_str);
                let id = format!(
                    "b664-{label}-{}-{class_name}-{stored:?}",
                    if strict { "strict" } else { "optout" }
                );
                let tenant = format!("tenant-{id}");
                let dir = tempfile::tempdir().unwrap();
                // A Postgres ledger is reset per case; a SQLite one is new.
                let url = if url.is_empty() {
                    format!("sqlite://{}", dir.path().join("state.db").display())
                } else {
                    url.to_owned()
                };
                let result = match stored {
                    StoredBy::SizeOnly if strict => "failed",
                    StoredBy::SizeOnly => "size_only",
                    StoredBy::Digest => "strong",
                };
                let rechecks = || {
                    vtop_core::telemetry::init()
                        .expect("metrics registry")
                        .recovery_rechecks_total
                        .with_label_values(&[
                            tenant.as_str(),
                            "file",
                            TelemetryFormat::Raw.extension(),
                            class_name,
                            result,
                        ])
                        .get()
                };
                let rechecks_before = rechecks();
                let before = size_only_count(&tenant);
                let mut engine =
                    verified_ledger(dir.path(), &url, &id, &tenant, stored, class, false, strict)
                        .await;
                let summary = engine.recover().await.unwrap();
                let moved = size_only_count(&tenant) - before;
                let row = engine.store.get_batch(&id).await.unwrap().unwrap();
                let case = format!("{id}: {summary:?}");
                assert_eq!(
                    rechecks() - rechecks_before,
                    1,
                    "{case}: vtop_recovery_rechecks_total{{verification_class={class_name:?}, \
                     result={result:?}}} counts the re-check"
                );
                if strict && matches!(stored, StoredBy::SizeOnly) {
                    assert_eq!(
                        (summary.committed, summary.replay_required, moved),
                        (0, 1, 0),
                        "{case}"
                    );
                    assert_eq!(
                        (row.state, row.verification_class),
                        (BatchState::ReplayRequired, class),
                        "{case}"
                    );
                    assert_said(
                        &id,
                        &[
                            "failed storage re-check; replaying",
                            &format!(r#""verification_class":"{class_name}""#),
                        ],
                    );
                    continue;
                }
                let counted = match class {
                    Some(VerificationClass::SizeOnly) => 1,
                    Some(VerificationClass::Strong) => 0,
                    None => u64::from(matches!(stored, StoredBy::SizeOnly)),
                };
                assert_eq!((summary.committed, moved), (1, counted), "{case}");
                assert_eq!(
                    (row.state, row.verification_class),
                    (BatchState::SourceCommitted, class),
                    "{case}"
                );
                let recheck = match stored {
                    StoredBy::SizeOnly => "size_only",
                    StoredBy::Digest => "strong",
                };
                let fields = [
                    format!(r#""verification_class":"{class_name}""#),
                    format!(r#""recheck":"{recheck}""#),
                ];
                assert_said(
                    &id,
                    &["recovered: source_committed", &fields[0], &fields[1]],
                );
                match (class, stored) {
                    (None, _) => assert_said(
                        &id,
                        &[
                            &format!("verification class unknown for batch {id}"),
                            "counts its commit by that",
                            &fields[0],
                            &fields[1],
                        ],
                    ),
                    (Some(VerificationClass::Strong), StoredBy::SizeOnly) => assert_said(
                        &id,
                        &[
                            "was verified strong, but recovery's re-check could confirm only its size",
                            "vtopctl scrub",
                            r#""level":"WARN""#,
                            &fields[0],
                        ],
                    ),
                    (Some(VerificationClass::SizeOnly), StoredBy::Digest) => assert_said(
                        &id,
                        &[
                            "was verified size-only, and recovery's re-check now matched",
                            "counted as size-only",
                            &fields[0],
                        ],
                    ),
                    // Class and re-check agree: nothing more to say.
                    _ => {
                        assert_not_said(&id, "verification class unknown");
                        assert_not_said(&id, "re-check could confirm only");
                        assert_not_said(&id, "re-check now matched");
                    }
                }
                if counted == 1 {
                    assert_said(
                        &id,
                        &["source_committed on a size-only verification", &fields[0]],
                    );
                } else {
                    assert_not_said(&id, "source_committed on a size-only verification");
                }
            }
        }
    }
}

#[tokio::test]
async fn recovery_counts_and_reports_every_class_recheck_and_mode() {
    recovery_matrix("", "sqlite", || Box::pin(async {})).await;
}

/// A row recovery re-checked and found good, but whose source commit failed,
/// is committed by a later pass (#659's settle pass) - and counted there by
/// the class it was verified with, not by the re-check (#664).
#[tokio::test]
async fn a_recovered_row_whose_commit_failed_is_counted_by_its_class_when_it_settles() {
    let _ = logs();
    for (class, stored, expected) in [
        (Some(VerificationClass::Strong), StoredBy::SizeOnly, 0),
        (Some(VerificationClass::SizeOnly), StoredBy::Digest, 1),
        (None, StoredBy::SizeOnly, 1),
        (None, StoredBy::Digest, 0),
    ] {
        let class_name = class.map_or("unknown", VerificationClass::as_str);
        let id = format!("b664-settle-recovered-{class_name}-{stored:?}");
        let tenant = format!("tenant-{id}");
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("state.db").display());
        let mut engine =
            verified_ledger(dir.path(), &url, &id, &tenant, stored, class, false, false).await;
        let fails = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(1));
        let adapter = engine.adapters.remove(&SourceType::File).unwrap();
        engine.adapters.insert(
            SourceType::File,
            Box::new(FlakyCommit {
                inner: adapter,
                fails: fails.clone(),
            }),
        );
        let before = size_only_count(&tenant);
        let summary = engine.recover().await.unwrap();
        assert_eq!(
            (summary.committed, summary.still_pending),
            (0, 1),
            "{id}: the recovery commit failed: {summary:?}"
        );
        assert_eq!(size_only_count(&tenant) - before, 0, "{id}: nothing yet");
        engine.process_once(SourceType::File).await.unwrap();
        let row = engine.store.get_batch(&id).await.unwrap().unwrap();
        assert_eq!(
            (row.state, row.verification_class),
            (BatchState::SourceCommitted, class),
            "{id}"
        );
        assert_eq!(
            size_only_count(&tenant) - before,
            expected,
            "{id}: counted at the settle by its class"
        );
        assert_said(
            &id,
            &[
                "source_committed (an earlier pass left it VERIFIED)",
                &format!(r#""verification_class":"{class_name}""#),
            ],
        );
        engine.process_once(SourceType::File).await.unwrap();
        assert_eq!(size_only_count(&tenant) - before, expected, "{id}: once");
    }
}

/// A batch the running engine verified size-only, whose source commit
/// failed, is counted once - at its verification - and not again when a later
/// pass settles it (#659), though the ledger now says size-only (#664).
#[tokio::test]
async fn a_pipeline_row_whose_commit_failed_is_not_counted_again_when_it_settles() {
    let tenant = "tenant-664-pipeline-settle";
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    std::fs::write(&input, "line-0\nline-1\n").unwrap();
    let url = format!("sqlite://{}", dir.path().join("state.db").display());
    let mut cfg = file_config(
        work.to_str().unwrap(),
        &url,
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    cfg.engine.tenant = tenant.into();
    // Checksums off, under the opt-out testkit sets: size-only.
    cfg.checksum.algorithm = vtop_core::types::ChecksumAlgorithm::None;
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let adapter = engine.adapters.remove(&SourceType::File).unwrap();
    engine.adapters.insert(
        SourceType::File,
        Box::new(FlakyCommit {
            inner: adapter,
            fails: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        }),
    );
    let before = size_only_count(tenant);
    let outcomes = engine.process_once(SourceType::File).await.unwrap();
    assert!(!outcomes[0].committed, "{outcomes:?}");
    assert_eq!(
        size_only_count(tenant) - before,
        1,
        "counted at verification"
    );
    let rows = engine.store.list_batches().await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| (row.state, row.verification_class))
            .collect::<Vec<_>>(),
        vec![(BatchState::Verified, Some(VerificationClass::SizeOnly))]
    );
    engine.process_once(SourceType::File).await.unwrap();
    let row = engine
        .store
        .get_batch(&rows[0].batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, BatchState::SourceCommitted);
    assert_eq!(
        size_only_count(tenant) - before,
        1,
        "the settle pass does not count a pipeline row's verification again"
    );
}

/// A VERIFIED row recovery holds back under a failed lower range is counted
/// at no restart, whatever its class (#664 with #629's review).
#[tokio::test]
async fn a_held_back_row_is_counted_at_no_restart_whatever_its_class() {
    for class in [
        None,
        Some(VerificationClass::Strong),
        Some(VerificationClass::SizeOnly),
    ] {
        let class_name = class.map_or("unknown", VerificationClass::as_str);
        let id = format!("b664-held-{class_name}");
        let tenant = format!("tenant-{id}");
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("state.db").display());
        let before = size_only_count(&tenant);
        let mut engine = verified_ledger(
            dir.path(),
            &url,
            &id,
            &tenant,
            StoredBy::SizeOnly,
            class,
            true,
            false,
        )
        .await;
        for pass in 1..=2 {
            let summary = engine.recover().await.unwrap();
            assert_eq!(summary.committed, 0, "{id} pass {pass}: held back");
            let row = engine.store.get_batch(&id).await.unwrap().unwrap();
            assert_eq!(
                (row.state, row.verification_class),
                (BatchState::Verified, class),
                "{id} pass {pass}"
            );
            assert_eq!(size_only_count(&tenant) - before, 0, "{id} pass {pass}");
        }
    }
}

/// A row verified size-only that later failed and was handed back, then
/// retired as SUPERSEDED: the retirement counts nothing - not the size-only
/// counter either - and the row keeps its class (#664 with #567).
#[tokio::test]
async fn a_retired_row_verified_size_only_counts_nothing() {
    let tenant = "tenant-664-retired-size-only";
    let size_only = || size_only_count(tenant);
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let input = dir.path().join("in.log");
    std::fs::write(&input, "line-0\nline-1\nline-2\nline-3\n").unwrap();
    let inode = inode_of(&input);
    let url = format!("sqlite://{}", dir.path().join("state.db").display());
    let path = input.to_string_lossy().into_owned();
    {
        let store = SqliteStateStore::connect(&url).await.unwrap();
        let mut stale = file_row("b664-stale", &path, inode, (0, 14), "2026-01-01T00:00:00Z");
        stale.tenant = tenant.into();
        store.save_batch_state(&stale).await.unwrap();
        for st in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
        ] {
            store
                .update_batch_state("b664-stale", st, &BatchPatch::default())
                .await
                .unwrap();
        }
        store
            .mark_verified("b664-stale", VerificationClass::SizeOnly)
            .await
            .unwrap();
        // Its re-check failed: handed back, as recovery leaves it.
        store
            .mark_failed("b664-stale", "storage re-check failed")
            .await
            .unwrap();
        store
            .update_batch_state(
                "b664-stale",
                BatchState::ReplayRequired,
                &BatchPatch::default(),
            )
            .await
            .unwrap();
        let mut retry = file_row("b664-retry", &path, inode, (0, 28), "2026-01-01T00:00:02Z");
        retry.tenant = tenant.into();
        commit_row(&store, &retry).await;
    }
    let before = size_only();
    let mut cfg = file_config(work.to_str().unwrap(), &url, vec![path.clone()], "mock");
    cfg.engine.tenant = tenant.into();
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let summary = engine.recover().await.unwrap();
    assert_eq!(summary.superseded, 1, "{summary:?}");
    let row = engine.store.get_batch("b664-stale").await.unwrap().unwrap();
    assert_eq!(
        (row.state, row.verification_class),
        (BatchState::Superseded, Some(VerificationClass::SizeOnly))
    );
    assert_eq!(
        size_only() - before,
        0,
        "a retirement counts no size-only commit"
    );
}

/// Fails the first `fails` source commits, then commits; everything else is
/// the wrapped adapter's.
struct FlakyCommit {
    inner: Box<dyn vtop_adapters::base::SourceAdapter>,
    fails: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl vtop_adapters::base::SourceAdapter for FlakyCommit {
    async fn discover_sources(
        &self,
    ) -> Result<Vec<vtop_adapters::base::DiscoveredSource>, vtop_core::errors::VtopError> {
        self.inner.discover_sources().await
    }
    async fn read_batch_candidates(
        &mut self,
        source: &vtop_adapters::base::DiscoveredSource,
        records: usize,
        bytes: usize,
        wait: std::time::Duration,
    ) -> Result<Vec<vtop_adapters::base::ReadResult>, vtop_core::errors::VtopError> {
        self.inner
            .read_batch_candidates(source, records, bytes, wait)
            .await
    }
    // Forwarded too: the file source overrides it, and the tests must read
    // through the production path, not the trait's default.
    async fn read_all_batch_candidates(
        &mut self,
        sources: &[vtop_adapters::base::DiscoveredSource],
        records: usize,
        bytes: usize,
        wait: std::time::Duration,
    ) -> Result<vtop_adapters::base::AdapterReadReport, vtop_core::errors::VtopError> {
        self.inner
            .read_all_batch_candidates(sources, records, bytes, wait)
            .await
    }
    async fn commit_progress(
        &mut self,
        marker: &ProgressMarker,
    ) -> Result<(), vtop_core::errors::VtopError> {
        use std::sync::atomic::Ordering;
        // One adapter, driven by one engine at a time: no race to guard.
        let left = self.fails.load(Ordering::SeqCst);
        if left > 0 {
            self.fails.store(left - 1, Ordering::SeqCst);
            return Err(vtop_core::errors::VtopError::Source(
                "simulated source commit failure".into(),
            ));
        }
        self.inner.commit_progress(marker).await
    }
    async fn replay_from_marker(
        &mut self,
        marker: &ProgressMarker,
    ) -> Result<(), vtop_core::errors::VtopError> {
        self.inner.replay_from_marker(marker).await
    }
    fn read_position(&self, marker: &ProgressMarker) -> Option<i128> {
        self.inner.read_position(marker)
    }
    fn committed_through(&self, marker: &ProgressMarker) -> Option<i128> {
        self.inner.committed_through(marker)
    }
    async fn replay_from_markers(
        &mut self,
        markers: &[ProgressMarker],
    ) -> Vec<Result<(), vtop_core::errors::VtopError>> {
        self.inner.replay_from_markers(markers).await
    }
    fn source_type(&self) -> SourceType {
        self.inner.source_type()
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self.inner.as_any_mut()
    }
}

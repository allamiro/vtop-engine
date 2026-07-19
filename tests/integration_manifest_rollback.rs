//! Integration tests for #135: manifest version pinning defeats rollback,
//! deletion fails closed, and legacy rows still reject replayed manifests.
//!
//! The MAC (#67) proves a manifest is authentic; these tests cover freshness:
//! an attacker with write access who overwrites or deletes the manifest's
//! current key must not be able to change what recovery trusts.

use std::io::Write;
use std::path::{Path, PathBuf};
use vtop_cli::testkit::file_config;
use vtop_cli::Engine;
use vtop_core::config::StreamsConfig;
use vtop_core::manifest::{ManifestBuilder, VtopManifest};
use vtop_core::state_machine::BatchState;
use vtop_core::types::{CompressionType, ProgressMarker, SourceType, TelemetryFormat};
use vtop_state::{BatchPatch, BatchRecord, SqliteStateStore, StateStore};

#[allow(clippy::too_many_arguments)]
fn build_manifest(
    work_dir: &Path,
    batch_id: &str,
    source_name: &str,
    progress: ProgressMarker,
    object_uri: &str,
    manifest_uri: &str,
    object_size: u64,
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
        object_checksum_algorithm: "sha256".into(),
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

struct Scenario {
    engine: Engine,
    batch_id: &'static str,
    manifest_uri: &'static str,
    /// The manifest the ledger trusts and its stored version id.
    pinned_version: Option<String>,
}

/// Seed a VERIFIED-but-uncommitted batch whose object and manifest exist in
/// the mock backend, recording (or omitting) the manifest's stored version.
async fn seed(
    dir: &Path,
    batch_id: &'static str,
    object_uri: &'static str,
    manifest_uri: &'static str,
    pin_version: bool,
) -> (Scenario, VtopManifest) {
    let work = dir.join(format!("work-{batch_id}"));
    let input = dir.join(format!("{batch_id}.log"));
    {
        let mut f = std::fs::File::create(&input).unwrap();
        writeln!(f, "line-0").unwrap();
    }
    let db = dir.join(format!("{batch_id}.db"));
    let url = format!("sqlite://{}", db.display());

    let marker = ProgressMarker::File {
        path: input.to_string_lossy().into_owned(),
        inode: None,
        start_byte: 0,
        end_byte: 7,
        file_size: 7,
        mtime: String::new(),
    };
    let object_bytes = b"archived-bytes";
    let object_digest = vtop_core::checksum::sha256_bytes(object_bytes);
    let (manifest, manifest_path) = build_manifest(
        &work,
        batch_id,
        &input.to_string_lossy(),
        marker.clone(),
        object_uri,
        manifest_uri,
        object_bytes.len() as u64,
        &object_digest,
        1,
    );

    let cfg = file_config(
        work.to_str().unwrap(),
        &url,
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    let engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();

    let staged = dir.join(format!("{batch_id}.object"));
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
    let stored = engine
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
    let version_id = stored.version_id.expect("mock backend assigns versions");

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
        object_sha256: Some(object_digest),
        manifest_sha256: Some(manifest.manifest.sha256.clone()),
        manifest_version_id: None,
        object_size_bytes: Some(object_bytes.len() as i64),
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
        ] {
            store
                .update_batch_state(batch_id, st, &BatchPatch::default())
                .await
                .unwrap();
        }
        // The manifest-upload transition carries the stored version, exactly
        // as the live pipeline records it.
        let man_patch = BatchPatch {
            manifest_version_id: pin_version.then(|| version_id.clone()),
            ..Default::default()
        };
        store
            .update_batch_state(batch_id, BatchState::ManifestUploaded, &man_patch)
            .await
            .unwrap();
        store
            .update_batch_state(batch_id, BatchState::Verified, &BatchPatch::default())
            .await
            .unwrap();
    }

    (
        Scenario {
            engine,
            batch_id,
            manifest_uri,
            pinned_version: pin_version.then_some(version_id),
        },
        manifest,
    )
}

/// Overwrite the manifest's current key with a different validly-self-hashed
/// manifest for the same batch — the rollback/replay shape (#135). Returns the
/// replacement's hash so tests can prove which one recovery trusted.
async fn overwrite_current_manifest(sc: &Scenario, dir: &Path, replayed_count: usize) -> String {
    let attack_work = dir.join(format!("attack-{}", sc.batch_id));
    let marker = ProgressMarker::File {
        path: "attacker".into(),
        inode: None,
        start_byte: 0,
        end_byte: 1,
        file_size: 1,
        mtime: String::new(),
    };
    let (replayed, replayed_path) = build_manifest(
        &attack_work,
        sc.batch_id,
        "attacker",
        marker,
        "s3://telemetry-data/x/other-object.raw.gz",
        sc.manifest_uri,
        1,
        "00",
        replayed_count,
    );
    sc.engine
        .backend
        .put_manifest(&replayed_path, sc.manifest_uri, None)
        .await
        .unwrap();
    replayed.manifest.sha256
}

#[tokio::test]
async fn pinned_version_neutralizes_current_key_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let (mut sc, trusted) = seed(
        dir.path(),
        "b-pin-rollback",
        "s3://telemetry-data/x/b-pin-rollback.raw.gz",
        "s3://telemetry-data/x/b-pin-rollback.manifest.json",
        true,
    )
    .await;
    let replayed_sha = overwrite_current_manifest(&sc, dir.path(), 999).await;
    assert_ne!(replayed_sha, trusted.manifest.sha256);

    // Recovery reads the pinned immutable version, so the overwritten current
    // key changes nothing: the batch commits on the manifest it verified.
    let summary = sc.engine.recover().await.unwrap();
    assert_eq!(summary.committed, 1, "pinned recovery commits");
    let got = sc
        .engine
        .store
        .get_batch(sc.batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.state, BatchState::SourceCommitted);
    assert_eq!(got.manifest_version_id, sc.pinned_version);
}

#[tokio::test]
async fn deleted_pinned_version_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let (mut sc, _trusted) = seed(
        dir.path(),
        "b-pin-deleted",
        "s3://telemetry-data/x/b-pin-deleted.raw.gz",
        "s3://telemetry-data/x/b-pin-deleted.manifest.json",
        true,
    )
    .await;

    // Delete the manifest object (all versions). The pinned read must fail
    // and recovery must refuse to commit — never fall back to a current key.
    sc.engine
        .backend
        .delete_object(sc.manifest_uri)
        .await
        .unwrap();
    let summary = sc.engine.recover().await.unwrap();
    assert_eq!(summary.committed, 0, "deleted pin must not commit");
    let got = sc
        .engine
        .store
        .get_batch(sc.batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        got.state,
        BatchState::ReplayRequired,
        "uncommitted batch is flagged for replay, source progress untouched"
    );
}

#[tokio::test]
async fn legacy_row_without_version_rejects_replayed_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let (mut sc, _trusted) = seed(
        dir.path(),
        "b-legacy-replay",
        "s3://telemetry-data/x/b-legacy-replay.raw.gz",
        "s3://telemetry-data/x/b-legacy-replay.manifest.json",
        false,
    )
    .await;
    overwrite_current_manifest(&sc, dir.path(), 999).await;

    // No pinned version: recovery reads the current key and finds a validly
    // self-hashed manifest that does not match the ledger's recorded hash.
    // The binding check must reject it.
    let summary = sc.engine.recover().await.unwrap();
    assert_eq!(summary.committed, 0, "replayed manifest must not commit");
    let got = sc
        .engine
        .store
        .get_batch(sc.batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.state, BatchState::ReplayRequired);
}

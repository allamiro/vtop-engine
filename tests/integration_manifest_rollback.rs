//! Integration tests for #135: manifest version pinning defeats rollback,
//! deletion fails closed, and legacy rows still reject replayed manifests.
//!
//! The MAC (#67) proves a manifest is authentic; these tests cover freshness:
//! an attacker with write access who overwrites or deletes the manifest's
//! current key must not be able to change what recovery trusts.
//!
//! #671: a manifest from a newer release is reported as that and left
//! pending, and a 0.4 manifest's unknown fields do not fail verification.

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
    seed_version(
        dir,
        batch_id,
        object_uri,
        manifest_uri,
        pin_version,
        "0.3",
        true,
    )
    .await
}

/// [`seed`], with the stored manifest relabelled `version` and, for any
/// version but 0.3, carrying a field 0.3 does not have, sealed by the 0.4
/// canonical rule (sorted-key compact JSON of the stored document). The row
/// stops at MANIFEST_UPLOADED unless `verified`.
async fn seed_version(
    dir: &Path,
    batch_id: &'static str,
    object_uri: &'static str,
    manifest_uri: &'static str,
    pin_version: bool,
    version: &str,
    verified: bool,
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
        content_fingerprint: None,
        tail_fingerprint: None,
    };
    let object_bytes = b"archived-bytes";
    let object_digest = vtop_core::checksum::sha256_bytes(object_bytes);
    let (mut manifest, manifest_path) = build_manifest(
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

    if version != "0.3" {
        manifest.version = version.into();
        let mut value = serde_json::to_value(&manifest).unwrap();
        value["extension"] = serde_json::json!({"future_field": true});
        value["manifest"]["sha256"] = serde_json::json!("");
        let digest = vtop_core::checksum::sha256_bytes(&serde_json::to_vec(&value).unwrap());
        manifest.manifest.sha256 = digest.clone();
        value["manifest"]["sha256"] = serde_json::json!(digest);
        std::fs::write(&manifest_path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    }

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
        if verified {
            store
                .update_batch_state(batch_id, BatchState::Verified, &BatchPatch::default())
                .await
                .unwrap();
        }
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
        content_fingerprint: None,
        tail_fingerprint: None,
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

/// A VERIFIED batch whose manifest is from a newer release (#671) is neither
/// committed nor marked for replay: before, it failed the re-check as
/// tampered and was marked FAILED and REPLAY_REQUIRED, so an upgraded engine
/// could no longer commit it. The ledger row is left exactly as it was.
#[tokio::test]
async fn a_newer_manifest_stays_pending_without_replay_or_commit() {
    let dir = tempfile::tempdir().unwrap();
    let (mut sc, _) = seed(
        dir.path(),
        "future-version",
        "s3://telemetry-data/future.raw",
        "s3://telemetry-data/future.manifest.json",
        false,
    )
    .await;
    let staged = dir.path().join("future.json");
    std::fs::write(
        &staged,
        br#"{"protocol":"VTOP","version":"0.9","future":"unknown"}"#,
    )
    .unwrap();
    sc.engine
        .backend
        .put_manifest(&staged, sc.manifest_uri, None)
        .await
        .unwrap();
    let summary = sc.engine.recover().await.unwrap();
    assert_eq!(
        summary.still_pending, 1,
        "unsupported manifest must stay pending: {summary:?}"
    );
    assert_eq!(summary.replay_required, 0);
    assert_eq!(summary.committed, 0);
    let report = vtop_cli::commands::verify_manifest_deep(
        sc.engine.store.as_ref(),
        sc.engine.backend.as_ref(),
        sc.manifest_uri,
        None,
    )
    .await;
    let Err(err) = report else {
        panic!("a newer version must be refused before decoding");
    };
    assert!(
        matches!(
            err,
            vtop_core::errors::VtopError::UnsupportedManifestVersion { .. }
        ),
        "{err}"
    );
    assert!(err.to_string().contains("newer than this build supports"));
    assert_eq!(
        sc.engine
            .store
            .get_batch(sc.batch_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        BatchState::Verified
    );
}

/// A 0.4 manifest with a field this build does not know (#671) passes the
/// scrub's check and `verify-manifest`, and recovery commits its batch: a
/// release-A engine finishes a newer writer's batch from a shared ledger.
#[tokio::test]
async fn a_supported_04_batch_recovers_from_a_shared_ledger_without_replay() {
    let dir = tempfile::tempdir().unwrap();
    let (mut sc, _) = seed_version(
        dir.path(),
        "reader-04",
        "s3://telemetry-data/04.raw",
        "s3://telemetry-data/04.manifest.json",
        true,
        "0.4",
        true,
    )
    .await;
    let check = vtop_cli::scrub::check_stored_batch(
        sc.engine.backend.as_ref(),
        None,
        &sc.engine
            .store
            .get_batch(sc.batch_id)
            .await
            .unwrap()
            .unwrap(),
    )
    .await;
    assert_eq!(check.result.as_str(), "ok", "{check:?}");
    let report = vtop_cli::commands::verify_manifest_deep(
        sc.engine.store.as_ref(),
        sc.engine.backend.as_ref(),
        sc.manifest_uri,
        None,
    )
    .await
    .unwrap();
    assert!(report.passed);
    let summary = sc.engine.recover().await.unwrap();
    assert_eq!(summary.committed, 1, "{summary:?}");
    assert_eq!(summary.replay_required, 0);
}

/// Only a VERIFIED row's manifest is re-checked. A row that never reached
/// VERIFIED is re-read whatever its manifest says, as a newer release's own
/// recovery would re-read it: the version changes nothing there.
#[tokio::test]
async fn a_newer_unverified_manifest_is_replayed_like_any_unverified_row() {
    let dir = tempfile::tempdir().unwrap();
    let (mut sc, _) = seed_version(
        dir.path(),
        "unverified-future",
        "s3://telemetry-data/pending.raw",
        "s3://telemetry-data/pending.manifest.json",
        true,
        "0.9",
        false,
    )
    .await;
    let summary = sc.engine.recover().await.unwrap();
    assert_eq!(summary.replay_required, 1, "{summary:?}");
    assert_eq!(summary.committed, 0);
    assert_eq!(
        sc.engine
            .store
            .get_batch(sc.batch_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        BatchState::ReplayRequired
    );
}

/// Add a VERIFIED row of the seeded batch's file, `start..end`, archived to
/// the same object.
async fn add_verified_row(sc: &Scenario, dir: &Path, batch_id: &str, (start, end): (u64, u64)) {
    let mut row = sc
        .engine
        .store
        .get_batch(sc.batch_id)
        .await
        .unwrap()
        .unwrap();
    row.batch_id = batch_id.into();
    row.state = BatchState::Batching;
    row.manifest_uri = Some(format!("s3://telemetry-data/{batch_id}.manifest.json"));
    row.manifest_version_id = None;
    row.owner = None;
    row.lease_expires_at = None;
    for marker in [&mut row.progress_start, &mut row.progress_end] {
        let ProgressMarker::File {
            start_byte,
            end_byte,
            file_size,
            ..
        } = marker
        else {
            panic!("file marker");
        };
        *start_byte = start;
        *end_byte = end;
        *file_size = end;
    }
    let (manifest, path) = build_manifest(
        dir,
        &row.batch_id,
        &row.source_name,
        row.progress_end.clone(),
        row.object_uri.as_deref().unwrap(),
        row.manifest_uri.as_deref().unwrap(),
        row.object_size_bytes.unwrap() as u64,
        row.object_sha256.as_deref().unwrap(),
        1,
    );
    let stored = sc
        .engine
        .backend
        .put_manifest(&path, row.manifest_uri.as_deref().unwrap(), None)
        .await
        .unwrap();
    row.manifest_sha256 = Some(manifest.manifest.sha256);
    row.manifest_version_id = stored.version_id;
    let store = SqliteStateStore::connect(&format!(
        "sqlite://{}",
        dir.join(format!("{}.db", sc.batch_id)).display()
    ))
    .await
    .unwrap();
    store.save_batch_state(&row).await.unwrap();
    for state in [
        BatchState::Sealed,
        BatchState::Compressed,
        BatchState::Checksummed,
        BatchState::ObjectUploaded,
        BatchState::ManifestUploaded,
        BatchState::Verified,
    ] {
        store
            .update_batch_state(&row.batch_id, state, &BatchPatch::default())
            .await
            .unwrap();
    }
}

/// A newer-release VERIFIED range is uncommitted, like any range recovery
/// leaves pending: a VERIFIED range above it in the same file must not commit
/// past it (#639), or the committed point would move over bytes the older
/// engine never archived itself.
#[tokio::test]
async fn a_newer_manifest_holds_a_later_verified_range_of_the_same_source() {
    let dir = tempfile::tempdir().unwrap();
    let (mut sc, _) = seed_version(
        dir.path(),
        "lower-future",
        "s3://telemetry-data/lower.raw",
        "s3://telemetry-data/lower.manifest.json",
        true,
        "0.9",
        true,
    )
    .await;
    add_verified_row(&sc, dir.path(), "later-verified", (7, 14)).await;
    let summary = sc.engine.recover().await.unwrap();
    assert_eq!(
        summary.committed, 0,
        "a later range must not commit across an unsupported lower range: {summary:?}"
    );
    assert_eq!(summary.still_pending, 2);
    assert_eq!(summary.replay_required, 0);
    for batch_id in ["later-verified", sc.batch_id] {
        assert_eq!(
            sc.engine
                .store
                .get_batch(batch_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            BatchState::Verified
        );
    }
}

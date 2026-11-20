//! `manifest.write_version` in the running engine (#671, release B): the
//! default writes the same 0.3 bytes as v0.8.0, the opt-in writes a 0.4
//! manifest with its verification plan that this build verifies (keyed or
//! not) and commits, and a 0.4 batch whose backend has no known method fails
//! naming it.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use vtop_adapters::base::SourceAdapter;
use vtop_adapters::FileSource;
use vtop_cli::testkit::{file_config, pipeline};
use vtop_cli::Engine;
use vtop_core::config::{StreamsConfig, VtopConfig};
use vtop_core::manifest::{
    ManifestBuilder, ManifestMacKey, ManifestWriteVersion, ParsedManifest, VerificationPlan,
    VtopManifest,
};
use vtop_core::state_machine::BatchState;
use vtop_core::telemetry;
use vtop_core::types::{SourceType, TelemetryFormat};
use vtop_state::{BatchRecord, SqliteStateStore, StateStore};
use vtop_upload::{MockBackend, UploadBackend};

fn write_input(dir: &Path, name: &str, lines: usize) -> PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    for i in 0..lines {
        writeln!(f, "{name} record {i}").unwrap();
    }
    path
}

fn config(dir: &Path, input: &Path) -> VtopConfig {
    file_config(
        dir.join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.join("state.db").display()),
        vec![input.to_string_lossy().into_owned()],
        "mock",
    )
}

/// Archive one file through the engine, returning the committed row and the
/// manifest bytes the store holds for it (its pinned version).
async fn archive_one(cfg: VtopConfig) -> (BatchRecord, Vec<u8>) {
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    let mock = Arc::new(MockBackend::new());
    engine.backend = mock.clone();
    let outcomes = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    assert!(outcomes[0].committed, "{outcomes:?}");
    let row = engine
        .store
        .get_batch(&outcomes[0].batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, BatchState::SourceCommitted);
    let bytes = mock
        .get_manifest_pinned(
            row.manifest_uri.as_deref().unwrap(),
            row.manifest_version_id.as_deref().unwrap(),
            1 << 20,
        )
        .await
        .unwrap();
    (row, bytes)
}

fn builder_from(m: &VtopManifest) -> ManifestBuilder {
    ManifestBuilder {
        batch_id: m.batch_id.clone(),
        tenant: m.tenant.clone(),
        source_type: m.source_type.clone(),
        source_name: m.source_name.clone(),
        format: m.format.clone(),
        compression: m.compression,
        record_count: m.record_count,
        first_timestamp: m.first_timestamp.clone(),
        last_timestamp: m.last_timestamp.clone(),
        source_progress: m.source_progress.clone(),
        object_uri: m.object.uri.clone(),
        object_size: m.object.size_bytes,
        object_checksum_algorithm: m.object.checksum_algorithm.clone(),
        object_checksum: m.object.checksum.clone(),
        manifest_uri: m.manifest.uri.clone(),
        path_template: m.partitioning.path_template.clone(),
        resolved_prefix: m.partitioning.resolved_prefix.clone(),
        upload_backend: m.upload_backend.clone(),
        created_at: m.created_at.clone(),
    }
}

fn written(tenant: &str, version: &str) -> u64 {
    telemetry::metrics()
        .map(|m| {
            m.manifests_written_total
                .with_label_values(&[tenant, version])
                .get()
        })
        .unwrap_or(0)
}

/// With no `manifest:` section the engine stores exactly what the v0.8.0
/// entry point writes for the same fields (`build_with_mac` +
/// `to_json_bytes`, which `manifest_writer.rs` pins to the released binary's
/// bytes): version 0.3, no plan.
#[tokio::test]
async fn the_default_engine_writes_03_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let input = write_input(dir.path(), "a.log", 3);
    let cfg = config(dir.path(), &input);
    assert_eq!(cfg.manifest.write_version, ManifestWriteVersion::V0_3);
    let (row, bytes) = archive_one(cfg).await;

    let parsed = ParsedManifest::from_slice(&bytes).unwrap();
    assert_eq!(parsed.fields().version, "0.3");
    assert!(!String::from_utf8_lossy(&bytes).contains("verification_plan"));
    let legacy = builder_from(parsed.fields())
        .build_with_mac(None)
        .unwrap()
        .to_json_bytes()
        .unwrap();
    assert_eq!(legacy, bytes);
    assert_eq!(
        row.manifest_sha256.as_deref(),
        Some(parsed.fields().manifest.sha256.as_str())
    );
}

/// Opted in, the engine writes 0.4 with the plan its configuration implies,
/// verifies it, pins it, commits the batch and counts it by version.
#[tokio::test]
async fn an_opted_in_engine_writes_a_04_manifest_that_verifies_and_commits() {
    telemetry::init().unwrap();
    let tenant = "release-b-opt-in";
    let dir = tempfile::tempdir().unwrap();
    let input = write_input(dir.path(), "a.log", 3);
    let mut cfg = config(dir.path(), &input);
    cfg.engine.tenant = tenant.into();
    cfg.manifest.write_version = ManifestWriteVersion::V0_4;
    cfg.upload.require_strong_verification = true;
    let before = (written(tenant, "0.3"), written(tenant, "0.4"));
    let (row, bytes) = archive_one(cfg).await;

    let parsed = ParsedManifest::from_slice(&bytes).unwrap();
    parsed.verify_authentication(None).unwrap();
    let fields = parsed.fields();
    assert_eq!(fields.version, "0.4");
    // The mock backend reads back; the plan says so, with the policy in force.
    assert_eq!(
        fields.verification_plan,
        Some(VerificationPlan {
            method: "read_back".into(),
            require_strong: true,
        })
    );
    assert_eq!(fields.state, BatchState::ManifestUploaded);
    assert_eq!(
        fields.verification_status,
        vtop_core::manifest::VerificationStatus::NotVerified
    );
    assert_eq!(
        row.manifest_sha256.as_deref(),
        Some(fields.manifest.sha256.as_str())
    );
    assert_eq!(
        (written(tenant, "0.3"), written(tenant, "0.4")),
        (before.0, before.1 + 1),
        "one manifest counted, under version 0.4"
    );
}

/// A MAC'd 0.4 manifest passes the engine's keyed read-back of the stored
/// bytes, so the batch commits; the stored MAC verifies with the key only.
#[tokio::test]
async fn a_signed_04_manifest_passes_the_keyed_read_back_and_commits() {
    let env = format!("VTOP_TEST_RELEASE_B_MAC_{}", std::process::id());
    std::env::set_var(&env, "6b".repeat(32));
    let dir = tempfile::tempdir().unwrap();
    let input = write_input(dir.path(), "a.log", 4);
    let mut cfg = config(dir.path(), &input);
    cfg.manifest_mac_key_env = Some(env.clone());
    cfg.manifest.write_version = ManifestWriteVersion::V0_4;
    let engine_built = archive_one(cfg).await;
    std::env::remove_var(&env);
    let (_, bytes) = engine_built;

    let parsed = ParsedManifest::from_slice(&bytes).unwrap();
    assert_eq!(parsed.fields().version, "0.4");
    let key = ManifestMacKey::from_hex(&"6b".repeat(32)).unwrap();
    let wrong = ManifestMacKey::from_hex(&"6c".repeat(32)).unwrap();
    parsed.verify_authentication(Some(&key)).unwrap();
    assert!(parsed.verify_authentication(Some(&wrong)).is_err());
}

/// A 0.4 manifest states its verification method, so a backend outside the
/// method table fails the batch, naming the backend, instead of writing
/// `unknown`. The same configuration at 0.3 is unchanged and commits.
#[tokio::test]
async fn a_04_batch_on_a_backend_with_no_known_method_fails_naming_it() {
    for (version, commits) in [
        (ManifestWriteVersion::V0_3, true),
        (ManifestWriteVersion::V0_4, false),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let input = write_input(dir.path(), "a.log", 2);
        let mut cfg = file_config(
            dir.path().join("work").to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.upload.backend = "custom_store".into();
        cfg.manifest.write_version = version;
        let store = SqliteStateStore::connect("sqlite::memory:").await.unwrap();
        let backend: Arc<dyn UploadBackend> = Arc::new(MockBackend::new());
        let mut adapter = FileSource::new(
            vec![input.to_string_lossy().into_owned()],
            TelemetryFormat::Raw,
            false,
        );
        let source = adapter.discover_sources().await.unwrap().pop().unwrap();
        let read = adapter
            .read_batch_candidates(&source, 1000, 1 << 20, std::time::Duration::ZERO)
            .await
            .unwrap()
            .remove(0);
        let outcome = pipeline(&store, backend, &cfg)
            .process(&mut adapter, &source, read, None)
            .await
            .unwrap();
        assert_eq!(outcome.committed, commits, "{version}: {outcome:?}");
        if !commits {
            let rec = store.get_batch(&outcome.batch_id).await.unwrap().unwrap();
            let message = rec.error_message.unwrap_or_default();
            assert!(message.contains("custom_store"), "{message}");
            assert!(message.contains("verification method"), "{message}");
        }
    }
}

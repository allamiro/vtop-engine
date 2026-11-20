//! Verification receipts in the running engine (#671 release B, PR 3).
//!
//! With `manifest.verification_receipts: true`, once a batch is VERIFIED the
//! engine writes `<batch_id>.receipt.json` beside its manifest: the batch,
//! the manifest's self-hash and version, the object, the class the ledger
//! recorded (#664) and the method the backend actually ran (D9), signed like
//! a 0.4 manifest. The receipt is evidence about verification, not a
//! precondition for it (D6): a receipt that cannot be written never holds
//! back the commit. Messages are asserted in
//! `verification_receipt_messages.rs`, a binary with one test.
#![cfg(unix)]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use vtop_cli::Engine;
use vtop_core::config::{
    ManifestKeyConfig, ManifestMacConfig, ObjectLockConfig, ObjectLockMode, StreamsConfig,
    VtopConfig,
};
use vtop_core::manifest::{
    ManifestKeyring, ManifestWriteVersion, ParsedReceipt, VerificationReceipt,
    RECEIPT_ORIGIN_ENGINE,
};
use vtop_core::partitioning::receipt_uri;
use vtop_core::state_machine::BatchState;
use vtop_core::telemetry;
use vtop_core::types::{CompressionType, SourceType};
use vtop_state::BatchRecord;
use vtop_upload::{MockBackend, ObjectLockPolicy, PartFault, UploadBackend};

fn write_lines(path: &Path, tag: &str, lines: usize) {
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    for i in 0..lines {
        writeln!(
            f,
            "{tag} record {i:04} payload abcdefghijklmnopqrstuvwxyz0123456789 ABCDEFGHIJKLMNOP"
        )
        .unwrap();
    }
}

fn config(dir: &Path, tenant: &str, input: &Path) -> VtopConfig {
    let mut cfg = vtop_cli::testkit::file_config(
        dir.join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.join("state.db").display()),
        vec![input.to_string_lossy().into_owned()],
        "mock",
    );
    cfg.engine.tenant = tenant.into();
    cfg.partitioning.template = "tenant={tenant}/source={source}/format={format}/".into();
    cfg.manifest.verification_receipts = true;
    cfg
}

/// Small parts, no compression: about 3.6 KB of records is four parts.
fn multipart(mut cfg: VtopConfig) -> VtopConfig {
    cfg.compression.kind = CompressionType::None;
    cfg.upload.multipart_part_size_bytes = 1_000;
    cfg.upload.multipart_threshold_bytes = 1_000;
    cfg.upload.multipart_max_parallelism = 4;
    cfg
}

async fn engine(cfg: VtopConfig, backend: &Arc<MockBackend>) -> Engine {
    telemetry::init().unwrap();
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    engine.backend = backend.clone() as Arc<dyn UploadBackend>;
    engine
}

async fn row(engine: &Engine, batch_id: &str) -> BatchRecord {
    engine.store.get_batch(batch_id).await.unwrap().unwrap()
}

/// The receipt stored beside `row`'s manifest, parsed and authenticated.
async fn stored_receipt(
    backend: &MockBackend,
    row: &BatchRecord,
    keyring: &ManifestKeyring,
) -> VerificationReceipt {
    let uri = receipt_uri(row.manifest_uri.as_deref().unwrap()).unwrap();
    let bytes = backend.get_object_bounded(&uri, 1 << 16).await.unwrap();
    let parsed = ParsedReceipt::from_slice(&bytes).unwrap();
    let auth = parsed.authenticate(keyring);
    assert!(auth.is_ok(), "{:?}", auth.result);
    parsed.fields().clone()
}

fn receipts(tenant: &str, result: &str) -> u64 {
    telemetry::metrics()
        .map(|m| {
            m.verification_receipts_total
                .with_label_values(&[tenant, result])
                .get()
        })
        .unwrap_or(0)
}

/// The receipt binds the batch as the ledger recorded it: the manifest's
/// self-hash and pinned version, the object and its digest, and the class
/// the ledger holds (#664).
fn assert_binds(receipt: &VerificationReceipt, row: &BatchRecord) {
    assert_eq!(receipt.batch_id, row.batch_id);
    assert_eq!(
        Some(receipt.manifest.uri.as_str()),
        row.manifest_uri.as_deref()
    );
    assert_eq!(
        Some(receipt.manifest.sha256.as_str()),
        row.manifest_sha256.as_deref()
    );
    assert_eq!(receipt.manifest.version_id, row.manifest_version_id);
    assert_eq!(Some(receipt.object.uri.as_str()), row.object_uri.as_deref());
    assert_eq!(
        Some(receipt.object.checksum.as_str()),
        row.object_sha256.as_deref()
    );
    assert_eq!(
        Some(receipt.object.size_bytes as i64),
        row.object_size_bytes
    );
    assert_eq!(
        Some(receipt.verification.class.as_str()),
        row.verification_class.map(|c| c.as_str()),
        "the receipt's class is the ledger's"
    );
    assert_eq!(receipt.origin, RECEIPT_ORIGIN_ENGINE);
    assert_eq!(receipt.verified_by, "vtop-test");
}

#[tokio::test]
async fn receipts_are_off_by_default_and_none_is_written() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let mut cfg = config(dir.path(), "receipts-off", &input);
    cfg.manifest = Default::default();
    assert!(!cfg.manifest.verification_receipts);
    let backend = Arc::new(MockBackend::new());
    let mut engine = engine(cfg, &backend).await;
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out[0].committed, "{out:?}");
    let row = row(&engine, &out[0].batch_id).await;
    let uri = receipt_uri(row.manifest_uri.as_deref().unwrap()).unwrap();
    assert!(!backend.contains(&uri), "no receipt without the setting");
    assert_eq!(receipts("receipts-off", "written"), 0);
}

/// D9 and the plan's test table: under S3 semantics a single PUT is
/// verified by the store's checksum and a multipart object (no whole-object
/// service checksum) by reading it back. The manifest's plan says
/// `store_checksum` for both, the configured method; the receipt names the
/// one that ran.
#[tokio::test]
async fn a_receipt_names_the_ledger_class_and_the_method_the_backend_ran() {
    for (case, multipart_on, expected) in [
        ("single put", false, "store_checksum"),
        ("multipart", true, "read_back"),
    ] {
        let tenant = format!("receipts-method-{}", multipart_on as u8);
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        write_lines(&input, "app", 40);
        let mut cfg = config(dir.path(), &tenant, &input);
        cfg.upload.backend = "s3_native".into();
        cfg.manifest.write_version = ManifestWriteVersion::V0_4;
        if multipart_on {
            cfg = multipart(cfg);
        }
        let backend = Arc::new(MockBackend::new().with_s3_checksum_semantics());
        let written_before = receipts(&tenant, "written");
        let mut engine = engine(cfg, &backend).await;
        let out = engine.process_once(SourceType::File).await.unwrap();
        assert_eq!(out.len(), 1, "{case}: {out:?}");
        assert!(out[0].committed, "{case}: {out:?}");
        let row = row(&engine, &out[0].batch_id).await;
        assert_eq!(row.state, BatchState::SourceCommitted);

        let receipt = stored_receipt(&backend, &row, &ManifestKeyring::default()).await;
        assert_binds(&receipt, &row);
        assert_eq!(receipt.verification.class, "strong", "{case}");
        assert_eq!(receipt.verification.method, expected, "{case}");
        assert!(
            receipt.object.version_id.is_some(),
            "{case}: the object version the write returned is kept"
        );
        // The manifest states the configured plan, the receipt what ran.
        let manifest = backend
            .get_manifest_pinned(
                row.manifest_uri.as_deref().unwrap(),
                row.manifest_version_id.as_deref().unwrap(),
                1 << 20,
            )
            .await
            .unwrap();
        let manifest = vtop_core::manifest::ParsedManifest::from_slice(&manifest).unwrap();
        assert_eq!(
            manifest.fields().verification_plan.as_ref().unwrap().method,
            "store_checksum",
            "{case}"
        );
        assert_eq!(receipts(&tenant, "written"), written_before + 1, "{case}");
    }
}

/// #702 (#668): an object completed from a resumed multipart session is
/// verified by reading it back like any other, and its receipt says so.
#[tokio::test]
async fn a_resumed_multipart_object_is_receipted_with_the_method_that_ran() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 40);
    let mut cfg = multipart(config(dir.path(), "receipts-resume", &input));
    cfg.upload.backend = "s3_native".into();
    let mut value = serde_json::to_value(&cfg).unwrap();
    value["upload"]["multipart_part_attempts"] = 2.into();
    let cfg: VtopConfig = serde_json::from_value(value).unwrap();
    let backend = Arc::new(
        MockBackend::new()
            .with_s3_checksum_semantics()
            .with_part_fault(2, PartFault::Reset, 2),
    );
    let mut engine = engine(cfg, &backend).await;
    let first = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(first[0].final_state, BatchState::Failed, "{first:?}");
    let failed = row(&engine, &first[0].batch_id).await;
    assert!(
        failed.manifest_uri.is_none(),
        "the upload failed before any manifest (or receipt) was written: {failed:?}"
    );
    let sends = |b: &MockBackend| [1, 2, 3, 4].map(|n| b.part_sends(n));
    assert_eq!(sends(&backend), [1, 2, 1, 1], "part 2 had two attempts");

    let second = engine.process_once(SourceType::File).await.unwrap();
    assert!(second[0].committed, "{second:?}");
    // Resumed, not uploaded again: only part 2 was sent once more (#668).
    assert_eq!(
        sends(&backend),
        [1, 3, 1, 1],
        "the replay resumed the session"
    );
    let row = row(&engine, &second[0].batch_id).await;
    let receipt = stored_receipt(&backend, &row, &ManifestKeyring::default()).await;
    assert_binds(&receipt, &row);
    assert_eq!(receipt.verification.method, "read_back");
    assert_eq!(receipt.verification.class, "strong");
}

/// A size-only pass (allowed by `require_strong_verification: false`) is
/// receipted as what it was.
#[tokio::test]
async fn a_size_only_batch_is_receipted_size_only() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let mut cfg = config(dir.path(), "receipts-size-only", &input);
    cfg.upload.backend = "mock_limited".into();
    cfg.upload.require_strong_verification = false;
    let backend = Arc::new(MockBackend::limited());
    let mut engine = engine(cfg, &backend).await;
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out[0].committed, "{out:?}");
    let row = row(&engine, &out[0].batch_id).await;
    let receipt = stored_receipt(&backend, &row, &ManifestKeyring::default()).await;
    assert_binds(&receipt, &row);
    assert_eq!(receipt.verification.class, "size_only");
    assert_eq!(receipt.verification.method, "size_only");
}

/// D6: the receipt is not a commit precondition. A store that refuses every
/// receipt PUT still commits every batch, and each failure is counted.
#[tokio::test]
async fn a_receipt_put_that_always_fails_does_not_block_the_commit() {
    let tenant = "receipts-put-fails";
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let cfg = config(dir.path(), tenant, &input);
    let backend = Arc::new(MockBackend::new().with_failing_put_suffix(".receipt.json"));
    let (written, failed) = (receipts(tenant, "written"), receipts(tenant, "failed"));
    let mut engine = engine(cfg, &backend).await;
    for round in 0..2 {
        if round == 1 {
            write_lines(&input, "more", 2);
        }
        let out = engine.process_once(SourceType::File).await.unwrap();
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].committed, "round {round}: {out:?}");
        let row = row(&engine, &out[0].batch_id).await;
        assert_eq!(row.state, BatchState::SourceCommitted);
        let uri = receipt_uri(row.manifest_uri.as_deref().unwrap()).unwrap();
        assert!(!backend.contains(&uri));
    }
    assert_eq!(receipts(tenant, "written"), written);
    assert_eq!(receipts(tenant, "failed"), failed + 2);
    // Nothing staged is left behind.
    let staged: Vec<PathBuf> = std::fs::read_dir(dir.path().join("work"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.to_string_lossy().ends_with(".receipt.json"))
        .collect();
    assert!(staged.is_empty(), "{staged:?}");
}

/// #685: with a keyring, the receipt is signed by the active key and names
/// it, as a 0.4 manifest does, and verifies with that key only.
#[tokio::test]
async fn a_receipt_is_signed_by_the_active_key_and_names_it() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let keys = dir.path().join("keys");
    std::fs::create_dir_all(&keys).unwrap();
    for (id, byte) in [("q3", "3c"), ("q4", "4d")] {
        std::fs::write(keys.join(id), byte.repeat(32)).unwrap();
        // Owner-only, as the engine requires of a key file (#733).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(keys.join(id), std::fs::Permissions::from_mode(0o400))
                .unwrap();
        }
    }
    let mut cfg = config(dir.path(), "receipts-keyring", &input);
    cfg.manifest.write_version = ManifestWriteVersion::V0_4;
    cfg.manifest.mac = Some(ManifestMacConfig {
        active: "q4".into(),
        keys: [("q3", true), ("q4", false)]
            .iter()
            .map(|(id, legacy)| ManifestKeyConfig {
                id: (*id).into(),
                env: None,
                file: Some(keys.join(id).to_string_lossy().into_owned()),
                legacy: *legacy,
            })
            .collect(),
        retired: Vec::new(),
    });
    let keyring = cfg.resolve_manifest_keyring().unwrap();
    let backend = Arc::new(MockBackend::new());
    let mut engine = engine(cfg, &backend).await;
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out[0].committed, "{out:?}");
    let row = row(&engine, &out[0].batch_id).await;
    let receipt = stored_receipt(&backend, &row, &keyring).await;
    assert_eq!(receipt.receipt.mac_key_id.as_deref(), Some("q4"));
    assert!(receipt.receipt.mac.is_some());
    // An unsigned reader cannot pass it off as unkeyed: with a keyring that
    // lacks q4, it is an unknown key.
    let uri = receipt_uri(row.manifest_uri.as_deref().unwrap()).unwrap();
    let bytes = backend.get_object_bounded(&uri, 1 << 16).await.unwrap();
    let only_q3 = ManifestKeyring::new(
        "q3",
        vec![vtop_core::manifest::KeyringKey {
            id: "q3".into(),
            key: vtop_core::manifest::ManifestMacKey::from_hex(&"3c".repeat(32)).unwrap(),
            source: vtop_core::manifest::KeySourceKind::File,
            legacy: true,
        }],
        Vec::new(),
    )
    .unwrap();
    let outcome = ParsedReceipt::from_slice(&bytes)
        .unwrap()
        .authenticate(&only_q3);
    assert_eq!(
        outcome.key.map(|k| k.result),
        Some(vtop_core::manifest::KeyResult::UnknownKey)
    );
}

/// #684: with the Object Lock profile on, the receipt is written under the
/// same retention as the manifest beside it (it goes through the manifest
/// write path), and the store kept it.
#[tokio::test]
async fn under_object_lock_a_receipt_carries_the_manifests_retention() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let lock = ObjectLockConfig {
        mode: ObjectLockMode::Governance,
        retain_days: 30,
        legal_hold: false,
    };
    let cfg = config(dir.path(), "receipts-locked", &input);
    let backend =
        Arc::new(MockBackend::new().with_object_lock(ObjectLockPolicy::from_config(&lock)));
    let mut engine = engine(cfg, &backend).await;
    engine.config.upload.require_object_versioning = true;
    engine.config.upload.object_lock = Some(lock);
    let written = receipts("receipts-locked", "written");
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out[0].committed, "{out:?}");
    let row = row(&engine, &out[0].batch_id).await;
    let uri = receipt_uri(row.manifest_uri.as_deref().unwrap()).unwrap();
    let kept = backend.stored_retention(&uri, None);
    let manifest_kept = backend.stored_retention(
        row.manifest_uri.as_deref().unwrap(),
        row.manifest_version_id.as_deref(),
    );
    assert_eq!(
        kept.mode.as_deref(),
        Some("GOVERNANCE"),
        "the receipt is locked: {kept:?}"
    );
    assert!(
        kept.retain_until_unix_secs >= manifest_kept.retain_until_unix_secs
            && manifest_kept.retain_until_unix_secs.is_some(),
        "at least as long as its manifest: {kept:?} {manifest_kept:?}"
    );
    assert_eq!(receipts("receipts-locked", "written"), written + 1);
}

/// Review of #711: a receipt the store did not lock is a failed RECEIPT, not
/// an Object Lock check failure. `vtop_object_lock_checks_total` counts the
/// object and manifest checks that fail batches, and its alert pages; the
/// batch here committed.
#[tokio::test]
async fn a_receipt_the_store_does_not_lock_is_a_failed_receipt_not_a_lock_check_failure() {
    let tenant = "receipts-unlocked";
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let lock = ObjectLockConfig {
        mode: ObjectLockMode::Governance,
        retain_days: 30,
        legal_hold: false,
    };
    let backend = Arc::new(
        MockBackend::new()
            .with_object_lock(ObjectLockPolicy::from_config(&lock))
            .with_lock_fault(vtop_upload::LockFault::Ignored),
    );
    backend.set_lock_fault_only_for(Some(".receipt.json"));
    let mut engine = engine(config(dir.path(), tenant, &input), &backend).await;
    engine.config.upload.require_object_versioning = true;
    engine.config.upload.object_lock = Some(lock);
    let lock_checks = |result: &str| {
        telemetry::metrics()
            .map(|m| {
                m.object_lock_checks_total
                    .with_label_values(&[tenant, result])
                    .get()
            })
            .unwrap_or(0)
    };
    let not_ok_before: Vec<u64> = telemetry::OBJECT_LOCK_CHECK_RESULTS
        .iter()
        .filter(|r| **r != "ok")
        .map(|r| lock_checks(r))
        .collect();
    let (failed, ok_before) = (receipts(tenant, "failed"), lock_checks("ok"));
    let out = engine.process_once(SourceType::File).await.unwrap();
    assert!(out[0].committed, "{out:?}");
    assert_eq!(receipts(tenant, "failed"), failed + 1);
    let not_ok_after: Vec<u64> = telemetry::OBJECT_LOCK_CHECK_RESULTS
        .iter()
        .filter(|r| **r != "ok")
        .map(|r| lock_checks(r))
        .collect();
    assert_eq!(
        not_ok_after, not_ok_before,
        "no lock-check failure is counted"
    );
    assert_eq!(
        lock_checks("ok"),
        ok_before + 2,
        "the object's and the manifest's checks only"
    );
}

/// Review of #711: a receipt PUT the store never answers is abandoned at the
/// receipt's own deadline (at most the request timeout, and never more than
/// 30 s), so it cannot hold the batch's commit for the SDK's whole retry
/// budget.
#[tokio::test]
async fn a_receipt_put_that_never_answers_is_abandoned_at_its_deadline() {
    let tenant = "receipts-hang";
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("app.log");
    write_lines(&input, "app", 3);
    let mut cfg = config(dir.path(), tenant, &input);
    cfg.upload.request_timeout_seconds = 1;
    let backend = Arc::new(MockBackend::new());
    backend.set_hanging_put_suffix(Some(".receipt.json"));
    let failed = receipts(tenant, "failed");
    let mut engine = engine(cfg, &backend).await;
    let started = std::time::Instant::now();
    let out = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        engine.process_once(SourceType::File),
    )
    .await
    .expect("the receipt's deadline must end the wait")
    .unwrap();
    assert!(out[0].committed, "{out:?}");
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
    assert_eq!(receipts(tenant, "failed"), failed + 1);
}

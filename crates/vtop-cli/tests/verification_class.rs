//! The class of verification the running engine records with VERIFIED
//! (#664), for each backend and each check it can run: a checksum the store
//! computed, a read-back of the stored bytes (also the fallback when the
//! store has no whole-object checksum, as after a multipart upload), or size
//! alone. The class is what the backend's result says - strong, or
//! `backend_limited` - so each method is driven through the real pipeline and
//! its row, its counter and its log line are checked.

use std::sync::{Arc, Mutex, OnceLock};
use vtop_cli::{testkit::file_config, Engine};
use vtop_core::config::StreamsConfig;
use vtop_core::state_machine::BatchState;
use vtop_core::types::{ChecksumAlgorithm, SourceType, TelemetryFormat};
use vtop_state::VerificationClass;
use vtop_upload::localfs_backend::LocalFsBackend;
use vtop_upload::{MockBackend, UploadBackend};

/// Every log line this process writes, as JSON (see the note on the same
/// capture in tests/integration_state_recovery.rs).
fn logs() -> &'static Captured {
    static LOGS: OnceLock<Captured> = OnceLock::new();
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
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Captured {
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

fn size_only_count(tenant: &str) -> u64 {
    vtop_core::telemetry::init()
        .expect("metrics registry")
        .verification_backend_limited_total
        .with_label_values(&[tenant, "file", TelemetryFormat::Raw.extension()])
        .get()
}

/// One pipeline pass over a two-line file with `backend` and `algorithm`.
/// Returns the batch row and how far the size-only counter moved.
async fn one_pass(
    tenant: &str,
    backend: impl FnOnce(&std::path::Path) -> Arc<dyn UploadBackend>,
    algorithm: ChecksumAlgorithm,
    strict: bool,
    multipart: bool,
) -> (vtop_state::BatchRecord, bool, u64) {
    let _ = logs();
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
    cfg.checksum.algorithm = algorithm;
    cfg.upload.require_strong_verification = strict;
    if multipart {
        // Any object goes up in parts, so the store keeps no whole-object
        // checksum and the check falls back to a read-back.
        cfg.upload.multipart_threshold_bytes = 1;
        cfg.upload.multipart_part_size_bytes = 5;
    }
    let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    engine.backend = backend(&dir.path().join("store"));
    let before = size_only_count(tenant);
    let outcomes = engine
        .process_once(SourceType::File)
        .await
        .unwrap_or_default();
    let committed = outcomes.iter().any(|o| o.committed);
    let rows = engine.store.list_batches().await.unwrap();
    assert_eq!(rows.len(), 1, "{tenant}: {rows:?}");
    (rows[0].clone(), committed, size_only_count(tenant) - before)
}

#[tokio::test]
async fn the_engine_records_the_class_each_backend_and_method_verified_with() {
    type Make = fn(&std::path::Path) -> Arc<dyn UploadBackend>;
    let mock: Make = |_| Arc::new(MockBackend::new());
    let s3: Make = |_| Arc::new(MockBackend::new().with_s3_checksum_semantics());
    let limited: Make = |_| Arc::new(MockBackend::limited());
    let localfs: Make = |root| Arc::new(LocalFsBackend::new(root));
    use ChecksumAlgorithm::{Blake3, None as Off, Sha256};
    use VerificationClass::{SizeOnly, Strong};
    // (case, backend, algorithm, multipart, class, what the backend says it checked)
    let cases: [(&str, Make, ChecksumAlgorithm, bool, VerificationClass, &str); 9] = [
        (
            "store-checksum",
            s3,
            Sha256,
            false,
            Strong,
            "S3 service-computed SHA-256 verified",
        ),
        (
            "s3-multipart-read-back",
            s3,
            Sha256,
            true,
            Strong,
            "verified by read-back",
        ),
        (
            "s3-blake3-read-back",
            s3,
            Blake3,
            false,
            Strong,
            "read-back BLAKE3 verified",
        ),
        (
            "mock-sha256-read-back",
            mock,
            Sha256,
            false,
            Strong,
            "stored content checksum verified",
        ),
        (
            "localfs-sha256-read-back",
            localfs,
            Sha256,
            false,
            Strong,
            "stored content sha256 verified",
        ),
        (
            "mock-checksums-off",
            mock,
            Off,
            false,
            SizeOnly,
            "size-only (checksums disabled)",
        ),
        (
            "s3-checksums-off",
            s3,
            Off,
            false,
            SizeOnly,
            "checksums disabled",
        ),
        (
            "localfs-checksums-off",
            localfs,
            Off,
            false,
            SizeOnly,
            "checksums disabled",
        ),
        (
            "backend-limited",
            limited,
            Sha256,
            false,
            SizeOnly,
            "mock: size-only verification",
        ),
    ];
    for (case, backend, algorithm, multipart, class, check) in cases {
        let tenant = format!("tenant-664-{case}");
        let (row, committed, moved) = one_pass(&tenant, backend, algorithm, false, multipart).await;
        assert!(committed, "{case}: {row:?}");
        assert_eq!(
            (row.state, row.verification_class),
            (BatchState::SourceCommitted, Some(class)),
            "{case}"
        );
        assert_eq!(
            moved,
            u64::from(class == SizeOnly),
            "{case}: size-only counter"
        );
        let lines = logs().about(&row.batch_id);
        let said = lines.iter().any(|line| {
            line.contains("verification_passed")
                && line.contains(&format!(r#""verification_class":"{}""#, class.as_str()))
                && line.contains(check)
        });
        assert!(
            said,
            "{case}: no verification_passed line with class {} and check {check:?}:\n{}",
            class.as_str(),
            lines.join("\n")
        );
    }
}

/// Under the strong default a size-only result is refused: the batch fails
/// before VERIFIED and its range is handed back for a re-read, so no class
/// is recorded and nothing is counted, and the refusal says why.
#[tokio::test]
async fn a_size_only_result_under_the_strong_default_records_no_class() {
    let tenant = "tenant-664-strict-limited";
    let (row, committed, moved) = one_pass(
        tenant,
        |_| Arc::new(MockBackend::limited()),
        ChecksumAlgorithm::Sha256,
        true,
        false,
    )
    .await;
    assert!(!committed);
    assert_eq!(
        (row.state, row.verification_class),
        (BatchState::ReplayRequired, None)
    );
    assert_eq!(moved, 0);
    assert!(
        row.error_message
            .as_deref()
            .is_some_and(|m| m.contains("strong verification required")),
        "{row:?}"
    );
}

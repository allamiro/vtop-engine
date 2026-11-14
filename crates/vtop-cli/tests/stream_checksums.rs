//! Per-stream algorithms survive restart and coexist in one archive (#669).
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use vtop_cli::{scrub, testkit::file_config, Engine};
use vtop_core::config::{StreamsConfig, VtopConfig};
use vtop_core::errors::VtopError;
use vtop_core::manifest::VtopManifest;
use vtop_core::state_machine::BatchState;
use vtop_core::types::{ChecksumAlgorithm, SourceType};
use vtop_upload::{
    MockBackend, ObjectChecksum, ObjectHead, StoredManifest, StoredObject, UploadBackend,
    VerificationResult,
};

#[derive(Default)]
struct RecordingBackend {
    inner: MockBackend,
    verified: Mutex<Vec<(String, String)>>,
}

#[async_trait]
impl UploadBackend for RecordingBackend {
    async fn put_object(
        &self,
        path: &Path,
        uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredObject, VtopError> {
        self.inner.put_object(path, uri, checksum).await
    }
    async fn put_manifest(
        &self,
        path: &Path,
        uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredManifest, VtopError> {
        self.inner.put_manifest(path, uri, checksum).await
    }
    async fn head_object(&self, uri: &str) -> Result<ObjectHead, VtopError> {
        self.inner.head_object(uri).await
    }
    async fn get_object(&self, uri: &str) -> Result<Vec<u8>, VtopError> {
        self.inner.get_object(uri).await
    }
    async fn get_object_bounded(&self, uri: &str, max: usize) -> Result<Vec<u8>, VtopError> {
        self.inner.get_object_bounded(uri, max).await
    }
    async fn get_manifest_pinned(
        &self,
        uri: &str,
        version: &str,
        max: usize,
    ) -> Result<Vec<u8>, VtopError> {
        self.inner.get_manifest_pinned(uri, version, max).await
    }
    async fn verify_object(
        &self,
        uri: &str,
        size: u64,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<VerificationResult, VtopError> {
        self.verified
            .lock()
            .unwrap()
            .push((uri.into(), checksum.map_or("none", |c| c.algorithm).into()));
        self.inner.verify_object(uri, size, checksum).await
    }
    async fn delete_object(&self, uri: &str) -> Result<(), VtopError> {
        self.inner.delete_object(uri).await
    }
    fn backend_name(&self) -> &'static str {
        "mock"
    }
    fn supports_checksum_verification(&self) -> bool {
        true
    }
    fn supports_multipart(&self) -> bool {
        false
    }
}

fn setup(dir: &Path, algorithms: &[&str]) -> (VtopConfig, StreamsConfig) {
    let mut paths = Vec::new();
    let mut entries = Vec::new();
    for (i, algorithm) in algorithms.iter().enumerate() {
        let path = dir.join(format!("stream-{i}.log"));
        std::fs::write(&path, format!("stream {i} record\n")).unwrap();
        paths.push(path.to_string_lossy().into_owned());
        entries.push(serde_json::json!({"source_name": path, "source_type": "file", "format": "raw", "tenant": "stream-checksum-669", "checksum": {"algorithm": algorithm}}));
    }
    let mut config = file_config(
        dir.join("work").to_str().unwrap(),
        &format!("sqlite://{}", dir.join("state.db").display()),
        paths,
        "mock",
    );
    config.upload.require_strong_verification = true;
    let streams = serde_json::from_value(serde_json::json!({"streams":entries})).unwrap();
    (config, streams)
}

async fn assert_archived_algorithms(
    engine: &Engine,
    backend: &RecordingBackend,
    expected: &[&str],
) {
    let mut rows = engine.store.list_batches().await.unwrap();
    rows.sort_by(|a, b| a.source_name.cmp(&b.source_name));
    assert_eq!(rows.len(), expected.len());
    for (row, algorithm) in rows.iter().zip(expected) {
        let manifest: VtopManifest = serde_json::from_slice(
            &backend
                .get_object(row.manifest_uri.as_deref().unwrap())
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            &manifest.object.checksum_algorithm, algorithm,
            "manifest for {} must use its stream algorithm",
            row.source_name
        );
        let bytes = backend
            .get_object(row.object_uri.as_deref().unwrap())
            .await
            .unwrap();
        let digest = vtop_core::checksum::digest_bytes(algorithm.parse().unwrap(), &bytes)
            .unwrap_or_default();
        assert_eq!(
            row.object_sha256.as_deref().unwrap_or_default(),
            digest,
            "ledger digest for {}",
            row.source_name
        );
        assert_eq!(manifest.object.checksum, digest);
        assert!(
            backend
                .verified
                .lock()
                .unwrap()
                .iter()
                .any(
                    |(uri, algo)| Some(uri.as_str()) == row.object_uri.as_deref()
                        && algo == algorithm
                ),
            "verify_object did not receive {algorithm} for {}",
            row.source_name
        );
    }
}

#[tokio::test]
async fn streams_choose_distinct_algorithms_and_mixed_batches_scrub_clean() {
    let dir = tempfile::tempdir().unwrap();
    let (config, streams) = setup(dir.path(), &["sha256", "blake3"]);
    let mut engine = Engine::new(config, streams).await.unwrap();
    let backend = Arc::new(RecordingBackend::default());
    engine.backend = backend.clone();
    let outcomes = engine.process_once(SourceType::File).await.unwrap();
    assert_eq!(outcomes.len(), 2);
    assert!(
        outcomes.iter().all(|outcome| outcome.committed),
        "{outcomes:?}"
    );
    assert_archived_algorithms(&engine, &backend, &["sha256", "blake3"]).await;
    backend.verified.lock().unwrap().clear();
    let rows = scrub::committed_rows(engine.store.list_batches().await.unwrap(), Vec::new());
    let report = engine
        .scrub(rows, &scrub::ScrubOptions::default())
        .await
        .unwrap();
    assert!(report.passed, "{report:?}");
    assert_eq!(report.results.get("ok"), Some(&2), "{report:?}");
    assert_archived_algorithms(&engine, &backend, &["sha256", "blake3"]).await;
}

#[tokio::test]
async fn recovery_uses_manifest_algorithm_after_stream_config_change() {
    let dir = tempfile::tempdir().unwrap();
    let (config, streams) = setup(dir.path(), &["blake3"]);
    let backend = Arc::new(RecordingBackend::default());
    let mut first = Engine::new(config.clone(), streams).await.unwrap();
    first.backend = backend.clone();
    let adapter = first.adapters.remove(&SourceType::File).unwrap();
    first
        .adapters
        .insert(SourceType::File, Box::new(FailCommit(adapter)));
    let outcomes = first.process_once(SourceType::File).await.unwrap();
    assert!(
        !outcomes[0].committed,
        "batch must stop VERIFIED before its source commits"
    );
    assert_eq!(
        first.store.list_batches().await.unwrap()[0].state,
        BatchState::Verified
    );
    assert_archived_algorithms(&first, &backend, &["blake3"]).await;
    drop(first);
    let mut restart_config = config;
    restart_config.checksum.algorithm = ChecksumAlgorithm::Sha256;
    let mut restart = Engine::new(restart_config, StreamsConfig { streams: vec![] })
        .await
        .unwrap();
    restart.backend = backend.clone();
    backend.verified.lock().unwrap().clear();
    let summary = restart.recover().await.unwrap();
    assert_eq!(
        (
            summary.committed,
            summary.replay_required,
            summary.still_pending
        ),
        (1, 0, 0),
        "{summary:?}"
    );
    assert_archived_algorithms(&restart, &backend, &["blake3"]).await;
    assert!(restart
        .store
        .list_batches()
        .await
        .unwrap()
        .iter()
        .all(|row| row.state == BatchState::SourceCommitted));
}

#[tokio::test]
async fn stream_none_with_opt_out_commits_and_counts_size_only() {
    let metrics = vtop_core::telemetry::init().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (mut config, mut streams) = setup(dir.path(), &["none"]);
    config.upload.require_strong_verification = false;
    streams.streams[0].tenant = "stream-none-669".into();
    let mut engine = Engine::new(config, streams).await.unwrap();
    let backend = Arc::new(RecordingBackend::default());
    engine.backend = backend.clone();
    let outcome = engine.process_once(SourceType::File).await.unwrap();
    assert!(outcome[0].committed, "{outcome:?}");
    assert_archived_algorithms(&engine, &backend, &["none"]).await;
    let scrape = metrics.encode().unwrap();
    assert!(
        scrape.lines().any(
            |line| line.starts_with("vtop_verification_backend_limited_total{")
                && line.contains("tenant=\"stream-none-669\"")
                && line.ends_with(" 1")
        ),
        "size-only stream must increment its counter: {scrape}"
    );
}

#[test]
fn validate_rejects_stream_none_under_strong_floor_and_warns_under_opt_out() {
    for strong in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let (mut config, streams) = setup(dir.path(), &["none"]);
        config.upload.require_strong_verification = strong;
        let path = dir.path().join("config.yaml");
        std::fs::write(&path, serde_yaml::to_string(&config).unwrap()).unwrap();
        // Serializing the decoded streams would drop the unknown checksum on
        // the old reader, hiding precisely the compatibility bug under test.
        let stream = &streams.streams[0];
        std::fs::write(dir.path().join("streams.yaml"), format!("streams:\n  - source_name: {}\n    source_type: file\n    format: raw\n    checksum:\n      algorithm: none\n", stream.source_name)).unwrap();
        let out = Command::new(env!("CARGO_BIN_EXE_vtopctl"))
            .args(["--json", "validate", "--config"])
            .arg(path)
            .output()
            .unwrap();
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.success(), !strong, "floor={strong}: {output}");
        assert!(
            output.contains(&stream.source_name) && output.contains("none"),
            "stream-specific diagnostic missing: {output}"
        );
        if !strong {
            assert!(
                output.contains("size alone"),
                "warning must describe the consequence: {output}"
            );
        }
    }
}

#[test]
fn unknown_stream_algorithm_is_rejected_instead_of_silently_ignored() {
    let bad = serde_json::from_value::<StreamsConfig>(
        serde_json::json!({"streams":[{"source_name":"unknown","source_type":"file","format":"raw","checksum":{"algorithm":"sha257"}}]}),
    );
    assert!(
        bad.is_err(),
        "unknown stream algorithm was silently ignored: {bad:?}"
    );
}

struct FailCommit(Box<dyn vtop_adapters::base::SourceAdapter>);
#[async_trait]
impl vtop_adapters::base::SourceAdapter for FailCommit {
    async fn discover_sources(
        &self,
    ) -> Result<Vec<vtop_adapters::base::DiscoveredSource>, VtopError> {
        self.0.discover_sources().await
    }
    async fn read_batch_candidates(
        &mut self,
        source: &vtop_adapters::base::DiscoveredSource,
        records: usize,
        bytes: usize,
        wait: std::time::Duration,
    ) -> Result<Vec<vtop_adapters::base::ReadResult>, VtopError> {
        self.0
            .read_batch_candidates(source, records, bytes, wait)
            .await
    }
    async fn commit_progress(
        &mut self,
        _: &vtop_core::types::ProgressMarker,
    ) -> Result<(), VtopError> {
        Err(VtopError::Source(
            "simulated crash after verification".into(),
        ))
    }
    async fn replay_from_marker(
        &mut self,
        marker: &vtop_core::types::ProgressMarker,
    ) -> Result<(), VtopError> {
        self.0.replay_from_marker(marker).await
    }
    fn source_type(&self) -> SourceType {
        SourceType::File
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[test]
fn startup_reports_each_streams_resolved_algorithm_and_method() {
    let dir = tempfile::tempdir().unwrap();
    let (config, streams) = setup(dir.path(), &["sha256", "blake3"]);
    let path = dir.path().join("config.yaml");
    std::fs::write(&path, serde_yaml::to_string(&config).unwrap()).unwrap();
    std::fs::write(
        dir.path().join("streams.yaml"),
        serde_yaml::to_string(&streams).unwrap(),
    )
    .unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_vtopctl"))
        .args(["--log-level", "info", "replay", "--config"])
        .arg(path)
        .env_remove("RUST_LOG")
        .env_remove("VTOP_LOG_FORMAT")
        .output()
        .unwrap();
    let log = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{log}");
    for (stream, algorithm) in streams.streams.iter().zip(["sha256", "blake3"]) {
        assert!(
            log.lines()
                .any(|line| line.contains("stream verification settings")
                    && line.contains(&stream.source_name)
                    && line.contains(algorithm)
                    && line.contains("read_back")),
            "missing settings for {}: {log}",
            stream.source_name
        );
    }
}

#[test]
fn resolution_prefers_stream_then_global_and_builtin_default() {
    let dir = tempfile::tempdir().unwrap();
    let (mut config, streams) = setup(dir.path(), &["blake3"]);
    let resolve = vtop_core::config::effective_algorithm;
    assert_eq!(
        resolve(&config, Some(&streams.streams[0])),
        ChecksumAlgorithm::Blake3
    );
    assert_eq!(resolve(&config, None), ChecksumAlgorithm::Sha256);
    config.checksum.algorithm = ChecksumAlgorithm::Blake3;
    let no_override = serde_json::from_value::<vtop_core::config::StreamConfig>(
        serde_json::json!({"source_name":"inherit","source_type":"file","format":"raw"}),
    )
    .unwrap();
    assert_eq!(
        resolve(&config, Some(&no_override)),
        ChecksumAlgorithm::Blake3
    );
    assert!(no_override.checksum.is_none());
    config.checksum = Default::default();
    assert_eq!(
        resolve(&config, Some(&no_override)),
        ChecksumAlgorithm::Sha256
    );
}

#[test]
fn effective_output_resolves_stream_algorithm_and_origin_with_shared_helper() {
    let dir = tempfile::tempdir().unwrap();
    let (config, mut streams) = setup(dir.path(), &["sha256", "blake3", "sha256"]);
    streams.streams[2].checksum = None;
    let path = dir.path().join("config.yaml");
    std::fs::write(&path, serde_yaml::to_string(&config).unwrap()).unwrap();
    std::fs::write(
        dir.path().join("streams.yaml"),
        serde_yaml::to_string(&streams).unwrap(),
    )
    .unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_vtopctl"))
        .args(["--json", "validate", "--print-effective", "--config"])
        .arg(path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let output: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(
        output["effective"]["default"]["checksum.algorithm"]["value"],
        "sha256"
    );
    for (i, (algorithm, origin)) in [
        ("sha256", "stream"),
        ("blake3", "stream"),
        ("sha256", "global"),
    ]
    .into_iter()
    .enumerate()
    {
        let actual = &output["effective"]["streams"][i];
        assert_eq!(actual["checksum.algorithm"]["value"], algorithm, "{actual}");
        assert_eq!(actual["checksum.algorithm"]["origin"], origin, "{actual}");
        assert_eq!(
            actual["verification_method"]["value"], "read_back",
            "{actual}"
        );
        assert_eq!(actual["verification_method"]["origin"], origin, "{actual}");
    }
}

#[test]
fn global_none_warning_accounts_for_streams_that_override_it() {
    let dir = tempfile::tempdir().unwrap();
    let (mut config, streams) = setup(dir.path(), &["sha256"]);
    config.checksum.algorithm = ChecksumAlgorithm::None;
    config.upload.require_strong_verification = false;
    let path = dir.path().join("config.yaml");
    std::fs::write(&path, serde_yaml::to_string(&config).unwrap()).unwrap();
    std::fs::write(
        dir.path().join("streams.yaml"),
        serde_yaml::to_string(&streams).unwrap(),
    )
    .unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_vtopctl"))
        .args(["--json", "validate", "--config"])
        .arg(path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let warning = report["warnings"][0].as_str().unwrap();
    assert!(
        warning.contains("inheriting the global algorithm"),
        "must not describe the strong stream as size-only: {warning}"
    );
}

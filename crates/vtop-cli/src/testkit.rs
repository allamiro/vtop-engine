//! Test helpers for exercising the engine without external infrastructure.
//!
//! Shipping a small testkit lets the integration tests (and downstream users)
//! drive the full pipeline against the in-memory mock backend.

use crate::engine::Pipeline;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use vtop_adapters::base::{DiscoveredSource, ReadResult, SourceAdapter};
use vtop_core::config::{
    BatchingConfig, ChecksumConfig, CompressionConfig, EngineConfig, FileSourceConfig,
    PartitioningConfig, SourcesConfig, UploadConfig, VtopConfig,
};
use vtop_core::errors::VtopError;
use vtop_core::partitioning::DEFAULT_TEMPLATE;
use vtop_core::types::{ChecksumAlgorithm, CompressionType, ProgressMarker, SourceType};

/// Build a minimal file-source config pointing at temp paths and a backend.
pub fn file_config(
    work_dir: &str,
    state_store: &str,
    paths: Vec<String>,
    backend: &str,
) -> VtopConfig {
    VtopConfig {
        engine: EngineConfig {
            name: "vtop-test".into(),
            tenant: "default".into(),
            state_store: state_store.into(),
            work_dir: work_dir.into(),
            log_level: "warn".into(),
        },
        batching: BatchingConfig {
            max_records: 10_000,
            max_bytes: 104_857_600,
            max_batch_age_seconds: 60,
            source_poll_wait_ms: 250,
            idle_poll_interval_ms: 2_000,
            max_concurrent_batches: 8,
        },
        compression: CompressionConfig {
            kind: CompressionType::Gzip,
            level: 6,
        },
        checksum: ChecksumConfig {
            algorithm: ChecksumAlgorithm::Sha256,
        },
        manifest_mac_key_env: None,
        sources: SourcesConfig {
            kafka: None,
            file: Some(FileSourceConfig {
                enabled: true,
                paths,
                delete_after_commit: false,
                whole_file: false,
            }),
            syslog_spool: None,
        },
        upload: UploadConfig {
            backend: backend.into(),
            bucket: "telemetry-data".into(),
            prefix: "telemetry-data".into(),
            endpoint_url: None,
            region: "us-east-1".into(),
            force_path_style: true,
            verify_tls: false,
            profile: None,
            command_binary: None,
            command_timeout_seconds: 300,
            command_max_output_bytes: 1024 * 1024,
            command_env_allowlist: Vec::new(),
            create_bucket: false,
            local_path: None,
            require_strong_verification: false,
        },
        partitioning: PartitioningConfig {
            template: DEFAULT_TEMPLATE.into(),
        },
    }
}

/// A `SourceAdapter` wrapper that fails `commit_progress` the first `n` times,
/// simulating a crash after VERIFIED but before SOURCE_COMMITTED.
pub struct FailCommitAdapter<A: SourceAdapter> {
    inner: A,
    fail_remaining: AtomicUsize,
}

impl<A: SourceAdapter> FailCommitAdapter<A> {
    pub fn new(inner: A, fail_times: usize) -> Self {
        Self {
            inner,
            fail_remaining: AtomicUsize::new(fail_times),
        }
    }
}

#[async_trait]
impl<A: SourceAdapter + 'static> SourceAdapter for FailCommitAdapter<A> {
    async fn discover_sources(&self) -> Result<Vec<DiscoveredSource>, VtopError> {
        self.inner.discover_sources().await
    }
    async fn read_batch_candidates(
        &mut self,
        source: &DiscoveredSource,
        max_records: usize,
        max_bytes: usize,
        max_wait: Duration,
    ) -> Result<Vec<ReadResult>, VtopError> {
        self.inner
            .read_batch_candidates(source, max_records, max_bytes, max_wait)
            .await
    }
    async fn commit_progress(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
        if self.fail_remaining.load(Ordering::SeqCst) > 0 {
            self.fail_remaining.fetch_sub(1, Ordering::SeqCst);
            return Err(VtopError::Source("simulated crash before commit".into()));
        }
        self.inner.commit_progress(marker).await
    }
    async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
        self.inner.replay_from_marker(marker).await
    }
    fn source_type(&self) -> SourceType {
        self.inner.source_type()
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Convenience: build a [`Pipeline`] over borrowed parts.
pub fn pipeline<'a>(
    store: &'a vtop_state::SqliteStateStore,
    backend: Arc<dyn vtop_upload::UploadBackend>,
    config: &'a VtopConfig,
) -> Pipeline<'a> {
    Pipeline {
        store,
        backend,
        config,
        manifest_mac_key: config.resolve_manifest_mac_key().unwrap(),
    }
}

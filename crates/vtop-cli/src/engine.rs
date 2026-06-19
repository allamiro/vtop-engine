//! VTOP Engine runtime.
//!
//! Drives a telemetry batch through the full state machine and enforces, in
//! code, the core rule:
//!
//! ```text
//! SOURCE_COMMITTED is forbidden until VERIFIED is true.
//! ```
//!
//! The state store is updated after *every* transition so the engine is
//! crash-recoverable, and `commit_progress` is invoked on the source adapter
//! only after the batch reaches `VERIFIED`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use vtop_adapters::base::{DiscoveredSource, ReadResult, SourceAdapter};
use vtop_adapters::{FileSource, KafkaSource, SyslogSpoolSource};
use vtop_core::batch::{AdaptiveBatcher, BatchLimits, SealReason, TelemetryBatch};
use vtop_core::compression::compress_batch;
use vtop_core::config::{StreamConfig, StreamsConfig, VtopConfig};
use vtop_core::errors::VtopError;
use vtop_core::manifest::{ManifestBuilder, VerificationStatus};
use vtop_core::metrics::BatchMetrics;
use vtop_core::partitioning::{self, PartitionContext};
use vtop_core::replay::{next_recovery_action, RecoveryAction};
use vtop_core::state_machine::BatchState;
use vtop_core::types::{ProgressMarker, SourceType};
use vtop_state::{BatchPatch, BatchRecord, SqliteStateStore};
use vtop_upload::{ObjectChecksum, UploadBackend};

/// Outcome of processing a single batch.
#[derive(Debug, Clone)]
pub struct BatchOutcome {
    pub batch_id: String,
    pub final_state: BatchState,
    pub committed: bool,
    pub record_count: usize,
    pub object_uri: Option<String>,
    /// End-to-end timing / size / throughput metrics for this batch.
    pub metrics: Option<BatchMetrics>,
}

/// Borrowed context shared by every pipeline step.
pub struct Pipeline<'a> {
    pub store: &'a SqliteStateStore,
    pub backend: Arc<dyn UploadBackend>,
    pub config: &'a VtopConfig,
}

impl<'a> Pipeline<'a> {
    /// Run a [`ReadResult`] all the way through the state machine. Source
    /// progress is committed only if (and after) verification passes.
    pub async fn process(
        &self,
        adapter: &mut dyn SourceAdapter,
        source: &DiscoveredSource,
        read: ReadResult,
        stream: Option<&StreamConfig>,
    ) -> Result<BatchOutcome, VtopError> {
        if read.is_empty() {
            return Ok(BatchOutcome {
                batch_id: String::new(),
                final_state: BatchState::Discovered,
                committed: false,
                record_count: 0,
                object_uri: None,
                metrics: None,
            });
        }

        let started = Instant::now();

        let tenant = stream
            .map(|s| s.tenant.clone())
            .unwrap_or_else(|| self.config.engine.tenant.clone());
        // Format precedence: explicit stream config > content detection.
        // Detection lets one pipeline handle CEF/JSON/JSONL/syslog/text without
        // per-source configuration and keeps the object extension + manifest
        // `format` accurate.
        let format = match stream.map(|s| s.format.clone()) {
            Some(f) => f,
            None => {
                let detected = vtop_core::detect::detect_batch(&read.records);
                tracing::info!(
                    source = %source.source_name,
                    format = %detected,
                    "format_detected"
                );
                detected
            }
        };
        // Optional path rename (e.g. app_events -> app) for the object key.
        let s3_source = stream
            .and_then(|s| s.s3_source_name.clone())
            .unwrap_or_else(|| source.source_name.clone());

        let batch_id = vtop_core::batch::build_batch_id(&source.source_name, &read.progress_end);
        let now = Utc::now().to_rfc3339();

        // ---- DISCOVERED -> BATCHING (persisted) -------------------------
        let mut batch = TelemetryBatch {
            batch_id: batch_id.clone(),
            tenant: tenant.clone(),
            source_type: source.source_type.clone(),
            source_name: source.source_name.clone(),
            format: format.clone(),
            records: read.records,
            record_count: 0,
            first_timestamp: read.first_timestamp.clone(),
            last_timestamp: read.last_timestamp.clone(),
            progress_start: read.progress_start.clone(),
            progress_end: read.progress_end.clone(),
            created_at: now.clone(),
            sealed_at: None,
            state: BatchState::Batching,
        };

        let record = BatchRecord {
            batch_id: batch_id.clone(),
            tenant: tenant.clone(),
            source_type: source.source_type.clone(),
            source_name: source.source_name.clone(),
            format: format.clone(),
            state: BatchState::Batching,
            progress_start: read.progress_start.clone(),
            progress_end: read.progress_end.clone(),
            object_uri: None,
            manifest_uri: None,
            object_sha256: None,
            manifest_sha256: None,
            record_count: Some(batch.records.len() as i64),
            error_message: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        self.store.save_batch_state(&record).await?;
        tracing::info!(batch_id, source = %source.source_name, "batch_started");

        // Helper to fail a batch and bail out.
        macro_rules! fail {
            ($msg:expr) => {{
                let m: String = $msg;
                self.store.mark_failed(&batch_id, &m).await?;
                tracing::error!(batch_id, error = %m, "batch failed");
                return Ok(BatchOutcome {
                    batch_id: batch_id.clone(),
                    final_state: BatchState::Failed,
                    committed: false,
                    record_count: batch.record_count,
                    object_uri: None,
                    metrics: None,
                });
            }};
        }

        // ---- BATCHING -> SEALED -----------------------------------------
        batch.seal()?;
        self.store
            .update_batch_state(&batch_id, BatchState::Sealed, &BatchPatch::default())
            .await?;
        tracing::info!(batch_id, records = batch.record_count, "batch_sealed");

        let mut metrics = BatchMetrics::new(&batch_id, batch.record_count, 0);

        // ---- SEALED -> COMPRESSED ---------------------------------------
        let work_dir = std::path::Path::new(&self.config.engine.work_dir);
        let t = Instant::now();
        let compressed = match compress_batch(
            &batch,
            self.config.compression.kind,
            self.config.compression.level,
            work_dir,
        ) {
            Ok(c) => c,
            Err(e) => fail!(format!("compression failed: {e}")),
        };
        metrics.compress_ms = t.elapsed().as_millis() as u64;
        metrics.uncompressed_bytes = compressed.uncompressed_bytes;
        metrics.set_compression(compressed.size_bytes);
        self.store
            .update_batch_state(&batch_id, BatchState::Compressed, &BatchPatch::default())
            .await?;
        tracing::info!(
            batch_id,
            size = compressed.size_bytes,
            uncompressed = compressed.uncompressed_bytes,
            ratio = format!("{:.2}", metrics.compression_ratio),
            "object_compressed"
        );

        // ---- COMPRESSED -> CHECKSUMMED ----------------------------------
        // Algorithm is configurable: sha256, blake3, or disabled (None).
        let algo = self.config.checksum.algorithm;
        let t = Instant::now();
        let object_checksum = match vtop_core::checksum::digest_file(algo, &compressed.path).await {
            Ok(d) => d,
            Err(e) => fail!(format!("checksum failed: {e}")),
        };
        metrics.checksum_ms = t.elapsed().as_millis() as u64;
        self.store
            .update_batch_state(&batch_id, BatchState::Checksummed, &BatchPatch::default())
            .await?;
        tracing::info!(
            batch_id,
            algorithm = %algo,
            checksum = object_checksum.as_deref().unwrap_or("(disabled)"),
            "checksum_calculated"
        );

        // ---- Resolve object / manifest URIs -----------------------------
        let ctx = PartitionContext::new(&tenant, &s3_source, format.clone(), Utc::now());
        let ctx = match &stream.and_then(|s| s.retention_class.clone()) {
            Some(rc) => ctx.with("retention_class", rc.clone()),
            None => ctx,
        };
        let resolved_prefix =
            partitioning::resolve_template(&self.config.partitioning.template, &ctx);
        // Bucket may be templated (e.g. "telemetry-{format}") for one bucket
        // per data format.
        let bucket = partitioning::resolve_bucket(&self.config.upload.bucket, &ctx);
        let object_uri = partitioning::object_uri(
            &bucket,
            &self.config.upload.prefix,
            &resolved_prefix,
            &batch_id,
            format.clone(),
            compressed.compression,
        );
        let manifest_uri = partitioning::manifest_uri(
            &bucket,
            &self.config.upload.prefix,
            &resolved_prefix,
            &batch_id,
        );

        // Optionally provision the (per-format) bucket on demand.
        if self.config.upload.create_bucket {
            if let Err(e) = self.backend.ensure_bucket(&bucket).await {
                fail!(format!("ensure_bucket {bucket} failed: {e}"));
            }
        }

        // ---- CHECKSUMMED -> OBJECT_UPLOADED -----------------------------
        // Engine-computed object checksum (algorithm + hex), if enabled.
        let object_ck = object_checksum
            .as_deref()
            .map(|h| ObjectChecksum::new(algo.as_str(), h));

        let t = Instant::now();
        if let Err(e) = self
            .backend
            .put_object(&compressed.path, &object_uri, object_ck)
            .await
        {
            fail!(format!("object upload failed: {e}"));
        }
        metrics.object_upload_ms = t.elapsed().as_millis() as u64;
        let obj_patch = BatchPatch {
            object_uri: Some(object_uri.clone()),
            object_sha256: Some(object_checksum.clone().unwrap_or_default()),
            record_count: Some(batch.record_count as i64),
            ..Default::default()
        };
        self.store
            .update_batch_state(&batch_id, BatchState::ObjectUploaded, &obj_patch)
            .await?;
        tracing::info!(batch_id, uri = %object_uri, "object_uploaded");

        // ---- Build + upload manifest ------------------------------------
        let manifest = ManifestBuilder {
            batch_id: batch_id.clone(),
            tenant: tenant.clone(),
            source_type: source.source_type.clone(),
            source_name: source.source_name.clone(),
            format: format.clone(),
            compression: compressed.compression,
            record_count: batch.record_count,
            first_timestamp: batch.first_timestamp.clone(),
            last_timestamp: batch.last_timestamp.clone(),
            source_progress: read.progress_end.clone(),
            object_uri: object_uri.clone(),
            object_size: compressed.size_bytes,
            object_checksum_algorithm: algo.as_str().to_string(),
            object_checksum: object_checksum.clone().unwrap_or_default(),
            manifest_uri: manifest_uri.clone(),
            path_template: self.config.partitioning.template.clone(),
            resolved_prefix: resolved_prefix.clone(),
            upload_backend: self.backend.backend_name().to_string(),
            created_at: now.clone(),
        }
        .build()?;

        let manifest_path = manifest.write_to_file(work_dir)?;
        // Storage-integrity digest of the manifest file (configured algorithm).
        // The manifest's own self-hash (manifest.manifest.sha256) is the
        // tamper-evidence record and is always SHA-256.
        let manifest_checksum = vtop_core::checksum::digest_file(algo, &manifest_path).await?;
        let manifest_ck = manifest_checksum
            .as_deref()
            .map(|h| ObjectChecksum::new(algo.as_str(), h));
        let manifest_size = std::fs::metadata(&manifest_path)?.len();

        // ---- OBJECT_UPLOADED -> MANIFEST_UPLOADED -----------------------
        let t = Instant::now();
        if let Err(e) = self
            .backend
            .put_manifest(&manifest_path, &manifest_uri, manifest_ck)
            .await
        {
            fail!(format!("manifest upload failed: {e}"));
        }
        metrics.manifest_upload_ms = t.elapsed().as_millis() as u64;
        let man_patch = BatchPatch {
            manifest_uri: Some(manifest_uri.clone()),
            manifest_sha256: Some(manifest.manifest.sha256.clone()),
            ..Default::default()
        };
        self.store
            .update_batch_state(&batch_id, BatchState::ManifestUploaded, &man_patch)
            .await?;
        tracing::info!(batch_id, uri = %manifest_uri, "manifest_uploaded");

        // ---- MANIFEST_UPLOADED -> VERIFIED (or FAILED) ------------------
        let t = Instant::now();
        // 1) the manifest is internally consistent (self-hash),
        let mut manifest = manifest;
        if let Err(e) = manifest.verify_self_hash() {
            fail!(format!("manifest self-hash verification failed: {e}"));
        }
        // 2) the stored object matches size + checksum,
        let obj_v = self
            .backend
            .verify_object(&object_uri, compressed.size_bytes, object_ck)
            .await?;
        if !obj_v.passed {
            fail!(format!("object verification failed: {}", obj_v.message));
        }
        // 3) the stored manifest matches size + checksum.
        let man_v = self
            .backend
            .verify_object(&manifest_uri, manifest_size, manifest_ck)
            .await?;
        if !man_v.passed {
            fail!(format!("manifest verification failed: {}", man_v.message));
        }
        metrics.verify_ms = t.elapsed().as_millis() as u64;
        if obj_v.backend_limited || man_v.backend_limited {
            tracing::warn!(batch_id, "verification_passed (backend_limited: size-only)");
            manifest.set_verification(VerificationStatus::BackendLimited);
        } else {
            tracing::info!(batch_id, "verification_passed");
            manifest.set_verification(VerificationStatus::Passed);
        }
        self.store.mark_verified(&batch_id).await?;

        // ---- VERIFIED -> SOURCE_COMMITTED -------------------------------
        // Only now is it legal to advance source progress.
        let t = Instant::now();
        if let Err(e) = adapter.commit_progress(&read.progress_end).await {
            // Verified but commit failed: leave as VERIFIED so recovery retries
            // the commit. NEVER lose the verified object.
            tracing::error!(batch_id, error = %e, "source_commit_failed (will retry on recovery)");
            metrics.finalize(started.elapsed().as_millis() as u64);
            return Ok(BatchOutcome {
                batch_id,
                final_state: BatchState::Verified,
                committed: false,
                record_count: batch.record_count,
                object_uri: Some(object_uri),
                metrics: Some(metrics),
            });
        }
        metrics.commit_ms = t.elapsed().as_millis() as u64;
        self.store.mark_source_committed(&batch_id).await?;
        tracing::info!(batch_id, "source_committed");

        metrics.finalize(started.elapsed().as_millis() as u64);
        tracing::info!(
            batch_id,
            records = metrics.records,
            uncompressed_bytes = metrics.uncompressed_bytes,
            compressed_bytes = metrics.compressed_bytes,
            compression_ratio = format!("{:.2}", metrics.compression_ratio),
            total_ms = metrics.total_ms,
            records_per_sec = format!("{:.0}", metrics.records_per_sec),
            upload_mib_per_sec = format!("{:.2}", metrics.upload_mib_per_sec),
            "batch_metrics"
        );

        Ok(BatchOutcome {
            batch_id,
            final_state: BatchState::SourceCommitted,
            committed: true,
            record_count: batch.record_count,
            object_uri: Some(object_uri),
            metrics: Some(metrics),
        })
    }
}

/// Per-source accumulation buffer that coalesces records read across multiple
/// poll cycles into one adaptive batch.
///
/// This is what wires [`AdaptiveBatcher`] (and therefore the
/// `max_batch_age_seconds` threshold) into the running engine: records are held
/// until a size/record/age threshold trips, instead of being flushed eagerly on
/// every read. Holding data here is replay-safe because nothing is persisted to
/// the state store or committed to the source until the buffer is flushed
/// through the pipeline; an unflushed buffer simply re-reads from the last
/// committed source position after a crash.
struct PendingBuffer {
    source: DiscoveredSource,
    batcher: AdaptiveBatcher,
    /// Progress marker covering the most recent read — the candidate commit
    /// point for the whole accumulated window.
    latest_end: Option<ProgressMarker>,
    first_timestamp: Option<String>,
    last_timestamp: Option<String>,
}

impl PendingBuffer {
    fn new(tenant: &str, source: DiscoveredSource, limits: BatchLimits) -> Self {
        let batcher = AdaptiveBatcher::new(
            tenant,
            source.source_type.clone(),
            source.source_name.clone(),
            source.format.clone(),
            limits,
        );
        Self {
            source,
            batcher,
            latest_end: None,
            first_timestamp: None,
            last_timestamp: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.batcher.is_empty()
    }

    /// Append a non-empty read into the buffer. The first read's
    /// `progress_start` becomes the window start (the replayable position); the
    /// latest read's `progress_end` becomes the candidate commit point.
    fn append(&mut self, read: ReadResult) {
        let start = read.progress_start.clone();
        for record in read.records {
            // Only the first marker observed sets the window start; the rest are
            // ignored by the batcher.
            self.batcher.push(record, &start, None);
        }
        if self.first_timestamp.is_none() {
            self.first_timestamp = read.first_timestamp;
        }
        if read.last_timestamp.is_some() {
            self.last_timestamp = read.last_timestamp;
        }
        self.latest_end = Some(read.progress_end);
    }

    /// Whether an accumulated buffer has tripped a sealing threshold
    /// (`max_records`, `max_bytes`, or `max_batch_age_seconds`).
    fn should_seal(&self, now: chrono::DateTime<Utc>) -> Option<SealReason> {
        self.batcher.should_seal(now)
    }

    /// Drain the buffer into a single [`ReadResult`] spanning the whole window,
    /// resetting the underlying batcher for reuse. Returns `None` if empty.
    fn drain(&mut self) -> Option<ReadResult> {
        let end = self.latest_end.take()?;
        let batch = self.batcher.seal(end).ok()?;
        Some(ReadResult {
            records: batch.records,
            progress_start: batch.progress_start,
            progress_end: batch.progress_end,
            first_timestamp: self.first_timestamp.take(),
            last_timestamp: self.last_timestamp.take(),
        })
    }
}

/// The full engine: config, streams, state store, upload backend, adapters.
pub struct Engine {
    pub config: VtopConfig,
    pub streams: StreamsConfig,
    pub store: SqliteStateStore,
    pub backend: Arc<dyn UploadBackend>,
    pub adapters: HashMap<SourceType, Box<dyn SourceAdapter>>,
    /// Per-source accumulation buffers, keyed by `(source_type, source_name)`.
    pending: HashMap<(SourceType, String), PendingBuffer>,
}

impl Engine {
    /// Build the engine from parsed config + streams, initializing the state
    /// store, the upload backend, and every enabled source adapter.
    pub async fn new(config: VtopConfig, streams: StreamsConfig) -> Result<Self, VtopError> {
        let store = SqliteStateStore::connect(&config.engine.state_store).await?;
        let backend = vtop_upload::build_backend(&config.upload).await?;

        let mut adapters: HashMap<SourceType, Box<dyn SourceAdapter>> = HashMap::new();
        if let Some(k) = &config.sources.kafka {
            if k.enabled {
                let fmt = default_format_for(&streams, SourceType::Kafka);
                adapters.insert(
                    SourceType::Kafka,
                    Box::new(KafkaSource::new(k.clone(), fmt)?),
                );
            }
        }
        if let Some(f) = &config.sources.file {
            if f.enabled {
                let fmt = default_format_for(&streams, SourceType::File);
                adapters.insert(
                    SourceType::File,
                    Box::new(FileSource::with_mode(
                        f.paths.clone(),
                        fmt,
                        f.delete_after_commit,
                        f.whole_file,
                    )),
                );
            }
        }
        if let Some(s) = &config.sources.syslog_spool {
            if s.enabled {
                adapters.insert(
                    SourceType::SyslogSpool,
                    Box::new(SyslogSpoolSource::new(s.paths.clone())),
                );
            }
        }

        Ok(Self {
            config,
            streams,
            store,
            backend,
            adapters,
            pending: HashMap::new(),
        })
    }

    /// Batching thresholds (records / bytes / age) from config.
    fn batch_limits(&self) -> BatchLimits {
        BatchLimits {
            max_records: self.config.batching.max_records,
            max_bytes: self.config.batching.max_bytes,
            max_batch_age_seconds: self.config.batching.max_batch_age_seconds,
        }
    }

    fn pipeline(&self) -> Pipeline<'_> {
        Pipeline {
            store: &self.store,
            backend: self.backend.clone(),
            config: &self.config,
        }
    }

    /// Enumerate all sources across all enabled adapters.
    pub async fn discover(&self) -> Result<Vec<DiscoveredSource>, VtopError> {
        let mut all = Vec::new();
        for adapter in self.adapters.values() {
            match adapter.discover_sources().await {
                Ok(mut s) => all.append(&mut s),
                Err(e) => tracing::warn!(error = %e, "source discovery failed for an adapter"),
            }
        }
        Ok(all)
    }

    /// Process one cycle for the given source type, flushing every buffer that
    /// has records. This is the single-shot entry point used by the
    /// `process-once` CLI command and the integration tests: whatever is read
    /// (plus anything already buffered) is sealed and pushed through the
    /// pipeline immediately.
    pub async fn process_once(
        &mut self,
        source_type: SourceType,
    ) -> Result<Vec<BatchOutcome>, VtopError> {
        self.run_source(source_type, true).await
    }

    /// Run one read+accumulate(+flush) cycle for a source type. When
    /// `force_flush` is false, only buffers that have tripped a sealing
    /// threshold (`max_records`, `max_bytes`, or `max_batch_age_seconds`) are
    /// flushed; the rest keep accumulating across cycles.
    async fn run_source(
        &mut self,
        source_type: SourceType,
        force_flush: bool,
    ) -> Result<Vec<BatchOutcome>, VtopError> {
        let Some(mut adapter) = self.adapters.remove(&source_type) else {
            return Err(VtopError::Source(format!(
                "no enabled adapter for source type {source_type}"
            )));
        };
        let result = self
            .run_cycle(adapter.as_mut(), &source_type, force_flush)
            .await;
        self.adapters.insert(source_type, adapter);
        result
    }

    async fn run_cycle(
        &mut self,
        adapter: &mut dyn SourceAdapter,
        source_type: &SourceType,
        force_flush: bool,
    ) -> Result<Vec<BatchOutcome>, VtopError> {
        let sources = adapter.discover_sources().await?;
        let max_wait = Duration::from_secs(2);

        // ---- Read + accumulate -----------------------------------------
        for source in sources {
            let read = adapter
                .read_batch_candidates(
                    &source,
                    self.config.batching.max_records,
                    self.config.batching.max_bytes,
                    max_wait,
                )
                .await?;
            if read.is_empty() {
                continue;
            }
            let limits = self.batch_limits();
            let tenant = self.config.engine.tenant.clone();
            self.pending
                .entry((source.source_type.clone(), source.source_name.clone()))
                .or_insert_with(|| PendingBuffer::new(&tenant, source.clone(), limits))
                .append(read);
        }

        // ---- Decide which buffers to flush -----------------------------
        let now = Utc::now();
        let mut flush_keys: Vec<(SourceType, String)> = Vec::new();
        for (key, buf) in self.pending.iter() {
            if &buf.source.source_type != source_type || buf.is_empty() {
                continue;
            }
            if let Some(reason) = buf.should_seal(now) {
                tracing::info!(source = %buf.source.source_name, ?reason, "batch_seal_threshold");
                flush_keys.push(key.clone());
            } else if force_flush {
                flush_keys.push(key.clone());
            }
        }

        // ---- Flush through the pipeline --------------------------------
        let mut outcomes = Vec::new();
        for key in flush_keys {
            let Some(mut buf) = self.pending.remove(&key) else {
                continue;
            };
            let Some(read) = buf.drain() else { continue };
            let stream = self.streams.lookup(&buf.source.source_name).cloned();
            let outcome = self
                .pipeline()
                .process(adapter, &buf.source, read, stream.as_ref())
                .await?;
            outcomes.push(outcome);
        }
        Ok(outcomes)
    }

    /// Recovery scan run at startup. Seeds adapters from committed batches and
    /// resolves incomplete batches without ever advancing source progress for
    /// unverified data.
    pub async fn recover(&mut self) -> Result<RecoverySummary, VtopError> {
        // Seed file/syslog committed offsets from previously committed batches.
        self.seed_committed_offsets().await?;

        let incomplete = self.store.list_incomplete_batches().await?;
        let mut summary = RecoverySummary::default();
        for rec in incomplete {
            let action = next_recovery_action(rec.state);
            tracing::info!(batch_id = %rec.batch_id, state = %rec.state, ?action, "recovery_scan");
            match action {
                RecoveryAction::RetrySourceCommit => {
                    // Verified but not committed — safe to commit now.
                    if let Some(adapter) = self.adapters.get_mut(&rec.source_type) {
                        match adapter.commit_progress(&rec.progress_end).await {
                            Ok(()) => {
                                self.store.mark_source_committed(&rec.batch_id).await?;
                                summary.committed += 1;
                                tracing::info!(batch_id = %rec.batch_id, "recovered: source_committed");
                            }
                            Err(e) => {
                                tracing::error!(batch_id = %rec.batch_id, error = %e, "recovery commit failed");
                                summary.still_pending += 1;
                            }
                        }
                    } else {
                        summary.still_pending += 1;
                    }
                }
                RecoveryAction::None => {}
                _ => {
                    // Any other incomplete state has no durable, replayable
                    // object we can finish from in this prototype: mark for
                    // replay so the *uncommitted* source range is re-read.
                    // Source progress was never advanced, so this is safe.
                    let _ = self
                        .store
                        .mark_failed(&rec.batch_id, "recovered: replay required")
                        .await;
                    self.store
                        .update_batch_state(
                            &rec.batch_id,
                            BatchState::ReplayRequired,
                            &BatchPatch::default(),
                        )
                        .await
                        .ok();
                    if let Some(adapter) = self.adapters.get_mut(&rec.source_type) {
                        let _ = adapter.replay_from_marker(&rec.progress_start).await;
                    }
                    summary.replay_required += 1;
                    tracing::warn!(batch_id = %rec.batch_id, "recovered: REPLAY_REQUIRED (source progress preserved)");
                }
            }
        }
        Ok(summary)
    }

    async fn seed_committed_offsets(&mut self) -> Result<(), VtopError> {
        let all = self.store.list_batches().await?;
        // path -> highest committed end_byte
        let mut file_max: HashMap<String, u64> = HashMap::new();
        let mut spool_max: HashMap<String, u64> = HashMap::new();
        for rec in all
            .into_iter()
            .filter(|r| r.state == BatchState::SourceCommitted)
        {
            match &rec.progress_end {
                ProgressMarker::File { path, end_byte, .. } => {
                    let e = file_max.entry(path.clone()).or_default();
                    *e = (*e).max(*end_byte);
                }
                ProgressMarker::SyslogSpool { path, end_byte, .. } => {
                    let e = spool_max.entry(path.clone()).or_default();
                    *e = (*e).max(*end_byte);
                }
                ProgressMarker::Kafka { .. } => { /* Kafka resumes from broker-side committed offset */
                }
            }
        }
        if let Some(a) = self.adapters.get_mut(&SourceType::File) {
            if let Some(fs) = a.as_any_mut().downcast_mut::<FileSource>() {
                for (p, b) in file_max {
                    fs.seed_committed(&p, b);
                }
            }
        }
        if let Some(a) = self.adapters.get_mut(&SourceType::SyslogSpool) {
            if let Some(ss) = a.as_any_mut().downcast_mut::<SyslogSpoolSource>() {
                for (p, b) in spool_max {
                    ss.seed_committed(&p, b);
                }
            }
        }
        Ok(())
    }

    /// Run the continuous processing loop until the shutdown signal fires.
    pub async fn run(&mut self) -> Result<(), VtopError> {
        self.recover().await?;
        let types: Vec<SourceType> = self.adapters.keys().cloned().collect();
        loop {
            // Accumulate across cycles; only threshold-tripped buffers flush.
            for st in &types {
                if let Err(e) = self.run_source(st.clone(), false).await {
                    tracing::error!(error = %e, source_type = %st, "process cycle error");
                }
            }
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("shutdown signal received; flushing and exiting");
                    // Force-flush any buffered-but-unsealed data so it is not
                    // left for the next start to re-read.
                    for st in &types {
                        if let Err(e) = self.run_source(st.clone(), true).await {
                            tracing::error!(error = %e, source_type = %st, "shutdown flush error");
                        }
                    }
                    return Ok(());
                }
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
        }
    }
}

/// Default format for an adapter, taken from the first matching stream of that
/// source type, falling back to `Raw`.
fn default_format_for(
    streams: &StreamsConfig,
    st: SourceType,
) -> vtop_core::types::TelemetryFormat {
    streams
        .streams
        .iter()
        .find(|s| s.source_type == st)
        .map(|s| s.format.clone())
        .unwrap_or(vtop_core::types::TelemetryFormat::Raw)
}

#[derive(Debug, Default, Clone)]
pub struct RecoverySummary {
    pub committed: usize,
    pub replay_required: usize,
    pub still_pending: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::file_config;
    use std::io::Write;
    use vtop_core::config::StreamsConfig;

    /// A sub-threshold read must be buffered (not flushed) on a non-forced
    /// cycle, then sealed and committed when the cycle forces a flush — proving
    /// `AdaptiveBatcher`/`BatchLimits` is actually driving runtime batching.
    #[tokio::test]
    async fn run_source_holds_subthreshold_data_until_forced() {
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        let input = dir.path().join("in.log");
        {
            let mut f = std::fs::File::create(&input).unwrap();
            writeln!(f, "only-one-line").unwrap();
        }

        let mut cfg = file_config(
            work.to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        // Thresholds high enough that one short line trips none of them during
        // the test (records, bytes, and a long age window).
        cfg.batching.max_records = 10_000;
        cfg.batching.max_bytes = 104_857_600;
        cfg.batching.max_batch_age_seconds = 3_600;

        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();

        // Non-forced cycle: the lone record is buffered, not flushed.
        let held = engine.run_source(SourceType::File, false).await.unwrap();
        assert!(
            held.is_empty(),
            "sub-threshold data must be held, not flushed eagerly"
        );

        // Forced cycle (e.g. process-once / shutdown): the buffer is sealed.
        let flushed = engine.run_source(SourceType::File, true).await.unwrap();
        assert_eq!(flushed.len(), 1, "force flush seals the buffered batch");
        assert_eq!(flushed[0].record_count, 1);
        assert!(
            flushed[0].committed,
            "flushed batch must commit after verify"
        );
    }
}

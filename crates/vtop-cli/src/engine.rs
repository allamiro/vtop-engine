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
use futures::StreamExt;
use vtop_adapters::base::{DiscoveredSource, ReadResult, SourceAdapter};
use vtop_adapters::{FileSource, KafkaSource, SyslogSpoolSource};
use vtop_core::batch::{AdaptiveBatcher, BatchLimits, SealReason, TelemetryBatch};
use vtop_core::compression::compress_batch;
use vtop_core::config::{ResolvedStateStore, StreamConfig, StreamsConfig, VtopConfig};
use vtop_core::errors::VtopError;
use vtop_core::manifest::{ManifestBuilder, ManifestMacKey, VtopManifest, MAX_MANIFEST_BYTES};
use vtop_core::metrics::BatchMetrics;
use vtop_core::partitioning::{self, PartitionContext};
use vtop_core::replay::{next_recovery_action, RecoveryAction};
use vtop_core::state_machine::BatchState;
use vtop_core::telemetry;
use vtop_core::types::{ChecksumAlgorithm, ProgressMarker, SourceType};
use vtop_state::{connect_state_store, BatchPatch, BatchRecord, StateStore};
use vtop_upload::{ObjectChecksum, UploadBackend};

/// How long a batch claim stays exclusive without renewal (#93). Batches
/// complete in seconds; the lease only has to outlive the slowest healthy
/// batch by a wide margin so a crashed engine's work transfers reasonably
/// fast. Renewal-per-transition can shorten this later if needed.
const LEASE_TTL_SECS: i64 = 600;

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

/// A batch that reached VERIFIED and is waiting only for its source commit.
///
/// Exists so the verify phase (concurrent, adapter-free) can hand work to the
/// commit phase (serial, needs `&mut` adapter) without either half knowing
/// about the other's scheduling.
pub struct VerifiedBatch {
    batch_id: String,
    object_uri: String,
    progress_end: ProgressMarker,
    record_count: usize,
    tenant: String,
    source_type: SourceType,
    format: vtop_core::types::TelemetryFormat,
    metrics: BatchMetrics,
    started: Instant,
}

/// Result of the verify phase: either the batch is ready to commit, or it
/// already reached a terminal outcome (empty read, or FAILED) and there is
/// nothing left to do.
pub enum VerifyStep {
    Verified(VerifiedBatch),
    Finished(BatchOutcome),
}

/// Borrowed context shared by every pipeline step.
pub struct Pipeline<'a> {
    pub store: &'a dyn StateStore,
    pub backend: Arc<dyn UploadBackend>,
    pub config: &'a VtopConfig,
    /// Runtime-only secret, resolved once at startup and never serialized.
    pub manifest_mac_key: Option<ManifestMacKey>,
}

impl<'a> Pipeline<'a> {
    /// Run a [`ReadResult`] all the way through the state machine. Source
    /// progress is committed only if (and after) verification passes.
    ///
    /// Thin wrapper over the two halves below, kept so existing callers and
    /// tests drive one batch end to end unchanged.
    pub async fn process(
        &self,
        adapter: &mut dyn SourceAdapter,
        source: &DiscoveredSource,
        read: ReadResult,
        stream: Option<&StreamConfig>,
    ) -> Result<BatchOutcome, VtopError> {
        match self.process_until_verified(source, read, stream).await? {
            VerifyStep::Finished(outcome) => Ok(outcome),
            VerifyStep::Verified(v) => self.commit_verified(adapter, v).await,
        }
    }

    /// Everything up to and including VERIFIED: seal, compress, checksum,
    /// upload, manifest, verify.
    ///
    /// Deliberately takes NO source adapter. That is what makes it safe to run
    /// for many batches concurrently — it touches only `&self` (store, backend,
    /// config), and the expensive stages here are dominated by blocking network
    /// I/O. `commit_progress` is the one step that needs exclusive adapter
    /// access, and it stays in `commit_verified` below.
    ///
    /// The invariant is unaffected: a batch still reaches SOURCE_COMMITTED only
    /// after IT has reached VERIFIED. Concurrency is ACROSS batches, never
    /// within one, and never between one batch's verify and another's commit.
    async fn process_until_verified(
        &self,
        source: &DiscoveredSource,
        read: ReadResult,
        stream: Option<&StreamConfig>,
    ) -> Result<VerifyStep, VtopError> {
        if read.is_empty() {
            return Ok(VerifyStep::Finished(BatchOutcome {
                batch_id: String::new(),
                final_state: BatchState::Discovered,
                committed: false,
                record_count: 0,
                object_uri: None,
                metrics: None,
            }));
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
            verbatim: read.verbatim,
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
            object_size_bytes: None,
            record_count: Some(batch.records.len() as i64),
            error_message: None,
            // Ownership (#93): this engine claims the batch at birth, leased so
            // a crashed engine's work is reclaimable and a live one's is not.
            owner: Some(self.config.engine.name.clone()),
            lease_expires_at: Some(
                (Utc::now() + chrono::Duration::seconds(LEASE_TTL_SECS)).to_rfc3339(),
            ),
            created_at: now.clone(),
            updated_at: now.clone(),
        };
        // Ledger-write time is accumulated across the WHOLE pipeline (~8
        // writes per batch): it was the unmeasured gap between staged time and
        // total_ms (#87), and an unmeasured stage is where regressions hide.
        let mut state_write = Duration::ZERO;
        let t_write = Instant::now();
        self.store.save_batch_state(&record).await?;
        state_write += t_write.elapsed();
        tracing::info!(batch_id, source = %source.source_name, "batch_started");

        // The record count `fail!` reports. Declared here, before the macro,
        // because macro hygiene only exposes bindings that already exist at the
        // macro's definition site — and `fail!` cannot read `batch` directly,
        // since compression moves it into a blocking task. `seal()` sets
        // record_count to exactly `records.len()`, so this is the sealed count
        // without having to wait for (or mutate after) the seal.
        let sealed_record_count = batch.records.len();

        // Helper to fail a batch and bail out.
        macro_rules! fail {
            ($msg:expr) => {{
                let m: String = $msg;
                self.store.mark_failed(&batch_id, &m).await?;
                // Failure accounting. Deliberately label-free of batch_id: the
                // identifier is unbounded and belongs in the log line below,
                // not in a metric label.
                if let Some(mx) = telemetry::metrics() {
                    let l = [tenant.as_str(), source.source_type.as_str(), format.extension()];
                    mx.failed_total.with_label_values(&l).inc();
                    mx.batches_total
                        .with_label_values(&[l[0], l[1], l[2], BatchState::Failed.as_str()])
                        .inc();
                }
                tracing::error!(batch_id, error = %m, "batch failed");
                return Ok(VerifyStep::Finished(BatchOutcome {
                    batch_id: batch_id.clone(),
                    final_state: BatchState::Failed,
                    committed: false,
                    // Read from the sealed count captured before compression
                    // rather than from `batch`: compression moves the batch into
                    // a blocking task, so a macro that touched `batch` directly
                    // would not compile at any call site after that point.
                    record_count: sealed_record_count,
                    object_uri: None,
                    metrics: None,
                }));
            }};
        }

        // Record every state the batch enters, not just terminal ones: the
        // dashboards claim `sum by (state)` shows WHERE batches stop, which is
        // only true if the intermediate states are counted too.
        macro_rules! mark_state {
            ($state:expr) => {
                if let Some(mx) = telemetry::metrics() {
                    mx.batches_total
                        .with_label_values(&[
                            tenant.as_str(),
                            source.source_type.as_str(),
                            format.extension(),
                            $state.as_str(),
                        ])
                        .inc();
                }
            };
        }
        mark_state!(BatchState::Batching);

        // ---- BATCHING -> SEALED -----------------------------------------
        batch.seal()?;
        let t_write = Instant::now();
        self.store
            .update_batch_state(&batch_id, BatchState::Sealed, &BatchPatch::default())
            .await?;
        state_write += t_write.elapsed();
        mark_state!(BatchState::Sealed);
        tracing::info!(batch_id, records = batch.record_count, "batch_sealed");

        let mut metrics = BatchMetrics::new(&batch_id, batch.record_count, 0);

        // ---- SEALED -> COMPRESSED ---------------------------------------
        let work_dir = std::path::PathBuf::from(&self.config.engine.work_dir);
        let t = Instant::now();
        // Compression is CPU-bound AND writes the object to disk, so running it
        // inline would block this tokio worker for the whole stage (80ms p95 —
        // the largest stage in the pipeline). That stalls everything else
        // sharing the runtime: the metrics server, signal handling, and every
        // other batch once batches run concurrently. spawn_blocking moves it to
        // the blocking pool where stalling is expected and harmless.
        //
        // `batch` is moved in and handed back out because compress_batch borrows
        // it and later stages still need it; the alternative (cloning every
        // record) would cost more than the stage itself.
        let kind = self.config.compression.kind;
        let level = self.config.compression.level;
        let wd = work_dir.clone();
        let (batch, compress_result) = tokio::task::spawn_blocking(move || {
            let r = compress_batch(&batch, kind, level, &wd);
            (batch, r)
        })
        .await
        .map_err(|e| VtopError::Config(format!("compression task panicked: {e}")))?;
        let compressed = match compress_result {
            Ok(c) => c,
            Err(e) => fail!(format!("compression failed: {e}")),
        };
        metrics.compress_ms = t.elapsed().as_millis() as u64;
        metrics.uncompressed_bytes = compressed.uncompressed_bytes;
        metrics.set_compression(compressed.size_bytes);
        let t_write = Instant::now();
        self.store
            .update_batch_state(&batch_id, BatchState::Compressed, &BatchPatch::default())
            .await?;
        state_write += t_write.elapsed();
        mark_state!(BatchState::Compressed);
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
        let t_write = Instant::now();
        self.store
            .update_batch_state(&batch_id, BatchState::Checksummed, &BatchPatch::default())
            .await?;
        state_write += t_write.elapsed();
        mark_state!(BatchState::Checksummed);
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
            // Recorded so recovery's storage re-check can compare size even
            // when no digest is available (#125).
            object_size_bytes: Some(compressed.size_bytes as i64),
            record_count: Some(batch.record_count as i64),
            ..Default::default()
        };
        let t_write = Instant::now();
        self.store
            .update_batch_state(&batch_id, BatchState::ObjectUploaded, &obj_patch)
            .await?;
        state_write += t_write.elapsed();
        mark_state!(BatchState::ObjectUploaded);
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
        .build_with_mac(self.manifest_mac_key.as_ref())?;

        let manifest_path = manifest.write_to_file(&work_dir)?;
        // Storage-integrity digest of the manifest file (configured algorithm).
        // The manifest's own self-hash (manifest.manifest.sha256) is the
        // reproducible corruption-detection record and is always SHA-256.
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
        let t_write = Instant::now();
        self.store
            .update_batch_state(&batch_id, BatchState::ManifestUploaded, &man_patch)
            .await?;
        state_write += t_write.elapsed();
        mark_state!(BatchState::ManifestUploaded);
        tracing::info!(batch_id, uri = %manifest_uri, "manifest_uploaded");

        // ---- MANIFEST_UPLOADED -> VERIFIED (or FAILED) ------------------
        let t = Instant::now();
        // 1) the manifest is internally consistent (self-hash),
        if let Err(e) = manifest.verify_authentication(self.manifest_mac_key.as_ref()) {
            fail!(format!("manifest authentication verification failed: {e}"));
        }
        // 2) the stored object matches size + checksum,
        let obj_v = self
            .backend
            .verify_object(&object_uri, compressed.size_bytes, object_ck)
            .await?;
        if !obj_v.passed {
            if let Some(mx) = telemetry::metrics() {
                mx.verification_failures_total
                    .with_label_values(&[
                        tenant.as_str(),
                        source.source_type.as_str(),
                        format.extension(),
                    ])
                    .inc();
            }
            fail!(format!("object verification failed: {}", obj_v.message));
        }
        // 3) the stored manifest matches size + checksum.
        let man_v = self
            .backend
            .verify_object(&manifest_uri, manifest_size, manifest_ck)
            .await?;
        if !man_v.passed {
            if let Some(mx) = telemetry::metrics() {
                mx.verification_failures_total
                    .with_label_values(&[
                        tenant.as_str(),
                        source.source_type.as_str(),
                        format.extension(),
                    ])
                    .inc();
            }
            fail!(format!("manifest verification failed: {}", man_v.message));
        }
        // A MAC is meaningful only if we authenticate the bytes that actually
        // reached storage. Backend metadata can describe content supplied by
        // the engine rather than content read back from the object (#64), so
        // keyed mode always downloads the small manifest and checks its
        // binding before the batch may reach VERIFIED.
        if let Some(key) = self.manifest_mac_key.as_ref() {
            let stored_bytes = match self
                .backend
                .get_object_bounded(&manifest_uri, MAX_MANIFEST_BYTES)
                .await
            {
                Ok(bytes) => bytes,
                Err(e) => fail!(format!("stored manifest download failed: {e}")),
            };
            let stored: VtopManifest = match serde_json::from_slice(&stored_bytes) {
                Ok(stored) => stored,
                Err(e) => fail!(format!("stored manifest parse failed: {e}")),
            };
            if stored.verify_authentication(Some(key)).is_err()
                || stored.batch_id != batch_id
                || stored.manifest.uri != manifest_uri
                || stored.object.uri != object_uri
            {
                fail!("stored manifest authentication or batch binding failed".to_string());
            }
        }
        metrics.verify_ms = t.elapsed().as_millis() as u64;
        let backend_limited = obj_v.backend_limited || man_v.backend_limited;
        // Strict by default: refuse to commit on size-only (backend-limited)
        // verification. `false` is an explicit compatibility/lab opt-out.
        if backend_limited && self.config.upload.require_strong_verification {
            fail!(format!(
                "strong verification required but backend only confirmed size/existence \
                 (object: {}; manifest: {})",
                obj_v.message, man_v.message
            ));
        }
        // The authoritative post-verification status lives in the state store
        // (VERIFIED -> SOURCE_COMMITTED below). The on-disk manifest was written
        // and uploaded *before* this step (its hash must be stable), so we do
        // not re-stamp it here — querying the store is the source of truth.
        if backend_limited {
            tracing::warn!(batch_id, "verification_passed (backend_limited: size-only)");
        } else {
            tracing::info!(batch_id, "verification_passed");
        }
        let t_write = Instant::now();
        self.store.mark_verified(&batch_id).await?;
        state_write += t_write.elapsed();
        // Counted only AFTER the store has persisted ManifestUploaded ->
        // Verified. Incrementing first would let a failed state write leave
        // verified_total claiming a verification the ledger never recorded -
        // and this counter is one half of how the invariant is checked.
        if let Some(mx) = telemetry::metrics() {
            let l = [
                tenant.as_str(),
                source.source_type.as_str(),
                format.extension(),
            ];
            mx.verified_total.with_label_values(&l).inc();
            mx.batches_total
                .with_label_values(&[l[0], l[1], l[2], BatchState::Verified.as_str()])
                .inc();
            if backend_limited {
                // Verified by size/existence under an explicit weak-mode
                // opt-out, so count it separately from strong verification.
                mx.verification_backend_limited_total
                    .with_label_values(&l)
                    .inc();
            }
        }

        // Converted ONCE: per-write as_millis() truncation made eight sub-ms
        // WAL writes export as 0.
        metrics.state_write_ms = state_write.as_millis() as u64;

        Ok(VerifyStep::Verified(VerifiedBatch {
            batch_id,
            object_uri,
            progress_end: read.progress_end.clone(),
            record_count: batch.record_count,
            tenant,
            source_type: source.source_type.clone(),
            format,
            metrics,
            started,
        }))
    }

    /// VERIFIED -> SOURCE_COMMITTED. Needs `&mut` on the adapter, so it runs
    /// serially even when the verify phase above ran concurrently. This is
    /// cheap (~1ms measured) next to the stages it follows.
    async fn commit_verified(
        &self,
        adapter: &mut dyn SourceAdapter,
        v: VerifiedBatch,
    ) -> Result<BatchOutcome, VtopError> {
        let VerifiedBatch {
            batch_id,
            object_uri,
            progress_end,
            record_count,
            tenant,
            source_type,
            format,
            mut metrics,
            started,
        } = v;

        // ---- VERIFIED -> SOURCE_COMMITTED -------------------------------
        // Only now is it legal to advance source progress.
        let t = Instant::now();
        if let Err(e) = adapter.commit_progress(&progress_end).await {
            // Verified but commit failed: leave as VERIFIED so recovery retries
            // the commit. NEVER lose the verified object.
            tracing::error!(batch_id, error = %e, "source_commit_failed (will retry on recovery)");
            metrics.finalize(started.elapsed().as_millis() as u64);
            return Ok(BatchOutcome {
                batch_id,
                final_state: BatchState::Verified,
                committed: false,
                record_count,
                object_uri: Some(object_uri),
                metrics: Some(metrics),
            });
        }
        metrics.commit_ms = t.elapsed().as_millis() as u64;
        let t_write = Instant::now();
        self.store.mark_source_committed(&batch_id).await?;
        metrics.state_write_ms += t_write.elapsed().as_millis() as u64;
        tracing::info!(batch_id, "source_committed");

        metrics.finalize(started.elapsed().as_millis() as u64);

        // Export what the batch actually measured. Recorded only on the
        // committed path: commits_total must never exceed verified_total, which
        // is how the core invariant becomes observable rather than merely
        // asserted in tests.
        if let Some(mx) = telemetry::metrics() {
            let l = [tenant.as_str(), source_type.as_str(), format.extension()];
            mx.commits_total.with_label_values(&l).inc();
            mx.batches_total
                .with_label_values(&[l[0], l[1], l[2], BatchState::SourceCommitted.as_str()])
                .inc();
            mx.records_total
                .with_label_values(&l)
                .inc_by(metrics.records as u64);
            mx.bytes_in_total
                .with_label_values(&l)
                .inc_by(metrics.uncompressed_bytes);
            mx.bytes_out_total
                .with_label_values(&l)
                .inc_by(metrics.compressed_bytes);
            mx.batch_duration_seconds
                .with_label_values(&l)
                .observe(metrics.total_ms as f64 / 1000.0);
            if metrics.compression_ratio.is_finite() && metrics.compression_ratio > 0.0 {
                mx.compression_ratio
                    .with_label_values(&l)
                    .observe(metrics.compression_ratio);
            }
            // Per-stage latency as histograms, so p95/p99 are answerable; an
            // average would hide the tail that actually pages someone.
            for (stage, ms) in [
                ("compress", metrics.compress_ms),
                ("checksum", metrics.checksum_ms),
                ("object_upload", metrics.object_upload_ms),
                ("manifest_upload", metrics.manifest_upload_ms),
                ("verify", metrics.verify_ms),
                ("commit", metrics.commit_ms),
                ("state_write", metrics.state_write_ms),
            ] {
                mx.stage_duration_seconds
                    .with_label_values(&[l[0], l[1], l[2], stage])
                    .observe(ms as f64 / 1000.0);
            }
        }

        tracing::info!(
            batch_id,
            records = metrics.records,
            uncompressed_bytes = metrics.uncompressed_bytes,
            compressed_bytes = metrics.compressed_bytes,
            compression_ratio = format!("{:.2}", metrics.compression_ratio),
            total_ms = metrics.total_ms,
            state_write_ms = metrics.state_write_ms,
            records_per_sec = format!("{:.0}", metrics.records_per_sec),
            upload_mib_per_sec = format!("{:.2}", metrics.upload_mib_per_sec),
            "batch_metrics"
        );

        Ok(BatchOutcome {
            batch_id,
            final_state: BatchState::SourceCommitted,
            committed: true,
            record_count,
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
    /// Record framing for this source (whole-file/binary = verbatim). All reads
    /// for a given source share the same framing.
    verbatim: bool,
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
            verbatim: false,
        }
    }

    fn is_empty(&self) -> bool {
        self.batcher.is_empty()
    }

    /// Append a non-empty read into the buffer. The first read's
    /// `progress_start` becomes the window start (the replayable position); the
    /// latest read's `progress_end` becomes the candidate commit point.
    fn append(&mut self, read: ReadResult) {
        self.verbatim = read.verbatim;
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
            verbatim: self.verbatim,
        })
    }
}

/// The full engine: config, streams, state store, upload backend, adapters.
pub struct Engine {
    pub config: VtopConfig,
    pub streams: StreamsConfig,
    pub store: Box<dyn StateStore>,
    pub backend: Arc<dyn UploadBackend>,
    pub adapters: HashMap<SourceType, Box<dyn SourceAdapter>>,
    /// Runtime-only manifest authentication secret, resolved from the named
    /// environment variable before any backend is initialized.
    manifest_mac_key: Option<ManifestMacKey>,
    /// Per-source accumulation buffers, keyed by `(source_type, source_name)`.
    pending: HashMap<(SourceType, String), PendingBuffer>,
    /// Set by a read cycle that returned any records; drives the adaptive
    /// inter-cycle sleep in [`Engine::run`]. Reset at the top of each cycle.
    cycle_had_data: bool,
    /// Exclusive work-dir lock held for the engine's lifetime (#66).
    /// Held only by [`Engine::new_exclusive`] instances; empty for read-only
    /// construction (#66).
    _instance_locks: Vec<InstanceLock>,
}

/// An OS-level exclusive lock held for the engine's lifetime and released
/// automatically on drop or process death (#66).
///
/// The engine is SINGLE-INSTANCE per state store: without claim/lease/fencing
/// (#93, HA plan Phase 5), two engines over the same store would both list the
/// same incomplete batches, both recover them, and both commit source
/// progress — duplicate ingestion at best, double-commit at worst.
///
/// The exclusion is keyed by BOTH shared resources: the state store (the real
/// hazard — two work dirs pointing at one SQLite file must still exclude each
/// other) and the work dir (shared temp objects/manifests). It CANNOT see a
/// second engine on another host pointed at the same Postgres — that
/// configuration is warned about at startup and remains unsupported until #93.
struct InstanceLock {
    _file: std::fs::File,
}

/// Strip a case-insensitive `sqlite:`/`sqlite://` scheme prefix.
fn strip_sqlite_scheme(s: &str) -> &str {
    for p in ["sqlite://", "sqlite:"] {
        if s.len() >= p.len() && s[..p.len()].eq_ignore_ascii_case(p) {
            return &s[p.len()..];
        }
    }
    s
}

/// The lock file representing a state store's identity on THIS host, or `None`
/// when no cross-process exclusion is needed (`:memory:` is per-process).
///
/// SQLite: a sibling lock next to the (parent-canonicalized) database file, so
/// two spellings of the same path collide. Postgres: a temp-dir lock keyed by
/// the connection string's digest — same-host protection only; the cross-host
/// gap is documented and tracked in #93.
fn state_store_lock_path(state_store: &str) -> Option<std::path::PathBuf> {
    let s = state_store.trim();
    let lower = s.to_ascii_lowercase();
    if lower.starts_with("postgres") {
        let digest = vtop_core::checksum::sha256_bytes(s.as_bytes());
        return Some(std::env::temp_dir().join(format!("vtop-pg-{}.lock", &digest[..16])));
    }
    let path = strip_sqlite_scheme(s);
    if path.trim_start_matches('/').starts_with(":memory:") || path == "memory:" {
        return None;
    }
    let pb = std::path::Path::new(path);
    let canon = pb
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .and_then(|d| d.canonicalize().ok())
        .map(|d| d.join(pb.file_name().unwrap_or_default()))
        .unwrap_or_else(|| pb.to_path_buf());
    Some(std::path::PathBuf::from(format!(
        "{}.vtop.lock",
        canon.display()
    )))
}

fn acquire_lock_file(path: &std::path::Path) -> Result<InstanceLock, VtopError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)?;
    match file.try_lock() {
        Ok(()) => Ok(InstanceLock { _file: file }),
        Err(std::fs::TryLockError::WouldBlock) => Err(VtopError::Config(format!(
            "another VTOP engine already holds {} — multi-instance operation is \
             UNSUPPORTED (single engine per state store; see #66, HA plan Phase 5/#93)",
            path.display()
        ))),
        Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
    }
}

/// Acquire every exclusion lock a MUTATING engine must hold (#66).
fn acquire_instance_locks(
    config: &VtopConfig,
    state_store: &ResolvedStateStore,
) -> Result<Vec<InstanceLock>, VtopError> {
    let mut locks = Vec::new();
    // The state store is the resource whose double-use corrupts progress.
    if let Some(store_lock) = state_store_lock_path(state_store.expose_secret()) {
        locks.push(acquire_lock_file(&store_lock)?);
    }
    // The work dir holds in-flight objects/manifests; sharing it is also a
    // misconfiguration even with distinct stores.
    let work_dir = std::path::Path::new(&config.engine.work_dir);
    std::fs::create_dir_all(work_dir)?;
    locks.push(acquire_lock_file(&work_dir.join(".vtop.instance.lock"))?);
    Ok(locks)
}

impl Engine {
    /// Build an engine for MUTATING use (`run`, `process-once`, `replay`):
    /// refuses to start while another engine holds the same state store or
    /// work dir on this host (#66). A shared NETWORK store (Postgres) on other
    /// hosts is invisible to a file lock: warned loudly until #93 adds real
    /// claim/lease semantics.
    pub async fn new_exclusive(
        config: VtopConfig,
        streams: StreamsConfig,
    ) -> Result<Self, VtopError> {
        config.validate()?;
        let state_store = config.engine.state_store.resolve()?;
        let locks = acquire_instance_locks(&config, &state_store)?;
        if state_store.is_postgres() {
            tracing::warn!(
                "state store is Postgres: the engine is SINGLE-INSTANCE per state \
                 store, and host-local locks cannot see engines on OTHER hosts. \
                 Do not run replicas against this database (see #66/#93)"
            );
        }
        let mut engine = Self::new_with_state_store(config, streams, state_store).await?;
        engine._instance_locks = locks;
        Ok(engine)
    }

    /// Build the engine from parsed config + streams, initializing the state
    /// store, the upload backend, and every enabled source adapter.
    ///
    /// Takes NO instance lock: read-only commands (`status`, `list-batches`,
    /// `discover`, `verify-manifest`) must work while the service runs — they
    /// cannot perform the recovery/commit races the lock exists to prevent.
    /// Anything that mutates goes through [`Engine::new_exclusive`].
    pub async fn new(config: VtopConfig, streams: StreamsConfig) -> Result<Self, VtopError> {
        config.validate()?;
        let state_store = config.engine.state_store.resolve()?;
        Self::new_with_state_store(config, streams, state_store).await
    }

    async fn new_with_state_store(
        config: VtopConfig,
        streams: StreamsConfig,
        state_store: ResolvedStateStore,
    ) -> Result<Self, VtopError> {
        let manifest_mac_key = config.resolve_manifest_mac_key()?;
        let store = connect_state_store(state_store.expose_secret()).await?;
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
            manifest_mac_key,
            pending: HashMap::new(),
            cycle_had_data: false,
            _instance_locks: Vec::new(),
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
            store: self.store.as_ref(),
            backend: self.backend.clone(),
            config: &self.config,
            manifest_mac_key: self.manifest_mac_key.clone(),
        }
    }

    /// Key used by read-only manifest verification, if authentication is
    /// required by this process's configuration.
    pub(crate) fn manifest_mac_key(&self) -> Option<&ManifestMacKey> {
        self.manifest_mac_key.as_ref()
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
        // Paid per source, serially — see `BatchingConfig::source_poll_wait_ms`.
        let max_wait = Duration::from_millis(self.config.batching.source_poll_wait_ms);

        // Bound the Kafka partition-metadata cache: drop entries for topics that
        // no longer exist, so a broker that churns through short-lived topics
        // does not grow the cache without limit. `sources` is the full live set
        // this cycle, which is exactly what pruning needs.
        if *source_type == SourceType::Kafka {
            if let Some(k) = adapter
                .as_any_mut()
                .downcast_mut::<vtop_adapters::KafkaSource>()
            {
                let live: Vec<String> = sources.iter().map(|s| s.source_name.clone()).collect();
                k.prune_partition_cache(&live);
            }
        }

        // ---- Read + accumulate -----------------------------------------
        //
        // Cycle-level accounting. Three optimisations (#94, #97) each targeted a
        // stage that turned out not to be the constraint, because the evidence
        // available was per-BATCH timing — which says nothing about how the
        // CYCLE divides between reading and waiting. These counters answer that
        // directly: how much wall-clock goes into reads that returned data
        // versus reads that returned nothing, and how many sources are in each
        // group. A cycle that is mostly `empty_read_ms` is starved by serial
        // polling, not by any downstream stage.
        let cycle_started = Instant::now();
        let mut productive_read_ms: u64 = 0;
        let mut empty_read_ms: u64 = 0;
        let mut failed_read_ms: u64 = 0;
        let mut productive_sources: usize = 0;
        let mut empty_sources: usize = 0;
        let mut failed_sources: usize = 0;
        let mut records_read: usize = 0;

        // ONE call reads every source: the default trait impl walks them
        // serially (file, syslog — independent handles), while Kafka overrides
        // it to assign all topic-partitions once and pay a single poll wait for
        // the whole topic set instead of one per topic (#96 A1). The adapter
        // reports how the pass's wall-clock divided between the buckets; the
        // per-source outcomes drive the counts and the buffer routing.
        let read_started = Instant::now();
        match adapter
            .read_all_batch_candidates(
                &sources,
                self.config.batching.max_records,
                self.config.batching.max_bytes,
                max_wait,
            )
            .await
        {
            Err(e) => {
                // The whole pass failed before any source progressed (e.g. the
                // shared consumer could not assign). One error event — the
                // metric counts error events, not sources multiplied by them.
                if let Some(mx) = telemetry::metrics() {
                    let t = self.config.engine.tenant.clone();
                    mx.source_read_errors_total
                        .with_label_values(&[t.as_str(), source_type.as_str()])
                        .inc();
                }
                failed_read_ms += read_started.elapsed().as_millis() as u64;
                failed_sources += sources.len();
                tracing::warn!(source_type = %source_type, error = %e, "adapter read pass failed; skipping this cycle");
            }
            Ok(report) => {
                productive_read_ms = report.productive_ms;
                empty_read_ms = report.empty_ms;
                failed_read_ms = report.failed_ms;
                for outcome in report.outcomes {
                    let Some(source) = sources.get(outcome.source_index) else {
                        debug_assert!(false, "adapter returned out-of-range source index");
                        continue;
                    };
                    // A single source failing must not abort the other sources'
                    // results — log and skip it.
                    let reads = match outcome.result {
                        Ok(r) => r,
                        Err(e) => {
                            if let Some(mx) = telemetry::metrics() {
                                // No source_name label: file/syslog source names
                                // are full paths, so a rotated file set would
                                // mint a series per file. The path is in the
                                // warning below instead. Resolve tenant the same
                                // way the pipeline does (stream override first,
                                // engine default second) so read errors line up
                                // with the batch metrics for the same source
                                // instead of always landing on the engine
                                // default.
                                let t = self
                                    .streams
                                    .lookup(&source.source_name)
                                    .map(|s| s.tenant.clone())
                                    .unwrap_or_else(|| self.config.engine.tenant.clone());
                                mx.source_read_errors_total
                                    .with_label_values(&[t.as_str(), source_type.as_str()])
                                    .inc();
                            }
                            failed_sources += 1;
                            tracing::warn!(source = %source.source_name, error = %e, "source read failed; skipping this cycle");
                            continue;
                        }
                    };
                    let got: usize = reads.iter().map(|r| r.records.len()).sum();
                    if got > 0 {
                        productive_sources += 1;
                        records_read += got;
                    } else {
                        empty_sources += 1;
                    }

                    // One source can return SEVERAL independently committable
                    // units: a Kafka topic yields one per partition it saw.
                    // Each gets routed to its own buffer.
                    for read in reads {
                        if read.is_empty() {
                            continue;
                        }
                        // Any data at all means the loop should come straight
                        // back rather than sleeping out the idle interval — a
                        // backlog must be drained at read speed, not at timer
                        // speed.
                        self.cycle_had_data = true;
                        let limits = self.batch_limits();
                        let tenant = self.config.engine.tenant.clone();
                        // Key the buffer by the MARKER (topic + partition for
                        // Kafka) so a multiplexed read pass routes each unit to
                        // the buffer of the topic it was actually read from,
                        // and a multi-partition topic never coalesces records
                        // from different partitions into one batch.
                        let key = buffer_key(source, &read.progress_start);
                        let buf_source = source_for_marker(source, &read.progress_start);
                        self.pending
                            .entry(key)
                            .or_insert_with(|| PendingBuffer::new(&tenant, buf_source, limits))
                            .append(read);
                    }
                }
            }
        }

        let read_phase_ms = cycle_started.elapsed().as_millis() as u64;
        // Logged once per cycle per source type. The ratio that matters is
        // empty_read_ms / read_phase_ms: how much of the cycle is spent waiting
        // on sources that had nothing, which is time no downstream fix recovers.
        if read_phase_ms > 0 {
            tracing::info!(
                source_type = %source_type,
                read_phase_ms,
                productive_read_ms,
                empty_read_ms,
                failed_read_ms,
                productive_sources,
                empty_sources,
                failed_sources,
                records_read,
                empty_wait_pct = format!(
                    "{:.1}",
                    (empty_read_ms as f64 / read_phase_ms as f64) * 100.0
                ),
                "read_cycle_profile"
            );
        }

        // Buffers accumulated but not yet sealed. A gauge that climbs without
        // bound means sealing has stalled.
        if let Some(mx) = telemetry::metrics() {
            mx.inflight_batches.set(self.pending.len() as i64);
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
        //
        // Two phases, because they have different constraints:
        //
        //   1. verify  - no adapter needed, dominated by blocking network I/O
        //                (upload + manifest + verify measured at ~64% of staged
        //                time). Run concurrently, bounded.
        //   2. commit  - needs `&mut` on the adapter, so it must be serial. It
        //                is ~1ms, so serializing it costs almost nothing.
        //
        // The invariant is untouched: each batch still reaches SOURCE_COMMITTED
        // only after IT reached VERIFIED. Concurrency is strictly across
        // batches.
        let mut work = Vec::new();
        for key in flush_keys {
            let Some(mut buf) = self.pending.remove(&key) else {
                continue;
            };
            let Some(read) = buf.drain() else { continue };
            let stream = self.streams.lookup(&buf.source.source_name).cloned();
            work.push((buf.source.clone(), read, stream));
        }

        let limit = self.config.batching.max_concurrent_batches.max(1);
        let pipeline = self.pipeline();
        // buffer_unordered keeps `limit` verifies in flight and yields each as
        // it finishes, so a slow upload never holds up the ones behind it.
        // `work` is MOVED into the futures rather than iterated by reference.
        // Cloning each ReadResult here would duplicate every payload before
        // buffer_unordered bounds anything, so with max_bytes at 100 MiB and a
        // concurrency of 8 the queue alone could hold ~800 MiB of avoidable
        // copies — the clone is not bounded by the concurrency limit, only the
        // execution is.
        let verified: Vec<Result<VerifyStep, VtopError>> =
            futures::stream::iter(work.into_iter().map(|(source, read, stream)| {
                let pipeline = &pipeline;
                async move {
                    pipeline
                        .process_until_verified(&source, read, stream.as_ref())
                        .await
                }
            }))
            .buffer_unordered(limit)
            .collect()
            .await;

        // Do NOT bail on the first error. Verify runs concurrently, so by the
        // time one batch fails, others may already be persisted as VERIFIED —
        // and their buffers have been drained from `pending`. Returning early
        // would drop those VerifiedBatch values without committing source
        // progress, leaving them verified-but-uncommitted. The next cycle would
        // re-read the same records, build the same batch_id, and fail the state
        // store INSERT as a duplicate, wedging that source until a restart ran
        // recovery.
        //
        // So: commit every batch that succeeded, then report the first error.
        // Nothing verified is abandoned, and the caller still sees the failure.
        let mut outcomes = Vec::new();
        let mut first_err: Option<VtopError> = None;
        for step in verified {
            match step {
                Ok(VerifyStep::Finished(outcome)) => outcomes.push(outcome),
                Ok(VerifyStep::Verified(v)) => {
                    let batch_id = v.batch_id.clone();
                    match self.pipeline().commit_verified(adapter, v).await {
                        Ok(outcome) => outcomes.push(outcome),
                        Err(e) => {
                            // Already VERIFIED and durable; recovery retries the
                            // commit. Keep going so siblings still commit.
                            tracing::error!(batch_id, error = %e, "commit failed after verify");
                            first_err.get_or_insert(e);
                        }
                    }
                }
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        Ok(outcomes)
    }

    /// Whether a batch the ledger calls VERIFIED still checks out against the
    /// stored bytes RIGHT NOW (#64, #69). Recovery authenticates the downloaded
    /// manifest, binds it to the ledger, then asks the backend to verify the
    /// stored object content using the manifest's algorithm. HEAD metadata and
    /// uploader-written sidecars are never sufficient for a strong result.
    async fn storage_still_verified(&self, rec: &BatchRecord) -> bool {
        let Some(uri) = rec.object_uri.as_deref() else {
            // A VERIFIED row without an object URI is corrupt bookkeeping;
            // never commit source progress on it.
            return false;
        };
        let Some(muri) = rec.manifest_uri.as_deref() else {
            // VERIFIED covers both the object and its manifest. A row that
            // cannot identify the manifest cannot safely advance its source.
            return false;
        };
        let bytes = match self
            .backend
            .get_object_bounded(muri, MAX_MANIFEST_BYTES)
            .await
        {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(muri, error = %e, "recovery re-check: manifest download failed");
                return false;
            }
        };
        let manifest: VtopManifest = match serde_json::from_slice(&bytes) {
            Ok(manifest) => manifest,
            Err(e) => {
                tracing::warn!(muri, error = %e, "recovery re-check: manifest parse failed");
                return false;
            }
        };

        let ledger_manifest_hash_matches = rec.manifest_sha256.as_deref().is_some_and(|want| {
            !want.is_empty() && want.eq_ignore_ascii_case(&manifest.manifest.sha256)
        });
        if manifest
            .verify_authentication(self.manifest_mac_key.as_ref())
            .is_err()
            || manifest.batch_id != rec.batch_id
            || manifest.manifest.uri != muri
            || manifest.object.uri != uri
            || !ledger_manifest_hash_matches
        {
            tracing::warn!(
                muri,
                batch_id = %rec.batch_id,
                "recovery re-check: manifest authentication or ledger binding failed"
            );
            return false;
        }

        // Bind the manifest back to the durable ledger before trusting it as
        // recovery input. The column retains its historical object_sha256 name
        // but may contain a BLAKE3 digest.
        if rec
            .object_size_bytes
            .is_some_and(|want| want < 0 || want as u64 != manifest.object.size_bytes)
            || rec.object_sha256.as_deref().is_some_and(|want| {
                !want.is_empty() && !want.eq_ignore_ascii_case(&manifest.object.checksum)
            })
        {
            tracing::warn!(
                batch_id = %rec.batch_id,
                "recovery re-check: manifest and ledger object metadata disagree"
            );
            return false;
        }

        let algo = match manifest
            .object
            .checksum_algorithm
            .parse::<ChecksumAlgorithm>()
        {
            Ok(algo) => algo,
            Err(e) => {
                tracing::warn!(batch_id = %rec.batch_id, error = %e, "recovery re-check: unsupported checksum");
                return false;
            }
        };
        let expected = if algo.is_enabled() {
            if manifest.object.checksum.is_empty() {
                return false;
            }
            Some(ObjectChecksum::new(
                algo.as_str(),
                &manifest.object.checksum,
            ))
        } else {
            None
        };
        let result = match self
            .backend
            .verify_object(uri, manifest.object.size_bytes, expected)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                tracing::warn!(uri, error = %e, "recovery re-check: content verification failed");
                return false;
            }
        };
        if !result.passed {
            tracing::warn!(uri, reason = %result.message, "recovery re-check: stored content mismatch");
            return false;
        }
        !result.backend_limited || !self.config.upload.require_strong_verification
    }

    /// Recovery scan run at startup. Seeds adapters from committed batches and
    /// resolves incomplete batches without ever advancing source progress for
    /// unverified data.
    pub async fn recover(&mut self) -> Result<RecoverySummary, VtopError> {
        // Seed file/syslog committed offsets from previously committed batches.
        self.seed_committed_offsets().await?;

        // CLAIM, not list (#93): only batches this engine owns, never owned, or
        // whose previous owner's lease has expired. Recovery on a shared store
        // must not touch another live engine's in-flight work.
        let now = Utc::now();
        let incomplete = self
            .store
            .claim_incomplete_batches(
                &self.config.engine.name,
                &now.to_rfc3339(),
                &(now + chrono::Duration::seconds(LEASE_TTL_SECS)).to_rfc3339(),
            )
            .await?;
        let mut summary = RecoverySummary::default();
        for rec in incomplete {
            let action = next_recovery_action(rec.state);
            tracing::info!(batch_id = %rec.batch_id, state = %rec.state, ?action, "recovery_scan");
            match action {
                RecoveryAction::RetrySourceCommit => {
                    // The ledger says VERIFIED, but that was true of STORAGE at
                    // verification time — an object deleted or replaced while
                    // the engine was down would still get its source progress
                    // committed, advancing past data that no longer exists as
                    // verified (#69). Re-check the store before committing;
                    // anything less trusts a stale claim about a system this
                    // process has never observed.
                    if !self.storage_still_verified(&rec).await {
                        tracing::warn!(
                            batch_id = %rec.batch_id,
                            object_uri = rec.object_uri.as_deref().unwrap_or(""),
                            "recovery: VERIFIED batch failed storage re-check; replaying instead of committing"
                        );
                        // The replay decision must be DURABLE before the
                        // adapter rewinds: if these transitions fail, the row
                        // stays VERIFIED and the next recovery re-checks it
                        // again — never rewind on a decision the ledger did
                        // not record (#123 review).
                        if let Err(e) = self
                            .store
                            .mark_failed(
                                &rec.batch_id,
                                "recovery: stale VERIFIED (storage re-check failed)",
                            )
                            .await
                        {
                            tracing::error!(batch_id = %rec.batch_id, error = %e, "recovery: could not persist re-check failure");
                            summary.still_pending += 1;
                            continue;
                        }
                        if let Err(e) = self
                            .store
                            .update_batch_state(
                                &rec.batch_id,
                                BatchState::ReplayRequired,
                                &BatchPatch::default(),
                            )
                            .await
                        {
                            tracing::error!(batch_id = %rec.batch_id, error = %e, "recovery: could not persist REPLAY_REQUIRED");
                            summary.still_pending += 1;
                            continue;
                        }
                        if let Some(adapter) = self.adapters.get_mut(&rec.source_type) {
                            let _ = adapter.replay_from_marker(&rec.progress_start).await;
                        }
                        // Dashboards must see this refusal path like any other
                        // replay (#123 review).
                        if let Some(mx) = telemetry::metrics() {
                            let l = [
                                rec.tenant.as_str(),
                                rec.source_type.as_str(),
                                rec.format.extension(),
                            ];
                            mx.replay_required_total.with_label_values(&l).inc();
                            mx.batches_total
                                .with_label_values(&[
                                    l[0],
                                    l[1],
                                    l[2],
                                    BatchState::ReplayRequired.as_str(),
                                ])
                                .inc();
                        }
                        summary.replay_required += 1;
                        continue;
                    }
                    // Verified — and just re-verified against storage — commit.
                    if let Some(adapter) = self.adapters.get_mut(&rec.source_type) {
                        match adapter.commit_progress(&rec.progress_end).await {
                            Ok(()) => {
                                self.store.mark_source_committed(&rec.batch_id).await?;
                                // A restart-recovery commit is still a commit:
                                // without it, commits_total under-counts on
                                // exactly the path where the invariant is most
                                // delicate (VERIFIED but not yet committed).
                                //
                                // The verification is counted HERE TOO, even
                                // though it happened in the process that
                                // crashed. Prometheus counters are per-process
                                // and reset on restart, so counting only the
                                // commit would leave this process reporting
                                // commits_total > verified_total - which reads
                                // as "SOURCE_COMMITTED without VERIFIED", the
                                // one alarm that must never cry wolf. The state
                                // store is the authority that the batch really
                                // did reach VERIFIED, so recording both keeps
                                // the pair self-consistent and honest.
                                if let Some(mx) = telemetry::metrics() {
                                    let l = [
                                        rec.tenant.as_str(),
                                        rec.source_type.as_str(),
                                        rec.format.extension(),
                                    ];
                                    mx.verified_total.with_label_values(&l).inc();
                                    mx.batches_total
                                        .with_label_values(&[
                                            l[0],
                                            l[1],
                                            l[2],
                                            BatchState::Verified.as_str(),
                                        ])
                                        .inc();
                                    mx.commits_total.with_label_values(&l).inc();
                                    mx.batches_total
                                        .with_label_values(&[
                                            l[0],
                                            l[1],
                                            l[2],
                                            BatchState::SourceCommitted.as_str(),
                                        ])
                                        .inc();
                                }
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
                    if let Some(mx) = telemetry::metrics() {
                        // Replay is safe by design, but a sustained rate means
                        // work is being repeated - worth seeing on a dashboard.
                        mx.replay_required_total
                            .with_label_values(&[
                                rec.tenant.as_str(),
                                rec.source_type.as_str(),
                                rec.format.extension(),
                            ])
                            .inc();
                        mx.batches_total
                            .with_label_values(&[
                                rec.tenant.as_str(),
                                rec.source_type.as_str(),
                                rec.format.extension(),
                                BatchState::ReplayRequired.as_str(),
                            ])
                            .inc();
                    }
                    summary.replay_required += 1;
                    tracing::warn!(batch_id = %rec.batch_id, "recovered: REPLAY_REQUIRED (source progress preserved)");
                }
            }
        }
        Ok(summary)
    }

    async fn seed_committed_offsets(&mut self) -> Result<(), VtopError> {
        // The per-path MAX(end_byte) is computed IN THE STORE. This runs at
        // every startup and the ledger grows without bound, so materialising
        // every row here made recovery memory scale with history (#77).
        // (Kafka needs no seeding: it resumes from the broker-side committed
        // offset.)
        let file_max = self.store.max_committed_end_bytes(SourceType::File).await?;
        let spool_max = self
            .store
            .max_committed_end_bytes(SourceType::SyslogSpool)
            .await?;
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
        let idle = Duration::from_millis(self.config.batching.idle_poll_interval_ms);
        loop {
            // Accumulate across cycles; only threshold-tripped buffers flush.
            self.cycle_had_data = false;
            for st in &types {
                if let Err(e) = self.run_source(st.clone(), false).await {
                    tracing::error!(error = %e, source_type = %st, "process cycle error");
                }
            }
            // Productive cycle: loop again immediately. Idle cycle: back off.
            // `Duration::ZERO` still yields to the runtime through `select!`,
            // so Ctrl-C stays responsive and this never starves the executor.
            let backoff = if self.cycle_had_data {
                Duration::ZERO
            } else {
                idle
            };
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
                _ = tokio::time::sleep(backoff) => {}
            }
        }
    }
}

/// Buffer key that isolates accumulation per source AND per Kafka partition, so
/// a multi-partition topic never coalesces records from different partitions
/// into one batch. File/syslog sources have no partition and key by name only.
///
/// For Kafka the key is derived from the MARKER's topic, not the requested
/// source: a multiplexed read pass (#96 A1) reads every topic in one call, so
/// the marker is the authority on which topic a unit actually came from. Keying
/// by the requested source would file another topic's records under the wrong
/// buffer — and its commit marker would then commit offsets for a topic the
/// buffer's records did not come from.
fn buffer_key(source: &DiscoveredSource, marker: &ProgressMarker) -> (SourceType, String) {
    match marker {
        ProgressMarker::Kafka {
            topic, partition, ..
        } => (source.source_type.clone(), format!("{topic}#p{partition}")),
        _ => (source.source_type.clone(), source.source_name.clone()),
    }
}

/// The source a read actually belongs to. Like [`buffer_key`], the marker is
/// authoritative for Kafka: the buffer's source drives stream lookup (tenant,
/// format) and batch metadata, so it must name the topic the records came
/// from, not the source the read was requested for.
fn source_for_marker(requested: &DiscoveredSource, marker: &ProgressMarker) -> DiscoveredSource {
    match marker {
        ProgressMarker::Kafka { topic, .. } if *topic != requested.source_name => {
            DiscoveredSource {
                source_type: requested.source_type.clone(),
                source_name: topic.clone(),
                format: requested.format.clone(),
            }
        }
        _ => requested.clone(),
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

    /// #66: a mutating engine is single-instance per state store AND work dir;
    /// a second one must be refused however the collision arises, read-only
    /// construction must keep working alongside, and the locks must free when
    /// the holder goes away.
    #[tokio::test]
    async fn second_exclusive_engine_is_refused_but_read_only_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.log");
        std::fs::write(&input, "one\n").unwrap();
        let db = dir.path().join("state.db");
        let cfg = |work: &str, store: &str| {
            crate::testkit::file_config(
                dir.path().join(work).to_str().unwrap(),
                store,
                vec![input.to_string_lossy().into_owned()],
                "mock",
            )
        };
        let store_uri = format!("sqlite://{}", db.display());

        let first =
            Engine::new_exclusive(cfg("work-a", &store_uri), StreamsConfig { streams: vec![] })
                .await
                .expect("first exclusive engine starts");

        // Same work dir → refused.
        let err = Engine::new_exclusive(
            cfg("work-a", "sqlite::memory:"),
            StreamsConfig { streams: vec![] },
        )
        .await
        .err()
        .expect("same work dir must be refused");
        assert!(matches!(err, VtopError::Config(_)), "{err:?}");
        assert!(err.to_string().contains("another VTOP engine"));

        // DIFFERENT work dir but the SAME state store → still refused. This is
        // the dangerous variant (both would recover and commit the same
        // batches); a work-dir-only lock waves it through.
        let err =
            Engine::new_exclusive(cfg("work-b", &store_uri), StreamsConfig { streams: vec![] })
                .await
                .err()
                .expect("same state store must be refused even from another work dir");
        assert!(err.to_string().contains("another VTOP engine"));

        // Read-only construction takes no lock: operators must be able to run
        // status/list-batches/verify-manifest beside the running service.
        let read_only = Engine::new(cfg("work-a", &store_uri), StreamsConfig { streams: vec![] })
            .await
            .expect("read-only engine must start alongside the exclusive one");
        drop(read_only);

        // Locks release with the engine: a successor can start.
        drop(first);
        Engine::new_exclusive(cfg("work-a", &store_uri), StreamsConfig { streams: vec![] })
            .await
            .expect("engine restarts after the previous one is gone");
    }

    /// The store lock is keyed by the store's identity, not its spelling:
    /// `sqlite:` and `sqlite://` forms of one path must collide, and
    /// `:memory:` stores (per-process by nature) must not lock at all.
    #[test]
    fn state_store_lock_path_normalizes_identity() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("s.db");
        let a = state_store_lock_path(&format!("sqlite://{}", db.display()));
        let b = state_store_lock_path(&format!("sqlite:{}", db.display()));
        assert_eq!(a, b, "scheme spelling must not change the lock identity");
        assert!(a.is_some());
        assert!(state_store_lock_path("sqlite::memory:").is_none());
        // Postgres: same conn string → same lock; different → different.
        let p1 = state_store_lock_path("postgres://u@h/db");
        let p2 = state_store_lock_path("postgres://u@h/db");
        let p3 = state_store_lock_path("postgres://u@h/other");
        assert_eq!(p1, p2);
        assert_ne!(p1, p3);
    }

    #[test]
    fn buffer_key_isolates_kafka_partitions() {
        use vtop_core::types::{ProgressMarker, TelemetryFormat};
        let topic = DiscoveredSource {
            source_type: SourceType::Kafka,
            source_name: "events".into(),
            format: TelemetryFormat::Cef,
        };
        let mk = |p: i32| ProgressMarker::Kafka {
            topic: "events".into(),
            partition: p,
            start_offset: 0,
            end_offset: 0,
            consumer_group: "g".into(),
        };
        // Same topic, different partitions -> distinct buffer keys (no mixing).
        assert_ne!(buffer_key(&topic, &mk(0)), buffer_key(&topic, &mk(1)));
        // Same partition -> same key (accumulates together).
        assert_eq!(buffer_key(&topic, &mk(0)), buffer_key(&topic, &mk(0)));
        // File sources key by name only (no partition suffix).
        let file = DiscoveredSource {
            source_type: SourceType::File,
            source_name: "/a.log".into(),
            format: TelemetryFormat::Raw,
        };
        let fm = ProgressMarker::File {
            path: "/a.log".into(),
            inode: None,
            start_byte: 0,
            end_byte: 0,
            file_size: 0,
            mtime: String::new(),
        };
        assert_eq!(buffer_key(&file, &fm).1, "/a.log");
    }

    /// #96 A2: a multiplexed Kafka read pass can return units for a topic other
    /// than the source the read was requested for. The MARKER must drive both
    /// the buffer key and the buffer's source — otherwise topic B's records
    /// land in topic A's buffer and B's offsets get committed against A's data.
    #[test]
    fn buffer_routing_follows_the_marker_topic_not_the_requested_source() {
        use vtop_core::types::{ProgressMarker, TelemetryFormat};
        let requested = DiscoveredSource {
            source_type: SourceType::Kafka,
            source_name: "topic_a".into(),
            format: TelemetryFormat::Cef,
        };
        let marker_for_b = ProgressMarker::Kafka {
            topic: "topic_b".into(),
            partition: 3,
            start_offset: 0,
            end_offset: 0,
            consumer_group: "g".into(),
        };
        // Key names the marker's topic and partition, not the requested source.
        assert_eq!(buffer_key(&requested, &marker_for_b).1, "topic_b#p3");
        // The buffer's source follows the marker too (drives stream lookup and
        // batch metadata), keeping everything else from the requested source.
        let routed = source_for_marker(&requested, &marker_for_b);
        assert_eq!(routed.source_name, "topic_b");
        assert_eq!(routed.source_type, SourceType::Kafka);
        assert_eq!(routed.format, TelemetryFormat::Cef);
        // Marker topic == requested source: unchanged.
        let marker_for_a = ProgressMarker::Kafka {
            topic: "topic_a".into(),
            partition: 0,
            start_offset: 0,
            end_offset: 0,
            consumer_group: "g".into(),
        };
        assert_eq!(
            source_for_marker(&requested, &marker_for_a).source_name,
            "topic_a"
        );
    }

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

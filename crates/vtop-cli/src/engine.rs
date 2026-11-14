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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use futures::StreamExt;
use vtop_adapters::base::{DiscoveredSource, ReadResult, SourceAdapter};
use vtop_adapters::file_source::ArchivedEnd;
#[cfg(feature = "kafka")]
use vtop_adapters::KafkaSource;
use vtop_adapters::{FileSource, SyslogSpoolSource};
use vtop_core::batch::{AdaptiveBatcher, BatchLimits, SealReason, TelemetryBatch};
use vtop_core::compression::compress_batch;
use vtop_core::config::{ResolvedStateStore, StreamConfig, StreamsConfig, VtopConfig};
use vtop_core::errors::VtopError;
use vtop_core::manifest::{
    ManifestBuilder, ManifestFormat, ManifestMacKey, ManifestWriteVersion, ParsedManifest,
    VerificationPlan, MAX_MANIFEST_BYTES,
};
use vtop_core::metrics::BatchMetrics;
use vtop_core::partitioning::{self, PartitionContext};
use vtop_core::replay::{next_recovery_action, RecoveryAction};
use vtop_core::state_machine::BatchState;
use vtop_core::telemetry;
use vtop_core::types::{ContentFingerprint, ProgressMarker, SourceType};
use vtop_core::work_dir::cleanup_work_dir;
use vtop_state::{connect_state_store_with_deadline, BatchPatch, BatchRecord, StateStore};
use vtop_upload::multipart::{self, MultipartFence, MultipartUploadConfig};
use vtop_upload::{ObjectChecksum, UploadBackend};

use crate::verification_mode::{self, Recheck};

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

/// Deletes batch-local scratch files on every normal return path, including
/// failures after compression. A process crash cannot run `Drop`; startup's
/// bounded work-dir sweep handles those leftovers.
struct StagedFiles {
    paths: Vec<std::path::PathBuf>,
}

impl StagedFiles {
    fn new(path: std::path::PathBuf) -> Self {
        Self { paths: vec![path] }
    }

    fn track(&mut self, path: std::path::PathBuf) {
        self.paths.push(path);
    }
}

impl Drop for StagedFiles {
    fn drop(&mut self) {
        for path in &self.paths {
            if let Err(error) = std::fs::remove_file(path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = %path.display(), %error, "failed to remove staging artifact");
                }
            }
        }
    }
}

/// Result of the verify phase: either the batch is ready to commit, or it
/// already reached a terminal outcome (empty read, or FAILED) and there is
/// nothing left to do.
#[expect(
    clippy::large_enum_variant,
    reason = "one step per batch, consumed at once; the gap is the progress marker, which \
              grew with the content fingerprint (#578), and boxing either side only moves it"
)]
pub enum VerifyStep {
    Verified(VerifiedBatch),
    Finished(BatchOutcome),
}

/// Per-bucket provisioning cells; see [`Pipeline::provisioned_buckets`].
/// The outer sync mutex is held only to fetch a bucket's cell, never across
/// the network call the cell serializes.
pub type ProvisionedBuckets =
    Arc<std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::OnceCell<()>>>>>;

/// Borrowed context shared by every pipeline step.
pub struct Pipeline<'a> {
    pub store: &'a dyn StateStore,
    pub backend: Arc<dyn UploadBackend>,
    pub config: &'a VtopConfig,
    /// Runtime-only secret, resolved once at startup and never serialized.
    pub manifest_mac_key: Option<ManifestMacKey>,
    /// Buckets whose versioning status passed the hardened-profile preflight
    /// (#135). Owned by [`Engine`] and shared into every pipeline so the
    /// check runs once per bucket per process, not per batch.
    pub versioned_buckets: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    /// Throttles the store answered with, counted across the process (#102):
    /// the engine reads it before and after a cycle to learn whether the
    /// cycle was throttled, which is what the width controller consumes.
    pub throttles: Arc<AtomicU64>,
    /// Uploads ISSUED, whatever became of them (#102): the width controller
    /// judges only cycles that reached the store.
    pub attempts: Arc<AtomicU64>,
    /// Per-bucket provisioning cells for `ensure_bucket` when
    /// `upload.create_bucket` is set. Owned by [`Engine`] and shared into
    /// every pipeline, like `versioned_buckets`, so provisioning runs once
    /// per bucket per process, not per batch (#102). Per bucket rather than
    /// at startup because the bucket name is templated per batch. Keyed
    /// cells rather than one guard because both failure modes are real:
    /// concurrent first batches for ONE bucket must not issue width-many
    /// CreateBucket calls, and first batches for DIFFERENT buckets must not
    /// wait on each other's network round trip.
    pub provisioned_buckets: ProvisionedBuckets,
    /// Rate limit for object-key notices that repeat on every batch while a
    /// setting stays as it is (#626). Owned by [`Engine`] and shared like
    /// `versioned_buckets`.
    pub key_notices: Arc<KeyNotices>,
}

/// Rate limit for the object-key notices a setting repeats on every batch
/// (#626): no checksum, a backend that cannot report an absent key, a probe
/// the store refuses. The first is logged at once; later ones at most once a
/// minute per kind, with how many were suppressed in between. Each decision
/// is still counted in `vtop_object_key_decisions_total`.
type NoticeKey = (&'static str, String);

#[derive(Debug)]
pub struct KeyNotices {
    every: Duration,
    /// (kind, source) -> (when one was last logged, suppressed since).
    last: std::sync::Mutex<HashMap<NoticeKey, (Instant, u64)>>,
}

impl Default for KeyNotices {
    fn default() -> Self {
        Self::every(Duration::from_secs(60))
    }
}

impl KeyNotices {
    fn every(every: Duration) -> Self {
        Self {
            every,
            last: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// `Some(suppressed since the last one logged)` when a notice of `kind`
    /// about `source` should be logged now, `None` when it is suppressed. Per
    /// source, so one stream's notices never hide another's; sources are
    /// bounded by configuration and discovery.
    fn admit(&self, kind: &'static str, source: &str) -> Option<u64> {
        self.admit_at(kind, source, Instant::now())
    }

    fn admit_at(&self, kind: &'static str, source: &str, now: Instant) -> Option<u64> {
        let mut last = self.last.lock().unwrap();
        match last.get_mut(&(kind, source.to_owned())) {
            None => {
                last.insert((kind, source.to_owned()), (now, 0));
                Some(0)
            }
            Some((at, suppressed)) if now.duration_since(*at) >= self.every => {
                let n = *suppressed;
                *at = now;
                *suppressed = 0;
                Some(n)
            }
            Some((_, suppressed)) => {
                *suppressed += 1;
                None
            }
        }
    }
}

impl<'a> Pipeline<'a> {
    /// What becomes of a failed multipart upload's session (#668).
    ///
    /// Kept, when a replay can resume it: the object is under its derived
    /// key, which a replay of the same bytes names again (#626), and the
    /// session on disk is one this upload can continue. Its completed parts
    /// stay at the store and the replay sends only what is missing. A replay
    /// whose bytes differ names another key and leaves this session to the
    /// abandoned-session sweep, and a bucket lifecycle rule is the backstop
    /// for an engine that never comes back.
    ///
    /// Aborted otherwise, as every failed upload was before #668: under a
    /// per-attempt key nothing will ever name the session again, and a
    /// session the upload refused to resume cannot be continued.
    #[expect(
        clippy::too_many_arguments,
        reason = "each argument is a fact the decision or its notice needs; a struct would \
                  only name them twice"
    )]
    async fn settle_failed_multipart(
        &self,
        batch_id: &str,
        object_uri: &str,
        key_is_derived: bool,
        fence: &MultipartFence,
        report: &multipart::MultipartReport,
        error: &VtopError,
        work_dir: &std::path::Path,
    ) {
        let cause = multipart::redact_urls(&error.to_string());
        let abandon = self.config.upload.multipart_abandon_after_secs;
        if report.session_left && key_is_derived && !fence.content_digest_hex.is_empty() {
            if let Some(mx) = telemetry::metrics() {
                mx.multipart_sessions_total
                    .with_label_values(&["kept_after_failure"])
                    .inc();
            }
            tracing::warn!(
                event = "multipart_session_kept",
                batch_id,
                key = object_uri,
                parts_kept = report.parts_kept,
                parts_total = report.parts_total,
                error = %cause,
                "{cause}; the batch was marked failed and will be replayed, and the replay \
                 resumes the session if its bytes are unchanged; incomplete uploads are aborted \
                 after upload.multipart_abandon_after_secs ({abandon} s); nothing to do unless \
                 this repeats, then check the link to the store"
            );
            return;
        }
        let session = work_dir.join(multipart::session_file_name(object_uri, fence));
        if !session.exists() {
            // Nothing was left to keep or abort: the upload failed before a
            // session was written, or the store had already lost it.
            return;
        }
        let why = if key_is_derived {
            "the session on disk cannot be resumed by this upload"
        } else {
            "it was written under the batch's per-attempt key, which no replay names again"
        };
        match multipart::abort_session(self.backend.as_ref(), work_dir, object_uri, fence).await {
            Ok(()) => tracing::warn!(
                event = "multipart_session_aborted",
                batch_id,
                key = object_uri,
                reason = why,
                error = %cause,
                "{cause}; the multipart upload of {object_uri} was aborted rather than kept: \
                 {why}; the replay uploads the object from its first byte; nothing to do"
            ),
            Err(abort_err) => tracing::warn!(
                event = "multipart_session_aborted",
                batch_id,
                key = object_uri,
                reason = why,
                error = %multipart::redact_urls(&abort_err.to_string()),
                "could not abort the failed multipart upload of {object_uri} ({}); the \
                 abandoned-session sweep aborts it after upload.multipart_abandon_after_secs \
                 ({abandon} s); nothing to do unless this repeats",
                multipart::redact_urls(&abort_err.to_string())
            ),
        }
    }

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
        match self
            .process_until_verified(source, read, stream, Utc::now())
            .await?
        {
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
    ///
    /// `sealed_at` is when the batch's contents were fixed: the caller takes
    /// it as the read leaves its buffer, before the batch queues for an upload
    /// slot. The time partition is taken from it (#559).
    async fn process_until_verified(
        &self,
        source: &DiscoveredSource,
        read: ReadResult,
        stream: Option<&StreamConfig>,
        sealed_at: chrono::DateTime<Utc>,
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

        // The range the batch's object holds, for its name and its manifest.
        // The ledger and the adapter commit keep `read.progress_end` as it is.
        let window = window_marker(&read);
        let batch_id = vtop_core::batch::build_batch_id(&source.source_name, &window);
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
            manifest_version_id: None,
            object_size_bytes: None,
            verification_class: None,
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
        //
        // The ledger records the decision the way recovery does, FAILED then
        // REPLAY_REQUIRED, but the decision does not wait on it (#627): the
        // caller hands the range back to the source whether or not these
        // writes land. A FAILED write that fails surfaces as `Err`, which the
        // cycle rewinds too; a REPLAY_REQUIRED write that fails is logged and
        // leaves the row FAILED, which the next recovery replays all the same.
        // See the rewind in `run_cycle` for why withholding it would lose data.
        macro_rules! fail {
            ($msg:expr) => {{
                let m: String = $msg;
                self.store.mark_failed(&batch_id, &m).await?;
                let replay_recorded = match self
                    .store
                    .update_batch_state(&batch_id, BatchState::ReplayRequired, &BatchPatch::default())
                    .await
                {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::warn!(
                            batch_id,
                            error = %e,
                            "could not record REPLAY_REQUIRED for a failed batch; the row stays \
                             FAILED and its range is handed back to the source regardless"
                        );
                        false
                    }
                };
                // Failure accounting. Deliberately label-free of batch_id: the
                // identifier is unbounded and belongs in the log line below,
                // not in a metric label.
                if let Some(mx) = telemetry::metrics() {
                    let l = [tenant.as_str(), source.source_type.as_str(), format.extension()];
                    mx.failed_total.with_label_values(&l).inc();
                    mx.batches_total
                        .with_label_values(&[l[0], l[1], l[2], BatchState::Failed.as_str()])
                        .inc();
                    if replay_recorded {
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
        // Stamped with the drain instant, not the time this line runs: the
        // batch may have waited for an upload slot and its first ledger write
        // since, possibly across an hour boundary, and the partition below and
        // the recorded seal time must name the same hour (review on #570).
        batch.seal_at(sealed_at)?;
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
        let mut staged_files = StagedFiles::new(compressed.path.clone());
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
        // Algorithm is configurable: sha256, blake3, or disabled (None), per
        // stream with the global value as the default (#669). This is the
        // only place a new batch's algorithm is chosen; recovery and scrub
        // re-verify with the algorithm recorded in each batch's manifest.
        let algo = vtop_core::config::effective_algorithm(self.config, stream);
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
        // The time partition comes from the seal time (or, on request, the
        // earliest record time), never from the wall clock at this point: by
        // now the batch has been compressed and checksummed, and a batch
        // sealed at 12:59 used to be filed under hour 13 (#559).
        let partition_at = partitioning::partition_time(
            self.config.partitioning.time_source,
            sealed_at,
            batch.first_timestamp.as_deref(),
        );
        let ctx = PartitionContext::new(&tenant, &s3_source, format.clone(), partition_at);
        let ctx = match &stream.and_then(|s| s.retention_class.clone()) {
            Some(rc) => ctx.with("retention_class", rc.clone()),
            None => ctx,
        };
        let resolved_prefix =
            partitioning::resolve_template(&self.config.partitioning.template, &ctx);
        // Bucket may be templated (e.g. "telemetry-{format}") for one bucket
        // per data format.
        let bucket = partitioning::resolve_bucket(&self.config.upload.bucket, &ctx);
        // Two candidate object keys (#626). The DERIVED key is named by the
        // source, the range and the digest of the bytes, so a replay that
        // produces the same bytes finds the object an earlier attempt left;
        // it needs a digest. The ATTEMPT key is named by this attempt's
        // batch id, unique by construction: the fallback when there is no
        // digest, when the backend cannot tell an absent key from a refused
        // request, or when the derived key holds bytes that do not verify.
        let attempt_object_uri = partitioning::object_uri(
            &bucket,
            &self.config.upload.prefix,
            &resolved_prefix,
            &batch_id,
            format.clone(),
            compressed.compression,
        );
        let derived_object_uri: Result<String, String> = match object_checksum.as_deref() {
            None => Err(
                "checksums are disabled (checksum.algorithm: none), so the object cannot be \
                 named by its content; enable a checksum so a replay of this range reuses it"
                    .to_string(),
            ),
            Some(_) if !self.backend.reports_absent_objects() => Err(format!(
                "backend {} cannot tell a missing key from a refused request, so a derived key \
                 could be written over; use the s3_native or localfs backend so a replay of \
                 this range reuses it",
                self.backend.backend_name()
            )),
            Some(digest) => Ok(partitioning::object_uri(
                &bucket,
                &self.config.upload.prefix,
                &resolved_prefix,
                &vtop_core::batch::build_object_name(
                    &self.config.engine.name,
                    &source.source_name,
                    &window,
                    digest,
                ),
                format.clone(),
                compressed.compression,
            )),
        };
        let manifest_uri = partitioning::manifest_uri(
            &bucket,
            &self.config.upload.prefix,
            &resolved_prefix,
            &batch_id,
        );

        // Optionally provision the (per-format) bucket on demand. Cached like
        // the versioning preflight below: uncached, every batch of a soak paid
        // a live CreateBucket round trip against the very store whose
        // absorption capacity was being measured, visible only inside
        // `total_ms` (#102). The in-flight state is keyed per bucket: batches
        // racing for the SAME bucket serialize on its cell (a check released
        // around the await let every concurrent first batch observe the
        // bucket absent and provision it again), while distinct buckets
        // provision concurrently — the registry lock is held only to fetch
        // the cell, never across the network call. A failed init leaves the
        // cell empty, so a transient provisioning failure is retried on the
        // next batch instead of latched as done.
        if self.config.upload.create_bucket {
            let cell = {
                let mut registry = self.provisioned_buckets.lock().unwrap();
                std::sync::Arc::clone(registry.entry(bucket.clone()).or_default())
            };
            if let Err(e) = cell
                .get_or_try_init(|| async {
                    note_store_request(
                        self.backend.ensure_bucket(&bucket).await,
                        "ensure_bucket",
                        [
                            tenant.as_str(),
                            source.source_type.as_str(),
                            format.extension(),
                        ],
                        &self.attempts,
                        &self.throttles,
                    )
                })
                .await
            {
                fail!(format!("ensure_bucket {bucket} failed: {e}"));
            }
        }

        // Hardened manifest profile (#135): refuse to upload into a bucket
        // that discards overwritten versions — version pinning below is only
        // meaningful if the store keeps what we pin.
        if self.config.upload.require_object_versioning {
            if !self.backend.supports_object_versions() {
                fail!(format!(
                    "require_object_versioning is set but backend {} does not expose \
                     immutable object versions",
                    self.backend.backend_name()
                ));
            }
            let checked = self.versioned_buckets.lock().unwrap().contains(&bucket);
            if !checked {
                if let Err(e) = note_store_request(
                    self.backend.verify_bucket_versioning(&bucket).await,
                    "bucket_versioning",
                    [
                        tenant.as_str(),
                        source.source_type.as_str(),
                        format.extension(),
                    ],
                    &self.attempts,
                    &self.throttles,
                ) {
                    fail!(format!("bucket versioning preflight failed: {e}"));
                }
                self.versioned_buckets
                    .lock()
                    .unwrap()
                    .insert(bucket.clone());
            }
        }

        // ---- CHECKSUMMED -> OBJECT_UPLOADED -----------------------------
        // Engine-computed object checksum (algorithm + hex), if enabled.
        let object_ck = object_checksum
            .as_deref()
            .map(|h| ObjectChecksum::new(algo.as_str(), h));

        // Which key the bytes go under, and whether they are already there
        // (#626). An object already at the derived key is reused only when it
        // verifies STRONGLY against this batch's digest: then it holds these
        // bytes and putting them again would only add a version. Anything
        // else at the key is never overwritten - it may be a damaged object
        // a scrub has yet to report - and the batch goes under its attempt
        // key instead. The reused object is verified again below like any
        // other, so VERIFIED still rests on a read of the stored bytes.
        let labels = [
            tenant.as_str(),
            source.source_type.as_str(),
            format.extension(),
        ];
        // Every outcome is logged for the operator with what happened, what
        // the engine did and what to do, with the batch id, keys and range as
        // fields, and counted by decision (#626). The notices a setting
        // repeats on every batch are rate-limited; the counter is not.
        let range = window.range_token();
        let source_name = source.source_name.as_str();
        let decided = |decision: &str| {
            debug_assert!(
                telemetry::OBJECT_KEY_DECISIONS.contains(&decision),
                "{decision} is not a pinned decision value"
            );
            if let Some(mx) = telemetry::metrics() {
                mx.object_key_decisions_total
                    .with_label_values(&[labels[0], labels[1], labels[2], decision])
                    .inc();
            }
        };
        let (object_uri, reused, key_is_derived) = match derived_object_uri {
            Err(why) => {
                decided("per_attempt");
                if let Some(suppressed) = self.key_notices.admit("per_attempt", source_name) {
                    tracing::info!(
                        event = "object_key_per_attempt",
                        batch_id,
                        source = source_name,
                        range,
                        key = %attempt_object_uri,
                        reason = %why,
                        suppressed,
                        "range {range} of {source_name} is written under its per-attempt key \
                         {attempt_object_uri}, so a replay of it will write another object \
                         (nothing is lost): {why} ({suppressed} more like this suppressed since \
                         the last notice)"
                    );
                }
                (attempt_object_uri, false, false)
            }
            Ok(derived) => match probe_object_key(
                self.backend.as_ref(),
                &derived,
                compressed.size_bytes,
                object_ck,
                labels,
                &self.attempts,
                &self.throttles,
            )
            .await
            {
                Ok(KeyProbe::Free) => {
                    decided("derived");
                    tracing::info!(
                        event = "object_key_derived",
                        batch_id,
                        source = source_name,
                        range,
                        key = %derived,
                        "no object for range {range} of {source_name} is stored yet; uploading it \
                         under its derived key {derived}, so a replay of the same bytes will reuse \
                         it; nothing to do"
                    );
                    (derived, false, true)
                }
                Ok(KeyProbe::Holds) => {
                    decided("reused");
                    tracing::info!(
                        event = "object_key_reused",
                        batch_id,
                        source = source_name,
                        range,
                        key = %derived,
                        "object for range {range} of {source_name} already archived and verified; \
                         reusing key {derived}, no upload; nothing to do"
                    );
                    (derived, true, true)
                }
                Ok(probe) => {
                    let (decision, what, todo) = match &probe {
                        KeyProbe::Differs(_) => (
                            "fallback_differs",
                            "exists but its bytes differ from this batch",
                            "nothing is lost; run `vtopctl scrub` to see whether a committed \
                             batch names the damaged object",
                        ),
                        KeyProbe::SizeOnly(_) => (
                            "fallback_size_only",
                            "exists but the backend could confirm only its size",
                            "nothing is lost; use a backend and checksum that verify stored \
                             content so a replay can reuse it",
                        ),
                        KeyProbe::Unreadable(_) => (
                            "fallback_unreadable",
                            "exists but could not be read to verify it",
                            "nothing is lost; run `vtopctl scrub` to see whether a committed \
                             batch names it, and check the store's health",
                        ),
                        KeyProbe::Unchecked(why) if why.contains("403") => (
                            "fallback_unchecked",
                            "could not be checked: the store refused the request",
                            "nothing is lost; grant list permission on the bucket: without it \
                             S3 answers a missing key with 403, not 404, and no replay can reuse \
                             an object",
                        ),
                        _ => (
                            "fallback_unchecked",
                            "could not be checked: the store did not answer the request",
                            "nothing is lost; check the store's health if this persists",
                        ),
                    };
                    decided(decision);
                    // A failed check of the key repeats for every batch while
                    // the store or its permissions stay as they are; an
                    // object that does not verify, verifies by size only or
                    // cannot be read is about one key and is always reported.
                    let suppressed = if decision == "fallback_unchecked" {
                        self.key_notices.admit(decision, source_name)
                    } else {
                        Some(0)
                    };
                    if let Some(suppressed) = suppressed {
                        let why = probe.reason();
                        tracing::warn!(
                            event = "object_key_fallback",
                            batch_id,
                            source = source_name,
                            range,
                            key = %derived,
                            fallback_key = %attempt_object_uri,
                            reason = %why,
                            decision,
                            suppressed,
                            "object at {derived} {what} ({why}); writing range {range} of \
                             {source_name} under {attempt_object_uri} instead of overwriting it; \
                             {todo}"
                        );
                    }
                    (attempt_object_uri, false, false)
                }
                Err(e) => {
                    decided("probe_throttled");
                    tracing::warn!(
                        event = "object_key_probe_throttled",
                        batch_id,
                        source = source_name,
                        range,
                        key = %derived,
                        error = %e,
                        "the store throttled the check of {derived}; nothing was written, the \
                         batch fails and range {range} of {source_name} is handed back for retry; \
                         nothing to do unless it persists, then reduce upload concurrency"
                    );
                    fail!(format!("object key probe of {derived} failed: {e}"))
                }
            },
        };
        metrics.object_reused = reused;

        // ONE PUT, OR RESUMABLE MULTIPART above the threshold (#482). The
        // retransmission unit for a single put is the whole batch — a lost
        // object near the end of a 100 MB batch costs the whole 100 MB
        // again through mark_failed -> replay. Multipart makes the part the
        // unit, using machinery the repository already owns; the config is
        // built ONE way for both this path and `vtopctl tier`, so part
        // sizes and fencing cannot drift.
        //
        // The fence is derived from the object key and the content digest,
        // NOT a lease this path does not have. What it protects: a session
        // resumed for THIS object identity carries THIS content — the
        // resume re-checks every kept part against the local bytes, so a
        // re-derived batch (a new digest) never adopts a stale session. It
        // does NOT fence against a concurrent writer to the same object
        // key. An attempt key has one writer by construction; a derived key
        // is named by its content digest, so two writers of it write the
        // same bytes (#626), which is the property that stands in for the
        // lease `tier` holds. A replay of the same bytes after a failed
        // attempt or a crash resumes the session that attempt left under the
        // derived key (#668), and sends only the parts it is missing.
        let mp_cfg = MultipartUploadConfig::from_upload(&self.config.upload, work_dir.clone());
        let t = Instant::now();
        let multipart_used =
            !reused && mp_cfg.should_multipart(self.backend.as_ref(), compressed.size_bytes);
        let fence = MultipartFence {
            // No segment lineage on the telemetry path; the identity is
            // the batch's bytes.
            expected_segment_generation: 0,
            fencing_epoch: 0,
            content_digest_hex: object_checksum.clone().unwrap_or_default(),
            content_digest_algorithm: algo.as_str().to_owned(),
            byte_length: compressed.size_bytes,
        };
        let mut mp_report = multipart::MultipartReport::default();
        let upload_result = if reused {
            None
        } else if multipart_used {
            let result = multipart::upload_resumable_with_report(
                self.backend.as_ref(),
                &mp_cfg,
                &compressed.path,
                &object_uri,
                object_ck,
                fence.clone(),
                &mut mp_report,
            )
            .await
            .map(|_| ());
            // Every request the upload made, and every throttle among them
            // (#668): the per-part retries inside it would otherwise hide all
            // but the last throttle from the width controller.
            note_multipart_requests(
                &mp_report,
                &object_uri,
                labels,
                &self.attempts,
                &self.throttles,
            );
            record_multipart(&mp_report);
            Some(result)
        } else {
            // A reused object made no request here, so none is counted.
            Some(note_store_request(
                self.backend
                    .put_object(&compressed.path, &object_uri, object_ck)
                    .await
                    .map(|_| ()),
                "object_upload",
                labels,
                &self.attempts,
                &self.throttles,
            ))
        };
        if let Some(Err(e)) = upload_result {
            if multipart_used {
                self.settle_failed_multipart(
                    &batch_id,
                    &object_uri,
                    key_is_derived,
                    &fence,
                    &mp_report,
                    &e,
                    &work_dir,
                )
                .await;
            }
            fail!(format!("object upload failed: {e}"));
        }
        metrics.object_upload_ms = t.elapsed().as_millis() as u64;
        if multipart_used {
            tracing::info!(batch_id, uri = %object_uri, "object_uploaded via multipart");
        }
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
        if !reused {
            tracing::info!(batch_id, uri = %object_uri, "object_uploaded");
        }

        // ---- Build + upload manifest ------------------------------------
        // `manifest.write_version` (#671 release B): 0.3 unless opted in.
        // A 0.4 plan names the method `validate --print-effective` and the
        // startup log print for this backend and algorithm; a backend with
        // none fails the batch rather than promising an unknown check.
        let manifest_format = match self.config.manifest.write_version {
            ManifestWriteVersion::V0_3 => ManifestFormat::V0_3,
            ManifestWriteVersion::V0_4 => match VerificationPlan::for_backend(
                &self.config.upload.backend,
                algo,
                self.config.upload.require_strong_verification,
            ) {
                Ok(plan) => ManifestFormat::V0_4 { plan },
                Err(e) => fail!(format!("manifest build failed: {e}")),
            },
        };
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
            // Without the #578 fields: an older verifier would drop them
            // and fail the self-hash (see `ProgressMarker::for_manifest`).
            source_progress: window.for_manifest(),
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
        .build_written(manifest_format, self.manifest_mac_key.as_ref())?;
        let manifest_version = manifest.fields().version.clone();

        // Track the expected path before writing so even a short/failed write
        // is removed on unwind through this function.
        let expected_manifest_path = work_dir.join(format!("{batch_id}.manifest.json"));
        staged_files.track(expected_manifest_path);
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
        let stored_manifest = match note_store_request(
            self.backend
                .put_manifest(&manifest_path, &manifest_uri, manifest_ck)
                .await,
            "manifest_upload",
            [
                tenant.as_str(),
                source.source_type.as_str(),
                format.extension(),
            ],
            &self.attempts,
            &self.throttles,
        ) {
            Ok(stored) => stored,
            Err(e) => {
                fail!(format!("manifest upload failed: {e}"))
            }
        };
        // The hardened profile pins recovery to this exact stored version;
        // an upload the store did not version cannot be pinned, so it cannot
        // advance (#135).
        if self.config.upload.require_object_versioning && stored_manifest.version_id.is_none() {
            fail!("manifest upload returned no object version to pin \
                   (require_object_versioning is set)"
                .to_string());
        }
        metrics.manifest_upload_ms = t.elapsed().as_millis() as u64;
        let man_patch = BatchPatch {
            manifest_uri: Some(manifest_uri.clone()),
            manifest_sha256: Some(manifest.fields().manifest.sha256.clone()),
            manifest_version_id: stored_manifest.version_id.clone(),
            ..Default::default()
        };
        let t_write = Instant::now();
        self.store
            .update_batch_state(&batch_id, BatchState::ManifestUploaded, &man_patch)
            .await?;
        state_write += t_write.elapsed();
        mark_state!(BatchState::ManifestUploaded);
        if let Some(mx) = telemetry::metrics() {
            mx.manifests_written_total
                .with_label_values(&[tenant.as_str(), manifest_version.as_str()])
                .inc();
        }
        tracing::info!(
            batch_id,
            uri = %manifest_uri,
            version = %manifest_version,
            "manifest_uploaded"
        );

        // ---- MANIFEST_UPLOADED -> VERIFIED (or FAILED) ------------------
        let t = Instant::now();
        // 1) the manifest is internally consistent (self-hash),
        if let Err(e) = manifest.verify_authentication(self.manifest_mac_key.as_ref()) {
            fail!(format!("manifest authentication verification failed: {e}"));
        }
        // 2) the stored object matches size + checksum,
        // A throttled verification is the store asking for less traffic too
        // (review): counted like an upload, so the cycle's verdict sees it.
        let obj_v = note_store_request(
            self.backend
                .verify_object(&object_uri, compressed.size_bytes, object_ck)
                .await,
            "object_verify",
            [
                tenant.as_str(),
                source.source_type.as_str(),
                format.extension(),
            ],
            &self.attempts,
            &self.throttles,
        )?;
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
        let man_v = note_store_request(
            self.backend
                .verify_object(&manifest_uri, manifest_size, manifest_ck)
                .await,
            "manifest_verify",
            [
                tenant.as_str(),
                source.source_type.as_str(),
                format.extension(),
            ],
            &self.attempts,
            &self.throttles,
        )?;
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
            // When storage assigned a version, authenticate the bytes of that
            // exact immutable version — the same bytes recovery will pin to —
            // instead of whatever currently sits at the key.
            let stored_read = note_store_request(
                match stored_manifest.version_id.as_deref() {
                    Some(version) => {
                        self.backend
                            .get_manifest_pinned(&manifest_uri, version, MAX_MANIFEST_BYTES)
                            .await
                    }
                    None => {
                        self.backend
                            .get_object_bounded(&manifest_uri, MAX_MANIFEST_BYTES)
                            .await
                    }
                },
                "manifest_readback",
                [
                    tenant.as_str(),
                    source.source_type.as_str(),
                    format.extension(),
                ],
                &self.attempts,
                &self.throttles,
            );
            let stored_bytes = match stored_read {
                Ok(bytes) => bytes,
                Err(e) => fail!(format!("stored manifest download failed: {e}")),
            };
            let stored = match ParsedManifest::from_slice(&stored_bytes) {
                Ok(stored) => stored,
                Err(e) => fail!(format!("stored manifest parse failed: {e}")),
            };
            if stored.verify_authentication(Some(key)).is_err()
                || stored.fields().batch_id != batch_id
                || stored.fields().manifest.uri != manifest_uri
                || stored.fields().object.uri != object_uri
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
        // The class goes into the ledger with the transition (#664): a
        // recovery after a restart counts the commit by it, not by what its
        // own re-check of the store finds then. The backends' own messages
        // name the check that ran (a store checksum, a read-back, size only).
        let class = if backend_limited {
            vtop_state::VerificationClass::SizeOnly
        } else {
            vtop_state::VerificationClass::Strong
        };
        if backend_limited {
            tracing::warn!(
                batch_id,
                verification_class = class.as_str(),
                object_check = %obj_v.message,
                manifest_check = %man_v.message,
                "verification_passed (backend_limited: size-only)"
            );
        } else {
            tracing::info!(
                batch_id,
                verification_class = class.as_str(),
                object_check = %obj_v.message,
                manifest_check = %man_v.message,
                "verification_passed"
            );
        }
        let t_write = Instant::now();
        self.store.mark_verified(&batch_id, class).await?;
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
            // A reused object put nothing on the wire (#626).
            if !metrics.object_reused {
                mx.bytes_out_total
                    .with_label_values(&l)
                    .inc_by(metrics.compressed_bytes);
            }
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
                // A reused object made no upload; a 0 ms sample would pull
                // the upload latency distribution down.
                if stage == "object_upload" && metrics.object_reused {
                    continue;
                }
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

    /// Whether a read starting at `start` picks up exactly where the buffered
    /// window ends. Byte-offset sources are continuous by construction until
    /// a rotation or truncation restarts the file at byte 0, or a different
    /// file takes the path (#558). Kafka partitions are not judged here.
    fn continues(&self, start: &ProgressMarker) -> bool {
        let Some(end) = self.latest_end.as_ref() else {
            return true;
        };
        // An unknown inode on either side cannot prove a change of file.
        let same_file = |a: Option<u64>, b: Option<u64>| match (a, b) {
            (Some(a), Some(b)) => a == b,
            _ => true,
        };
        match (end, start) {
            (
                ProgressMarker::File {
                    inode: end_inode,
                    end_byte,
                    ..
                },
                ProgressMarker::File {
                    inode: start_inode,
                    start_byte,
                    ..
                },
            )
            | (
                ProgressMarker::SyslogSpool {
                    inode: end_inode,
                    end_byte,
                    ..
                },
                ProgressMarker::SyslogSpool {
                    inode: start_inode,
                    start_byte,
                    ..
                },
            ) => same_file(*end_inode, *start_inode) && end_byte == start_byte,
            _ => true,
        }
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
        // The window's event-time range is the earliest and latest of its
        // reads' ranges, not the first read's start and the last read's end:
        // record times arrive out of order (#559).
        self.first_timestamp =
            vtop_core::event_time::earliest(self.first_timestamp.take(), read.first_timestamp);
        self.last_timestamp =
            vtop_core::event_time::latest(self.last_timestamp.take(), read.last_timestamp);
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

/// Count a throttle apart from every other upload failure (#102). The batch
/// fails and replays either way; what changes is what an operator sees. A
/// store asking for less is a signal a same-rate retry only worsens, and
/// until #102's controller consumes it, the counter is how it is seen.
/// Every store request feeds the cycle's verdict (review): it is an attempt,
/// and a throttle on it is a throttle — a preflight before the first upload
/// and a read-back after the last one included, since the store asks for less
/// traffic on any of them.
fn note_store_request<T>(
    result: Result<T, VtopError>,
    stage: &str,
    labels: [&str; 3],
    attempts: &AtomicU64,
    throttles: &AtomicU64,
) -> Result<T, VtopError> {
    attempts.fetch_add(1, Ordering::Relaxed);
    if let Err(error) = &result {
        note_upload_throttle(error, stage, labels, throttles);
    }
    result
}

/// What a batch's derived object key holds (#626).
#[derive(Debug, Clone, PartialEq, Eq)]
enum KeyProbe {
    /// The store says nothing is there: put the batch under it.
    Free,
    /// An object is there and verified strongly against the batch's size and
    /// digest: these bytes are already stored, reuse it.
    Holds,
    /// An object is there and does not verify against this batch.
    Differs(String),
    /// An object is there and the backend could confirm only its size.
    SizeOnly(String),
    /// An object is there and its verification failed with an error.
    Unreadable(String),
    /// The store refused or failed the check of the key (a 403 without list
    /// permission, a 5xx, a timeout): it did not say the key is absent.
    Unchecked(String),
}

impl KeyProbe {
    fn reason(&self) -> &str {
        match self {
            KeyProbe::Free | KeyProbe::Holds => "",
            KeyProbe::Differs(why)
            | KeyProbe::SizeOnly(why)
            | KeyProbe::Unreadable(why)
            | KeyProbe::Unchecked(why) => why,
        }
    }
}

/// Look at a derived object key before writing to it (#626). A throttle is
/// returned as an error, since the store is asking for less traffic and the
/// batch is retried like any other stage's; every other failure only makes
/// the key unusable, so the batch falls back to its attempt key rather than
/// fail on a probe a backend may refuse (a HEAD without list permission is a
/// 403 on S3).
async fn probe_object_key(
    backend: &dyn UploadBackend,
    uri: &str,
    size: u64,
    checksum: Option<ObjectChecksum<'_>>,
    labels: [&str; 3],
    attempts: &AtomicU64,
    throttles: &AtomicU64,
) -> Result<KeyProbe, VtopError> {
    match note_store_request(
        backend.head_object(uri).await,
        "object_probe",
        labels,
        attempts,
        throttles,
    ) {
        Ok(_) => {}
        Err(VtopError::NotFound(_)) => return Ok(KeyProbe::Free),
        Err(e) if e.is_upload_throttle() => return Err(e),
        Err(e) => return Ok(KeyProbe::Unchecked(format!("the probe failed: {e}"))),
    }
    match note_store_request(
        backend.verify_object(uri, size, checksum).await,
        "object_probe_verify",
        labels,
        attempts,
        throttles,
    ) {
        Ok(v) if v.passed && !v.backend_limited => Ok(KeyProbe::Holds),
        Ok(v) if v.passed => Ok(KeyProbe::SizeOnly(v.message)),
        Ok(v) => Ok(KeyProbe::Differs(v.message)),
        Err(e) if e.is_upload_throttle() => Err(e),
        Err(e) => Ok(KeyProbe::Unreadable(format!(
            "an object is there and could not be verified: {e}"
        ))),
    }
}

/// Count every request a multipart upload made, and every throttle among
/// them (#668). The upload retries a part itself, so a throttle it recovered
/// from never reaches the caller as an error; without this the width
/// controller would see only a throttle the retries could not outlast.
fn note_multipart_requests(
    report: &multipart::MultipartReport,
    object_uri: &str,
    labels: [&str; 3],
    attempts: &AtomicU64,
    throttles: &AtomicU64,
) {
    attempts.fetch_add(u64::from(report.requests), Ordering::Relaxed);
    let throttled = report.throttled_requests;
    if throttled == 0 {
        return;
    }
    throttles.fetch_add(u64::from(throttled), Ordering::Relaxed);
    if let Some(mx) = telemetry::metrics() {
        mx.upload_throttled_total
            .with_label_values(&[labels[0], labels[1], labels[2], "object_upload"])
            .inc_by(u64::from(throttled));
    }
    tracing::warn!(
        event = "upload_throttled",
        stage = "object_upload",
        key = object_uri,
        throttled,
        "the store throttled {throttled} multipart request(s) for {object_uri}; each was \
         retried after a pause, and the engine narrows its upload width when \
         batching.adaptive_width is on; nothing to do unless this persists, then reduce \
         upload concurrency"
    );
}

/// The multipart metrics for one upload call (#668).
fn record_multipart(report: &multipart::MultipartReport) {
    let Some(mx) = telemetry::metrics() else {
        return;
    };
    for (result, n) in [
        ("uploaded", report.parts_uploaded),
        ("reused", report.parts_reused),
        ("retried", report.part_retries),
        ("failed", report.parts_failed),
    ] {
        if n > 0 {
            mx.multipart_parts_total
                .with_label_values(&[result])
                .inc_by(u64::from(n));
        }
    }
    for (outcome, happened) in [
        ("completed", report.completed),
        ("resumed", report.resumed),
        ("restarted", report.restarted),
    ] {
        if happened {
            mx.multipart_sessions_total
                .with_label_values(&[outcome])
                .inc();
        }
    }
    mx.multipart_bytes_reused_total.inc_by(report.bytes_reused);
}

fn note_upload_throttle(error: &VtopError, stage: &str, labels: [&str; 3], throttles: &AtomicU64) {
    if !error.is_upload_throttle() {
        return;
    }
    throttles.fetch_add(1, Ordering::Relaxed);
    if let Some(mx) = telemetry::metrics() {
        mx.upload_throttled_total
            .with_label_values(&[labels[0], labels[1], labels[2], stage])
            .inc();
    }
    tracing::warn!(stage, error = %error, "upload_throttled");
}

/// AIMD over the upload width (#102): halve on a cycle the store throttled,
/// grow by one on a clean cycle, never below the floor nor above the
/// ceiling. Every clean cycle IS the forced probe the issue demands — the
/// controller cannot settle below the ceiling while the store is quiet —
/// and the floor keeps forward progress possible however hard the store
/// throttles, which is the piece the reverted poll-window heuristic (#99)
/// was missing: that one shrank until it could not succeed and had no way
/// back. The signal is the throttle count alone, not latency: a throttle is
/// the store saying "less" in so many words, while latency needs a load
/// nobody has yet put on the store to mean anything (#102's own warning).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WidthController {
    ceiling: usize,
    floor: usize,
    current: usize,
}

impl WidthController {
    pub fn new(ceiling: usize, floor: usize) -> Self {
        Self::under_operator_ceiling(ceiling, floor, None)
    }

    /// The controller, with its bounds clamped to the operator's
    /// `upload.max_concurrency_ceiling` (#481). BOTH bounds are clamped: the
    /// ceiling so no configured width exceeds the operator's, and the floor
    /// because a minimum width is a value below which the flow refuses to
    /// yield — left unclamped it would be the way round the ceiling. Validation
    /// already refuses either contradiction; this makes the controller hold the
    /// operator's number by construction rather than by the order of checks
    /// elsewhere. `None` is exactly [`WidthController::new`]. The AIMD law
    /// itself is untouched.
    pub fn under_operator_ceiling(
        ceiling: usize,
        floor: usize,
        operator_ceiling: Option<usize>,
    ) -> Self {
        let ceiling = operator_ceiling
            .map_or(ceiling, |operator| ceiling.min(operator))
            .max(1);
        let floor = floor.clamp(1, ceiling);
        Self {
            ceiling,
            floor,
            current: ceiling,
        }
    }

    pub fn width(&self) -> usize {
        self.current
    }

    /// One cycle's verdict in, the next cycle's width out.
    pub fn observe_cycle(&mut self, throttled: bool) -> usize {
        self.current = if throttled {
            (self.current / 2).max(self.floor)
        } else {
            self.current.saturating_add(1).min(self.ceiling)
        };
        self.current
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
    /// Process-wide hardened-profile preflight cache (#135); see
    /// [`Pipeline::versioned_buckets`].
    versioned_buckets: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    /// Throttles counted across the process, shared into every pipeline (#102).
    throttles: Arc<AtomicU64>,
    /// Uploads issued across every pipeline, see `Pipeline::attempts`.
    attempts: Arc<AtomicU64>,
    /// The width controller (#102); consulted only when
    /// `batching.adaptive_width.enabled`.
    width: WidthController,
    /// Process-wide bucket-provisioning cells (#102); see
    /// [`Pipeline::provisioned_buckets`].
    provisioned_buckets: ProvisionedBuckets,
    /// Process-wide object-key notice limiter (#626); see
    /// [`Pipeline::key_notices`].
    key_notices: Arc<KeyNotices>,
    /// Per-source accumulation buffers, keyed by `(source_type, source_name)`.
    pending: HashMap<(SourceType, String), PendingBuffer>,
    /// Set by a read cycle that returned any records; drives the adaptive
    /// inter-cycle sleep in [`Engine::run`]. Reset at the top of each cycle.
    cycle_had_data: bool,
    /// Set by a cycle in which any batch ended FAILED (or errored before
    /// VERIFIED) and was handed back to its source for re-reading. Reset at
    /// the top of each cycle by [`Engine::run`]; it keeps a failing store from
    /// being retried at read speed.
    cycle_had_failure: bool,
    /// Consecutive loop iterations that saw a failure. Drives the retry
    /// backoff in [`Engine::run`] and returns to zero on the first clean one.
    failure_streak: u32,
    /// Last committed-history prune attempt (#128); pruning is idle-cycle
    /// only and rate-limited, so a busy pipeline never pays for it.
    last_ledger_prune: Option<std::time::Instant>,
    /// Last abandoned-multipart sweep (#668). Failed uploads keep their
    /// sessions for the replay now, so a long-running engine sweeps every
    /// [`Engine::multipart_sweep_interval`], not only at startup.
    last_multipart_sweep: Option<std::time::Instant>,
    /// VERIFIED rows this process holds and has not committed (#624): held
    /// back by recovery above a range of the same unit that is being read
    /// again, or whose source commit failed. Each is re-checked after every
    /// later pass of its source type ([`Engine::settle_verified`]). Before,
    /// only the next restart looked at them again.
    unsettled: Vec<Unsettled>,
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

/// The state store, every request bounded by `engine.state_store_timeout_seconds`
/// (#556). One function for every path that opens the ledger - the engine and
/// `vtopctl prune-ledger` alike - so none can fall back to the default bound
/// while the operator configured another (review of #568).
pub(crate) async fn connect_configured_store(
    engine: &vtop_core::config::EngineConfig,
    state_store: &vtop_core::config::ResolvedStateStore,
) -> Result<Box<dyn StateStore>, VtopError> {
    connect_state_store_with_deadline(
        state_store.expose_secret(),
        Duration::from_secs(engine.state_store_timeout_seconds),
    )
    .await
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
        streams.validate(&config)?;
        let state_store = config.engine.state_store.resolve()?;
        let locks = acquire_instance_locks(&config, &state_store)?;
        let cleanup = cleanup_work_dir(
            std::path::Path::new(&config.engine.work_dir),
            Duration::from_secs(config.engine.work_retention_seconds),
            config.engine.work_max_bytes,
        )?;
        if cleanup.removed_files > 0 {
            tracing::info!(
                removed_files = cleanup.removed_files,
                removed_bytes = cleanup.removed_bytes,
                retained_bytes = cleanup.retained_bytes,
                "cleaned stale work-directory artifacts"
            );
        }
        if state_store.is_postgres() {
            tracing::warn!(
                "state store is Postgres: the engine is SINGLE-INSTANCE per state \
                 store, and host-local locks cannot see engines on OTHER hosts. \
                 Do not run replicas against this database (see #66/#93)"
            );
        }
        verification_mode::announce(&config, &streams);
        let mut engine = Self::new_with_state_store(config, streams, state_store).await?;
        engine._instance_locks = locks;
        // Reclaim multipart sessions no source will replay (#482, review):
        // a crash mid-upload leaves a *.multipart.json and a live upload id,
        // and since #668 a failed upload keeps its session for the replay.
        // The abandonment sweep frees the ones nobody resumed, under the
        // instance lock, best-effort; sessions younger than the abandon
        // window are left for the replay. It runs here and then periodically
        // from `run_until`.
        engine.sweep_abandoned_multipart().await;
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
        streams.validate(&config)?;
        let state_store = config.engine.state_store.resolve()?;
        Self::new_with_state_store(config, streams, state_store).await
    }

    async fn new_with_state_store(
        config: VtopConfig,
        streams: StreamsConfig,
        state_store: ResolvedStateStore,
    ) -> Result<Self, VtopError> {
        // #128: automatic pruning needs DELETE on the ledger. The documented
        // PostgreSQL runtime identity deliberately lacks it (least
        // privilege), so an enabled policy there would fail-and-retry
        // forever. Fail fast and point at the maintenance path instead.
        if config.engine.ledger_retention_days.is_some() && state_store.is_postgres() {
            return Err(VtopError::Config(
                "engine.ledger_retention_days requires DELETE on the ledger, which the \
                 PostgreSQL runtime identity deliberately lacks; unset it and schedule \
                 `vtopctl prune-ledger` with a maintenance identity instead"
                    .into(),
            ));
        }

        let manifest_mac_key = config.resolve_manifest_mac_key()?;
        let store = connect_configured_store(&config.engine, &state_store).await?;
        let backend = vtop_upload::build_backend(&config.upload).await?;

        let mut adapters: HashMap<SourceType, Box<dyn SourceAdapter>> = HashMap::new();
        if let Some(k) = &config.sources.kafka {
            if k.enabled {
                #[cfg(feature = "kafka")]
                {
                    let fmt = default_format_for(&streams, SourceType::Kafka);
                    adapters.insert(
                        SourceType::Kafka,
                        Box::new(KafkaSource::new(k.clone(), fmt)?),
                    );
                }
                #[cfg(not(feature = "kafka"))]
                return Err(VtopError::Config(
                    "Kafka source is enabled, but this build omits the `kafka` feature".to_owned(),
                ));
            }
        }
        if let Some(f) = &config.sources.file {
            if f.enabled {
                if f.delete_after_commit && !cfg!(unix) {
                    // Said at startup, not first when a file is due (#619).
                    // Without inode numbers no file can be proven to be the
                    // one a batch read, so none is ever deleted, whatever
                    // delete_without_open_check says (review of #689).
                    tracing::warn!(
                        "sources.file.delete_after_commit is set, but this platform has no \
                         inode numbers to prove a file is the one archived, so no file is ever \
                         deleted"
                    );
                } else if f.delete_after_commit
                    && !f.delete_without_open_check
                    && !cfg!(target_os = "linux")
                {
                    tracing::warn!(
                        "sources.file.delete_after_commit is set, but this platform has no check \
                         that no process holds a file open (the engine uses Linux write leases), \
                         so fully archived files are KEPT; set \
                         sources.file.delete_without_open_check: true to delete them on the quiet \
                         period alone"
                    );
                }
                let fmt = default_format_for(&streams, SourceType::File);
                adapters.insert(
                    SourceType::File,
                    Box::new(
                        FileSource::with_mode(
                            f.paths.clone(),
                            fmt,
                            f.delete_after_commit,
                            f.whole_file,
                        )
                        .with_delete_quiet_period(Duration::from_secs(f.delete_quiet_seconds))
                        .with_delete_without_open_check(f.delete_without_open_check),
                    ),
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

        prime_source_counters(&config, &streams, adapters.keys());
        prime_multipart_counters();

        // The active transport's max_concurrency narrows the batch ceiling by
        // MINIMUM (#480): the WidthController ceiling is the RESOLVED value, so
        // an accepted upload.transports.<t>.max_concurrency actually bounds
        // object concurrency at runtime rather than being recorded and ignored.
        // Gated on the backend — only s3_native routes through the seam, so a
        // stray tuning block on a mock/localfs/command backend never alters the
        // global batch pipeline (resolved_upload_ceiling).
        let resolved_ceiling = config
            .upload
            .resolved_upload_ceiling(config.batching.max_concurrent_batches);
        let width = WidthController::under_operator_ceiling(
            resolved_ceiling,
            config.batching.adaptive_width.min_width,
            config.upload.max_concurrency_ceiling,
        );
        Ok(Self {
            config,
            streams,
            store,
            backend,
            adapters,
            manifest_mac_key,
            versioned_buckets: Arc::default(),
            throttles: Arc::default(),
            attempts: Arc::default(),
            width,
            provisioned_buckets: Arc::default(),
            key_notices: Arc::default(),
            pending: HashMap::new(),
            cycle_had_data: false,
            cycle_had_failure: false,
            failure_streak: 0,
            last_ledger_prune: None,
            last_multipart_sweep: None,
            unsettled: Vec::new(),
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
            versioned_buckets: Arc::clone(&self.versioned_buckets),
            throttles: Arc::clone(&self.throttles),
            attempts: Arc::clone(&self.attempts),
            provisioned_buckets: Arc::clone(&self.provisioned_buckets),
            key_notices: Arc::clone(&self.key_notices),
        }
    }

    /// The upload width the next cycle will use (#102).
    pub(crate) fn upload_width(&self) -> usize {
        if self.config.batching.adaptive_width.enabled {
            self.width.width()
        } else {
            // Fixed width: the RESOLVED ceiling (#480), so the active
            // transport's max_concurrency narrows it here too — not just when
            // adaptive width is on. Backend-gated (s3_native only).
            // And under the operator's ceiling (#481), for the same reason the
            // controller is: by construction, not only by validation.
            let resolved = self
                .config
                .upload
                .resolved_upload_ceiling(self.config.batching.max_concurrent_batches);
            self.config
                .upload
                .max_concurrency_ceiling
                .map_or(resolved, |operator| resolved.min(operator))
                .max(1)
        }
    }

    /// Key used by read-only manifest verification, if authentication is
    /// required by this process's configuration.
    pub(crate) fn manifest_mac_key(&self) -> Option<&ManifestMacKey> {
        self.manifest_mac_key.as_ref()
    }

    /// Scrub committed `rows` against this engine's store, authenticating
    /// manifests with its key (#628). The key stays inside the engine: a
    /// caller gets the report, never the secret it could forge manifests
    /// with. The guard below fails to compile if the key accessor is ever
    /// made public again (review of #652):
    ///
    /// ```compile_fail
    /// fn leak(engine: &vtop_cli::Engine) {
    ///     let _ = engine.manifest_mac_key();
    /// }
    /// ```
    pub async fn scrub(
        &self,
        rows: Vec<crate::scrub::ScrubRow>,
        opts: &crate::scrub::ScrubOptions,
    ) -> Result<crate::scrub::ScrubReport, VtopError> {
        crate::scrub::scrub(
            self.backend.as_ref(),
            self.manifest_mac_key.as_ref(),
            rows,
            opts,
        )
        .await
    }

    /// [`Engine::scrub`] under locks the caller holds until it has published
    /// (`vtopctl scrub`, review of #652); the cursor's final position past a
    /// finding comes back pending, for the caller to commit after publishing.
    pub(crate) async fn scrub_held(
        &self,
        rows: Vec<crate::scrub::ScrubRow>,
        opts: &crate::scrub::ScrubOptions,
        held: &crate::scrub::RunLock,
    ) -> Result<crate::scrub::ScrubReport, VtopError> {
        crate::scrub::scrub_held(
            self.backend.as_ref(),
            self.manifest_mac_key.as_ref(),
            rows,
            opts,
            held,
        )
        .await
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
        // Rows left uncommitted by EARLIER passes are re-checked after this
        // one (#624): after, so a held-back range is settled by the pass whose
        // re-read committed past it; only earlier ones, so a commit that has
        // just failed is not retried at once against a source that just
        // refused it.
        let earlier = self
            .unsettled
            .iter()
            .filter(|row| row.source_type == source_type)
            .count();
        let result = self
            .run_cycle(adapter.as_mut(), &source_type, force_flush)
            .await;
        if earlier > 0 {
            self.settle_verified(adapter.as_mut(), &source_type, earlier)
                .await;
        }
        self.adapters.insert(source_type, adapter);
        result
    }

    /// Re-check the VERIFIED rows of `source_type` this process left
    /// uncommitted - the first `count` of them, the ones recorded before the
    /// pass that just ran - and commit those that may commit now (#624).
    ///
    /// Recovery ran only at startup, so a VERIFIED row it held back, or one
    /// whose source commit failed, stayed VERIFIED until the next restart.
    /// That restart then committed it after newer ranges had committed past
    /// it: the Kafka group offset went backwards (#614, #617), and the file
    /// adapter's cursor - its read head too, for a range read before the
    /// restart - went back to the stale end and read everything since again.
    ///
    /// Nothing is claimed. Every row here is this process's own: created by
    /// its pipeline, or claimed by its recovery, and in both cases it still
    /// holds the fence token (VERIFIED releases none) and has finished with
    /// it - the pass that produced it committed or gave up on it before
    /// returning. So no in-flight batch, of this engine or another, is ever
    /// touched: `claim_incomplete_batches` would claim this engine's own
    /// in-flight rows. A row can outlive its lease, so the claim is renewed -
    /// fenced, in one write - before the source is touched (review of #624);
    /// a refused renewal means another engine owns the row now, and it is
    /// dropped here without moving the source; that engine's recovery
    /// commits it.
    ///
    /// - A row recovery held back waits until its source has committed past
    ///   its END - by then the re-read below it has archived and committed its
    ///   bytes again, and committing it moves no source: the Kafka adapter
    ///   skips a commit at or below its frontier (#617), the file and spool
    ///   adapters one at or below a committed point they confirmed. Committing
    ///   it any earlier could move the committed point past bytes still being
    ///   re-read, which is what the hold-back is for.
    /// - A row whose commit failed is retried as the pipeline left it: the
    ///   pipeline had already decided it may commit. A newer range committed
    ///   since makes the retry the same superseded no-op.
    async fn settle_verified(
        &mut self,
        adapter: &mut dyn SourceAdapter,
        source_type: &SourceType,
        count: usize,
    ) {
        let mut earlier = Vec::new();
        let mut kept = Vec::new();
        for row in std::mem::take(&mut self.unsettled) {
            if &row.source_type == source_type && earlier.len() < count {
                earlier.push(row);
            } else {
                kept.push(row);
            }
        }
        for row in earlier {
            if !self.settle_one(adapter, &row).await {
                kept.push(row);
            }
        }
        self.unsettled = kept;
    }

    /// Settle one row; `true` when it needs no more attention (committed, or
    /// no longer this process's to commit).
    async fn settle_one(&self, adapter: &mut dyn SourceAdapter, row: &Unsettled) -> bool {
        let rec = match self.store.get_batch(&row.batch_id).await {
            Ok(Some(rec)) if rec.state == BatchState::Verified => rec,
            // Gone, or moved on: nothing left for this process to commit.
            Ok(_) => return true,
            Err(e) => {
                tracing::warn!(batch_id = %row.batch_id, error = %e, "could not re-read an uncommitted VERIFIED row; retried after the next pass");
                return false;
            }
        };
        if row.held_back
            && !held_row_may_settle(
                adapter.committed_through(&rec.progress_end),
                row.committed_at_hold,
                end_position(&rec.progress_end),
            )
        {
            return false;
        }
        // Renew the claim, fenced, BEFORE the source is touched (review of
        // #624). The row may have been held for longer than its lease, and an
        // engine that claimed it since may be about to re-read a range below
        // it; moving the source now would clamp that re-read, or with
        // `delete_after_commit` delete the file. A refusal at the ledger write
        // afterwards comes too late to undo either. After a renewal no other
        // engine can claim the row until the new lease runs out.
        let lease_until = (Utc::now() + chrono::Duration::seconds(LEASE_TTL_SECS)).to_rfc3339();
        match self.store.renew_claim(&rec.batch_id, &lease_until).await {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(
                    batch_id = %rec.batch_id,
                    "an uncommitted VERIFIED row is no longer this engine's (another engine claimed it); \
                     its new owner commits it"
                );
                return true;
            }
            Err(e) => {
                tracing::warn!(batch_id = %rec.batch_id, error = %e, "could not renew the claim on an uncommitted VERIFIED row; retried after the next pass");
                return false;
            }
        }
        if let Err(e) = adapter.commit_progress(&rec.progress_end).await {
            tracing::warn!(batch_id = %rec.batch_id, error = %e, "source commit still failing; retried after the next pass");
            return false;
        }
        match self.store.mark_source_committed(&rec.batch_id).await {
            Ok(()) => {
                if let Some(mx) = telemetry::metrics() {
                    let l = [
                        rec.tenant.as_str(),
                        rec.source_type.as_str(),
                        rec.format.extension(),
                    ];
                    // A row recovery left (held back, or its commit failed)
                    // was verified by the process that crashed; count it
                    // here, as `commit_recovered` does, so commits_total never
                    // exceeds verified_total. A pipeline row's verification
                    // was counted by the pipeline (review of #624).
                    if row.count_verified {
                        mx.verified_total.with_label_values(&l).inc();
                        mx.batches_total
                            .with_label_values(&[l[0], l[1], l[2], BatchState::Verified.as_str()])
                            .inc();
                    }
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
                // A size-only verification is counted at the commit, as
                // `commit_recovered` counts it (#651): by the class `rec`,
                // read again just above, was verified with (#664), and by
                // recovery's re-check only for a row with none.
                if let Some(recheck) = row.recheck {
                    verification_mode::count_size_only_commit(recheck, &rec);
                }
                tracing::info!(
                    batch_id = %rec.batch_id,
                    held_back = row.held_back,
                    verification_class = verification_mode::class_name(&rec),
                    "source_committed (an earlier pass left it VERIFIED)"
                );
                true
            }
            Err(e) => {
                let lost = e.to_string().contains("claimed by another engine")
                    || e.to_string().contains("not claimed by this engine");
                tracing::warn!(
                    batch_id = %rec.batch_id,
                    error = %e,
                    lost,
                    "could not record the commit of an earlier VERIFIED row"
                );
                lost
            }
        }
    }

    async fn run_cycle(
        &mut self,
        adapter: &mut dyn SourceAdapter,
        source_type: &SourceType,
        force_flush: bool,
    ) -> Result<Vec<BatchOutcome>, VtopError> {
        let sources = adapter.discover_sources().await?;
        // How long ONE PASS may wait for data. Whether that is paid once or
        // once per source is the adapter's business, not this loop's: the
        // Kafka adapter multiplexes every topic-partition into a single poll
        // (#96), while file and syslog-spool reads do not wait on it at all.
        // This comment used to say "paid per source, serially", which was the
        // pre-#96 truth and is the third copy of that claim to outlive the fix
        // — see `BatchingConfig::source_poll_wait_ms`.
        let max_wait = Duration::from_millis(self.config.batching.source_poll_wait_ms);

        // Bound the Kafka per-topic state: drop partition-metadata entries and
        // retention-loss bookmarks for topics that no longer exist, so a broker
        // that churns through short-lived topics does not grow either map
        // without limit. `sources` is the full live set this cycle, which is
        // exactly what pruning needs.
        #[cfg(feature = "kafka")]
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
        let mut retention_lost: u64 = 0;

        // ONE call reads every source: the default trait impl walks them
        // serially (file, syslog — independent handles), while Kafka overrides
        // it to assign all topic-partitions once and pay a single poll wait for
        // the whole topic set instead of one per topic (#96 A1). The adapter
        // reports how the pass's wall-clock divided between the buckets; the
        // per-source outcomes drive the counts and the buffer routing.
        //
        // Buffers sealed early because the next read did not continue them
        // (a rotated or truncated file): flushed this cycle, below.
        let mut discontinued: Vec<PendingBuffer> = Vec::new();
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
                retention_lost = report.retention_lost.iter().map(|l| l.records).sum();
                if let Some(mx) = telemetry::metrics() {
                    // The adapter observes loss per topic-partition, and the
                    // topic names the stream — so each entry lands on the
                    // tenant whose records were lost, resolved exactly as
                    // `source_read_errors_total` resolves it below. The topic
                    // itself stays in the adapter's warn line, not in the
                    // labels: a per-topic label would mint a series per
                    // topic, and the label set here matches the read-error
                    // counter's on purpose.
                    for loss in &report.retention_lost {
                        let t = tenant_for_source(
                            &self.streams,
                            &self.config.engine.tenant,
                            &loss.source_name,
                        );
                        mx.retention_lost_records_total
                            .with_label_values(&[t.as_str(), source_type.as_str()])
                            .inc_by(loss.records);
                    }
                }
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
                                let t = tenant_for_source(
                                    &self.streams,
                                    &self.config.engine.tenant,
                                    &source.source_name,
                                );
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
                        // A read that does not pick up where the buffered
                        // window ended — a rotated or truncated file restarted
                        // at byte 0 — cannot join it. Merged, the batch would
                        // carry a start from one file and an end from another:
                        // a range no rewind can re-read and no audit can
                        // reconcile. Seal what is buffered and start afresh.
                        if self
                            .pending
                            .get(&key)
                            .is_some_and(|buf| !buf.continues(&read.progress_start))
                        {
                            if let Some(old) = self.pending.remove(&key) {
                                tracing::warn!(
                                    source = %old.source.source_name,
                                    "read does not continue the buffered window; sealing it before the new content"
                                );
                                discontinued.push(old);
                            }
                        }
                        self.pending
                            .entry(key)
                            .or_insert_with(|| PendingBuffer::new(&tenant, buf_source, limits))
                            .append(read);
                    }
                }
            }
        }
        // A range handed back to a file rotated away that its re-read then had
        // to refuse (the rotated file was truncated, or no longer starts with
        // the bytes it was held for) is missing from the archive just as a
        // refused rewind is, and is counted the same way (review of #621). The
        // read error above only says a read failed.
        if let Some(files) = adapter.as_any_mut().downcast_mut::<FileSource>() {
            for path in files.take_refused_rereads() {
                self.count_refused_replay_of(source_type, &path);
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
                // Almost always zero; non-zero is records lost upstream of
                // the archive (#361), carried here so the one line operators
                // already watch is the one that says so.
                retention_lost,
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
        let sealed_early = discontinued.into_iter().map(Some);
        let flushed = flush_keys.into_iter().map(|key| self.pending.remove(&key));
        for buf in sealed_early.chain(flushed).collect::<Vec<_>>() {
            let Some(mut buf) = buf else { continue };
            let Some(read) = buf.drain() else { continue };
            // The batch's contents are fixed here. It may then queue behind
            // `limit` others for an upload slot, so the seal instant is taken
            // now, not when its future first runs (review on #570).
            let sealed_at = Utc::now();
            let stream = self.streams.lookup(&buf.source.source_name).cloned();
            work.push((buf.source.clone(), read, stream, sealed_at));
        }

        // The width this cycle runs at (#102): the ceiling, or the
        // controller's current width when it is on. The throttle count is
        // read before and after so the cycle's verdict is its own.
        let limit = self.upload_width();
        if let Some(mx) = telemetry::metrics() {
            mx.upload_width.set(limit as i64);
        }
        let throttles_before = self.throttles.load(Ordering::Relaxed);
        // A cycle that ISSUED no upload probes nothing (review): idle polling
        // after a throttle must not walk the width back up, and neither may a
        // cycle whose batches all failed before reaching the store.
        let attempts_before = self.attempts.load(Ordering::Relaxed);
        let pipeline = self.pipeline();
        // buffer_unordered keeps `limit` verifies in flight and yields each as
        // it finishes, so a slow upload never holds up the ones behind it.
        // `work` is MOVED into the futures rather than iterated by reference.
        // Cloning each ReadResult here would duplicate every payload before
        // buffer_unordered bounds anything, so with max_bytes at 100 MiB and a
        // concurrency of 8 the queue alone could hold ~800 MiB of avoidable
        // copies — the clone is not bounded by the concurrency limit, only the
        // execution is.
        //
        // Each result travels with the order its window was read in and the
        // marker its batch STARTED at: commits are applied in read order (see
        // below), and a batch that does not reach VERIFIED must hand its range
        // back to the source (see the rewind below), by when the `ReadResult`
        // that held the marker has been consumed.
        let mut verified: Vec<(usize, ProgressMarker, Result<VerifyStep, VtopError>)> =
            futures::stream::iter(work.into_iter().enumerate().map(
                |(order, (source, read, stream, sealed_at))| {
                    let pipeline = &pipeline;
                    let started_at = read.progress_start.clone();
                    async move {
                        let step = pipeline
                            .process_until_verified(&source, read, stream.as_ref(), sealed_at)
                            .await;
                        (order, started_at, step)
                    }
                },
            ))
            .buffer_unordered(limit)
            .collect()
            .await;
        // Verification finishes in whatever order the store answers, but the
        // commits are applied in the order the windows were READ. When a
        // rotation seals the old file's window and starts a new one in the
        // same cycle, both are in flight at once; committing the new file's
        // window first would leave the OLD one as the most recent commit, and
        // the restart seed (the newest committed row's inode) would then name a
        // file that is no longer there and re-read the live one from byte 0.
        verified.sort_by_key(|(order, _, _)| *order);

        // The cycle's verdict for the width controller (#102): any throttle
        // in it halves the next width; none grows it by one.
        if self.config.batching.adaptive_width.enabled
            && self.attempts.load(Ordering::Relaxed) > attempts_before
        {
            let throttled = self.throttles.load(Ordering::Relaxed) > throttles_before;
            let before = self.width.width();
            let after = self.width.observe_cycle(throttled);
            if after != before {
                tracing::info!(
                    from = before,
                    to = after,
                    throttled,
                    "upload_width_adjusted"
                );
            }
        }

        // Do NOT bail on the first error. Verify runs concurrently, so by the
        // time one batch fails, others may already be persisted as VERIFIED —
        // and their buffers have been drained from `pending`. Returning early
        // would drop those VerifiedBatch values without committing source
        // progress, leaving them verified-but-uncommitted until a restart ran
        // recovery.
        //
        // So: commit every batch that succeeded, then report the first error.
        // Nothing verified is abandoned, and the caller still sees the failure.
        //
        // A batch that did NOT reach VERIFIED is a different matter, and the
        // read head is the reason. Adapters advance their read position when
        // they READ, not when the batch commits, so a failed batch's records
        // are already behind the head: left alone, the next cycle reads on
        // from the end of the failed range, and the first later range to
        // verify commits PAST it. The failed bytes are then never archived,
        // and the restart that was supposed to repair this clamps its rewind
        // to the committed point (#552). Each such range is therefore handed
        // back to its source here, immediately, so the very next read starts
        // at the first byte that has not been archived.
        let mut outcomes = Vec::new();
        let mut first_err: Option<VtopError> = None;
        let mut rewinds: Vec<ProgressMarker> = Vec::new();
        for (_, started_at, step) in verified {
            match step {
                Ok(VerifyStep::Finished(outcome)) => {
                    if outcome.final_state == BatchState::Failed {
                        rewinds.push(started_at);
                    }
                    outcomes.push(outcome);
                }
                Ok(VerifyStep::Verified(v)) => {
                    let batch_id = v.batch_id.clone();
                    match self.pipeline().commit_verified(adapter, v).await {
                        Ok(outcome) => {
                            // The source commit failed: the row stays VERIFIED,
                            // and a later pass retries it (#624).
                            if !outcome.committed {
                                self.unsettled
                                    .push(Unsettled::commit_failed(&batch_id, source_type));
                            }
                            outcomes.push(outcome)
                        }
                        Err(e) => {
                            // Already VERIFIED and durable (the source may
                            // have committed, the ledger write did not); a
                            // later pass retries it (#624), and recovery
                            // after a restart. Keep going so siblings still
                            // commit.
                            tracing::error!(batch_id, error = %e, "commit failed after verify");
                            self.unsettled
                                .push(Unsettled::commit_failed(&batch_id, source_type));
                            first_err.get_or_insert(e);
                        }
                    }
                }
                Err(e) => {
                    // An error before the batch was VERIFIED (a ledger write
                    // that failed, say — the FAILED write of `fail!` included).
                    // Whether the ledger holds a row for it is unknown, and
                    // re-reading the range costs a duplicate at worst where
                    // skipping it costs the data.
                    //
                    // This is where the running path parts from recovery, which
                    // never rewinds on a decision the ledger did not record
                    // (#123). At startup the read head sits at the committed
                    // point, at or below every uncommitted range, so a withheld
                    // rewind there leaves the range ahead of the head. Here the
                    // head is already PAST the range: withheld, the next cycle
                    // reads on, a later batch commits past it, and the restart
                    // that marks the row for replay clamps its rewind to that
                    // commit — the range is never archived (#627).
                    rewinds.push(started_at);
                    first_err.get_or_insert(e);
                }
            }
        }
        if !rewinds.is_empty() {
            self.cycle_had_failure = true;
        }
        for (marker, result) in rewinds
            .iter()
            .zip(adapter.replay_from_markers(&rewinds).await)
        {
            match result {
                Ok(()) => tracing::warn!(
                    source_type = %source_type,
                    start = ?marker,
                    "batch did not reach VERIFIED; its range was handed back to the source and will be re-read"
                ),
                Err(e) => {
                    // The adapter could not rewind (a spool file that was
                    // rotated away, say). The range is NOT being re-read, so
                    // this must be loud: it is the one outcome that leaves the
                    // archive short of the source - and counted, so it can be
                    // alerted on rather than grepped for (#621).
                    self.count_refused_replay(source_type, marker);
                    tracing::error!(
                        source_type = %source_type,
                        start = ?marker,
                        error = %e,
                        "could not hand a failed range back to its source; it will not be re-read"
                    );
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
    async fn storage_still_verified(&self, rec: &BatchRecord) -> Recheck {
        // The check itself is shared with `vtopctl scrub` (#628), which runs
        // it on committed rows, so the two cannot drift apart. Its `Limited`
        // is exactly what this re-check calls size-only (#629): the backend
        // passed the object with `backend_limited`, having confirmed size and
        // existence but no digest. Whether that may commit, and how it is
        // logged, stays #651's decision.
        let check = crate::scrub::check_stored_batch(
            self.backend.as_ref(),
            self.manifest_mac_key.as_ref(),
            rec,
        )
        .await;
        match check.result {
            crate::scrub::ScrubResult::Ok => Recheck::Strong,
            crate::scrub::ScrubResult::UnsupportedVersion => {
                // Loud: only an engine older than the batch's writer gets
                // here (the check has proven the bytes are the writer's
                // pinned version), and the upgrade order says that should not
                // happen.
                if let Some(mx) = telemetry::metrics() {
                    mx.recovery_unsupported_manifest_total
                        .with_label_values(&[
                            rec.tenant.as_str(),
                            rec.source_type.as_str(),
                            rec.format.extension(),
                        ])
                        .inc();
                }
                tracing::error!(
                    batch_id = %rec.batch_id,
                    detail = %check.detail,
                    "recovery: a VERIFIED batch's manifest is from a newer release; it stays VERIFIED, \
                     neither committed nor replayed, until an upgraded engine recovers it - upgrade this engine"
                );
                Recheck::UnsupportedVersion
            }
            crate::scrub::ScrubResult::Limited => {
                verification_mode::recheck_size_only(&self.config, rec, &check.detail)
            }
            result => {
                tracing::warn!(
                    batch_id = %rec.batch_id,
                    result = result.as_str(),
                    detail = %check.detail,
                    "recovery re-check: the stored batch does not check out"
                );
                Recheck::Failed
            }
        }
    }

    /// Recovery scan run at startup. Seeds adapters from committed batches and
    /// resolves incomplete batches without ever advancing source progress for
    /// unverified data.
    pub async fn recover(&mut self) -> Result<RecoverySummary, VtopError> {
        // Seed file/syslog/Kafka committed positions from the ledger.
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
        // Rewinds are COLLECTED, not applied row by row. Each call to
        // `replay_from_marker` overwrites the adapter's read position, so with
        // several incomplete ranges for one source the last row visited used to
        // win and every earlier range was skipped (#552). Keeping the lowest
        // start per source unit and applying it once after the scan reads from
        // the first byte that is not archived, whatever order the rows came in.
        let mut rewinds = RewindPlan::default();
        // VERIFIED rows that still check out against storage. Their source
        // commits wait until every replay decision is known (see below).
        let mut verified: Vec<(BatchRecord, Recheck)> = Vec::new();
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
                    let recheck = self.storage_still_verified(&rec).await;
                    verification_mode::count_recheck(&rec, recheck);
                    if recheck == Recheck::UnsupportedVersion {
                        // Written by a newer release (#671): not stale, only
                        // unreadable here. It is neither committed nor marked
                        // for replay - the row stays VERIFIED for an upgraded
                        // engine to commit - and nothing is rewound for it.
                        if self.committed_past(&rec) {
                            // Its source has committed past it, proven as
                            // for any range #567 retires: nothing of it can be
                            // re-read and an upgraded engine's commit would be
                            // a no-op. Left VERIFIED, it was claimed,
                            // downloaded and counted pending at every restart
                            // and never pruned (review of #674). Retiring it
                            // makes no source commit.
                            self.retire_unsupported(&rec, &mut summary).await;
                            continue;
                        }
                        // Held (#639) otherwise: its range is not committed,
                        // so a VERIFIED range of the same unit above it must
                        // not commit past it.
                        rewinds.hold(&rec.progress_start);
                        summary.still_pending += 1;
                        continue;
                    }
                    if recheck == Recheck::Failed {
                        tracing::warn!(
                            batch_id = %rec.batch_id,
                            object_uri = rec.object_uri.as_deref().unwrap_or(""),
                            verification_class = verification_mode::class_name(&rec),
                            "recovery: VERIFIED batch failed storage re-check; replaying instead of committing"
                        );
                        // The replay decision must be DURABLE before the
                        // adapter rewinds: if these transitions fail, the row
                        // stays VERIFIED and the next recovery re-checks it
                        // again — never rewind on a decision the ledger did
                        // not record (#123 review).
                        //
                        // Not rewound, but HELD (#639): its range still has to
                        // be re-read, so no VERIFIED range of the same file
                        // above it may commit in this pass. Committing one
                        // moved the committed point past it, and the next
                        // recovery's rewind for it was clamped there.
                        if let Err(e) = self
                            .store
                            .mark_failed(
                                &rec.batch_id,
                                "recovery: stale VERIFIED (storage re-check failed)",
                            )
                            .await
                        {
                            tracing::error!(batch_id = %rec.batch_id, error = %e, "recovery: could not persist re-check failure");
                            rewinds.hold(&rec.progress_start);
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
                            rewinds.hold(&rec.progress_start);
                            summary.still_pending += 1;
                            continue;
                        }
                        rewinds.add(&rec.progress_start);
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
                    verification_mode::note_stored_class(&rec, recheck);
                    verified.push((rec, recheck));
                }
                RecoveryAction::None => {}
                _ if self.committed_past(&rec) => {
                    // Its source has committed past it (#567): a rewind would
                    // land at or above its end and read nothing again, so
                    // the row is retired instead of being marked for replay
                    // again. Before SUPERSEDED existed it was claimed,
                    // rewritten twice and re-planned at every restart, and
                    // retention, which deletes only terminal rows, kept it.
                    // A failed write rewinds nothing: there is nothing below
                    // the committed point to read, and the next recovery
                    // decides again.
                    match self.retire_superseded(&rec).await {
                        Ok(()) => {
                            summary.superseded += 1;
                            if let Some(mx) = telemetry::metrics() {
                                mx.batches_total
                                    .with_label_values(&[
                                        rec.tenant.as_str(),
                                        rec.source_type.as_str(),
                                        rec.format.extension(),
                                        BatchState::Superseded.as_str(),
                                    ])
                                    .inc();
                            }
                            tracing::info!(
                                batch_id = %rec.batch_id,
                                state = %rec.state,
                                "recovered: SUPERSEDED (its source has committed past it; nothing to re-read)"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                batch_id = %rec.batch_id,
                                error = %e,
                                "recovery: could not retire a range its source has committed past; \
                                 it stays as it is, and nothing below the committed point is re-read"
                            );
                            summary.still_pending += 1;
                        }
                    }
                }
                _ => {
                    // Any other incomplete state has no durable, replayable
                    // object we can finish from in this prototype: mark for
                    // replay so the *uncommitted* source range is re-read.
                    // Source progress was never advanced, so this is safe.
                    //
                    // The replay decision must be DURABLE before the adapter
                    // rewinds, exactly as on the stale-VERIFIED path: if either
                    // transition fails the row stays as it was, no rewind is
                    // recorded, and the next recovery decides again — never
                    // rewind on a decision the ledger did not record (#123).
                    // Both writes are fenced (#557): one refused because
                    // another engine claimed the batch in between means that
                    // engine re-reads the range now, and rewinding here as
                    // well would ingest it twice (review of #569).
                    //
                    // Held, not rewound, as on the stale-VERIFIED path (#639):
                    // a VERIFIED range above it must not commit past it.
                    if let Err(e) = self
                        .store
                        .mark_failed(&rec.batch_id, "recovered: replay required")
                        .await
                    {
                        tracing::error!(batch_id = %rec.batch_id, error = %e, "recovery: could not persist the replay decision; not rewinding");
                        rewinds.hold(&rec.progress_start);
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
                        tracing::error!(batch_id = %rec.batch_id, error = %e, "recovery: could not persist REPLAY_REQUIRED; not rewinding");
                        rewinds.hold(&rec.progress_start);
                        summary.still_pending += 1;
                        continue;
                    }
                    rewinds.add(&rec.progress_start);
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
        // Group by source type so each adapter rewinds its whole batch in one
        // call (see `SourceAdapter::replay_from_markers`).
        let mut by_source: Vec<(SourceType, Vec<ProgressMarker>)> = Vec::new();
        for marker in rewinds.markers() {
            match by_source
                .iter_mut()
                .find(|(source_type, _)| *source_type == marker.source_type())
            {
                Some((_, markers)) => markers.push(marker),
                None => by_source.push((marker.source_type(), vec![marker])),
            }
        }
        // First pass: rewind, only to learn where each unit's read head LANDS.
        // A rewind never goes below the source's committed point, so a stale
        // ledger row whose bytes a later batch already archived lands above its
        // own start. Counting such a row as "about to be re-read" made it hold
        // back every VERIFIED range above it for good (review of #563). Errors
        // are reported by the second pass.
        let mut landed: HashMap<String, i128> = HashMap::new();
        for (source_type, markers) in &by_source {
            if let Some(adapter) = self.adapters.get_mut(source_type) {
                let _ = adapter.replay_from_markers(markers).await;
            }
        }
        // A range handed back to a file that was rotated away is re-read from
        // where it was rotated to (#621) - and its FAILED row is rewound again
        // at every restart, so the re-read must start above what a later
        // batch already archived of that file.
        self.floor_rotated_replays().await;
        for (source_type, markers) in &by_source {
            let Some(adapter) = self.adapters.get(source_type) else {
                continue;
            };
            for marker in markers {
                if let Some(position) = adapter.read_position(marker) {
                    landed.insert(rewind_key(marker), position);
                }
            }
        }
        rewinds.rebase(&landed);
        // Commit the VERIFIED rows — unless a lower range of the same source
        // unit is about to be re-read. Committing the higher one first moves
        // the adapter's committed point past the lower one, and the rewind that
        // follows is clamped to it: the failed range would then be skipped (the
        // file adapters never rewind below their committed point). The row
        // stays VERIFIED, its object stays in the archive, and a later pass
        // commits it once the source has caught up. A lower range whose replay
        // decision could not be recorded holds it back too, though nothing
        // rewinds for it (#639).
        for (rec, recheck) in verified {
            if rewinds.holds_back(&rec.progress_start) {
                tracing::warn!(
                    batch_id = %rec.batch_id,
                    "recovery: not committing a VERIFIED range above an incomplete one of the same source; \
                     it stays VERIFIED until the lower range has been re-read"
                );
                summary.still_pending += 1;
                let at_hold = self
                    .adapters
                    .get(&rec.source_type)
                    .and_then(|adapter| adapter.committed_through(&rec.progress_end));
                self.unsettled
                    .push(Unsettled::held_back(&rec, at_hold, recheck));
                continue;
            }
            self.commit_recovered(&rec, recheck, &mut summary).await?;
        }
        // Second pass: the commits above moved committed points up, and the
        // first pass's rewinds are not clamped to them. Rewind again so no
        // range that was just committed is read a second time.
        for (source_type, markers) in by_source {
            let Some(adapter) = self.adapters.get_mut(&source_type) else {
                // The decision to replay is durable, but there is nothing to
                // rewind: this process has no adapter for that source type.
                tracing::error!(
                    %source_type,
                    ranges = markers.len(),
                    "recovery has no adapter for an incomplete range's source; it will not be re-read"
                );
                summary.still_pending += markers.len();
                continue;
            };
            for (marker, result) in markers
                .iter()
                .zip(adapter.replay_from_markers(&markers).await)
            {
                if let Err(e) = result {
                    // The decision to replay is already durable; what failed is
                    // the rewind itself. Swallowing this made recovery report
                    // success for a range that was never going to be re-read.
                    self.count_refused_replay(&source_type, marker);
                    tracing::error!(
                        start = ?marker,
                        error = %e,
                        "recovery could not hand an incomplete range back to its source; it will not be re-read"
                    );
                    summary.still_pending += 1;
                }
            }
        }
        Ok(summary)
    }

    /// Start each re-read of a rotated file (#621) above what the ledger says
    /// is already archived of that file. Failing to ask costs a duplicate of
    /// what was archived, never a skip, so it is a warning, not an error.
    async fn floor_rotated_replays(&mut self) {
        let Some(files) = self
            .adapters
            .get_mut(&SourceType::File)
            .and_then(|adapter| adapter.as_any_mut().downcast_mut::<FileSource>())
        else {
            return;
        };
        let paths = files.rotated_replay_paths();
        if paths.is_empty() {
            return;
        }
        let rows = match self.store.committed_cursors_by_file(SourceType::File).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(
                    paths = ?paths,
                    error = %e,
                    "recovery could not read what is already archived of rotated files; their \
                     failed ranges are re-read from their start, which may archive part of them \
                     again"
                );
                return;
            }
        };
        for path in paths {
            let archived: Vec<ArchivedEnd> = rows
                .iter()
                .filter(|row| row.path == path)
                .map(archived_end)
                .collect();
            files.raise_rotated_replay_floor(&path, &archived);
        }
    }

    /// A source refused to hand a failed range back (#621): counted under the
    /// tenant the range's source is charged to, like a read error.
    fn count_refused_replay(&self, source_type: &SourceType, marker: &ProgressMarker) {
        let source_name = match marker {
            ProgressMarker::Kafka { topic, .. } => topic.as_str(),
            ProgressMarker::File { path, .. } | ProgressMarker::SyslogSpool { path, .. } => {
                path.as_str()
            }
        };
        self.count_refused_replay_of(source_type, source_name);
    }

    fn count_refused_replay_of(&self, source_type: &SourceType, source_name: &str) {
        let Some(mx) = telemetry::metrics() else {
            return;
        };
        let tenant = tenant_for_source(&self.streams, &self.config.engine.tenant, source_name);
        mx.source_replay_refused_total
            .with_label_values(&[tenant.as_str(), source_type.as_str()])
            .inc();
    }

    /// Whether `rec`'s source has committed past the end of its range, in the
    /// same file (#567): the adapter's committed point for the unit - at
    /// startup, the ledger's newest committed row, which retention never
    /// prunes - is at or above where the range ends. A rewind to the range
    /// would then land at that point and re-read none of it.
    ///
    /// A range whose start and end name different units (a window across a
    /// rotation) is not judged: no one committed point describes both files.
    /// Nor is one whose adapter cannot say, which keeps the replay path.
    fn committed_past(&self, rec: &BatchRecord) -> bool {
        let (start, end) = (&rec.progress_start, &rec.progress_end);
        if unit_key(start) != unit_key(end) {
            return false;
        }
        let Some(adapter) = self.adapters.get(&start.source_type()) else {
            return false;
        };
        let range_end = end_position(end);
        [start, end].into_iter().all(|marker| {
            adapter
                .committed_through(marker)
                .is_some_and(|committed| committed >= range_end)
        })
    }

    /// Retire a VERIFIED row whose manifest is from a newer release and whose
    /// source has committed past it (#671): SUPERSEDED, as the
    /// `committed_past` branch of [`Engine::recover`] retires any range. A
    /// failed write leaves it as it was, pending, and nothing is rewound.
    async fn retire_unsupported(&self, rec: &BatchRecord, summary: &mut RecoverySummary) {
        match self.retire_superseded(rec).await {
            Ok(()) => {
                summary.superseded += 1;
                if let Some(mx) = telemetry::metrics() {
                    mx.batches_total
                        .with_label_values(&[
                            rec.tenant.as_str(),
                            rec.source_type.as_str(),
                            rec.format.extension(),
                            BatchState::Superseded.as_str(),
                        ])
                        .inc();
                }
                tracing::warn!(
                    batch_id = %rec.batch_id,
                    "recovered: SUPERSEDED (its manifest is from a newer release and its source has committed past it)"
                );
            }
            Err(e) => {
                tracing::error!(
                    batch_id = %rec.batch_id,
                    error = %e,
                    "recovery: could not retire a newer-release range its source has committed past; it stays as it is"
                );
                summary.still_pending += 1;
            }
        }
    }

    /// Retire a range its source has committed past (#567): SUPERSEDED, the
    /// terminal state no claim or recovery visits again. Legal only from a
    /// handed-back range, so a row a crashed engine left in flight is
    /// recorded as FAILED first. Neither write moves any source progress.
    async fn retire_superseded(&self, rec: &BatchRecord) -> Result<(), VtopError> {
        if !matches!(rec.state, BatchState::Failed | BatchState::ReplayRequired) {
            self.store
                .mark_failed(
                    &rec.batch_id,
                    "recovered: superseded (its source committed past it)",
                )
                .await?;
        }
        self.store
            .update_batch_state(
                &rec.batch_id,
                BatchState::Superseded,
                &BatchPatch::default(),
            )
            .await
    }

    /// Commit the source progress of a VERIFIED batch that was just re-checked
    /// against storage, and record it.
    async fn commit_recovered(
        &mut self,
        rec: &BatchRecord,
        recheck: Recheck,
        summary: &mut RecoverySummary,
    ) -> Result<(), VtopError> {
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
                            .with_label_values(&[l[0], l[1], l[2], BatchState::Verified.as_str()])
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
                    // Counted at the commit, like verified_total above, not
                    // at the re-check: a held-back row passes the re-check on
                    // every restart without committing (#629 review). By the
                    // class the row was verified with, as the claim read it
                    // from the ledger (#664); by the re-check only for a row
                    // with none.
                    verification_mode::count_size_only_commit(recheck, rec);
                    summary.committed += 1;
                    tracing::info!(
                        batch_id = %rec.batch_id,
                        verification_class = verification_mode::class_name(rec),
                        recheck = recheck.as_str(),
                        "recovered: source_committed"
                    );
                }
                Err(e) => {
                    tracing::error!(batch_id = %rec.batch_id, error = %e, "recovery commit failed");
                    summary.still_pending += 1;
                    self.unsettled
                        .push(Unsettled::recovered_commit_failed(rec, recheck));
                }
            }
        } else {
            summary.still_pending += 1;
        }
        Ok(())
    }

    async fn seed_committed_offsets(&mut self) -> Result<(), VtopError> {
        // The per-path cursor is computed IN THE STORE. This runs at every
        // startup and the ledger grows without bound, so materialising every
        // row here made recovery memory scale with history (#77).
        //
        // Each cursor names the inode its offset was read from, so a file
        // that was rotated while the engine was down is started at byte 0
        // instead of at the old file's high-water mark (#558) - and the
        // fingerprint of its first bytes, for a replacement that kept the
        // inode (#578), and of the bytes below the offset, for one that also
        // kept the first page (#620).
        //
        // Per FILE, not per path, for file sources (review of #621): a range
        // re-read from a rotated file commits under the path after the batches
        // of the file now there, so the path's newest row can name the rotated
        // file. The seed is then the row of the file at the path, when that
        // file is positively identified (`FileSource::seed_choice`); otherwise
        // the newest row, as before.
        let file_rows = self
            .store
            .committed_cursors_by_file(SourceType::File)
            .await?;
        let spool_cursors = self
            .store
            .committed_cursors(SourceType::SyslogSpool)
            .await?;
        if let Some(a) = self.adapters.get_mut(&SourceType::File) {
            if let Some(fs) = a.as_any_mut().downcast_mut::<FileSource>() {
                // The store answers each path's files together, newest first.
                for files in file_rows.chunk_by(|a, b| a.path == b.path) {
                    let archived: Vec<ArchivedEnd> = files.iter().map(archived_end).collect();
                    let c = &files[FileSource::seed_choice(&files[0].path, &archived)];
                    fs.seed_committed_from(
                        &c.path,
                        c.end_byte,
                        c.inode,
                        c.observed_size,
                        c.content_fingerprint.clone(),
                        c.tail_fingerprint.clone(),
                    );
                    // A file archived but not yet deleted before the restart
                    // (#619): re-armed only if it still has the mtime the
                    // committed batch saw.
                    fs.arm_delete_from_seed(&c.path, c.observed_mtime.as_deref());
                }
            }
        }
        if let Some(a) = self.adapters.get_mut(&SourceType::SyslogSpool) {
            if let Some(ss) = a.as_any_mut().downcast_mut::<SyslogSpoolSource>() {
                for c in spool_cursors {
                    ss.seed_committed_from(
                        &c.path,
                        c.end_byte,
                        c.inode,
                        c.observed_size,
                        c.content_fingerprint,
                    );
                }
            }
        }
        // Kafka resumes from the LEDGER, not from the consumer group's
        // committed offset on the broker (#614). That offset is committed
        // before the fenced ledger write, with no group generation Kafka could
        // check, so an engine that lost its claim on a batch can still move it
        // - backwards, re-reading what is archived, or forwards, past a range
        // that failed its re-check. Seeded before the claim below, so the
        // recovery rewinds are clamped to the ledger's frontier too.
        #[cfg(feature = "kafka")]
        if let Some(k) = self
            .adapters
            .get_mut(&SourceType::Kafka)
            .and_then(|a| a.as_any_mut().downcast_mut::<KafkaSource>())
        {
            let committed = self.store.committed_kafka_offsets().await?;
            k.seed_committed_from_ledger(committed.iter().map(|c| {
                (
                    c.topic.as_str(),
                    c.partition,
                    c.consumer_group.as_str(),
                    c.end_offset,
                )
            }));
        }
        Ok(())
    }

    /// How often a running engine sweeps abandoned multipart sessions
    /// (#668): a quarter of `upload.multipart_abandon_after_secs`, and not
    /// more often than every 15 minutes, so a session is aborted at most a
    /// quarter-window after it becomes abandoned.
    pub fn multipart_sweep_interval(&self) -> Duration {
        Duration::from_secs(self.config.upload.multipart_abandon_after_secs / 4)
            .max(Duration::from_secs(15 * 60))
    }

    /// Abort the multipart uploads of sessions older than
    /// `upload.multipart_abandon_after_secs` and delete their files (#668):
    /// the ones no replay resumed, because the replay's bytes differed or
    /// the range was never read again. A session whose abort fails is kept
    /// for the next sweep, so its upload id is not lost. Returns how many
    /// were aborted.
    pub async fn sweep_abandoned_multipart(&mut self) -> usize {
        self.last_multipart_sweep = Some(std::time::Instant::now());
        if !self.backend.supports_multipart() {
            return 0;
        }
        let abandon = self.config.upload.multipart_abandon_after_secs;
        let mp_cfg = MultipartUploadConfig::from_upload(
            &self.config.upload,
            std::path::PathBuf::from(&self.config.engine.work_dir),
        );
        match multipart::cleanup_abandoned(self.backend.as_ref(), &mp_cfg).await {
            Ok(0) => 0,
            Ok(aborted) => {
                if let Some(mx) = telemetry::metrics() {
                    mx.multipart_sessions_total
                        .with_label_values(&["swept"])
                        .inc_by(aborted as u64);
                }
                tracing::info!(
                    event = "multipart_sweep",
                    aborted,
                    abandon_after_secs = abandon,
                    "aborted {aborted} abandoned multipart upload(s) older than {abandon} s; \
                     their parts are no longer stored; nothing to do"
                );
                aborted
            }
            Err(e) => {
                tracing::warn!(
                    event = "multipart_sweep_failed",
                    error = %e,
                    "the abandoned multipart sweep failed ({e}); every session is kept and \
                     the next sweep tries again in {} s; batches are unaffected; check the \
                     work directory's permissions if this repeats",
                    self.multipart_sweep_interval().as_secs()
                );
                0
            }
        }
    }

    /// Whether [`Engine::multipart_sweep_interval`] has passed since the last
    /// sweep (#668).
    fn multipart_sweep_due(&self) -> bool {
        self.last_multipart_sweep
            .is_none_or(|at| at.elapsed() >= self.multipart_sweep_interval())
    }

    /// Run the continuous processing loop until the shutdown signal fires.
    /// #128: incremental committed-history retention. Disabled unless
    /// `engine.ledger_retention_days` is set; runs only on idle cycles, at
    /// most once a minute, one bounded batch per pass, and never touches
    /// the per-path cursor-seed rows (the store enforces that invariant).
    async fn maybe_prune_ledger(&mut self) {
        let Some(days) = self.config.engine.ledger_retention_days else {
            return;
        };
        let due = self
            .last_ledger_prune
            .map(|at| at.elapsed() >= Duration::from_secs(60))
            .unwrap_or(true);
        if !due {
            return;
        }
        self.last_ledger_prune = Some(std::time::Instant::now());
        let cutoff = (chrono::Utc::now() - chrono::Duration::days(i64::from(days))).to_rfc3339();
        // The rows go to a digest before they are deleted (#560): without it,
        // a coverage audit would read the pruned history as holes, and lose
        // the ledger's record of what each manifest was.
        let digest_dir = crate::coverage::default_digest_dir(&self.config.engine);
        match crate::coverage::prune_with_digest(
            self.store.as_ref(),
            &cutoff,
            self.config.engine.ledger_prune_batch,
            &digest_dir,
        )
        .await
        {
            Ok((0, _)) => {}
            Ok((deleted, digest)) => {
                tracing::info!(
                    deleted,
                    digest = ?digest,
                    "ledger retention pruned committed history"
                );
            }
            Err(error) => tracing::warn!(%error, "ledger retention prune failed"),
        }
    }

    /// Run the engine until the process is asked to stop (#526).
    ///
    /// SIGINT and, on Unix, SIGTERM are both orderly stops: the source pass in
    /// progress finishes, every buffered-but-unsealed batch is force-flushed,
    /// and `run` returns `Ok` (exit 0). A flush that leaves anything
    /// unarchived (a source pass that errors, or a batch that ends FAILED)
    /// returns an error (exit 1) once every source type has been tried. A
    /// SECOND signal while that flush runs abandons it and returns an error
    /// (exit 1); [`Engine::run_until`] says why that is safe.
    ///
    /// The listeners are registered HERE, once, before recovery, and live for
    /// the whole run. The shape this replaced created a fresh `ctrl_c()`
    /// future on every loop turn, which lost any signal that landed while a
    /// cycle was busy: tokio's process-wide handler had already replaced the
    /// default disposition, and no listener existed to receive it. A loaded
    /// engine ignored Ctrl-C indefinitely, and SIGTERM had no handler at all,
    /// so `docker stop` and pod termination skipped the flush.
    pub async fn run(&mut self) -> Result<(), VtopError> {
        let shutdown = OsShutdownSignals::install();
        self.run_until(shutdown).await
    }

    /// [`Engine::run`] with stop requests taken from `shutdown` instead of
    /// the process's signals, so a test can ask for a stop at an exact point
    /// in a cycle without signalling the test process.
    ///
    /// A request is observed between source passes, never inside one:
    /// `run_source` carries a batch through read, upload, verify and commit,
    /// and cutting it off half-way would make every orderly stop a crash
    /// stop. A request that arrives mid-pass stays pending in `shutdown` and
    /// is taken before the next pass starts. That check runs before EVERY
    /// source type's pass, not once per cycle, for two reasons. A request
    /// that arrived during `recover()` must not buy a whole processing cycle
    /// first; and an engine with several adapters (a Kafka poll wait among
    /// them) stops after the pass in progress rather than after all of them.
    /// Skipping the rest of a cycle loses nothing: the shutdown flush runs
    /// every source type's pass anyway. An idle cycle checks once more before
    /// its ledger-retention prune, which can spend a state-store timeout. After
    /// the cycle, the backoff `select!` is `biased` toward a stop so a
    /// productive loop's zero backoff cannot outrun it, and an idle engine's
    /// sleep is interrupted at once.
    ///
    /// A second request during the shutdown flush abandons the flush and
    /// returns an error. It is the operator's way out when the flush itself
    /// is stuck (an unreachable object store, retrying), and it is no worse
    /// than the SIGKILL they would reach for instead: no source progress is
    /// committed before its batch verifies, so a batch cut off mid-flush is
    /// recovered or re-read on the next start exactly as after a crash.
    /// Only a request sent after the shutdown began counts as the second:
    /// every request already pending when the first is observed is drained
    /// first. Without that, a SIGINT and a SIGTERM that both land during a
    /// busy pass (separate streams, so they do not coalesce) would have the
    /// second abandon the flush before it started.
    pub async fn run_until(
        &mut self,
        mut shutdown: impl ShutdownRequests,
    ) -> Result<(), VtopError> {
        self.recover().await?;
        let types: Vec<SourceType> = self.adapters.keys().cloned().collect();
        let idle = Duration::from_millis(self.config.batching.idle_poll_interval_ms);
        let cause = 'run: loop {
            // Accumulate across cycles; only threshold-tripped buffers flush.
            self.cycle_had_data = false;
            self.cycle_had_failure = false;
            for st in &types {
                // A stop already pending ends the run before this pass, not
                // after the whole cycle; see the doc comment for why that is
                // safe and why it also covers a stop sent during recovery.
                if let Some(cause) = shutdown.pending_request() {
                    break 'run cause;
                }
                if let Err(e) = self.run_source(st.clone(), false).await {
                    tracing::error!(error = %e, source_type = %st, "process cycle error");
                }
            }
            self.failure_streak = if self.cycle_had_failure {
                self.failure_streak.saturating_add(1)
            } else {
                0
            };
            // Abandoned multipart sessions are reclaimed while the engine
            // runs, busy or idle (#668): a failed upload keeps its session
            // for the replay, and one no replay resumes must not wait for a
            // restart to be aborted. Cheap when not due. When due it can
            // spend a request deadline per session against a store that does
            // not answer, so a stop, pending or arriving, wins the race.
            // Dropping the sweep half-way is safe: a session file is deleted
            // only after its upload's abort succeeded, so an interrupted one
            // is simply there for the next sweep.
            if self.multipart_sweep_due() {
                if let Some(cause) = shutdown.pending_request() {
                    break 'run cause;
                }
                tokio::select! {
                    biased;
                    cause = shutdown.next_request() => break 'run cause,
                    _ = self.sweep_abandoned_multipart() => {}
                }
            }
            // Failing cycle: wait, growing with the streak (the failed range is
            // read again, so a tight loop would hammer a store that is down).
            // Productive cycle: loop again immediately. Idle cycle: back off.
            // `Duration::ZERO` still yields to the runtime through `select!`,
            // so this never starves the executor.
            let backoff = if self.cycle_had_failure {
                failure_backoff(
                    idle,
                    self.failure_streak,
                    u64::from(Utc::now().timestamp_subsec_nanos()),
                )
            } else if self.cycle_had_data {
                Duration::ZERO
            } else {
                // Housekeeping can spend a state-store timeout plus digest
                // I/O; a stop that landed during the last pass must not wait
                // for it.
                if let Some(cause) = shutdown.pending_request() {
                    break 'run cause;
                }
                self.maybe_prune_ledger().await;
                idle
            };
            tokio::select! {
                biased;
                cause = shutdown.next_request() => break cause,
                () = tokio::time::sleep(backoff) => {}
            }
        };

        // Everything already queued when the stop was observed belongs to
        // that same stop — the other signal kind landing in the same busy
        // pass, or an impatient repeat. Only a request sent from here on may
        // abandon the flush.
        shutdown.drain_pending();
        tracing::info!(
            signal = cause,
            "shutdown signal received; flushing and exiting"
        );
        // Force-flush any buffered-but-unsealed data so it is not left for
        // the next start to re-read. Every source type is attempted even
        // after one fails, but a failure is not a complete flush: a pass that
        // returned `Err`, or a batch that ended FAILED (which `run_source`
        // reports as `Ok` with that outcome), left data unarchived, and
        // exiting 0 would tell a supervisor otherwise. The first such failure
        // is returned once every type has been tried. A source that cannot be
        // READ is deliberately not one: `run_cycle` logs it and still flushes
        // what is buffered, and the unread data never left the source, so the
        // next start reads it like data that arrives after the flush.
        let flush = async {
            let mut first_failure: Option<String> = None;
            for st in &types {
                match self.run_source(st.clone(), true).await {
                    Ok(outcomes) => {
                        let failed = outcomes
                            .iter()
                            .filter(|o| o.final_state == BatchState::Failed)
                            .count();
                        if failed > 0 {
                            tracing::error!(
                                source_type = %st,
                                failed,
                                "shutdown flush: batch(es) did not verify"
                            );
                            first_failure.get_or_insert_with(|| {
                                format!("{failed} {st} batch(es) did not verify")
                            });
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, source_type = %st, "shutdown flush error");
                        first_failure.get_or_insert_with(|| format!("{st} pass failed: {e}"));
                    }
                }
            }
            match first_failure {
                None => Ok(()),
                Some(cause) => Err(cause),
            }
        };
        tokio::select! {
            biased;
            second = shutdown.next_request() => {
                tracing::error!(
                    signal = second,
                    "second shutdown signal during the shutdown flush; abandoning the flush \
                     (unflushed data is recovered or re-read on the next start)"
                );
                Err(VtopError::Other(format!(
                    "shutdown flush abandoned by a second signal ({second})"
                )))
            }
            flushed = flush => match flushed {
                Ok(()) => {
                    tracing::info!("shutdown flush complete; exiting");
                    Ok(())
                }
                Err(cause) => {
                    tracing::error!(
                        %cause,
                        "shutdown flush incomplete; exiting with an error \
                         (unflushed data is recovered or re-read on the next start)"
                    );
                    Err(VtopError::Other(format!("shutdown flush incomplete: {cause}")))
                }
            }
        }
    }
}

/// Where [`Engine::run_until`] learns it has been asked to stop (#526).
///
/// The contract a stop depends on: a request that arrives while nobody is
/// awaiting `next_request` is NOT lost, it resolves the next call (or the
/// next `pending_request`) at once. Requests that pile up before a call may
/// coalesce into one.
#[async_trait::async_trait]
pub trait ShutdownRequests: Send {
    /// Resolve with a short name for what asked (e.g. `"SIGTERM"`) once a
    /// stop is requested. A source that can no longer deliver requests must
    /// park here forever: a closed channel is not an operator's stop.
    async fn next_request(&mut self) -> &'static str;

    /// Take a request that is already pending, without waiting: `None` means
    /// nothing has been asked yet. The same closed-is-not-a-stop rule holds.
    fn pending_request(&mut self) -> Option<&'static str>;

    /// Discard every request already pending, without waiting, so the next
    /// `next_request` resolves only for one sent after this call.
    fn drain_pending(&mut self) {
        while self.pending_request().is_some() {}
    }
}

/// The process's own stop signals: SIGINT and SIGTERM on Unix, Ctrl-C on
/// Windows.
///
/// Each is a persistent tokio signal stream registered once by
/// [`OsShutdownSignals::install`]. A stream records a delivery that lands
/// between polls and yields it on the next `recv`, which is the "not lost
/// while busy" half of the [`ShutdownRequests`] contract. Repeats of ONE kind
/// coalesce inside tokio: its handler only sets a per-signal pending flag,
/// and the driver turns that into a `watch` send, whose receiver reports
/// "changed" once however many sends it missed. SIGINT and SIGTERM are two
/// streams, though, so one of each is two requests; `run_until` drains both
/// when it observes the first.
pub struct OsShutdownSignals {
    #[cfg(unix)]
    interrupt: Option<tokio::signal::unix::Signal>,
    #[cfg(unix)]
    terminate: Option<tokio::signal::unix::Signal>,
    #[cfg(windows)]
    ctrl_c: Option<tokio::signal::windows::CtrlC>,
}

impl OsShutdownSignals {
    /// Register the handlers. One that cannot be registered is logged and
    /// left out rather than failing `run`: that signal keeps its default
    /// disposition, which still stops the process, only without the flush.
    /// `vtop-node` degrades the same way (#280).
    pub fn install() -> Self {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let register = |kind: SignalKind, name: &'static str| match signal(kind) {
                Ok(stream) => Some(stream),
                Err(error) => {
                    tracing::warn!(
                        %error,
                        signal = name,
                        "shutdown handler unavailable; this signal stops the engine without a flush"
                    );
                    None
                }
            };
            Self {
                interrupt: register(SignalKind::interrupt(), "SIGINT"),
                terminate: register(SignalKind::terminate(), "SIGTERM"),
            }
        }
        #[cfg(windows)]
        {
            let ctrl_c = match tokio::signal::windows::ctrl_c() {
                Ok(stream) => Some(stream),
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "Ctrl-C handler unavailable; Ctrl-C stops the engine without a flush"
                    );
                    None
                }
            };
            Self { ctrl_c }
        }
    }
}

#[async_trait::async_trait]
impl ShutdownRequests for OsShutdownSignals {
    async fn next_request(&mut self) -> &'static str {
        #[cfg(unix)]
        {
            // A handler that was never registered, or a stream that reports
            // closed, parks instead of reading as a stop.
            async fn delivered(stream: &mut Option<tokio::signal::unix::Signal>) {
                if let Some(stream) = stream {
                    if stream.recv().await.is_some() {
                        return;
                    }
                }
                std::future::pending().await
            }
            tokio::select! {
                () = delivered(&mut self.interrupt) => "SIGINT",
                () = delivered(&mut self.terminate) => "SIGTERM",
            }
        }
        #[cfg(windows)]
        {
            if let Some(stream) = &mut self.ctrl_c {
                if stream.recv().await.is_some() {
                    return "Ctrl-C";
                }
            }
            std::future::pending().await
        }
    }

    fn pending_request(&mut self) -> Option<&'static str> {
        use futures::FutureExt;
        #[cfg(unix)]
        {
            // One poll with a no-op waker: `recv` is cancel-safe (the stream
            // keeps its notification state), so a `Pending` here loses
            // nothing and a later `recv().await` re-registers a real waker.
            fn delivered_now(stream: &mut Option<tokio::signal::unix::Signal>) -> bool {
                stream
                    .as_mut()
                    .is_some_and(|stream| stream.recv().now_or_never() == Some(Some(())))
            }
            if delivered_now(&mut self.interrupt) {
                return Some("SIGINT");
            }
            if delivered_now(&mut self.terminate) {
                return Some("SIGTERM");
            }
            None
        }
        #[cfg(windows)]
        {
            let delivered = self
                .ctrl_c
                .as_mut()
                .is_some_and(|stream| stream.recv().now_or_never() == Some(Some(())));
            delivered.then_some("Ctrl-C")
        }
    }
}

/// The lowest start position to rewind to, per source unit (one file or spool
/// file at one inode, or one Kafka topic-partition), gathered while recovery
/// scans incomplete batches. See the note at the top of [`Engine::recover`].
///
/// The inode is part of the unit: a path that was rotated has incomplete
/// ranges for the old file and for the current one, and a lower offset in the
/// old file must not hide the current file's range from the plan. The same
/// holds within one inode (#578): content proven to belong to another file -
/// a replacement at the same inode - is its own generation with its own
/// lowest start, so the replacement's rewind is attempted (and lands) even
/// though the old content's lower one is refused. Content that cannot be told
/// apart stays one generation, so one file is never rewound twice.
#[derive(Default)]
struct RewindPlan {
    /// Per unit, one rewind per proven content generation.
    lowest: HashMap<String, Vec<Rewind>>,
    /// The lowest pending start per source (a path or a topic-partition), per
    /// inode and content fingerprint, for [`RewindPlan::holds_back`].
    by_source: HashMap<String, Vec<PendingGroup>>,
    /// The same, for ranges that are HELD and not rewound: their replay
    /// decision could not be recorded (#639). See [`RewindPlan::hold`].
    held: HashMap<String, Vec<PendingGroup>>,
    added: u64,
}

/// The lowest start of one content generation of a unit.
struct Rewind {
    marker: ProgressMarker,
    /// Every fingerprint its ranges carried: what a later range is compared
    /// with to tell whether it is this content or another.
    contents: Vec<ContentFingerprint>,
    /// When it was last added. Recovery visits incomplete rows oldest first,
    /// so a higher number is a more recent file; the adapters keep one cursor
    /// per path, so the rewind applied LAST at a path decides where it
    /// resumes, and that must be the most recent file, not whichever inode
    /// sorts last as text (review of #563).
    recency: u64,
}

impl Rewind {
    fn proven_other(&self, content: Option<&ContentFingerprint>) -> bool {
        self.contents
            .iter()
            .any(|mine| proven_other(Some(mine), content))
    }
}

/// Whether two contents are proven different: fingerprints of the same
/// length with other bytes. Content that cannot be compared (different
/// lengths, or none) is not.
fn proven_other(a: Option<&ContentFingerprint>, b: Option<&ContentFingerprint>) -> bool {
    a.zip(b)
        .is_some_and(|(a, b)| a.same_content(b) == Some(false))
}

/// Which rewind a landing belongs to: the unit, and the content its marker
/// carries when it carries one - distinct per generation of a unit (see
/// [`RewindPlan`]).
fn rewind_key(marker: &ProgressMarker) -> String {
    match marker.content_fingerprint() {
        Some(content) => format!("{}#{}:{}", unit_key(marker), content.len, content.sha256),
        None => unit_key(marker),
    }
}

/// Pending ranges of one source read from one file: one inode, one content
/// fingerprint (#578; `None` for a legacy marker or a file that was empty).
struct PendingGroup {
    inode: Option<u64>,
    content: Option<ContentFingerprint>,
    lowest: i128,
}

impl PendingGroup {
    fn is(&self, marker: &ProgressMarker) -> bool {
        self.inode == marker_inode(marker) && self.content.as_ref() == marker.content_fingerprint()
    }

    /// Whether this group's content is proven to differ from `content`:
    /// fingerprints of the same length with other bytes. Content that cannot
    /// be compared (different lengths, or none) is not proven other.
    fn proven_other(&self, content: Option<&ContentFingerprint>) -> bool {
        proven_other(self.content.as_ref(), content)
    }
}

/// Note that a range starting at `start` is still to be read, in the group of
/// its source, inode and content.
fn note_pending(groups: &mut HashMap<String, Vec<PendingGroup>>, start: &ProgressMarker) {
    let position = start_position(start);
    let groups = groups.entry(source_key(start)).or_default();
    match groups.iter_mut().find(|group| group.is(start)) {
        Some(group) => group.lowest = group.lowest.min(position),
        None => groups.push(PendingGroup {
            inode: marker_inode(start),
            content: start.content_fingerprint().cloned(),
            lowest: position,
        }),
    }
}

impl RewindPlan {
    /// Record that the range starting at `start` is being handed back to its
    /// source. (Only where it starts matters: see [`RewindPlan::holds_back`].)
    fn add(&mut self, start: &ProgressMarker) {
        let position = start_position(start);
        note_pending(&mut self.by_source, start);
        self.added += 1;
        let content = start.content_fingerprint();
        let generations = self.lowest.entry(unit_key(start)).or_default();
        match generations
            .iter_mut()
            .find(|generation| !generation.proven_other(content))
        {
            Some(generation) => {
                generation.recency = self.added;
                if let Some(content) = content {
                    if !generation.contents.contains(content) {
                        generation.contents.push(content.clone());
                    }
                }
                if position < start_position(&generation.marker) {
                    generation.marker = start.clone();
                }
            }
            None => generations.push(Rewind {
                marker: start.clone(),
                contents: content.into_iter().cloned().collect(),
                recency: self.added,
            }),
        }
    }

    /// Record that the range starting at `start` must still be re-read but is
    /// NOT handed back to its source now: recovery could not record the
    /// decision to replay it, and never rewinds on a decision the ledger did
    /// not record (#123). The row stays as it was and the next recovery
    /// decides it again.
    ///
    /// It holds back a VERIFIED range above it all the same (#639). Left out
    /// of the plan, it let that range commit; the commit moved the unit's
    /// committed point past it, the next recovery's rewind for it was clamped
    /// to that point, and the range was never archived.
    ///
    /// It produces no rewind ([`RewindPlan::markers`]), and [`RewindPlan::rebase`]
    /// does not move it: no rewind lands for it, so a landing at its file
    /// (another range's) says nothing about whether its bytes are archived.
    fn hold(&mut self, start: &ProgressMarker) {
        note_pending(&mut self.held, start);
    }

    /// Replace each unit's raw start with the position its rewind actually
    /// landed on (`effective`, by [`rewind_key`] of the rewind's marker). A rewind never goes below what the
    /// source has already committed, so a stale ledger row whose range a later
    /// batch re-archived lands AT the committed point, not at its own start:
    /// nothing is about to be re-read for it, and it must not hold back a
    /// VERIFIED range above it (review of #563). A unit with no entry keeps its
    /// raw start, which is the conservative answer.
    fn rebase(&mut self, effective: &HashMap<String, i128>) {
        for rewind in self.lowest.values().flatten() {
            let marker = &rewind.marker;
            let Some(&landed) = effective.get(&rewind_key(marker)) else {
                continue;
            };
            // Every group of THAT file moves (review of #578): a file under
            // one page fingerprints a different length at each read, so one
            // file's ranges can sit in several groups, which were one before
            // groups carried content. A group proven to be other content -
            // a replacement at the same inode - does not: the landing says
            // nothing about it.
            let inode = marker_inode(marker);
            for group in self
                .by_source
                .get_mut(&source_key(marker))
                .into_iter()
                .flatten()
                .filter(|group| {
                    group.inode == inode && !rewind.proven_other(group.content.as_ref())
                })
            {
                group.lowest = group.lowest.max(landed);
            }
        }
    }

    /// Whether committing the VERIFIED range starting at `start` would strand
    /// bytes that are about to be re-read.
    ///
    /// The file adapters never rewind below their committed point, so a commit
    /// moves a source unit's frontier to the end of the committed range and
    /// everything below it is treated as archived. A VERIFIED range archives
    /// exactly `start..end`. Pending bytes at or above its end stay above the
    /// frontier and are still re-read; pending bytes inside it are archived by
    /// it. The only bytes it can strand are pending ones BELOW its start: so
    /// the commit is held back exactly when a range of the same unit that
    /// starts below it is about to be re-read.
    ///
    /// A VERIFIED range that replaces a stale row for the same bytes starts
    /// where the pending range starts, so it commits (the clamped rewind then
    /// correctly does nothing).
    ///
    /// A marker with no inode (written before inodes were recorded, or on a
    /// platform without them) cannot be told apart from any file at its path,
    /// so it is compared against every inode's pending ranges, and a pending
    /// range with no inode counts against every VERIFIED range at the path.
    ///
    /// Nor does a pending range of OTHER CONTENT at the same inode (#578): a
    /// file replaced at the same inode by the one the VERIFIED range was read
    /// from. The adapter refuses to rewind the replacement to it, so it is
    /// never re-read and cannot be stranded; holding back for it kept the
    /// replacement's VERIFIED range uncommitted on every restart (review of
    /// #578). "Other" means proven: fingerprints of the same length that
    /// differ. Content that cannot be compared - different lengths, or none -
    /// holds back as before.
    ///
    /// A HELD range ([`RewindPlan::hold`]) counts exactly like one being
    /// re-read, with the same inode and content rules.
    fn holds_back(&self, start: &ProgressMarker) -> bool {
        let from = start_position(start);
        let inode = marker_inode(start);
        let content = start.content_fingerprint();
        [&self.by_source, &self.held].into_iter().any(|pending| {
            pending.get(&source_key(start)).is_some_and(|groups| {
                groups
                    .iter()
                    .filter(|group| {
                        inode.is_none() || group.inode.is_none() || group.inode == inode
                    })
                    .filter(|group| !group.proven_other(content))
                    .any(|group| group.lowest < from)
            })
        })
    }

    fn markers(&self) -> Vec<ProgressMarker> {
        let mut rewinds: Vec<&Rewind> = self.lowest.values().flatten().collect();
        // Grouped by source, and within a source oldest file first so the most
        // recent file's rewind is applied last. The rewind key breaks ties, so
        // the log and the tests do not depend on hash order.
        rewinds.sort_by_cached_key(|rewind| {
            (
                source_key(&rewind.marker),
                rewind.recency,
                rewind_key(&rewind.marker),
            )
        });
        rewinds
            .into_iter()
            .map(|rewind| rewind.marker.clone())
            .collect()
    }
}

/// The source a marker belongs to, ignoring which file at that path it was
/// read from: a path, or a Kafka topic-partition.
fn source_key(marker: &ProgressMarker) -> String {
    match marker {
        ProgressMarker::Kafka {
            topic,
            partition,
            consumer_group,
            ..
        } => format!("kafka|{consumer_group}|{topic}#p{partition}"),
        ProgressMarker::File { path, .. } => format!("file|{path}"),
        ProgressMarker::SyslogSpool { spool_id, path, .. } => format!("spool|{spool_id}|{path}"),
    }
}

/// The inode a marker was read from, where it records one.
fn marker_inode(marker: &ProgressMarker) -> Option<u64> {
    match marker {
        ProgressMarker::Kafka { .. } => None,
        ProgressMarker::File { inode, .. } | ProgressMarker::SyslogSpool { inode, .. } => *inode,
    }
}

/// The source unit a marker belongs to: the same for a batch's start and end
/// markers, and for every batch read from the same file at the same inode.
fn unit_key(marker: &ProgressMarker) -> String {
    match marker_inode(marker) {
        Some(inode) => format!("{}|{inode}", source_key(marker)),
        None => format!("{}|?", source_key(marker)),
    }
}

/// A VERIFIED row this process left uncommitted (#624); see
/// [`Engine::settle_verified`].
#[derive(Debug, Clone)]
struct Unsettled {
    batch_id: String,
    source_type: SourceType,
    /// Held back by recovery above a range being read again, rather than
    /// left by a source commit that failed.
    held_back: bool,
    /// The adapter's committed point for the row when recovery held it back:
    /// the restart seed. Settling waits for the point to move past it
    /// (review of #624).
    committed_at_hold: Option<i128>,
    /// Whether settling it counts its verification too: a row recovery left
    /// was verified by the process that crashed, and this one has not counted
    /// it; a pipeline row's verification is counted already.
    count_verified: bool,
    /// What recovery's storage re-check of the row concluded (#651), for a
    /// row recovery left. A size-only verification is counted when the row
    /// commits, as `commit_recovered` counts it, never at the hold - by the
    /// class the ledger holds for the row, and by this only for a row with
    /// none (#664).
    recheck: Option<Recheck>,
}

impl Unsettled {
    fn held_back(rec: &BatchRecord, committed_at_hold: Option<i128>, recheck: Recheck) -> Self {
        Self {
            batch_id: rec.batch_id.clone(),
            source_type: rec.source_type.clone(),
            held_back: true,
            committed_at_hold,
            count_verified: true,
            recheck: Some(recheck),
        }
    }

    /// Recovery re-checked the row and its source commit failed.
    fn recovered_commit_failed(rec: &BatchRecord, recheck: Recheck) -> Self {
        Self {
            batch_id: rec.batch_id.clone(),
            source_type: rec.source_type.clone(),
            held_back: false,
            committed_at_hold: None,
            count_verified: true,
            recheck: Some(recheck),
        }
    }

    /// The pipeline verified the row in this process and its commit failed.
    fn commit_failed(batch_id: &str, source_type: &SourceType) -> Self {
        Self {
            batch_id: batch_id.to_owned(),
            source_type: source_type.clone(),
            held_back: false,
            committed_at_hold: None,
            count_verified: false,
            recheck: None,
        }
    }
}

/// Whether a VERIFIED row recovery held back may settle now (#624): its
/// source's committed point is at or past the row's end, AND has moved up
/// since the hold. At the hold that point is the restart seed; recovery holds
/// the row back because a range below it is to be read again - or is held
/// with no rewind at all (#639) - and the seed alone says nothing about
/// whether that has happened (review of #624). Only a point this process
/// moved does. A point that went down (a rotation restarted the cursor) is
/// not that either.
fn held_row_may_settle(committed: Option<i128>, at_hold: Option<i128>, end: i128) -> bool {
    committed.is_some_and(|now| now >= end && at_hold.is_none_or(|held| now > held))
}

/// One past where a range ends within its unit, in the unit
/// `SourceAdapter::committed_through` answers in: a byte offset, or for
/// Kafka the offset after the last one read (`end_offset` is inclusive).
fn end_position(marker: &ProgressMarker) -> i128 {
    match marker {
        ProgressMarker::Kafka { end_offset, .. } => i128::from(*end_offset) + 1,
        ProgressMarker::File { end_byte, .. } | ProgressMarker::SyslogSpool { end_byte, .. } => {
            i128::from(*end_byte)
        }
    }
}

/// Where a range begins within its unit.
fn start_position(marker: &ProgressMarker) -> i128 {
    match marker {
        ProgressMarker::Kafka { start_offset, .. } => i128::from(*start_offset),
        ProgressMarker::File { start_byte, .. }
        | ProgressMarker::SyslogSpool { start_byte, .. } => i128::from(*start_byte),
    }
}

/// The longest the loop waits before reading again after cycles in which a
/// batch failed.
const FAILURE_BACKOFF_CAP: Duration = Duration::from_secs(60);

/// How long to wait after the `streak`-th consecutive cycle that failed a
/// batch: the idle interval, doubling each time and capped, less up to a
/// quarter of that so that engines which failed together do not retry together.
///
/// The jitter is SUBTRACTED, not added: added past the cap it would be cut off
/// by the cap, and a long outage — exactly when many engines are retrying —
/// would put them all back in lockstep at the ceiling.
///
/// Before failed ranges were re-read, a failing store was answered by draining
/// the source into FAILED batches at read speed. Now the same range is read
/// again, so without a wait a persistent outage would be hammered in a tight
/// loop (#552). `seed` supplies the jitter; callers pass something that
/// varies between processes (the clock's sub-second part).
fn failure_backoff(base: Duration, streak: u32, seed: u64) -> Duration {
    let doublings = streak.saturating_sub(1).min(16);
    let wait = base
        .checked_mul(1u32 << doublings)
        .unwrap_or(FAILURE_BACKOFF_CAP)
        .min(FAILURE_BACKOFF_CAP);
    let spread = u64::try_from(wait.as_millis() / 4).unwrap_or(0);
    let jitter = if spread == 0 { 0 } else { seed % (spread + 1) };
    wait - Duration::from_millis(jitter)
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

/// The range a drained window's object holds, for its batch id and manifest
/// (#622): the first read's start to the last read's end. Each read's end
/// marker carries that read's own start, so `progress_end` alone named only
/// the last read, and the manifest of a batch of several reads claimed less
/// than its object held. `progress_end` itself stays the commit marker and
/// the ledger's end: the file adapter settles a commit by the exact range of
/// the read it issued, and a span would match none of them.
fn window_marker(read: &ReadResult) -> ProgressMarker {
    ProgressMarker::spanning(&read.progress_start, &read.progress_end)
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

/// What the file adapter needs of a committed row to judge which file it was
/// read from (#621): its inode, end, first page and the page below its end.
fn archived_end(row: &vtop_state::CommittedCursor) -> ArchivedEnd {
    ArchivedEnd {
        inode: row.inode,
        end_byte: row.end_byte,
        content: row.content_fingerprint.clone(),
        tail: row.tail_fingerprint.clone(),
    }
}

/// The tenant a source's per-source metrics are charged to: the stream
/// override when the source name matches a configured stream, the engine
/// default otherwise — the same resolution the pipeline applies to batches.
/// Shared by `source_read_errors_total` and `retention_lost_records_total` so
/// a source's failures and its losses land beside its batches instead of
/// always on the engine default.
fn tenant_for_source(streams: &StreamsConfig, default_tenant: &str, source_name: &str) -> String {
    streams
        .lookup(source_name)
        .map(|s| s.tenant.clone())
        .unwrap_or_else(|| default_tenant.to_string())
}

/// Every `(tenant, source_type)` pair the engine can charge a read error or a
/// retention loss to gets its counter children at zero before the first read
/// (#454). A counter child that first appears already at N has no earlier
/// sample, so `increase()` and `rate()` over it are empty for that first — and,
/// for a one-off loss, only — step: the alert on retention loss would never
/// fire for a fresh deployment's first incident. Primed, the first loss is a
/// step from zero. The pairs are the ones `tenant_for_source` can resolve to:
/// the engine default and each configured stream's tenant, per enabled source.
fn prime_source_counters<'a>(
    config: &VtopConfig,
    streams: &StreamsConfig,
    source_types: impl Iterator<Item = &'a SourceType>,
) {
    let Some(mx) = telemetry::metrics() else {
        return;
    };
    for source_type in source_types {
        let mut tenants: Vec<&str> = vec![config.engine.tenant.as_str()];
        tenants.extend(
            streams
                .streams
                .iter()
                .filter(|s| s.source_type == *source_type)
                .map(|s| s.tenant.as_str()),
        );
        tenants.sort_unstable();
        tenants.dedup();
        for tenant in tenants {
            let labels = [tenant, source_type.as_str()];
            let _ = mx.source_read_errors_total.with_label_values(&labels);
            let _ = mx.retention_lost_records_total.with_label_values(&labels);
            let _ = mx.source_replay_refused_total.with_label_values(&labels);
        }
    }
}

/// The multipart counters' children at zero before the first upload (#668),
/// for the reason [`prime_source_counters`] gives: the alert on sessions kept
/// after failures compares them with `resumed` and `swept`, and a child that
/// does not exist yet compares as nothing rather than as zero.
fn prime_multipart_counters() {
    let Some(mx) = telemetry::metrics() else {
        return;
    };
    for result in telemetry::MULTIPART_PART_RESULTS {
        let _ = mx.multipart_parts_total.with_label_values(&[result]);
    }
    for outcome in telemetry::MULTIPART_SESSION_OUTCOMES {
        let _ = mx.multipart_sessions_total.with_label_values(&[outcome]);
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
    /// Handed-back ranges whose source had already committed past them,
    /// retired as SUPERSEDED instead of rewound (#567).
    pub superseded: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::file_config;
    use std::io::Write;
    use vtop_core::config::StreamsConfig;

    /// #559: a buffered window spans several reads; its event-time range is
    /// the earliest and latest of theirs, not the first read's start and the
    /// last read's end (producer timestamps arrive out of order).
    #[test]
    fn a_buffered_window_keeps_the_earliest_and_latest_record_time() {
        let source = DiscoveredSource {
            source_type: SourceType::Kafka,
            source_name: "t".into(),
            format: vtop_core::types::TelemetryFormat::Raw,
        };
        let mut buf = PendingBuffer::new("default", source, BatchLimits::default());
        let marker = |offset: i64| ProgressMarker::Kafka {
            topic: "t".into(),
            partition: 0,
            start_offset: offset,
            end_offset: offset,
            consumer_group: "g".into(),
        };
        let read = |offset: i64, lo: Option<&str>, hi: Option<&str>| ReadResult {
            records: vec![b"r".to_vec()],
            progress_start: marker(offset),
            progress_end: marker(offset),
            first_timestamp: lo.map(str::to_owned),
            last_timestamp: hi.map(str::to_owned),
            verbatim: false,
        };
        buf.append(read(
            0,
            Some("2026-01-01T10:00:00.000Z"),
            Some("2026-01-01T10:30:00.000Z"),
        ));
        buf.append(read(
            1,
            Some("2026-01-01T09:00:00.000Z"),
            Some("2026-01-01T09:05:00.000Z"),
        ));
        buf.append(read(2, None, None));
        let drained = buf.drain().unwrap();
        assert_eq!(
            drained.first_timestamp.as_deref(),
            Some("2026-01-01T09:00:00.000Z")
        );
        assert_eq!(
            drained.last_timestamp.as_deref(),
            Some("2026-01-01T10:30:00.000Z")
        );
    }

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
            content_fingerprint: None,
            tail_fingerprint: None,
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

    /// The retention counter resolves each entry's tenant through the same
    /// stream lookup the pipeline uses (#361): the loss arrives named by
    /// topic, the topic names the stream, and the stream names the tenant.
    #[test]
    fn a_retention_loss_on_an_overridden_stream_is_charged_to_that_streams_tenant() {
        let streams = StreamsConfig {
            streams: vec![StreamConfig {
                configured_fields: Default::default(),
                source_name: "app_events".into(),
                source_type: SourceType::Kafka,
                format: vtop_core::types::TelemetryFormat::Raw,
                tenant: "acme".into(),
                s3_source_name: None,
                retention_class: None,
                checksum: None,
            }],
        };
        assert_eq!(
            tenant_for_source(&streams, "default", "app_events"),
            "acme",
            "the topic names the stream and the stream names the tenant — \
             landing this loss on the engine default would report acme's \
             lost records against a tenant that never owned them"
        );
        assert_eq!(
            tenant_for_source(&streams, "default", "unconfigured_topic"),
            "default",
            "a source with no stream override falls back to the engine \
             default, exactly where its batches land"
        );
    }

    #[tokio::test]
    async fn max_concurrency_tuning_narrows_the_upload_width() {
        // The active transport's max_concurrency actually bounds runtime object
        // concurrency (#480), not just the summary: with the default batch
        // ceiling of 8, a tuning of 2 resolves the fixed width to 2, and no
        // tuning leaves it at 8. This pins that resolved_max_concurrency is
        // WIRED into the width, closing the "recorded but not applied" gap.
        // A mock backend does NOT route through the EgressTransport seam, so a
        // stray transport tuning block must not narrow its width — it stays at
        // the batch ceiling of 8. (The s3_native narrowing itself is covered by
        // the pure resolved_upload_ceiling test in vtop-core.) This pins that
        // the engine reads the backend-gated resolved_upload_ceiling.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.log");
        std::fs::write(&input, "one line\n").unwrap();
        let paths = vec![input.to_string_lossy().into_owned()];

        let mut mock_with_stray_tuning = file_config(
            dir.path().join("w1").to_str().unwrap(),
            "sqlite::memory:",
            paths,
            "mock",
        );
        mock_with_stray_tuning.upload.transports.insert(
            "tcp_tls".into(),
            vtop_core::config::EgressTuning {
                max_concurrency: Some(2),
                ..Default::default()
            },
        );
        let engine = Engine::new(mock_with_stray_tuning, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        assert_eq!(
            engine.upload_width(),
            8,
            "a non-s3_native backend must not be narrowed by a transport tuning block"
        );
    }

    /// Every batch commits off the back of ITS OWN verification, with several
    /// of them admitted to the pipeline at once (#87).
    ///
    /// WHAT THIS DOES NOT PIN, stated because the test's name could be read
    /// as claiming it: this does not prove any work OVERLAPS. Every assertion
    /// below is about final outcomes, so a runtime that processed the five
    /// batches strictly serially would pass it unchanged (review). Proving
    /// overlap needs an instrumented backend that records peak in-flight work
    /// — a barrier or an atomic counter the pipeline increments — which the
    /// mock backend does not offer today.
    ///
    /// What it does pin is the property concurrency was allowed to cost, and
    /// that is worth its own test: with several batches in flight, each must
    /// reach SOURCE_COMMITTED off the back of its own verify, and none may be
    /// left verified-but-uncommitted. A batch in that state is the wedge the
    /// flush loop documents at length: the next cycle rebuilds the same
    /// batch_id, the ledger refuses the duplicate, and
    /// the source stops until a restart runs recovery.
    #[tokio::test]
    async fn concurrent_batches_each_commit_after_their_own_verify() {
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");

        // One file per source, so the flush loop sees several keys and the
        // concurrent path is the one under test rather than a single batch
        // taking the same code path trivially.
        let mut paths = Vec::new();
        for i in 0..5 {
            let input = dir.path().join(format!("in-{i}.log"));
            let mut f = std::fs::File::create(&input).unwrap();
            for line in 0..3 {
                writeln!(f, "source {i} line {line}").unwrap();
            }
            paths.push(input.to_string_lossy().into_owned());
        }

        let mut cfg = file_config(work.to_str().unwrap(), "sqlite::memory:", paths, "mock");
        // Above one, or the test would prove nothing about concurrency; below
        // the batch count, so the limit is actually exercised as a limit
        // rather than admitting everything at once.
        cfg.batching.max_concurrent_batches = 3;
        cfg.batching.max_batch_age_seconds = 3_600;

        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let flushed = engine.run_source(SourceType::File, true).await.unwrap();

        assert_eq!(
            flushed.len(),
            5,
            "every source must produce its batch: a concurrency limit bounds \
             how many run at once, never how many run"
        );
        for outcome in &flushed {
            assert!(
                outcome.committed,
                "batch {} verified but did not commit — a verified-but-uncommitted \
                 batch wedges its source until recovery runs",
                outcome.batch_id
            );
            assert_eq!(outcome.final_state, BatchState::SourceCommitted);
            assert_eq!(outcome.record_count, 3);
        }

        let ids: std::collections::BTreeSet<_> =
            flushed.iter().map(|o| o.batch_id.clone()).collect();
        assert_eq!(
            ids.len(),
            5,
            "each batch keeps its own identity; a shared batch_id would collide \
             in the ledger the moment two of them committed"
        );
    }

    /// Review on #570: a batch waits for an upload slot (and its first ledger
    /// write) after it leaves the buffer, and that wait can cross an hour. The
    /// partition is the hour the batch was drained in - the instant the caller
    /// passes - not the hour processing reached it.
    #[tokio::test]
    async fn a_batch_is_filed_under_the_hour_it_was_drained_not_processed() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.log");
        std::fs::write(&input, "one\n").unwrap();
        let cfg = file_config(
            dir.path().join("work").to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        let engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let source = DiscoveredSource {
            source_type: SourceType::File,
            source_name: "in".into(),
            format: vtop_core::types::TelemetryFormat::Raw,
        };
        let marker = ProgressMarker::File {
            path: input.to_string_lossy().into_owned(),
            inode: None,
            start_byte: 0,
            end_byte: 4,
            file_size: 4,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: None,
        };
        let read = ReadResult {
            records: vec![b"one".to_vec()],
            progress_start: marker.clone(),
            progress_end: marker,
            first_timestamp: None,
            last_timestamp: None,
            verbatim: false,
        };
        let drained = chrono::DateTime::parse_from_rfc3339("2020-01-02T03:59:59.900Z")
            .unwrap()
            .with_timezone(&Utc);
        let step = engine
            .pipeline()
            .process_until_verified(&source, read, None, drained)
            .await
            .unwrap();
        let VerifyStep::Verified(verified) = step else {
            panic!("the batch did not verify");
        };
        assert!(
            verified
                .object_uri
                .contains("year=2020/month=01/day=02/hour=03"),
            "filed under the hour processing reached it, not the hour it was \
             drained in (2020-01-02 03:59:59.900): {}",
            verified.object_uri
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

    /// #102: a throttle is counted apart from other upload failures, and the
    /// batch fails without committing exactly as any failed upload does — the
    /// classification changes what an operator sees, not what the engine
    /// does with the batch.
    /// The throttle counter is process-wide, so tests that throttle the mock
    /// and read deltas of it run one at a time.
    static THROTTLE_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// A counter whose first sample is already the loss shows no increase
    /// (#454, review): building the engine primes both per-source counters at
    /// zero for every tenant/source pair it can charge, read back here through
    /// the registry as a scrape would.
    #[tokio::test]
    async fn source_counters_are_primed_at_zero_before_the_first_read() {
        let mx = telemetry::init().unwrap();
        fn primed(family: &str, tenant: &str) -> Option<u64> {
            let mx = telemetry::init().unwrap();
            mx.registry
                .gather()
                .iter()
                .filter(|f| f.name().ends_with(family))
                .flat_map(|f| f.get_metric().iter())
                .find(|m| {
                    let labels = m.get_label();
                    labels
                        .iter()
                        .any(|l| l.name() == "tenant" && l.value() == tenant)
                        && labels
                            .iter()
                            .any(|l| l.name() == "source_type" && l.value() == "file")
                })
                .map(|m| m.get_counter().get_value() as u64)
        }
        let _ = mx;
        assert!(
            primed("retention_lost_records_total", "primed-default").is_none(),
            "nothing is charged before an engine exists"
        );

        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        let input = dir.path().join("in.log");
        std::fs::write(&input, "a line\n").unwrap();
        let mut cfg = file_config(
            work.to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.engine.tenant = "primed-default".into();
        let streams = StreamsConfig { streams: vec![] };
        let _engine = Engine::new(cfg, streams).await.unwrap();

        assert_eq!(
            primed("retention_lost_records_total", "primed-default"),
            Some(0),
            "the default tenant's loss counter exists at zero before any read"
        );
        assert_eq!(
            primed("source_read_errors_total", "primed-default"),
            Some(0),
            "and so does its read-error counter"
        );
    }

    #[tokio::test]
    async fn a_throttled_upload_is_counted_apart_and_the_batch_is_not_committed() {
        let _serial = THROTTLE_TESTS.lock().await;
        // Read back through the registry, as a scrape would: the family
        // name is the exported one and the stage is the label.
        fn throttled_so_far(stage: &str) -> u64 {
            let mx = telemetry::init().unwrap();
            mx.registry
                .gather()
                .iter()
                .filter(|family| family.name().ends_with("upload_throttled_total"))
                .flat_map(|family| family.get_metric().iter())
                .filter(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "stage" && label.value() == stage)
                })
                .map(|metric| metric.get_counter().get_value() as u64)
                .sum()
        }

        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        let input = dir.path().join("in.log");
        std::fs::write(&input, "a line\n").unwrap();
        let mut cfg = file_config(
            work.to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.max_batch_age_seconds = 3_600;
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let mock = Arc::new(vtop_upload::MockBackend::new().with_throttled_puts(1));
        engine.backend = mock.clone();

        let object_before = throttled_so_far("object_upload");
        let manifest_before = throttled_so_far("manifest_upload");
        let outcomes = engine.run_source(SourceType::File, true).await.unwrap();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(
            outcomes[0].final_state,
            BatchState::Failed,
            "a throttled put fails the batch the way any failed put does"
        );
        assert!(
            !outcomes[0].committed,
            "nothing verified, nothing committed"
        );
        assert_eq!(
            throttled_so_far("object_upload") - object_before,
            1,
            "the throttle is counted apart, on the stage that saw it"
        );
        assert_eq!(
            throttled_so_far("manifest_upload") - manifest_before,
            0,
            "the manifest was never attempted"
        );

        // The manifest stage has its own accounting, so it needs its own
        // throttle (review): the object put lands, the manifest put is
        // refused, and only `manifest_upload` moves. Same test function on
        // purpose — the registry is process-wide, and two tests reading
        // deltas of the same counter in parallel would race each other.
        let input = dir.path().join("in-manifest.log");
        std::fs::write(&input, "another line\n").unwrap();
        let mut cfg = file_config(
            dir.path().join("work-manifest").to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.max_batch_age_seconds = 3_600;
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        engine.backend = Arc::new(vtop_upload::MockBackend::new().with_throttled_manifest_puts(1));
        let object_before = throttled_so_far("object_upload");
        let manifest_before = throttled_so_far("manifest_upload");
        let outcomes = engine.run_source(SourceType::File, true).await.unwrap();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].final_state, BatchState::Failed);
        assert!(!outcomes[0].committed);
        assert_eq!(
            throttled_so_far("object_upload") - object_before,
            0,
            "the object put landed"
        );
        assert_eq!(
            throttled_so_far("manifest_upload") - manifest_before,
            1,
            "the manifest stage counts its own throttle"
        );
    }

    /// The width controller alone (#102): halves on a throttle, never below
    /// the floor; grows by one on a clean cycle, never above the ceiling;
    /// and recovers all the way back once the store goes quiet.
    #[test]
    fn a_store_request_is_an_attempt_and_a_throttled_one_is_a_throttle() {
        let attempts = AtomicU64::new(0);
        let throttles = AtomicU64::new(0);
        let labels = ["t", "file", "csv"];
        assert!(note_store_request(Ok(()), "ensure_bucket", labels, &attempts, &throttles).is_ok());
        assert!(note_store_request::<()>(
            Err(VtopError::UploadThrottled("slow down".into())),
            "bucket_versioning",
            labels,
            &attempts,
            &throttles
        )
        .is_err());
        assert!(note_store_request::<()>(
            Err(VtopError::Config("not a throttle".into())),
            "manifest_readback",
            labels,
            &attempts,
            &throttles
        )
        .is_err());
        assert_eq!(
            attempts.load(Ordering::Relaxed),
            3,
            "every request is an attempt"
        );
        assert_eq!(
            throttles.load(Ordering::Relaxed),
            1,
            "only the throttled one is a throttle"
        );
    }

    #[test]
    fn the_width_controller_halves_on_throttle_and_climbs_back_to_the_ceiling() {
        let mut width = WidthController::new(8, 1);
        assert_eq!(width.width(), 8, "starts at the ceiling");
        assert_eq!(width.observe_cycle(true), 4);
        assert_eq!(width.observe_cycle(true), 2);
        assert_eq!(width.observe_cycle(true), 1);
        assert_eq!(width.observe_cycle(true), 1, "the floor holds");
        let climb: Vec<usize> = (0..9).map(|_| width.observe_cycle(false)).collect();
        assert_eq!(
            climb,
            vec![2, 3, 4, 5, 6, 7, 8, 8, 8],
            "one per clean cycle, then the ceiling"
        );
        // A ceiling at the top of the type (review): climbing must saturate.
        let mut unbounded = WidthController::new(usize::MAX, 1);
        assert_eq!(unbounded.observe_cycle(false), usize::MAX);

        let mut floored = WidthController::new(8, 3);
        assert_eq!(floored.observe_cycle(true), 4);
        assert_eq!(floored.observe_cycle(true), 3, "a floor of three");
        assert_eq!(
            WidthController::new(0, 0).width(),
            1,
            "a ceiling of zero is one"
        );
        assert_eq!(
            WidthController::new(2, 9).width(),
            2,
            "a floor above the ceiling is the ceiling"
        );
    }

    #[tokio::test]
    async fn an_engine_configured_without_either_ceiling_builds_todays_controller() {
        // The default is unset (#481), asserted on the constructed engine
        // rather than on the parts: the controller it holds is the one
        // `WidthController::new` built before the operator ceiling existed,
        // and the fixed width is the resolved batch ceiling. (That no shaper is
        // in the upload path is pinned beside the shaper, in vtop-upload.)
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.log");
        std::fs::write(&input, "one line\n").unwrap();
        let mut cfg = file_config(
            dir.path().join("work").to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.max_concurrent_batches = 12;
        cfg.batching.adaptive_width.min_width = 3;
        assert_eq!(cfg.upload.max_concurrency_ceiling, None);
        assert_eq!(cfg.upload.max_egress_bytes_per_second, None);
        let engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        assert_eq!(
            engine.width,
            WidthController::new(12, 3),
            "no operator ceiling must mean no clamp on the controller"
        );
        assert_eq!(engine.upload_width(), 12);
    }

    #[test]
    fn the_operator_ceiling_clamps_the_controllers_ceiling_and_its_floor() {
        // No operator ceiling is exactly today's controller (#481).
        assert_eq!(
            WidthController::under_operator_ceiling(8, 3, None),
            WidthController::new(8, 3),
            "an unset ceiling must construct the controller it always did"
        );
        // A ceiling below the configured one wins, and climbing stops there.
        let mut width = WidthController::under_operator_ceiling(8, 1, Some(5));
        assert_eq!(
            width.width(),
            5,
            "the width starts at the OPERATOR's ceiling"
        );
        assert_eq!(width.observe_cycle(true), 2);
        let climb: Vec<usize> = (0..5).map(|_| width.observe_cycle(false)).collect();
        assert_eq!(
            climb,
            vec![3, 4, 5, 5, 5],
            "clean cycles climb to the operator ceiling, never past it"
        );
        // The floor cannot defeat it: a floor of 6 under a ceiling of 4 is 4,
        // so even a store that throttles every cycle leaves the width at 4.
        let mut floored = WidthController::under_operator_ceiling(8, 6, Some(4));
        assert_eq!(floored.observe_cycle(true), 4);
        assert_eq!(
            floored.observe_cycle(false),
            4,
            "a floor above the operator ceiling must not hold the width above it"
        );
        // An operator ceiling ABOVE the configured one changes nothing: it is
        // a maximum, not a target.
        assert_eq!(
            WidthController::under_operator_ceiling(8, 1, Some(32)),
            WidthController::new(8, 1)
        );
    }

    /// The controller in the engine (#102): a throttled cycle halves the
    /// next cycle's width, clean cycles climb it back to the ceiling, and
    /// with the knob off the width is the ceiling whatever the store said.
    #[tokio::test]
    async fn the_engine_backs_its_upload_width_off_a_throttle_and_recovers() {
        let _serial = THROTTLE_TESTS.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        let input = dir.path().join("in.log");
        std::fs::write(&input, "line 1\n").unwrap();
        let mut cfg = file_config(
            work.to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.max_batch_age_seconds = 3_600;
        cfg.batching.max_concurrent_batches = 4;
        cfg.batching.adaptive_width.enabled = true;
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let mock = Arc::new(vtop_upload::MockBackend::new().with_throttled_puts(1));
        engine.backend = mock.clone();
        assert_eq!(
            engine.upload_width(),
            4,
            "the ceiling, until the store says otherwise"
        );

        let append = |n: u32| {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&input)
                .unwrap();
            writeln!(file, "line {n}").unwrap();
        };
        // Cycle 1: the one throttled put — the width halves for the next.
        let outcomes = engine.run_source(SourceType::File, true).await.unwrap();
        assert_eq!(outcomes[0].final_state, BatchState::Failed);
        assert_eq!(engine.upload_width(), 2, "halved after a throttled cycle");
        // The failed range is handed back to the source (#552), so the next
        // cycle is NOT idle: it re-reads line 1, reaches the store, and its
        // clean verdict walks the width back up. This used to assert an idle
        // cycle here — "nothing to flush" — which was the bug: line 1 had
        // been read, failed, and would never be read again.
        let retry = engine.run_source(SourceType::File, true).await.unwrap();
        assert!(
            retry[0].committed,
            "the failed range is re-read and now commits"
        );
        assert_eq!(engine.upload_width(), 3, "a clean retry climbs one");
        // A genuinely idle cycle probes nothing and moves nothing (review).
        let idle = engine.run_source(SourceType::File, true).await.unwrap();
        assert!(idle.is_empty(), "nothing to flush");
        assert_eq!(
            engine.upload_width(),
            3,
            "idle polling must not walk the width back up"
        );
        // Clean cycles climb back one at a time, and stop at the ceiling.
        for (n, want) in [(2, 4), (3, 4), (4, 4)] {
            append(n);
            let outcomes = engine.run_source(SourceType::File, true).await.unwrap();
            assert!(outcomes[0].committed, "a clean cycle commits");
            assert_eq!(engine.upload_width(), want, "after clean cycle {n}");
        }

        // The knob off: the ceiling, whatever the store said.
        let mut cfg = file_config(
            dir.path().join("work-fixed").to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.max_batch_age_seconds = 3_600;
        cfg.batching.max_concurrent_batches = 4;
        let mut fixed = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        fixed.backend = Arc::new(vtop_upload::MockBackend::new().with_throttled_puts(1));
        let _ = fixed.run_source(SourceType::File, true).await.unwrap();
        assert_eq!(
            fixed.upload_width(),
            4,
            "with the knob off the width is the configured one"
        );
    }

    /// #552: a batch that fails must hand its range back to the source. The
    /// adapters advance their read position when they READ, so without the
    /// rewind the next cycle read on from the end of the failed range and the
    /// first later batch to verify committed past it: the failed bytes were
    /// never archived, and no restart repaired that because the file adapter
    /// clamps a replay to the committed point.
    #[tokio::test]
    async fn a_failed_range_is_read_again_and_a_later_range_cannot_commit_past_it() {
        // A throttled put moves a process-wide counter that another test reads
        // as a delta, so the throttle tests take turns.
        let _serial = THROTTLE_TESTS.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.log");
        std::fs::write(&input, "line 1\n").unwrap();
        let mut cfg = file_config(
            dir.path().join("work").to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.max_batch_age_seconds = 3_600;
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        // The store refuses exactly one put, then recovers.
        engine.backend = Arc::new(vtop_upload::MockBackend::new().with_throttled_puts(1));

        let first = engine.run_source(SourceType::File, true).await.unwrap();
        assert_eq!(first[0].final_state, BatchState::Failed);

        // The file keeps growing while the store is unhappy.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&input)
                .unwrap();
            writeln!(f, "line 2").unwrap();
        }
        let second = engine.run_source(SourceType::File, true).await.unwrap();
        assert!(second[0].committed, "the retry commits");

        let committed: Vec<_> = engine
            .store
            .list_batches()
            .await
            .unwrap()
            .into_iter()
            .filter(|b| b.state == BatchState::SourceCommitted)
            .collect();
        assert_eq!(committed.len(), 1, "one batch committed: {committed:?}");
        let ProgressMarker::File { start_byte, .. } = &committed[0].progress_start else {
            panic!("a file batch carries a file marker");
        };
        assert_eq!(
            *start_byte, 0,
            "the committed batch must start at byte 0: the failed range [0, 7) was \
             handed back, not skipped"
        );
        assert_eq!(
            committed[0].record_count,
            Some(2),
            "both lines are archived"
        );
        let ProgressMarker::File { end_byte: tail, .. } = &committed[0].progress_end else {
            panic!("a file batch carries a file marker");
        };
        assert_eq!(
            *tail,
            std::fs::metadata(&input).unwrap().len(),
            "and the batch ends at the end of the file"
        );
    }

    /// Every committed file batch as (inode, start byte, end byte, records).
    async fn committed_file_ranges(engine: &Engine) -> Vec<(Option<u64>, u64, u64, Option<i64>)> {
        let mut got: Vec<_> = engine
            .store
            .list_batches()
            .await
            .unwrap()
            .into_iter()
            .filter(|b| b.state == BatchState::SourceCommitted)
            .map(|b| {
                let (
                    ProgressMarker::File {
                        inode, start_byte, ..
                    },
                    ProgressMarker::File { end_byte, .. },
                ) = (&b.progress_start, &b.progress_end)
                else {
                    panic!("a file batch carries file markers");
                };
                (*inode, *start_byte, *end_byte, b.record_count)
            })
            .collect();
        got.sort();
        got
    }

    /// #621: a batch fails and its range is handed back while the file is
    /// still at its path - then logrotate renames it, compresses it and
    /// removes the original before the re-read. The next read found another
    /// inode at the path and restarted on the new file; the failed range was
    /// never archived and nothing said so. It is now re-read through the
    /// descriptor the adapter kept on the old file, under the old inode, and
    /// the new file is archived from its first byte.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_failed_range_whose_file_is_rotated_before_the_re_read_is_archived_from_the_old_file()
    {
        use std::os::unix::fs::MetadataExt;
        let _serial = THROTTLE_TESTS.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        std::fs::write(&input, "old 1\nold 2\n").unwrap(); // 12 bytes
        let old_inode = std::fs::metadata(&input).unwrap().ino();
        let mut cfg = file_config(
            dir.path().join("work").to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.max_batch_age_seconds = 3_600;
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        engine.backend = Arc::new(vtop_upload::MockBackend::new().with_throttled_puts(1));
        let first = engine.run_source(SourceType::File, true).await.unwrap();
        assert_eq!(first[0].final_state, BatchState::Failed);

        let rotated = dir.path().join("app.log.1");
        std::fs::rename(&input, &rotated).unwrap();
        std::fs::write(&input, "new 1\n").unwrap(); // 6 bytes
        std::fs::remove_file(&rotated).unwrap();
        let new_inode = std::fs::metadata(&input).unwrap().ino();

        for _ in 0..3 {
            engine.run_source(SourceType::File, true).await.unwrap();
        }
        assert_eq!(
            committed_file_ranges(&engine).await,
            {
                let mut want = vec![
                    (Some(old_inode), 0, 12, Some(2)),
                    (Some(new_inode), 0, 6, Some(1)),
                ];
                want.sort();
                want
            },
            "the failed range once, from the old file, and the new file whole"
        );
    }

    /// #621 across a restart: the batch failed, the engine stopped before the
    /// re-read, and the file was rotated by rename while it was down. Recovery
    /// refused the rewind at every start and the range was never archived. It
    /// now finds the rotated file beside the path (same inode, same first
    /// bytes) and re-reads the range from it - once: a later restart, which
    /// still sees the old FAILED row, finds the range archived and does not
    /// archive it again.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_failed_range_rotated_away_while_the_engine_was_down_is_archived_once_by_recovery() {
        use std::os::unix::fs::MetadataExt;
        let _serial = THROTTLE_TESTS.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        std::fs::write(&input, "old 1\nold 2\n").unwrap(); // 12 bytes
        let old_inode = std::fs::metadata(&input).unwrap().ino();
        let url = format!("sqlite://{}", dir.path().join("state.db").display());
        let config = |work: &str| {
            let mut cfg = file_config(
                dir.path().join(work).to_str().unwrap(),
                &url,
                vec![input.to_string_lossy().into_owned()],
                "mock",
            );
            cfg.batching.max_batch_age_seconds = 3_600;
            cfg
        };

        {
            let mut first = Engine::new(config("work-1"), StreamsConfig { streams: vec![] })
                .await
                .unwrap();
            first.backend = Arc::new(vtop_upload::MockBackend::new().with_throttled_puts(1));
            let done = first.run_source(SourceType::File, true).await.unwrap();
            assert_eq!(done[0].final_state, BatchState::Failed);
        } // the engine stops before the re-read

        std::fs::rename(&input, dir.path().join("app.log.1")).unwrap();
        std::fs::write(&input, "new 1\n").unwrap(); // 6 bytes
        let new_inode = std::fs::metadata(&input).unwrap().ino();

        let want = {
            let mut want = vec![
                (Some(old_inode), 0, 12, Some(2)),
                (Some(new_inode), 0, 6, Some(1)),
            ];
            want.sort();
            want
        };
        {
            let mut second = Engine::new(config("work-2"), StreamsConfig { streams: vec![] })
                .await
                .unwrap();
            let summary = second.recover().await.unwrap();
            assert_eq!(summary.still_pending, 0, "{summary:?}");
            for _ in 0..3 {
                second.run_source(SourceType::File, true).await.unwrap();
            }
            assert_eq!(
                committed_file_ranges(&second).await,
                want,
                "the failed range from the rotated file, and the new file whole"
            );
        }
        let mut third = Engine::new(config("work-3"), StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        third.recover().await.unwrap();
        for _ in 0..3 {
            third.run_source(SourceType::File, true).await.unwrap();
        }
        assert_eq!(
            committed_file_ranges(&third).await,
            want,
            "a later restart finds the range archived and archives nothing again"
        );
    }

    /// Review of #621 (Codex): a re-read of a rotated file must not become the
    /// path's restart position. One cycle sees the rotation: the old file's
    /// buffered window is sealed and fails, and the new file's window commits.
    /// The old range is then re-read from the rotated file and commits, after
    /// the new file's batch. The restart seed was the newest committed row of
    /// the path, by creation, so it named the rotated file. The restart then
    /// took the new file for a rotation and archived it again from byte 0.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_re_read_of_a_rotated_file_does_not_take_over_the_paths_restart_position() {
        use std::os::unix::fs::MetadataExt;
        let _serial = THROTTLE_TESTS.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        std::fs::write(&input, "old 1\nold 2\n").unwrap(); // 12 bytes
        let old_inode = std::fs::metadata(&input).unwrap().ino();
        let url = format!("sqlite://{}", dir.path().join("state.db").display());
        let config = |work: &str| {
            let mut cfg = file_config(
                dir.path().join(work).to_str().unwrap(),
                &url,
                vec![input.to_string_lossy().into_owned()],
                "mock",
            );
            cfg.batching.max_records = 10_000;
            cfg.batching.max_bytes = 104_857_600;
            cfg.batching.max_batch_age_seconds = 3_600;
            // One upload at a time, in the order the windows were sealed: the
            // old file's window goes first, so the one throttled put below is
            // its upload, whatever the pipeline does between puts.
            cfg.batching.max_concurrent_batches = 1;
            cfg
        };
        let new_inode;
        {
            let mut first = Engine::new(config("work-1"), StreamsConfig { streams: vec![] })
                .await
                .unwrap();
            let held = first.run_source(SourceType::File, false).await.unwrap();
            assert!(
                held.is_empty(),
                "the old file's window is buffered, not flushed"
            );

            std::fs::rename(&input, dir.path().join("app.log.1")).unwrap();
            std::fs::write(&input, "new 1\nnew 2\nnew 3\n").unwrap(); // 18 bytes
            new_inode = std::fs::metadata(&input).unwrap().ino();

            // One cycle: the rotation seals the old window, the new file is
            // read and flushed. The old window's upload is throttled; the new
            // one's goes through.
            first.backend = Arc::new(vtop_upload::MockBackend::new().with_throttled_puts(1));
            let rotated = first.run_source(SourceType::File, true).await.unwrap();
            let failed: Vec<_> = rotated
                .iter()
                .filter(|o| o.final_state == BatchState::Failed)
                .collect();
            assert_eq!(failed.len(), 1, "the old window fails: {rotated:?}");
            assert_eq!(
                committed_file_ranges(&first).await,
                vec![(Some(new_inode), 0, 18, Some(3))],
                "and the new file's window commits in the same cycle"
            );
            // The old range is re-read from the rotated file and commits
            // later, under the same path.
            for _ in 0..3 {
                first.run_source(SourceType::File, true).await.unwrap();
            }
            let mut want = vec![
                (Some(old_inode), 0, 12, Some(2)),
                (Some(new_inode), 0, 18, Some(3)),
            ];
            want.sort();
            assert_eq!(committed_file_ranges(&first).await, want);
        } // the engine stops

        let mut second = Engine::new(config("work-2"), StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        second.recover().await.unwrap();
        for _ in 0..3 {
            second.run_source(SourceType::File, true).await.unwrap();
        }
        let new_batches: Vec<_> = committed_file_ranges(&second)
            .await
            .into_iter()
            .filter(|(inode, ..)| *inode == Some(new_inode))
            .collect();
        assert_eq!(
            new_batches,
            vec![(Some(new_inode), 0, 18, Some(3))],
            "the new file resumes at its committed 18 after the restart; it is not \
             archived again from byte 0"
        );

        // And it resumes there: a line appended now is archived on its own.
        std::fs::OpenOptions::new()
            .append(true)
            .open(&input)
            .unwrap()
            .write_all(b"new 4\n")
            .unwrap();
        second.run_source(SourceType::File, true).await.unwrap();
        assert!(
            committed_file_ranges(&second)
                .await
                .contains(&(Some(new_inode), 18, 24, Some(1))),
            "the restart position kept: {:?}",
            committed_file_ranges(&second).await
        );
    }

    /// `vtop_source_replay_refused_total` for one tenant's file sources, read
    /// back through the registry as a scrape would.
    fn refused_so_far(tenant: &str) -> u64 {
        let mx = telemetry::init().unwrap();
        mx.registry
            .gather()
            .iter()
            .filter(|f| f.name().ends_with("source_replay_refused_total"))
            .flat_map(|f| f.get_metric().iter())
            .find(|m| {
                let labels = m.get_label();
                labels
                    .iter()
                    .any(|l| l.name() == "tenant" && l.value() == tenant)
                    && labels
                        .iter()
                        .any(|l| l.name() == "source_type" && l.value() == "file")
            })
            .map_or(0, |m| m.get_counter().get_value() as u64)
    }

    /// #621 (review): the batch fails, its range is handed back, the file is
    /// rotated away - and the rotated file is then truncated above its first
    /// page, so its fingerprint still matches. The re-read through the held
    /// descriptor archived the shortened prefix as though it were the range.
    /// It is refused instead, and counted like any refused rewind.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_rotated_file_truncated_before_its_re_read_is_refused_and_counted() {
        use std::os::unix::fs::MetadataExt;
        let _serial = THROTTLE_TESTS.lock().await;
        let tenant = "truncated-621";
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        let old: String = (0..200)
            .map(|i| format!("line {i:04} of the old file\n"))
            .collect();
        std::fs::write(&input, &old).unwrap();
        let old_inode = std::fs::metadata(&input).unwrap().ino();
        let mut cfg = file_config(
            dir.path().join("work").to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.max_batch_age_seconds = 3_600;
        cfg.engine.tenant = tenant.into();
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        engine.backend = Arc::new(vtop_upload::MockBackend::new().with_throttled_puts(1));
        let first = engine.run_source(SourceType::File, true).await.unwrap();
        assert_eq!(first[0].final_state, BatchState::Failed);

        let rotated = dir.path().join("app.log.1");
        std::fs::rename(&input, &rotated).unwrap();
        std::fs::write(&input, "new 1\n").unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&rotated)
            .unwrap()
            .set_len(4_500)
            .unwrap();

        let before = refused_so_far(tenant);
        for _ in 0..3 {
            engine.run_source(SourceType::File, true).await.unwrap();
        }
        assert_eq!(
            refused_so_far(tenant) - before,
            1,
            "the refused re-read is counted under the engine's tenant"
        );
        let committed = committed_file_ranges(&engine).await;
        assert!(
            committed
                .iter()
                .all(|(inode, ..)| *inode != Some(old_inode)),
            "nothing of the shortened file is archived as the range: {committed:?}"
        );
        assert_eq!(committed.len(), 1, "the new file is: {committed:?}");
    }

    /// #621: when the rotated file is gone (compressed and removed while the
    /// engine was down), the range cannot be re-read. That stays loud - and is
    /// counted, not only logged, so it can be alerted on.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_range_whose_file_is_gone_is_refused_and_counted() {
        let _serial = THROTTLE_TESTS.lock().await;
        let tenant = "refused-621";
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        std::fs::write(&input, "old 1\nold 2\n").unwrap();
        let url = format!("sqlite://{}", dir.path().join("state.db").display());
        let config = |work: &str| {
            let mut cfg = file_config(
                dir.path().join(work).to_str().unwrap(),
                &url,
                vec![input.to_string_lossy().into_owned()],
                "mock",
            );
            cfg.batching.max_batch_age_seconds = 3_600;
            cfg.engine.tenant = tenant.into();
            cfg
        };
        {
            let mut first = Engine::new(config("work-1"), StreamsConfig { streams: vec![] })
                .await
                .unwrap();
            first.backend = Arc::new(vtop_upload::MockBackend::new().with_throttled_puts(1));
            let done = first.run_source(SourceType::File, true).await.unwrap();
            assert_eq!(done[0].final_state, BatchState::Failed);
        }
        std::fs::rename(&input, dir.path().join("app.log.1")).unwrap();
        std::fs::write(&input, "new 1\n").unwrap();
        std::fs::remove_file(dir.path().join("app.log.1")).unwrap();

        let before = refused_so_far(tenant);
        let mut second = Engine::new(config("work-2"), StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let summary = second.recover().await.unwrap();
        assert_eq!(summary.still_pending, 1, "{summary:?}");
        assert_eq!(
            refused_so_far(tenant) - before,
            1,
            "the refused rewind is counted under the engine's tenant"
        );
    }

    /// A ledger write [`FaultyLedger`] can refuse: a row's insert, or a
    /// transition to the named state. `mark_failed`, `mark_verified` and
    /// `mark_source_committed` count as transitions to theirs.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum LedgerWrite {
        Insert,
        To(BatchState),
    }

    /// Delegates to a real store, except that each armed write is refused
    /// once: the ledger of a full disk or an unreachable PostgreSQL, for one
    /// write (#627).
    struct FaultyLedger {
        inner: Box<dyn StateStore>,
        armed: std::sync::Mutex<Vec<LedgerWrite>>,
    }

    impl FaultyLedger {
        /// Put `engine`'s own store behind a ledger that refuses `writes`.
        async fn install(engine: &mut Engine, writes: &[LedgerWrite]) {
            let stand_in = vtop_state::connect_state_store("sqlite::memory:")
                .await
                .unwrap();
            let inner = std::mem::replace(&mut engine.store, stand_in);
            engine.store = Box::new(FaultyLedger {
                inner,
                armed: std::sync::Mutex::new(writes.to_vec()),
            });
        }

        fn refuse(&self, write: LedgerWrite) -> Result<(), VtopError> {
            let mut armed = self.armed.lock().unwrap();
            match armed.iter().position(|w| *w == write) {
                Some(i) => {
                    armed.remove(i);
                    Err(VtopError::State(format!(
                        "test: ledger write {write:?} refused"
                    )))
                }
                None => Ok(()),
            }
        }
    }

    #[async_trait::async_trait]
    impl StateStore for FaultyLedger {
        async fn save_batch_state(&self, rec: &BatchRecord) -> Result<(), VtopError> {
            self.refuse(LedgerWrite::Insert)?;
            self.inner.save_batch_state(rec).await
        }
        async fn update_batch_state(
            &self,
            batch_id: &str,
            to: BatchState,
            patch: &BatchPatch,
        ) -> Result<(), VtopError> {
            self.refuse(LedgerWrite::To(to))?;
            self.inner.update_batch_state(batch_id, to, patch).await
        }
        async fn get_batch(&self, batch_id: &str) -> Result<Option<BatchRecord>, VtopError> {
            self.inner.get_batch(batch_id).await
        }
        async fn claim_incomplete_batches(
            &self,
            owner: &str,
            now: &str,
            lease_until: &str,
        ) -> Result<Vec<BatchRecord>, VtopError> {
            self.inner
                .claim_incomplete_batches(owner, now, lease_until)
                .await
        }
        async fn renew_claim(&self, batch_id: &str, lease_until: &str) -> Result<bool, VtopError> {
            self.inner.renew_claim(batch_id, lease_until).await
        }
        async fn max_committed_end_bytes(
            &self,
            source_type: SourceType,
        ) -> Result<Vec<(String, u64)>, VtopError> {
            self.inner.max_committed_end_bytes(source_type).await
        }
        async fn committed_cursors(
            &self,
            source_type: SourceType,
        ) -> Result<Vec<vtop_state::CommittedCursor>, VtopError> {
            self.inner.committed_cursors(source_type).await
        }
        async fn committed_cursors_by_file(
            &self,
            source_type: SourceType,
        ) -> Result<Vec<vtop_state::CommittedCursor>, VtopError> {
            self.inner.committed_cursors_by_file(source_type).await
        }
        async fn committed_kafka_offsets(
            &self,
        ) -> Result<Vec<vtop_state::CommittedKafkaOffset>, VtopError> {
            self.inner.committed_kafka_offsets().await
        }
        async fn list_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
            self.inner.list_batches().await
        }
        async fn list_incomplete_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
            self.inner.list_incomplete_batches().await
        }
        async fn list_failed_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
            self.inner.list_failed_batches().await
        }
        async fn mark_failed(&self, batch_id: &str, message: &str) -> Result<(), VtopError> {
            self.refuse(LedgerWrite::To(BatchState::Failed))?;
            self.inner.mark_failed(batch_id, message).await
        }
        async fn mark_verified(
            &self,
            batch_id: &str,
            class: vtop_state::VerificationClass,
        ) -> Result<(), VtopError> {
            self.refuse(LedgerWrite::To(BatchState::Verified))?;
            self.inner.mark_verified(batch_id, class).await
        }
        async fn mark_source_committed(&self, batch_id: &str) -> Result<(), VtopError> {
            self.refuse(LedgerWrite::To(BatchState::SourceCommitted))?;
            self.inner.mark_source_committed(batch_id).await
        }
        async fn prune_committed_history(
            &self,
            older_than: &str,
            limit: u32,
        ) -> Result<u64, VtopError> {
            self.inner.prune_committed_history(older_than, limit).await
        }
        async fn prune_candidates(
            &self,
            older_than: &str,
            limit: u32,
        ) -> Result<Vec<BatchRecord>, VtopError> {
            self.inner.prune_candidates(older_than, limit).await
        }
        async fn prune_listed(
            &self,
            older_than: &str,
            batch_ids: &[String],
        ) -> Result<u64, VtopError> {
            self.inner.prune_listed(older_than, batch_ids).await
        }
    }

    /// The byte range a file batch covers.
    fn file_range(row: &BatchRecord) -> (u64, u64) {
        let (ProgressMarker::File { start_byte, .. }, ProgressMarker::File { end_byte, .. }) =
            (&row.progress_start, &row.progress_end)
        else {
            panic!("a file batch carries file markers: {row:?}");
        };
        (*start_byte, *end_byte)
    }

    /// The ranges the ledger says are in the archive (VERIFIED or
    /// SOURCE_COMMITTED), in file order.
    async fn archived_ranges(engine: &Engine) -> Vec<(u64, u64)> {
        let mut ranges: Vec<_> = engine
            .store
            .list_batches()
            .await
            .unwrap()
            .iter()
            .filter(|b| matches!(b.state, BatchState::Verified | BatchState::SourceCommitted))
            .map(file_range)
            .collect();
        ranges.sort_unstable();
        ranges
    }

    /// Asserts the archive holds `[0, len)` with no gap and no byte twice.
    fn assert_gap_free(case: &str, ranges: &[(u64, u64)], len: u64) {
        let mut next = 0;
        for &(start, end) in ranges {
            assert_eq!(
                start, next,
                "{case}: the archive skips or repeats bytes at {next}: {ranges:?}"
            );
            next = end;
        }
        assert_eq!(next, len, "{case}: the archive stops short: {ranges:?}");
    }

    fn append_line(path: &std::path::Path, line: &str) {
        let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
        writeln!(f, "{line}").unwrap();
    }

    /// #627: while the engine runs, a batch whose pipeline breaks at ANY
    /// ledger write is handed back to its source, even when the write that
    /// would have recorded the decision is the one that broke. The range is
    /// read again by the next cycle and the archive ends up gap-free.
    #[tokio::test]
    async fn a_failure_at_any_ledger_write_while_running_never_leaves_a_gap() {
        // Throttled puts move a process-wide counter (see THROTTLE_TESTS).
        let _serial = THROTTLE_TESTS.lock().await;
        use BatchState::*;
        use LedgerWrite::{Insert, To};
        // (case, refused writes, object put throttled, cycle 1 returns Err,
        //  ledger state of the first attempt's row after cycle 2 — `None`
        //  when the attempt left no row).
        type Case = (
            &'static str,
            &'static [LedgerWrite],
            bool,
            bool,
            Option<BatchState>,
        );
        let cases: &[Case] = &[
            ("insert", &[Insert], false, true, None),
            ("SEALED", &[To(Sealed)], false, true, Some(Batching)),
            ("COMPRESSED", &[To(Compressed)], false, true, Some(Sealed)),
            (
                "CHECKSUMMED",
                &[To(Checksummed)],
                false,
                true,
                Some(Compressed),
            ),
            (
                "OBJECT_UPLOADED",
                &[To(ObjectUploaded)],
                false,
                true,
                Some(Checksummed),
            ),
            (
                "MANIFEST_UPLOADED",
                &[To(ManifestUploaded)],
                false,
                true,
                Some(ObjectUploaded),
            ),
            (
                "VERIFIED",
                &[To(Verified)],
                false,
                true,
                Some(ManifestUploaded),
            ),
            // The batch fails at its upload and both decision writes land:
            // the ledger records what recovery would.
            ("a failed upload", &[], true, false, Some(ReplayRequired)),
            // FAILED itself cannot be written: the row stays where the
            // pipeline left it, and the range is handed back regardless.
            (
                "a failed upload whose FAILED write fails",
                &[To(Failed)],
                true,
                true,
                Some(Checksummed),
            ),
            (
                "a failed upload whose REPLAY_REQUIRED write fails",
                &[To(ReplayRequired)],
                true,
                false,
                Some(Failed),
            ),
            // After VERIFIED nothing is handed back: the range is archived,
            // the row stays VERIFIED, and the next pass retries the commit
            // (#624) - superseded by then, so the source does not move back.
            (
                "SOURCE_COMMITTED",
                &[To(SourceCommitted)],
                false,
                true,
                Some(SourceCommitted),
            ),
        ];
        for &(case, refused, throttled, first_errs, first_row) in cases {
            let dir = tempfile::tempdir().unwrap();
            let input = dir.path().join("in.log");
            std::fs::write(&input, "line 1\n").unwrap();
            let mut cfg = file_config(
                dir.path().join("work").to_str().unwrap(),
                "sqlite::memory:",
                vec![input.to_string_lossy().into_owned()],
                "mock",
            );
            cfg.batching.max_batch_age_seconds = 3_600;
            let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
                .await
                .unwrap();
            engine.backend = Arc::new(
                vtop_upload::MockBackend::new().with_throttled_puts(usize::from(throttled)),
            );
            FaultyLedger::install(&mut engine, refused).await;

            let first = engine.run_source(SourceType::File, true).await;
            assert_eq!(first.is_err(), first_errs, "{case}: cycle 1 gave {first:?}");
            let attempt: Vec<_> = engine.store.list_batches().await.unwrap();

            append_line(&input, "line 2");
            let second = engine.run_source(SourceType::File, true).await.unwrap();
            assert!(
                second.iter().any(|o| o.committed),
                "{case}: the next cycle commits: {second:?}"
            );

            let len = std::fs::metadata(&input).unwrap().len();
            assert_gap_free(case, &archived_ranges(&engine).await, len);

            let rows = engine.store.list_batches().await.unwrap();
            match first_row {
                None => assert!(attempt.is_empty(), "{case}: no row was inserted"),
                Some(state) => {
                    assert_eq!(attempt.len(), 1, "{case}: one row after cycle 1");
                    let row = rows
                        .iter()
                        .find(|b| b.batch_id == attempt[0].batch_id)
                        .unwrap();
                    assert_eq!(row.state, state, "{case}: the first attempt's row");
                }
            }
        }
    }

    /// #627, end to end: the FAILED write of a batch that failed after its
    /// object landed fails too. The range is handed back anyway, so the cost
    /// is the one object the failed attempt left in the store, never a gap;
    /// and the stale row the restart then finds re-reads nothing: its source
    /// has committed past it, so recovery retires it as SUPERSEDED (#567)
    /// instead of planning a rewind the committed point would clamp to
    /// nothing.
    #[tokio::test]
    async fn a_failed_failed_write_costs_one_duplicate_object_and_no_gap() {
        let _serial = THROTTLE_TESTS.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.log");
        std::fs::write(&input, "line 1\n").unwrap();
        let store = format!("sqlite://{}", dir.path().join("state.db").display());
        let mut cfg = file_config(
            dir.path().join("work").to_str().unwrap(),
            &store,
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.max_batch_age_seconds = 3_600;

        let mut engine = Engine::new(cfg.clone(), StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        // The object lands; the manifest put is refused; so is FAILED.
        let mock = Arc::new(vtop_upload::MockBackend::new().with_throttled_manifest_puts(1));
        engine.backend = mock.clone();
        FaultyLedger::install(&mut engine, &[LedgerWrite::To(BatchState::Failed)]).await;

        assert!(engine.run_source(SourceType::File, true).await.is_err());
        let stale = engine.store.list_batches().await.unwrap().remove(0);
        assert_eq!(
            stale.state,
            BatchState::ObjectUploaded,
            "the decision was never recorded"
        );
        let orphan = stale.object_uri.clone().expect("the object was recorded");
        assert!(mock.contains(&orphan), "the failed attempt's object landed");

        append_line(&input, "line 2");
        let second = engine.run_source(SourceType::File, true).await.unwrap();
        assert!(second[0].committed);
        let len = std::fs::metadata(&input).unwrap().len();
        assert_eq!(
            archived_ranges(&engine).await,
            vec![(0, len)],
            "line 1 is archived again, with line 2: the duplicate"
        );
        drop(engine);

        // The restart finds the stale row below the committed point and
        // retires it: nothing is read again.
        let mut restarted = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let summary = restarted.recover().await.unwrap();
        assert_eq!(
            (summary.superseded, summary.replay_required),
            (1, 0),
            "{summary:?}"
        );
        let row = restarted
            .store
            .get_batch(&stale.batch_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.state, BatchState::Superseded);
        let after = restarted.run_source(SourceType::File, true).await.unwrap();
        assert!(after.is_empty(), "nothing is re-archived: {after:?}");
        assert_eq!(archived_ranges(&restarted).await, vec![(0, len)]);
    }

    /// Delegates to the real adapter but never rewinds: the running engine as
    /// it would behave under recovery's rule, which withholds the rewind when
    /// the decision could not be recorded.
    struct WithholdsRewind(Box<dyn SourceAdapter>);

    #[async_trait::async_trait]
    impl SourceAdapter for WithholdsRewind {
        async fn discover_sources(&self) -> Result<Vec<DiscoveredSource>, VtopError> {
            self.0.discover_sources().await
        }
        async fn read_batch_candidates(
            &mut self,
            source: &DiscoveredSource,
            max_records: usize,
            max_bytes: usize,
            max_wait: Duration,
        ) -> Result<Vec<ReadResult>, VtopError> {
            self.0
                .read_batch_candidates(source, max_records, max_bytes, max_wait)
                .await
        }
        async fn commit_progress(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
            self.0.commit_progress(marker).await
        }
        async fn replay_from_marker(&mut self, _marker: &ProgressMarker) -> Result<(), VtopError> {
            Ok(())
        }
        fn source_type(&self) -> SourceType {
            self.0.source_type()
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self.0.as_any_mut()
        }
    }

    /// #624 (review): a held-back row settles only on a committed point this
    /// process has moved since it held the row back, and only one at or past
    /// the row's end. The point at the hold is the restart seed - and when a
    /// held range (#639) lies below it, the seed alone is no evidence the
    /// re-read has passed anything.
    #[test]
    fn a_held_back_row_settles_only_once_the_committed_point_has_moved_past_it() {
        // Nothing known, or below the end: wait.
        assert!(!held_row_may_settle(None, None, 20));
        assert!(!held_row_may_settle(Some(15), Some(10), 20));
        // At or past the end, having moved since the hold: settle.
        assert!(held_row_may_settle(Some(20), Some(10), 20));
        assert!(held_row_may_settle(Some(40), None, 20));
        // At or past the end only because the seed already was: wait.
        assert!(!held_row_may_settle(Some(30), Some(30), 20));
        // A committed point that went DOWN since (a rotation restarted the
        // cursor) is not progress either.
        assert!(!held_row_may_settle(Some(25), Some(30), 20));
    }

    /// #624 (review): a VERIFIED row whose source commit failed during
    /// recovery was verified by the process that crashed, so this process
    /// has not counted it. Settling it later counts the verification with
    /// the commit, as `commit_recovered` does - or commits_total runs ahead
    /// of verified_total, the one alarm that must not cry wolf.
    #[tokio::test]
    async fn settling_a_row_whose_recovered_commit_failed_counts_its_verification() {
        let tenant = "settle-counts-624";
        let counter = |family: &str| -> u64 {
            let mx = telemetry::init().unwrap();
            mx.registry
                .gather()
                .iter()
                .filter(|f| f.name().ends_with(family))
                .flat_map(|f| f.get_metric().iter())
                .filter(|m| {
                    m.get_label()
                        .iter()
                        .any(|l| l.name() == "tenant" && l.value() == tenant)
                })
                .map(|m| m.get_counter().get_value() as u64)
                .sum()
        };
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.log");
        std::fs::write(&input, "line 1\n").unwrap();
        let url = format!("sqlite://{}", dir.path().join("state.db").display());
        let backend = Arc::new(vtop_upload::MockBackend::new());
        let engine = |work: &str| {
            let mut cfg = file_config(
                dir.path().join(work).to_str().unwrap(),
                &url,
                vec![input.to_string_lossy().into_owned()],
                "mock",
            );
            cfg.engine.tenant = tenant.into();
            let backend = backend.clone();
            async move {
                let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
                    .await
                    .unwrap();
                engine.backend = backend;
                engine
            }
        };
        let fail_commits = |engine: &mut Engine, failures: usize| {
            let inner = engine.adapters.remove(&SourceType::File).unwrap();
            engine.adapters.insert(
                SourceType::File,
                Box::new(FailingCommits { inner, failures }),
            );
        };

        // A process verifies the batch, cannot commit it, and crashes.
        let mut first = engine("work-1").await;
        fail_commits(&mut first, usize::MAX);
        let outcomes = first.process_once(SourceType::File).await.unwrap();
        assert!(!outcomes[0].committed, "{outcomes:?}");
        drop(first);

        // The restart re-checks it, and its commit fails once more.
        let mut second = engine("work-2").await;
        fail_commits(&mut second, 1);
        let verified_before = counter("verified_total");
        let commits_before = counter("commits_total");
        let summary = second.recover().await.unwrap();
        assert_eq!(
            (summary.committed, summary.still_pending),
            (0, 1),
            "{summary:?}"
        );
        // A later pass settles it.
        second.process_once(SourceType::File).await.unwrap();
        let rows = second.store.list_batches().await.unwrap();
        assert!(
            rows.iter().all(|r| r.state == BatchState::SourceCommitted),
            "{rows:?}"
        );
        // Every commit this process made is matched by a verification it
        // counted - the re-read's batch and the settled row alike.
        let commits = counter("commits_total") - commits_before;
        assert!(commits >= 1, "nothing was committed");
        assert_eq!(
            counter("verified_total") - verified_before,
            commits,
            "commits_total ran ahead of verified_total: the settled row's verification was not counted"
        );
    }

    /// Counts the source commits that reach the real adapter.
    struct CountsCommits {
        inner: Box<dyn SourceAdapter>,
        failures: usize,
        reached: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl SourceAdapter for CountsCommits {
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
            if self.failures > 0 {
                self.failures -= 1;
                return Err(VtopError::Source("source commit refused".into()));
            }
            self.reached.fetch_add(1, Ordering::SeqCst);
            self.inner.commit_progress(marker).await
        }
        async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
            self.inner.replay_from_marker(marker).await
        }
        fn committed_through(&self, marker: &ProgressMarker) -> Option<i128> {
            self.inner.committed_through(marker)
        }
        fn source_type(&self) -> SourceType {
            self.inner.source_type()
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self.inner.as_any_mut()
        }
    }

    /// #624 (review, P1): a row this process left uncommitted can be claimed
    /// by another engine once its lease runs out - and that engine may be
    /// about to re-read a range below it. Settling must not move the source
    /// for it: a commit moves the committed point, and with
    /// `delete_after_commit` can delete the file. Detecting the lost claim at
    /// the ledger write afterwards is too late, so the claim is renewed,
    /// fenced, BEFORE the source is touched; a refused renewal drops the row.
    #[tokio::test]
    async fn a_row_another_engine_has_claimed_is_never_committed_to_its_source() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.log");
        std::fs::write(&input, "line 1\n").unwrap();
        let url = format!("sqlite://{}", dir.path().join("state.db").display());
        let mut cfg = file_config(
            dir.path().join("work").to_str().unwrap(),
            &url,
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.max_batch_age_seconds = 3_600;
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let reached = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let inner = engine.adapters.remove(&SourceType::File).unwrap();
        engine.adapters.insert(
            SourceType::File,
            Box::new(CountsCommits {
                inner,
                failures: 1,
                reached: reached.clone(),
            }),
        );
        let first = engine.run_source(SourceType::File, true).await.unwrap();
        assert!(!first[0].committed, "{first:?}");

        // The lease runs out and another engine claims the row.
        let other = vtop_state::SqliteStateStore::connect(&url).await.unwrap();
        let later = Utc::now() + chrono::Duration::days(1);
        let claimed = other
            .claim_incomplete_batches(
                "engine-b",
                &later.to_rfc3339(),
                &(later + chrono::Duration::days(1)).to_rfc3339(),
            )
            .await
            .unwrap();
        assert!(claimed.iter().any(|r| r.batch_id == first[0].batch_id));

        engine.run_source(SourceType::File, true).await.unwrap();
        assert_eq!(
            reached.load(Ordering::SeqCst),
            0,
            "the source was committed for a row another engine has claimed"
        );
        assert_eq!(
            engine
                .store
                .get_batch(&first[0].batch_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            BatchState::Verified,
            "the new owner's row"
        );
        assert!(
            engine.unsettled.is_empty(),
            "a row claimed away is dropped, not retried for ever"
        );
    }

    /// Delegates to the real adapter, but its next `failures` source commits
    /// fail (a Kafka coordinator that is unavailable, say).
    struct FailingCommits {
        inner: Box<dyn SourceAdapter>,
        failures: usize,
    }

    #[async_trait::async_trait]
    impl SourceAdapter for FailingCommits {
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
            if self.failures > 0 {
                self.failures -= 1;
                return Err(VtopError::Source("source commit refused".into()));
            }
            self.inner.commit_progress(marker).await
        }
        async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
            self.inner.replay_from_marker(marker).await
        }
        fn read_position(&self, marker: &ProgressMarker) -> Option<i128> {
            self.inner.read_position(marker)
        }
        fn committed_through(&self, marker: &ProgressMarker) -> Option<i128> {
            self.inner.committed_through(marker)
        }
        fn source_type(&self) -> SourceType {
            self.inner.source_type()
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self.inner.as_any_mut()
        }
    }

    /// #624: a batch whose source commit failed after VERIFIED stayed
    /// VERIFIED until the next restart: nothing in the running loop looked
    /// at it again. It is retried after a later pass of its source - and,
    /// because a newer range has committed past it by then, retried without
    /// moving the committed point back.
    #[tokio::test]
    async fn a_verified_batch_whose_commit_failed_is_committed_on_a_later_pass() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.log");
        std::fs::write(&input, "line 1\n").unwrap();
        let store = format!("sqlite://{}", dir.path().join("state.db").display());
        let mut cfg = file_config(
            dir.path().join("work").to_str().unwrap(),
            &store,
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.max_batch_age_seconds = 3_600;
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let inner = engine.adapters.remove(&SourceType::File).unwrap();
        engine.adapters.insert(
            SourceType::File,
            Box::new(FailingCommits { inner, failures: 1 }),
        );

        let first = engine.run_source(SourceType::File, true).await.unwrap();
        assert!(!first[0].committed, "{first:?}");
        let stuck = first[0].batch_id.clone();
        assert_eq!(
            engine.store.get_batch(&stuck).await.unwrap().unwrap().state,
            BatchState::Verified
        );

        append_line(&input, "line 2");
        let second = engine.run_source(SourceType::File, true).await.unwrap();
        assert!(second.iter().all(|o| o.committed), "{second:?}");
        let newer = engine
            .store
            .get_batch(&second[0].batch_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            engine.store.get_batch(&stuck).await.unwrap().unwrap().state,
            BatchState::SourceCommitted,
            "the batch whose commit failed is committed by a later pass, without a restart"
        );
        let len = std::fs::metadata(&input).unwrap().len();
        assert_eq!(
            engine.adapters[&SourceType::File].committed_through(&newer.progress_end),
            Some(i128::from(len)),
            "committing the older range moved the committed point back"
        );
        assert_gap_free("commit retried", &archived_ranges(&engine).await, len);
        let after = engine.run_source(SourceType::File, true).await.unwrap();
        assert!(after.is_empty(), "nothing is read again: {after:?}");
    }

    /// #627, the other direction, and why it is not taken: had the running
    /// engine followed recovery's rule and withheld the rewind when FAILED
    /// could not be written, the range it had already read past would be
    /// lost for good. The next cycle reads on, a later batch commits past the
    /// range, and at the restart its rewind would be clamped to that commit -
    /// so recovery retires the row as SUPERSEDED (#567). That is no cover
    /// for the loss: the coverage audit still names the row in the hole.
    #[tokio::test]
    async fn withholding_the_rewind_on_an_unrecorded_failure_would_lose_the_range() {
        let _serial = THROTTLE_TESTS.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.log");
        std::fs::write(&input, "line 1\n").unwrap();
        let store = format!("sqlite://{}", dir.path().join("state.db").display());
        let mut cfg = file_config(
            dir.path().join("work").to_str().unwrap(),
            &store,
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.max_batch_age_seconds = 3_600;

        let mut engine = Engine::new(cfg.clone(), StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        engine.backend = Arc::new(vtop_upload::MockBackend::new().with_throttled_puts(1));
        FaultyLedger::install(&mut engine, &[LedgerWrite::To(BatchState::Failed)]).await;
        let inner = engine.adapters.remove(&SourceType::File).unwrap();
        engine
            .adapters
            .insert(SourceType::File, Box::new(WithholdsRewind(inner)));

        assert!(engine.run_source(SourceType::File, true).await.is_err());
        append_line(&input, "line 2");
        let second = engine.run_source(SourceType::File, true).await.unwrap();
        assert!(second[0].committed);
        let len = std::fs::metadata(&input).unwrap().len();
        let line_1 = "line 1\n".len() as u64;
        assert_eq!(
            archived_ranges(&engine).await,
            vec![(line_1, len)],
            "the next batch read on from the end of the failed range"
        );
        drop(engine);

        let mut restarted = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let summary = restarted.recover().await.unwrap();
        assert_eq!(
            (summary.superseded, summary.replay_required),
            (1, 0),
            "the source committed past the range: nothing could re-read it ({summary:?})"
        );
        let after = restarted.run_source(SourceType::File, true).await.unwrap();
        assert!(after.is_empty(), "nothing is read again: {after:?}");
        assert_eq!(
            archived_ranges(&restarted).await,
            vec![(line_1, len)],
            "line 1 is never archived"
        );
        let rows = restarted.store.list_batches().await.unwrap();
        let retired: Vec<String> = rows
            .iter()
            .filter(|row| row.state == BatchState::Superseded)
            .map(|row| row.batch_id.clone())
            .collect();
        let holes: Vec<_> = crate::coverage::audit(rows)
            .into_iter()
            .flat_map(|report| report.failed_holes)
            .collect();
        assert_eq!(
            holes
                .iter()
                .map(|hole| (hole.start, hole.end))
                .collect::<Vec<_>>(),
            vec![(0, line_1)],
            "the audit still reports the lost range: {holes:?}"
        );
        assert_eq!(
            holes[0].failed_batches, retired,
            "and names the retired row"
        );
    }

    /// The adapters keep one cursor per path, so when a path has incomplete
    /// rows for an old and a current file, the rewind applied LAST decides where
    /// the path resumes. It must be the most recent file's, whatever the inodes
    /// look like as text (inode 9 sorts after inode 10).
    #[test]
    fn the_most_recent_file_at_a_path_is_rewound_last() {
        let file = |inode: u64, start: u64| ProgressMarker::File {
            path: "/a".into(),
            inode: Some(inode),
            start_byte: start,
            end_byte: start + 10,
            file_size: 0,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: None,
        };
        let inodes = |plan: &RewindPlan| -> Vec<Option<u64>> {
            plan.markers().iter().map(marker_inode).collect()
        };
        // Recovery visits rows oldest first: inode 10 is the older file.
        let mut plan = RewindPlan::default();
        plan.add(&file(10, 500));
        plan.add(&file(9, 40));
        assert_eq!(inodes(&plan), vec![Some(10), Some(9)]);
        // And the other way round: the lexically smaller inode is the older.
        let mut plan = RewindPlan::default();
        plan.add(&file(9, 40));
        plan.add(&file(10, 500));
        assert_eq!(inodes(&plan), vec![Some(9), Some(10)]);
        // A later row of an already-planned file makes it the most recent.
        plan.add(&file(9, 60));
        assert_eq!(inodes(&plan), vec![Some(10), Some(9)]);
    }

    /// Several incomplete ranges of one source: recovery used to apply each
    /// rewind in turn, so the last row visited decided where the source
    /// resumed. The plan keeps the lowest start per unit, and keeps units apart
    /// — including two inodes of one path.
    #[test]
    fn a_rewind_plan_keeps_the_lowest_start_per_unit_whatever_the_order() {
        let file = |path: &str, inode: Option<u64>, start: u64| ProgressMarker::File {
            path: path.into(),
            inode,
            start_byte: start,
            end_byte: start + 10,
            file_size: 0,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: None,
        };
        let kafka = |partition: i32, start: i64| ProgressMarker::Kafka {
            topic: "t".into(),
            partition,
            start_offset: start,
            end_offset: start + 9,
            consumer_group: "g".into(),
        };
        for order in [[0usize, 1, 2], [2, 1, 0], [1, 2, 0]] {
            let candidates = [
                file("/a", Some(1), 100),
                file("/a", Some(1), 20),
                file("/a", Some(1), 60),
            ];
            let mut plan = RewindPlan::default();
            for i in order {
                plan.add(&candidates[i]);
            }
            // The current file at the same path, at a HIGHER offset than the
            // old one's lowest: it must not be hidden by it.
            let add = |plan: &mut RewindPlan, m: ProgressMarker| plan.add(&m);
            add(&mut plan, file("/a", Some(2), 500));
            add(&mut plan, file("/b", None, 5));
            add(&mut plan, kafka(0, 50));
            add(&mut plan, kafka(0, 10));
            add(&mut plan, kafka(1, 99));
            let got: Vec<(String, i128)> = plan
                .markers()
                .iter()
                .map(|m| (unit_key(m), start_position(m)))
                .collect();
            assert_eq!(
                got,
                vec![
                    ("file|/a|1".to_string(), 20),
                    ("file|/a|2".to_string(), 500),
                    ("file|/b|?".to_string(), 5),
                    ("kafka|g|t#p0|?".to_string(), 10),
                    ("kafka|g|t#p1|?".to_string(), 99),
                ],
                "order {order:?}"
            );
        }
    }

    /// A stale ledger row whose bytes a later batch already archived rewinds to
    /// the committed point, not to its own start. The plan must follow where
    /// the rewind landed, or the stale row holds back every VERIFIED range above
    /// it for ever (review of #563).
    #[test]
    fn a_rebased_plan_stops_a_covered_row_from_holding_back_a_verified_range() {
        let file = |start: u64, end: u64| ProgressMarker::File {
            path: "/a".into(),
            inode: Some(1),
            start_byte: start,
            end_byte: end,
            file_size: 0,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: None,
        };
        let mut plan = RewindPlan::default();
        plan.add(&file(0, 100));
        assert!(plan.holds_back(&file(100, 200)), "raw start 0 is below 100");
        // The rewind landed at 100: [0, 100) was already archived.
        let landed = HashMap::from([(unit_key(&file(0, 100)), 100)]);
        plan.rebase(&landed);
        assert!(!plan.holds_back(&file(100, 200)));
        // A rewind that landed BELOW the VERIFIED range still holds it back.
        let mut plan = RewindPlan::default();
        plan.add(&file(0, 100));
        plan.rebase(&HashMap::from([(unit_key(&file(0, 100)), 0)]));
        assert!(plan.holds_back(&file(100, 200)));
        // A unit the adapter could not place keeps its raw start.
        let mut plan = RewindPlan::default();
        plan.add(&file(0, 100));
        plan.rebase(&HashMap::new());
        assert!(plan.holds_back(&file(100, 200)));
    }

    /// Committing a VERIFIED range moves its unit's frontier to the end of that
    /// range, and the file adapters never rewind below the frontier. A
    /// VERIFIED range archives exactly its own bytes, so the only bytes a
    /// commit can strand are pending ones BELOW its start.
    #[test]
    fn a_verified_range_is_held_back_only_by_a_pending_range_that_starts_below_it() {
        let file = |path: &str, inode: u64, start: u64, end: u64| ProgressMarker::File {
            path: path.into(),
            inode: Some(inode),
            start_byte: start,
            end_byte: end,
            file_size: 0,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: None,
        };
        let mut plan = RewindPlan::default();
        plan.add(&file("/a", 1, 100, 150));
        // Starts above the pending range: its commit would strand bytes 100..
        assert!(plan.holds_back(&file("/a", 1, 150, 200)));
        assert!(
            plan.holds_back(&file("/a", 1, 120, 200)),
            "starts inside it"
        );
        // The same start (the object that replaced a stale row for these
        // bytes), or lower: nothing pending is left below its start.
        assert!(!plan.holds_back(&file("/a", 1, 100, 150)));
        assert!(!plan.holds_back(&file("/a", 1, 90, 200)));
        assert!(
            !plan.holds_back(&file("/a", 1, 90, 101)),
            "pending 101.. stays above the frontier"
        );
        assert!(!plan.holds_back(&file("/a", 1, 50, 100)));
        assert!(!plan.holds_back(&file("/a", 1, 0, 40)));
        // Another inode of the path, another path: unaffected.
        assert!(!plan.holds_back(&file("/a", 2, 150, 200)));
        assert!(!plan.holds_back(&file("/b", 1, 150, 200)));
        // The LOWEST pending start decides, however many there are.
        plan.add(&file("/a", 1, 300, 320));
        plan.add(&file("/a", 1, 60, 80));
        assert!(plan.holds_back(&file("/a", 1, 90, 200)), "60 is below 90");
        assert!(!plan.holds_back(&file("/a", 1, 60, 400)));
        // Kafka offsets are inclusive: the pending range 10..=19.
        let kafka = |start: i64, end: i64| ProgressMarker::Kafka {
            topic: "t".into(),
            partition: 0,
            start_offset: start,
            end_offset: end,
            consumer_group: "g".into(),
        };
        let mut plan = RewindPlan::default();
        plan.add(&kafka(10, 19));
        assert!(
            plan.holds_back(&kafka(15, 25)),
            "starts above the pending offset"
        );
        assert!(!plan.holds_back(&kafka(10, 19)), "the same offsets");
        assert!(!plan.holds_back(&kafka(0, 12)), "starts below it");
    }

    /// A marker with no inode (written before inodes were recorded) cannot be
    /// told apart from any file at its path: it must not slip past a pending
    /// range that has one, nor hide one from a VERIFIED range that has one.
    #[test]
    fn a_marker_without_an_inode_is_compared_against_every_inode_at_its_path() {
        let spool = |inode: Option<u64>, start: u64, end: u64| ProgressMarker::SyslogSpool {
            spool_id: "s".into(),
            path: "/spool".into(),
            inode,
            start_byte: start,
            end_byte: end,
            received_time_start: None,
            received_time_end: None,
            file_size: None,
            content_fingerprint: None,
        };
        // Pending range with an inode, VERIFIED range from a legacy marker.
        let mut plan = RewindPlan::default();
        plan.add(&spool(Some(7), 100, 150));
        assert!(plan.holds_back(&spool(None, 150, 200)));
        assert!(!plan.holds_back(&spool(None, 100, 150)), "same start");
        // Legacy pending range, VERIFIED range with an inode.
        let mut plan = RewindPlan::default();
        plan.add(&spool(None, 100, 150));
        assert!(plan.holds_back(&spool(Some(7), 150, 200)));
        assert!(plan.holds_back(&spool(Some(9), 150, 200)));
        // Two KNOWN, different inodes stay apart.
        let mut plan = RewindPlan::default();
        plan.add(&spool(Some(7), 100, 150));
        assert!(!plan.holds_back(&spool(Some(9), 150, 200)));
    }

    /// Review of #578 (Codex): content A was replaced by B at the same inode.
    /// A's incomplete range can never be re-read - the adapter refuses to
    /// rewind B to it - so it must not hold back B's VERIFIED range above it,
    /// or that range stays VERIFIED on every restart while B's bytes are read
    /// again behind it. Content proven different is another file; content
    /// that cannot be compared (fingerprints of different lengths, or none)
    /// holds back as before.
    #[test]
    fn a_pending_range_of_replaced_content_does_not_hold_back_the_replacement() {
        let page = |tag: u8| ContentFingerprint::of(&[tag; 5_000]);
        let file = |start: u64, content: Option<ContentFingerprint>| ProgressMarker::File {
            path: "/a".into(),
            inode: Some(1),
            start_byte: start,
            end_byte: start + 100,
            file_size: 8_192,
            mtime: String::new(),
            content_fingerprint: content,
            tail_fingerprint: None,
        };
        let spool = |start: u64, content: Option<ContentFingerprint>| ProgressMarker::SyslogSpool {
            spool_id: "s".into(),
            path: "/s".into(),
            inode: Some(1),
            start_byte: start,
            end_byte: start + 100,
            received_time_start: None,
            received_time_end: None,
            file_size: Some(8_192),
            content_fingerprint: content,
        };
        for marker in [file, spool] {
            let mut plan = RewindPlan::default();
            plan.add(&marker(0, page(b'A')));
            assert!(
                !plan.holds_back(&marker(100, page(b'B'))),
                "A's pending [0,100) cannot strand bytes of B, which replaced it"
            );
            // The same content, or content that cannot be compared, still
            // holds back - and so does a pending range of B's own.
            assert!(plan.holds_back(&marker(100, page(b'A'))));
            assert!(plan.holds_back(&marker(100, None)));
            assert!(plan.holds_back(&marker(100, ContentFingerprint::of(b"short"))));
            plan.add(&marker(50, page(b'B')));
            assert!(
                plan.holds_back(&marker(100, page(b'B'))),
                "B's own pending [50,150) is below B's VERIFIED range"
            );
        }
    }

    /// Review of #578 (Codex): a file under one page fingerprints a different
    /// length at each read, so its pending ranges fall into several groups.
    /// When the unit's rewind lands above all of them (a later batch already
    /// re-archived them), every group of that file must move with it - before
    /// the groups were split they were one, and a group left at its raw start
    /// held back the VERIFIED range above it on every restart. A group of
    /// content proven to be another file's does not move.
    #[test]
    fn a_landed_rewind_rebases_every_group_of_the_same_file() {
        let text: Vec<u8> = (0..40)
            .flat_map(|i| format!("line {i:03}\n").into_bytes())
            .collect();
        let file = |start: u64, content: Option<ContentFingerprint>| ProgressMarker::File {
            path: "/a".into(),
            inode: Some(1),
            start_byte: start,
            end_byte: start + 100,
            file_size: 300,
            mtime: String::new(),
            content_fingerprint: content,
            tail_fingerprint: None,
        };
        let mut plan = RewindPlan::default();
        plan.add(&file(0, ContentFingerprint::of(&text[..100])));
        plan.add(&file(100, ContentFingerprint::of(&text[..200])));
        plan.rebase(&HashMap::from([(
            rewind_key(&file(0, ContentFingerprint::of(&text[..100]))),
            200,
        )]));
        assert!(
            !plan.holds_back(&file(200, ContentFingerprint::of(&text[..300]))),
            "both pending ranges were re-archived through 200; the [100,200) group must \
             not stay at its raw start"
        );

        let page = |tag: u8| ContentFingerprint::of(&[tag; 5_000]);
        let mut plan = RewindPlan::default();
        plan.add(&file(0, page(b'A')));
        plan.add(&file(50, page(b'B')));
        plan.rebase(&HashMap::from([(rewind_key(&file(0, page(b'A'))), 300)]));
        assert!(
            plan.holds_back(&file(100, page(b'B'))),
            "a landing in A's content says nothing about B's pending range"
        );
    }

    /// Review of #578 (Codex): pending ranges of old content A (from 0) and of
    /// its same-inode replacement B (from 100). One rewind per inode went to
    /// A's start, which the adapter refuses, so B never landed and its stale
    /// start held back B's VERIFIED range on every restart. Each proven
    /// content generation now gets its own rewind, the newest applied last;
    /// content that cannot be told apart stays one rewind, so a growing
    /// small file is never rewound twice.
    #[test]
    fn each_proven_content_generation_gets_its_own_rewind() {
        let page = |tag: u8| ContentFingerprint::of(&[tag; 5_000]);
        let file = |start: u64, content: Option<ContentFingerprint>| ProgressMarker::File {
            path: "/a".into(),
            inode: Some(1),
            start_byte: start,
            end_byte: start + 100,
            file_size: 8_192,
            mtime: String::new(),
            content_fingerprint: content,
            tail_fingerprint: None,
        };
        let mut plan = RewindPlan::default();
        plan.add(&file(0, page(b'A')));
        plan.add(&file(100, page(b'B')));
        let markers = plan.markers();
        assert_eq!(
            markers.iter().map(start_position).collect::<Vec<_>>(),
            [0, 100],
            "A's rewind, then B's - the most recent content applied last"
        );
        // A's rewind is refused (no landing); B's lands at 300, where a later
        // batch has already committed B through.
        plan.rebase(&HashMap::from([(rewind_key(&markers[1]), 300)]));
        assert!(
            !plan.holds_back(&file(300, page(b'B'))),
            "B's re-archived pending range must not hold back B's VERIFIED [300,400)"
        );

        let text: Vec<u8> = (0..40)
            .flat_map(|i| format!("line {i:03}\n").into_bytes())
            .collect();
        let mut plan = RewindPlan::default();
        plan.add(&file(0, ContentFingerprint::of(&text[..100])));
        plan.add(&file(100, ContentFingerprint::of(&text[..200])));
        plan.add(&file(150, None));
        assert_eq!(
            plan.markers()
                .iter()
                .map(start_position)
                .collect::<Vec<_>>(),
            [0],
            "one file whose fingerprints cannot be compared is one rewind, to its lowest start"
        );
    }

    #[test]
    fn the_failure_backoff_doubles_is_capped_and_keeps_its_jitter_at_the_cap() {
        let base = Duration::from_secs(2);
        let top = |streak| failure_backoff(base, streak, 0);
        assert_eq!(top(1), Duration::from_secs(2));
        assert_eq!(top(2), Duration::from_secs(4));
        assert_eq!(top(3), Duration::from_secs(8));
        assert_eq!(top(6), FAILURE_BACKOFF_CAP, "capped");
        assert_eq!(
            top(10_000),
            FAILURE_BACKOFF_CAP,
            "a long outage stays capped"
        );
        // Jitter only ever shortens, by at most a quarter, and it must still
        // spread engines out when they are AT the cap.
        for streak in [1u32, 3, 6, 10_000] {
            let full = top(streak);
            let spread = Duration::from_millis(u64::try_from(full.as_millis() / 4).unwrap());
            let samples: Vec<Duration> = [0u64, 1, 499, 500, 7_777, u64::MAX]
                .iter()
                .map(|seed| failure_backoff(base, streak, *seed))
                .collect();
            for got in &samples {
                assert!(
                    *got <= full && *got >= full - spread,
                    "streak {streak}: {got:?}"
                );
            }
            assert!(
                samples
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    > 1,
                "streak {streak}: jitter produced one value for every seed"
            );
        }
        // A zero streak (no failure yet) and a zero base cannot divide or panic.
        assert_eq!(failure_backoff(base, 0, 0), Duration::from_secs(2));
        assert_eq!(failure_backoff(Duration::ZERO, 3, 7), Duration::ZERO);
    }

    /// #102 (review on #391): with `create_bucket` set, provisioning ran
    /// inside `process_until_verified`, so every batch of a soak paid a live
    /// CreateBucket round trip against the very store whose absorption
    /// capacity the soak measures. Provisioning is cached the way the
    /// versioning preflight is: once per bucket per process — per bucket,
    /// not at startup, because the bucket name is templated per batch.
    #[tokio::test]
    async fn bucket_provisioning_runs_once_per_bucket_not_once_per_batch() {
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        let input_a = dir.path().join("in-a.log");
        let input_b = dir.path().join("in-b.log");
        std::fs::write(&input_a, "a line 1\n").unwrap();
        std::fs::write(&input_b, "b line 1\n").unwrap();

        let mut cfg = file_config(
            work.to_str().unwrap(),
            "sqlite::memory:",
            vec![
                input_a.to_string_lossy().into_owned(),
                input_b.to_string_lossy().into_owned(),
            ],
            "mock",
        );
        cfg.upload.create_bucket = true;
        // One bucket per source, so a single engine covers both cache
        // verdicts: two sources must provision two buckets, while a later
        // batch from a source whose bucket already exists must provision
        // nothing.
        cfg.upload.bucket = "telemetry-{source}".into();
        cfg.batching.max_batch_age_seconds = 3_600;

        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        // `Engine::new` builds its backend from the config string; counting
        // calls needs a handle the test keeps, so swap in one of its own.
        let mock = Arc::new(vtop_upload::MockBackend::new());
        engine.backend = mock.clone();

        let first = engine.run_source(SourceType::File, true).await.unwrap();
        assert_eq!(first.len(), 2, "both sources must flush a batch");
        assert!(
            first.iter().all(|o| o.committed),
            "provisioning through the cache must leave batches able to verify \
             and commit"
        );
        assert_eq!(
            mock.ensure_bucket_calls(),
            2,
            "distinct buckets are distinct provisioning targets: caching one \
             must not suppress creating the other"
        );

        // A second batch from source A resolves to the bucket provisioned
        // above.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&input_a)
                .unwrap();
            writeln!(f, "a line 2").unwrap();
        }
        let second = engine.run_source(SourceType::File, true).await.unwrap();
        assert_eq!(
            second.len(),
            1,
            "only the appended source has new data to flush"
        );
        assert!(
            second[0].committed,
            "the cached skip must be invisible to the batch: a bucket marked \
             provisioned still has to verify and commit exactly as the first one did"
        );
        assert_eq!(
            mock.ensure_bucket_calls(),
            2,
            "a bucket provisioned earlier in this process must not be \
             re-provisioned per batch — that per-batch round trip is exactly \
             the unmeasured store load #102's soak was absorbing"
        );
    }

    /// The cache is only a cache once a provisioning completes; before that,
    /// width-many first batches race through `buffer_unordered`, and a
    /// check-then-insert released around the `ensure_bucket` await let every
    /// one of them observe the bucket absent. The guard held across the
    /// await is what makes "once per bucket" true at any width.
    #[tokio::test]
    async fn two_concurrent_first_batches_provision_their_shared_bucket_once() {
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        let input_a = dir.path().join("in-a.log");
        let input_b = dir.path().join("in-b.log");
        std::fs::write(&input_a, "a line 1\n").unwrap();
        std::fs::write(&input_b, "b line 1\n").unwrap();

        let mut cfg = file_config(
            work.to_str().unwrap(),
            "sqlite::memory:",
            vec![
                input_a.to_string_lossy().into_owned(),
                input_b.to_string_lossy().into_owned(),
            ],
            "mock",
        );
        cfg.upload.create_bucket = true;
        // ONE bucket, TWO sources, width two: both first batches are in
        // flight together and both resolve the same provisioning target.
        cfg.upload.bucket = "telemetry-shared".into();
        cfg.batching.max_concurrent_batches = 2;
        cfg.batching.max_batch_age_seconds = 3_600;

        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let mock = Arc::new(vtop_upload::MockBackend::new());
        engine.backend = mock.clone();

        let outcomes = engine.run_source(SourceType::File, true).await.unwrap();
        assert_eq!(outcomes.len(), 2, "both sources must flush a batch");
        assert!(
            outcomes.iter().all(|o| o.committed),
            "serialized provisioning must be invisible to the batches: both \
             still verify and commit"
        );
        assert_eq!(
            mock.ensure_bucket_calls(),
            1,
            "two concurrent first batches must provision their shared bucket \
             exactly once: width-many CreateBucket calls against the measured \
             store is the race the awaited guard exists to close"
        );
    }

    /// #558: a file rotated while a batch is still buffered. The buffered
    /// window belongs to the old file; the next read restarts the path at byte
    /// 0. Merged into one batch it carried a start from one file and an end
    /// from the other. It must be sealed as its own batch first.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_rotation_seals_the_buffered_window_instead_of_merging_two_files() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        std::fs::write(&input, "old 1\nold 2\n").unwrap();
        let mut cfg = file_config(
            dir.path().join("work").to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        // Nothing seals on its own: the first cycle only buffers.
        cfg.batching.max_records = 10_000;
        cfg.batching.max_bytes = 104_857_600;
        cfg.batching.max_batch_age_seconds = 3_600;
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();

        let held = engine.run_source(SourceType::File, false).await.unwrap();
        assert!(held.is_empty(), "buffered, not flushed");

        // Rotate: the old file keeps its inode under another name.
        std::fs::rename(&input, dir.path().join("app.log.1")).unwrap();
        let fresh: String = (0..30).map(|i| format!("new {i}\n")).collect();
        std::fs::write(&input, &fresh).unwrap();

        let outcomes = engine.run_source(SourceType::File, true).await.unwrap();
        assert!(
            outcomes.iter().all(|o| o.committed),
            "both windows commit: {outcomes:?}"
        );
        let mut committed: Vec<_> = engine
            .store
            .list_batches()
            .await
            .unwrap()
            .into_iter()
            .filter(|b| b.state == BatchState::SourceCommitted)
            .collect();
        committed.sort_by_key(|b| b.record_count);
        assert_eq!(committed.len(), 2, "two files, two batches: {committed:?}");
        assert_eq!(committed[0].record_count, Some(2), "the old file's window");
        assert_eq!(committed[1].record_count, Some(30), "the new file, whole");
        for b in &committed {
            let (
                ProgressMarker::File {
                    inode: si,
                    start_byte,
                    ..
                },
                ProgressMarker::File { inode: ei, .. },
            ) = (&b.progress_start, &b.progress_end)
            else {
                panic!("file markers");
            };
            assert_eq!(si, ei, "one batch describes ONE file");
            assert_eq!(*start_byte, 0);
        }
        assert_ne!(
            committed[0].progress_end, committed[1].progress_end,
            "and the two are different files"
        );
    }

    /// Records every read the wrapped adapter hands out and every marker the
    /// engine commits through it, so a test can hold the commit to exactly
    /// what was read.
    struct RecordingAdapter {
        inner: Box<dyn SourceAdapter>,
        reads: Arc<std::sync::Mutex<Vec<ProgressMarker>>>,
        commits: Arc<std::sync::Mutex<Vec<ProgressMarker>>>,
    }

    #[async_trait::async_trait]
    impl SourceAdapter for RecordingAdapter {
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
            let reads = self
                .inner
                .read_batch_candidates(source, max_records, max_bytes, max_wait)
                .await?;
            self.reads.lock().unwrap().extend(
                reads
                    .iter()
                    .filter(|r| !r.is_empty())
                    .map(|r| r.progress_end.clone()),
            );
            Ok(reads)
        }
        async fn commit_progress(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
            self.commits.lock().unwrap().push(marker.clone());
            self.inner.commit_progress(marker).await
        }
        async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
            self.inner.replay_from_marker(marker).await
        }
        fn source_type(&self) -> SourceType {
            self.inner.source_type()
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self.inner.as_any_mut()
        }
    }

    /// #622: a batch buffered from three reads holds bytes `0..total`, and
    /// its manifest and batch id must say so. They used to carry the LAST
    /// read's range (`b<l1+l2>-<total>`), so an audit from manifests alone
    /// saw a hole at `0..l1+l2` and could not place the first records. The
    /// ledger row and the adapter commit keep the last read's marker: the
    /// file adapter settles a commit by that read's exact range.
    #[tokio::test]
    async fn a_window_of_several_reads_names_its_whole_range_in_its_manifest_and_batch_id() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        let lines = ["one\n", "two two\n", "three three three\n"];
        std::fs::write(&input, lines[0]).unwrap();
        let mut cfg = file_config(
            dir.path().join("work").to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        // Nothing seals on its own: only the forced cycle flushes.
        cfg.batching.max_records = 10_000;
        cfg.batching.max_bytes = 104_857_600;
        cfg.batching.max_batch_age_seconds = 3_600;
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
        let commits = Arc::new(std::sync::Mutex::new(Vec::new()));
        let inner = engine.adapters.remove(&SourceType::File).unwrap();
        engine.adapters.insert(
            SourceType::File,
            Box::new(RecordingAdapter {
                inner,
                reads: reads.clone(),
                commits: commits.clone(),
            }),
        );

        let append = |line: &str| {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&input)
                .unwrap();
            f.write_all(line.as_bytes()).unwrap();
        };
        assert!(engine
            .run_source(SourceType::File, false)
            .await
            .unwrap()
            .is_empty());
        append(lines[1]);
        assert!(engine
            .run_source(SourceType::File, false)
            .await
            .unwrap()
            .is_empty());
        append(lines[2]);
        let done = engine.run_source(SourceType::File, true).await.unwrap();
        assert_eq!(done.len(), 1, "three reads, one window: {done:?}");
        assert!(done[0].committed, "{done:?}");
        assert_eq!(done[0].record_count, 3);

        let total: u64 = lines.iter().map(|l| l.len() as u64).sum();
        let last_read_start = (lines[0].len() + lines[1].len()) as u64;
        let reads = reads.lock().unwrap().clone();
        assert_eq!(reads.len(), 3, "the window was built from three reads");

        let rows = engine.store.list_batches().await.unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        let bytes = engine
            .backend
            .get_object(row.manifest_uri.as_deref().unwrap())
            .await
            .unwrap();
        let manifest: vtop_core::manifest::VtopManifest = serde_json::from_slice(&bytes).unwrap();
        manifest.verify_self_hash().unwrap();
        let ProgressMarker::File {
            start_byte,
            end_byte,
            ..
        } = &manifest.source_progress
        else {
            panic!("a file manifest: {:?}", manifest.source_progress);
        };
        assert_eq!(
            (*start_byte, *end_byte),
            (0, total),
            "the manifest names the bytes its object holds, from the first read's \
             start to the last read's end (the last read alone is {last_read_start}..{total})"
        );
        let token = format!("-b0-{total}-");
        assert!(
            row.batch_id.contains(&token) && manifest.batch_id == row.batch_id,
            "the batch id carries the manifest's range {token}: {}",
            row.batch_id
        );
        assert!(
            row.object_uri.as_deref().unwrap().contains(&token),
            "and so does the object key: {:?}",
            row.object_uri
        );

        // The ledger and the adapter keep the last read's marker, exactly as
        // the adapter issued it: that is what the file adapter settles a
        // commit by.
        let ProgressMarker::File {
            start_byte: ledger_start,
            ..
        } = &row.progress_start
        else {
            panic!("file markers");
        };
        assert_eq!(*ledger_start, 0, "the ledger's start is the first read's");
        assert_eq!(
            row.progress_end, reads[2],
            "the ledger's end is the last read's marker, unchanged"
        );
        assert_eq!(
            *commits.lock().unwrap(),
            vec![reads[2].clone()],
            "the adapter is handed the last read's marker, not the manifest's range"
        );
    }

    /// One read of `src` through `adapter`, which yields exactly one unit.
    async fn read_one(adapter: &mut FileSource, src: &DiscoveredSource) -> ReadResult {
        let mut reads = adapter
            .read_batch_candidates(src, 10_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(reads.len(), 1);
        reads.remove(0)
    }

    /// #622, the other half: the marker a multi-read window commits is the
    /// last read's own, so the file adapter finds that read on record and
    /// settles the commit in process. Were it handed the manifest's range
    /// (first start..last end), no read would match, the commit would be taken
    /// for one recovered after a restart, and the read head - already past the
    /// window, on the next one's bytes - would be pulled back to the window's
    /// end and those bytes read again.
    #[tokio::test]
    async fn the_marker_a_window_commits_is_settled_by_the_file_adapter_in_process() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        let path = input.to_string_lossy().into_owned();
        let append = |line: &str| {
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&input)
                .unwrap();
            f.write_all(line.as_bytes()).unwrap();
        };
        let src = DiscoveredSource {
            source_type: SourceType::File,
            source_name: path.clone(),
            format: vtop_core::types::TelemetryFormat::Raw,
        };
        let mut adapter = FileSource::new(
            vec![path.clone()],
            vtop_core::types::TelemetryFormat::Raw,
            false,
        );
        let mut buf = PendingBuffer::new("t", src.clone(), BatchLimits::default());
        for line in ["a\n", "bb\n", "ccc\n"] {
            append(line);
            buf.append(read_one(&mut adapter, &src).await);
        }
        let window = buf.drain().unwrap();
        let byte_range = |m: &ProgressMarker| match m {
            ProgressMarker::File {
                start_byte,
                end_byte,
                ..
            } => (*start_byte, *end_byte),
            other => panic!("a file marker: {other:?}"),
        };
        assert_eq!(byte_range(&window_marker(&window)), (0, 9));
        assert_eq!(byte_range(&window.progress_end), (5, 9));

        // The next window has started reading before this one commits.
        append("dddd\n");
        let ahead = read_one(&mut adapter, &src).await;
        assert_eq!(byte_range(&ahead.progress_end), (9, 14));

        adapter.commit_progress(&window.progress_end).await.unwrap();

        append("eeeee\n");
        let after = read_one(&mut adapter, &src).await;
        assert_eq!(
            byte_range(&after.progress_end),
            (14, 20),
            "the commit settled in process: the read head stayed past the window \
             and nothing was read twice"
        );
    }

    /// #622 for Kafka: a window of three reads of one partition, each marker
    /// shaped as the Kafka adapter builds it (an end marker carries its own
    /// read's start). The manifest and batch id span the first read's first
    /// offset to the last read's last; the commit marker stays the last
    /// read's.
    #[test]
    fn a_kafka_window_of_several_reads_names_its_first_to_last_offset() {
        let source = DiscoveredSource {
            source_type: SourceType::Kafka,
            source_name: "t".into(),
            format: vtop_core::types::TelemetryFormat::Raw,
        };
        let mut buf = PendingBuffer::new("default", source, BatchLimits::default());
        let mk = |start: i64, end: i64| ProgressMarker::Kafka {
            topic: "t".into(),
            partition: 3,
            start_offset: start,
            end_offset: end,
            consumer_group: "g".into(),
        };
        for (start, end) in [(100, 149), (150, 199), (200, 249)] {
            buf.append(ReadResult {
                records: vec![b"r".to_vec()],
                progress_start: mk(start, start),
                progress_end: mk(start, end),
                first_timestamp: None,
                last_timestamp: None,
                verbatim: false,
            });
        }
        let drained = buf.drain().unwrap();
        let window = window_marker(&drained);
        assert_eq!(
            window,
            mk(100, 249),
            "first read's start to last read's end"
        );
        assert_eq!(
            drained.progress_end,
            mk(200, 249),
            "the commit marker is the last read's, unchanged"
        );
        let id = vtop_core::batch::build_batch_id("t", &window);
        assert!(id.contains("-p3-100-249-"), "{id}");
    }

    /// #558: the engine restarts after the file was rotated while it was down.
    /// The ledger remembers the OLD file's high-water mark; the cursor must be
    /// seeded with the inode that mark belongs to, so the new file is read from
    /// its first byte rather than from the old file's offset.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_file_rotated_while_the_engine_was_down_is_read_from_its_first_byte() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("app.log");
        std::fs::write(&input, "old 1\nold 2\n").unwrap(); // 12 bytes
        let url = format!("sqlite://{}", dir.path().join("state.db").display());
        let config = |work: &str| {
            let mut cfg = file_config(
                dir.path().join(work).to_str().unwrap(),
                &url,
                vec![input.to_string_lossy().into_owned()],
                "mock",
            );
            cfg.batching.max_batch_age_seconds = 3_600;
            cfg
        };

        {
            let mut first = Engine::new(config("work-1"), StreamsConfig { streams: vec![] })
                .await
                .unwrap();
            let done = first.process_once(SourceType::File).await.unwrap();
            assert!(done[0].committed);
        } // the engine stops

        std::fs::rename(&input, dir.path().join("app.log.1")).unwrap();
        let fresh: String = (0..40).map(|i| format!("new {i}\n")).collect();
        assert!(fresh.len() > 12, "longer than the old high-water mark");
        std::fs::write(&input, &fresh).unwrap();

        let mut second = Engine::new(config("work-2"), StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        second.recover().await.unwrap();
        let done = second.process_once(SourceType::File).await.unwrap();
        assert!(done[0].committed);
        assert_eq!(
            done[0].record_count, 40,
            "all 40 lines of the new file; the old mark (byte 12) must not skip its start"
        );
    }

    #[test]
    fn a_buffer_continues_only_a_read_that_starts_where_it_ended_in_the_same_file() {
        let file = |inode: Option<u64>, start: u64, end: u64| ProgressMarker::File {
            path: "/a".into(),
            inode,
            start_byte: start,
            end_byte: end,
            file_size: 0,
            mtime: String::new(),
            content_fingerprint: None,
            tail_fingerprint: None,
        };
        let limits = BatchLimits::default();
        let src = DiscoveredSource {
            source_type: SourceType::File,
            source_name: "/a".into(),
            format: vtop_core::types::TelemetryFormat::Raw,
        };
        let mut buf = PendingBuffer::new("t", src, limits);
        assert!(
            buf.continues(&file(Some(1), 0, 5)),
            "an empty buffer takes anything"
        );
        buf.append(ReadResult {
            records: vec![b"x".to_vec()],
            progress_start: file(Some(1), 0, 0),
            progress_end: file(Some(1), 0, 10),
            first_timestamp: None,
            last_timestamp: None,
            verbatim: false,
        });
        assert!(
            buf.continues(&file(Some(1), 10, 10)),
            "picks up at byte 10, same inode"
        );
        assert!(
            buf.continues(&file(None, 10, 10)),
            "an unknown inode proves nothing"
        );
        assert!(
            !buf.continues(&file(Some(1), 0, 0)),
            "restarted at 0: truncation"
        );
        assert!(
            !buf.continues(&file(Some(1), 4, 4)),
            "a gap or overlap is not a continuation"
        );
        assert!(
            !buf.continues(&file(Some(2), 10, 10)),
            "another file took the path"
        );
    }

    /// A stop request fed by the test instead of the process's signals, so
    /// shutdown can be asked for at an exact point in a cycle. Like the OS
    /// streams, a request sent while the engine is busy waits in the channel
    /// for the next `next_request`.
    struct ChannelShutdown(tokio::sync::mpsc::UnboundedReceiver<&'static str>);

    #[async_trait::async_trait]
    impl ShutdownRequests for ChannelShutdown {
        async fn next_request(&mut self) -> &'static str {
            match self.0.recv().await {
                Some(cause) => cause,
                // The test dropped its sender: park, as the contract requires.
                None => std::future::pending().await,
            }
        }

        fn pending_request(&mut self) -> Option<&'static str> {
            // Empty and disconnected both read as "nothing asked": a dropped
            // sender is not a stop.
            self.0.try_recv().ok()
        }
    }

    /// Wraps the engine's real adapter and holds every cycle at its start
    /// (`discover_sources`) until the test hands out a permit, announcing
    /// each arrival first. A cycle — the normal loop's or the shutdown
    /// flush's — is therefore "mid-flight" for exactly as long as the test
    /// says, and the count of arrivals says how many cycles ran.
    struct GatedAdapter {
        inner: Box<dyn SourceAdapter>,
        arrived: tokio::sync::mpsc::UnboundedSender<()>,
        permits: Arc<tokio::sync::Semaphore>,
    }

    #[async_trait::async_trait]
    impl SourceAdapter for GatedAdapter {
        async fn discover_sources(&self) -> Result<Vec<DiscoveredSource>, VtopError> {
            let _ = self.arrived.send(());
            self.permits
                .acquire()
                .await
                .expect("the gate is never closed")
                .forget();
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
            self.inner.commit_progress(marker).await
        }
        async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
            self.inner.replay_from_marker(marker).await
        }
        fn source_type(&self) -> SourceType {
            self.inner.source_type()
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            // Recovery downcasts to the concrete adapter to seed cursors.
            self.inner.as_any_mut()
        }
    }

    /// An engine over one file holding `lines`, with thresholds no test here
    /// trips (so data is only ever sealed by the shutdown flush), its file
    /// adapter behind a [`GatedAdapter`], and a mock backend.
    async fn gated_engine(
        dir: &std::path::Path,
        lines: &[&str],
        idle_poll_interval_ms: u64,
    ) -> (
        Engine,
        tokio::sync::mpsc::UnboundedReceiver<()>,
        Arc<tokio::sync::Semaphore>,
    ) {
        let input = dir.join("in.log");
        let mut body = String::new();
        for line in lines {
            body.push_str(line);
            body.push('\n');
        }
        std::fs::write(&input, body).unwrap();
        let mut cfg = file_config(
            dir.join("work").to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.max_records = 10_000;
        cfg.batching.max_bytes = 104_857_600;
        cfg.batching.max_batch_age_seconds = 3_600;
        cfg.batching.idle_poll_interval_ms = idle_poll_interval_ms;
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        engine.backend = Arc::new(vtop_upload::MockBackend::new());
        let inner = engine.adapters.remove(&SourceType::File).unwrap();
        let (arrived, arrivals) = tokio::sync::mpsc::unbounded_channel();
        let permits = Arc::new(tokio::sync::Semaphore::new(0));
        engine.adapters.insert(
            SourceType::File,
            Box::new(GatedAdapter {
                inner,
                arrived,
                permits: permits.clone(),
            }),
        );
        (engine, arrivals, permits)
    }

    /// REGRESSION (#526): `run` used to create a fresh `ctrl_c()` listener on
    /// every loop turn, so a SIGINT that arrived while a cycle was busy had
    /// no listener, was swallowed by tokio's handler, and the loaded engine
    /// kept running indefinitely. Here the stop is requested while a
    /// productive cycle is held open; the engine must finish that cycle, run
    /// the shutdown flush as the very next thing, and return `Ok`.
    #[tokio::test]
    async fn a_stop_requested_mid_cycle_is_honoured_after_that_cycle_with_the_flush() {
        let dir = tempfile::tempdir().unwrap();
        // A long idle interval: if the stop were missed, nothing but another
        // cycle or a minute's sleep could follow, and the timeout would fire.
        let (mut engine, mut arrivals, permits) = gated_engine(
            dir.path(),
            &["buffered, never sealed by a threshold"],
            60_000,
        )
        .await;
        let (stop, requests) = tokio::sync::mpsc::unbounded_channel();

        let driver = async {
            arrivals.recv().await.expect("the first cycle starts");
            // The cycle is mid-flight: this is the moment the old loop had
            // no listener for.
            stop.send("test-stop").unwrap();
            permits.add_permits(1);
            arrivals
                .recv()
                .await
                .expect("the shutdown flush starts its own pass");
            permits.add_permits(1);
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::join!(engine.run_until(ChannelShutdown(requests)), driver)
        })
        .await
        .expect(
            "a stop requested mid-cycle must end the run after that cycle; a lost \
             request leaves `vtopctl run` running until SIGKILL (#526)",
        );

        assert!(
            result.is_ok(),
            "an orderly stop is a successful exit, not an error: {result:?}"
        );
        assert!(
            permits.available_permits() == 0 && arrivals.try_recv().is_err(),
            "exactly two passes must run — the held cycle and the flush; another \
             normal cycle in between means the stop was not observed at the next \
             select, the window a busy engine kept missing (#526)"
        );
        let batches = engine.store.list_batches().await.unwrap();
        assert_eq!(
            batches.len(),
            1,
            "the shutdown flush must seal the buffered record into one batch; \
             none means the stop skipped the flush and left the data for the \
             next start to re-read"
        );
        assert_eq!(batches[0].state, BatchState::SourceCommitted);
        assert_eq!(batches[0].record_count, Some(1));
    }

    /// An idle engine is asleep in the backoff between cycles; a stop must
    /// interrupt that sleep rather than wait out the idle interval.
    #[tokio::test]
    async fn an_idle_engine_stops_promptly_without_waiting_out_its_backoff() {
        let dir = tempfile::tempdir().unwrap();
        // Ten minutes of backoff: only an interrupted sleep ends this test
        // inside its timeout.
        let (mut engine, mut arrivals, permits) = gated_engine(dir.path(), &[], 600_000).await;
        let (stop, requests) = tokio::sync::mpsc::unbounded_channel();

        let driver = async {
            arrivals.recv().await.expect("the first cycle starts");
            permits.add_permits(1);
            // Let the empty cycle finish and the engine settle into its
            // backoff before asking it to stop.
            tokio::time::sleep(Duration::from_millis(200)).await;
            stop.send("test-stop").unwrap();
            arrivals.recv().await.expect("the shutdown flush starts");
            permits.add_permits(1);
        };
        let started = Instant::now();
        let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::join!(engine.run_until(ChannelShutdown(requests)), driver)
        })
        .await
        .expect(
            "an idle engine must stop when asked, not after its idle interval; a \
             stop that waits out the backoff turns every orderly stop into a \
             grace-period SIGKILL",
        );

        assert!(
            result.is_ok(),
            "an orderly idle stop exits cleanly: {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the stop must interrupt the ten-minute backoff, not wait it out"
        );
        assert!(
            engine.store.list_batches().await.unwrap().is_empty(),
            "an idle engine has nothing to flush"
        );
    }

    /// A second stop while the shutdown flush is running abandons the flush
    /// and reports an error (#526): it is how an operator ends a flush that is
    /// itself stuck without reaching for SIGKILL. Abandoning must commit
    /// nothing — the held data stays for the next start to re-read.
    #[tokio::test]
    async fn a_second_stop_during_the_shutdown_flush_abandons_it_with_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let (mut engine, mut arrivals, permits) =
            gated_engine(dir.path(), &["held when the flush is abandoned"], 60_000).await;
        let (stop, requests) = tokio::sync::mpsc::unbounded_channel();

        let driver = async {
            arrivals.recv().await.expect("the first cycle starts");
            stop.send("first-stop").unwrap();
            permits.add_permits(1);
            // The flush is now held at its start, as a flush stuck on an
            // unreachable store would be. No permit is ever granted for it.
            arrivals.recv().await.expect("the shutdown flush starts");
            stop.send("second-stop").unwrap();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::join!(engine.run_until(ChannelShutdown(requests)), driver)
        })
        .await
        .expect(
            "a second stop must end a stuck shutdown flush promptly; otherwise the \
             only way out of a hung flush is SIGKILL",
        );

        let error = result.expect_err(
            "an abandoned flush is not an orderly stop: exiting 0 would tell a \
             supervisor the data was flushed when it was not",
        );
        assert!(
            error.to_string().contains("second-stop"),
            "the error must name the request that abandoned the flush, got: {error}"
        );
        assert!(
            engine.store.list_batches().await.unwrap().is_empty(),
            "the abandoned flush must not have sealed or committed anything"
        );
    }

    /// REGRESSION (#532 review): SIGINT and SIGTERM are separate streams, so
    /// both landing while a pass is busy leaves TWO requests pending. The
    /// first ended the loop and the second then resolved the flush's
    /// abandon branch at once: the engine exited 1 without flushing, though
    /// nothing was sent after the shutdown began. Requests queued before the
    /// stop is observed belong to that stop and must be drained.
    #[tokio::test]
    async fn stops_queued_before_the_first_is_observed_do_not_abandon_the_flush() {
        let dir = tempfile::tempdir().unwrap();
        let (mut engine, mut arrivals, permits) =
            gated_engine(dir.path(), &["flushed despite two queued stops"], 60_000).await;
        let (stop, requests) = tokio::sync::mpsc::unbounded_channel();

        // Grant every pass; the run's own return ends the race, so an
        // abandoned flush surfaces as the `Err` asserted below, not a hang.
        let driver = async {
            arrivals.recv().await.expect("the first cycle starts");
            // Both kinds arrive while the pass is held open, as a Ctrl-C
            // followed by a supervisor's SIGTERM would.
            stop.send("SIGINT").unwrap();
            stop.send("SIGTERM").unwrap();
            permits.add_permits(1);
            while arrivals.recv().await.is_some() {
                permits.add_permits(1);
            }
            std::future::pending::<()>().await
        };
        let result = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::select! {
                result = engine.run_until(ChannelShutdown(requests)) => result,
                () = driver => unreachable!("the driver never finishes"),
            }
        })
        .await
        .expect("two queued stops must end the run");

        assert!(
            result.is_ok(),
            "a stop request queued BEFORE the shutdown began is not a second signal; \
             reading it as one abandons the flush and exits 1 for an orderly stop: \
             {result:?}"
        );
        let batches = engine.store.list_batches().await.unwrap();
        assert_eq!(
            batches.len(),
            1,
            "the flush must have run to completion and sealed the buffered record"
        );
        assert_eq!(batches[0].state, BatchState::SourceCommitted);
    }

    /// REGRESSION (#532 review): the loop only looked for a stop after a full
    /// cycle, so a signal that arrived during `recover()` — kept pending by
    /// the persistent stream — still bought one normal processing pass over
    /// every adapter first. A stop pending when the loop starts must go
    /// straight to the shutdown flush.
    #[tokio::test]
    async fn a_stop_pending_when_the_run_starts_goes_straight_to_the_flush() {
        let dir = tempfile::tempdir().unwrap();
        let (mut engine, mut arrivals, permits) =
            gated_engine(dir.path(), &["sealed only by the flush"], 60_000).await;
        let (stop, requests) = tokio::sync::mpsc::unbounded_channel();
        // Sent before `run_until` is even called: from the loop's point of
        // view it arrived during recovery.
        stop.send("during-recovery").unwrap();

        // Grant every pass that arrives and count them; this never finishes,
        // so the run's own return ends the race.
        let passes = std::sync::atomic::AtomicUsize::new(0);
        let driver = async {
            while arrivals.recv().await.is_some() {
                passes.fetch_add(1, Ordering::SeqCst);
                permits.add_permits(1);
            }
            std::future::pending::<()>().await
        };
        let result = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::select! {
                result = engine.run_until(ChannelShutdown(requests)) => result,
                () = driver => unreachable!("the driver never finishes"),
            }
        })
        .await
        .expect("a stop pending at start must end the run");

        assert!(result.is_ok(), "an orderly stop exits cleanly: {result:?}");
        assert_eq!(
            passes.load(Ordering::SeqCst),
            1,
            "only the shutdown flush may run; a second pass means a normal cycle \
             ran first, delaying a stop sent during recovery by a whole cycle over \
             every adapter"
        );
        assert_eq!(
            engine.store.list_batches().await.unwrap().len(),
            1,
            "the one pass must have been the flush, sealing the buffered record"
        );
    }

    /// Run `engine` with a stop already pending, granting every pass, and
    /// return what `run_until` returned. Only the shutdown flush runs.
    async fn run_straight_to_the_flush(
        engine: &mut Engine,
        mut arrivals: tokio::sync::mpsc::UnboundedReceiver<()>,
        permits: Arc<tokio::sync::Semaphore>,
    ) -> Result<(), VtopError> {
        let (stop, requests) = tokio::sync::mpsc::unbounded_channel();
        stop.send("test-stop").unwrap();
        let driver = async {
            while arrivals.recv().await.is_some() {
                permits.add_permits(1);
            }
            std::future::pending::<()>().await
        };
        tokio::time::timeout(Duration::from_secs(20), async {
            tokio::select! {
                result = engine.run_until(ChannelShutdown(requests)) => result,
                () = driver => unreachable!("the driver never finishes"),
            }
        })
        .await
        .expect("a stop pending at start must end the run")
    }

    /// REGRESSION (#532 review): a batch the shutdown flush could not archive
    /// (here verification fails, as it would against a store that keeps
    /// refusing it) ends FAILED and `run_source` returns `Ok` with that
    /// outcome. The flush ignored outcomes, so it logged `shutdown flush
    /// complete` and `run` exited 0 with the buffered data unarchived: a
    /// supervisor read an incomplete shutdown as a clean one.
    #[tokio::test]
    async fn a_batch_that_fails_in_the_shutdown_flush_is_not_a_clean_stop() {
        let dir = tempfile::tempdir().unwrap();
        let (mut engine, arrivals, permits) =
            gated_engine(dir.path(), &["refused by the store at shutdown"], 60_000).await;
        engine.backend = Arc::new(vtop_upload::MockBackend::failing());

        let result = run_straight_to_the_flush(&mut engine, arrivals, permits).await;

        let error = result.expect_err(
            "a flush that left a batch FAILED did not flush the buffered data; \
             exiting 0 tells a supervisor it did",
        );
        assert!(
            error.to_string().contains("shutdown flush incomplete"),
            "the error must say the flush did not complete, got: {error}"
        );
        let batches = engine.store.list_batches().await.unwrap();
        assert_eq!(batches.len(), 1, "the flush sealed the buffered record");
        // FAILED, then REPLAY_REQUIRED (#627): marked for replay, not committed.
        assert_eq!(
            batches[0].state,
            BatchState::ReplayRequired,
            "the batch never verified, so its source progress must not commit"
        );
    }

    /// Delegates to the real adapter except that discovery always fails, so
    /// `run_source` itself returns `Err`.
    struct DiscoveryFails(Box<dyn SourceAdapter>);

    #[async_trait::async_trait]
    impl SourceAdapter for DiscoveryFails {
        async fn discover_sources(&self) -> Result<Vec<DiscoveredSource>, VtopError> {
            Err(VtopError::Source("test: discovery unavailable".into()))
        }
        async fn read_batch_candidates(
            &mut self,
            source: &DiscoveredSource,
            max_records: usize,
            max_bytes: usize,
            max_wait: Duration,
        ) -> Result<Vec<ReadResult>, VtopError> {
            self.0
                .read_batch_candidates(source, max_records, max_bytes, max_wait)
                .await
        }
        async fn commit_progress(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
            self.0.commit_progress(marker).await
        }
        async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
            self.0.replay_from_marker(marker).await
        }
        fn source_type(&self) -> SourceType {
            self.0.source_type()
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self.0.as_any_mut()
        }
    }

    /// REGRESSION (#532 review): a source type whose flush pass returned
    /// `Err` was only logged, and the flush still reported complete and
    /// exited 0. The error must be returned, naming its cause.
    #[tokio::test]
    async fn a_source_pass_that_errors_in_the_shutdown_flush_is_not_a_clean_stop() {
        let dir = tempfile::tempdir().unwrap();
        let (mut engine, arrivals, permits) =
            gated_engine(dir.path(), &["never read at shutdown"], 60_000).await;
        // Discovery fails before the gate, so no pass ever arrives: the
        // driver only needs to exist.
        let inner = engine.adapters.remove(&SourceType::File).unwrap();
        engine
            .adapters
            .insert(SourceType::File, Box::new(DiscoveryFails(inner)));

        let result = run_straight_to_the_flush(&mut engine, arrivals, permits).await;

        let error = result.expect_err(
            "a source pass that failed during the flush flushed nothing; exiting 0 \
             tells a supervisor it did",
        );
        let text = error.to_string();
        assert!(
            text.contains("shutdown flush incomplete") && text.contains("discovery unavailable"),
            "the error must say the flush did not complete and why, got: {text}"
        );
    }

    /// Delegates to the real adapter except that every read fails once
    /// `failing` is set, so a pass reads nothing but still flushes what is
    /// already buffered.
    struct ReadsFail {
        inner: Box<dyn SourceAdapter>,
        failing: Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait::async_trait]
    impl SourceAdapter for ReadsFail {
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
            if self.failing.load(Ordering::SeqCst) {
                return Err(VtopError::Source("test: source unreadable".into()));
            }
            self.inner
                .read_batch_candidates(source, max_records, max_bytes, max_wait)
                .await
        }
        async fn commit_progress(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
            self.inner.commit_progress(marker).await
        }
        async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
            self.inner.replay_from_marker(marker).await
        }
        fn source_type(&self) -> SourceType {
            self.inner.source_type()
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self.inner.as_any_mut()
        }
    }

    /// The flush's promise is the data the engine already holds: every
    /// buffered window is sealed, verified and committed. A source that
    /// cannot be READ during the flush pass breaks no part of that. Its
    /// unread data was never taken from the source and its progress is not
    /// committed, so the next start reads it, exactly like data that arrives
    /// after the flush; the read error is logged by `run_cycle`. Exiting 1
    /// for it would turn a file rotated away between discovery and read into
    /// a failed shutdown. (#532 review: a read error is not an incomplete
    /// flush, while a failed batch or an `Err` pass is.)
    #[tokio::test]
    async fn a_read_error_in_the_shutdown_flush_still_flushes_the_buffer_and_exits_cleanly() {
        let dir = tempfile::tempdir().unwrap();
        let (mut engine, mut arrivals, permits) =
            gated_engine(dir.path(), &["buffered before the source broke"], 60_000).await;
        let failing = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let inner = engine.adapters.remove(&SourceType::File).unwrap();
        engine.adapters.insert(
            SourceType::File,
            Box::new(ReadsFail {
                inner,
                failing: failing.clone(),
            }),
        );
        let (stop, requests) = tokio::sync::mpsc::unbounded_channel();

        let driver = async {
            // First pass reads the line into a buffer that no threshold seals.
            arrivals.recv().await.expect("the first cycle starts");
            stop.send("test-stop").unwrap();
            permits.add_permits(1);
            // The flush pass: its read fails, the buffer must still flush.
            arrivals.recv().await.expect("the shutdown flush starts");
            failing.store(true, Ordering::SeqCst);
            permits.add_permits(1);
            std::future::pending::<()>().await
        };
        let result = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::select! {
                result = engine.run_until(ChannelShutdown(requests)) => result,
                () = driver => unreachable!("the driver never finishes"),
            }
        })
        .await
        .expect("the run must end after the flush");

        assert!(
            result.is_ok(),
            "an unreadable source left nothing buffered unarchived: {result:?}"
        );
        let batches = engine.store.list_batches().await.unwrap();
        assert_eq!(
            batches.len(),
            1,
            "the buffered window must be sealed despite the failed read"
        );
        assert_eq!(batches[0].state, BatchState::SourceCommitted);
        assert_eq!(batches[0].record_count, Some(1));
    }

    /// REGRESSION (#532 review): with ledger retention on, a stop that
    /// arrived during the last pass of an IDLE cycle was not looked at until
    /// the backoff `select!`, after `maybe_prune_ledger` — housekeeping that
    /// can spend a state-store timeout plus digest I/O — had run. A stop
    /// already pending must skip the prune and go to the flush.
    #[tokio::test]
    async fn a_stop_during_an_idle_pass_does_not_wait_for_ledger_pruning() {
        let dir = tempfile::tempdir().unwrap();
        let (mut engine, mut arrivals, permits) = gated_engine(dir.path(), &[], 60_000).await;
        engine.config.engine.ledger_retention_days = Some(1);
        engine.config.engine.ledger_digest_dir =
            Some(dir.path().join("digests").to_string_lossy().into_owned());
        let (stop, requests) = tokio::sync::mpsc::unbounded_channel();

        let driver = async {
            arrivals
                .recv()
                .await
                .expect("the first (idle) cycle starts");
            // The only adapter's pass is held open: this is the last pass of
            // the cycle, and it will read nothing.
            stop.send("test-stop").unwrap();
            permits.add_permits(1);
            while arrivals.recv().await.is_some() {
                permits.add_permits(1);
            }
            std::future::pending::<()>().await
        };
        let result = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::select! {
                result = engine.run_until(ChannelShutdown(requests)) => result,
                () = driver => unreachable!("the driver never finishes"),
            }
        })
        .await
        .expect("a stop during an idle pass must end the run");

        assert!(
            result.is_ok(),
            "an orderly idle stop exits cleanly: {result:?}"
        );
        assert!(
            engine.last_ledger_prune.is_none(),
            "the ledger prune ran after the stop was already pending; it can take \
             the state-store timeout and push the stop past its grace period"
        );
    }

    /// #639: a range whose replay decision recovery could not record is HELD:
    /// it produces no rewind, survives a rebase, and holds back a VERIFIED
    /// range above it with the same inode, content and partition rules as a
    /// range that is being re-read.
    #[test]
    fn a_held_range_holds_back_a_verified_range_above_it_without_a_rewind() {
        let page = |tag: u8| ContentFingerprint::of(&[tag; 5_000]);
        let file =
            |inode: u64, start: u64, content: Option<ContentFingerprint>| ProgressMarker::File {
                path: "/a".into(),
                inode: Some(inode),
                start_byte: start,
                end_byte: start + 100,
                file_size: 8_192,
                mtime: String::new(),
                content_fingerprint: content,
                tail_fingerprint: None,
            };
        let mut plan = RewindPlan::default();
        plan.hold(&file(1, 0, page(b'A')));
        assert!(plan.markers().is_empty(), "a held range is not rewound");
        assert!(plan.holds_back(&file(1, 100, page(b'A'))));
        assert!(
            plan.holds_back(&file(1, 100, None)),
            "content that cannot be compared"
        );
        assert!(!plan.holds_back(&file(1, 0, page(b'A'))), "the same start");
        assert!(!plan.holds_back(&file(2, 100, page(b'A'))), "another inode");
        assert!(
            !plan.holds_back(&file(1, 100, page(b'B'))),
            "content proven to be another file's"
        );
        // A rewind of the same file that lands ABOVE the verified range does
        // not release the hold: it says nothing about the held range's bytes.
        plan.add(&file(1, 300, page(b'A')));
        plan.rebase(&HashMap::from([(
            rewind_key(&file(1, 300, page(b'A'))),
            300,
        )]));
        assert_eq!(plan.markers().len(), 1, "only the added range rewinds");
        assert!(
            plan.holds_back(&file(1, 100, page(b'A'))),
            "the held [0,100) still holds back [100,200) after the rebase"
        );

        let kafka = |group: &str, partition: i32, start: i64| ProgressMarker::Kafka {
            topic: "t".into(),
            partition,
            start_offset: start,
            end_offset: start + 9,
            consumer_group: group.into(),
        };
        let mut plan = RewindPlan::default();
        plan.hold(&kafka("g", 0, 10));
        assert!(plan.markers().is_empty());
        assert!(plan.holds_back(&kafka("g", 0, 20)));
        assert!(!plan.holds_back(&kafka("g", 0, 10)), "the same offsets");
        assert!(!plan.holds_back(&kafka("g", 1, 20)), "another partition");
        assert!(!plan.holds_back(&kafka("h", 0, 20)), "another group");
    }

    /// Where a batch starts and ends within its unit: bytes for a file,
    /// offsets for Kafka (made half-open like bytes, `end_offset + 1`).
    fn unit_range(row: &BatchRecord) -> (i128, i128) {
        let end = match &row.progress_end {
            ProgressMarker::Kafka { end_offset, .. } => i128::from(*end_offset) + 1,
            ProgressMarker::File { end_byte, .. }
            | ProgressMarker::SyslogSpool { end_byte, .. } => i128::from(*end_byte),
        };
        (start_position(&row.progress_start), end)
    }

    /// The ledger's row whose range starts at `start`, from the first
    /// attempt at it (the oldest).
    async fn row_at(engine: &Engine, start: i128) -> BatchRecord {
        let mut rows: Vec<_> = engine
            .store
            .list_batches()
            .await
            .unwrap()
            .into_iter()
            .filter(|b| start_position(&b.progress_start) == start)
            .collect();
        rows.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        rows.into_iter()
            .next()
            .unwrap_or_else(|| panic!("the ledger has no row starting at {start}"))
    }

    /// Asserts the SOURCE_COMMITTED rows cover `[0, len)` with no gap. A
    /// range archived twice (a VERIFIED row re-read before it committed) is
    /// a duplicate, which replay allows; a hole is lost data.
    async fn assert_committed_without_a_gap(case: &str, engine: &Engine, len: i128) {
        let mut ranges: Vec<(i128, i128)> = engine
            .store
            .list_batches()
            .await
            .unwrap()
            .iter()
            .filter(|b| b.state == BatchState::SourceCommitted)
            .map(unit_range)
            .collect();
        ranges.sort_unstable();
        let mut covered = 0;
        for &(start, end) in &ranges {
            assert!(
                start <= covered,
                "{case}: [{covered}, {start}) was never archived; committed ranges: {ranges:?}"
            );
            covered = covered.max(end);
        }
        assert_eq!(
            covered, len,
            "{case}: the committed ranges stop short of the source's end: {ranges:?}"
        );
    }

    /// Which of recovery's four replay-decision writes fails for the lower
    /// range (#639), and what that range was in the ledger.
    #[derive(Clone, Copy, Debug)]
    enum LostDecision {
        /// A VERIFIED row that fails its storage re-check; FAILED is refused.
        RecheckFailed,
        /// The same; FAILED lands and REPLAY_REQUIRED is refused.
        RecheckReplayRequired,
        /// A row left mid-pipeline (BATCHING); FAILED is refused.
        DecisionFailed,
        /// The same; FAILED lands and REPLAY_REQUIRED is refused.
        DecisionReplayRequired,
    }

    impl LostDecision {
        /// The lower row is VERIFIED (and fails its re-check) rather than
        /// left mid-pipeline.
        fn lower_verified(self) -> bool {
            matches!(self, Self::RecheckFailed | Self::RecheckReplayRequired)
        }

        fn refused(self) -> LedgerWrite {
            match self {
                Self::RecheckFailed | Self::DecisionFailed => LedgerWrite::To(BatchState::Failed),
                Self::RecheckReplayRequired | Self::DecisionReplayRequired => {
                    LedgerWrite::To(BatchState::ReplayRequired)
                }
            }
        }
    }

    /// Reads whatever `adapter` has for its first source and drops it: the
    /// read a process made before it crashed with that range in flight. The
    /// next read starts after it, as it did for that process.
    async fn read_and_drop(adapter: &mut dyn SourceAdapter) {
        let sources = adapter.discover_sources().await.unwrap();
        let reads = adapter
            .read_batch_candidates(&sources[0], 10_000, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert!(
            reads.iter().any(|r| !r.records.is_empty()),
            "the in-flight range is read"
        );
    }

    /// The ledger a crash leaves behind (#639): a lower range A of a source
    /// unit that recovery must replay, and a VERIFIED range B above it. Built
    /// by a first engine, `first`, whose `source` reads A, then (after
    /// `grow`) B. A is left VERIFIED or mid-pipeline per `fault`; B is
    /// VERIFIED, its commit refused.
    async fn leave_a_lower_range_and_a_verified_one_above_it(
        first: &mut Engine,
        source: SourceType,
        fault: LostDecision,
        grow: impl FnOnce(),
    ) {
        use BatchState::*;
        use LedgerWrite::To;
        // A: its pipeline stops before it is committed. A VERIFIED A reads on
        // past it; a mid-pipeline A is handed back by the running cycle, and
        // the crashed process's read of it is replayed by hand.
        let a_stops_at = if fault.lower_verified() {
            To(SourceCommitted)
        } else {
            To(Sealed)
        };
        // A VERIFIED A whose commit write failed is retried by the running
        // engine after its next pass (#624); the crash this fixture stands
        // for comes first, so that retry is refused as well.
        let mut refused = vec![a_stops_at, To(SourceCommitted)];
        if fault.lower_verified() {
            refused.push(To(SourceCommitted));
        }
        FaultyLedger::install(first, &refused).await;
        assert!(
            first.process_once(source.clone()).await.is_err(),
            "A's write fails"
        );
        if !fault.lower_verified() {
            read_and_drop(first.adapters.get_mut(&source).unwrap().as_mut()).await;
        }
        grow();
        assert!(
            first.process_once(source).await.is_err(),
            "B verifies and its commit write fails"
        );
    }

    /// The four failure paths of [`Engine::recover`] for one file: the lower
    /// range A's replay decision cannot be recorded, so A is not rewound -
    /// and the VERIFIED range B above it must not commit either, or the next
    /// recovery's rewind for A is clamped to B's end and A is never archived.
    /// Once the store answers again, A is re-read and the archive has no gap.
    async fn a_lost_replay_decision_holds_back_the_file_range_above_it(fault: LostDecision) {
        let case = format!("{fault:?}");
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.log");
        std::fs::write(&input, "line 1\n").unwrap(); // A: [0, 7)
        let url = format!("sqlite://{}", dir.path().join("state.db").display());
        let backend = Arc::new(vtop_upload::MockBackend::new());
        // Each engine is a restart: the same ledger and object store, a fresh
        // process (and adapter) seeded from the ledger alone.
        let engine = |work: &str| {
            let mut cfg = file_config(
                dir.path().join(work).to_str().unwrap(),
                &url,
                vec![input.to_string_lossy().into_owned()],
                "mock",
            );
            cfg.batching.max_batch_age_seconds = 3_600;
            let backend = backend.clone();
            async move {
                let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
                    .await
                    .unwrap();
                engine.backend = backend;
                engine
            }
        };

        let mut first = engine("work-1").await;
        leave_a_lower_range_and_a_verified_one_above_it(
            &mut first,
            SourceType::File,
            fault,
            || {
                let mut f = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&input)
                    .unwrap();
                writeln!(f, "line 2").unwrap(); // B: [7, 14)
            },
        )
        .await;
        let a = row_at(&first, 0).await;
        let b = row_at(&first, 7).await;
        let a_state = if fault.lower_verified() {
            BatchState::Verified
        } else {
            BatchState::Batching
        };
        assert_eq!(a.state, a_state, "{case}: A as the crash left it");
        assert_eq!(unit_range(&b), (7, 14), "{case}");
        assert_eq!(
            b.state,
            BatchState::Verified,
            "{case}: B as the crash left it"
        );
        if fault.lower_verified() {
            // A's object no longer checks out: recovery must replay A.
            backend.corrupt(a.object_uri.as_deref().unwrap());
        }
        drop(first);

        // Restart 1: A's replay decision cannot be recorded.
        let mut second = engine("work-2").await;
        FaultyLedger::install(&mut second, &[fault.refused()]).await;
        let summary = second.recover().await.unwrap();
        assert_ne!(
            row_at(&second, 0).await.state,
            BatchState::ReplayRequired,
            "{case}: the refused write kept A's replay decision out of the ledger"
        );
        assert_eq!(
            row_at(&second, 7).await.state,
            BatchState::Verified,
            "{case}: B committed while A, below it, was neither rewound nor recorded; \
             its commit moves the committed point to 14 and A's next rewind is clamped there"
        );
        assert_eq!(summary.committed, 0, "{case}: {summary:?}");
        assert_eq!(
            summary.still_pending, 2,
            "{case}: A (no decision) and B (held back): {summary:?}"
        );
        drop(second);

        // Restart 2: the store answers. A's decision lands, A is re-read.
        let mut third = engine("work-3").await;
        third.recover().await.unwrap();
        assert_eq!(
            row_at(&third, 0).await.state,
            BatchState::ReplayRequired,
            "{case}: A's replay decision is recorded"
        );
        let reread = third.process_once(SourceType::File).await.unwrap();
        assert!(
            reread.iter().any(|o| o.committed),
            "{case}: the re-read commits: {reread:?}"
        );
        assert_committed_without_a_gap(&case, &third, 14).await;
        drop(third);

        // Restart 3: nothing below B is pending any more, so B commits.
        let mut fourth = engine("work-4").await;
        fourth.recover().await.unwrap();
        assert_eq!(
            row_at(&fourth, 7).await.state,
            BatchState::SourceCommitted,
            "{case}: the hold on B is released once A is archived"
        );
        assert_committed_without_a_gap(&case, &fourth, 14).await;
    }

    /// #671: a VERIFIED range whose manifest is from a newer release stays
    /// pending - not committed, not replayed - and holds back a VERIFIED range
    /// above it like any uncommitted range (#639). Once the file has committed
    /// past it, though (proven as for any range retired by #567), nothing
    /// below the committed point can be re-read: it holds nothing back, and
    /// it is retired SUPERSEDED instead of being claimed, downloaded and
    /// counted pending at every restart (review of #674). No source commit is
    /// made for it. Each meeting with a newer manifest is counted.
    #[tokio::test]
    async fn a_newer_release_range_the_file_committed_past_is_superseded() {
        use BatchState::*;
        let tenant = "newer-release-671";
        let unsupported = || -> u64 {
            let mx = telemetry::init().unwrap();
            mx.registry
                .gather()
                .iter()
                .filter(|f| f.name() == "vtop_recovery_unsupported_manifest_total")
                .flat_map(|f| f.get_metric().iter())
                .filter(|m| {
                    m.get_label()
                        .iter()
                        .any(|l| l.name() == "tenant" && l.value() == tenant)
                })
                .map(|m| m.get_counter().get_value() as u64)
                .sum()
        };
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.log");
        std::fs::write(&input, "line 1\n").unwrap(); // A: [0, 7)
        let url = format!("sqlite://{}", dir.path().join("state.db").display());
        let backend = Arc::new(vtop_upload::MockBackend::new());
        let engine = |work: &str| {
            let mut cfg = file_config(
                dir.path().join(work).to_str().unwrap(),
                &url,
                vec![input.to_string_lossy().into_owned()],
                "mock",
            );
            cfg.engine.tenant = tenant.into();
            cfg.batching.max_batch_age_seconds = 3_600;
            let backend = backend.clone();
            async move {
                let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
                    .await
                    .unwrap();
                engine.backend = backend;
                engine
            }
        };

        let mut first = engine("work-1").await;
        first.process_once(SourceType::File).await.unwrap();
        let a = row_at(&first, 0).await;
        assert_eq!(a.state, SourceCommitted);
        // N: the same bytes, left VERIFIED by a newer release's engine, its
        // manifest pinned and carrying the self-hash the ledger recorded.
        let newer_uri = "s3://telemetry-data/newer.manifest.json";
        let newer_hash = "ab".repeat(32);
        let staged = dir.path().join("newer-671.json");
        std::fs::write(
            &staged,
            serde_json::to_vec(&serde_json::json!({
                "protocol": "VTOP",
                "version": "0.9",
                "batch_id": "newer-release",
                "manifest": {"uri": newer_uri, "sha256": newer_hash},
            }))
            .unwrap(),
        )
        .unwrap();
        let stored = backend
            .put_manifest(&staged, newer_uri, None)
            .await
            .unwrap();
        let newer = BatchRecord {
            batch_id: "newer-release".into(),
            state: Batching,
            manifest_uri: Some(newer_uri.into()),
            manifest_sha256: Some(newer_hash.clone()),
            manifest_version_id: stored.version_id,
            owner: None,
            lease_expires_at: None,
            ..a.clone()
        };
        first.store.save_batch_state(&newer).await.unwrap();
        for state in [
            Sealed,
            Compressed,
            Checksummed,
            ObjectUploaded,
            ManifestUploaded,
            Verified,
        ] {
            first
                .store
                .update_batch_state(&newer.batch_id, state, &BatchPatch::default())
                .await
                .unwrap();
        }
        // B: [7, 14), VERIFIED, its commit refused.
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&input)
            .unwrap();
        writeln!(f, "line 2").unwrap();
        FaultyLedger::install(&mut first, &[LedgerWrite::To(SourceCommitted)]).await;
        assert!(
            first.process_once(SourceType::File).await.is_err(),
            "B verifies and its commit write fails"
        );
        assert_eq!(row_at(&first, 7).await.state, Verified);
        drop(first);

        let before = unsupported();
        let mut second = engine("work-2").await;
        let summary = second.recover().await.unwrap();
        assert_eq!(
            row_at(&second, 7).await.state,
            SourceCommitted,
            "B is held back by a range its file has committed past: {summary:?}"
        );
        let newer = second
            .store
            .get_batch("newer-release")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(newer.state, Superseded, "{summary:?}");
        assert_eq!(summary.superseded, 1, "{summary:?}");
        assert_eq!(summary.still_pending, 0, "{summary:?}");
        assert_eq!(summary.replay_required, 0, "{summary:?}");
        assert_eq!(unsupported() - before, 1, "the newer manifest is counted");
    }

    /// #639, `Engine::recover`'s stale-VERIFIED path: FAILED is refused.
    #[tokio::test]
    async fn a_stale_verified_range_whose_failed_write_fails_holds_back_the_range_above_it() {
        a_lost_replay_decision_holds_back_the_file_range_above_it(LostDecision::RecheckFailed)
            .await;
    }

    /// #639, `Engine::recover`'s stale-VERIFIED path: REPLAY_REQUIRED is
    /// refused.
    #[tokio::test]
    async fn a_stale_verified_range_whose_replay_required_write_fails_holds_back_the_range_above_it(
    ) {
        a_lost_replay_decision_holds_back_the_file_range_above_it(
            LostDecision::RecheckReplayRequired,
        )
        .await;
    }

    /// #639, `Engine::recover`'s path for a row left mid-pipeline: FAILED is
    /// refused.
    #[tokio::test]
    async fn an_incomplete_range_whose_failed_write_fails_holds_back_the_range_above_it() {
        a_lost_replay_decision_holds_back_the_file_range_above_it(LostDecision::DecisionFailed)
            .await;
    }

    /// #639, `Engine::recover`'s path for a row left mid-pipeline:
    /// REPLAY_REQUIRED is refused.
    #[tokio::test]
    async fn an_incomplete_range_whose_replay_required_write_fails_holds_back_the_range_above_it() {
        a_lost_replay_decision_holds_back_the_file_range_above_it(
            LostDecision::DecisionReplayRequired,
        )
        .await;
    }

    /// One Kafka partition whose resume point is the ledger's, as the real
    /// adapter's is since #614: seeded from the newest SOURCE_COMMITTED row,
    /// moved up by each commit, and never rewound below it. The real adapter
    /// commits to a broker; this one keeps the frontier in memory, which is
    /// all recovery's decisions depend on.
    struct LedgerFrontierPartition {
        /// Offsets `0..log_end` exist; record `n` is `r{n}`.
        log_end: Arc<std::sync::atomic::AtomicI64>,
        /// The next offset a read returns.
        next: i64,
        /// One past the highest committed offset.
        committed: i64,
    }

    impl LedgerFrontierPartition {
        const TOPIC: &'static str = "t";
        const GROUP: &'static str = "g";

        /// A fresh process: resumes from the ledger's frontier, or from the
        /// start of the partition when the ledger has none.
        async fn seeded(
            store: &dyn StateStore,
            log_end: Arc<std::sync::atomic::AtomicI64>,
        ) -> Self {
            let committed = store
                .committed_kafka_offsets()
                .await
                .unwrap()
                .iter()
                .filter(|c| {
                    c.topic == Self::TOPIC && c.partition == 0 && c.consumer_group == Self::GROUP
                })
                .map(|c| c.end_offset + 1)
                .max()
                .unwrap_or(0);
            Self {
                log_end,
                next: committed,
                committed,
            }
        }
    }

    #[async_trait::async_trait]
    impl SourceAdapter for LedgerFrontierPartition {
        async fn discover_sources(&self) -> Result<Vec<DiscoveredSource>, VtopError> {
            Ok(vec![DiscoveredSource {
                source_type: SourceType::Kafka,
                source_name: Self::TOPIC.into(),
                format: vtop_core::types::TelemetryFormat::Raw,
            }])
        }
        async fn read_batch_candidates(
            &mut self,
            _source: &DiscoveredSource,
            _max_records: usize,
            _max_bytes: usize,
            _max_wait: Duration,
        ) -> Result<Vec<ReadResult>, VtopError> {
            let end = self.log_end.load(std::sync::atomic::Ordering::SeqCst);
            if self.next >= end {
                return Ok(Vec::new());
            }
            let marker = |start: i64, end: i64| ProgressMarker::Kafka {
                topic: Self::TOPIC.into(),
                partition: 0,
                start_offset: start,
                end_offset: end,
                consumer_group: Self::GROUP.into(),
            };
            let read = ReadResult {
                records: (self.next..end)
                    .map(|n| format!("r{n}").into_bytes())
                    .collect(),
                progress_start: marker(self.next, self.next),
                progress_end: marker(self.next, end - 1),
                first_timestamp: None,
                last_timestamp: None,
                verbatim: false,
            };
            self.next = end;
            Ok(vec![read])
        }
        async fn commit_progress(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
            let ProgressMarker::Kafka { end_offset, .. } = marker else {
                return Err(VtopError::Source("not a kafka marker".into()));
            };
            self.committed = self.committed.max(end_offset + 1);
            Ok(())
        }
        async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
            let ProgressMarker::Kafka { start_offset, .. } = marker else {
                return Err(VtopError::Source("not a kafka marker".into()));
            };
            self.next = (*start_offset).max(self.committed);
            Ok(())
        }
        fn read_position(&self, _marker: &ProgressMarker) -> Option<i128> {
            Some(i128::from(self.next))
        }
        fn source_type(&self) -> SourceType {
            SourceType::Kafka
        }
        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    /// #639 for a Kafka partition, which resumes from the ledger's newest
    /// committed row since #614: a lower range whose replay decision cannot
    /// be recorded holds back the VERIFIED range above it, and is re-read
    /// once the store answers.
    #[tokio::test]
    async fn a_kafka_range_whose_replay_decision_is_lost_holds_back_the_range_above_it() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("state.db").display());
        let backend = Arc::new(vtop_upload::MockBackend::new());
        let log_end = Arc::new(std::sync::atomic::AtomicI64::new(1)); // A: offset 0
        let engine = |work: &str| {
            let cfg = {
                let mut cfg = file_config(
                    dir.path().join(work).to_str().unwrap(),
                    &url,
                    Vec::new(),
                    "mock",
                );
                cfg.batching.max_batch_age_seconds = 3_600;
                // The partition below is the only source.
                cfg.sources.file = None;
                cfg
            };
            let backend = backend.clone();
            let log_end = log_end.clone();
            async move {
                let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
                    .await
                    .unwrap();
                engine.backend = backend;
                let partition =
                    LedgerFrontierPartition::seeded(engine.store.as_ref(), log_end).await;
                engine
                    .adapters
                    .insert(SourceType::Kafka, Box::new(partition));
                engine
            }
        };

        let mut first = engine("work-1").await;
        leave_a_lower_range_and_a_verified_one_above_it(
            &mut first,
            SourceType::Kafka,
            LostDecision::DecisionFailed,
            || log_end.store(2, std::sync::atomic::Ordering::SeqCst), // B: offset 1
        )
        .await;
        assert_eq!(row_at(&first, 0).await.state, BatchState::Batching);
        let b = row_at(&first, 1).await;
        assert_eq!(unit_range(&b), (1, 2));
        assert_eq!(b.state, BatchState::Verified);
        drop(first);

        let mut second = engine("work-2").await;
        FaultyLedger::install(&mut second, &[LedgerWrite::To(BatchState::Failed)]).await;
        let summary = second.recover().await.unwrap();
        assert_eq!(
            row_at(&second, 1).await.state,
            BatchState::Verified,
            "offset 1 committed while offset 0, below it, was neither rewound nor recorded; \
             the ledger's frontier moves to 2 and offset 0's next rewind is clamped there"
        );
        assert_eq!(summary.committed, 0, "{summary:?}");
        drop(second);

        let mut third = engine("work-3").await;
        third.recover().await.unwrap();
        let reread = third.process_once(SourceType::Kafka).await.unwrap();
        assert!(
            reread.iter().any(|o| o.committed),
            "the re-read commits: {reread:?}"
        );
        assert_committed_without_a_gap("kafka", &third, 2).await;
        drop(third);

        let mut fourth = engine("work-4").await;
        fourth.recover().await.unwrap();
        assert_eq!(
            row_at(&fourth, 1).await.state,
            BatchState::SourceCommitted,
            "the hold on offset 1 is released once offset 0 is archived"
        );
    }

    /// A store whose objects exist but cannot be read back to verify.
    struct UnreadableObjects(Arc<vtop_upload::MockBackend>);

    #[async_trait::async_trait]
    impl UploadBackend for UnreadableObjects {
        async fn put_object(
            &self,
            local_path: &std::path::Path,
            object_uri: &str,
            checksum: Option<ObjectChecksum<'_>>,
        ) -> Result<vtop_upload::StoredObject, VtopError> {
            self.0.put_object(local_path, object_uri, checksum).await
        }
        async fn put_manifest(
            &self,
            local_path: &std::path::Path,
            manifest_uri: &str,
            checksum: Option<ObjectChecksum<'_>>,
        ) -> Result<vtop_upload::StoredManifest, VtopError> {
            self.0
                .put_manifest(local_path, manifest_uri, checksum)
                .await
        }
        async fn head_object(&self, uri: &str) -> Result<vtop_upload::ObjectHead, VtopError> {
            self.0.head_object(uri).await
        }
        async fn get_object(&self, uri: &str) -> Result<Vec<u8>, VtopError> {
            self.0.get_object(uri).await
        }
        async fn get_object_bounded(&self, uri: &str, max: usize) -> Result<Vec<u8>, VtopError> {
            self.0.get_object_bounded(uri, max).await
        }
        async fn verify_object(
            &self,
            uri: &str,
            _size: u64,
            _expected: Option<ObjectChecksum<'_>>,
        ) -> Result<vtop_upload::VerificationResult, VtopError> {
            Err(VtopError::Upload(format!(
                "get_object {uri}: connection reset"
            )))
        }
        async fn delete_object(&self, uri: &str) -> Result<(), VtopError> {
            self.0.delete_object(uri).await
        }
        fn backend_name(&self) -> &'static str {
            "unreadable"
        }
        fn supports_checksum_verification(&self) -> bool {
            true
        }
        fn supports_multipart(&self) -> bool {
            false
        }
        fn reports_absent_objects(&self) -> bool {
            true
        }
    }

    /// #626: what may be done with a derived key. Only an absent key is
    /// written to, and only an object that verifies STRONGLY against the
    /// batch's digest is reused; damaged bytes and a size-only pass leave the
    /// key alone.
    #[tokio::test]
    async fn a_derived_key_is_reused_only_for_strongly_verified_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("object");
        std::fs::write(&path, b"the batch's bytes\n").unwrap();
        let digest = vtop_core::checksum::sha256_bytes(b"the batch's bytes\n");
        let size = std::fs::metadata(&path).unwrap().len();
        let ck = || Some(ObjectChecksum::new("sha256", &digest));
        let (attempts, throttles) = (AtomicU64::new(0), AtomicU64::new(0));
        let uri = "s3://bucket/x/vtop-src-b0-18-abc.raw";
        let probe = |backend: Arc<vtop_upload::MockBackend>| {
            let (attempts, throttles) = (&attempts, &throttles);
            let ck = ck();
            async move {
                probe_object_key(
                    backend.as_ref(),
                    uri,
                    size,
                    ck,
                    ["default", "file", "raw"],
                    attempts,
                    throttles,
                )
                .await
                .unwrap()
            }
        };

        let strong = Arc::new(vtop_upload::MockBackend::new());
        assert_eq!(probe(strong.clone()).await, KeyProbe::Free);
        strong.put_object(&path, uri, ck()).await.unwrap();
        assert_eq!(probe(strong.clone()).await, KeyProbe::Holds);
        strong.corrupt(uri);
        assert!(
            matches!(probe(strong.clone()).await, KeyProbe::Differs(_)),
            "damaged bytes are neither reused nor written over"
        );

        let limited = Arc::new(vtop_upload::MockBackend::limited());
        limited.put_object(&path, uri, ck()).await.unwrap();
        assert!(
            matches!(probe(limited).await, KeyProbe::SizeOnly(_)),
            "a size-only pass does not prove the bytes are this batch's"
        );
        assert_eq!(throttles.load(Ordering::Relaxed), 0);

        // An object is there but reading it to verify fails: not reused, not
        // written over, and told apart from a refused check.
        let unreadable = UnreadableObjects(Arc::new(vtop_upload::MockBackend::new()));
        unreadable.0.put_object(&path, uri, ck()).await.unwrap();
        let got = probe_object_key(
            &unreadable,
            uri,
            size,
            ck(),
            ["default", "file", "raw"],
            &attempts,
            &throttles,
        )
        .await
        .unwrap();
        assert!(
            matches!(&got, KeyProbe::Unreadable(why) if why.contains("could not be verified")),
            "{got:?}"
        );

        // A HEAD the store refused says nothing about the key: it is not
        // free, so nothing is written to it.
        let refused = Arc::new(
            vtop_upload::MockBackend::new().with_head_fault(vtop_upload::HeadFault::Refused),
        );
        assert!(
            matches!(probe(refused).await, KeyProbe::Unchecked(why) if why.contains("403")),
            "a refused probe falls back to the attempt key"
        );

        // A throttled HEAD is the store asking for less: the batch fails like
        // any other stage, and the throttle is counted.
        let throttled =
            vtop_upload::MockBackend::new().with_head_fault(vtop_upload::HeadFault::Throttled);
        let err = probe_object_key(
            &throttled,
            uri,
            size,
            ck(),
            ["default", "file", "raw"],
            &attempts,
            &throttles,
        )
        .await
        .expect_err("a throttled probe is not an answer");
        assert!(err.is_upload_throttle(), "{err}");
        assert_eq!(throttles.load(Ordering::Relaxed), 1);
    }

    /// #626: a notice a setting repeats on every batch is logged once, then at
    /// most once per interval, saying how many were suppressed in between.
    #[test]
    fn key_notices_admit_the_first_then_one_per_interval_with_the_suppressed_count() {
        let notices = KeyNotices::every(Duration::from_secs(60));
        let t0 = Instant::now();
        let at = |secs| t0 + Duration::from_secs(secs);
        assert_eq!(
            notices.admit_at("per_attempt", "a", at(0)),
            Some(0),
            "the first is logged"
        );
        assert_eq!(notices.admit_at("per_attempt", "a", at(1)), None);
        assert_eq!(notices.admit_at("per_attempt", "a", at(59)), None);
        assert_eq!(
            notices.admit_at("per_attempt", "b", at(2)),
            Some(0),
            "another source is not hidden by the first"
        );
        assert_eq!(
            notices.admit_at("fallback_unchecked", "a", at(3)),
            Some(0),
            "each kind has its own interval"
        );
        assert_eq!(
            notices.admit_at("per_attempt", "a", at(60)),
            Some(2),
            "the next one reports the two suppressed"
        );
        assert_eq!(notices.admit_at("per_attempt", "a", at(61)), None);
    }

    /// A file source holding about 3.6 KB of records, uploaded uncompressed
    /// in 1000-byte parts: four multipart parts (#668).
    fn multipart_engine_config(dir: &std::path::Path, tenant: &str) -> VtopConfig {
        use std::io::Write;
        let input = dir.join("in.log");
        let mut f = std::fs::File::create(&input).unwrap();
        for i in 0..40 {
            writeln!(
                f,
                "record {i:04} payload abcdefghijklmnopqrstuvwxyz0123456789 ABCDEFGHIJKLMNOPQRS"
            )
            .unwrap();
        }
        let mut cfg = file_config(
            dir.join("work").to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.engine.tenant = tenant.into();
        cfg.batching.max_batch_age_seconds = 3_600;
        cfg.compression.kind = vtop_core::types::CompressionType::None;
        cfg.upload.multipart_part_size_bytes = 1_000;
        cfg.upload.multipart_threshold_bytes = 1_000;
        cfg
    }

    /// #668 with #102: a part the store throttled is retried inside the
    /// upload, so the batch commits, but the throttle still reaches the width
    /// controller and the throttle counter. Swallowed by the retry, it would
    /// hide exactly the signal the controller narrows the width on.
    #[tokio::test]
    async fn a_throttled_part_is_retried_and_still_narrows_the_upload_width() {
        let _serial = THROTTLE_TESTS.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let tenant = "multipart-throttle-668";
        let mut cfg = multipart_engine_config(dir.path(), tenant);
        cfg.batching.max_concurrent_batches = 4;
        cfg.batching.adaptive_width.enabled = true;
        let mut engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let mock = Arc::new(vtop_upload::MockBackend::new().with_part_fault(
            2,
            vtop_upload::PartFault::Throttled,
            1,
        ));
        engine.backend = mock.clone();
        let counter = telemetry::init()
            .unwrap()
            .upload_throttled_total
            .with_label_values(&[tenant, "file", "raw", "object_upload"]);
        let before = counter.get();

        let outcomes = engine.run_source(SourceType::File, true).await.unwrap();
        assert_eq!(outcomes.len(), 1, "{outcomes:?}");
        assert!(
            outcomes[0].committed,
            "the throttled part was retried and the object completed: {outcomes:?}"
        );
        assert_eq!(mock.part_sends(2), 2);
        assert_eq!(
            counter.get() - before,
            1,
            "the throttle the retry absorbed is counted"
        );
        assert_eq!(
            engine.upload_width(),
            2,
            "and the width controller halved the next cycle's width on it"
        );
    }

    #[tokio::test]
    async fn the_multipart_sweep_runs_every_quarter_window_and_not_more_than_every_15_minutes() {
        let dir = tempfile::tempdir().unwrap();
        for (abandon, every) in [(86_400, 21_600), (600, 900)] {
            let mut cfg = multipart_engine_config(dir.path(), "default");
            cfg.upload.multipart_abandon_after_secs = abandon;
            let engine = Engine::new(cfg, StreamsConfig { streams: vec![] })
                .await
                .unwrap();
            assert_eq!(
                engine.multipart_sweep_interval(),
                Duration::from_secs(every),
                "abandon window {abandon} s"
            );
            assert!(
                engine.multipart_sweep_due(),
                "an engine that has not swept yet is due"
            );
        }
    }

    /// #668: failed uploads keep their sessions for the replay now, so the
    /// abandoned-session sweep runs while the engine runs, not only at
    /// startup. Inside `run_until`, a session older than the abandon window
    /// has its upload aborted at the store and its file removed; a younger
    /// one, which a replay may still resume, is left alone.
    #[tokio::test]
    async fn the_running_engine_sweeps_abandoned_multipart_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        let input = dir.path().join("idle.log");
        std::fs::write(&input, "").unwrap();
        let mut cfg = file_config(
            work.to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.idle_poll_interval_ms = 20;
        let mut engine = Engine::new(cfg.clone(), StreamsConfig { streams: vec![] })
            .await
            .unwrap();
        let backend = Arc::new(vtop_upload::MockBackend::new().with_part_fault(
            2,
            vtop_upload::PartFault::Reset,
            usize::MAX,
        ));
        engine.backend = backend.clone();

        // Two failed uploads, each leaving a session and an upload at the store.
        let mut mp_cfg = MultipartUploadConfig::from_upload(&cfg.upload, work.clone());
        mp_cfg.part_size_bytes = 1_000;
        mp_cfg.part_attempts = 1;
        let mut left = Vec::new();
        for name in ["aged", "young"] {
            let data = vec![name.len() as u8; 3_000];
            let file = dir.path().join(name);
            std::fs::write(&file, &data).unwrap();
            let digest = vtop_core::checksum::sha256_bytes(&data);
            let fence = MultipartFence {
                expected_segment_generation: 0,
                fencing_epoch: 0,
                content_digest_hex: digest.clone(),
                content_digest_algorithm: "sha256".into(),
                byte_length: 3_000,
            };
            let uri = format!("s3://telemetry-data/{name}.raw");
            multipart::upload_resumable(
                backend.as_ref(),
                &mp_cfg,
                &file,
                &uri,
                Some(ObjectChecksum::new("sha256", &digest)),
                fence.clone(),
            )
            .await
            .expect_err("part 2 fails");
            let path = work.join(multipart::session_file_name(&uri, &fence));
            let session = multipart::load_session(&path).unwrap();
            left.push((path, session));
        }
        let (aged_path, mut aged) = left.remove(0);
        let (young_path, young) = left.remove(0);
        aged.updated_at_unix_secs = 1;
        multipart::save_session(&aged_path, &aged).unwrap();

        let swept = telemetry::init()
            .unwrap()
            .multipart_sessions_total
            .with_label_values(&["swept"]);
        let swept_before = swept.get();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let watch = async {
            for _ in 0..500 {
                if !backend.has_pending_multipart(&aged.object_uri, &aged.upload_id) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            tx.send("SIGTERM").unwrap();
        };
        let (ran, ()) = tokio::join!(engine.run_until(ChannelShutdown(rx)), watch);
        ran.unwrap();

        assert!(
            !backend.has_pending_multipart(&aged.object_uri, &aged.upload_id),
            "the aged session's upload was aborted at the store while the engine ran"
        );
        assert!(!aged_path.exists(), "and its session file removed");
        assert!(
            backend.has_pending_multipart(&young.object_uri, &young.upload_id),
            "a session younger than the window is left for its replay"
        );
        assert!(young_path.exists());
        assert!(engine.last_multipart_sweep.is_some());
        assert!(
            !engine.multipart_sweep_due(),
            "the next sweep waits for the interval"
        );
        assert_eq!(swept.get() - swept_before, 1);
    }

    /// A stop that arrives while the abandoned-session sweep is waiting on a
    /// store that does not answer ends the run at once (#668, review): each
    /// abort would otherwise wait out its request deadline, and a pod's
    /// termination grace period could expire mid-sweep. Dropping the sweep is
    /// safe: a session is deleted only after its abort succeeded.
    #[tokio::test]
    async fn a_stop_during_a_sweep_against_a_silent_store_is_not_held_up() {
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        let input = dir.path().join("idle.log");
        std::fs::write(&input, "").unwrap();
        let mut cfg = file_config(
            work.to_str().unwrap(),
            "sqlite::memory:",
            vec![input.to_string_lossy().into_owned()],
            "mock",
        );
        cfg.batching.idle_poll_interval_ms = 20;
        let mut engine = Engine::new(cfg.clone(), StreamsConfig { streams: vec![] })
            .await
            .unwrap();

        // An aged session, left by an upload to a store that then went silent.
        let failing = vtop_upload::MockBackend::new().with_part_fault(
            1,
            vtop_upload::PartFault::Reset,
            usize::MAX,
        );
        let mut mp_cfg = MultipartUploadConfig::from_upload(&cfg.upload, work.clone());
        mp_cfg.part_size_bytes = 1_000;
        mp_cfg.part_attempts = 1;
        let data = vec![7_u8; 3_000];
        let file = dir.path().join("object");
        std::fs::write(&file, &data).unwrap();
        let digest = vtop_core::checksum::sha256_bytes(&data);
        let fence = MultipartFence {
            expected_segment_generation: 0,
            fencing_epoch: 0,
            content_digest_hex: digest.clone(),
            content_digest_algorithm: "sha256".into(),
            byte_length: 3_000,
        };
        let uri = "s3://telemetry-data/silent.raw";
        multipart::upload_resumable(
            &failing,
            &mp_cfg,
            &file,
            uri,
            Some(ObjectChecksum::new("sha256", &digest)),
            fence.clone(),
        )
        .await
        .expect_err("part 1 fails");
        let path = work.join(multipart::session_file_name(uri, &fence));
        let mut session = multipart::load_session(&path).unwrap();
        session.updated_at_unix_secs = 1;
        multipart::save_session(&path, &session).unwrap();
        engine.backend = Arc::new(vtop_upload::MockBackend::new().with_hanging_abort());

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let stop = async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            tx.send("SIGTERM").unwrap();
        };
        let run = tokio::time::timeout(
            Duration::from_secs(10),
            engine.run_until(ChannelShutdown(rx)),
        );
        let (ran, ()) = tokio::join!(run, stop);
        ran.expect("the stop must not wait for the silent store's abort")
            .unwrap();
        assert!(
            path.exists(),
            "the session whose abort never answered is kept for the next sweep"
        );
    }
}

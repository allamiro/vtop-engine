//! Resumable multipart upload orchestration (#191).
//!
//! Persists upload id, completed parts, and per-part digests so a broker or
//! `vtopctl tier` restart can continue without retransmitting finished parts.
//! Whole-object `verify_object` remains the strong gate before any evidence
//! commit — multipart ETags are never integrity evidence.
//!
//! Stale-generation fencing: a session is keyed by destination URI plus the
//! operator fence tokens (generation, fencing epoch, content digest). A resume
//! that disagrees with those tokens refuses.
//!
//! A failed part keeps what already completed (#668). Parts go out through a
//! bounded pool and each success is written to the session as it finishes; a
//! failed part is retried alone, up to `upload.multipart_part_attempts`, with
//! exponential backoff and full jitter; and a session is identified by its
//! content, never by the local file's name, so the engine's replay of the same
//! bytes (from a file named after another batch attempt) resumes it.

use crate::base::{ObjectChecksum, StoredObject, UploadBackend, UploadedPart};
use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
use vtop_core::config::MAX_SEMAPHORE_PERMITS;
use vtop_core::errors::VtopError;

/// Tunables for one resumable upload run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartUploadConfig {
    pub part_size_bytes: u64,
    pub threshold_bytes: u64,
    pub max_parallelism: usize,
    pub abandon_after_secs: u64,
    /// Directory holding `*.multipart.json` session files.
    pub state_dir: PathBuf,
    /// Requests per part before the part, and the call, fail (#668): the
    /// first plus the retries. `upload.multipart_part_attempts`.
    pub part_attempts: u32,
    /// The wait between two attempts at one part (#668).
    pub part_backoff: PartBackoff,
}

/// The wait before a part's next attempt (#668): exponential from `base`,
/// capped at `cap`, with full jitter (anywhere from zero to that ceiling), so
/// parts that failed together do not all come back at once. Fixed in code,
/// not configuration: the attempt count is the operator's knob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartBackoff {
    pub base: Duration,
    pub cap: Duration,
}

impl Default for PartBackoff {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            cap: Duration::from_secs(30),
        }
    }
}

impl PartBackoff {
    /// The longest wait after `failed` attempts (1 for the first failure).
    pub fn ceiling(&self, failed: u32) -> Duration {
        let doublings = failed.saturating_sub(1).min(31);
        self.base.saturating_mul(1_u32 << doublings).min(self.cap)
    }

    /// The wait after `failed` attempts, drawn from `[0, ceiling]` by `seed`.
    pub fn delay(&self, failed: u32, seed: u64) -> Duration {
        // splitmix64: a well-spread fraction from a seed that is only a
        // clock reading and two small numbers.
        let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        let fraction = (z >> 11) as f64 / (1_u64 << 53) as f64;
        self.ceiling(failed).mul_f64(fraction)
    }
}

/// What one resumable upload did (#668), filled in as it goes so a caller
/// learns it on failure as well as on success. The engine turns it into
/// metrics and into its width controller's throttle count: retries inside
/// this module would otherwise hide every throttle the store answered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MultipartReport {
    /// Parts the object has.
    pub parts_total: u32,
    /// Parts an earlier call left in the session that matched the local
    /// bytes and were not sent again.
    pub parts_reused: u32,
    /// Their bytes: what resuming saved.
    pub bytes_reused: u64,
    /// Parts this call sent successfully. After a restart this includes the
    /// parts sent to the upload the store could not continue: they crossed
    /// the link too.
    pub parts_uploaded: u32,
    /// Extra attempts at a part, beyond its first.
    pub part_retries: u32,
    /// Parts whose last attempt in this call failed.
    pub parts_failed: u32,
    /// Every request this call made to the store.
    pub requests: u32,
    /// Of those, the ones the store answered with a throttle.
    pub throttled_requests: u32,
    /// An in-progress session an earlier call left was adopted (it stays
    /// true if that upload is then restarted).
    pub resumed: bool,
    /// The store no longer held the upload and a new one was started.
    pub restarted: bool,
    /// The object was completed.
    pub completed: bool,
    /// When the call failed: parts the session still holds.
    pub parts_kept: u32,
    /// When the call failed: an in-progress session is on disk that a later
    /// call with the same bytes resumes. False when there is nothing to
    /// resume, or when the session on disk cannot be resumed (it disagrees
    /// with this upload's fence), which a caller should abort.
    pub session_left: bool,
}

impl MultipartReport {
    fn count<T>(&mut self, result: &Result<T, VtopError>) {
        self.requests += 1;
        if result.as_ref().is_err_and(VtopError::is_upload_throttle) {
            self.throttled_requests += 1;
        }
    }
}

impl MultipartUploadConfig {
    pub fn from_upload(upload: &vtop_core::config::UploadConfig, state_dir: PathBuf) -> Self {
        // The active transport's egress tuning maps onto the multipart knobs
        // (#480): `part_size_bytes` / `parts_in_flight` are the symmetric names
        // both paths share, and for tcp_tls they ARE the multipart part size and
        // parallelism. When set, the tuning wins; otherwise the older
        // `multipart_*` keys still work unchanged, so no existing deployment
        // changes behaviour. The two spellings are equivalent, and the older one
        // is deprecated in documentation only.
        //
        // `active_egress_tuning` is the ONE gate: it selects by the EFFECTIVE
        // transport (VTOP_S3_TRANSPORT over the config value), so an overridden
        // run reads the right block rather than falling to defaults, and it
        // returns all-`None` unless the backend is s3_native. That second half
        // matters here as much as on the width path (review): a mock backend
        // also advertises `supports_multipart()`, and validation skips the
        // `transports` map entirely for a non-s3_native backend — so without the
        // gate, a benchmark matrix that switches to mock while keeping one
        // tcp_tls block would chunk at a part size nobody validated and the
        // summary records as blank, or fail outright on `parts_in_flight: 0`.
        let tuning = upload.active_egress_tuning();
        Self {
            part_size_bytes: tuning
                .part_size_bytes
                .unwrap_or(upload.multipart_part_size_bytes),
            threshold_bytes: upload.multipart_threshold_bytes,
            max_parallelism: tuning
                .parts_in_flight
                .unwrap_or(upload.multipart_max_parallelism),
            abandon_after_secs: upload.multipart_abandon_after_secs,
            state_dir,
            part_attempts: upload.multipart_part_attempts,
            part_backoff: PartBackoff::default(),
        }
    }

    pub fn should_multipart(&self, backend: &dyn UploadBackend, size: u64) -> bool {
        // The configured part size must clear the backend's floor (#482,
        // review): S3 rejects a non-final part below 5 MiB, so a too-small
        // part size routed to multipart fails every non-final part. This is
        // the ONE decision both callers share, so the floor lives here —
        // neither the engine nor `vtopctl tier` can select a doomed
        // multipart independently.
        backend.supports_multipart()
            && size >= self.threshold_bytes
            && size > 0
            && self.part_size_bytes >= backend.min_part_size_bytes()
    }
}

/// Fence tokens that must match for a session to be resumed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultipartFence {
    pub expected_segment_generation: u64,
    pub fencing_epoch: u64,
    /// Whole-file content digest (hex) the upload is transferring.
    pub content_digest_hex: String,
    pub content_digest_algorithm: String,
    pub byte_length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedPart {
    pub part_number: u32,
    pub etag: String,
    pub size_bytes: u64,
    /// Client-computed BLAKE3 (or configured algo) over the part bytes.
    pub part_digest_hex: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MultipartSessionPhase {
    InProgress,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultipartSession {
    pub schema_version: u32,
    pub backend_id: String,
    pub object_uri: String,
    pub local_path: PathBuf,
    pub upload_id: String,
    pub part_size_bytes: u64,
    pub fence: MultipartFence,
    pub phase: MultipartSessionPhase,
    /// Completed parts keyed by part number.
    pub parts: BTreeMap<u32, PersistedPart>,
    pub created_at_unix_secs: u64,
    pub updated_at_unix_secs: u64,
    /// Set once complete succeeds so a retry can short-circuit.
    pub completed_version_id: Option<String>,
    /// The Object Lock retention CreateMultipartUpload asked for (#684). S3
    /// fixes it there, so after a resume this, not the time of the resume,
    /// is the date the completed object carries. Optional and defaulted, so
    /// `SESSION_SCHEMA` stays 1: a session written before the field reads as
    /// one created without a lock, and an older binary ignores the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<crate::base::RequestedRetention>,
}

const SESSION_SCHEMA: u32 = 1;

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Stable session filename for one logical upload identity.
pub fn session_file_name(object_uri: &str, fence: &MultipartFence) -> String {
    let mut hasher = blake3::Hasher::new_derive_key("vtop multipart session id v1");
    hasher.update(object_uri.as_bytes());
    hasher.update(&fence.expected_segment_generation.to_be_bytes());
    hasher.update(&fence.fencing_epoch.to_be_bytes());
    hasher.update(fence.content_digest_algorithm.as_bytes());
    hasher.update(fence.content_digest_hex.as_bytes());
    hasher.update(&fence.byte_length.to_be_bytes());
    let hex = hasher.finalize().to_hex();
    format!("{}.multipart.json", &hex.as_str()[..32])
}

fn session_path(state_dir: &Path, object_uri: &str, fence: &MultipartFence) -> PathBuf {
    state_dir.join(session_file_name(object_uri, fence))
}

pub fn load_session(path: &Path) -> Result<MultipartSession, VtopError> {
    let bytes = std::fs::read(path).map_err(|e| {
        VtopError::Upload(format!("reading multipart session {}: {e}", path.display()))
    })?;
    serde_json::from_slice(&bytes).map_err(|e| {
        VtopError::Upload(format!(
            "decoding multipart session {}: {e}",
            path.display()
        ))
    })
}

pub fn save_session(path: &Path, session: &MultipartSession) -> Result<(), VtopError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            VtopError::Upload(format!(
                "creating multipart state dir {}: {e}",
                parent.display()
            ))
        })?;
    }
    let bytes = serde_json::to_vec_pretty(session)
        .map_err(|e| VtopError::Upload(format!("encoding multipart session: {e}")))?;
    let tmp = path.with_extension("multipart.json.tmp");
    // The session is the only record of the upload id, and since #668 it is
    // relied on across crashes, so it is made durable the way the repo's
    // other published files are: the data synced before the rename, so the
    // name never points at unwritten bytes, and the directory after it, so
    // the rename itself survives.
    let write = || -> std::io::Result<()> {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()
    };
    write().map_err(|e| {
        VtopError::Upload(format!("writing multipart session {}: {e}", tmp.display()))
    })?;
    std::fs::rename(&tmp, path).map_err(|e| {
        VtopError::Upload(format!(
            "publishing multipart session {}: {e}",
            path.display()
        ))
    })?;
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| {
                VtopError::Upload(format!(
                    "syncing multipart state dir {}: {e}",
                    parent.display()
                ))
            })?;
    }
    Ok(())
}

pub fn delete_session(path: &Path) -> Result<(), VtopError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(VtopError::Upload(format!(
            "deleting multipart session {}: {e}",
            path.display()
        ))),
    }
}

fn part_count(byte_length: u64, part_size: u64) -> u32 {
    if byte_length == 0 {
        return 0;
    }
    let n = byte_length.div_ceil(part_size);
    u32::try_from(n).unwrap_or(u32::MAX)
}

async fn read_part_bytes(path: &Path, offset: u64, len: u64) -> Result<(Bytes, String), VtopError> {
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|e| VtopError::Upload(format!("opening {} for multipart: {e}", path.display())))?;
    file.seek(SeekFrom::Start(offset))
        .await
        .map_err(|e| VtopError::Upload(format!("seeking {} for multipart: {e}", path.display())))?;
    let mut buf = vec![0_u8; len as usize];
    file.read_exact(&mut buf).await.map_err(|e| {
        VtopError::Upload(format!(
            "reading part at offset {offset} from {}: {e}",
            path.display()
        ))
    })?;
    let digest = vtop_core::checksum::blake3_bytes(&buf);
    Ok((Bytes::from(buf), digest))
}

/// Upload (or resume) a local file via multipart. On success the session file
/// is removed after a completed phase is recorded (idempotent retries that
/// find a completed session re-verify and return the pinned version).
pub async fn upload_resumable(
    backend: &dyn UploadBackend,
    cfg: &MultipartUploadConfig,
    local_path: &Path,
    object_uri: &str,
    checksum: Option<ObjectChecksum<'_>>,
    fence: MultipartFence,
) -> Result<StoredObject, VtopError> {
    let mut report = MultipartReport::default();
    upload_resumable_with_report(
        backend,
        cfg,
        local_path,
        object_uri,
        checksum,
        fence,
        &mut report,
    )
    .await
}

/// [`upload_resumable`], recording what it did in `report` as it goes, so the
/// caller learns it whether the upload succeeds or fails (#668).
pub async fn upload_resumable_with_report(
    backend: &dyn UploadBackend,
    cfg: &MultipartUploadConfig,
    local_path: &Path,
    object_uri: &str,
    checksum: Option<ObjectChecksum<'_>>,
    fence: MultipartFence,
    report: &mut MultipartReport,
) -> Result<StoredObject, VtopError> {
    if !backend.supports_multipart() {
        return Err(VtopError::Upload(format!(
            "backend {} does not support resumable multipart",
            backend.backend_name()
        )));
    }
    if cfg.part_size_bytes == 0 || cfg.max_parallelism == 0 || cfg.part_attempts == 0 {
        return Err(VtopError::Config(
            "multipart part size, parallelism and part attempts must be > 0".into(),
        ));
    }
    // The other end of the same knob, guarded in the same place and for the
    // sharper failure (review). Zero degrades — the pool below would carry no
    // parts — and is refused above; a count above tokio's ceiling does not
    // degrade at all: it was a semaphore's permit count, and `Semaphore::new`
    // PANICS, so an operator's typo aborted the process mid-copy instead of
    // failing one upload. The pool no longer builds a semaphore, but the
    // bound stays: a width past it is a typo, not a plan.
    //
    // `UploadConfig::validate_multipart_limits` bounds the same value at config
    // load, which is where an operator would rather hear about it, but that is
    // not the last word: `vtopctl tier copy` applies
    // --multipart-max-parallelism to the DERIVED MultipartUploadConfig after
    // build_backend has validated the UploadConfig, so the number that becomes
    // the pool's width was never the number that check saw. This function is
    // the one door every caller — the engine, a tier copy, a flag nobody has
    // written yet — passes through, so the bound it cannot be routed around
    // lives here.
    if cfg.max_parallelism > MAX_SEMAPHORE_PERMITS {
        return Err(VtopError::Config(format!(
            "multipart parallelism {} exceeds the maximum permits a semaphore can hold \
             ({MAX_SEMAPHORE_PERMITS}); the value bounds the parts in flight and an \
             oversized one is refused rather than attempted",
            cfg.max_parallelism
        )));
    }
    if fence.byte_length == 0 {
        return Err(VtopError::Upload(
            "refusing multipart upload of an empty object".into(),
        ));
    }
    let meta_len = tokio::fs::metadata(local_path)
        .await
        .map_err(|e| VtopError::Upload(format!("stat {}: {e}", local_path.display())))?
        .len();
    if meta_len != fence.byte_length {
        return Err(VtopError::Upload(format!(
            "multipart fence byte_length {} disagrees with local file {}",
            fence.byte_length, meta_len
        )));
    }
    if let Some(c) = checksum {
        if c.hex != fence.content_digest_hex
            || !c
                .algorithm
                .eq_ignore_ascii_case(&fence.content_digest_algorithm)
        {
            return Err(VtopError::Upload(
                "multipart fence content digest disagrees with upload checksum".into(),
            ));
        }
    }

    let path = session_path(&cfg.state_dir, object_uri, &fence);
    let call = SessionCall {
        backend,
        cfg,
        local_path,
        object_uri,
        checksum,
        fence: &fence,
        path: &path,
    };
    match call.run(report).await {
        // The store cannot continue the upload (#668): S3's NoSuchUpload,
        // after a lifecycle rule or a sweep aborted it or an earlier attempt
        // completed it, or InvalidPart on complete, where the upload is still
        // there. The same request would fail the same way on every replay,
        // so the upload is aborted (best effort; a gone one needs nothing),
        // the session dropped, and the object started again under a new
        // upload, once. A second failure in the same call is an error with
        // nothing kept, not a loop.
        Err(VtopError::UploadSessionGone(cause)) => {
            let cause = redact_urls(&cause);
            tracing::warn!(
                event = "multipart_session_restarted",
                key = object_uri,
                error = %cause,
                "the store cannot continue the multipart upload for {object_uri} ({cause}); \
                 abandoning it and starting a new upload that every part is sent to; no data \
                 is affected, nothing to do"
            );
            abort_and_forget(backend, &path, report).await?;
            report.restarted = true;
            report.session_left = false;
            match call.run(report).await {
                Err(VtopError::UploadSessionGone(again)) => {
                    abort_and_forget(backend, &path, report).await?;
                    report.session_left = false;
                    report.parts_kept = 0;
                    Err(VtopError::Upload(format!(
                        "the store refused the multipart upload for {object_uri} again after a \
                         restart ({}); both uploads were abandoned and nothing is kept, and the \
                         next attempt starts a new upload",
                        redact_urls(&again)
                    )))
                }
                other => other,
            }
        }
        other => other,
    }
}

/// Abort the upload a session names, best effort, and delete the session
/// (#668). Used when the store cannot continue it: a gone upload answers the
/// abort with NoSuchUpload, which is what was wanted; any other failure
/// leaves parts a bucket lifecycle rule reclaims.
async fn abort_and_forget(
    backend: &dyn UploadBackend,
    path: &Path,
    report: &mut MultipartReport,
) -> Result<(), VtopError> {
    if let Ok(session) = load_session(path) {
        if session.phase == MultipartSessionPhase::InProgress {
            let aborted = backend
                .abort_multipart_upload(&session.object_uri, &session.upload_id)
                .await;
            report.count(&aborted);
            if let Err(e) = aborted {
                if !matches!(e, VtopError::UploadSessionGone(_)) {
                    tracing::warn!(
                        event = "multipart_abort_failed",
                        key = %session.object_uri,
                        error = %redact_urls(&e.to_string()),
                        "could not abort the multipart upload for {} that the store cannot \
                         continue ({}); its parts stay at the store until a bucket lifecycle \
                         rule (AbortIncompleteMultipartUpload) removes them; add one if the \
                         bucket has none",
                        session.object_uri,
                        redact_urls(&e.to_string())
                    );
                }
            }
        }
    }
    delete_session(path)
}

/// One pass at an upload: load or create its session, send what is missing,
/// complete. [`upload_resumable_with_report`] runs it again once when the
/// store has lost the upload.
struct SessionCall<'a> {
    backend: &'a dyn UploadBackend,
    cfg: &'a MultipartUploadConfig,
    local_path: &'a Path,
    object_uri: &'a str,
    checksum: Option<ObjectChecksum<'a>>,
    fence: &'a MultipartFence,
    path: &'a Path,
}

impl SessionCall<'_> {
    async fn run(&self, report: &mut MultipartReport) -> Result<StoredObject, VtopError> {
        let (backend, object_uri, fence, path) =
            (self.backend, self.object_uri, self.fence, self.path);
        let mut adopted = false;
        let mut session = match load_session(path) {
            Ok(existing) => {
                if let Err(refused) = validate_resume(&existing, backend, object_uri, fence) {
                    // The session on disk cannot be this upload's: the caller
                    // aborts it rather than keep it.
                    report.session_left = false;
                    return Err(refused);
                }
                if existing.phase == MultipartSessionPhase::Completed {
                    let stored =
                        finish_idempotent(backend, object_uri, &existing, self.checksum, report)
                            .await?;
                    delete_session(path)?;
                    report.completed = true;
                    return Ok(stored);
                }
                if let Some(why) = lock_mismatch(&existing, backend) {
                    // S3 fixed the session's lock when it was created
                    // (#684): an upload created without the lock this
                    // backend asks for, or with another mode or hold, would
                    // complete an object that fails its retention check on
                    // every replay. It is aborted, best effort, and the
                    // object starts again on an upload that carries the lock.
                    tracing::info!(
                        event = "multipart_session_lock_mismatch",
                        key = object_uri,
                        "the multipart session for {object_uri} was created {why}; aborting it \
                         and starting a new upload that carries the lock; nothing to do"
                    );
                    let aborted = backend
                        .abort_multipart_upload(&existing.object_uri, &existing.upload_id)
                        .await;
                    report.count(&aborted);
                    if let Err(e) = aborted {
                        if !matches!(e, VtopError::UploadSessionGone(_)) {
                            let cause = redact_urls(&e.to_string());
                            tracing::warn!(
                                event = "multipart_abort_failed",
                                key = %existing.object_uri,
                                error = %cause,
                                "could not abort the multipart upload for {} that cannot carry \
                                 the lock ({cause}); its parts stay at the store until a bucket \
                                 lifecycle rule (AbortIncompleteMultipartUpload) removes them; \
                                 add one if the bucket has none",
                                existing.object_uri
                            );
                        }
                    }
                    delete_session(path)?;
                    self.create(report).await?
                } else if existing.fence.content_digest_hex.is_empty() {
                    // Nothing identifies the bytes this session was for
                    // (checksums are off), so it is never resumed (#668): its
                    // upload is aborted, best effort, and the object starts
                    // again. A failed abort leaves parts the abandon window
                    // and a bucket lifecycle rule reclaim.
                    let aborted = backend
                        .abort_multipart_upload(&existing.object_uri, &existing.upload_id)
                        .await;
                    report.count(&aborted);
                    delete_session(path)?;
                    self.create(report).await?
                } else {
                    report.resumed = true;
                    adopted = true;
                    existing
                }
            }
            Err(e) => {
                if path.exists() {
                    // A session file that cannot be read names an upload
                    // nothing else records; it is replaced, and the upload's
                    // parts are left to a bucket lifecycle rule.
                    tracing::warn!(
                        event = "multipart_session_unreadable",
                        key = object_uri,
                        error = %e,
                        "the multipart session file for {object_uri} could not be read ({e}); \
                         starting a new upload in its place; the parts of the upload it named \
                         are reclaimed only by a bucket lifecycle rule \
                         (AbortIncompleteMultipartUpload); nothing else to do"
                    );
                }
                self.create(report).await?
            }
        };

        // The session is identified by its content, not by where the bytes
        // are read from (#668): the engine names its compressed file after
        // the attempt's batch id, so a replay reads the same bytes from
        // another path. Every kept part is re-read from the current file and
        // its digest compared before it is skipped, and the whole object is
        // read back before anything is committed, so the path adds nothing.
        session.local_path = self.local_path.to_path_buf();
        // Re-verify persisted parts against local bytes; drop mismatches so
        // they are retransmitted.
        revalidate_persisted_parts(&mut session, self.local_path).await?;
        save_session(path, &session)?;
        let total = part_count(session.fence.byte_length, session.part_size_bytes);
        report.parts_total = total;
        report.parts_reused = session.parts.len() as u32;
        report.bytes_reused = session.parts.values().map(|p| p.size_bytes).sum();
        report.session_left = true;
        if adopted {
            tracing::info!(
                event = "multipart_session_resumed",
                key = object_uri,
                parts_reused = report.parts_reused,
                parts_total = total,
                bytes_reused = report.bytes_reused,
                "resuming multipart session for {object_uri}: {} of {total} parts already \
                 uploaded ({} not re-sent); nothing to do",
                report.parts_reused,
                human_bytes(report.bytes_reused)
            );
        }

        if let Err(failed) = upload_missing_parts(
            backend,
            self.cfg,
            &mut session,
            self.local_path,
            path,
            report,
        )
        .await
        {
            report.parts_kept = session.parts.len() as u32;
            if matches!(failed, VtopError::UploadSessionGone(_)) {
                report.session_left = false;
            }
            return Err(failed);
        }

        let parts: Vec<UploadedPart> = session
            .parts
            .values()
            .map(|p| UploadedPart {
                part_number: p.part_number,
                etag: p.etag.clone(),
            })
            .collect();
        let completed = backend
            .complete_multipart_upload(object_uri, &session.upload_id, &parts)
            .await;
        report.count(&completed);
        let stored = match completed {
            Ok(stored) => stored,
            Err(err) => {
                // Crash between complete and session update: object may already
                // exist. Strong verify + optional version recovery makes complete
                // idempotent. (S3 answers a second complete of the same upload
                // with NoSuchUpload, which is why this is tried before the
                // restart a gone session otherwise gets.)
                match recover_completed(
                    backend,
                    object_uri,
                    self.checksum,
                    fence.byte_length,
                    report,
                )
                .await
                {
                    Ok(stored) => stored,
                    Err(_) => {
                        report.parts_kept = session.parts.len() as u32;
                        if matches!(err, VtopError::UploadSessionGone(_)) {
                            report.session_left = false;
                        }
                        return Err(err);
                    }
                }
            }
        };

        // The lock was fixed at create (#684); the session is what knows it.
        let stored = StoredObject {
            retention: session.retention,
            ..stored
        };
        session.phase = MultipartSessionPhase::Completed;
        session.completed_version_id = stored.version_id.clone();
        session.updated_at_unix_secs = now_unix_secs();
        save_session(path, &session)?;
        // Keep the completed marker briefly so concurrent/retry callers see it,
        // then delete — a missing file simply starts a new upload next time.
        delete_session(path)?;
        report.completed = true;
        report.session_left = false;
        report.parts_kept = 0;
        Ok(stored)
    }

    async fn create(&self, report: &mut MultipartReport) -> Result<MultipartSession, VtopError> {
        let created = self
            .backend
            .create_multipart_upload(self.object_uri, "application/octet-stream", self.checksum)
            .await;
        report.count(&created);
        let created = created?;
        let now = now_unix_secs();
        let session = MultipartSession {
            schema_version: SESSION_SCHEMA,
            backend_id: self.backend.backend_name().to_owned(),
            object_uri: self.object_uri.to_owned(),
            local_path: self.local_path.to_path_buf(),
            upload_id: created.upload_id,
            part_size_bytes: self.cfg.part_size_bytes,
            fence: self.fence.clone(),
            phase: MultipartSessionPhase::InProgress,
            parts: BTreeMap::new(),
            created_at_unix_secs: now,
            updated_at_unix_secs: now,
            completed_version_id: None,
            retention: created.retention,
        };
        save_session(self.path, &session)?;
        Ok(session)
    }
}

/// Why a session cannot complete the object this backend would write
/// (#684), or `None` when it can: its upload was created without the lock
/// the backend asks for now, or with another mode or legal hold. The date
/// may differ (it was fixed at creation, and the engine checks against it);
/// a lock this backend does not ask for is left as it is.
fn lock_mismatch(session: &MultipartSession, backend: &dyn UploadBackend) -> Option<String> {
    let policy = backend.object_lock()?;
    match session.retention {
        None => Some("without an Object Lock".to_owned()),
        Some(r) if r.mode != policy.mode || r.legal_hold != policy.legal_hold => Some(format!(
            "under mode {} with legal_hold={}, not mode {} with legal_hold={}",
            r.mode, r.legal_hold, policy.mode, policy.legal_hold
        )),
        // A session lives as long as replays keep resuming it: one whose
        // lock has already ended would complete an object nobody has to keep
        // (review of #708).
        Some(r) if r.retain_until_unix_secs <= now_unix_secs() as i64 => Some(format!(
            "under a retention that ended at {}",
            crate::base::format_unix_secs(r.retain_until_unix_secs)
        )),
        Some(_) => None,
    }
}

fn validate_resume(
    session: &MultipartSession,
    backend: &dyn UploadBackend,
    object_uri: &str,
    fence: &MultipartFence,
) -> Result<(), VtopError> {
    if session.schema_version != SESSION_SCHEMA {
        return Err(VtopError::Upload(format!(
            "unsupported multipart session schema {}",
            session.schema_version
        )));
    }
    if session.backend_id != backend.backend_name() {
        return Err(VtopError::Upload(format!(
            "multipart session backend {} does not match live backend {}",
            session.backend_id,
            backend.backend_name()
        )));
    }
    if session.object_uri != object_uri {
        return Err(VtopError::Upload(
            "multipart session object_uri mismatch (stale fence)".into(),
        ));
    }
    if &session.fence != fence {
        return Err(VtopError::Upload(
            "multipart session fence mismatch: refusing to resume a stale generation \
             (abort the abandoned upload and start a new session)"
                .into(),
        ));
    }
    if session.part_size_bytes == 0 {
        return Err(VtopError::Upload(
            "multipart session records a part size of zero; refusing to resume it".into(),
        ));
    }
    // The local path is deliberately not compared (#668): see
    // `SessionCall::run`.
    Ok(())
}

async fn finish_idempotent(
    backend: &dyn UploadBackend,
    object_uri: &str,
    session: &MultipartSession,
    checksum: Option<ObjectChecksum<'_>>,
    report: &mut MultipartReport,
) -> Result<StoredObject, VtopError> {
    let stored = recover_completed(
        backend,
        object_uri,
        checksum,
        session.fence.byte_length,
        report,
    )
    .await?;
    if session.completed_version_id.is_some()
        && stored.version_id != session.completed_version_id
        && session
            .completed_version_id
            .as_ref()
            .is_some_and(|id| !id.is_empty())
    {
        // Prefer the pinned version from the completed session when the
        // current key has moved — evidence must cite the verified generation.
        return Ok(StoredObject {
            version_id: session.completed_version_id.clone(),
            retention: session.retention,
        });
    }
    Ok(StoredObject {
        retention: session.retention,
        ..stored
    })
}

async fn recover_completed(
    backend: &dyn UploadBackend,
    object_uri: &str,
    checksum: Option<ObjectChecksum<'_>>,
    byte_length: u64,
    report: &mut MultipartReport,
) -> Result<StoredObject, VtopError> {
    let verification = backend
        .verify_object(object_uri, byte_length, checksum)
        .await;
    report.count(&verification);
    let verification = verification?;
    if !verification.passed || verification.backend_limited {
        return Err(VtopError::Upload(format!(
            "idempotent multipart complete could not strongly verify {object_uri}: {}",
            verification.message
        )));
    }
    let head = backend.head_object(object_uri).await;
    report.count(&head);
    head?;
    // head_object does not currently surface version id; leave None and let
    // callers that require versioning re-complete or treat as unversioned.
    // Mock/S3 complete paths return version ids on the happy path.
    Ok(StoredObject {
        version_id: None,
        retention: None,
    })
}

async fn revalidate_persisted_parts(
    session: &mut MultipartSession,
    local_path: &Path,
) -> Result<(), VtopError> {
    let mut stale = Vec::new();
    for (num, part) in &session.parts {
        let offset = u64::from(part.part_number.saturating_sub(1)) * session.part_size_bytes;
        let (_bytes, digest) = read_part_bytes(local_path, offset, part.size_bytes).await?;
        if digest != part.part_digest_hex {
            stale.push(*num);
        }
    }
    for num in stale {
        session.parts.remove(&num);
    }
    session.updated_at_unix_secs = now_unix_secs();
    Ok(())
}

/// How one part's requests ended.
enum PartError {
    /// The local file could not be read: no request was sent.
    Local(VtopError),
    /// The store no longer holds the upload; retrying cannot help.
    Gone(VtopError),
    /// The part's last attempt failed.
    Store { last: VtopError },
}

/// One part's run through its attempts.
struct PartRun {
    part_number: u32,
    attempts: u32,
    throttled: u32,
    outcome: Result<(UploadedPart, u64, String), PartError>,
}

/// What every part in one call shares.
struct PartPool<'a> {
    backend: &'a dyn UploadBackend,
    local_path: &'a Path,
    object_uri: &'a str,
    upload_id: &'a str,
    total: u32,
    attempts: u32,
    backoff: PartBackoff,
    /// Parts the session holds, for the retry notice.
    kept: &'a AtomicU32,
    /// Set once a part has failed for good: no part starts, and no part
    /// begins another attempt, after that.
    stop: &'a AtomicBool,
    notices: &'a RetryNotices,
}

impl PartPool<'_> {
    async fn send(&self, part_number: u32, offset: u64, len: u64) -> PartRun {
        let mut run = PartRun {
            part_number,
            attempts: 0,
            throttled: 0,
            outcome: Err(PartError::Local(VtopError::Upload("not started".into()))),
        };
        let (data, digest) = match read_part_bytes(self.local_path, offset, len).await {
            Ok(read) => read,
            Err(e) => {
                run.outcome = Err(PartError::Local(e));
                return run;
            }
        };
        loop {
            run.attempts += 1;
            // `UploadPart` is idempotent per part number: a retry after an
            // unknown outcome replaces the part, it cannot double it.
            let sent = self
                .backend
                .upload_part(self.object_uri, self.upload_id, part_number, data.clone())
                .await;
            let error = match sent {
                Ok(uploaded) => {
                    run.outcome = Ok((uploaded, len, digest));
                    return run;
                }
                Err(e) => e,
            };
            if error.is_upload_throttle() {
                run.throttled += 1;
            }
            if matches!(error, VtopError::UploadSessionGone(_)) {
                run.outcome = Err(PartError::Gone(error));
                return run;
            }
            if run.attempts >= self.attempts || self.stop.load(Ordering::SeqCst) {
                run.outcome = Err(PartError::Store { last: error });
                return run;
            }
            let delay = self
                .backoff
                .delay(run.attempts, jitter_seed(part_number, run.attempts));
            if let Some(suppressed) = self.notices.admit() {
                let cause = redact_urls(&error.to_string());
                tracing::warn!(
                    event = "multipart_part_retry",
                    key = self.object_uri,
                    part = part_number,
                    parts_total = self.total,
                    attempt = run.attempts,
                    attempts = self.attempts,
                    delay_ms = delay.as_millis() as u64,
                    throttled = error.is_upload_throttle(),
                    suppressed,
                    error = %cause,
                    "{}",
                    retry_message(
                        part_number,
                        self.total,
                        self.object_uri,
                        run.attempts,
                        self.attempts,
                        &cause,
                        delay,
                        self.kept.load(Ordering::SeqCst),
                        suppressed,
                    )
                );
            }
            tokio::time::sleep(delay).await;
            if self.stop.load(Ordering::SeqCst) {
                run.outcome = Err(PartError::Store { last: error });
                return run;
            }
        }
    }
}

/// The retry notice (#668): what happened, what the engine does, what to do.
#[expect(
    clippy::too_many_arguments,
    reason = "every argument is one fact the notice states; a struct would only name them twice"
)]
fn retry_message(
    part: u32,
    total: u32,
    object_uri: &str,
    attempt: u32,
    attempts: u32,
    cause: &str,
    delay: Duration,
    kept: u32,
    suppressed: u64,
) -> String {
    let kept = match kept {
        1 => "1 completed part is kept".to_owned(),
        n => format!("{n} completed parts are kept"),
    };
    let suppressed = match suppressed {
        0 => String::new(),
        n => {
            format!(" ({n} more part retries of this object were not logged since the last notice)")
        }
    };
    format!(
        "part {part} of {total} of {object_uri} failed (attempt {attempt} of {attempts}: \
         {cause}); retrying this part in {:.1} s; {kept}; nothing to do unless this repeats, \
         then check the link to the store{suppressed}",
        delay.as_secs_f64()
    )
}

/// At most one retry notice per object a minute, with how many were held
/// back in between (#668): a slow link can fail many parts in a row, and
/// each is counted in the report whether or not it is logged.
struct RetryNotices {
    every: Duration,
    state: std::sync::Mutex<(Option<tokio::time::Instant>, u64)>,
}

impl RetryNotices {
    fn new(every: Duration) -> Self {
        Self {
            every,
            state: std::sync::Mutex::new((None, 0)),
        }
    }

    /// `Some(suppressed since the last notice)` when one should be logged.
    fn admit(&self) -> Option<u64> {
        let now = tokio::time::Instant::now();
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        match state.0 {
            Some(last) if now.duration_since(last) < self.every => {
                state.1 += 1;
                None
            }
            _ => {
                let suppressed = state.1;
                *state = (Some(now), 0);
                Some(suppressed)
            }
        }
    }
}

fn jitter_seed(part: u32, attempt: u32) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    mix_seed(nanos, part, attempt)
}

/// The clock reading folded with the part and the attempt, so parts that fail
/// at the same instant still draw different waits. Apart from the clock it
/// is a function of its arguments, which is what lets a test fix the clock.
fn mix_seed(nanos: u64, part: u32, attempt: u32) -> u64 {
    nanos ^ (u64::from(part) << 32) ^ u64::from(attempt)
}

/// Remove what a URL in an error must not carry (#668, vtop-observability):
/// the user info and the query string, where a presigned request keeps its
/// signature and credential scope. The scheme, host and path stay, so the
/// message still says where the request went. `s3://` object paths are not
/// URLs of this kind and are left alone.
pub fn redact_urls(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    loop {
        let next = ["https://", "http://"]
            .iter()
            .filter_map(|scheme| rest.find(scheme).map(|at| (at, scheme.len())))
            .min();
        let Some((start, scheme_len)) = next else {
            out.push_str(rest);
            return out;
        };
        out.push_str(&rest[..start]);
        let end = rest[start..]
            .find(|c: char| {
                c.is_whitespace() || matches!(c, '"' | '\'' | '<' | '>' | ')' | ']' | '}')
            })
            .map(|offset| start + offset)
            .unwrap_or(rest.len());
        let url = &rest[start..end];
        let (scheme, after) = url.split_at(scheme_len);
        let authority_end = after.find(['/', '?', '#']).unwrap_or(after.len());
        let authority = &after[..authority_end];
        let host = authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host);
        let tail = &after[authority_end..];
        let path_end = tail.find(['?', '#']).unwrap_or(tail.len());
        out.push_str(scheme);
        out.push_str(host);
        out.push_str(&tail[..path_end]);
        if path_end < tail.len() {
            out.push_str("?[redacted]");
        }
        rest = &rest[end..];
    }
}

fn human_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    let b = bytes as f64;
    if b >= KIB * KIB * KIB {
        format!("{:.1} GiB", b / (KIB * KIB * KIB))
    } else if b >= KIB * KIB {
        format!("{:.1} MiB", b / (KIB * KIB))
    } else if b >= KIB {
        format!("{:.1} KiB", b / KIB)
    } else {
        format!("{bytes} bytes")
    }
}

/// Send every part the session does not hold (#668). Up to `max_parallelism`
/// parts are in flight, and the next one starts as soon as one finishes,
/// rather than in rounds that each wait for their slowest part. Every success
/// is written to the session the moment it arrives. On the first part that
/// fails for good no new part is started and no part begins another attempt,
/// but the requests already on the wire are waited for and their parts kept:
/// their bandwidth is already spent. (The rounds this replaces walked their
/// results with `?`, so a part that finished beside a failing one was
/// uploaded and then forgotten.)
async fn upload_missing_parts(
    backend: &dyn UploadBackend,
    cfg: &MultipartUploadConfig,
    session: &mut MultipartSession,
    local_path: &Path,
    session_path: &Path,
    report: &mut MultipartReport,
) -> Result<(), VtopError> {
    let total = part_count(session.fence.byte_length, session.part_size_bytes);
    if total == 0 {
        return Err(VtopError::Upload("multipart part count is zero".into()));
    }
    let missing: Vec<u32> = (1..=total)
        .filter(|n| !session.parts.contains_key(n))
        .collect();
    let mut pending = missing.into_iter();
    let width = cfg.max_parallelism.max(1);
    let kept = AtomicU32::new(session.parts.len() as u32);
    let stop = AtomicBool::new(false);
    let notices = RetryNotices::new(Duration::from_secs(60));
    let upload_id = session.upload_id.clone();
    let object_uri = session.object_uri.clone();
    let (part_size, byte_length) = (session.part_size_bytes, session.fence.byte_length);
    let pool = PartPool {
        backend,
        local_path,
        object_uri: &object_uri,
        upload_id: &upload_id,
        total,
        attempts: cfg.part_attempts.max(1),
        backoff: cfg.part_backoff,
        kept: &kept,
        stop: &stop,
        notices: &notices,
    };

    let mut in_flight = FuturesUnordered::new();
    let mut failed: Option<(u32, u32, VtopError)> = None;
    let mut gone: Option<VtopError> = None;
    let mut local: Option<VtopError> = None;
    loop {
        while !stop.load(Ordering::SeqCst) && in_flight.len() < width {
            let Some(part_number) = pending.next() else {
                break;
            };
            let offset = u64::from(part_number - 1) * part_size;
            let len = byte_length.saturating_sub(offset).min(part_size);
            in_flight.push(pool.send(part_number, offset, len));
        }
        let Some(run) = in_flight.next().await else {
            break;
        };
        report.requests += run.attempts;
        report.throttled_requests += run.throttled;
        report.part_retries += run.attempts.saturating_sub(1);
        match run.outcome {
            Ok((uploaded, len, digest)) => {
                session.parts.insert(
                    uploaded.part_number,
                    PersistedPart {
                        part_number: uploaded.part_number,
                        etag: uploaded.etag,
                        size_bytes: len,
                        part_digest_hex: digest,
                    },
                );
                session.updated_at_unix_secs = now_unix_secs();
                report.parts_uploaded += 1;
                kept.store(session.parts.len() as u32, Ordering::SeqCst);
                if let Err(e) = save_session(session_path, session) {
                    stop.store(true, Ordering::SeqCst);
                    local.get_or_insert(e);
                }
            }
            Err(PartError::Local(e)) => {
                stop.store(true, Ordering::SeqCst);
                local.get_or_insert(e);
            }
            Err(PartError::Gone(e)) => {
                stop.store(true, Ordering::SeqCst);
                gone.get_or_insert(e);
            }
            Err(PartError::Store { last }) => {
                report.parts_failed += 1;
                stop.store(true, Ordering::SeqCst);
                failed.get_or_insert((run.part_number, run.attempts, last));
            }
        }
    }
    // A lost upload first: whatever else failed, the next step is a new one.
    if let Some(e) = gone {
        return Err(e);
    }
    if let Some(e) = local {
        return Err(e);
    }
    if let Some((part, attempts, last)) = failed {
        let cause = redact_urls(&last.to_string());
        let message = format!(
            "part {part} of {total} of {object_uri} failed after {attempts} attempt{} \
             ({cause}); the upload session keeps {} of {total} parts",
            if attempts == 1 { "" } else { "s" },
            session.parts.len()
        );
        // A throttle stays a throttle, for a caller that tells them apart.
        return Err(if last.is_upload_throttle() {
            VtopError::UploadThrottled(message)
        } else {
            VtopError::Upload(message)
        });
    }
    if session.parts.len() as u32 != total {
        return Err(VtopError::Upload(format!(
            "multipart incomplete: have {} parts, need {total}",
            session.parts.len()
        )));
    }
    Ok(())
}

/// Abort and delete session files older than `abandon_after_secs`.
pub async fn cleanup_abandoned(
    backend: &dyn UploadBackend,
    cfg: &MultipartUploadConfig,
) -> Result<usize, VtopError> {
    let dir = &cfg.state_dir;
    if !dir.exists() {
        return Ok(0);
    }
    let now = now_unix_secs();
    let mut cleaned = 0_usize;
    let entries = std::fs::read_dir(dir).map_err(|e| {
        VtopError::Upload(format!(
            "listing multipart state dir {}: {e}",
            dir.display()
        ))
    })?;
    for entry in entries {
        let entry = entry
            .map_err(|e| VtopError::Upload(format!("reading multipart state dir entry: {e}")))?;
        let path = entry.path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if !name.ends_with(".multipart.json") {
            continue;
        }
        let session = match load_session(&path) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    event = "multipart_session_unreadable",
                    path = %path.display(),
                    error = %e,
                    "an unreadable multipart session file was skipped by the sweep ({e}); \
                     the upload it named, if any, is reclaimed only by a bucket lifecycle rule \
                     (AbortIncompleteMultipartUpload); remove the file once the bucket has one"
                );
                continue;
            }
        };
        let age = now.saturating_sub(session.updated_at_unix_secs);
        if age < cfg.abandon_after_secs {
            continue;
        }
        // The session file is the ONLY record of the remote upload id
        // (#482, review): deleting it after a FAILED abort orphans the
        // remote parts with nothing left to retry the abort against. So a
        // still-in-progress session is deleted only once its abort
        // succeeds; a failed abort leaves the session for the next sweep,
        // and a transient endpoint error self-heals when the store
        // recovers. A session already Completed has no live upload to
        // abort and is always removed.
        if session.phase == MultipartSessionPhase::InProgress {
            if let Err(e) = backend
                .abort_multipart_upload(&session.object_uri, &session.upload_id)
                .await
                // The store no longer holds it (#668): a lifecycle rule
                // aborted it already, which is what this sweep was for.
                .or_else(|e| match e {
                    VtopError::UploadSessionGone(_) => Ok(()),
                    other => Err(other),
                })
            {
                let cause = redact_urls(&e.to_string());
                tracing::warn!(
                    event = "multipart_abort_failed",
                    key = %session.object_uri,
                    error = %cause,
                    "could not abort the abandoned multipart upload for {} ({cause}); the \
                     session is kept so the next sweep tries again and the upload is not \
                     orphaned; nothing to do unless this repeats, then check the store",
                    session.object_uri
                );
                continue;
            }
        }
        delete_session(&path)?;
        cleaned += 1;
    }
    Ok(cleaned)
}

/// Abort a live session that failed a fence check (operator-driven).
pub async fn abort_session(
    backend: &dyn UploadBackend,
    state_dir: &Path,
    object_uri: &str,
    fence: &MultipartFence,
) -> Result<(), VtopError> {
    let path = session_path(state_dir, object_uri, fence);
    if let Ok(session) = load_session(&path) {
        if session.phase == MultipartSessionPhase::InProgress {
            match backend
                .abort_multipart_upload(&session.object_uri, &session.upload_id)
                .await
            {
                // Already gone at the store (#668): nothing left to abort.
                Ok(()) | Err(VtopError::UploadSessionGone(_)) => {}
                Err(e) => return Err(e),
            }
        }
        delete_session(&path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockBackend;
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn tmp_file(data: &[u8]) -> (tempfile::NamedTempFile, String) {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(data).unwrap();
        f.flush().unwrap();
        let digest = vtop_core::checksum::blake3_bytes(data);
        (f, digest)
    }

    fn fence(digest: &str, len: u64, generation: u64) -> MultipartFence {
        MultipartFence {
            expected_segment_generation: generation,
            fencing_epoch: 1,
            content_digest_hex: digest.to_owned(),
            content_digest_algorithm: "blake3".to_owned(),
            byte_length: len,
        }
    }

    #[test]
    fn tuning_maps_onto_the_old_multipart_keys_equivalently() {
        // The old multipart keys still work (#480): a config using only
        // multipart_part_size_bytes / multipart_max_parallelism resolves to the
        // SAME part size and parallelism as the equivalent upload.transports
        // .tcp_tls block, so no existing deployment changes behaviour and the
        // two spellings are interchangeable.
        use vtop_core::config::UploadConfig;
        let old: UploadConfig = serde_json::from_str(
            r#"{"bucket":"b","multipart_part_size_bytes":12345678,"multipart_max_parallelism":7}"#,
        )
        .unwrap();
        let new: UploadConfig = serde_json::from_str(
            r#"{"bucket":"b","transports":{"tcp_tls":{"part_size_bytes":12345678,"parts_in_flight":7}}}"#,
        )
        .unwrap();
        let dir = PathBuf::from("/tmp/x");
        let old_cfg = MultipartUploadConfig::from_upload(&old, dir.clone());
        let new_cfg = MultipartUploadConfig::from_upload(&new, dir);
        assert_eq!(old_cfg.part_size_bytes, new_cfg.part_size_bytes);
        assert_eq!(old_cfg.max_parallelism, new_cfg.max_parallelism);
        assert_eq!(new_cfg.part_size_bytes, 12_345_678);
        assert_eq!(new_cfg.max_parallelism, 7);
    }

    #[test]
    fn a_tuning_block_on_a_non_s3_native_backend_never_reaches_the_multipart_knobs() {
        // Only s3_native routes through the EgressTransport seam (#480,
        // review), and validation skips the whole `transports` map for any
        // other backend — so a mock run carrying a stray tcp_tls block must
        // keep the plain multipart_* knobs. The mock backend advertises
        // supports_multipart(), so without the gate this run would chunk at a
        // part size nobody validated while the benchmark summary records the
        // tuning as blank. The zero parallelism is deliberate: it is a value
        // validation would have refused for s3_native but never sees here, and
        // applying it would fail the upload outright.
        use vtop_core::config::UploadConfig;
        let mock: UploadConfig = serde_json::from_str(
            r#"{"bucket":"b","backend":"mock","multipart_part_size_bytes":12345678,
                "multipart_max_parallelism":7,
                "transports":{"tcp_tls":{"part_size_bytes":999,"parts_in_flight":0}}}"#,
        )
        .unwrap();
        let cfg = MultipartUploadConfig::from_upload(&mock, PathBuf::from("/tmp/x"));
        assert_eq!(
            cfg.part_size_bytes, 12_345_678,
            "a mock run must keep multipart_part_size_bytes, not the unvalidated tuning"
        );
        assert_eq!(
            cfg.max_parallelism, 7,
            "a mock run must keep multipart_max_parallelism, not a parallelism of zero"
        );

        // The same block on s3_native — the backend that does route through the
        // seam — is honoured, so the gate is on the backend and not on the block.
        let s3: UploadConfig = serde_json::from_str(
            r#"{"bucket":"b","multipart_part_size_bytes":12345678,
                "transports":{"tcp_tls":{"part_size_bytes":999}}}"#,
        )
        .unwrap();
        let cfg = MultipartUploadConfig::from_upload(&s3, PathBuf::from("/tmp/x"));
        assert_eq!(
            cfg.part_size_bytes, 999,
            "s3_native does read the tuning block"
        );
    }

    fn cfg(dir: &Path, part_size: u64) -> MultipartUploadConfig {
        MultipartUploadConfig {
            part_size_bytes: part_size,
            threshold_bytes: part_size,
            max_parallelism: 2,
            abandon_after_secs: 60,
            state_dir: dir.to_path_buf(),
            part_attempts: 3,
            part_backoff: quick(),
        }
    }

    /// A backoff short enough for a test on the real clock.
    fn quick() -> PartBackoff {
        PartBackoff {
            base: Duration::from_millis(1),
            cap: Duration::from_millis(4),
        }
    }

    /// #482, THE test the fix must turn green — asserted as the DESIRED end
    /// state, failing today by the exact six-link chain the issue walks:
    /// the engine's default checksum is SHA-256; multipart creation never
    /// requests a whole-object checksum; S3 (here, the S3-semantics mock,
    /// judging through the SAME shared function as the real backend)
    /// returns no whole-object SHA-256 for a multipart completion; the
    /// SHA-256 arm therefore reads LIMITED; and require_strong_verification
    /// — the default — fails the batch. An above-threshold object uploaded
    /// through the multipart path under the default configuration must
    /// verify NON-limited, or the engine cannot be given this path.
    #[tokio::test]
    async fn a_multipart_object_under_default_sha256_verifies_strongly() {
        let data = vec![5_u8; 10_000];
        let sha256 = vtop_core::checksum::sha256_bytes(&data);
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&data).unwrap();
        f.flush().unwrap();
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new().with_s3_checksum_semantics();
        let uri = "s3://bucket/batch.jsonl.gz";
        upload_resumable(
            &backend,
            &cfg(state.path(), 3_000),
            f.path(),
            uri,
            Some(ObjectChecksum::new("sha256", &sha256)),
            MultipartFence {
                expected_segment_generation: 0,
                fencing_epoch: 0,
                content_digest_hex: sha256.clone(),
                content_digest_algorithm: "sha256".to_owned(),
                byte_length: data.len() as u64,
            },
        )
        .await
        .unwrap();
        let res = backend
            .verify_object(
                uri,
                data.len() as u64,
                Some(ObjectChecksum::new("sha256", &sha256)),
            )
            .await
            .unwrap();
        assert!(
            res.passed && !res.backend_limited,
            "a multipart object under the DEFAULT config must verify strongly, \
             or wiring multipart into the engine fails every batch above the \
             threshold: {res:?}"
        );
    }

    /// The backend minimum gates should_multipart (#482, review): S3
    /// rejects a non-final part below 5 MiB, so a sub-floor part size must
    /// not select multipart regardless of threshold and object size. The
    /// s3_native floor is a named constant so the number cannot drift.
    #[test]
    fn should_multipart_refuses_a_part_size_below_the_backend_floor() {
        assert_eq!(
            crate::s3_native::S3NativeBackend::part_size_floor(),
            5 * 1024 * 1024
        );
        let dir = tempfile::tempdir().unwrap();
        let cfg = MultipartUploadConfig {
            part_size_bytes: 3_000,
            threshold_bytes: 3_000,
            max_parallelism: 2,
            abandon_after_secs: 60,
            state_dir: dir.path().to_path_buf(),
            part_attempts: 3,
            part_backoff: PartBackoff::default(),
        };
        assert!(
            cfg.should_multipart(&MockBackend::new(), 10_000),
            "a backend with no floor accepts the small part size"
        );
        assert!(
            !cfg.should_multipart(
                &MockBackend::new().with_min_part_size(5 * 1024 * 1024),
                10_000
            ),
            "a 3 KB part size must not multipart to a 5 MiB-floor backend"
        );
    }

    /// The S3-semantics mock's own faithfulness pin: a SINGLE put under the
    /// same semantics verifies strongly through the service SHA-256 — the
    /// path production takes today — so the failing multipart test above is
    /// isolating the multipart hole, not a broken mock.
    #[tokio::test]
    async fn a_single_put_under_s3_semantics_verifies_strongly_via_the_service_sha256() {
        let data = vec![5_u8; 1_000];
        let sha256 = vtop_core::checksum::sha256_bytes(&data);
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&data).unwrap();
        f.flush().unwrap();
        let backend = MockBackend::new().with_s3_checksum_semantics();
        let uri = "s3://bucket/small.jsonl.gz";
        backend
            .put_object(f.path(), uri, Some(ObjectChecksum::new("sha256", &sha256)))
            .await
            .unwrap();
        let res = backend
            .verify_object(
                uri,
                data.len() as u64,
                Some(ObjectChecksum::new("sha256", &sha256)),
            )
            .await
            .unwrap();
        assert!(res.passed && !res.backend_limited, "{res:?}");
    }

    #[tokio::test]
    async fn multipart_uploads_and_verifies() {
        let data = vec![7_u8; 10_000];
        let (file, digest) = tmp_file(&data);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new();
        let uri = "s3://bucket/large.segment";
        let stored = upload_resumable(
            &backend,
            &cfg(state.path(), 3_000),
            file.path(),
            uri,
            Some(ObjectChecksum::new("blake3", &digest)),
            fence(&digest, data.len() as u64, 1),
        )
        .await
        .unwrap();
        assert!(stored.version_id.is_some());
        let res = backend
            .verify_object(
                uri,
                data.len() as u64,
                Some(ObjectChecksum::new("blake3", &digest)),
            )
            .await
            .unwrap();
        assert!(res.passed && !res.backend_limited);
        // Session file removed after success.
        assert!(std::fs::read_dir(state.path()).unwrap().next().is_none());
    }

    #[tokio::test]
    async fn resume_skips_completed_parts_after_interrupt() {
        let data = vec![9_u8; 9_000];
        let (file, digest) = tmp_file(&data);
        let state = tempfile::tempdir().unwrap();
        let fail_after = Arc::new(AtomicUsize::new(2));
        let backend = MockBackend::new().with_multipart_fail_after_parts(fail_after.clone());
        let uri = "s3://bucket/resume.segment";
        let f = fence(&digest, data.len() as u64, 3);
        let cfg = cfg(state.path(), 2_000);
        let err = upload_resumable(
            &backend,
            &cfg,
            file.path(),
            uri,
            Some(ObjectChecksum::new("blake3", &digest)),
            f.clone(),
        )
        .await
        .expect_err("injected failure after 2 parts");
        assert!(err.to_string().contains("injected"), "{err}");

        // Session persisted with some parts.
        let session_path = session_path(state.path(), uri, &f);
        let session = load_session(&session_path).unwrap();
        assert_eq!(session.parts.len(), 2);
        assert_eq!(session.phase, MultipartSessionPhase::InProgress);

        // Clear the fault and resume — should not re-upload the two parts.
        fail_after.store(usize::MAX, Ordering::SeqCst);
        let parts_before = backend.multipart_parts_uploaded();
        let stored = upload_resumable(
            &backend,
            &cfg,
            file.path(),
            uri,
            Some(ObjectChecksum::new("blake3", &digest)),
            f,
        )
        .await
        .unwrap();
        assert!(stored.version_id.is_some());
        let parts_after = backend.multipart_parts_uploaded();
        // 9_000 / 2_000 = 5 parts total; 2 already done ⇒ 3 new uploads.
        assert_eq!(parts_after - parts_before, 3);
    }

    #[tokio::test]
    async fn complete_is_idempotent_when_session_already_completed() {
        let data = vec![1_u8; 4_000];
        let (file, digest) = tmp_file(&data);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new();
        let uri = "s3://bucket/idem.segment";
        let f = fence(&digest, data.len() as u64, 1);
        let cfg = cfg(state.path(), 1_500);
        let first = upload_resumable(
            &backend,
            &cfg,
            file.path(),
            uri,
            Some(ObjectChecksum::new("blake3", &digest)),
            f.clone(),
        )
        .await
        .unwrap();

        // Manually write a completed session pointing at the same object.
        let path = session_path(state.path(), uri, &f);
        let now = now_unix_secs();
        save_session(
            &path,
            &MultipartSession {
                schema_version: SESSION_SCHEMA,
                backend_id: "mock".into(),
                object_uri: uri.into(),
                local_path: file.path().to_path_buf(),
                upload_id: "already-done".into(),
                part_size_bytes: 1_500,
                fence: f.clone(),
                phase: MultipartSessionPhase::Completed,
                parts: BTreeMap::new(),
                created_at_unix_secs: now,
                updated_at_unix_secs: now,
                completed_version_id: first.version_id.clone(),
                retention: None,
            },
        )
        .unwrap();

        let second = upload_resumable(
            &backend,
            &cfg,
            file.path(),
            uri,
            Some(ObjectChecksum::new("blake3", &digest)),
            f,
        )
        .await
        .unwrap();
        assert_eq!(second.version_id, first.version_id);
    }

    #[tokio::test]
    async fn stale_generation_fence_refuses_resume() {
        let data = vec![2_u8; 5_000];
        let (file, digest) = tmp_file(&data);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new();
        let uri = "s3://bucket/fence.segment";
        let fail_after = Arc::new(AtomicUsize::new(1));
        let backend = backend.with_multipart_fail_after_parts(fail_after);
        let cfg = cfg(state.path(), 2_000);
        let _ = upload_resumable(
            &backend,
            &cfg,
            file.path(),
            uri,
            Some(ObjectChecksum::new("blake3", &digest)),
            fence(&digest, data.len() as u64, 1),
        )
        .await
        .expect_err("interrupt");

        let err = upload_resumable(
            &backend,
            &cfg,
            file.path(),
            uri,
            Some(ObjectChecksum::new("blake3", &digest)),
            fence(&digest, data.len() as u64, 99),
        )
        .await
        .expect_err("generation 99 must not resume generation 1 session");
        // Different fence ⇒ different session file ⇒ starts new upload; that
        // is fine. Explicit same-file fence mismatch is tested via load+validate.
        let path = session_path(state.path(), uri, &fence(&digest, data.len() as u64, 1));
        let session = load_session(&path).unwrap();
        let mismatch = validate_resume(
            &session,
            &backend,
            uri,
            &fence(&digest, data.len() as u64, 99),
        )
        .expect_err("fence mismatch");
        assert!(
            mismatch.to_string().contains("fence mismatch"),
            "{mismatch}"
        );
        let _ = err;
    }

    #[tokio::test]
    async fn a_parallelism_above_tokios_ceiling_is_refused_before_the_semaphore_is_built() {
        // `vtopctl tier copy` applies --multipart-max-parallelism to the
        // DERIVED MultipartUploadConfig, after build_backend has already
        // validated the UploadConfig it was handed — so the number that
        // becomes the permit count below was never the number the config
        // check saw (review). Above tokio's ceiling `Semaphore::new` does not
        // degrade, it panics, and a tier copy would take the process down on
        // the first eligible object rather than refuse a typo'd flag.
        //
        // The guard belongs here, beside the zero guard and for the same
        // reason: this is the one door every caller reaches the semaphore
        // through, so a flag nobody has written yet cannot go around it.
        let data = vec![7_u8; 4_000];
        let (file, digest) = tmp_file(&data);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new();
        let mut cfg = cfg(state.path(), 2_000);
        cfg.max_parallelism = usize::MAX;
        let err = upload_resumable(
            &backend,
            &cfg,
            file.path(),
            "s3://bucket/oversized-parallelism.segment",
            Some(ObjectChecksum::new("blake3", &digest)),
            fence(&digest, data.len() as u64, 1),
        )
        .await
        .expect_err(
            "an oversized parallelism must be refused as a config error; reaching \
             Semaphore::new with it aborts the whole process mid-copy",
        );
        assert!(
            err.to_string().contains("semaphore")
                && err.to_string().contains(&usize::MAX.to_string()),
            "the refusal must name the ceiling it enforces AND the value it saw, or an \
             operator cannot tell which flag to fix: {err}"
        );
    }

    #[tokio::test]
    async fn abandoned_cleanup_aborts_and_deletes() {
        let data = vec![3_u8; 3_000];
        let (file, digest) = tmp_file(&data);
        let state = tempfile::tempdir().unwrap();
        let backend =
            MockBackend::new().with_multipart_fail_after_parts(Arc::new(AtomicUsize::new(1)));
        let uri = "s3://bucket/abandon.segment";
        let f = fence(&digest, data.len() as u64, 1);
        let mut cfg = cfg(state.path(), 1_000);
        let _ = upload_resumable(
            &backend,
            &cfg,
            file.path(),
            uri,
            Some(ObjectChecksum::new("blake3", &digest)),
            f.clone(),
        )
        .await
        .expect_err("interrupt");
        assert!(backend.pending_multipart_count() >= 1);

        // Age the session.
        let path = session_path(state.path(), uri, &f);
        let mut session = load_session(&path).unwrap();
        session.updated_at_unix_secs = 1;
        save_session(&path, &session).unwrap();
        cfg.abandon_after_secs = 10;

        let cleaned = cleanup_abandoned(&backend, &cfg).await.unwrap();
        assert_eq!(cleaned, 1);
        assert!(!path.exists());
        assert_eq!(backend.pending_multipart_count(), 0);
    }

    /// A failed abort keeps the session (#482, review): the session file is
    /// the only record of the remote upload id, so deleting it after the
    /// abort failed would orphan the remote parts with nothing left to
    /// retry against. The next sweep tries again once the store recovers.
    #[tokio::test]
    async fn a_failed_abort_keeps_the_session_for_the_next_sweep() {
        let data = vec![3_u8; 3_000];
        let (file, digest) = tmp_file(&data);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new()
            .with_multipart_fail_after_parts(Arc::new(AtomicUsize::new(1)))
            .with_failing_abort();
        let uri = "s3://bucket/abandon.segment";
        let f = fence(&digest, data.len() as u64, 1);
        let mut cfg = cfg(state.path(), 1_000);
        let _ = upload_resumable(
            &backend,
            &cfg,
            file.path(),
            uri,
            Some(ObjectChecksum::new("blake3", &digest)),
            f.clone(),
        )
        .await
        .expect_err("interrupt");
        let path = session_path(state.path(), uri, &f);
        let mut session = load_session(&path).unwrap();
        session.updated_at_unix_secs = 1;
        save_session(&path, &session).unwrap();
        cfg.abandon_after_secs = 10;

        let cleaned = cleanup_abandoned(&backend, &cfg).await.unwrap();
        assert_eq!(cleaned, 0, "a failed abort cleans nothing");
        assert!(path.exists(), "the session survives for the next sweep");
        assert!(
            backend.pending_multipart_count() >= 1,
            "the remote upload is still tracked, not orphaned"
        );
    }

    fn three_part_file(seed: u8) -> (tempfile::NamedTempFile, Vec<u8>, String) {
        let data: Vec<u8> = (0..3_000_u32).map(|i| seed ^ (i % 251) as u8).collect();
        let (file, digest) = tmp_file(&data);
        (file, data, digest)
    }

    async fn run(
        backend: &MockBackend,
        cfg: &MultipartUploadConfig,
        path: &Path,
        uri: &str,
        digest: &str,
        len: u64,
    ) -> (Result<StoredObject, VtopError>, MultipartReport) {
        let mut report = MultipartReport::default();
        let result = upload_resumable_with_report(
            backend,
            cfg,
            path,
            uri,
            Some(ObjectChecksum::new("blake3", digest)),
            fence(digest, len, 1),
            &mut report,
        )
        .await;
        (result, report)
    }

    /// #668: the report counts every request, so the engine's throttle count
    /// and metrics see what the per-part retries did, on success...
    #[tokio::test]
    async fn the_report_counts_every_request_and_every_retry() {
        let (file, data, digest) = three_part_file(1);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new().with_part_fault(2, crate::PartFault::Throttled, 1);
        let (result, report) = run(
            &backend,
            &cfg(state.path(), 1_000),
            file.path(),
            "s3://b/report.raw",
            &digest,
            data.len() as u64,
        )
        .await;
        result.unwrap();
        assert_eq!(
            report,
            MultipartReport {
                parts_total: 3,
                parts_reused: 0,
                bytes_reused: 0,
                parts_uploaded: 3,
                part_retries: 1,
                parts_failed: 0,
                // create + four part requests + complete
                requests: 6,
                throttled_requests: 1,
                resumed: false,
                restarted: false,
                completed: true,
                parts_kept: 0,
                session_left: false,
            }
        );
    }

    /// ...and on failure, with what the session keeps, and then on the resume.
    #[tokio::test(start_paused = true)]
    async fn the_report_says_what_was_kept_and_what_a_resume_reused() {
        let (file, data, digest) = three_part_file(2);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new().with_part_fault(2, crate::PartFault::Reset, usize::MAX);
        let mut cfg = cfg(state.path(), 1_000);
        cfg.max_parallelism = 3;
        let uri = "s3://b/kept.raw";
        let (result, report) =
            run(&backend, &cfg, file.path(), uri, &digest, data.len() as u64).await;
        result.expect_err("part 2 never succeeds");
        assert_eq!(report.parts_failed, 1);
        assert_eq!(report.part_retries, 2, "part 2 had three attempts");
        assert_eq!(report.parts_kept, 2);
        assert!(report.session_left && !report.completed);

        backend.set_part_fault(2, crate::PartFault::Reset, 0);
        let (result, report) =
            run(&backend, &cfg, file.path(), uri, &digest, data.len() as u64).await;
        result.unwrap();
        assert!(report.resumed && report.completed && !report.restarted);
        assert_eq!(
            (
                report.parts_reused,
                report.bytes_reused,
                report.parts_uploaded
            ),
            (2, 2_000, 1)
        );
    }

    #[tokio::test]
    async fn the_report_says_when_a_lost_upload_was_restarted() {
        let (file, data, digest) = three_part_file(3);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new().with_part_fault(1, crate::PartFault::SessionGone, 1);
        let (result, report) = run(
            &backend,
            &cfg(state.path(), 1_000),
            file.path(),
            "s3://b/restart.raw",
            &digest,
            data.len() as u64,
        )
        .await;
        result.unwrap();
        assert!(report.restarted && report.completed, "{report:?}");
    }

    /// The wait before each retry is bounded by an exponential ceiling (1 s,
    /// 2 s, 4 s ... capped at 30 s) and drawn anywhere below it (full
    /// jitter), so parts that failed together do not return together.
    #[test]
    fn the_backoff_doubles_to_its_cap_and_is_jittered_below_it() {
        let b = PartBackoff::default();
        let ceilings: Vec<u64> = (1..=8).map(|n| b.ceiling(n).as_secs()).collect();
        assert_eq!(ceilings, vec![1, 2, 4, 8, 16, 30, 30, 30]);
        assert_eq!(b.ceiling(u32::MAX), Duration::from_secs(30), "no overflow");
        let delays: std::collections::BTreeSet<Duration> =
            (0..64_u64).map(|seed| b.delay(3, seed)).collect();
        assert!(delays.iter().all(|d| *d <= Duration::from_secs(4)));
        assert!(
            delays.len() > 32,
            "jitter spreads the waits: {} distinct of 64",
            delays.len()
        );
    }

    /// A part that never succeeds costs exactly its attempts, and the waits
    /// between them stay under the backoff's ceilings (paused clock).
    #[tokio::test(start_paused = true)]
    async fn a_permanent_failure_costs_its_attempts_and_bounded_waits() {
        let (file, data, digest) = three_part_file(4);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new().with_part_fault(1, crate::PartFault::Reset, usize::MAX);
        let mut cfg = cfg(state.path(), 1_000);
        cfg.max_parallelism = 1;
        cfg.part_attempts = 5;
        cfg.part_backoff = PartBackoff::default();
        let started = tokio::time::Instant::now();
        let (result, report) = run(
            &backend,
            &cfg,
            file.path(),
            "s3://b/permanent.raw",
            &digest,
            data.len() as u64,
        )
        .await;
        result.expect_err("part 1 never succeeds");
        assert_eq!(backend.part_sends(1), 5);
        assert_eq!(report.part_retries, 4);
        // The paused clock moves only by the waits: more than nothing (four
        // retries with no pause would leave it at zero), at most the sum of
        // their ceilings.
        assert!(
            started.elapsed() > Duration::ZERO
                && started.elapsed() <= Duration::from_secs(1 + 2 + 4 + 8),
            "four waits, each under its ceiling: {:?}",
            started.elapsed()
        );
    }

    /// A presigned URL in an SDK error keeps its signature in the query
    /// string; the messages this module writes never carry it, nor user info.
    #[test]
    fn urls_in_a_cause_lose_their_query_and_user_info() {
        let cause = "dispatch failure: io error sending request to \
            https://AKIDEXAMPLE:secret@bucket.s3.example.com/key/part?uploadId=abc&partNumber=2\
            &X-Amz-Signature=deadbeef&X-Amz-Credential=AKIDEXAMPLE%2F20261114 (connection reset); \
            object s3://bucket/key";
        let redacted = redact_urls(cause);
        for secret in [
            "X-Amz-Signature",
            "deadbeef",
            "AKIDEXAMPLE",
            "secret",
            "uploadId",
        ] {
            assert!(!redacted.contains(secret), "{secret} leaked: {redacted}");
        }
        assert!(
            redacted.contains("https://bucket.s3.example.com/key/part?[redacted]"),
            "{redacted}"
        );
        assert!(redacted.contains("object s3://bucket/key"), "{redacted}");
        assert_eq!(redact_urls("no url here"), "no url here");
    }

    #[test]
    fn the_retry_notice_says_what_happened_what_is_done_and_what_to_do() {
        let message = retry_message(
            2,
            3,
            "s3://b/k",
            1,
            3,
            "connection reset",
            Duration::from_millis(2_100),
            1,
            0,
        );
        assert_eq!(
            message,
            "part 2 of 3 of s3://b/k failed (attempt 1 of 3: connection reset); retrying \
             this part in 2.1 s; 1 completed part is kept; nothing to do unless this \
             repeats, then check the link to the store"
        );
        let message = retry_message(2, 3, "s3://b/k", 2, 3, "x", Duration::ZERO, 2, 5);
        assert!(message.contains("2 completed parts are kept"), "{message}");
        assert!(message.contains("(5 more part retries"), "{message}");
    }

    /// The notices are rate-limited per object, and say how many were held.
    #[tokio::test(start_paused = true)]
    async fn retry_notices_are_rate_limited_with_a_suppressed_count() {
        let notices = RetryNotices::new(Duration::from_secs(60));
        assert_eq!(notices.admit(), Some(0));
        assert_eq!(notices.admit(), None);
        assert_eq!(notices.admit(), None);
        tokio::time::advance(Duration::from_secs(61)).await;
        assert_eq!(notices.admit(), Some(2));
    }

    /// A session file written by the v0.8.0 release's own code still resumes
    /// after #668: from another local path, its kept part checked against
    /// the bytes and reused, and only the missing parts sent. The fixture was
    /// generated at the v0.8.0 tag, not by the code under test: that tag's
    /// `upload_resumable`, mock backend and `with_multipart_fail_after_parts(1)`,
    /// a 3000-byte file of `5 ^ (i % 251)` read from `/out/fixture.raw`,
    /// 1000-byte parts, one in flight, uploaded to `s3://bucket/fixture.raw`
    /// under a BLAKE3 fence of generation 1 and epoch 1. It failed after
    /// part 1, and the session file it left is committed as written.
    #[tokio::test]
    async fn a_session_written_by_v0_8_0_resumes() {
        let (file, data, digest) = three_part_file(5);
        let state = tempfile::tempdir().unwrap();
        let uri = "s3://bucket/fixture.raw";
        let f = fence(&digest, data.len() as u64, 1);
        let text = include_str!("../tests/fixtures/multipart-session-v0.8.0.json");
        let written: MultipartSession = serde_json::from_str(text).unwrap();
        assert_eq!(written.fence, f, "the fixture is for these bytes");
        assert_ne!(
            written.local_path,
            file.path(),
            "and was read from another path"
        );
        std::fs::write(state.path().join(session_file_name(uri, &f)), text).unwrap();
        let backend = MockBackend::new();
        backend.seed_multipart(uri, &written.upload_id, vec![(1, data[..1_000].to_vec())]);

        let (result, report) = run(
            &backend,
            &cfg(state.path(), 1_000),
            file.path(),
            uri,
            &digest,
            data.len() as u64,
        )
        .await;
        result.unwrap();
        assert!(report.resumed, "{report:?}");
        assert_eq!(backend.part_sends(1), 0, "the kept part is not sent again");
        assert_eq!([backend.part_sends(2), backend.part_sends(3)], [1, 1]);
        let v = backend
            .verify_object(
                uri,
                data.len() as u64,
                Some(ObjectChecksum::new("blake3", &digest)),
            )
            .await
            .unwrap();
        assert!(v.passed && !v.backend_limited, "{v:?}");
    }

    // ---- #729: what the resumable upload accepts, refuses and keeps ----

    /// A call that never ends is a failure of its own, not a hung test: a
    /// retry loop that loses its bound would otherwise stall the suite.
    async fn bounded<F: std::future::Future>(call: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(60), call)
            .await
            .expect("the upload call never finished: its retries must be bounded")
    }

    async fn upload_resumable(
        backend: &dyn UploadBackend,
        cfg: &MultipartUploadConfig,
        local_path: &Path,
        object_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
        fence: MultipartFence,
    ) -> Result<StoredObject, VtopError> {
        bounded(super::upload_resumable(
            backend, cfg, local_path, object_uri, checksum, fence,
        ))
        .await
    }

    async fn upload_resumable_with_report(
        backend: &dyn UploadBackend,
        cfg: &MultipartUploadConfig,
        local_path: &Path,
        object_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
        fence: MultipartFence,
        report: &mut MultipartReport,
    ) -> Result<StoredObject, VtopError> {
        bounded(super::upload_resumable_with_report(
            backend, cfg, local_path, object_uri, checksum, fence, report,
        ))
        .await
    }

    /// The operator-facing events a test run logged, by their `event` field.
    #[derive(Clone, Default)]
    struct EventLog(Arc<std::sync::Mutex<Vec<String>>>);

    struct EventName(Option<String>);

    impl tracing::field::Visit for EventName {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "event" {
                self.0 = Some(value.to_owned());
            }
        }
        fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
    }

    impl tracing::Subscriber for EventLog {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            let mut name = EventName(None);
            event.record(&mut name);
            if let Some(name) = name.0 {
                self.0.lock().unwrap().push(name);
            }
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    impl EventLog {
        fn count(&self, event: &str) -> usize {
            self.0
                .lock()
                .unwrap()
                .iter()
                .filter(|e| *e == event)
                .count()
        }
    }

    fn requested(mode: crate::base::ObjectLockMode, hold: bool) -> crate::base::RequestedRetention {
        crate::base::RequestedRetention {
            mode,
            retain_until_unix_secs: 4_000_000_000,
            legal_hold: hold,
        }
    }

    fn lock_policy(mode: crate::base::ObjectLockMode, hold: bool) -> crate::base::ObjectLockPolicy {
        crate::base::ObjectLockPolicy::from_config(&crate::base::ObjectLockConfig {
            mode,
            retain_days: 30,
            legal_hold: hold,
        })
    }

    /// A session an earlier process left, with nothing uploaded yet.
    fn earlier_session(
        uri: &str,
        local: &Path,
        fence: &MultipartFence,
        part_size: u64,
        retention: Option<crate::base::RequestedRetention>,
    ) -> MultipartSession {
        let now = now_unix_secs();
        MultipartSession {
            schema_version: SESSION_SCHEMA,
            backend_id: "mock".into(),
            object_uri: uri.into(),
            local_path: local.to_path_buf(),
            upload_id: "mpu-earlier".into(),
            part_size_bytes: part_size,
            fence: fence.clone(),
            phase: MultipartSessionPhase::InProgress,
            parts: BTreeMap::new(),
            created_at_unix_secs: now,
            updated_at_unix_secs: now,
            completed_version_id: None,
            retention,
        }
    }

    /// The wait is the first output of splitmix64 seeded with the `seed`,
    /// scaled into `[0, ceiling]`: the published test vectors of that
    /// generator for seeds 0 and 1, so the draw is uniform and the same on
    /// every platform.
    #[test]
    fn the_jitter_is_a_uniform_draw_of_the_seed() {
        let backoff = PartBackoff {
            base: Duration::from_secs(1_000),
            cap: Duration::from_secs(1_000),
        };
        for (seed, output) in [
            (0_u64, 0xe220_a839_7b1d_cdaf_u64),
            (1, 0x910a_2dec_8902_5cc1),
        ] {
            let fraction = (output >> 11) as f64 / (1_u64 << 53) as f64;
            assert_eq!(
                backoff.delay(1, seed),
                Duration::from_secs(1_000).mul_f64(fraction),
                "seed {seed}"
            );
        }
        // A draw never exceeds its ceiling, whatever the seed.
        let default = PartBackoff::default();
        for seed in (0..2_000_u64).map(|n| n.wrapping_mul(0x9e37_79b9_7f4a_7c15)) {
            assert!(default.delay(2, seed) <= default.ceiling(2), "seed {seed}");
        }
    }

    /// Parts that fail at the same instant must not draw the same wait: the
    /// seed differs for every part and every attempt, whatever the clock.
    #[test]
    fn the_seed_differs_for_every_part_and_attempt_at_one_clock_reading() {
        let nanos = 0x0123_4567_89ab_cdef_u64;
        let seeds: std::collections::BTreeSet<u64> = (1..=8_u32)
            .flat_map(|part| (1..=8_u32).map(move |attempt| mix_seed(nanos, part, attempt)))
            .collect();
        assert_eq!(seeds.len(), 64);
        assert_ne!(jitter_seed(1, 1), jitter_seed(2, 1));
    }

    #[test]
    fn a_session_is_not_taken_for_an_object_of_no_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = MultipartUploadConfig {
            threshold_bytes: 0,
            ..cfg(dir.path(), 1_000)
        };
        let backend = MockBackend::new();
        assert!(!cfg.should_multipart(&backend, 0), "nothing to send");
        assert!(cfg.should_multipart(&backend, 1));
        let cfg = MultipartUploadConfig {
            threshold_bytes: 5_000,
            ..cfg
        };
        assert!(!cfg.should_multipart(&backend, 4_999));
        assert!(
            cfg.should_multipart(&backend, 5_000),
            "the threshold itself multiparts"
        );
    }

    /// A session file that is already gone is deleted; one that cannot be
    /// deleted for another reason is an error the caller sees.
    #[test]
    fn deleting_a_session_tolerates_a_missing_file_and_nothing_else() {
        let dir = tempfile::tempdir().unwrap();
        delete_session(&dir.path().join("absent.multipart.json")).unwrap();
        let present = dir.path().join("present.multipart.json");
        std::fs::write(&present, b"{}").unwrap();
        delete_session(&present).unwrap();
        assert!(!present.exists());
        // A directory is not a file the sweep may remove.
        let not_a_file = dir.path().join("dir.multipart.json");
        std::fs::create_dir(&not_a_file).unwrap();
        let err = delete_session(&not_a_file).unwrap_err();
        assert!(
            err.to_string().contains("deleting multipart session"),
            "{err}"
        );
    }

    /// Each unusable setting is refused ON ITS OWN, before anything is sent.
    #[tokio::test]
    async fn every_unusable_tuning_value_is_refused_alone() {
        let (file, data, digest) = three_part_file(7);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new();
        let good = cfg(state.path(), 1_000);
        let broken = [
            MultipartUploadConfig {
                part_size_bytes: 0,
                ..good.clone()
            },
            MultipartUploadConfig {
                max_parallelism: 0,
                ..good.clone()
            },
            MultipartUploadConfig {
                part_attempts: 0,
                ..good.clone()
            },
        ];
        for bad in broken {
            let err = upload_resumable(
                &backend,
                &bad,
                file.path(),
                "s3://b/tuning.raw",
                Some(ObjectChecksum::new("blake3", &digest)),
                fence(&digest, data.len() as u64, 1),
            )
            .await
            .expect_err("a zero tuning value must be refused");
            assert!(matches!(err, VtopError::Config(_)), "{err:?}");
        }
        assert_eq!(backend.part_sends_total(), 0);
    }

    /// The widest pool the limit allows is accepted; one past it is not.
    #[tokio::test]
    async fn a_parallelism_at_the_permit_limit_is_accepted() {
        let (file, data, digest) = three_part_file(8);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new();
        let at_limit = MultipartUploadConfig {
            max_parallelism: MAX_SEMAPHORE_PERMITS,
            ..cfg(state.path(), 1_000)
        };
        upload_resumable(
            &backend,
            &at_limit,
            file.path(),
            "s3://b/wide.raw",
            Some(ObjectChecksum::new("blake3", &digest)),
            fence(&digest, data.len() as u64, 1),
        )
        .await
        .expect("a pool as wide as the limit is a plan, not a typo");
        let past = MultipartUploadConfig {
            max_parallelism: MAX_SEMAPHORE_PERMITS + 1,
            ..at_limit
        };
        assert!(upload_resumable(
            &backend,
            &past,
            file.path(),
            "s3://b/wider.raw",
            Some(ObjectChecksum::new("blake3", &digest)),
            fence(&digest, data.len() as u64, 1),
        )
        .await
        .is_err());
    }

    /// The fence describes the bytes the checksum describes: a different
    /// digest or a different algorithm is refused, each alone; the
    /// algorithm's spelling is not compared by case.
    #[tokio::test]
    async fn a_checksum_that_disagrees_with_the_fence_is_refused() {
        let (file, data, digest) = three_part_file(9);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new();
        let cfg = cfg(state.path(), 1_000);
        let other_digest = vtop_core::checksum::blake3_bytes(b"other bytes");
        let sha = vtop_core::checksum::sha256_bytes(&data);
        for (algorithm, hex) in [
            ("blake3", other_digest.as_str()),
            ("sha256", digest.as_str()),
            ("sha256", sha.as_str()),
        ] {
            let err = upload_resumable(
                &backend,
                &cfg,
                file.path(),
                "s3://b/fence-digest.raw",
                Some(ObjectChecksum::new(algorithm, hex)),
                fence(&digest, data.len() as u64, 1),
            )
            .await
            .expect_err("the checksum names other bytes than the fence");
            assert!(
                err.to_string().contains("disagrees with upload checksum"),
                "{err}"
            );
        }
        assert_eq!(backend.part_sends_total(), 0);
        upload_resumable(
            &backend,
            &cfg,
            file.path(),
            "s3://b/fence-digest.raw",
            Some(ObjectChecksum::new("BLAKE3", &digest)),
            fence(&digest, data.len() as u64, 1),
        )
        .await
        .expect("the same digest and algorithm, spelled in capitals");
    }

    /// Completed an object, then leave behind the session a crash between the
    /// store's answer and the delete would leave: `Completed`, naming
    /// `version` and `retention`.
    async fn completed_object_with_session(
        backend: &MockBackend,
        state: &Path,
        file: &Path,
        (digest, len): (&str, u64),
        uri: &str,
        version: Option<&str>,
        retention: Option<crate::base::RequestedRetention>,
    ) {
        let (first, _) = run(backend, &cfg(state, 1_000), file, uri, digest, len).await;
        first.unwrap();
        let f = fence(digest, len, 1);
        let mut session = earlier_session(uri, file, &f, 1_000, retention);
        session.phase = MultipartSessionPhase::Completed;
        session.completed_version_id = version.map(str::to_owned);
        save_session(&session_path(state, uri, &f), &session).unwrap();
    }

    /// A call that finds its upload already completed re-verifies the object
    /// and cites the version the session recorded, with the lock it carries:
    /// a version the session pinned, not one the current key has moved to.
    #[tokio::test]
    async fn a_completed_session_returns_its_pinned_version_and_its_lock() {
        let (file, data, digest) = three_part_file(10);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new();
        let uri = "s3://b/pinned.raw";
        let lock = requested(crate::base::ObjectLockMode::Governance, true);
        completed_object_with_session(
            &backend,
            state.path(),
            file.path(),
            (&digest, data.len() as u64),
            uri,
            Some("v-pinned"),
            Some(lock),
        )
        .await;
        let (result, report) = run(
            &backend,
            &cfg(state.path(), 1_000),
            file.path(),
            uri,
            &digest,
            data.len() as u64,
        )
        .await;
        let stored = result.unwrap();
        assert_eq!(stored.version_id.as_deref(), Some("v-pinned"));
        assert_eq!(stored.retention, Some(lock));
        assert!(report.completed && !report.resumed, "{report:?}");
        assert!(
            std::fs::read_dir(state.path()).unwrap().next().is_none(),
            "the finished session is removed"
        );
    }

    /// An empty recorded version names nothing: it is never cited.
    #[tokio::test]
    async fn an_empty_recorded_version_is_not_cited() {
        let (file, data, digest) = three_part_file(11);
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new();
        let uri = "s3://b/unpinned.raw";
        let lock = requested(crate::base::ObjectLockMode::Compliance, false);
        for (n, version) in [Some(""), None].into_iter().enumerate() {
            let uri = format!("{uri}-{n}");
            completed_object_with_session(
                &backend,
                state.path(),
                file.path(),
                (&digest, data.len() as u64),
                &uri,
                version,
                Some(lock),
            )
            .await;
            let (result, _) = run(
                &backend,
                &cfg(state.path(), 1_000),
                file.path(),
                &uri,
                &digest,
                data.len() as u64,
            )
            .await;
            let stored = result.unwrap();
            assert_eq!(stored.version_id, None, "{version:?}");
            assert_eq!(stored.retention, Some(lock), "{version:?}");
        }
    }

    /// The re-check of an already completed upload is the strong one: a
    /// store that cannot read the body back, or that reads another one,
    /// does not complete it.
    #[tokio::test]
    async fn a_completed_session_is_accepted_only_on_a_strong_verification() {
        let (file, data, digest) = three_part_file(12);
        let len = data.len() as u64;
        for (backend, says) in [
            (MockBackend::limited(), "could not strongly verify"),
            (MockBackend::failing(), "could not strongly verify"),
        ] {
            let state = tempfile::tempdir().unwrap();
            let uri = "s3://b/recheck.raw";
            completed_object_with_session(
                &backend,
                state.path(),
                file.path(),
                (&digest, len),
                uri,
                Some("v1"),
                None,
            )
            .await;
            let (result, report) = run(
                &backend,
                &cfg(state.path(), 1_000),
                file.path(),
                uri,
                &digest,
                len,
            )
            .await;
            let err = result.expect_err("a weak or failed re-check must not complete the object");
            assert!(err.to_string().contains(says), "{err}");
            assert!(!report.completed, "{report:?}");
        }
    }

    /// A throttle the store answers to a request that is not a part is still
    /// a throttle in the report, so the engine's width controller sees it.
    #[tokio::test]
    async fn a_throttle_on_a_request_that_is_not_a_part_is_counted() {
        let (file, data, digest) = three_part_file(13);
        let len = data.len() as u64;
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new().with_head_fault(crate::HeadFault::Throttled);
        let uri = "s3://b/head-throttle.raw";
        completed_object_with_session(
            &backend,
            state.path(),
            file.path(),
            (&digest, len),
            uri,
            Some("v1"),
            None,
        )
        .await;
        let (result, report) = run(
            &backend,
            &cfg(state.path(), 1_000),
            file.path(),
            uri,
            &digest,
            len,
        )
        .await;
        assert!(
            matches!(result, Err(VtopError::UploadThrottled(_))),
            "{result:?}"
        );
        assert_eq!(
            (report.requests, report.throttled_requests),
            (2, 1),
            "the read-back and the head, the head throttled"
        );
    }

    /// A kept part whose bytes no longer match the local file is not trusted:
    /// it is sent again, and only it.
    #[tokio::test]
    async fn a_kept_part_that_no_longer_matches_the_local_bytes_is_sent_again() {
        let (file, data, digest) = three_part_file(14);
        let len = data.len() as u64;
        let state = tempfile::tempdir().unwrap();
        let fail_after = Arc::new(AtomicUsize::new(2));
        let backend = MockBackend::new().with_multipart_fail_after_parts(fail_after.clone());
        let mut cfg = cfg(state.path(), 1_000);
        cfg.max_parallelism = 1;
        let uri = "s3://b/revalidate.raw";
        let (result, _) = run(&backend, &cfg, file.path(), uri, &digest, len).await;
        result.expect_err("interrupted after two parts");
        let path = session_path(state.path(), uri, &fence(&digest, len, 1));
        let mut session = load_session(&path).unwrap();
        assert_eq!(session.parts.len(), 2);
        session.parts.get_mut(&1).unwrap().part_digest_hex = "0".repeat(64);
        save_session(&path, &session).unwrap();

        fail_after.store(usize::MAX, Ordering::SeqCst);
        let (result, report) = run(&backend, &cfg, file.path(), uri, &digest, len).await;
        result.unwrap();
        assert_eq!(report.parts_reused, 1, "only part 2 is still trusted");
        assert_eq!(backend.part_sends(1), 2, "part 1 is sent again");
        assert_eq!(backend.part_sends(2), 1, "part 2 is not");
    }

    /// A retry notice is let through again once the whole window has passed,
    /// not a moment after it.
    #[tokio::test(start_paused = true)]
    async fn a_retry_notice_is_admitted_when_the_window_has_just_passed() {
        let notices = RetryNotices::new(Duration::from_secs(60));
        assert_eq!(notices.admit(), Some(0));
        tokio::time::advance(Duration::from_secs(59)).await;
        assert_eq!(notices.admit(), None);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            notices.admit(),
            Some(1),
            "60 s after the notice is the next one"
        );
    }

    #[test]
    fn a_url_without_a_query_is_left_as_it_is() {
        let text = "PUT https://bucket.s3.example.com/key/part failed; see s3://b/k";
        assert_eq!(redact_urls(text), text);
        assert_eq!(
            redact_urls("https://u:p@host.example/a#frag and http://h.example"),
            "https://host.example/a?[redacted] and http://h.example"
        );
    }

    /// The sizes the resume notice speaks in.
    #[test]
    fn sizes_are_spoken_in_the_largest_unit_they_fill() {
        const KIB: u64 = 1024;
        for (bytes, said) in [
            (0, "0 bytes"),
            (1_023, "1023 bytes"),
            (KIB, "1.0 KiB"),
            (1_536, "1.5 KiB"),
            (KIB * KIB - 1, "1024.0 KiB"),
            (KIB * KIB, "1.0 MiB"),
            (KIB * KIB * 5 / 2, "2.5 MiB"),
            (KIB * KIB * KIB - 1, "1024.0 MiB"),
            (KIB * KIB * KIB, "1.0 GiB"),
            (KIB * KIB * KIB * 7 / 2, "3.5 GiB"),
        ] {
            assert_eq!(human_bytes(bytes), said, "{bytes} bytes");
        }
    }

    /// The sweep leaves a session younger than the abandon window alone and
    /// takes one as old as it (a window of zero takes every session).
    #[tokio::test]
    async fn the_sweep_leaves_a_young_session_and_takes_one_as_old_as_the_window() {
        let (file, data, digest) = three_part_file(15);
        let len = data.len() as u64;
        let state = tempfile::tempdir().unwrap();
        let backend =
            MockBackend::new().with_multipart_fail_after_parts(Arc::new(AtomicUsize::new(1)));
        let uri = "s3://b/young.raw";
        let mut cfg = cfg(state.path(), 1_000);
        let (result, _) = run(&backend, &cfg, file.path(), uri, &digest, len).await;
        result.expect_err("interrupted");
        let path = session_path(state.path(), uri, &fence(&digest, len, 1));

        cfg.abandon_after_secs = 3_600;
        assert_eq!(cleanup_abandoned(&backend, &cfg).await.unwrap(), 0);
        assert!(path.exists(), "a young session is kept");
        assert_eq!(backend.pending_multipart_count(), 1);

        cfg.abandon_after_secs = 0;
        assert_eq!(cleanup_abandoned(&backend, &cfg).await.unwrap(), 1);
        assert!(!path.exists());
        assert_eq!(backend.pending_multipart_count(), 0);
    }

    /// The sweep removes a session the store no longer holds, and a completed
    /// one, without asking the store to abort anything.
    #[tokio::test]
    async fn the_sweep_removes_what_the_store_has_already_dropped() {
        let (file, data, digest) = three_part_file(16);
        let len = data.len() as u64;
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new().with_failing_abort();
        let mut cfg = cfg(state.path(), 1_000);
        cfg.abandon_after_secs = 0;
        let f = fence(&digest, len, 1);
        let mut completed = earlier_session("s3://b/done.raw", file.path(), &f, 1_000, None);
        completed.phase = MultipartSessionPhase::Completed;
        let path = session_path(state.path(), "s3://b/done.raw", &f);
        save_session(&path, &completed).unwrap();
        // Not a session file: never touched.
        let other = state.path().join("notes.txt");
        std::fs::write(&other, b"keep").unwrap();

        assert_eq!(cleanup_abandoned(&backend, &cfg).await.unwrap(), 1);
        assert!(!path.exists() && other.exists());

        // An in-progress one the store dropped: its abort answers "gone",
        // which is what the sweep wanted.
        let backend = MockBackend::new();
        let gone = earlier_session("s3://b/gone.raw", file.path(), &f, 1_000, None);
        let path = session_path(state.path(), "s3://b/gone.raw", &f);
        save_session(&path, &gone).unwrap();
        assert_eq!(cleanup_abandoned(&backend, &cfg).await.unwrap(), 1);
        assert!(!path.exists());
    }

    /// Leaves the session an interrupted upload (one part sent, then a fault)
    /// leaves behind, and returns the backend that holds its upload.
    async fn leave_interrupted_session(
        failing_abort: bool,
        state: &Path,
        file: &Path,
        (digest, len): (&str, u64),
        uri: &str,
    ) -> MockBackend {
        let backend =
            MockBackend::new().with_multipart_fail_after_parts(Arc::new(AtomicUsize::new(1)));
        let backend = if failing_abort {
            backend.with_failing_abort()
        } else {
            backend
        };
        let (result, _) = run(&backend, &cfg(state, 1_000), file, uri, digest, len).await;
        result.expect_err("interrupted");
        backend
    }

    /// An operator's abort ends the store's upload and forgets the session;
    /// an upload the store already dropped needs nothing; a failed abort
    /// keeps the session so it can be tried again; a finished session has
    /// nothing to abort.
    #[tokio::test]
    async fn aborting_a_session_ends_the_upload_and_keeps_the_record_until_it_has() {
        let (file, data, digest) = three_part_file(17);
        let len = data.len() as u64;
        let f = fence(&digest, len, 1);
        let uri = "s3://b/abort.raw";
        let (file, digest) = (file.path(), digest.as_str());

        // Aborted at the store and forgotten.
        let state = tempfile::tempdir().unwrap();
        let backend =
            leave_interrupted_session(false, state.path(), file, (digest, len), uri).await;
        assert_eq!(backend.pending_multipart_count(), 1);
        abort_session(&backend, state.path(), uri, &f)
            .await
            .unwrap();
        assert_eq!(
            backend.pending_multipart_count(),
            0,
            "the upload was aborted"
        );
        assert!(!session_path(state.path(), uri, &f).exists());

        // Already dropped by the store: nothing left to abort.
        let state = tempfile::tempdir().unwrap();
        let backend =
            leave_interrupted_session(false, state.path(), file, (digest, len), uri).await;
        backend.expire_pending_multiparts();
        abort_session(&backend, state.path(), uri, &f)
            .await
            .unwrap();
        assert!(!session_path(state.path(), uri, &f).exists());

        // The store refuses: the error is returned and the record stays.
        let state = tempfile::tempdir().unwrap();
        let backend = leave_interrupted_session(true, state.path(), file, (digest, len), uri).await;
        abort_session(&backend, state.path(), uri, &f)
            .await
            .expect_err("a failed abort is an error");
        assert!(session_path(state.path(), uri, &f).exists());
        assert_eq!(backend.pending_multipart_count(), 1);

        // A finished session: removed, with no abort sent.
        let state = tempfile::tempdir().unwrap();
        let backend = MockBackend::new().with_failing_abort();
        let mut done = earlier_session(uri, file, &f, 1_000, None);
        done.phase = MultipartSessionPhase::Completed;
        save_session(&session_path(state.path(), uri, &f), &done).unwrap();
        abort_session(&backend, state.path(), uri, &f)
            .await
            .unwrap();
        assert!(!session_path(state.path(), uri, &f).exists());

        // No session: nothing to do.
        abort_session(&backend, state.path(), uri, &f)
            .await
            .unwrap();
    }

    /// A session is refused for the lock only where its mode or legal hold
    /// differs from what the backend asks for, or it has none, or its lock
    /// has ended; a backend that asks for no lock accepts any session.
    #[test]
    fn a_session_whose_lock_cannot_serve_this_backend_says_why() {
        use crate::base::ObjectLockMode::{Compliance, Governance};
        let dir = tempfile::tempdir().unwrap();
        let f = fence("ab", 1, 1);
        let session = |retention| earlier_session("s3://b/k", dir.path(), &f, 1_000, retention);
        let backend = MockBackend::new().with_object_lock(lock_policy(Governance, false));

        assert_eq!(
            lock_mismatch(&session(Some(requested(Governance, false))), &backend),
            None
        );
        assert_eq!(
            lock_mismatch(&session(None), &backend).as_deref(),
            Some("without an Object Lock")
        );
        assert_eq!(
            lock_mismatch(&session(Some(requested(Compliance, false))), &backend).as_deref(),
            Some("under mode compliance with legal_hold=false, not mode governance with legal_hold=false")
        );
        assert_eq!(
            lock_mismatch(&session(Some(requested(Governance, true))), &backend).as_deref(),
            Some("under mode governance with legal_hold=true, not mode governance with legal_hold=false")
        );
        let ended = crate::base::RequestedRetention {
            retain_until_unix_secs: 1_000,
            ..requested(Governance, false)
        };
        assert!(lock_mismatch(&session(Some(ended)), &backend)
            .unwrap()
            .starts_with("under a retention that ended at"));
        // No lock asked for: whatever the session carries is left as it is.
        let unlocked = MockBackend::new();
        assert_eq!(lock_mismatch(&session(None), &unlocked), None);
        assert_eq!(
            lock_mismatch(&session(Some(requested(Compliance, true))), &unlocked),
            None
        );
    }

    /// An upload the store cannot continue is abandoned; when even the abort
    /// fails the operator is told its parts stay at the store, and when the
    /// store says it is gone already there is nothing to tell.
    #[tokio::test(start_paused = true)]
    async fn a_failed_abort_of_a_lost_upload_is_logged_and_a_gone_one_is_not() {
        let (file, data, digest) = three_part_file(18);
        let len = data.len() as u64;
        for (failing_abort, logged) in [(false, 0), (true, 1)] {
            let state = tempfile::tempdir().unwrap();
            let backend = MockBackend::new().with_part_fault(1, crate::PartFault::SessionGone, 1);
            let backend = if failing_abort {
                backend.with_failing_abort()
            } else {
                backend
            };
            let log = EventLog::default();
            let _guard = tracing::subscriber::set_default(log.clone());
            let (result, report) = run(
                &backend,
                &cfg(state.path(), 1_000),
                file.path(),
                "s3://b/lost.raw",
                &digest,
                len,
            )
            .await;
            result.unwrap();
            assert!(report.restarted && report.completed, "{report:?}");
            assert_eq!(log.count("multipart_session_restarted"), 1);
            assert_eq!(
                log.count("multipart_abort_failed"),
                logged,
                "failing: {failing_abort}"
            );
        }
    }

    /// A session created without the lock this backend asks for is aborted
    /// and replaced; as above, only a failed abort is the operator's news.
    #[tokio::test(start_paused = true)]
    async fn a_failed_abort_of_a_session_without_the_lock_is_logged_and_a_gone_one_is_not() {
        use crate::base::ObjectLockMode::Governance;
        let (file, data, digest) = three_part_file(19);
        let len = data.len() as u64;
        for (failing_abort, logged) in [(false, 0), (true, 1)] {
            let state = tempfile::tempdir().unwrap();
            let backend = MockBackend::new().with_object_lock(lock_policy(Governance, false));
            let backend = if failing_abort {
                backend.with_failing_abort()
            } else {
                backend
            };
            let uri = "s3://b/unlocked-session.raw";
            let f = fence(&digest, len, 1);
            save_session(
                &session_path(state.path(), uri, &f),
                &earlier_session(uri, file.path(), &f, 1_000, None),
            )
            .unwrap();
            let log = EventLog::default();
            let _guard = tracing::subscriber::set_default(log.clone());
            let (result, report) = run(
                &backend,
                &cfg(state.path(), 1_000),
                file.path(),
                uri,
                &digest,
                len,
            )
            .await;
            let stored = result.unwrap();
            assert!(
                stored.retention.is_some(),
                "the new upload carries the lock"
            );
            assert!(report.completed && !report.resumed, "{report:?}");
            assert_eq!(log.count("multipart_session_lock_mismatch"), 1);
            assert_eq!(
                log.count("multipart_abort_failed"),
                logged,
                "failing: {failing_abort}"
            );
        }
    }
}

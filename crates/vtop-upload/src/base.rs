//! Upload backend trait and shared types.
//!
//! The native S3 backend is the primary production backend; command-based
//! backends (s3cmd/awscli/minio) are compatibility mode. Every backend MUST
//! support `verify_object`, and the engine MUST NOT commit source progress
//! until verification passes.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use vtop_core::checksum::digest_reader;
use vtop_core::errors::VtopError;
use vtop_core::types::ChecksumAlgorithm;

pub use vtop_core::config::{ObjectLockConfig, ObjectLockMode};

/// An engine-computed object checksum: the algorithm name (`sha256`, `blake3`)
/// plus the lowercase-hex digest. Carrying the algorithm lets a backend choose
/// the correct content-derived verification — e.g. native S3 uses a
/// server-computed `x-amz-checksum-sha256` for SHA-256 and streams the stored
/// body for BLAKE3.
#[derive(Debug, Clone, Copy)]
pub struct ObjectChecksum<'a> {
    pub algorithm: &'a str,
    pub hex: &'a str,
}

impl<'a> ObjectChecksum<'a> {
    pub fn new(algorithm: &'a str, hex: &'a str) -> Self {
        Self { algorithm, hex }
    }
    pub fn is_sha256(&self) -> bool {
        self.algorithm.eq_ignore_ascii_case("sha256")
    }
}

/// Result of a manifest upload.
#[derive(Debug, Clone, Default)]
pub struct StoredManifest {
    /// Immutable object version assigned by the store (S3 `x-amz-version-id`).
    /// `None` when the backend or bucket does not expose versions.
    pub version_id: Option<String>,
    /// The Object Lock retention the backend ASKED the store for (#684), or
    /// `None` when it asked for none. What the store kept is a separate
    /// question, answered by [`UploadBackend::verify_retention`].
    pub retention: Option<RequestedRetention>,
}

/// Result of a telemetry/segment object upload.
#[derive(Debug, Clone, Default)]
pub struct StoredObject {
    /// Immutable object version assigned by the store (S3 `x-amz-version-id`).
    /// `None` when the backend or bucket does not expose versions.
    pub version_id: Option<String>,
    /// The Object Lock retention the backend asked the store for (#684); for
    /// a multipart object, the one fixed at CreateMultipartUpload.
    pub retention: Option<RequestedRetention>,
}

/// A started multipart upload (#684): the store's upload id, and the Object
/// Lock retention the request asked for. S3 fixes an object's lock when the
/// multipart upload is CREATED, so this is the date the completed object
/// carries, however long after it is completed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedUpload {
    pub upload_id: String,
    pub retention: Option<RequestedRetention>,
}

/// The S3 Object Lock retention a backend applies to every object and
/// manifest it writes (#684), built from `upload.object_lock`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectLockPolicy {
    pub mode: ObjectLockMode,
    retain_for: Duration,
    pub legal_hold: bool,
}

impl ObjectLockPolicy {
    pub fn from_config(cfg: &ObjectLockConfig) -> Self {
        Self {
            mode: cfg.mode,
            retain_for: Duration::from_secs(u64::from(cfg.retain_days) * 86_400),
            legal_hold: cfg.legal_hold,
        }
    }

    /// A retention of `retain_for`, which configuration cannot express (it
    /// counts whole days). For a live test that has to clean up after itself:
    /// a lock of a few seconds expires before the test ends. Never used by
    /// the engine.
    #[doc(hidden)]
    pub fn with_retention_for_tests(
        mode: ObjectLockMode,
        retain_for: Duration,
        legal_hold: bool,
    ) -> Self {
        Self {
            mode,
            retain_for,
            legal_hold,
        }
    }

    pub fn retain_for(&self) -> Duration {
        self.retain_for
    }

    /// Whether this policy is the one `cfg` describes.
    pub fn matches(&self, cfg: &ObjectLockConfig) -> bool {
        *self == Self::from_config(cfg)
    }

    /// The retention to ask for on a write made at `now`, in whole seconds.
    /// Both terms are rounded UP, so the date asked for is never earlier than
    /// `now + retain_for`. A clock before 1970 counts as 1970: the date is
    /// then in the past and the store refuses the write, which fails the
    /// batch rather than archiving it unlocked.
    pub fn request_at(&self, now: SystemTime) -> RequestedRetention {
        let since = now.duration_since(UNIX_EPOCH).unwrap_or_default();
        let round_up = |d: Duration| d.as_secs().saturating_add(u64::from(d.subsec_nanos() > 0));
        let until = round_up(since).saturating_add(round_up(self.retain_for));
        RequestedRetention {
            mode: self.mode,
            retain_until_unix_secs: i64::try_from(until).unwrap_or(i64::MAX),
            legal_hold: self.legal_hold,
        }
    }
}

/// The Object Lock retention one write asked the store for (#684). The
/// engine compares what the store kept against THIS, never against its own
/// clock at check time, so skew between the engine and the store cannot fail
/// a batch. Persisted in a multipart session (`serde`), so a resumed upload
/// still knows the date fixed at its creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestedRetention {
    pub mode: ObjectLockMode,
    pub retain_until_unix_secs: i64,
    pub legal_hold: bool,
}

impl RequestedRetention {
    /// The strict check for an object this attempt wrote: the same mode, a
    /// date no earlier than the one asked for (a longer bucket default
    /// passes), and the legal hold if one was asked for.
    pub fn expectation(&self) -> RetentionExpectation {
        RetentionExpectation {
            mode: self.mode,
            retain_until_at_least_unix_secs: self.retain_until_unix_secs,
            legal_hold: self.legal_hold,
        }
    }
}

/// What a store reports about one object version's Object Lock (#684): each
/// part `None` when the store returned no such header.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoredRetention {
    /// The mode as the store spells it (`GOVERNANCE`, `COMPLIANCE`).
    pub mode: Option<String>,
    pub retain_until_unix_secs: Option<i64>,
    pub legal_hold: Option<bool>,
}

/// What a stored object's retention must satisfy (#684).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionExpectation {
    pub mode: ObjectLockMode,
    /// The earliest acceptable retain-until date, in Unix seconds.
    pub retain_until_at_least_unix_secs: i64,
    pub legal_hold: bool,
}

/// The outcome of [`UploadBackend::verify_retention`] (#684). Its
/// [`label`](Self::label) is the `result` of `vtop_object_lock_checks_total`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionCheck {
    /// The object is under the expected mode until at least the date.
    Ok { retain_until_unix_secs: i64 },
    /// The store reports no retention on the object.
    Missing,
    /// The store reports another mode.
    ModeMismatch { found: String },
    /// The retention ends before the expected date (for a reused object: it
    /// has already ended).
    TooShort { retain_until_unix_secs: i64 },
    /// A legal hold was expected and the object has none.
    LegalHoldMissing,
    /// The backend cannot read an object's retention at all.
    Unsupported,
}

impl RetentionCheck {
    pub fn passed(&self) -> bool {
        matches!(self, Self::Ok { .. })
    }

    /// The bounded `result` label; `Unsupported` counts as `error`.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Ok { .. } => "ok",
            Self::Missing => "missing",
            Self::ModeMismatch { .. } => "mode_mismatch",
            Self::TooShort { .. } => "too_short",
            Self::LegalHoldMissing => "legal_hold_missing",
            Self::Unsupported => "error",
        }
    }

    /// What was found against what was expected, for an operator message.
    pub fn describe(&self, expected: &RetentionExpectation) -> String {
        let want = format!(
            "{} until at least {}",
            expected.mode,
            format_unix_secs(expected.retain_until_at_least_unix_secs)
        );
        match self {
            Self::Ok {
                retain_until_unix_secs,
            } => format!(
                "{} until {} (expected {want})",
                expected.mode,
                format_unix_secs(*retain_until_unix_secs)
            ),
            Self::Missing => format!("expected {want}, found no retention"),
            Self::ModeMismatch { found } => format!("expected {want}, found mode {found}"),
            Self::TooShort {
                retain_until_unix_secs,
            } => format!(
                "expected {want}, found retain-until {}",
                format_unix_secs(*retain_until_unix_secs)
            ),
            Self::LegalHoldMissing => {
                format!("expected {want} with a legal hold, found no legal hold")
            }
            Self::Unsupported => "the backend cannot read an object's retention".to_string(),
        }
    }
}

/// A Unix time as an RFC 3339 UTC date, for messages.
pub fn format_unix_secs(secs: i64) -> String {
    aws_smithy_types::DateTime::from_secs(secs)
        .fmt(aws_smithy_types::date_time::Format::DateTime)
        .unwrap_or_else(|_| format!("{secs}s after 1970"))
}

/// The retention judgment, in exactly one place (#684): the S3 backend and
/// the mock both consult it, so the mock cannot drift from the code it
/// stands in for, as with [`judge_service_sha256`].
pub fn judge_retention(
    stored: &StoredRetention,
    expected: &RetentionExpectation,
) -> RetentionCheck {
    let (Some(mode), Some(until)) = (stored.mode.as_deref(), stored.retain_until_unix_secs) else {
        return RetentionCheck::Missing;
    };
    if !mode.eq_ignore_ascii_case(expected.mode.as_s3()) {
        return RetentionCheck::ModeMismatch {
            found: mode.to_string(),
        };
    }
    if until < expected.retain_until_at_least_unix_secs {
        return RetentionCheck::TooShort {
            retain_until_unix_secs: until,
        };
    }
    if expected.legal_hold && stored.legal_hold != Some(true) {
        return RetentionCheck::LegalHoldMissing;
    }
    RetentionCheck::Ok {
        retain_until_unix_secs: until,
    }
}

/// One completed multipart part returned by the storage service.
///
/// The `etag` is the store's part receipt (required to complete the upload).
/// It is **not** an integrity checksum — multipart ETags are not MD5 and must
/// never authorize evidence commit. Per-part digests live in the persisted
/// session state; whole-object `verify_object` remains the strong gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadedPart {
    pub part_number: u32,
    pub etag: String,
}

/// Result of a HEAD/stat on a stored object.
#[derive(Debug, Clone)]
pub struct ObjectHead {
    pub uri: String,
    pub size_bytes: Option<u64>,
    pub etag: Option<String>,
    /// SHA-256 computed by the storage service over the stored body.
    ///
    /// Engine-written user metadata and local sidecars MUST NOT populate this
    /// field: they describe what the uploader claimed, not what is stored.
    pub checksum_sha256: Option<String>,
}

/// Outcome of an object verification attempt.
#[derive(Debug, Clone)]
pub struct VerificationResult {
    /// True only if the backend could positively confirm integrity.
    pub passed: bool,
    /// True if the backend cannot perform a strong (hash) check and fell back
    /// to a weaker check (e.g. size + existence only).
    pub backend_limited: bool,
    pub message: String,
}

impl VerificationResult {
    pub fn passed(message: impl Into<String>) -> Self {
        Self {
            passed: true,
            backend_limited: false,
            message: message.into(),
        }
    }
    pub fn limited(message: impl Into<String>) -> Self {
        Self {
            passed: true,
            backend_limited: true,
            message: message.into(),
        }
    }
    pub fn failed(message: impl Into<String>) -> Self {
        Self {
            passed: false,
            backend_limited: false,
            message: message.into(),
        }
    }
}

/// The SHA-256 service-checksum judgment, in exactly one place (#482): the
/// S3-native backend consults it for the real head, and the S3-semantics
/// mock consults it for the simulated one, so the mock that reproduces the
/// composite-checksum gap can never drift from the code whose gap it
/// reproduces. A `None` is DELIBERATELY limited, not failed: S3 returns no
/// whole-object SHA-256 for a multipart upload (only a composite with a
/// part-count suffix), and absence of service evidence is weaker than
/// contradiction.
pub(crate) fn judge_service_sha256(
    service: Option<&str>,
    expected_hex: &str,
) -> VerificationResult {
    match service {
        Some(stored) if stored.eq_ignore_ascii_case(expected_hex) => {
            VerificationResult::passed("S3 service-computed SHA-256 verified")
        }
        Some(_) => VerificationResult::failed("S3 service-computed SHA-256 mismatch"),
        None => VerificationResult::limited(
            "object size matches; S3 returned no service-computed SHA-256",
        ),
    }
}

/// Verify a downloaded/local file by hashing the bytes read from its open file
/// handle. This is intentionally independent of sidecars and user metadata.
pub(crate) async fn verify_file_content(
    path: &Path,
    expected_size: u64,
    expected: Option<ObjectChecksum<'_>>,
    backend: &str,
) -> Result<VerificationResult, VtopError> {
    let Some(expected) = expected else {
        let size = tokio::fs::metadata(path).await?.len();
        return if size == expected_size {
            Ok(VerificationResult::limited(format!(
                "{backend}: stored content size matches (checksums disabled)"
            )))
        } else {
            Ok(VerificationResult::failed(format!(
                "size mismatch: expected {expected_size}, read {size} stored bytes"
            )))
        };
    };

    let file = tokio::fs::File::open(path).await?;
    Ok(
        verify_reader_content(file, expected_size, expected, backend)
            .await?
            .0,
    )
}

/// Hash at most `expected_size + 1` bytes from a stored-content stream. The
/// extra byte distinguishes an exact-size object from an oversized replacement
/// without buffering or persisting the remainder.
pub(crate) async fn verify_reader_content<R>(
    reader: R,
    expected_size: u64,
    expected: ObjectChecksum<'_>,
    backend: &str,
) -> Result<(VerificationResult, bool), VtopError>
where
    R: AsyncRead + Unpin,
{
    let algo = match expected.algorithm.parse::<ChecksumAlgorithm>() {
        Ok(ChecksumAlgorithm::None) => {
            return Ok((
                VerificationResult::failed("checksum value supplied with disabled algorithm"),
                false,
            ))
        }
        Ok(algo) => algo,
        Err(e) => return Ok((VerificationResult::failed(e), false)),
    };
    let read_limit = expected_size.saturating_add(1);
    let Some((actual, bytes_read)) = digest_reader(algo, reader.take(read_limit)).await? else {
        return Ok((
            VerificationResult::failed("content digest was not computed"),
            false,
        ));
    };
    if bytes_read != expected_size {
        return Ok((
            VerificationResult::failed(format!(
                "size mismatch: expected {expected_size}, read {bytes_read} stored bytes"
            )),
            bytes_read > expected_size,
        ));
    }
    if actual.eq_ignore_ascii_case(expected.hex) {
        Ok((
            VerificationResult::passed(format!(
                "{backend}: stored content {algorithm} verified",
                algorithm = algo.as_str()
            )),
            false,
        ))
    } else {
        Ok((
            VerificationResult::failed(format!(
                "stored content checksum mismatch for {}",
                algo.as_str()
            )),
            false,
        ))
    }
}

/// Verify content emitted on a command backend's stdout without first writing
/// an attacker-controlled object to disk.
pub(crate) async fn verify_command_content(
    cmd: &mut Command,
    expected_size: u64,
    expected: ObjectChecksum<'_>,
    backend: &str,
    timeout: Duration,
) -> Result<VerificationResult, VtopError> {
    let deadline = command_deadline(timeout);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = spawn_retrying_text_busy(cmd, backend, "verification").await?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| VtopError::Upload(format!("{backend} verification stdout unavailable")))?;
    let evidence = ReadBackEvidence::collect(&mut child, backend, "verification")?;
    let completed = tokio::time::timeout_at(deadline, async {
        // The tool's exit is watched WHILE stdout drains (review of #530): a
        // descendant it left behind can hold stdout open after the tool
        // itself has failed, and draining first would wait out the deadline
        // and lose the failure's evidence. A failed exit is classified now;
        // a successful one still waits for the content.
        let read = verify_reader_content(stdout, expected_size, expected, backend);
        tokio::pin!(read);
        let mut verified = None;
        let status = loop {
            tokio::select! {
                outcome = &mut read, if verified.is_none() => {
                    let (result, oversized) = outcome?;
                    if oversized {
                        let _ = child.kill().await;
                    }
                    verified = Some((result, oversized));
                }
                status = child.wait() => break status.map_err(|e| {
                    VtopError::Upload(format!("waiting for {backend} verification: {e}"))
                })?,
            }
        };
        let oversized = verified.as_ref().is_some_and(|(_, oversized)| *oversized);
        if !status.success() && !oversized {
            return Ok(Err(status));
        }
        let (result, _) = match verified {
            Some(verified) => verified,
            None => read.await?,
        };
        Ok::<_, VtopError>(Ok(result))
    })
    .await;
    match completed {
        Ok(Ok(Ok(result))) => Ok(result),
        Ok(Ok(Err(status))) => Err(evidence
            .classify(
                format!("{backend} verification command exited with {status}"),
                deadline,
            )
            .await),
        Ok(Err(error)) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            Err(error)
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            Err(VtopError::Upload(format!(
                "{backend} verification exceeded the {}s timeout",
                timeout.as_secs()
            )))
        }
    }
}

/// Read a small object with a hard in-memory cap.
pub(crate) async fn read_bounded<R>(
    reader: R,
    max_bytes: usize,
    object_uri: &str,
) -> Result<Vec<u8>, VtopError>
where
    R: AsyncRead + Unpin,
{
    let limit = max_bytes.saturating_add(1) as u64;
    let mut bytes = Vec::with_capacity(max_bytes.min(64 * 1024));
    reader.take(limit).read_to_end(&mut bytes).await?;
    if bytes.len() > max_bytes {
        return Err(VtopError::Upload(format!(
            "stored object {object_uri} exceeds the {max_bytes}-byte read limit"
        )));
    }
    Ok(bytes)
}

/// Read bounded command stdout, killing the producer as soon as the cap is
/// exceeded so it cannot continue filling a pipe or temporary filesystem.
pub(crate) async fn read_command_bounded(
    cmd: &mut Command,
    max_bytes: usize,
    object_uri: &str,
    backend: &str,
    timeout: Duration,
) -> Result<Vec<u8>, VtopError> {
    let deadline = command_deadline(timeout);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = spawn_retrying_text_busy(cmd, backend, "download").await?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| VtopError::Upload(format!("{backend} download stdout unavailable")))?;
    let evidence = ReadBackEvidence::collect(&mut child, backend, "download")?;
    let completed = tokio::time::timeout_at(deadline, async {
        // Exit watched while stdout drains, as in `verify_command_content`
        // (review of #530): a failed tool is classified on its own exit, not
        // after a descendant holding stdout lets go.
        let read = read_bounded(stdout, max_bytes, object_uri);
        tokio::pin!(read);
        let mut bytes = None;
        let status = loop {
            tokio::select! {
                outcome = &mut read, if bytes.is_none() => bytes = Some(outcome?),
                status = child.wait() => break status.map_err(|e| {
                    VtopError::Upload(format!("waiting for {backend} download: {e}"))
                })?,
            }
        };
        if !status.success() {
            return Ok(Err(status));
        }
        let bytes = match bytes {
            Some(bytes) => bytes,
            None => read.await?,
        };
        Ok::<_, VtopError>(Ok(bytes))
    })
    .await;
    match completed {
        Ok(Ok(Ok(bytes))) => Ok(bytes),
        Ok(Ok(Err(status))) => Err(evidence
            .classify(
                format!("{backend} download command exited with {status}"),
                deadline,
            )
            .await),
        Ok(Err(error)) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            Err(error)
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            Err(VtopError::Upload(format!(
                "{backend} download exceeded the {}s timeout",
                timeout.as_secs()
            )))
        }
    }
}

/// A read-back tool's stderr, kept as evidence of WHY it failed (#481).
///
/// REGRESSION: both read-back helpers used to send the tool's stderr to
/// `/dev/null` and report only the exit status, as a generic `Upload`. So an
/// `aws s3 cp … -`, `s3cmd get … -` or `mc cat` that the store THROTTLED
/// during verification or a manifest read-back reached the engine as an
/// ordinary failure, and `note_store_request` told the width controller the
/// store was fine while it was asking for less. The write path (`command.rs`)
/// already classified its evidence; the read path now does the same, on the
/// same last line, through the same `upload_failure`.
///
/// Drained on its own task from the moment the child starts, so a tool that
/// fills stderr can never stall the stdout read. Dropping it aborts the drain.
///
/// The kept tail lives OUTSIDE the task, shared with it, so that a wait for
/// the drain which runs out of time can still read what was captured. The
/// case that matters: the tool printed `SlowDown` and exited, but a
/// grandchild it left behind inherited stderr and holds the pipe open. The
/// drain never reaches EOF, yet the line the classifier needs is already in
/// the buffer; waiting on a `JoinHandle<String>` alone would throw it away
/// with the timeout and report the throttle as an ordinary failure.
struct ReadBackEvidence {
    drain: tokio::task::JoinHandle<()>,
    kept: Arc<Mutex<Vec<u8>>>,
}

impl Drop for ReadBackEvidence {
    fn drop(&mut self) {
        self.drain.abort();
    }
}

/// Spawn a command backend's tool, retrying a briefly busy executable.
///
/// ETXTBSY (`Text file busy`, os error 26) means the executable is still
/// open for writing somewhere — an installer replacing it in place, or a
/// sibling `fork` that inherited the write descriptor before `exec`. It
/// clears on its own, so the spawn is retried a bounded number of times with
/// a short backoff before it is reported. Every command spawn goes through
/// here (the write path's `CommandPolicy::capture`, the read-back and the
/// verify), so they cannot drift apart again: the read-back and verify used
/// to spawn once and fail outright, which this PR's concurrent test fixtures
/// exposed as an intermittent `Text file busy`.
pub(crate) async fn spawn_retrying_text_busy(
    command: &mut Command,
    backend: &str,
    operation: &str,
) -> Result<tokio::process::Child, VtopError> {
    retry_text_busy(|| command.spawn(), backend, operation).await
}

/// How many times a spawn that hit ETXTBSY is retried, `10 ms × n` apart.
const TEXT_BUSY_RETRIES: u32 = 4;
/// `ETXTBSY` on Linux and the BSDs, macOS included.
const TEXT_BUSY: i32 = 26;

/// `spawn_retrying_text_busy` over any spawn, so the retry policy and its
/// error messages can be tested without racing a real busy executable.
async fn retry_text_busy<T>(
    mut spawn: impl FnMut() -> std::io::Result<T>,
    backend: &str,
    operation: &str,
) -> Result<T, VtopError> {
    let mut busy_retries = 0_u32;
    loop {
        match spawn() {
            Ok(spawned) => return Ok(spawned),
            Err(error)
                if error.raw_os_error() == Some(TEXT_BUSY) && busy_retries < TEXT_BUSY_RETRIES =>
            {
                busy_retries += 1;
                tokio::time::sleep(Duration::from_millis(10 * u64::from(busy_retries))).await;
            }
            Err(error) => {
                let hint = if error.kind() == std::io::ErrorKind::PermissionDenied {
                    "; verify that the command is executable and its filesystem is not mounted noexec"
                } else if error.raw_os_error() == Some(TEXT_BUSY) {
                    "; the executable remained busy after bounded retries; install/replace it atomically and retry"
                } else {
                    ""
                };
                return Err(VtopError::Upload(format!(
                    "spawning {backend} {operation}: {error}{hint}"
                )));
            }
        }
    }
}

/// The instant a command invocation must be finished by, fixed BEFORE the
/// tool is spawned. `upload.command_timeout_seconds` bounds the whole call:
/// running the tool and then collecting its stderr evidence both spend from
/// this one deadline, so neither can restart the clock (#529 review). The
/// setting is an unbounded `u64`, so an absurd value saturates to a
/// practically-never deadline instead of panicking on `Instant` overflow.
pub(crate) fn command_deadline(timeout: Duration) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    now.checked_add(timeout)
        .unwrap_or_else(|| now + Duration::from_secs(30 * 365 * 24 * 60 * 60))
}

/// How long a command's output drains may run on after the tool has
/// EXITED. Everything the tool wrote is already in the pipe by then (at most
/// one pipe buffer, read in microseconds), so this only has to cover
/// scheduling the drain; past it, a descendant is what holds the pipe open.
/// Shared by the read-back (`ReadBackEvidence::classify`) and the write path
/// (`CommandPolicy::capture`, #593), and never spent past the command's own
/// deadline by either.
pub(crate) const EXITED_DRAIN_GRACE: Duration = Duration::from_secs(1);

impl ReadBackEvidence {
    /// The most stderr kept. Enough for any CLI's last error line; the rest
    /// is drained, not stored, so a chatty tool can neither block on a full
    /// pipe nor grow this process. The TAIL is what is kept: the classifier
    /// reads the last line, and a tool that prints progress before failing
    /// would otherwise have its error discarded as the part past the cap.
    const MAX_BYTES: usize = 64 * 1024;
    /// How much of the last line the error message repeats.
    const MAX_SHOWN_CHARS: usize = 240;
    fn collect(
        child: &mut tokio::process::Child,
        backend: &str,
        operation: &str,
    ) -> Result<Self, VtopError> {
        let mut stderr = child.stderr.take().ok_or_else(|| {
            VtopError::Upload(format!("{backend} {operation} stderr unavailable"))
        })?;
        let kept = Arc::new(Mutex::new(Vec::new()));
        let shared = Arc::clone(&kept);
        let drain = tokio::spawn(async move {
            let mut chunk = [0_u8; 4096];
            while let Ok(read) = stderr.read(&mut chunk).await {
                if read == 0 {
                    break;
                }
                // Held only for the copy, never across the next read.
                let mut kept = shared.lock().unwrap_or_else(|poison| poison.into_inner());
                kept.extend_from_slice(&chunk[..read]);
                if kept.len() > Self::MAX_BYTES {
                    let excess = kept.len() - Self::MAX_BYTES;
                    kept.drain(..excess);
                }
            }
        });
        Ok(Self { drain, kept })
    }

    /// The error for a tool that EXITED non-zero: its last stderr line, and
    /// the throttle classification that line supports. Called once the child
    /// has exited, so the pipe is normally at EOF and the drain finishes at
    /// once. A grandchild the tool left behind can hold the pipe open, so the
    /// wait is bounded twice: by `EXITED_DRAIN_GRACE` from now, and by what is
    /// left of the command's own `deadline` — evidence is worth waiting for,
    /// not worth hanging on, and never worth stretching the invocation past
    /// `upload.command_timeout_seconds`. The grace is what keeps a tool that
    /// failed AT ONCE from waiting out the whole deadline (review of #530:
    /// with the 300 s default, an immediately-failed read-back took about
    /// five minutes). When the wait ends early, the tail captured so far is
    /// classified: the tool's error line was written before it exited, so it
    /// is already kept.
    async fn classify(mut self, detail: String, deadline: tokio::time::Instant) -> VtopError {
        let until = tokio::time::Instant::now()
            .checked_add(EXITED_DRAIN_GRACE)
            .map_or(deadline, |grace| grace.min(deadline));
        let _ = tokio::time::timeout_at(until, &mut self.drain).await;
        let evidence = {
            let kept = self
                .kept
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            String::from_utf8_lossy(&kept).into_owned()
        };
        let shown: String = evidence
            .lines()
            .rev()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .unwrap_or_default()
            .chars()
            .take(Self::MAX_SHOWN_CHARS)
            .collect();
        let detail = if shown.is_empty() {
            detail
        } else {
            format!("{detail}: {shown}")
        };
        upload_failure(detail, &evidence)
    }
}

/// S3's throttling error codes, as the AWS SDK's own retry classifier lists
/// them (#102). A code on this list means the store wants fewer requests,
/// whichever tool relayed it.
pub const THROTTLE_ERROR_CODES: &[&str] = &[
    "Throttling",
    "ThrottlingException",
    "ThrottledException",
    "RequestThrottledException",
    "TooManyRequestsException",
    "TooManyRequests",
    "ProvisionedThroughputExceededException",
    "RequestLimitExceeded",
    "BandwidthLimitExceeded",
    "LimitExceededException",
    "RequestThrottled",
    "SlowDown",
    "PriorRequestNotComplete",
];

/// Whether an S3 error code is a throttle (#102).
pub fn is_throttle_code(code: &str) -> bool {
    THROTTLE_ERROR_CODES.contains(&code)
}

/// Whether an HTTP status carries a throttle on its own: 429 by definition,
/// and 503, which is how S3 says `SlowDown` (#102).
pub fn is_throttle_status(status: u16) -> bool {
    matches!(status, 429 | 503)
}

/// Whether a tool's output relays a throttle (#102), read from the places
/// the tools put an error CODE or STATUS — never from the output at large,
/// where an object key named `SlowDown/` would be mistaken for one (review):
///
/// - awscli: `An error occurred (SlowDown) when calling the PutObject …`
/// - s3cmd:  `ERROR: S3 error: 503 (SlowDown): Please reduce your request rate.`
/// - mc:     `mc: <ERROR> … Please reduce your request rate.` — mc prints no
///   code, only S3's message, so that sentence (S3's own wording for every
///   throttle) is the one message-text match; and a bare `429 Too Many
///   Requests` / `503 Service Unavailable` status line as any of them may
///   relay it from a proxy.
pub fn looks_throttled(text: &str) -> bool {
    // The LAST non-empty line only (review): it is the tool's verdict, and
    // an earlier line — a retry it reported and recovered from, say — is
    // not. This is also the line the error message carries.
    text.lines()
        .rev()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .is_some_and(line_relays_throttle)
}

fn line_relays_throttle(line: &str) -> bool {
    // awscli: the code in parentheses right after "error occurred".
    if let Some(rest) = line.split("error occurred (").nth(1) {
        if let Some(code) = rest.split(')').next() {
            if is_throttle_code(code) {
                return true;
            }
        }
    }
    // s3cmd: "S3 error: <status> (<code>)".
    if let Some(rest) = line.split("S3 error: ").nth(1) {
        let mut parts = rest.splitn(2, ' ');
        let status = parts.next().unwrap_or_default().trim_end_matches(':');
        if status.parse::<u16>().is_ok_and(is_throttle_status) {
            return true;
        }
        if let Some(code) = parts.next().and_then(|tail| {
            tail.strip_prefix('(')
                .and_then(|inner| inner.split(')').next())
        }) {
            if is_throttle_code(code) {
                return true;
            }
        }
    }
    // A status line with its reason phrase, as a proxy relays it: the line
    // IS the status, optionally behind an `HTTP/1.1 ` or `HTTP ` prefix —
    // not a phrase found somewhere inside a line (review), where an object
    // key could put it.
    let status_line = line
        .trim()
        .trim_start_matches("HTTP/1.1 ")
        .trim_start_matches("HTTP/1.0 ")
        .trim_start_matches("HTTP/2 ")
        .trim_start_matches("HTTP ");
    if status_line.starts_with("429 Too Many Requests")
        || status_line.starts_with("503 Service Unavailable")
    {
        return true;
    }
    // S3's message for every throttle, which mc relays without the code.
    line.contains("Please reduce your request rate")
}

/// The engine's error for a failed backend call, told apart by what the
/// evidence says (#102).
pub(crate) fn upload_failure(detail: String, evidence: &str) -> VtopError {
    if looks_throttled(evidence) {
        VtopError::UploadThrottled(detail)
    } else {
        VtopError::Upload(detail)
    }
}

/// Pluggable object-storage backend.
#[async_trait]
pub trait UploadBackend: Send + Sync {
    /// Upload the compressed telemetry object. `checksum` is the engine-computed
    /// object digest (algorithm + hex), or `None` when checksums are disabled.
    /// Backends that can store it (native S3, awscli, localfs) record it for
    /// verification; native S3 additionally requests server-side validation when
    /// the algorithm is SHA-256.
    async fn put_object(
        &self,
        local_path: &Path,
        object_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredObject, VtopError>;

    /// Upload the manifest JSON (with its digest as the checksum).
    ///
    /// Returns the immutable object version the store assigned, when the
    /// backend exposes one. The engine persists it so recovery can read the
    /// exact stored version instead of an overwritable current key (#135).
    async fn put_manifest(
        &self,
        local_path: &Path,
        manifest_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredManifest, VtopError>;

    /// Download a specific immutable version of a manifest, bounded like
    /// `get_object_bounded`. The default fails closed: a backend that cannot
    /// address stored versions must not silently substitute the current key,
    /// because that is exactly the rollback surface version pinning removes.
    async fn get_manifest_pinned(
        &self,
        manifest_uri: &str,
        _version_id: &str,
        _max_bytes: usize,
    ) -> Result<Vec<u8>, VtopError> {
        Err(VtopError::Upload(format!(
            "backend {} cannot read a pinned object version of {manifest_uri}",
            self.backend_name()
        )))
    }

    /// Whether this backend can return and re-address immutable object
    /// versions. `false` means `put_manifest` never yields a version and
    /// `get_manifest_pinned`/`verify_bucket_versioning` fail closed.
    fn supports_object_versions(&self) -> bool {
        false
    }

    /// Preflight for the hardened profile: confirm the bucket keeps immutable
    /// versions of overwritten objects. Default fails closed for backends
    /// without versioning.
    async fn verify_bucket_versioning(&self, bucket: &str) -> Result<(), VtopError> {
        Err(VtopError::Upload(format!(
            "backend {} cannot confirm object versioning on bucket {bucket}",
            self.backend_name()
        )))
    }

    /// The Object Lock retention this backend applies to every object and
    /// manifest it writes (#684). `None` (the default) means it sends none.
    fn object_lock(&self) -> Option<ObjectLockPolicy> {
        None
    }

    /// Preflight for the Object Lock profile (#684): whether the bucket has
    /// Object Lock enabled (S3 `GetObjectLockConfiguration`). `Ok(false)` is
    /// the store's answer that it has not; `Err` is a check that could not be
    /// made. The default fails closed, like
    /// [`verify_bucket_versioning`](Self::verify_bucket_versioning).
    async fn bucket_object_lock_enabled(&self, bucket: &str) -> Result<bool, VtopError> {
        Err(VtopError::Upload(format!(
            "backend {} cannot read the Object Lock configuration of bucket {bucket}",
            self.backend_name()
        )))
    }

    /// Read one object version's Object Lock retention and judge it against
    /// `expected` (#684). `version_id` names the exact version a write
    /// returned; `None` reads the current version of the key.
    ///
    /// Separate from [`verify_object`](Self::verify_object) on purpose:
    /// recovery and the scrub verify content long after the write, when a
    /// retention that has ended is normal, and must not fail for it. The
    /// default is [`RetentionCheck::Unsupported`], which the engine treats as
    /// a failure when the profile is set.
    async fn verify_retention(
        &self,
        _object_uri: &str,
        _version_id: Option<&str>,
        _expected: &RetentionExpectation,
    ) -> Result<RetentionCheck, VtopError> {
        Ok(RetentionCheck::Unsupported)
    }

    /// HEAD/stat an object.
    async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError>;

    /// Whether [`head_object`](Self::head_object) answers an absent object
    /// with [`VtopError::NotFound`], and answers it with that ONLY when the
    /// store said the key holds nothing (#626).
    ///
    /// The engine writes a batch under its derived object key only when the
    /// key is free or already holds this batch's verified bytes; anything it
    /// cannot tell apart from "free" could be an object it would overwrite. A
    /// backend that relays a tool's words (the command backends) or cannot
    /// separate a missing key from a refused request answers `false`, and the
    /// engine names that backend's objects per attempt, as before.
    fn reports_absent_objects(&self) -> bool {
        false
    }

    /// Download an object's full content.
    ///
    /// Used by explicit deep-verification tooling. Recovery and archiving use
    /// `get_object_bounded` for manifests; telemetry-object verification uses
    /// backend streaming APIs. All verification must inspect ACTUAL stored
    /// bytes — metadata can describe a replaced object without detecting it.
    async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError>;

    /// Download a small object's content with a hard byte limit. Recovery and
    /// manifest verification use this instead of the unbounded tooling path so
    /// a replaced manifest cannot exhaust memory or temporary disk.
    async fn get_object_bounded(
        &self,
        object_uri: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, VtopError>;

    /// Verify a stored object against an expected size and (when provided)
    /// checksum. A non-limited success MUST be derived from the stored body or
    /// from a checksum the storage service computed over that body. Uploader
    /// metadata and sidecars are never strong evidence. `expected = None`
    /// means checksums are disabled, so only size/existence can be confirmed
    /// (a backend-limited result).
    async fn verify_object(
        &self,
        object_uri: &str,
        expected_size: u64,
        expected: Option<ObjectChecksum<'_>>,
    ) -> Result<VerificationResult, VtopError>;

    /// Delete an object (used only for cleanup / explicit operations).
    async fn delete_object(&self, object_uri: &str) -> Result<(), VtopError>;

    /// Download a specific immutable version of an object, bounded like
    /// `get_object_bounded`. Default fails closed: backends that cannot
    /// address versions must not silently substitute the current key
    /// (rehydration must pin the tier-evidence version).
    async fn get_object_pinned(
        &self,
        object_uri: &str,
        _version_id: &str,
        _max_bytes: usize,
    ) -> Result<Vec<u8>, VtopError> {
        Err(VtopError::Upload(format!(
            "backend {} cannot read a pinned object version of {object_uri}",
            self.backend_name()
        )))
    }

    /// Ensure a bucket exists (idempotent). Default is a no-op; backends that
    /// support it (native S3) override this. Only invoked when the engine is
    /// configured with `upload.create_bucket = true` — used to provision
    /// per-format buckets on demand.
    async fn ensure_bucket(&self, _bucket: &str) -> Result<(), VtopError> {
        Ok(())
    }

    fn backend_name(&self) -> &'static str;

    fn supports_checksum_verification(&self) -> bool;

    /// Whether this backend implements the resumable multipart API below
    /// (`create_multipart_upload` / `upload_part` / `complete_multipart_upload`
    /// / `abort_multipart_upload`). Compatibility backends that may multipart
    /// internally inside a single `put_object` still return `false` here —
    /// resume across process restart requires an explicit upload id.
    fn supports_multipart(&self) -> bool;

    /// The smallest part size this backend accepts for a non-final part, in
    /// bytes (#482). S3 rejects a non-final part below 5 MiB, so an engine
    /// path configured with a smaller part size must NOT route multipart to
    /// it — every non-final part would fail. Zero means no minimum (the
    /// in-process mock). The default is 0; a real S3 backend overrides it.
    fn min_part_size_bytes(&self) -> u64 {
        0
    }

    /// Start a resumable multipart upload. Returns the backend upload id and
    /// the Object Lock retention it asked for (#684), fixed here for the
    /// completed object.
    async fn create_multipart_upload(
        &self,
        object_uri: &str,
        _content_type: &str,
        _checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<CreatedUpload, VtopError> {
        Err(VtopError::Upload(format!(
            "backend {} does not support resumable multipart upload for {object_uri}",
            self.backend_name()
        )))
    }

    /// Upload one part. `part_number` is 1-based. `data` is the exact part body.
    async fn upload_part(
        &self,
        object_uri: &str,
        _upload_id: &str,
        _part_number: u32,
        _data: bytes::Bytes,
    ) -> Result<UploadedPart, VtopError> {
        Err(VtopError::Upload(format!(
            "backend {} does not support resumable multipart upload for {object_uri}",
            self.backend_name()
        )))
    }

    /// Assemble uploaded parts into the final object. Must be idempotent when
    /// the same parts/upload id are completed again after a crash mid-ack.
    async fn complete_multipart_upload(
        &self,
        object_uri: &str,
        _upload_id: &str,
        _parts: &[UploadedPart],
    ) -> Result<StoredObject, VtopError> {
        Err(VtopError::Upload(format!(
            "backend {} does not support resumable multipart upload for {object_uri}",
            self.backend_name()
        )))
    }

    /// Abort an in-progress multipart upload and release remote part storage.
    async fn abort_multipart_upload(
        &self,
        object_uri: &str,
        _upload_id: &str,
    ) -> Result<(), VtopError> {
        Err(VtopError::Upload(format!(
            "backend {} does not support resumable multipart upload for {object_uri}",
            self.backend_name()
        )))
    }
}

/// Parse an `s3://bucket/key` URI into `(bucket, key)`.
pub fn parse_s3_uri(uri: &str) -> Result<(String, String), VtopError> {
    let rest = uri
        .strip_prefix("s3://")
        .ok_or_else(|| VtopError::Upload(format!("not an s3:// uri: {uri}")))?;
    let (bucket, key) = rest
        .split_once('/')
        .ok_or_else(|| VtopError::Upload(format!("s3 uri missing key: {uri}")))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(VtopError::Upload(format!("malformed s3 uri: {uri}")));
    }
    Ok((bucket.to_string(), key.to_string()))
}

#[cfg(test)]
mod throttle_tests {
    use super::*;

    /// The vocabulary matches what the three CLIs actually print for a
    /// throttled put, and nothing about an ordinary failure (#102).
    #[test]
    fn a_throttle_is_recognised_in_every_tools_words_and_in_no_others() {
        for relayed in [
            "An error occurred (SlowDown) when calling the PutObject operation (reached max retries: 4): Please reduce your request rate.",
            "ERROR: S3 error: 503 (SlowDown): Please reduce your request rate.",
            "ERROR: S3 error: 429 (TooManyRequests)",
            "mc: <ERROR> Failed to copy `x`. Please reduce your request rate.",
            "An error occurred (Throttling) when calling the PutObject operation",
            "upload failed: s3://bucket/key\nAn error occurred (RequestLimitExceeded) when calling the PutObject operation",
            "HTTP 429 Too Many Requests",
            "HTTP/1.1 503 Service Unavailable",
            "503 Service Unavailable",
        ] {
            assert!(looks_throttled(relayed), "{relayed}");
            assert!(matches!(
                upload_failure("put".into(), relayed),
                VtopError::UploadThrottled(_)
            ));
        }
        for ordinary in [
            "An error occurred (NoSuchBucket) when calling the PutObject operation",
            "ERROR: S3 error: 403 (AccessDenied)",
            "mc: <ERROR> Unable to validate source `x`: file does not exist",
            "connection reset by peer",
            // An earlier line's throttle is not the verdict (review).
            "An error occurred (SlowDown) when calling the PutObject operation; retrying\nAn error occurred (AccessDenied) when calling the PutObject operation",
            // A key that happens to be named after a code is not a code (review).
            "An error occurred (AccessDenied) when calling the PutObject operation: s3://bucket/SlowDown/Throttling.log",
            "upload failed: s3://bucket/TooManyRequests/file to s3://bucket/503 Service/Unavailable",
            "ERROR: S3 error: 403 (AccessDenied): s3://bucket/SlowDown",
        ] {
            assert!(!looks_throttled(ordinary), "{ordinary}");
            assert!(matches!(
                upload_failure("put".into(), ordinary),
                VtopError::Upload(_)
            ));
        }
        assert!(is_throttle_status(429) && is_throttle_status(503));
        assert!(!is_throttle_status(500) && !is_throttle_status(404));
        assert!(is_throttle_code("SlowDown") && !is_throttle_code("NoSuchKey"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_s3_uri() {
        let (b, k) = parse_s3_uri("s3://telemetry-data/tenant=default/x/batch.cef.gz").unwrap();
        assert_eq!(b, "telemetry-data");
        assert_eq!(k, "tenant=default/x/batch.cef.gz");
    }

    #[test]
    fn rejects_bad_uri() {
        assert!(parse_s3_uri("http://x/y").is_err());
        assert!(parse_s3_uri("s3://bucketonly").is_err());
    }

    #[tokio::test]
    async fn bounded_read_rejects_one_byte_over_the_limit() {
        let data = b"12345";
        assert_eq!(
            read_bounded(&data[..4], 4, "s3://b/exact").await.unwrap(),
            b"1234"
        );
        let err = read_bounded(&data[..], 4, "s3://b/large")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("4-byte read limit"));
    }

    #[tokio::test]
    async fn content_verification_stops_after_the_first_oversized_byte() {
        let data = b"payload-that-is-too-long";
        let digest = vtop_core::checksum::sha256_bytes(data);
        let (result, oversized) =
            verify_reader_content(&data[..], 3, ObjectChecksum::new("sha256", &digest), "test")
                .await
                .unwrap();
        assert!(!result.passed);
        assert!(oversized);
        assert!(result.message.contains("read 4 stored bytes"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_download_timeout_kills_a_hung_producer() {
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("exec /bin/sleep 5")
            .env_clear()
            .kill_on_drop(true);
        let started = std::time::Instant::now();
        let error = read_command_bounded(
            &mut command,
            8,
            "s3://bucket/object",
            "test",
            Duration::from_millis(50),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("timeout"));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    /// The one spawn policy every command backend call shares: a busy
    /// executable (ETXTBSY) is retried at most `TEXT_BUSY_RETRIES` times and
    /// then reported with the install-atomically hint; any other error is
    /// reported at once. Driven by a scripted spawn, not by racing a real
    /// busy file. Before this, the read-back and verify spawned once, so the
    /// "busy then free" case below failed them outright.
    #[tokio::test]
    async fn a_busy_executable_is_retried_a_bounded_number_of_times_on_every_spawn() {
        use std::io::{Error, ErrorKind};
        let scripted = |failures: Vec<Error>| {
            let mut failures = failures.into_iter();
            let mut attempts = 0_u32;
            move || {
                attempts += 1;
                match failures.next() {
                    Some(error) => Err(error),
                    None => Ok(attempts),
                }
            }
        };
        let busy = || Error::from_raw_os_error(TEXT_BUSY);

        // Busy for every retry the policy allows, then free: spawned.
        let failures = (0..TEXT_BUSY_RETRIES).map(|_| busy()).collect();
        let attempts = retry_text_busy(scripted(failures), "aws cli", "download")
            .await
            .expect("a spawn that frees up within the retries must succeed");
        assert_eq!(attempts, TEXT_BUSY_RETRIES + 1);

        // Busy one time more than that: reported, with the hint, after
        // exactly 1 + TEXT_BUSY_RETRIES attempts.
        let mut calls = 0_u32;
        let mut spawn = scripted((0..=TEXT_BUSY_RETRIES).map(|_| busy()).collect());
        let error = retry_text_busy(
            || {
                calls += 1;
                spawn()
            },
            "aws cli",
            "verification",
        )
        .await
        .unwrap_err();
        assert_eq!(calls, TEXT_BUSY_RETRIES + 1, "retries must be bounded");
        let message = error.to_string();
        assert!(
            message.contains("spawning aws cli verification:")
                && message.contains("install/replace it atomically"),
            "unexpected error: {message}"
        );

        // Not busy: no retry, reported at once (with the noexec hint for a
        // permission error).
        for (error, hint) in [
            (Error::from(ErrorKind::PermissionDenied), Some("noexec")),
            (Error::from(ErrorKind::NotFound), None),
        ] {
            let mut calls = 0_u32;
            let mut spawn = scripted(vec![error]);
            let error = retry_text_busy(
                || {
                    calls += 1;
                    spawn()
                },
                "s3cmd",
                "upload",
            )
            .await
            .unwrap_err();
            assert_eq!(calls, 1, "only a busy executable is retried: {error}");
            let message = error.to_string();
            assert!(message.contains("spawning s3cmd upload:"), "{message}");
            assert_eq!(hint.is_some_and(|h| message.contains(h)), hint.is_some());
            assert!(!message.contains("atomically"), "{message}");
        }
    }
}

#[cfg(test)]
mod object_lock_tests {
    use super::*;

    fn at(secs: u64, nanos: u32) -> SystemTime {
        UNIX_EPOCH + Duration::new(secs, nanos)
    }

    /// #684: the date asked for is never earlier than now plus the
    /// retention: both terms round UP to a whole second, and whole seconds
    /// are what S3 echoes back, so the comparison is exact.
    #[test]
    fn the_date_asked_for_is_now_plus_the_retention_rounded_up() {
        let policy = ObjectLockPolicy::from_config(&ObjectLockConfig {
            mode: ObjectLockMode::Governance,
            retain_days: 1,
            legal_hold: false,
        });
        assert_eq!(
            policy.request_at(at(1_000, 0)).retain_until_unix_secs,
            1_000 + 86_400
        );
        assert_eq!(
            policy.request_at(at(1_000, 1)).retain_until_unix_secs,
            1_001 + 86_400,
            "a sub-second clock rounds up, never down"
        );
        let short = ObjectLockPolicy::with_retention_for_tests(
            ObjectLockMode::Compliance,
            Duration::from_millis(1_500),
            true,
        );
        let asked = short.request_at(at(10, 0));
        assert_eq!(asked.retain_until_unix_secs, 12);
        assert!(asked.legal_hold && asked.mode == ObjectLockMode::Compliance);
        // A clock before 1970 asks for a date in the past, which the store
        // refuses: the write fails rather than landing unlocked.
        let before = UNIX_EPOCH - Duration::from_secs(5);
        assert_eq!(policy.request_at(before).retain_until_unix_secs, 86_400);
        assert!(policy.matches(&ObjectLockConfig {
            mode: ObjectLockMode::Governance,
            retain_days: 1,
            legal_hold: false,
        }));
    }

    /// The comparison is against what was ASKED for, so the stored date equal
    /// to it passes, one second earlier fails, and later (a longer bucket
    /// default) passes. No clock is read here: skew between engine and store
    /// cannot fail a batch.
    #[test]
    fn the_judgment_compares_against_the_date_asked_for() {
        let expected = RetentionExpectation {
            mode: ObjectLockMode::Governance,
            retain_until_at_least_unix_secs: 2_000,
            legal_hold: true,
        };
        let kept = |mode: &str, until: i64, hold: Option<bool>| StoredRetention {
            mode: Some(mode.into()),
            retain_until_unix_secs: Some(until),
            legal_hold: hold,
        };
        assert!(judge_retention(&kept("GOVERNANCE", 2_000, Some(true)), &expected).passed());
        assert!(judge_retention(&kept("governance", 9_999, Some(true)), &expected).passed());
        assert_eq!(
            judge_retention(&kept("GOVERNANCE", 1_999, Some(true)), &expected),
            RetentionCheck::TooShort {
                retain_until_unix_secs: 1_999
            }
        );
        assert!(matches!(
            judge_retention(&kept("COMPLIANCE", 2_000, Some(true)), &expected),
            RetentionCheck::ModeMismatch { .. }
        ));
        assert_eq!(
            judge_retention(&kept("GOVERNANCE", 2_000, None), &expected),
            RetentionCheck::LegalHoldMissing
        );
        assert_eq!(
            judge_retention(&StoredRetention::default(), &expected),
            RetentionCheck::Missing
        );
        let mode_only = StoredRetention {
            mode: Some("GOVERNANCE".into()),
            ..StoredRetention::default()
        };
        assert_eq!(
            judge_retention(&mode_only, &expected),
            RetentionCheck::Missing
        );
        for (check, label) in [
            (RetentionCheck::Missing, "missing"),
            (RetentionCheck::LegalHoldMissing, "legal_hold_missing"),
            (RetentionCheck::Unsupported, "error"),
        ] {
            assert_eq!(check.label(), label);
        }
    }
}

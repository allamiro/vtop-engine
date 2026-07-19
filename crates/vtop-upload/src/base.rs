//! Upload backend trait and shared types.
//!
//! The native S3 backend is the primary production backend; command-based
//! backends (s3cmd/awscli/minio) are compatibility mode. Every backend MUST
//! support `verify_object`, and the engine MUST NOT commit source progress
//! until verification passes.

use async_trait::async_trait;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use vtop_core::checksum::digest_reader;
use vtop_core::errors::VtopError;
use vtop_core::types::ChecksumAlgorithm;

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
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| VtopError::Upload(format!("spawning {backend} verification: {e}")))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| VtopError::Upload(format!("{backend} verification stdout unavailable")))?;
    let completed = tokio::time::timeout(timeout, async {
        let (result, oversized) =
            verify_reader_content(stdout, expected_size, expected, backend).await?;
        if oversized {
            let _ = child.kill().await;
        }
        let status = child
            .wait()
            .await
            .map_err(|e| VtopError::Upload(format!("waiting for {backend} verification: {e}")))?;
        if !status.success() && !oversized {
            return Err(VtopError::Upload(format!(
                "{backend} verification command exited with {status}"
            )));
        }
        Ok::<_, VtopError>(result)
    })
    .await;
    match completed {
        Ok(Ok(result)) => Ok(result),
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
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| VtopError::Upload(format!("spawning {backend} download: {e}")))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| VtopError::Upload(format!("{backend} download stdout unavailable")))?;
    let completed = tokio::time::timeout(timeout, async {
        let bytes = read_bounded(stdout, max_bytes, object_uri).await?;
        let status = child
            .wait()
            .await
            .map_err(|e| VtopError::Upload(format!("waiting for {backend} download: {e}")))?;
        if !status.success() {
            return Err(VtopError::Upload(format!(
                "{backend} download command exited with {status}"
            )));
        }
        Ok::<_, VtopError>(bytes)
    })
    .await;
    match completed {
        Ok(Ok(bytes)) => Ok(bytes),
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
    ) -> Result<(), VtopError>;

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

    /// HEAD/stat an object.
    async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError>;

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

    /// Ensure a bucket exists (idempotent). Default is a no-op; backends that
    /// support it (native S3) override this. Only invoked when the engine is
    /// configured with `upload.create_bucket = true` — used to provision
    /// per-format buckets on demand.
    async fn ensure_bucket(&self, _bucket: &str) -> Result<(), VtopError> {
        Ok(())
    }

    fn backend_name(&self) -> &'static str;

    fn supports_checksum_verification(&self) -> bool;

    fn supports_multipart(&self) -> bool;
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
}

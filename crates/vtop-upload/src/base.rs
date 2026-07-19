//! Upload backend trait and shared types.
//!
//! The native S3 backend is the primary production backend; command-based
//! backends (s3cmd/awscli/minio) are compatibility mode. Every backend MUST
//! support `verify_object`, and the engine MUST NOT commit source progress
//! until verification passes.

use async_trait::async_trait;
use std::path::Path;
use vtop_core::errors::VtopError;

/// An engine-computed object checksum: the algorithm name (`sha256`, `blake3`)
/// plus the lowercase-hex digest. Carrying the algorithm lets a backend choose
/// the strongest available verification — e.g. native S3 uses server-validated
/// `x-amz-checksum-sha256` only for SHA-256, and metadata for other algorithms.
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

/// Result of a HEAD/stat on a stored object.
#[derive(Debug, Clone)]
pub struct ObjectHead {
    pub uri: String,
    pub size_bytes: Option<u64>,
    pub etag: Option<String>,
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
    async fn put_manifest(
        &self,
        local_path: &Path,
        manifest_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<(), VtopError>;

    /// HEAD/stat an object.
    async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError>;

    /// Download an object's full content.
    ///
    /// For explicit verification tooling (`vtopctl verify-manifest`), which
    /// must hash the ACTUAL stored bytes — metadata can lie about a replaced
    /// object, content cannot (#68). Never called on the archiving hot path,
    /// so implementations may favour simplicity over streaming.
    async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError>;

    /// Verify a stored object against an expected size and (when provided)
    /// checksum. `expected = None` means checksums are disabled, so only
    /// size/existence can be confirmed (a backend-limited result).
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
}

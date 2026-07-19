//! Upload backend trait and shared types.
//!
//! The native S3 backend is the primary production backend; command-based
//! backends (s3cmd/awscli/minio) are compatibility mode. Every backend MUST
//! support `verify_object`, and the engine MUST NOT commit source progress
//! until verification passes.

use async_trait::async_trait;
use std::path::Path;
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

    let algo = match expected.algorithm.parse::<ChecksumAlgorithm>() {
        Ok(ChecksumAlgorithm::None) => {
            return Ok(VerificationResult::failed(
                "checksum value supplied with disabled algorithm",
            ))
        }
        Ok(algo) => algo,
        Err(e) => return Ok(VerificationResult::failed(e)),
    };
    let file = tokio::fs::File::open(path).await?;
    let Some((actual, bytes_read)) = digest_reader(algo, file).await? else {
        return Ok(VerificationResult::failed(
            "content digest was not computed",
        ));
    };
    if bytes_read != expected_size {
        return Ok(VerificationResult::failed(format!(
            "size mismatch: expected {expected_size}, read {bytes_read} stored bytes"
        )));
    }
    if actual.eq_ignore_ascii_case(expected.hex) {
        Ok(VerificationResult::passed(format!(
            "{backend}: stored content {algorithm} verified",
            algorithm = algo.as_str()
        )))
    } else {
        Ok(VerificationResult::failed(format!(
            "stored content checksum mismatch for {}",
            algo.as_str()
        )))
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
    /// Used by explicit verification tooling and by the archiving path when
    /// manifest authentication is enabled. Both must inspect ACTUAL stored
    /// bytes — metadata can describe a replaced object without detecting it.
    /// Manifests are small, so implementations may return them as one buffer;
    /// telemetry-object verification should use a streaming API when added.
    async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError>;

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
}

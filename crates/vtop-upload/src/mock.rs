//! In-memory mock upload backend for unit and integration tests.
//!
//! Stores objects in memory, computes real SHA-256, and can be configured to
//! fail verification — exercising the "verification fails -> source not
//! committed" path without any external service.

use crate::base::{ObjectChecksum, ObjectHead, StoredManifest, UploadBackend, VerificationResult};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use vtop_core::errors::VtopError;

#[derive(Default)]
struct Stored {
    size: u64,
    /// Engine-provided object checksum (None when checksums are disabled).
    checksum: Option<String>,
    /// Full content, so `get_object`-based verification is testable.
    data: Vec<u8>,
    /// Prevent the corrupt-on-verify fault from toggling the same byte back to
    /// its original value on a retry.
    corrupted: bool,
}

/// Version history for one key. IDs come from a monotonic counter so a
/// deleted version's ID is never reused by a later upload.
#[derive(Default)]
struct VersionHistory {
    next: u64,
    entries: Vec<(String, Vec<u8>)>,
}

/// A test double for [`UploadBackend`].
pub struct MockBackend {
    objects: Mutex<HashMap<String, Stored>>,
    /// Immutable version history per key, mirroring a versioned bucket: every
    /// store appends `(version_id, bytes)` and nothing mutates old entries.
    /// `corrupt()`/`corrupt_on_verify` intentionally touch only the current
    /// key, so pinned reads stay stable the way S3 versions do.
    versions: Mutex<HashMap<String, VersionHistory>>,
    /// When true, `verify_object` always reports failure.
    fail_verification: bool,
    /// When true, `verify_object` reports backend-limited (size-only) success.
    backend_limited: bool,
    /// Test-only attack model: alter the stored body immediately before
    /// verification while leaving uploader-provided checksum metadata intact.
    corrupt_on_verify: bool,
}

impl Default for MockBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MockBackend {
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
            versions: Mutex::new(HashMap::new()),
            fail_verification: false,
            backend_limited: false,
            corrupt_on_verify: false,
        }
    }

    /// A mock that always fails verification.
    pub fn failing() -> Self {
        Self {
            fail_verification: true,
            ..Self::new()
        }
    }

    /// A mock that can only do size verification (no strong hash check).
    pub fn limited() -> Self {
        Self {
            backend_limited: true,
            ..Self::new()
        }
    }

    /// A mock storage service that replaces stored bytes after upload but
    /// leaves size and uploader metadata unchanged.
    pub fn corrupting() -> Self {
        Self {
            corrupt_on_verify: true,
            ..Self::new()
        }
    }

    /// True if the object exists in the mock store.
    pub fn contains(&self, uri: &str) -> bool {
        self.objects.lock().unwrap().contains_key(uri)
    }

    /// Test hook: flip one byte of the stored content while leaving the
    /// recorded size and checksum untouched — the shape of silent corruption
    /// or replacement that a HEAD/metadata check cannot see and content
    /// verification must (#68).
    pub fn corrupt(&self, uri: &str) {
        if let Some(s) = self.objects.lock().unwrap().get_mut(uri) {
            if let Some(b) = s.data.first_mut() {
                *b ^= 0xff;
                s.corrupted = true;
            }
        }
    }

    async fn store(
        &self,
        local_path: &Path,
        uri: &str,
        checksum: Option<&str>,
    ) -> Result<String, VtopError> {
        let data = tokio::fs::read(local_path).await?;
        let stored = Stored {
            size: data.len() as u64,
            checksum: checksum.map(|s| s.to_string()),
            data: data.clone(),
            corrupted: false,
        };
        self.objects.lock().unwrap().insert(uri.to_string(), stored);
        let mut versions = self.versions.lock().unwrap();
        let history = versions.entry(uri.to_string()).or_default();
        history.next += 1;
        let version_id = format!("v{}", history.next);
        history.entries.push((version_id.clone(), data));
        Ok(version_id)
    }

    /// Test hook: remove one stored version, simulating retention expiry or a
    /// privileged versioned delete.
    pub fn delete_version(&self, uri: &str, version_id: &str) {
        if let Some(history) = self.versions.lock().unwrap().get_mut(uri) {
            history.entries.retain(|(id, _)| id != version_id);
        }
    }
}

#[async_trait]
impl UploadBackend for MockBackend {
    async fn put_object(
        &self,
        local_path: &Path,
        object_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<(), VtopError> {
        self.store(local_path, object_uri, checksum.map(|c| c.hex))
            .await
            .map(|_| ())
    }

    async fn put_manifest(
        &self,
        local_path: &Path,
        manifest_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredManifest, VtopError> {
        let version_id = self
            .store(local_path, manifest_uri, checksum.map(|c| c.hex))
            .await?;
        Ok(StoredManifest {
            version_id: Some(version_id),
        })
    }

    async fn get_manifest_pinned(
        &self,
        manifest_uri: &str,
        version_id: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, VtopError> {
        let versions = self.versions.lock().unwrap();
        let data = versions
            .get(manifest_uri)
            .and_then(|history| {
                history
                    .entries
                    .iter()
                    .find(|(id, _)| id == version_id)
                    .map(|(_, data)| data.clone())
            })
            .ok_or_else(|| VtopError::NotFound(format!("{manifest_uri} (version {version_id})")))?;
        if data.len() > max_bytes {
            return Err(VtopError::Upload(format!(
                "stored object {manifest_uri} exceeds the {max_bytes}-byte read limit"
            )));
        }
        Ok(data)
    }

    async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError> {
        let map = self.objects.lock().unwrap();
        let s = map
            .get(object_uri)
            .ok_or_else(|| VtopError::NotFound(object_uri.to_string()))?;
        Ok(s.data.clone())
    }

    async fn get_object_bounded(
        &self,
        object_uri: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, VtopError> {
        let map = self.objects.lock().unwrap();
        let stored = map
            .get(object_uri)
            .ok_or_else(|| VtopError::NotFound(object_uri.to_string()))?;
        if stored.data.len() > max_bytes {
            return Err(VtopError::Upload(format!(
                "stored object {object_uri} exceeds the {max_bytes}-byte read limit"
            )));
        }
        Ok(stored.data.clone())
    }

    async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError> {
        let map = self.objects.lock().unwrap();
        let s = map
            .get(object_uri)
            .ok_or_else(|| VtopError::NotFound(object_uri.to_string()))?;
        Ok(ObjectHead {
            uri: object_uri.to_string(),
            size_bytes: Some(s.size),
            etag: s.checksum.clone(),
            // The stored checksum is uploader-provided test metadata. Strong
            // verification below hashes `data` instead.
            checksum_sha256: None,
        })
    }

    async fn verify_object(
        &self,
        object_uri: &str,
        expected_size: u64,
        expected: Option<ObjectChecksum<'_>>,
    ) -> Result<VerificationResult, VtopError> {
        if self.fail_verification {
            return Ok(VerificationResult::failed(
                "mock: forced verification failure",
            ));
        }
        let mut map = self.objects.lock().unwrap();
        let stored = map
            .get_mut(object_uri)
            .ok_or_else(|| VtopError::NotFound(object_uri.to_string()))?;
        if self.corrupt_on_verify && !stored.corrupted {
            if let Some(first) = stored.data.first_mut() {
                *first ^= 0xff;
                stored.corrupted = true;
            }
        }
        if stored.data.len() as u64 != expected_size {
            return Ok(VerificationResult::failed("mock: size mismatch"));
        }
        if self.backend_limited {
            return Ok(VerificationResult::limited("mock: size-only verification"));
        }
        let Some(expected) = expected else {
            return Ok(VerificationResult::limited(
                "mock: size-only (checksums disabled)",
            ));
        };
        let algo = match expected
            .algorithm
            .parse::<vtop_core::types::ChecksumAlgorithm>()
        {
            Ok(algo) if algo.is_enabled() => algo,
            Ok(_) => {
                return Ok(VerificationResult::failed(
                    "mock: checksum supplied with disabled algorithm",
                ))
            }
            Err(e) => return Ok(VerificationResult::failed(e)),
        };
        let actual = vtop_core::checksum::digest_bytes(algo, &stored.data)
            .expect("enabled checksum algorithm has a digest");
        if actual.eq_ignore_ascii_case(expected.hex) {
            Ok(VerificationResult::passed(
                "mock: stored content checksum verified",
            ))
        } else {
            Ok(VerificationResult::failed("mock: checksum mismatch"))
        }
    }

    async fn delete_object(&self, object_uri: &str) -> Result<(), VtopError> {
        self.objects.lock().unwrap().remove(object_uri);
        self.versions.lock().unwrap().remove(object_uri);
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "mock"
    }

    fn supports_object_versions(&self) -> bool {
        true
    }

    async fn verify_bucket_versioning(&self, _bucket: &str) -> Result<(), VtopError> {
        Ok(())
    }
    fn supports_checksum_verification(&self) -> bool {
        !self.backend_limited
    }
    fn supports_multipart(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tmp(data: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(data).unwrap();
        f.flush().unwrap();
        f
    }

    fn ck(hex: &str) -> ObjectChecksum<'_> {
        ObjectChecksum::new("sha256", hex)
    }

    #[tokio::test]
    async fn round_trip_and_verify() {
        let b = MockBackend::new();
        let f = tmp(b"payload");
        let uri = "s3://bucket/obj";
        let digest = vtop_core::checksum::sha256_bytes(b"payload");
        b.put_object(f.path(), uri, Some(ck(&digest)))
            .await
            .unwrap();
        let res = b.verify_object(uri, 7, Some(ck(&digest))).await.unwrap();
        assert!(res.passed && !res.backend_limited);
    }

    #[tokio::test]
    async fn same_size_corruption_fails_with_uploader_metadata_unchanged() {
        let b = MockBackend::new();
        let f = tmp(b"payload");
        let uri = "s3://bucket/obj";
        let digest = vtop_core::checksum::sha256_bytes(b"payload");
        b.put_object(f.path(), uri, Some(ck(&digest)))
            .await
            .unwrap();
        b.corrupt(uri);

        let res = b.verify_object(uri, 7, Some(ck(&digest))).await.unwrap();
        assert!(!res.passed);
        assert!(!res.backend_limited);
    }

    #[tokio::test]
    async fn corrupting_backend_stays_corrupt_across_retries() {
        let b = MockBackend::corrupting();
        let f = tmp(b"payload");
        let uri = "s3://bucket/retry";
        let digest = vtop_core::checksum::sha256_bytes(b"payload");
        b.put_object(f.path(), uri, Some(ck(&digest)))
            .await
            .unwrap();

        for _ in 0..2 {
            let res = b.verify_object(uri, 7, Some(ck(&digest))).await.unwrap();
            assert!(!res.passed);
        }
    }

    #[tokio::test]
    async fn bounded_download_rejects_oversized_content() {
        let b = MockBackend::new();
        let f = tmp(b"12345");
        let uri = "s3://bucket/manifest";
        b.put_manifest(f.path(), uri, None).await.unwrap();
        assert!(b.get_object_bounded(uri, 5).await.is_ok());
        assert!(b.get_object_bounded(uri, 4).await.is_err());
    }

    #[tokio::test]
    async fn disabled_checksum_is_backend_limited() {
        let b = MockBackend::new();
        let f = tmp(b"payload");
        b.put_object(f.path(), "s3://b/o", None).await.unwrap();
        let res = b.verify_object("s3://b/o", 7, None).await.unwrap();
        assert!(res.passed && res.backend_limited);
    }

    #[tokio::test]
    async fn failing_mock_fails_verification() {
        let b = MockBackend::failing();
        let f = tmp(b"x");
        b.put_object(f.path(), "s3://b/o", Some(ck("x")))
            .await
            .unwrap();
        let res = b.verify_object("s3://b/o", 1, Some(ck("x"))).await.unwrap();
        assert!(!res.passed);
    }

    #[tokio::test]
    async fn pinned_version_is_immutable_across_overwrites() {
        let b = MockBackend::new();
        let uri = "s3://b/m.json";
        let v1 = tmp(b"manifest-v1");
        let first = b.put_manifest(v1.path(), uri, None).await.unwrap();
        let first_version = first.version_id.unwrap();
        let v2 = tmp(b"manifest-v2-rollback");
        let second = b.put_manifest(v2.path(), uri, None).await.unwrap();
        assert_ne!(Some(&first_version), second.version_id.as_ref());

        // The current key serves the overwrite; the pin still serves v1.
        assert_eq!(b.get_object(uri).await.unwrap(), b"manifest-v2-rollback");
        assert_eq!(
            b.get_manifest_pinned(uri, &first_version, 1024)
                .await
                .unwrap(),
            b"manifest-v1"
        );
    }

    #[tokio::test]
    async fn deleted_version_fails_pinned_read_and_is_never_reused() {
        let b = MockBackend::new();
        let uri = "s3://b/m.json";
        let f = tmp(b"manifest-v1");
        let stored = b.put_manifest(f.path(), uri, None).await.unwrap();
        let version = stored.version_id.unwrap();
        b.delete_version(uri, &version);
        assert!(b.get_manifest_pinned(uri, &version, 1024).await.is_err());

        // A later upload must not resurrect the deleted ID: the old pin keeps
        // failing instead of silently serving the new bytes.
        let g = tmp(b"manifest-after-delete");
        let second = b.put_manifest(g.path(), uri, None).await.unwrap();
        assert_ne!(second.version_id.as_deref(), Some(version.as_str()));
        assert!(b.get_manifest_pinned(uri, &version, 1024).await.is_err());
    }

    #[tokio::test]
    async fn pinned_read_enforces_byte_bound() {
        let b = MockBackend::new();
        let uri = "s3://b/m.json";
        let f = tmp(b"12345");
        let stored = b.put_manifest(f.path(), uri, None).await.unwrap();
        let version = stored.version_id.unwrap();
        assert!(b.get_manifest_pinned(uri, &version, 5).await.is_ok());
        assert!(b.get_manifest_pinned(uri, &version, 4).await.is_err());
    }
}

//! In-memory mock upload backend for unit and integration tests.
//!
//! Stores objects in memory, computes real SHA-256, and can be configured to
//! fail verification — exercising the "verification fails -> source not
//! committed" path without any external service.

use crate::base::{
    ObjectChecksum, ObjectHead, StoredManifest, StoredObject, UploadBackend, UploadedPart,
    VerificationResult,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use vtop_core::errors::VtopError;

#[derive(Default)]
struct Stored {
    size: u64,
    /// Engine-provided object checksum (None when checksums are disabled).
    checksum: Option<String>,
    /// What the SERVICE computed, under s3_checksum_semantics: a real
    /// SHA-256 for a single PUT, `None` for a multipart completion (S3's
    /// composite does not decode as one). Distinct from `checksum`, which
    /// is uploader-provided metadata and never evidence.
    service_sha256: Option<String>,
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

#[derive(Default)]
struct PendingMultipart {
    parts: HashMap<u32, Vec<u8>>,
}

/// A test double for [`UploadBackend`].
pub struct MockBackend {
    objects: Mutex<HashMap<String, Stored>>,
    /// Immutable version history per key, mirroring a versioned bucket: every
    /// store appends `(version_id, bytes)` and nothing mutates old entries.
    /// `corrupt()`/`corrupt_on_verify` intentionally touch only the current
    /// key, so pinned reads stay stable the way S3 versions do.
    versions: Mutex<HashMap<String, VersionHistory>>,
    /// In-progress multipart uploads keyed by `(object_uri, upload_id)`.
    multiparts: Mutex<HashMap<(String, String), PendingMultipart>>,
    next_upload_id: Mutex<u64>,
    /// When true, `verify_object` always reports failure.
    fail_verification: bool,
    /// When true, `verify_object` reports backend-limited (size-only) success.
    backend_limited: bool,
    /// Test-only attack model: alter the stored body immediately before
    /// verification while leaving uploader-provided checksum metadata intact.
    corrupt_on_verify: bool,
    /// Fail `upload_part` once the number of successful part uploads reaches
    /// this counter's value (test interrupt/resume). `usize::MAX` disables.
    multipart_fail_after_parts: Arc<AtomicUsize>,
    multipart_parts_uploaded: Arc<AtomicUsize>,
    /// `ensure_bucket` calls, counted so tests can pin how often the engine
    /// pays the provisioning round trip — once per bucket per process, not
    /// per batch (#102).
    bucket_ensures: AtomicUsize,
    /// How many upcoming OBJECT puts answer with a throttle (#102), counted
    /// down as they are refused; `throttled_manifest_puts` is the manifest
    /// stage's own budget, so a test can throttle either stage by name
    /// (review) — an object put always precedes its manifest put, so one
    /// shared budget could never reach the manifest stage.
    throttled_object_puts: AtomicUsize,
    throttled_manifest_puts: AtomicUsize,
    /// S3-FAITHFUL checksum semantics (#482): a single PUT records a
    /// service-computed SHA-256; a multipart completion records NONE —
    /// mirroring the composite value b64_to_hex_sha256 rejects — and
    /// `verify_object` judges the SHA-256 arm through the SAME shared
    /// function the real backend uses, so the mock that reproduces the
    /// composite-checksum gap cannot drift from the code whose gap it
    /// reproduces. Off by default: every existing test keeps the mock's
    /// hash-the-stored-bytes verification.
    s3_checksum_semantics: bool,
    /// A configurable non-final-part floor (#482), so should_multipart's
    /// backend-minimum gate is testable without a live S3. Zero by default.
    min_part_size: u64,
    /// When true, abort_multipart_upload fails — so the sweep's
    /// keep-session-on-failed-abort path is testable (#482).
    fail_abort: bool,
}

impl Default for MockBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MockBackend {
    pub fn with_s3_checksum_semantics(mut self) -> Self {
        self.s3_checksum_semantics = true;
        self
    }

    pub fn with_min_part_size(mut self, bytes: u64) -> Self {
        self.min_part_size = bytes;
        self
    }

    pub fn with_failing_abort(mut self) -> Self {
        self.fail_abort = true;
        self
    }

    pub fn new() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
            versions: Mutex::new(HashMap::new()),
            multiparts: Mutex::new(HashMap::new()),
            next_upload_id: Mutex::new(0),
            fail_verification: false,
            backend_limited: false,
            corrupt_on_verify: false,
            s3_checksum_semantics: false,
            min_part_size: 0,
            fail_abort: false,
            multipart_fail_after_parts: Arc::new(AtomicUsize::new(usize::MAX)),
            multipart_parts_uploaded: Arc::new(AtomicUsize::new(0)),
            bucket_ensures: AtomicUsize::new(0),
            throttled_object_puts: AtomicUsize::new(0),
            throttled_manifest_puts: AtomicUsize::new(0),
        }
    }

    /// The next `count` OBJECT puts answer as an overloaded store does
    /// (#102): `UploadThrottled`, nothing stored.
    pub fn with_throttled_puts(self, count: usize) -> Self {
        self.throttled_object_puts.store(count, Ordering::SeqCst);
        self
    }

    /// The next `count` MANIFEST puts answer with a throttle (#102); the
    /// object put before each still lands.
    pub fn with_throttled_manifest_puts(self, count: usize) -> Self {
        self.throttled_manifest_puts.store(count, Ordering::SeqCst);
        self
    }

    fn take_throttle(&self, budget: &AtomicUsize, uri: &str) -> Result<(), VtopError> {
        let refused = budget
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        if refused {
            Err(VtopError::UploadThrottled(format!(
                "mock put {uri}: SlowDown (http 503): please reduce your request rate"
            )))
        } else {
            Ok(())
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

    /// Fail part uploads after `n` successful parts (shared counter for resume tests).
    pub fn with_multipart_fail_after_parts(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.multipart_fail_after_parts = counter;
        self
    }

    pub fn multipart_parts_uploaded(&self) -> usize {
        self.multipart_parts_uploaded.load(Ordering::SeqCst)
    }

    pub fn pending_multipart_count(&self) -> usize {
        self.multiparts.lock().unwrap().len()
    }

    pub fn ensure_bucket_calls(&self) -> usize {
        self.bucket_ensures.load(Ordering::SeqCst)
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
                // A corrupted body must recompute its service digest (or the
                // s3-semantics verify would accept the STALE service SHA-256
                // without reading the tampered bytes, review). Recompute so
                // the mock stays a faithful adversary: S3 would return the
                // digest of what it now stores.
                if s.service_sha256.is_some() {
                    s.service_sha256 = Some(vtop_core::checksum::sha256_bytes(&s.data));
                }
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
        // Under S3 semantics a single PUT gets a service-computed SHA-256,
        // exactly as the real backend receives one for a whole-object
        // upload it sent the checksum with (#482).
        let service = self
            .s3_checksum_semantics
            .then(|| vtop_core::checksum::sha256_bytes(&data));
        self.store_bytes(uri, data, checksum, service)
    }

    fn store_bytes(
        &self,
        uri: &str,
        data: Vec<u8>,
        checksum: Option<&str>,
        service_sha256: Option<String>,
    ) -> Result<String, VtopError> {
        let stored = Stored {
            size: data.len() as u64,
            checksum: checksum.map(|s| s.to_string()),
            service_sha256,
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
    ) -> Result<StoredObject, VtopError> {
        self.take_throttle(&self.throttled_object_puts, object_uri)?;
        let version_id = self
            .store(local_path, object_uri, checksum.map(|c| c.hex))
            .await?;
        Ok(StoredObject {
            version_id: Some(version_id),
        })
    }

    async fn put_manifest(
        &self,
        local_path: &Path,
        manifest_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredManifest, VtopError> {
        self.take_throttle(&self.throttled_manifest_puts, manifest_uri)?;
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
        self.get_object_pinned(manifest_uri, version_id, max_bytes)
            .await
    }

    async fn get_object_pinned(
        &self,
        object_uri: &str,
        version_id: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, VtopError> {
        let versions = self.versions.lock().unwrap();
        let data = versions
            .get(object_uri)
            .and_then(|history| {
                history
                    .entries
                    .iter()
                    .find(|(id, _)| id == version_id)
                    .map(|(_, data)| data.clone())
            })
            .ok_or_else(|| VtopError::NotFound(format!("{object_uri} (version {version_id})")))?;
        if data.len() > max_bytes {
            return Err(VtopError::Upload(format!(
                "stored object {object_uri} exceeds the {max_bytes}-byte read limit"
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
            // Under S3 semantics the head carries what the SERVICE computed
            // (#482); otherwise the stored checksum stays uploader-provided
            // test metadata and strong verification hashes `data` instead.
            checksum_sha256: if self.s3_checksum_semantics {
                s.service_sha256.clone()
            } else {
                None
            },
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
                if stored.service_sha256.is_some() {
                    stored.service_sha256 = Some(vtop_core::checksum::sha256_bytes(&stored.data));
                }
            }
        }
        if stored.data.len() as u64 != expected_size {
            return Ok(VerificationResult::failed("mock: size mismatch"));
        }
        // S3-FAITHFUL arms (#482): the SHA-256 judgment goes through the
        // SAME shared function the real backend uses, so a service value of
        // None — a multipart completion — reads limited here exactly as it
        // does against S3. BLAKE3 hashes the stored body, which is the real
        // read-back's semantics.
        if self.s3_checksum_semantics {
            let Some(expected) = expected else {
                return Ok(VerificationResult::limited(
                    "object present and size matches (checksums disabled)",
                ));
            };
            return Ok(match expected.algorithm.to_ascii_lowercase().as_str() {
                "sha256" => {
                    let judged = crate::base::judge_service_sha256(
                        stored.service_sha256.as_deref(),
                        expected.hex,
                    );
                    if !judged.backend_limited {
                        judged
                    } else {
                        // The read-back fallback, as the real backend does
                        // it (#482): no service checksum means hash the
                        // stored body, never settle for limited.
                        let actual = vtop_core::checksum::sha256_bytes(&stored.data);
                        if actual.eq_ignore_ascii_case(expected.hex) {
                            VerificationResult::passed(
                                "stored content SHA-256 verified by read-back (no \
                                 service whole-object checksum; multipart or \
                                 unchecked upload)",
                            )
                        } else {
                            VerificationResult::failed(
                                "stored content SHA-256 mismatch on read-back",
                            )
                        }
                    }
                }
                "blake3" => {
                    let actual = vtop_core::checksum::blake3_bytes(&stored.data);
                    if actual.eq_ignore_ascii_case(expected.hex) {
                        VerificationResult::passed("mock s3: read-back BLAKE3 verified")
                    } else {
                        VerificationResult::failed("mock s3: read-back BLAKE3 mismatch")
                    }
                }
                other => VerificationResult::failed(format!(
                    "mock s3: unsupported checksum algorithm {other}"
                )),
            });
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

    /// Overrides the default no-op only to count: the trait's default would
    /// succeed invisibly, and the count is the whole point of mocking this.
    async fn ensure_bucket(&self, _bucket: &str) -> Result<(), VtopError> {
        self.bucket_ensures.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn supports_checksum_verification(&self) -> bool {
        !self.backend_limited
    }
    fn supports_multipart(&self) -> bool {
        true
    }
    fn min_part_size_bytes(&self) -> u64 {
        self.min_part_size
    }

    async fn create_multipart_upload(
        &self,
        object_uri: &str,
        _content_type: &str,
        _checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<String, VtopError> {
        let mut next = self.next_upload_id.lock().unwrap();
        *next += 1;
        let upload_id = format!("mpu-{next}");
        self.multiparts.lock().unwrap().insert(
            (object_uri.to_owned(), upload_id.clone()),
            PendingMultipart::default(),
        );
        Ok(upload_id)
    }

    async fn upload_part(
        &self,
        object_uri: &str,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<UploadedPart, VtopError> {
        let uploaded = self.multipart_parts_uploaded.fetch_add(1, Ordering::SeqCst);
        let limit = self.multipart_fail_after_parts.load(Ordering::SeqCst);
        if uploaded >= limit {
            return Err(VtopError::Upload(format!(
                "mock: injected multipart failure after {limit} parts"
            )));
        }
        let mut map = self.multiparts.lock().unwrap();
        let pending = map
            .get_mut(&(object_uri.to_owned(), upload_id.to_owned()))
            .ok_or_else(|| {
                VtopError::Upload(format!(
                    "mock: unknown multipart upload {upload_id} for {object_uri}"
                ))
            })?;
        pending.parts.insert(part_number, data.to_vec());
        Ok(UploadedPart {
            part_number,
            etag: format!("etag-{part_number}-{}", data.len()),
        })
    }

    async fn complete_multipart_upload(
        &self,
        object_uri: &str,
        upload_id: &str,
        parts: &[UploadedPart],
    ) -> Result<StoredObject, VtopError> {
        let mut map = self.multiparts.lock().unwrap();
        let pending = map
            .remove(&(object_uri.to_owned(), upload_id.to_owned()))
            .ok_or_else(|| {
                VtopError::Upload(format!(
                    "mock: unknown multipart upload {upload_id} for {object_uri}"
                ))
            })?;
        let mut body = Vec::new();
        let mut ordered = parts.to_vec();
        ordered.sort_by_key(|p| p.part_number);
        for part in &ordered {
            let bytes = pending.parts.get(&part.part_number).ok_or_else(|| {
                VtopError::Upload(format!(
                    "mock: missing part {} for multipart {upload_id}",
                    part.part_number
                ))
            })?;
            body.extend_from_slice(bytes);
        }
        // A multipart completion records NO service SHA-256 (#482): S3
        // returns a composite with a part-count suffix, which the real
        // head-decoding rejects — this None IS that composite, as the
        // verify path experiences it.
        let version_id = self.store_bytes(object_uri, body, None, None)?;
        Ok(StoredObject {
            version_id: Some(version_id),
        })
    }

    async fn abort_multipart_upload(
        &self,
        object_uri: &str,
        upload_id: &str,
    ) -> Result<(), VtopError> {
        if self.fail_abort {
            return Err(VtopError::Upload("mock: forced abort failure".into()));
        }
        self.multiparts
            .lock()
            .unwrap()
            .remove(&(object_uri.to_owned(), upload_id.to_owned()));
        Ok(())
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

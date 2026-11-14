//! Local-filesystem upload backend.
//!
//! Writes telemetry objects under a local directory tree
//! (`<root>/<bucket>/<key>`), with the engine-provided checksum stored in a
//! sidecar file (`<key>.vtopck`) as an inventory hint. Verification ignores the
//! sidecar and streams the stored body through the configured digest. Useful
//! for air-gapped / offline archival and for benchmarking the pipeline with
//! real disk I/O but no object-storage service. `s3://bucket/key` URIs are
//! mapped onto the local tree.

use crate::base::{
    parse_s3_uri, read_bounded, verify_file_content, ObjectChecksum, ObjectHead, StoredManifest,
    StoredObject, UploadBackend, VerificationResult,
};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use vtop_core::errors::VtopError;

pub struct LocalFsBackend {
    root: PathBuf,
}

/// A read that failed: [`VtopError::NotFound`] only when the file is absent.
/// Every error used to read as NotFound, so an object that exists but cannot
/// be read (a permission error, an I/O failure) was reported as gone by
/// anything telling the two apart, `vtopctl scrub` among them (#628).
fn read_error(object_uri: &str, error: std::io::Error) -> VtopError {
    if error.kind() == std::io::ErrorKind::NotFound {
        VtopError::NotFound(object_uri.to_string())
    } else {
        VtopError::Upload(format!("localfs read {object_uri}: {error}"))
    }
}

impl LocalFsBackend {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn object_path(&self, uri: &str) -> Result<PathBuf, VtopError> {
        let (bucket, key) = parse_s3_uri(uri)?;
        // Defense in depth: reject any path-traversal / absolute segment so a
        // crafted URI can never escape the configured root, even if the key was
        // produced outside the normal (sanitized) partitioning path.
        for seg in std::iter::once(bucket.as_str()).chain(key.split('/')) {
            if seg == ".." || seg == "." {
                return Err(VtopError::Upload(format!(
                    "refusing path-traversal segment in object uri: {uri}"
                )));
            }
        }
        if key.starts_with('/') || bucket.starts_with('/') {
            return Err(VtopError::Upload(format!(
                "refusing absolute path in object uri: {uri}"
            )));
        }
        Ok(self.root.join(bucket).join(key))
    }

    fn checksum_sidecar(path: &Path) -> PathBuf {
        let mut p = path.as_os_str().to_owned();
        p.push(".vtopck");
        PathBuf::from(p)
    }

    async fn store(
        &self,
        local_path: &Path,
        uri: &str,
        checksum: Option<&str>,
    ) -> Result<(), VtopError> {
        let dest = self.object_path(uri)?;
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        // Copy beside the key, then rename onto it (review of #626): a
        // derived key can have a second writer of the same bytes, and a copy
        // straight onto the key truncates it first, so a crash mid-copy left
        // a verified object torn. The rename replaces the key whole.
        let mut staging = dest.as_os_str().to_owned();
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        staging.push(format!(".{}-{n}.partial", std::process::id()));
        let staging = PathBuf::from(staging);
        if let Err(e) = tokio::fs::copy(local_path, &staging).await {
            let _ = tokio::fs::remove_file(&staging).await;
            return Err(e.into());
        }
        if let Err(e) = tokio::fs::rename(&staging, &dest).await {
            let _ = tokio::fs::remove_file(&staging).await;
            return Err(e.into());
        }
        let sidecar = Self::checksum_sidecar(&dest);
        match checksum {
            Some(c) => tokio::fs::write(&sidecar, c).await?,
            None => {
                let _ = tokio::fs::remove_file(&sidecar).await;
            }
        }
        tracing::info!(uri, path = %dest.display(), "object written via localfs");
        Ok(())
    }
}

#[async_trait]
impl UploadBackend for LocalFsBackend {
    async fn put_object(
        &self,
        local_path: &Path,
        object_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredObject, VtopError> {
        self.store(local_path, object_uri, checksum.map(|c| c.hex))
            .await
            .map(|()| StoredObject::default())
    }

    async fn put_manifest(
        &self,
        local_path: &Path,
        manifest_uri: &str,
        checksum: Option<ObjectChecksum<'_>>,
    ) -> Result<StoredManifest, VtopError> {
        self.store(local_path, manifest_uri, checksum.map(|c| c.hex))
            .await?;
        // A plain filesystem tree has no object versions to pin.
        Ok(StoredManifest::default())
    }

    async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError> {
        let path = self.object_path(object_uri)?;
        tokio::fs::read(&path)
            .await
            .map_err(|e| read_error(object_uri, e))
    }

    async fn get_object_bounded(
        &self,
        object_uri: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, VtopError> {
        let path = self.object_path(object_uri)?;
        let file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| read_error(object_uri, e))?;
        read_bounded(file, max_bytes, object_uri).await
    }

    async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError> {
        let path = self.object_path(object_uri)?;
        let meta = tokio::fs::metadata(&path)
            .await
            .map_err(|e| read_error(object_uri, e))?;
        Ok(ObjectHead {
            uri: object_uri.to_string(),
            size_bytes: Some(meta.len()),
            etag: None,
            // The .vtopck file is written by this engine. It is useful as an
            // inventory hint, but cannot prove what bytes are in `path`.
            checksum_sha256: None,
        })
    }

    async fn verify_object(
        &self,
        object_uri: &str,
        expected_size: u64,
        expected: Option<ObjectChecksum<'_>>,
    ) -> Result<VerificationResult, VtopError> {
        let path = self.object_path(object_uri)?;
        verify_file_content(&path, expected_size, expected, "localfs").await
    }

    async fn delete_object(&self, object_uri: &str) -> Result<(), VtopError> {
        let path = self.object_path(object_uri)?;
        let _ = tokio::fs::remove_file(Self::checksum_sidecar(&path)).await;
        tokio::fs::remove_file(&path).await?;
        Ok(())
    }

    async fn ensure_bucket(&self, bucket: &str) -> Result<(), VtopError> {
        tokio::fs::create_dir_all(self.root.join(bucket)).await?;
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "localfs"
    }
    fn supports_checksum_verification(&self) -> bool {
        true
    }

    fn reports_absent_objects(&self) -> bool {
        // read_error: NotFound only for an absent file (#626 relies on it).
        true
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

    /// Only an absent file is NotFound. Something at the path that cannot be
    /// read (here a directory, which a test running as root cannot get from a
    /// permission bit) is an error of its own, so a scrub reports it as
    /// unreadable rather than missing (#628).
    #[tokio::test]
    async fn only_an_absent_file_reads_as_not_found() {
        let root = tempfile::tempdir().unwrap();
        let b = LocalFsBackend::new(root.path());
        let absent = "s3://bucket/a/absent.gz";
        assert!(matches!(
            b.get_object(absent).await,
            Err(VtopError::NotFound(_))
        ));
        let unreadable = "s3://bucket/a/dir.gz";
        std::fs::create_dir_all(b.object_path(unreadable).unwrap()).unwrap();
        for result in [
            b.get_object(unreadable).await,
            b.get_object_bounded(unreadable, 1024).await,
        ] {
            match result {
                Err(VtopError::NotFound(_)) => panic!("an unreadable path is not an absent one"),
                Err(_) => {}
                Ok(bytes) => panic!("read {} bytes from a directory", bytes.len()),
            }
        }
    }

    #[tokio::test]
    async fn writes_and_verifies() {
        let root = tempfile::tempdir().unwrap();
        let b = LocalFsBackend::new(root.path());
        let f = tmp(b"payload");
        let uri = "s3://telemetry-cef/a/b/obj.cef.gz";
        let digest = vtop_core::checksum::sha256_bytes(b"payload");
        let ck = ObjectChecksum::new("sha256", &digest);
        b.put_object(f.path(), uri, Some(ck)).await.unwrap();
        let res = b.verify_object(uri, 7, Some(ck)).await.unwrap();
        assert!(res.passed && !res.backend_limited);
        // wrong checksum fails
        let bad = b
            .verify_object(uri, 7, Some(ObjectChecksum::new("sha256", "0000")))
            .await
            .unwrap();
        assert!(!bad.passed);
    }

    #[tokio::test]
    async fn same_size_replacement_fails_even_when_sidecar_is_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let b = LocalFsBackend::new(root.path());
        let f = tmp(b"payload-A");
        let uri = "s3://telemetry-cef/a/obj.cef.gz";
        let digest = vtop_core::checksum::sha256_bytes(b"payload-A");
        let ck = ObjectChecksum::new("sha256", &digest);
        b.put_object(f.path(), uri, Some(ck)).await.unwrap();

        // Replace only the stored body. The engine-written .vtopck sidecar
        // still asserts the original digest and must not make this pass.
        tokio::fs::write(b.object_path(uri).unwrap(), b"payload-B")
            .await
            .unwrap();
        let res = b.verify_object(uri, 9, Some(ck)).await.unwrap();
        assert!(!res.passed);
        assert!(!res.backend_limited);
    }

    #[tokio::test]
    async fn rejects_path_traversal() {
        let root = tempfile::tempdir().unwrap();
        let b = LocalFsBackend::new(root.path());
        let f = tmp(b"x");
        let err = b
            .put_object(f.path(), "s3://bucket/../../etc/evil", None)
            .await
            .unwrap_err();
        assert!(matches!(err, VtopError::Upload(_)));
    }

    #[tokio::test]
    async fn disabled_checksum_is_limited() {
        let root = tempfile::tempdir().unwrap();
        let b = LocalFsBackend::new(root.path());
        let f = tmp(b"xyz");
        let uri = "s3://telemetry-raw/o";
        b.put_object(f.path(), uri, None).await.unwrap();
        let res = b.verify_object(uri, 3, None).await.unwrap();
        assert!(res.passed && res.backend_limited);
    }
}

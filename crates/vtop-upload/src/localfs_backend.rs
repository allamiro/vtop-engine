//! Local-filesystem upload backend.
//!
//! Writes telemetry objects under a local directory tree
//! (`<root>/<bucket>/<key>`), with the engine-provided checksum stored in a
//! sidecar file (`<key>.vtopck`) as an inventory hint. Verification ignores the
//! sidecar and streams the stored body through the configured digest. Useful
//! for air-gapped / offline archival and for benchmarking the pipeline with
//! real disk I/O but no object-storage service. `s3://bucket/key` URIs are
//! mapped onto the local tree.
//!
//! A write is durable before it is reported done (the commit rule needs it:
//! verification reads the bytes back, and a page-cache read passes for an
//! object a power loss would then take). The staging file is synced, renamed
//! onto the key, and then the directory holding the key is synced, as is every
//! directory the write created and the parent each of those hangs from.

use crate::base::{
    parse_s3_uri, read_bounded, verify_file_content, ObjectChecksum, ObjectHead, StoredManifest,
    StoredObject, UploadBackend, VerificationResult,
};
use async_trait::async_trait;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use vtop_core::errors::VtopError;

/// The two durability calls a write makes. They sit behind a trait so a test
/// can see that each write makes them, and in what order relative to the
/// rename; a power loss cannot be unit-tested, a missing call can.
pub(crate) trait DurabilitySync: Send + Sync {
    /// Flush the file's data and metadata to stable storage. The handle is
    /// the one the file was written through, so a file whose copied mode is
    /// read-only (a read-only source) is synced without reopening it.
    fn sync_file(&self, file: &std::fs::File, path: &Path) -> std::io::Result<()>;
    /// Flush a directory's entries (a create, a rename) to stable storage.
    fn sync_dir(&self, path: &Path) -> std::io::Result<()>;
}

/// The real calls: `fsync` the file, `fsync` the directory (a no-op where the
/// platform cannot open a directory, as on Windows).
struct OsSync;

impl DurabilitySync for OsSync {
    fn sync_file(&self, file: &std::fs::File, _path: &Path) -> std::io::Result<()> {
        file.sync_all()
    }

    fn sync_dir(&self, path: &Path) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            std::fs::File::open(path)?.sync_all()
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Ok(())
        }
    }
}

pub struct LocalFsBackend {
    root: PathBuf,
    sync: Arc<dyn DurabilitySync>,
    /// Directories this backend created whose own sync and whose parent's sync
    /// have not both succeeded yet. A directory that exists is not a directory
    /// that is durable: a sync that failed, or a concurrent writer still
    /// creating the tree, leaves it visible to the next put, which must finish
    /// the job rather than trust that it exists.
    unsynced_dirs: Arc<Mutex<HashSet<PathBuf>>>,
}

/// `create_dir_all`, then sync what it created: each new directory and the
/// parent that holds its entry. Without this a new bucket or partition
/// directory, and every object under it, can vanish after a power loss even
/// though the file inside was synced. A directory counts as done only once
/// both syncs succeeded (see `LocalFsBackend::unsynced_dirs`), so a retry after
/// a failed sync, or a put racing the writer that is creating the tree, syncs
/// the ancestors it finds already present.
fn create_dirs_durably(
    dir: &Path,
    sync: &dyn DurabilitySync,
    unsynced: &Mutex<HashSet<PathBuf>>,
) -> std::io::Result<()> {
    let mut todo = Vec::new();
    {
        let mut pending = unsynced.lock().unwrap_or_else(|p| p.into_inner());
        let mut at = Some(dir);
        while let Some(d) = at {
            if d.as_os_str().is_empty() {
                break;
            }
            if !d.exists() {
                pending.insert(d.to_path_buf());
            } else if !pending.contains(d) {
                break;
            }
            todo.push(d.to_path_buf());
            at = d.parent();
        }
    }
    std::fs::create_dir_all(dir)?;
    // Deepest first: a directory is synced before the entry that names it.
    for d in &todo {
        sync.sync_dir(d)?;
        // The parent of a relative top directory is the working directory
        // (`Path::parent` says "" for it), whose entry for `d` is the one
        // that makes a new relative root survive.
        if let Some(parent) = d.parent() {
            let parent = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            sync.sync_dir(parent)?;
        }
        unsynced.lock().unwrap_or_else(|p| p.into_inner()).remove(d);
    }
    Ok(())
}

/// Copy `local_path` beside `dest`, sync it, rename it onto `dest`, sync the
/// directory, then write the checksum sidecar (or remove a stale one) and sync
/// the directory again. Nothing is reported done before the syncs return.
fn store_blocking(
    sync: &dyn DurabilitySync,
    unsynced: &Mutex<HashSet<PathBuf>>,
    local_path: &Path,
    dest: &Path,
    checksum: Option<&str>,
) -> std::io::Result<()> {
    let parent = dest.parent().filter(|p| !p.as_os_str().is_empty());
    if let Some(parent) = parent {
        create_dirs_durably(parent, sync, unsynced)?;
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
    let publish = || -> std::io::Result<()> {
        use std::io::Write;
        let mut src = std::fs::File::open(local_path)?;
        let mode = src.metadata()?.permissions();
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        // Owner-only from the first byte: the source's own mode is applied
        // after the copy, and until then a umask-default 0644 file would let
        // another user read a 0600 object as it is written.
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut out = options.open(&staging)?;
        std::io::copy(&mut src, &mut out)?;
        out.flush()?;
        // The source's mode, as a copy keeps it, set through the handle so a
        // read-only source leaves a staging file this handle can still sync.
        out.set_permissions(mode)?;
        // The data before the rename, so the key never names unwritten bytes.
        sync.sync_file(&out, &staging)?;
        drop(out);
        std::fs::rename(&staging, dest)
    };
    if let Err(e) = publish() {
        let _ = std::fs::remove_file(&staging);
        return Err(e);
    }
    let sidecar = LocalFsBackend::checksum_sidecar(dest);
    match checksum {
        Some(c) => {
            use std::io::Write;
            let mut f = std::fs::File::create(&sidecar)?;
            f.write_all(c.as_bytes())?;
            sync.sync_file(&f, &sidecar)?;
        }
        None => {
            let _ = std::fs::remove_file(&sidecar);
        }
    }
    // The directory after the rename (and the sidecar change), so the new
    // name itself survives a power loss.
    if let Some(parent) = parent {
        sync.sync_dir(parent)?;
    }
    Ok(())
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
        Self {
            root: root.into(),
            sync: Arc::new(OsSync),
            unsynced_dirs: Arc::default(),
        }
    }

    #[cfg(test)]
    fn with_sync(root: impl Into<PathBuf>, sync: Arc<dyn DurabilitySync>) -> Self {
        Self {
            root: root.into(),
            sync,
            unsynced_dirs: Arc::default(),
        }
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
        let sync = Arc::clone(&self.sync);
        let unsynced = Arc::clone(&self.unsynced_dirs);
        let local_path = local_path.to_path_buf();
        let checksum = checksum.map(str::to_owned);
        let target = dest.clone();
        tokio::task::spawn_blocking(move || {
            store_blocking(
                sync.as_ref(),
                &unsynced,
                &local_path,
                &target,
                checksum.as_deref(),
            )
        })
        .await
        .map_err(|e| VtopError::Upload(format!("localfs write task: {e}")))??;
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
            encryption: None,
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
        let dir = self.root.join(bucket);
        let sync = Arc::clone(&self.sync);
        let unsynced = Arc::clone(&self.unsynced_dirs);
        tokio::task::spawn_blocking(move || create_dirs_durably(&dir, sync.as_ref(), &unsynced))
            .await
            .map_err(|e| VtopError::Upload(format!("localfs mkdir task: {e}")))??;
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

    /// Records every durability call with what the tree looked like at that
    /// moment, then performs it for real.
    #[derive(Default)]
    struct Recorder {
        events: std::sync::Mutex<Vec<String>>,
        watch: std::sync::Mutex<Option<PathBuf>>,
    }

    impl Recorder {
        fn log(&self, kind: &str, path: &Path) {
            let published = self
                .watch
                .lock()
                .unwrap()
                .as_ref()
                .map(|d| d.exists())
                .unwrap_or(false);
            let name = path.to_string_lossy().into_owned();
            self.events
                .lock()
                .unwrap()
                .push(format!("{kind} {name} key_exists={published}"));
        }
        fn events(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    impl DurabilitySync for Recorder {
        fn sync_file(&self, file: &std::fs::File, path: &Path) -> std::io::Result<()> {
            self.log("file", path);
            OsSync.sync_file(file, path)
        }
        fn sync_dir(&self, path: &Path) -> std::io::Result<()> {
            self.log("dir", path);
            OsSync.sync_dir(path)
        }
    }

    fn recording(root: &Path) -> (LocalFsBackend, Arc<Recorder>) {
        let rec = Arc::new(Recorder::default());
        (LocalFsBackend::with_sync(root, rec.clone()), rec)
    }

    /// An object, a manifest and every directory they create are synced
    /// before the put returns: the staging file while the key is still
    /// absent (so the key never names unwritten bytes), then the key's
    /// directory after the rename, then each created directory and its
    /// parent. Before the fix none of these calls were made, and a power loss
    /// after SOURCE_COMMITTED could take an object the ledger counted as
    /// archived.
    #[tokio::test]
    async fn a_write_syncs_the_file_then_the_directory_before_it_returns() {
        for manifest in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let (b, rec) = recording(root.path());
            let uri = "s3://bkt/y=2026/m=11/obj.cef.gz";
            let dest = b.object_path(uri).unwrap();
            *rec.watch.lock().unwrap() = Some(dest.clone());
            let f = tmp(b"payload");
            let digest = vtop_core::checksum::sha256_bytes(b"payload");
            let ck = ObjectChecksum::new("sha256", &digest);
            if manifest {
                b.put_manifest(f.path(), uri, Some(ck)).await.unwrap();
            } else {
                b.put_object(f.path(), uri, Some(ck)).await.unwrap();
            }
            let ev = rec.events();
            let dir = dest.parent().unwrap().to_string_lossy().into_owned();

            // The staging file was synced while the key did not yet exist.
            let staged = ev
                .iter()
                .position(|e| e.starts_with("file ") && e.contains(".partial"))
                .unwrap_or_else(|| panic!("staging file never synced: {ev:?}"));
            assert!(ev[staged].ends_with("key_exists=false"), "{ev:?}");
            // The key's directory was synced after the rename, last of all.
            let last = ev.last().unwrap();
            assert!(
                last.starts_with(&format!("dir {dir} ")) && last.ends_with("key_exists=true"),
                "the key's directory is not synced after the rename: {ev:?}"
            );
            // The sidecar is synced too.
            assert!(ev.iter().any(|e| e.contains(".vtopck")), "{ev:?}");
            // Every directory the write created, and the root it hangs from.
            for d in [
                root.path().join("bkt"),
                root.path().join("bkt/y=2026"),
                root.path().join("bkt/y=2026/m=11"),
                root.path().to_path_buf(),
            ] {
                let want = format!("dir {} ", d.display());
                assert!(
                    ev.iter().any(|e| e.starts_with(&want)),
                    "{} not synced: {ev:?}",
                    d.display()
                );
            }
        }
    }

    /// A failing sync is a failing write: nothing is reported done past an
    /// fsync that did not succeed.
    #[tokio::test]
    async fn a_failed_sync_fails_the_write() {
        struct Failing;
        impl DurabilitySync for Failing {
            fn sync_file(&self, _: &std::fs::File, _: &Path) -> std::io::Result<()> {
                Err(std::io::Error::other("fsync failed"))
            }
            fn sync_dir(&self, _: &Path) -> std::io::Result<()> {
                Ok(())
            }
        }
        let root = tempfile::tempdir().unwrap();
        let b = LocalFsBackend::with_sync(root.path(), Arc::new(Failing));
        let f = tmp(b"payload");
        let uri = "s3://bkt/obj.gz";
        assert!(b.put_object(f.path(), uri, None).await.is_err());
        // Neither the key nor a staging file is left behind.
        assert!(!b.object_path(uri).unwrap().exists());
        let left: Vec<_> = std::fs::read_dir(root.path().join("bkt"))
            .unwrap()
            .collect();
        assert!(left.is_empty(), "{left:?}");
    }

    /// `ensure_bucket` syncs the directory it creates and the parent.
    #[tokio::test]
    async fn ensure_bucket_syncs_what_it_creates() {
        let root = tempfile::tempdir().unwrap();
        let (b, rec) = recording(root.path());
        b.ensure_bucket("fresh").await.unwrap();
        let ev = rec.events();
        for d in [root.path().join("fresh"), root.path().to_path_buf()] {
            let want = format!("dir {} ", d.display());
            assert!(ev.iter().any(|e| e.starts_with(&want)), "{ev:?}");
        }
    }

    /// A directory that exists is not a directory that is durable. When the
    /// first put's directory sync fails, the directories it created stay
    /// visible, so the retry must still sync them and the parents that name
    /// them, not stop at the first ancestor that exists.
    #[tokio::test]
    async fn a_retry_after_a_failed_directory_sync_still_syncs_the_ancestors() {
        struct FailsOnce {
            failed: std::sync::atomic::AtomicBool,
            dirs: Mutex<Vec<PathBuf>>,
        }
        impl DurabilitySync for FailsOnce {
            fn sync_file(&self, file: &std::fs::File, _: &Path) -> std::io::Result<()> {
                file.sync_all()
            }
            fn sync_dir(&self, path: &Path) -> std::io::Result<()> {
                if !self.failed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    return Err(std::io::Error::other("dir fsync failed"));
                }
                self.dirs.lock().unwrap().push(path.to_path_buf());
                Ok(())
            }
        }
        let root = tempfile::tempdir().unwrap();
        let sync = Arc::new(FailsOnce {
            failed: Default::default(),
            dirs: Default::default(),
        });
        let b = LocalFsBackend::with_sync(root.path(), sync.clone());
        let f = tmp(b"payload");
        let uri = "s3://bkt/y=2026/obj.gz";
        assert!(b.put_object(f.path(), uri, None).await.is_err());
        b.put_object(f.path(), uri, None).await.unwrap();
        let dirs = sync.dirs.lock().unwrap().clone();
        for d in [
            root.path().join("bkt/y=2026"),
            root.path().join("bkt"),
            root.path().to_path_buf(),
        ] {
            assert!(
                dirs.contains(&d),
                "{} not synced on retry: {dirs:?}",
                d.display()
            );
        }
    }

    /// A read-only source leaves a read-only staging file, as a copy keeps the
    /// mode. The put still works and syncs it (the sync goes through the
    /// handle the file was written with, not a reopen for writing).
    #[cfg(unix)]
    #[tokio::test]
    async fn a_read_only_source_is_still_uploaded_and_synced() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let (b, rec) = recording(root.path());
        let f = tmp(b"payload");
        std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(0o444)).unwrap();
        let uri = "s3://bkt/ro.gz";
        b.put_object(f.path(), uri, None).await.unwrap();
        assert_eq!(
            std::fs::read(b.object_path(uri).unwrap()).unwrap(),
            b"payload"
        );
        assert!(rec.events().iter().any(|e| e.starts_with("file ")));
    }

    /// The stored object has the source's mode, as a copy keeps it (the
    /// staging file starts owner-only and takes the source's mode through its
    /// handle, so a 0600 source is never exposed as a 0644 file mid-copy).
    #[cfg(unix)]
    #[tokio::test]
    async fn the_stored_object_keeps_the_source_mode() {
        use std::os::unix::fs::PermissionsExt;
        for mode in [0o600, 0o640, 0o444] {
            let root = tempfile::tempdir().unwrap();
            let b = LocalFsBackend::new(root.path());
            let f = tmp(b"payload");
            std::fs::set_permissions(f.path(), std::fs::Permissions::from_mode(mode)).unwrap();
            let uri = "s3://bkt/mode.gz";
            b.put_object(f.path(), uri, None).await.unwrap();
            let got = std::fs::metadata(b.object_path(uri).unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(got, mode);
        }
    }

    /// A new relative root (`upload.local_path: archive`) is named by an entry
    /// in the working directory; that directory is synced too, not skipped
    /// because `Path::parent` of `archive` is empty.
    #[tokio::test]
    async fn a_new_relative_root_syncs_the_working_directory() {
        struct Cleanup(PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let name = PathBuf::from(format!("vtop-relroot-test-{}", std::process::id()));
        let _cleanup = Cleanup(name.clone());
        let rec = Recorder::default();
        let unsynced = Mutex::default();
        create_dirs_durably(&name.join("bkt"), &rec, &unsynced).unwrap();
        let ev = rec.events();
        for want in [format!("dir {} ", name.display()), "dir . ".to_string()] {
            assert!(
                ev.iter().any(|e| e.starts_with(&want)),
                "{want:?} not in {ev:?}"
            );
        }
    }
}

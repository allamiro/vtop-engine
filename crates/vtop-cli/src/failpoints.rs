//! Failpoints: kill the engine with `SIGKILL` at a named line (#725).
//!
//! The engine crash sweep (`scripts/live-chaos/engine/run-sweep.sh`) needs to
//! stop a real `vtopctl run` between two steps of the archive protocol, at a
//! line it chooses, and then restart it on the same ledger, source and bucket.
//! A timed external `kill -9` cannot choose the line. A failpoint can: with
//! `VTOP_FAILPOINT=<point>` (or `<point>:<n>` for the nth time the line is
//! reached) the process sends itself `SIGKILL` there. No destructor runs and
//! nothing is flushed, which is what an operator's `kill -9` or the kernel's
//! OOM killer leaves behind.
//!
//! **A production binary cannot kill itself.** Everything that reads
//! `VTOP_FAILPOINT` or sends the signal is behind the cargo feature
//! `failpoints`, which is off by default and enabled by no release build, the
//! container image or the Helm chart (`scripts/check-failpoints-absent.sh`
//! checks that). Without the feature [`hit`] is an empty inline function and
//! [`arm_from_env`] does not look at the environment.
//!
//! With the feature, an unknown point name refuses to start and lists the
//! valid ones, so a typo cannot run a "sweep" in which nothing was killed.
//!
//! The point names are constants: an engine call site names one with
//! `failpoints::hit(failpoints::BEFORE_SOURCE_COMMIT)`, so a misspelt point is a
//! compile error. [`POINTS`] lists every one of them; the sweep reads that list
//! from the binary (the refusal message) and fails if it did not run every
//! entry, so a point added without a leg cannot go unnoticed.

/// The row's first ledger write (BATCHING), before and after.
pub const BEFORE_INSERT: &str = "before_insert";
pub const AFTER_INSERT: &str = "after_insert";
pub const BEFORE_LEDGER_SEALED: &str = "before_ledger_sealed";
pub const AFTER_LEDGER_SEALED: &str = "after_ledger_sealed";
pub const BEFORE_LEDGER_COMPRESSED: &str = "before_ledger_compressed";
pub const AFTER_LEDGER_COMPRESSED: &str = "after_ledger_compressed";
pub const BEFORE_LEDGER_CHECKSUMMED: &str = "before_ledger_checksummed";
pub const AFTER_LEDGER_CHECKSUMMED: &str = "after_ledger_checksummed";
/// Inside a multipart upload: one part is stored, the upload is not complete.
pub const MID_OBJECT_PUT: &str = "mid_object_put";
/// The object is in the store; the ledger does not say so yet.
pub const AFTER_OBJECT_PUT_BEFORE_LEDGER: &str = "after_object_put_before_ledger";
pub const BEFORE_LEDGER_OBJECT_UPLOADED: &str = "before_ledger_object_uploaded";
pub const AFTER_LEDGER_OBJECT_UPLOADED: &str = "after_ledger_object_uploaded";
/// The manifest is in the store; the ledger does not say so yet.
pub const AFTER_MANIFEST_PUT_BEFORE_LEDGER: &str = "after_manifest_put_before_ledger";
pub const BEFORE_LEDGER_MANIFEST_UPLOADED: &str = "before_ledger_manifest_uploaded";
pub const AFTER_LEDGER_MANIFEST_UPLOADED: &str = "after_ledger_manifest_uploaded";
pub const BEFORE_LEDGER_VERIFIED: &str = "before_ledger_verified";
pub const AFTER_LEDGER_VERIFIED: &str = "after_ledger_verified";
/// The commit boundary: VERIFIED is in the ledger, the source has not moved.
pub const BEFORE_SOURCE_COMMIT: &str = "before_source_commit";
/// The commit boundary: the source has moved, SOURCE_COMMITTED is not written.
pub const AFTER_SOURCE_COMMIT_BEFORE_LEDGER: &str = "after_source_commit_before_ledger";
pub const BEFORE_LEDGER_SOURCE_COMMITTED: &str = "before_ledger_source_committed";
pub const AFTER_LEDGER_SOURCE_COMMITTED: &str = "after_ledger_source_committed";

/// Every failpoint, in the order a batch reaches them.
pub const POINTS: &[&str] = &[
    BEFORE_INSERT,
    AFTER_INSERT,
    BEFORE_LEDGER_SEALED,
    AFTER_LEDGER_SEALED,
    BEFORE_LEDGER_COMPRESSED,
    AFTER_LEDGER_COMPRESSED,
    BEFORE_LEDGER_CHECKSUMMED,
    AFTER_LEDGER_CHECKSUMMED,
    MID_OBJECT_PUT,
    AFTER_OBJECT_PUT_BEFORE_LEDGER,
    BEFORE_LEDGER_OBJECT_UPLOADED,
    AFTER_LEDGER_OBJECT_UPLOADED,
    AFTER_MANIFEST_PUT_BEFORE_LEDGER,
    BEFORE_LEDGER_MANIFEST_UPLOADED,
    AFTER_LEDGER_MANIFEST_UPLOADED,
    BEFORE_LEDGER_VERIFIED,
    AFTER_LEDGER_VERIFIED,
    BEFORE_SOURCE_COMMIT,
    AFTER_SOURCE_COMMIT_BEFORE_LEDGER,
    BEFORE_LEDGER_SOURCE_COMMITTED,
    AFTER_LEDGER_SOURCE_COMMITTED,
];

/// The points around a ledger transition to `state`, `(before, after)`, for
/// the states a batch passes through on its way to SOURCE_COMMITTED. `None`
/// for the states only a failure reaches (FAILED, REPLAY_REQUIRED,
/// SUPERSEDED) and for the ones no transition writes: a sweep that armed
/// one would never see it fire.
// `allow`, not `expect`: without the feature only the unit tests call this,
// so the lint fires in a plain build and not in a test build, and an
// `expect` would be unfulfilled in the second.
#[cfg_attr(not(feature = "failpoints"), allow(dead_code))]
pub(crate) fn ledger_points(state: vtop_core::BatchState) -> Option<(&'static str, &'static str)> {
    use vtop_core::BatchState as S;
    match state {
        S::Sealed => Some((BEFORE_LEDGER_SEALED, AFTER_LEDGER_SEALED)),
        S::Compressed => Some((BEFORE_LEDGER_COMPRESSED, AFTER_LEDGER_COMPRESSED)),
        S::Checksummed => Some((BEFORE_LEDGER_CHECKSUMMED, AFTER_LEDGER_CHECKSUMMED)),
        S::ObjectUploaded => Some((BEFORE_LEDGER_OBJECT_UPLOADED, AFTER_LEDGER_OBJECT_UPLOADED)),
        S::ManifestUploaded => Some((
            BEFORE_LEDGER_MANIFEST_UPLOADED,
            AFTER_LEDGER_MANIFEST_UPLOADED,
        )),
        S::Verified => Some((BEFORE_LEDGER_VERIFIED, AFTER_LEDGER_VERIFIED)),
        S::SourceCommitted => Some((
            BEFORE_LEDGER_SOURCE_COMMITTED,
            AFTER_LEDGER_SOURCE_COMMITTED,
        )),
        S::Discovered | S::Batching | S::Failed | S::ReplayRequired | S::Superseded => None,
    }
}

/// The environment variable that arms a failpoint in a `failpoints` build.
pub const ENV: &str = "VTOP_FAILPOINT";

/// What `VTOP_FAILPOINT` asked for: kill at the `nth` time `point` is reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    pub point: &'static str,
    pub nth: u64,
}

/// Parse a `VTOP_FAILPOINT` value: `<point>` or `<point>:<n>`, `n >= 1`. The
/// error lists every valid point, one per line, so the sweep can read the
/// list from the binary it is about to run.
pub fn parse(value: &str) -> Result<Spec, String> {
    let (name, nth) = match value.split_once(':') {
        None => (value, 1),
        Some((name, n)) => match n.parse::<u64>() {
            Ok(n) if n >= 1 => (name, n),
            _ => {
                return Err(format!(
                    "{ENV}={value:?}: the hit count after ':' must be a whole number from 1, \
                     got {n:?}"
                ))
            }
        },
    };
    match POINTS.iter().find(|p| **p == name) {
        Some(point) => Ok(Spec { point, nth }),
        None => {
            let mut message = format!(
                "{ENV}={value:?} names no failpoint this build knows, so the engine refuses \
                 to start (a sweep in which nothing is killed would prove nothing). Set it to \
                 one of these, optionally followed by :<n> for the nth hit:"
            );
            for p in POINTS {
                message.push_str("\n  ");
                message.push_str(p);
            }
            Err(message)
        }
    }
}

/// Arm the failpoint `VTOP_FAILPOINT` names, once per process, before the
/// engine opens anything. Without the `failpoints` feature this does nothing
/// and does not read the environment.
pub fn arm_from_env() -> Result<(), vtop_core::errors::VtopError> {
    #[cfg(feature = "failpoints")]
    {
        imp::arm(std::env::var(ENV).ok().as_deref()).map_err(vtop_core::errors::VtopError::Config)
    }
    #[cfg(not(feature = "failpoints"))]
    {
        Ok(())
    }
}

/// The engine reached `point`. In a `failpoints` build whose armed point this
/// is, on its armed hit, the process sends itself `SIGKILL` and never
/// returns. Otherwise nothing happens.
#[inline(always)]
pub fn hit(point: &'static str) {
    #[cfg(feature = "failpoints")]
    imp::hit(point);
    #[cfg(not(feature = "failpoints"))]
    let _ = point;
}

#[cfg(feature = "failpoints")]
mod imp {
    use super::{parse, Spec};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::OnceLock;

    struct Armed {
        spec: Spec,
        seen: AtomicU64,
    }

    static ARMED: OnceLock<Option<Armed>> = OnceLock::new();

    pub(super) fn arm(value: Option<&str>) -> Result<(), String> {
        let armed = match value {
            None | Some("") => None,
            Some(v) => Some(parse(v)?),
        };
        if let Some(spec) = &armed {
            tracing::warn!(
                point = spec.point,
                nth = spec.nth,
                "failpoint {} is armed for hit {}: this engine kills itself with SIGKILL when it \
                 gets there. This build has the failpoints feature, which exists for the crash \
                 sweep; it must never be deployed",
                spec.point,
                spec.nth,
            );
        }
        // First arm wins: one process, one failpoint.
        let _ = ARMED.set(armed.map(|spec| Armed {
            spec,
            seen: AtomicU64::new(0),
        }));
        Ok(())
    }

    pub(super) fn hit(point: &'static str) {
        let Some(Some(armed)) = ARMED.get() else {
            return;
        };
        if armed.spec.point != point {
            return;
        }
        let n = armed.seen.fetch_add(1, Ordering::SeqCst) + 1;
        if n != armed.spec.nth {
            return;
        }
        // stderr is written synchronously, so this line is out before the
        // signal; the sweep reads it to tell a failpoint kill from a crash.
        tracing::warn!(
            point,
            hit = n,
            "failpoint {point} fired: the process is being killed here on purpose with SIGKILL. \
             This build has the failpoints feature and must not be deployed",
        );
        kill_self();
    }

    #[cfg(unix)]
    fn kill_self() -> ! {
        use rustix::process::{getpid, kill_process, Signal};
        let _ = kill_process(getpid(), Signal::KILL);
        // SIGKILL cannot be caught; the process is gone before this runs.
        loop {
            std::thread::park();
        }
    }

    #[cfg(not(unix))]
    fn kill_self() -> ! {
        std::process::abort()
    }
}

/// The decorators that place the failpoints around the effects the engine
/// does not call directly: every ledger write, wherever it is made, and each
/// multipart part. Installed by the engine only in a `failpoints` build.
#[cfg(feature = "failpoints")]
pub(crate) mod wrap {
    use super::{hit, ledger_points, AFTER_INSERT, BEFORE_INSERT, MID_OBJECT_PUT};
    use std::path::Path;
    use std::sync::Arc;
    use vtop_core::errors::VtopError;
    use vtop_core::types::SourceType;
    use vtop_core::BatchState;
    use vtop_state::{
        BatchPatch, BatchRecord, CommittedCursor, CommittedKafkaOffset, StateStore,
        VerificationClass,
    };
    use vtop_upload::{
        BucketFeature, CreatedUpload, ObjectChecksum, ObjectHead, ObjectLockPolicy, RetentionCheck,
        RetentionExpectation, StoredManifest, StoredObject, UploadBackend, UploadedPart,
        VerificationResult,
    };

    /// The state store, with `before_*`/`after_*` around each write that
    /// moves a batch towards SOURCE_COMMITTED. Wrapping the store rather
    /// than each call site covers every path that writes a transition -
    /// the pipeline, recovery and the settle pass alike - and a state added
    /// later is covered once [`ledger_points`] names it.
    pub(crate) struct Ledger(pub(crate) Box<dyn StateStore>);

    async fn around<T, F>(state: BatchState, write: F) -> Result<T, VtopError>
    where
        F: std::future::Future<Output = Result<T, VtopError>>,
    {
        let points = ledger_points(state);
        if let Some((before, _)) = points {
            hit(before);
        }
        let out = write.await?;
        if let Some((_, after)) = points {
            hit(after);
        }
        Ok(out)
    }

    #[async_trait::async_trait]
    impl StateStore for Ledger {
        async fn save_batch_state(&self, rec: &BatchRecord) -> Result<(), VtopError> {
            hit(BEFORE_INSERT);
            self.0.save_batch_state(rec).await?;
            hit(AFTER_INSERT);
            Ok(())
        }
        async fn update_batch_state(
            &self,
            batch_id: &str,
            to: BatchState,
            patch: &BatchPatch,
        ) -> Result<(), VtopError> {
            around(to, self.0.update_batch_state(batch_id, to, patch)).await
        }
        async fn get_batch(&self, batch_id: &str) -> Result<Option<BatchRecord>, VtopError> {
            self.0.get_batch(batch_id).await
        }
        async fn claim_incomplete_batches(
            &self,
            owner: &str,
            now: &str,
            lease_until: &str,
        ) -> Result<Vec<BatchRecord>, VtopError> {
            self.0
                .claim_incomplete_batches(owner, now, lease_until)
                .await
        }
        async fn renew_claim(&self, batch_id: &str, lease_until: &str) -> Result<bool, VtopError> {
            self.0.renew_claim(batch_id, lease_until).await
        }
        async fn max_committed_end_bytes(
            &self,
            source_type: SourceType,
        ) -> Result<Vec<(String, u64)>, VtopError> {
            self.0.max_committed_end_bytes(source_type).await
        }
        async fn committed_cursors(
            &self,
            source_type: SourceType,
        ) -> Result<Vec<CommittedCursor>, VtopError> {
            self.0.committed_cursors(source_type).await
        }
        async fn committed_kafka_offsets(&self) -> Result<Vec<CommittedKafkaOffset>, VtopError> {
            self.0.committed_kafka_offsets().await
        }
        async fn committed_cursors_by_file(
            &self,
            source_type: SourceType,
        ) -> Result<Vec<CommittedCursor>, VtopError> {
            self.0.committed_cursors_by_file(source_type).await
        }
        async fn list_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
            self.0.list_batches().await
        }
        async fn list_incomplete_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
            self.0.list_incomplete_batches().await
        }
        async fn list_failed_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
            self.0.list_failed_batches().await
        }
        async fn mark_failed(&self, batch_id: &str, message: &str) -> Result<(), VtopError> {
            self.0.mark_failed(batch_id, message).await
        }
        async fn mark_verified(
            &self,
            batch_id: &str,
            class: VerificationClass,
        ) -> Result<(), VtopError> {
            around(BatchState::Verified, self.0.mark_verified(batch_id, class)).await
        }
        async fn mark_source_committed(&self, batch_id: &str) -> Result<(), VtopError> {
            around(
                BatchState::SourceCommitted,
                self.0.mark_source_committed(batch_id),
            )
            .await
        }
        async fn prune_committed_history(
            &self,
            older_than: &str,
            limit: u32,
        ) -> Result<u64, VtopError> {
            self.0.prune_committed_history(older_than, limit).await
        }
        async fn prune_candidates(
            &self,
            older_than: &str,
            limit: u32,
        ) -> Result<Vec<BatchRecord>, VtopError> {
            self.0.prune_candidates(older_than, limit).await
        }
        async fn prune_listed(
            &self,
            older_than: &str,
            batch_ids: &[String],
        ) -> Result<u64, VtopError> {
            self.0.prune_listed(older_than, batch_ids).await
        }
        async fn append_audit(
            &self,
            row: &vtop_state::AuditInsert,
            keyring: &vtop_core::manifest::ManifestKeyring,
        ) -> Result<vtop_state::AuditRow, VtopError> {
            self.0.append_audit(row, keyring).await
        }
        async fn list_audit(
            &self,
            after_seq: i64,
            limit: u32,
        ) -> Result<Vec<vtop_state::AuditRow>, VtopError> {
            self.0.list_audit(after_seq, limit).await
        }
    }

    /// The upload backend, with `mid_object_put` after each stored part of
    /// a multipart upload: the store holds a part, the upload is not
    /// complete, and the session file in the work directory says which. Every
    /// other method is passed through unchanged, defaults included, so the
    /// engine sees the backend's own answers.
    pub(crate) struct Backend(pub(crate) Arc<dyn UploadBackend>);

    #[async_trait::async_trait]
    impl UploadBackend for Backend {
        async fn put_object(
            &self,
            local_path: &Path,
            object_uri: &str,
            checksum: Option<ObjectChecksum<'_>>,
        ) -> Result<StoredObject, VtopError> {
            self.0.put_object(local_path, object_uri, checksum).await
        }
        async fn put_manifest(
            &self,
            local_path: &Path,
            manifest_uri: &str,
            checksum: Option<ObjectChecksum<'_>>,
        ) -> Result<StoredManifest, VtopError> {
            self.0
                .put_manifest(local_path, manifest_uri, checksum)
                .await
        }
        async fn get_manifest_pinned(
            &self,
            manifest_uri: &str,
            version_id: &str,
            max_bytes: usize,
        ) -> Result<Vec<u8>, VtopError> {
            self.0
                .get_manifest_pinned(manifest_uri, version_id, max_bytes)
                .await
        }
        fn supports_object_versions(&self) -> bool {
            self.0.supports_object_versions()
        }
        async fn bucket_versioning(&self, bucket: &str) -> Result<BucketFeature, VtopError> {
            self.0.bucket_versioning(bucket).await
        }
        fn object_lock(&self) -> Option<ObjectLockPolicy> {
            self.0.object_lock()
        }
        async fn bucket_object_lock(&self, bucket: &str) -> Result<BucketFeature, VtopError> {
            self.0.bucket_object_lock(bucket).await
        }
        async fn verify_retention(
            &self,
            object_uri: &str,
            version_id: Option<&str>,
            expected: &RetentionExpectation,
        ) -> Result<RetentionCheck, VtopError> {
            self.0
                .verify_retention(object_uri, version_id, expected)
                .await
        }
        async fn head_object(&self, object_uri: &str) -> Result<ObjectHead, VtopError> {
            self.0.head_object(object_uri).await
        }
        fn reports_absent_objects(&self) -> bool {
            self.0.reports_absent_objects()
        }
        async fn get_object(&self, object_uri: &str) -> Result<Vec<u8>, VtopError> {
            self.0.get_object(object_uri).await
        }
        async fn get_object_bounded(
            &self,
            object_uri: &str,
            max_bytes: usize,
        ) -> Result<Vec<u8>, VtopError> {
            self.0.get_object_bounded(object_uri, max_bytes).await
        }
        async fn verify_object(
            &self,
            object_uri: &str,
            expected_size: u64,
            expected: Option<ObjectChecksum<'_>>,
        ) -> Result<VerificationResult, VtopError> {
            self.0
                .verify_object(object_uri, expected_size, expected)
                .await
        }
        async fn delete_object(&self, object_uri: &str) -> Result<(), VtopError> {
            self.0.delete_object(object_uri).await
        }
        async fn get_object_pinned(
            &self,
            object_uri: &str,
            version_id: &str,
            max_bytes: usize,
        ) -> Result<Vec<u8>, VtopError> {
            self.0
                .get_object_pinned(object_uri, version_id, max_bytes)
                .await
        }
        async fn ensure_bucket(&self, bucket: &str) -> Result<(), VtopError> {
            self.0.ensure_bucket(bucket).await
        }
        fn backend_name(&self) -> &'static str {
            self.0.backend_name()
        }
        fn supports_checksum_verification(&self) -> bool {
            self.0.supports_checksum_verification()
        }
        fn supports_multipart(&self) -> bool {
            self.0.supports_multipart()
        }
        fn min_part_size_bytes(&self) -> u64 {
            self.0.min_part_size_bytes()
        }
        async fn create_multipart_upload(
            &self,
            object_uri: &str,
            content_type: &str,
            checksum: Option<ObjectChecksum<'_>>,
        ) -> Result<CreatedUpload, VtopError> {
            self.0
                .create_multipart_upload(object_uri, content_type, checksum)
                .await
        }
        async fn upload_part(
            &self,
            object_uri: &str,
            upload_id: &str,
            part_number: u32,
            data: bytes::Bytes,
        ) -> Result<UploadedPart, VtopError> {
            let part = self
                .0
                .upload_part(object_uri, upload_id, part_number, data)
                .await?;
            hit(MID_OBJECT_PUT);
            Ok(part)
        }
        async fn complete_multipart_upload(
            &self,
            object_uri: &str,
            upload_id: &str,
            parts: &[UploadedPart],
        ) -> Result<StoredObject, VtopError> {
            self.0
                .complete_multipart_upload(object_uri, upload_id, parts)
                .await
        }
        async fn abort_multipart_upload(
            &self,
            object_uri: &str,
            upload_id: &str,
        ) -> Result<(), VtopError> {
            self.0.abort_multipart_upload(object_uri, upload_id).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vtop_core::BatchState;

    #[test]
    fn every_point_is_listed_once() {
        let mut seen = std::collections::HashSet::new();
        for p in POINTS {
            assert!(seen.insert(*p), "{p} is listed twice");
            assert!(
                p.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{p}: the sweep parses names as [a-z_]+"
            );
        }
    }

    #[test]
    fn every_ledger_point_is_a_listed_point() {
        for state in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
            BatchState::Verified,
            BatchState::SourceCommitted,
        ] {
            let (before, after) = ledger_points(state).unwrap();
            assert!(POINTS.contains(&before), "{before}");
            assert!(POINTS.contains(&after), "{after}");
        }
    }

    #[test]
    fn a_point_parses_with_and_without_a_hit_count() {
        assert_eq!(
            parse("before_source_commit").unwrap(),
            Spec {
                point: BEFORE_SOURCE_COMMIT,
                nth: 1
            }
        );
        assert_eq!(
            parse("after_ledger_sealed:3").unwrap(),
            Spec {
                point: AFTER_LEDGER_SEALED,
                nth: 3
            }
        );
        assert!(parse("after_ledger_sealed:0").is_err());
        assert!(parse("after_ledger_sealed:x").is_err());
    }

    #[test]
    fn an_unknown_point_is_refused_with_every_valid_one() {
        let err = parse("before_sorce_commit").unwrap_err();
        let listed: Vec<&str> = err.lines().filter_map(|l| l.strip_prefix("  ")).collect();
        assert_eq!(listed, POINTS);
    }

    /// F1: without the feature the hook does nothing and the variable is not
    /// read, whatever it holds.
    #[cfg(not(feature = "failpoints"))]
    #[test]
    fn without_the_feature_nothing_is_armed_and_nothing_is_killed() {
        // Not set here (tests share the process environment): `arm_from_env`
        // compiles to `Ok(())` without the feature, so there is nothing for
        // a value to change.
        assert!(arm_from_env().is_ok());
        for p in POINTS {
            hit(p);
        }
    }
}

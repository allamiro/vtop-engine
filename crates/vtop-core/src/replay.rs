//! Replay / crash-recovery decision logic.
//!
//! This module is protocol-independent: it maps a persisted [`BatchState`] to
//! the recovery action the engine *could* take.
//!
//! NOTE on current engine behavior: the fine-grained `Retry*` actions
//! ([`RecoveryAction::RetryCompression`], `RetryChecksum`, `RetryObjectUpload`,
//! `RetryManifestUpload`, `RetryVerification`) describe the ideal incremental
//! resume for each intermediate state, but `Engine::recover()` does **not**
//! resume from a half-produced local object today. It handles only two cases
//! specially — `Verified` → retry the source commit, and `SourceCommitted` →
//! nothing — and treats **every other** non-committed state as
//! [`RecoveryAction::ReplayFromSource`] (mark `REPLAY_REQUIRED` and re-read from
//! the last committed source position). The `Retry*` variants are therefore
//! reserved for future granular recovery. Either way the invariant holds.
//!
//! Invariant: source progress is NEVER advanced for an unverified batch.

use crate::state_machine::BatchState;

/// The action the engine should take to recover an incomplete batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    /// Re-run compression (object not yet produced).
    RetryCompression,
    /// Compute the object checksum.
    RetryChecksum,
    /// Re-upload the compressed object.
    RetryObjectUpload,
    /// Re-build and re-upload the manifest.
    RetryManifestUpload,
    /// Re-run verification; on failure the batch becomes REPLAY_REQUIRED.
    RetryVerification,
    /// Object & manifest are verified but progress was never committed —
    /// safe to retry the source commit.
    RetrySourceCommit,
    /// The batch failed and must be replayed from its source marker.
    ReplayFromSource,
    /// Nothing to do: the batch is already fully committed.
    None,
}

/// Decide the recovery action for a batch found in `state` at startup.
///
/// ```text
/// SEALED            -> RetryCompression
/// COMPRESSED        -> RetryChecksum
/// CHECKSUMMED       -> RetryObjectUpload
/// OBJECT_UPLOADED   -> RetryManifestUpload
/// MANIFEST_UPLOADED -> RetryVerification
/// VERIFIED          -> RetrySourceCommit     (verified but not committed)
/// FAILED            -> ReplayFromSource
/// REPLAY_REQUIRED   -> ReplayFromSource
/// DISCOVERED/BATCHING -> ReplayFromSource    (no durable object yet)
/// SOURCE_COMMITTED  -> None
/// ```
pub fn next_recovery_action(state: BatchState) -> RecoveryAction {
    use BatchState::*;
    match state {
        Discovered | Batching => RecoveryAction::ReplayFromSource,
        Sealed => RecoveryAction::RetryCompression,
        Compressed => RecoveryAction::RetryChecksum,
        Checksummed => RecoveryAction::RetryObjectUpload,
        ObjectUploaded => RecoveryAction::RetryManifestUpload,
        ManifestUploaded => RecoveryAction::RetryVerification,
        Verified => RecoveryAction::RetrySourceCommit,
        Failed | ReplayRequired => RecoveryAction::ReplayFromSource,
        SourceCommitted => RecoveryAction::None,
    }
}

/// True if a batch in this state still holds uncommitted source progress that
/// must remain replayable (i.e. the source offset must NOT be advanced).
pub fn requires_replay_protection(state: BatchState) -> bool {
    !matches!(state, BatchState::SourceCommitted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use BatchState::*;

    #[test]
    fn incomplete_states_map_to_retry_steps() {
        assert_eq!(
            next_recovery_action(Sealed),
            RecoveryAction::RetryCompression
        );
        assert_eq!(
            next_recovery_action(Compressed),
            RecoveryAction::RetryChecksum
        );
        assert_eq!(
            next_recovery_action(Checksummed),
            RecoveryAction::RetryObjectUpload
        );
        assert_eq!(
            next_recovery_action(ObjectUploaded),
            RecoveryAction::RetryManifestUpload
        );
        assert_eq!(
            next_recovery_action(ManifestUploaded),
            RecoveryAction::RetryVerification
        );
    }

    #[test]
    fn verified_but_not_committed_retries_commit() {
        assert_eq!(
            next_recovery_action(Verified),
            RecoveryAction::RetrySourceCommit
        );
    }

    #[test]
    fn committed_needs_nothing() {
        assert_eq!(next_recovery_action(SourceCommitted), RecoveryAction::None);
        assert!(!requires_replay_protection(SourceCommitted));
    }

    #[test]
    fn everything_uncommitted_is_replay_protected() {
        for s in [
            Discovered,
            Batching,
            Sealed,
            Compressed,
            Checksummed,
            ObjectUploaded,
            ManifestUploaded,
            Verified,
            Failed,
            ReplayRequired,
        ] {
            assert!(requires_replay_protection(s), "{s:?} must be protected");
        }
    }
}

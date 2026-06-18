//! Explicit, strongly-typed batch state machine.
//!
//! The central safety rule of the VTOP Engine is enforced *here*, in code:
//!
//! ```text
//! SOURCE_COMMITTED is forbidden until VERIFIED is true.
//! ```
//!
//! A source progress marker (Kafka offset, file byte offset, syslog spool
//! offset) MUST NOT be committed until the batch reaches
//! [`BatchState::Verified`]. The only legal predecessor of
//! [`BatchState::SourceCommitted`] is [`BatchState::Verified`].

use crate::errors::VtopError;
use serde::{Deserialize, Serialize};

/// The lifecycle state of a telemetry batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchState {
    Discovered,
    Batching,
    Sealed,
    Compressed,
    Checksummed,
    ObjectUploaded,
    ManifestUploaded,
    Verified,
    SourceCommitted,
    Failed,
    ReplayRequired,
}

impl BatchState {
    /// Stable lowercase snake_case string used in the state store and manifest.
    pub fn as_str(&self) -> &'static str {
        match self {
            BatchState::Discovered => "discovered",
            BatchState::Batching => "batching",
            BatchState::Sealed => "sealed",
            BatchState::Compressed => "compressed",
            BatchState::Checksummed => "checksummed",
            BatchState::ObjectUploaded => "object_uploaded",
            BatchState::ManifestUploaded => "manifest_uploaded",
            BatchState::Verified => "verified",
            BatchState::SourceCommitted => "source_committed",
            BatchState::Failed => "failed",
            BatchState::ReplayRequired => "replay_required",
        }
    }

    /// Whether the source progress for this batch is safe to commit.
    pub fn is_committable(&self) -> bool {
        matches!(self, BatchState::Verified)
    }

    /// Whether this is a terminal success state.
    pub fn is_committed(&self) -> bool {
        matches!(self, BatchState::SourceCommitted)
    }
}

impl std::str::FromStr for BatchState {
    type Err = VtopError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "discovered" => BatchState::Discovered,
            "batching" => BatchState::Batching,
            "sealed" => BatchState::Sealed,
            "compressed" => BatchState::Compressed,
            "checksummed" => BatchState::Checksummed,
            "object_uploaded" => BatchState::ObjectUploaded,
            "manifest_uploaded" => BatchState::ManifestUploaded,
            "verified" => BatchState::Verified,
            "source_committed" => BatchState::SourceCommitted,
            "failed" => BatchState::Failed,
            "replay_required" => BatchState::ReplayRequired,
            other => return Err(VtopError::Other(format!("unknown batch state: {other}"))),
        })
    }
}

impl std::fmt::Display for BatchState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Returns `true` if `from -> to` is a legal transition.
///
/// Legal transitions:
/// ```text
/// DISCOVERED        -> BATCHING
/// BATCHING          -> SEALED
/// SEALED            -> COMPRESSED
/// COMPRESSED        -> CHECKSUMMED
/// CHECKSUMMED       -> OBJECT_UPLOADED
/// OBJECT_UPLOADED   -> MANIFEST_UPLOADED
/// MANIFEST_UPLOADED -> VERIFIED
/// VERIFIED          -> SOURCE_COMMITTED
/// ANY_STATE         -> FAILED
/// FAILED            -> REPLAY_REQUIRED
/// REPLAY_REQUIRED   -> BATCHING
/// ```
pub fn can_transition(from: BatchState, to: BatchState) -> bool {
    use BatchState::*;

    // Any non-terminal-committed state may fail.
    if to == Failed && from != SourceCommitted {
        return true;
    }

    // Hard guard: the *only* path into SourceCommitted is from Verified.
    if to == SourceCommitted {
        return from == Verified;
    }

    matches!(
        (from, to),
        (Discovered, Batching)
            | (Batching, Sealed)
            | (Sealed, Compressed)
            | (Compressed, Checksummed)
            | (Checksummed, ObjectUploaded)
            | (ObjectUploaded, ManifestUploaded)
            | (ManifestUploaded, Verified)
            | (Verified, SourceCommitted)
            | (Failed, ReplayRequired)
            | (ReplayRequired, Batching)
    )
}

/// Validates and performs a transition, returning the new state or an error.
///
/// This is the *only* sanctioned way to change a batch's state. The engine
/// must route every state change through this function so that the
/// verification-before-commit rule cannot be bypassed.
pub fn transition(from: BatchState, to: BatchState) -> Result<BatchState, VtopError> {
    // Explicit, dedicated guard for the core invariant so the error is precise.
    if to == BatchState::SourceCommitted && from != BatchState::Verified {
        return Err(VtopError::CommitBeforeVerified { actual: from });
    }

    if can_transition(from, to) {
        Ok(to)
    } else {
        Err(VtopError::IllegalStateTransition { from, to })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use BatchState::*;

    /// Helper: every state except `Verified` must be rejected for commit.
    fn assert_commit_rejected(from: BatchState) {
        let res = transition(from, SourceCommitted);
        assert!(
            matches!(res, Err(VtopError::CommitBeforeVerified { .. })),
            "expected commit from {from:?} to be rejected, got {res:?}"
        );
    }

    #[test]
    fn test_cannot_commit_from_discovered() {
        assert_commit_rejected(Discovered);
    }

    #[test]
    fn test_cannot_commit_from_batching() {
        assert_commit_rejected(Batching);
    }

    #[test]
    fn test_cannot_commit_from_sealed() {
        assert_commit_rejected(Sealed);
    }

    #[test]
    fn test_cannot_commit_from_compressed() {
        assert_commit_rejected(Compressed);
    }

    #[test]
    fn test_cannot_commit_from_checksummed() {
        assert_commit_rejected(Checksummed);
    }

    #[test]
    fn test_cannot_commit_from_object_uploaded() {
        assert_commit_rejected(ObjectUploaded);
    }

    #[test]
    fn test_cannot_commit_from_manifest_uploaded() {
        assert_commit_rejected(ManifestUploaded);
    }

    #[test]
    fn test_can_commit_from_verified() {
        let res = transition(Verified, SourceCommitted).expect("verified -> committed must pass");
        assert_eq!(res, SourceCommitted);
    }

    #[test]
    fn test_happy_path_is_fully_legal() {
        let path = [
            Discovered,
            Batching,
            Sealed,
            Compressed,
            Checksummed,
            ObjectUploaded,
            ManifestUploaded,
            Verified,
            SourceCommitted,
        ];
        for pair in path.windows(2) {
            transition(pair[0], pair[1])
                .unwrap_or_else(|e| panic!("{:?} -> {:?} should be legal: {e}", pair[0], pair[1]));
        }
    }

    #[test]
    fn test_skipping_states_is_illegal() {
        assert!(transition(Sealed, ObjectUploaded).is_err());
        assert!(transition(Discovered, Compressed).is_err());
        assert!(transition(Batching, Verified).is_err());
    }

    #[test]
    fn test_any_state_can_fail() {
        for s in [
            Discovered,
            Batching,
            Sealed,
            Compressed,
            Checksummed,
            ObjectUploaded,
            ManifestUploaded,
            Verified,
        ] {
            assert!(can_transition(s, Failed), "{s:?} should be able to fail");
        }
    }

    #[test]
    fn test_failed_recovery_path() {
        assert!(transition(Failed, ReplayRequired).is_ok());
        assert!(transition(ReplayRequired, Batching).is_ok());
    }

    #[test]
    fn test_committed_cannot_regress_to_failed() {
        // Once committed, the batch is done; it should not be marked failed.
        assert!(!can_transition(SourceCommitted, Failed));
    }
}

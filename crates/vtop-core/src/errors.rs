//! Error types for the VTOP Engine core.

use crate::state_machine::BatchState;
use thiserror::Error;

/// The canonical error type used across the engine.
#[derive(Debug, Error)]
pub enum VtopError {
    #[error("illegal state transition: {from:?} -> {to:?}")]
    IllegalStateTransition { from: BatchState, to: BatchState },

    #[error("commit forbidden: batch is in {actual:?}, SOURCE_COMMITTED requires VERIFIED")]
    CommitBeforeVerified { actual: BatchState },

    #[error(
        "batch is in an invalid state for this operation: expected {expected:?}, got {actual:?}"
    )]
    InvalidStateForOperation {
        expected: BatchState,
        actual: BatchState,
    },

    #[error("verification failed for {uri}: {message}")]
    VerificationFailed { uri: String, message: String },

    #[error("checksum mismatch for {uri}: expected {expected}, got {actual}")]
    ChecksumMismatch {
        uri: String,
        expected: String,
        actual: String,
    },

    #[error("configuration error: {0}")]
    Config(String),

    #[error("source adapter error: {0}")]
    Source(String),

    #[error("upload backend error: {0}")]
    Upload(String),
    /// The object store asked for LESS, not for a retry (#102): an HTTP 429
    /// or 503, or one of S3's throttling codes (`SlowDown`, `Throttling`,
    /// `RequestLimitExceeded`, ...), surfaced after the backend's own retries
    /// were spent. Kept apart from `Upload` because the two call for opposite
    /// responses — a throttle answered by retrying at the same rate is the
    /// overload it complains about — and so the engine can count them apart,
    /// for an operator now and for a concurrency controller later.
    #[error("upload backend throttled: {0}")]
    UploadThrottled(String),

    #[error("state store error: {0}")]
    State(String),

    #[error("compression error: {0}")]
    Compression(String),

    #[error("manifest error: {0}")]
    Manifest(String),

    #[error("replay error: {0}")]
    Replay(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serde_json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("serde_yaml error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("{0}")]
    Other(String),
}

impl VtopError {
    /// Whether this is the store saying "slow down" (#102).
    pub fn is_upload_throttle(&self) -> bool {
        matches!(self, VtopError::UploadThrottled(_))
    }
}

/// Convenience result alias.
pub type VtopResult<T> = Result<T, VtopError>;

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

/// Convenience result alias.
pub type VtopResult<T> = Result<T, VtopError>;

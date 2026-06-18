//! Persisted row models for the state store.

use serde::{Deserialize, Serialize};
use vtop_core::manifest::VtopManifest;
use vtop_core::state_machine::BatchState;
use vtop_core::types::{ProgressMarker, SourceType, TelemetryFormat};

/// A persisted batch record. This is the durable journal entry that makes the
/// engine crash-recoverable: every state transition is written here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRecord {
    pub batch_id: String,
    pub tenant: String,
    pub source_type: SourceType,
    pub source_name: String,
    pub format: TelemetryFormat,
    pub state: BatchState,
    pub progress_start: ProgressMarker,
    pub progress_end: ProgressMarker,
    pub object_uri: Option<String>,
    pub manifest_uri: Option<String>,
    pub object_sha256: Option<String>,
    pub manifest_sha256: Option<String>,
    pub record_count: Option<i64>,
    pub error_message: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl BatchRecord {
    /// Whether this record still holds uncommitted source progress.
    pub fn is_incomplete(&self) -> bool {
        !matches!(self.state, BatchState::SourceCommitted)
    }
}

/// Patch applied when advancing a batch's state. Fields set to `Some` are
/// written; `None` fields are left unchanged.
#[derive(Debug, Clone, Default)]
pub struct BatchPatch {
    pub object_uri: Option<String>,
    pub manifest_uri: Option<String>,
    pub object_sha256: Option<String>,
    pub manifest_sha256: Option<String>,
    pub record_count: Option<i64>,
    pub error_message: Option<String>,
}

impl BatchPatch {
    /// Derive a patch from a fully built manifest (object + manifest hashes,
    /// URIs and record count).
    pub fn from_manifest(m: &VtopManifest) -> Self {
        Self {
            object_uri: Some(m.object.uri.clone()),
            manifest_uri: Some(m.manifest.uri.clone()),
            object_sha256: Some(m.object.sha256.clone()),
            manifest_sha256: Some(m.manifest.sha256.clone()),
            record_count: Some(m.record_count as i64),
            error_message: None,
        }
    }
}

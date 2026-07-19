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
    /// Immutable object version the store assigned to the uploaded manifest
    /// (#135). Recovery reads this exact version instead of the overwritable
    /// current key; `None` on legacy rows or non-versioning backends.
    pub manifest_version_id: Option<String>,
    /// Size in bytes of the uploaded (compressed) object. Lets recovery's
    /// storage re-check compare size even when no digest is available (#125).
    pub object_size_bytes: Option<i64>,
    pub record_count: Option<i64>,
    pub error_message: Option<String>,
    /// Engine instance that owns this in-flight batch (#93). `None` on rows
    /// written before ownership existed.
    pub owner: Option<String>,
    /// RFC3339 instant after which the owner's claim may be taken over by
    /// another engine. A live engine's batches are NEVER touched; a dead
    /// engine's are reclaimable once this passes.
    pub lease_expires_at: Option<String>,
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
    pub manifest_version_id: Option<String>,
    pub object_size_bytes: Option<i64>,
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
            object_sha256: Some(m.object.checksum.clone()),
            manifest_sha256: Some(m.manifest.sha256.clone()),
            // The version is assigned by storage at upload time, not derivable
            // from the manifest itself.
            manifest_version_id: None,
            object_size_bytes: Some(m.object.size_bytes as i64),
            record_count: Some(m.record_count as i64),
            error_message: None,
        }
    }
}

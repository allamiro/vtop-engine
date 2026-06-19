//! VTOP manifest model.
//!
//! Every telemetry object MUST have a manifest. The manifest binds the source
//! progress marker to the object's SHA-256 hash, forming the chain of custody
//! that the commit rule depends on.

use crate::checksum::sha256_bytes;
use crate::errors::VtopError;
use crate::state_machine::BatchState;
use crate::types::{CompressionType, ProgressMarker, SourceType, TelemetryFormat};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Protocol identifier embedded in every manifest.
pub const VTOP_PROTOCOL: &str = "VTOP";
/// Manifest schema version.
pub const VTOP_VERSION: &str = "0.1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    NotVerified,
    Passed,
    Failed,
    BackendLimited,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub uri: String,
    pub size_bytes: u64,
    /// Checksum algorithm used for this object: `sha256`, `blake3`, or `none`.
    pub checksum_algorithm: String,
    /// Lowercase hex digest of the object (empty when checksums are disabled).
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestMetadata {
    pub uri: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetadata {
    pub path_template: String,
    pub resolved_prefix: String,
}

/// The manifest object written alongside every telemetry object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VtopManifest {
    pub protocol: String,
    pub version: String,
    pub batch_id: String,
    pub tenant: String,
    pub source_type: SourceType,
    pub source_name: String,
    pub format: TelemetryFormat,
    pub compression: CompressionType,
    pub record_count: usize,
    pub first_timestamp: Option<String>,
    pub last_timestamp: Option<String>,
    /// The bound source progress marker — the heart of replay safety.
    pub source_progress: ProgressMarker,
    pub object: ObjectMetadata,
    pub manifest: ManifestMetadata,
    pub partitioning: PartitionMetadata,
    pub upload_backend: String,
    pub state: BatchState,
    pub verification_status: VerificationStatus,
    pub created_at: String,
}

/// Inputs needed to assemble a manifest.
pub struct ManifestBuilder {
    pub batch_id: String,
    pub tenant: String,
    pub source_type: SourceType,
    pub source_name: String,
    pub format: TelemetryFormat,
    pub compression: CompressionType,
    pub record_count: usize,
    pub first_timestamp: Option<String>,
    pub last_timestamp: Option<String>,
    pub source_progress: ProgressMarker,
    pub object_uri: String,
    pub object_size: u64,
    pub object_checksum_algorithm: String,
    pub object_checksum: String,
    pub manifest_uri: String,
    pub path_template: String,
    pub resolved_prefix: String,
    pub upload_backend: String,
    pub created_at: String,
}

impl ManifestBuilder {
    /// Build the manifest. The `manifest.sha256` field is computed from the
    /// canonical serialization of the manifest *with that field empty*, so the
    /// hash is reproducible and self-consistent.
    pub fn build(self) -> Result<VtopManifest, VtopError> {
        let mut manifest = VtopManifest {
            protocol: VTOP_PROTOCOL.to_string(),
            version: VTOP_VERSION.to_string(),
            batch_id: self.batch_id,
            tenant: self.tenant,
            source_type: self.source_type,
            source_name: self.source_name,
            format: self.format,
            compression: self.compression,
            record_count: self.record_count,
            first_timestamp: self.first_timestamp,
            last_timestamp: self.last_timestamp,
            source_progress: self.source_progress,
            object: ObjectMetadata {
                uri: self.object_uri,
                size_bytes: self.object_size,
                checksum_algorithm: self.object_checksum_algorithm,
                checksum: self.object_checksum,
            },
            manifest: ManifestMetadata {
                uri: self.manifest_uri,
                sha256: String::new(),
            },
            partitioning: PartitionMetadata {
                path_template: self.path_template,
                resolved_prefix: self.resolved_prefix,
            },
            upload_backend: self.upload_backend,
            state: BatchState::ManifestUploaded,
            verification_status: VerificationStatus::NotVerified,
            created_at: self.created_at,
        };

        manifest.manifest.sha256 = manifest.self_hash()?;
        Ok(manifest)
    }
}

impl VtopManifest {
    /// Canonical JSON bytes for hashing/upload (pretty, stable field order from
    /// the struct definition).
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, VtopError> {
        Ok(serde_json::to_vec_pretty(self)?)
    }

    /// Compute the manifest's own SHA-256 over its serialization with the
    /// `manifest.sha256` field blanked out (so the value is reproducible).
    pub fn self_hash(&self) -> Result<String, VtopError> {
        let mut clone = self.clone();
        clone.manifest.sha256 = String::new();
        let bytes = serde_json::to_vec(&clone)?;
        Ok(sha256_bytes(&bytes))
    }

    /// Recompute and compare the embedded manifest hash. Used during
    /// verification of a manifest read back from storage.
    pub fn verify_self_hash(&self) -> Result<(), VtopError> {
        let expected = self.self_hash()?;
        if expected.eq_ignore_ascii_case(&self.manifest.sha256) {
            Ok(())
        } else {
            Err(VtopError::ChecksumMismatch {
                uri: self.manifest.uri.clone(),
                expected,
                actual: self.manifest.sha256.clone(),
            })
        }
    }

    /// Persist the manifest JSON to `work_dir`, returning the local path.
    pub fn write_to_file(&self, work_dir: &Path) -> Result<PathBuf, VtopError> {
        std::fs::create_dir_all(work_dir)?;
        let path = work_dir.join(format!("{}.manifest.json", self.batch_id));
        std::fs::write(&path, self.to_json_bytes()?)?;
        Ok(path)
    }

    /// Mark verification outcome and advance state accordingly.
    pub fn set_verification(&mut self, status: VerificationStatus) {
        self.verification_status = status;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kafka_marker() -> ProgressMarker {
        ProgressMarker::Kafka {
            topic: "app_events".into(),
            partition: 0,
            start_offset: 481000,
            end_offset: 482499,
            consumer_group: "vtop-engine".into(),
        }
    }

    fn builder() -> ManifestBuilder {
        ManifestBuilder {
            batch_id: "vtop-test".into(),
            tenant: "default".into(),
            source_type: SourceType::Kafka,
            source_name: "app_events".into(),
            format: TelemetryFormat::Cef,
            compression: CompressionType::Gzip,
            record_count: 1500,
            first_timestamp: None,
            last_timestamp: None,
            source_progress: kafka_marker(),
            object_uri: "s3://telemetry-data/x/batch.cef.gz".into(),
            object_size: 924822,
            object_checksum_algorithm: "sha256".into(),
            object_checksum: "abc123".into(),
            manifest_uri: "s3://telemetry-data/x/batch.manifest.json".into(),
            path_template: "tenant={tenant}/...".into(),
            resolved_prefix: "tenant=default/source=app_events/...".into(),
            upload_backend: "s3_native".into(),
            created_at: "2026-06-18T15:00:00Z".into(),
        }
    }

    #[test]
    fn manifest_binds_source_progress_to_object_hash() {
        let m = builder().build().unwrap();
        // The manifest carries the source progress marker.
        assert_eq!(m.source_progress, kafka_marker());
        // And the object hash.
        assert_eq!(m.object.checksum, "abc123");
        assert_eq!(m.object.checksum_algorithm, "sha256");
        assert_eq!(m.protocol, "VTOP");
    }

    #[test]
    fn manifest_self_hash_is_stable_and_verifiable() {
        let m = builder().build().unwrap();
        assert!(!m.manifest.sha256.is_empty());
        m.verify_self_hash().expect("self hash must verify");
    }

    #[test]
    fn tampering_breaks_self_hash() {
        let mut m = builder().build().unwrap();
        m.record_count = 9999; // tamper
        assert!(m.verify_self_hash().is_err());
    }

    #[test]
    fn json_includes_source_progress_marker() {
        let m = builder().build().unwrap();
        let json = String::from_utf8(m.to_json_bytes().unwrap()).unwrap();
        assert!(json.contains("source_progress"));
        assert!(json.contains("start_offset"));
        assert!(json.contains("481000"));
    }
}

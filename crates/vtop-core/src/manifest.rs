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
///
/// `0.3` (#622): `source_progress` runs from the batch's first read's start to
/// its last read's end. Up to `0.2` it carried the last read's start, so a
/// batch built from several reads claimed less than its object held; the
/// ledger's `progress_start` is the authority for those. The shape is
/// unchanged, so every released verifier reads both: the bump tells a reader
/// which meaning a manifest's start has.
pub const VTOP_VERSION: &str = "0.3";
/// Maximum serialized manifest size accepted from storage.
///
/// Manifests contain metadata only and are normally a few KiB. A hard 1 MiB
/// cap prevents a replaced manifest key from exhausting memory during startup
/// recovery while leaving ample room for long source markers and extensions.
pub const MAX_MANIFEST_BYTES: usize = 1024 * 1024;

/// Runtime-only key used to authenticate manifests with BLAKE3 keyed hashing.
///
/// The key is deliberately opaque and has no serialization implementation: it
/// may be loaded from an environment variable, but it can never accidentally
/// become part of a config dump or manifest.
#[derive(Clone)]
pub struct ManifestMacKey([u8; 32]);

impl std::fmt::Debug for ManifestMacKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ManifestMacKey([REDACTED])")
    }
}

impl ManifestMacKey {
    /// Decode the required 32-byte key from its 64-character hex form.
    pub fn from_hex(value: &str) -> Result<Self, VtopError> {
        if value.len() != 64 {
            return Err(VtopError::Config(
                "manifest MAC key must be exactly 32 bytes (64 hex characters)".into(),
            ));
        }
        let mut key = [0_u8; 32];
        hex::decode_to_slice(value, &mut key).map_err(|_| {
            VtopError::Config(
                "manifest MAC key must be exactly 32 bytes (64 hex characters)".into(),
            )
        })?;
        Ok(Self(key))
    }

    fn authenticate(&self, bytes: &[u8]) -> blake3::Hash {
        blake3::keyed_hash(&self.0, bytes)
    }
}

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
    /// Optional keyed BLAKE3 authenticator over the canonical manifest.
    ///
    /// Missing on version 0.1 and unsigned manifests. When a runtime MAC key
    /// is configured, verification requires this field to exist and match.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mac: Option<String>,
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

/// The newest manifest version this build reads.
pub const MAX_READABLE_VERSION: &str = "0.4";

/// How a manifest version is authenticated, decided from its `version` field
/// before anything else in the document is interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashScheme {
    /// 0.1-0.3: the hash and MAC cover `serde_json::to_vec` of the decoded
    /// struct. Kept exactly, so every archived manifest still verifies.
    Legacy,
    /// 0.4: the hash and MAC cover the STORED document, canonicalized, so
    /// fields this build does not know are authenticated too (#671).
    Canonical,
}

/// A manifest read from storage, with the bytes its version's scheme
/// authenticates. Dereferences to the decoded fields this build knows, which
/// bind it to the batch and the ledger; for a 0.4 manifest the hash and MAC
/// also cover the fields it does not know.
///
/// Every reader of a stored manifest goes through [`ParsedManifest::from_slice`]
/// (recovery's re-check, the in-run keyed read-back, `vtopctl scrub` and
/// `vtopctl verify-manifest`): decoding into [`VtopManifest`] and re-encoding
/// drops a newer writer's fields, and the recomputed hash then reports a
/// newer manifest as tampered.
#[derive(Debug)]
pub struct ParsedManifest {
    decoded: VtopManifest,
    auth_bytes: Vec<u8>,
}

impl std::ops::Deref for ParsedManifest {
    type Target = VtopManifest;

    fn deref(&self) -> &Self::Target {
        &self.decoded
    }
}

impl ParsedManifest {
    /// Parse a stored manifest. `protocol` and `version` are checked first:
    /// a version newer than [`MAX_READABLE_VERSION`] is
    /// [`VtopError::UnsupportedManifestVersion`] before any other field is
    /// decoded or any digest computed, so a newer writer never looks like a
    /// damaged older one. Anything else wrong is a malformed document
    /// ([`VtopError::Manifest`] or [`VtopError::Json`]).
    pub fn from_slice(bytes: &[u8]) -> Result<Self, VtopError> {
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(VtopError::Manifest(format!(
                "manifest exceeds the {MAX_MANIFEST_BYTES}-byte limit"
            )));
        }
        let header = ManifestHeader::from_slice(bytes)?;
        match header.scheme()? {
            HashScheme::Legacy => {
                // Decoded straight from the bytes, as every earlier reader
                // did, and hashed by re-encoding the struct: unchanged.
                let decoded: VtopManifest = serde_json::from_slice(bytes)?;
                let auth_bytes = decoded.auth_bytes()?;
                Ok(Self {
                    decoded,
                    auth_bytes,
                })
            }
            HashScheme::Canonical => {
                let StrictJson(value) = serde_json::from_slice(bytes)?;
                let decoded: VtopManifest = serde_json::from_value(value.clone())?;
                let auth_bytes = canonical_auth_bytes(value)?;
                Ok(Self {
                    decoded,
                    auth_bytes,
                })
            }
        }
    }

    pub fn verify_self_hash(&self) -> Result<(), VtopError> {
        check_self_hash(&self.decoded, &self.auth_bytes)
    }

    pub fn verify_authentication(&self, mac_key: Option<&ManifestMacKey>) -> Result<(), VtopError> {
        self.verify_self_hash()?;
        check_mac(&self.decoded, &self.auth_bytes, mac_key)
    }
}

/// The two envelope fields every manifest version carries, read from a
/// document that must be a JSON object. Anything else in it is ignored here,
/// so a newer writer's fields cannot fail this.
struct ManifestHeader {
    protocol: Option<serde_json::Value>,
    version: Option<serde_json::Value>,
}

impl ManifestHeader {
    fn from_slice(bytes: &[u8]) -> Result<Self, VtopError> {
        // A map, not a derived struct: serde would accept a positional array.
        let mut object: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(bytes)?;
        Ok(Self {
            protocol: object.remove("protocol"),
            version: object.remove("version"),
        })
    }

    fn scheme(&self) -> Result<HashScheme, VtopError> {
        if self.protocol.as_ref().and_then(serde_json::Value::as_str) != Some(VTOP_PROTOCOL) {
            return Err(VtopError::Manifest(format!(
                "manifest protocol must be {VTOP_PROTOCOL}"
            )));
        }
        let version = self
            .version
            .as_ref()
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| VtopError::Manifest("manifest version must be a string".into()))?;
        version_scheme(version)
    }
}

fn version_scheme(version: &str) -> Result<HashScheme, VtopError> {
    match version {
        "0.1" | "0.2" | "0.3" => return Ok(HashScheme::Legacy),
        "0.4" => return Ok(HashScheme::Canonical),
        _ => {}
    }
    // `MAJOR.MINOR`, both all digits. A number too large for u64 is still a
    // larger number, so it saturates rather than reads as malformed.
    let numeric = |part: &str| {
        (!part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
            .then(|| part.parse::<u64>().unwrap_or(u64::MAX))
    };
    let parsed = version
        .split_once('.')
        .and_then(|(major, minor)| Some((numeric(major)?, numeric(minor)?)));
    // Bounded: the field comes from storage and lands in logs.
    let shown: String = if version.chars().count() > 32 {
        version.chars().take(32).chain("...".chars()).collect()
    } else {
        version.to_string()
    };
    match parsed {
        Some(parsed) if parsed > (0, 4) => Err(VtopError::UnsupportedManifestVersion {
            version: shown,
            supported: MAX_READABLE_VERSION,
        }),
        _ => Err(VtopError::Manifest(format!(
            "malformed manifest version {shown:?}"
        ))),
    }
}

/// The 0.4 hash/MAC input: the stored document with `manifest.sha256` set to
/// `""` (and `manifest.mac` too where the key is present), object keys sorted
/// at every depth, serialized as compact JSON. Unknown fields are kept.
fn canonical_auth_bytes(mut value: serde_json::Value) -> Result<Vec<u8>, VtopError> {
    let metadata = value
        .get_mut("manifest")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| VtopError::Manifest("manifest metadata must be an object".into()))?;
    metadata.insert("sha256".into(), serde_json::Value::String(String::new()));
    if metadata.contains_key("mac") {
        metadata.insert("mac".into(), serde_json::Value::String(String::new()));
    }
    // Sorted explicitly: serde_json's map is sorted only while no crate in
    // the build enables its `preserve_order` feature.
    sort_json_keys(&mut value);
    Ok(serde_json::to_vec(&value)?)
}

fn sort_json_keys(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for value in map.values_mut() {
                sort_json_keys(value);
            }
            map.sort_keys();
        }
        serde_json::Value::Array(values) => {
            for value in values {
                sort_json_keys(value);
            }
        }
        _ => {}
    }
}

/// A JSON value parsed for the 0.4 canonical form, refusing the two inputs
/// whose canonical bytes would not pin down what the document says:
///
/// - a duplicate object key: `serde_json` keeps the last, other parsers the
///   first, so two readers could authenticate one document and act on
///   different values;
/// - a number that is not an integer in the 64-bit range: it is re-encoded
///   as an `f64`, so different stored numbers (`0.1` and
///   `0.10000000000000001`, or two integers above `u64::MAX`) would share
///   one hash, and an edit between them would pass verification.
struct StrictJson(serde_json::Value);

impl<'de> Deserialize<'de> for StrictJson {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(StrictVisitor).map(StrictJson)
    }
}

struct StrictVisitor;

impl<'de> serde::de::Visitor<'de> for StrictVisitor {
    type Value = serde_json::Value;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a JSON value with integer numbers and no duplicate keys")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Bool(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
        Ok(v.into())
    }

    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Self::Value, E> {
        Err(E::custom(format_args!(
            "a version 0.4 manifest number must be an integer in the 64-bit range, not {v}"
        )))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
        Ok(serde_json::Value::String(v.to_owned()))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E> {
        Ok(serde_json::Value::String(v))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(StrictJson(value)) = seq.next_element()? {
            values.push(value);
        }
        Ok(serde_json::Value::Array(values))
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut object = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if object.contains_key(&key) {
                return Err(serde::de::Error::custom(format_args!(
                    "duplicate key {key:?} in a version 0.4 manifest"
                )));
            }
            let StrictJson(value) = map.next_value()?;
            object.insert(key, value);
        }
        Ok(serde_json::Value::Object(object))
    }
}

fn check_self_hash(manifest: &VtopManifest, bytes: &[u8]) -> Result<(), VtopError> {
    let expected = sha256_bytes(bytes);
    if expected.eq_ignore_ascii_case(&manifest.manifest.sha256) {
        Ok(())
    } else {
        Err(VtopError::ChecksumMismatch {
            uri: manifest.manifest.uri.clone(),
            expected,
            actual: manifest.manifest.sha256.clone(),
        })
    }
}

fn check_mac(
    manifest: &VtopManifest,
    bytes: &[u8],
    mac_key: Option<&ManifestMacKey>,
) -> Result<(), VtopError> {
    let Some(key) = mac_key else {
        return Ok(());
    };
    let actual_hex = manifest.manifest.mac.as_deref().ok_or_else(|| {
        VtopError::Manifest(
            "manifest MAC is required because manifest_mac_key_env is configured".into(),
        )
    })?;
    let actual = hex::decode(actual_hex)
        .map_err(|_| VtopError::Manifest("manifest MAC is not valid hex".into()))?;
    if actual.len() != 32 {
        return Err(VtopError::Manifest(
            "manifest MAC must be exactly 32 bytes".into(),
        ));
    }
    let expected = key.authenticate(bytes);
    // Compare without an early exit so a remote verification path does not
    // leak how many prefix bytes were correct.
    let mismatch = expected
        .as_bytes()
        .iter()
        .zip(actual.iter())
        .fold(0_u8, |acc, (left, right)| acc | (left ^ right));
    if mismatch == 0 {
        Ok(())
    } else {
        Err(VtopError::Manifest(
            "manifest MAC verification failed".into(),
        ))
    }
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
        self.build_with_mac(None)
    }

    /// Build a manifest, authenticating it when a runtime key is configured.
    pub fn build_with_mac(
        self,
        mac_key: Option<&ManifestMacKey>,
    ) -> Result<VtopManifest, VtopError> {
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
                // Presence is authenticated too: signed manifests canonicalize
                // this to an empty string, while unsigned manifests omit it.
                mac: mac_key.map(|_| String::new()),
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
        if let Some(key) = mac_key {
            manifest.manifest.mac = Some(
                key.authenticate(&manifest.auth_bytes()?)
                    .to_hex()
                    .to_string(),
            );
        }
        Ok(manifest)
    }
}

impl VtopManifest {
    /// Canonical JSON bytes for hashing/upload (pretty, stable field order from
    /// the struct definition).
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, VtopError> {
        Ok(serde_json::to_vec_pretty(self)?)
    }

    /// Canonical bytes shared by the unkeyed self-hash and keyed authenticator.
    /// Both embedded values are blanked so neither calculation is circular.
    fn auth_bytes(&self) -> Result<Vec<u8>, VtopError> {
        let mut clone = self.clone();
        clone.manifest.sha256 = String::new();
        if clone.manifest.mac.is_some() {
            clone.manifest.mac = Some(String::new());
        }
        Ok(serde_json::to_vec(&clone)?)
    }

    /// Compute the manifest's own SHA-256 over its canonical serialization.
    pub fn self_hash(&self) -> Result<String, VtopError> {
        Ok(sha256_bytes(&self.auth_bytes()?))
    }

    /// Recompute and compare the embedded manifest hash of a decoded manifest.
    ///
    /// Only the legacy scheme (0.1-0.3) can be recomputed from decoded
    /// fields: a 0.4 manifest's hash covers fields the struct may not hold,
    /// so it is verified from its stored bytes with [`ParsedManifest`].
    pub fn verify_self_hash(&self) -> Result<(), VtopError> {
        self.require_legacy_scheme()?;
        check_self_hash(self, &self.auth_bytes()?)
    }

    /// Verify the reproducible self-hash and, when configured, require a valid
    /// keyed BLAKE3 authenticator. Legacy scheme only, as for
    /// [`VtopManifest::verify_self_hash`].
    ///
    /// Supplying a key deliberately rejects legacy/unsigned manifests. This is
    /// the migration boundary: operators must verify their backlog before
    /// enabling authentication rather than silently grandfathering it forever.
    pub fn verify_authentication(&self, mac_key: Option<&ManifestMacKey>) -> Result<(), VtopError> {
        self.verify_self_hash()?;
        check_mac(self, &self.auth_bytes()?, mac_key)
    }

    fn require_legacy_scheme(&self) -> Result<(), VtopError> {
        let header = ManifestHeader {
            protocol: Some(self.protocol.clone().into()),
            version: Some(self.version.clone().into()),
        };
        match header.scheme()? {
            HashScheme::Legacy => Ok(()),
            HashScheme::Canonical => Err(VtopError::Manifest(format!(
                "a version {} manifest is verified from its stored bytes, not from decoded fields",
                self.version
            ))),
        }
    }

    /// Persist the manifest JSON to `work_dir`, returning the local path.
    pub fn write_to_file(&self, work_dir: &Path) -> Result<PathBuf, VtopError> {
        std::fs::create_dir_all(work_dir)?;
        let path = work_dir.join(format!("{}.manifest.json", self.batch_id));
        let bytes = self.to_json_bytes()?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(VtopError::Manifest(format!(
                "manifest exceeds the {MAX_MANIFEST_BYTES}-byte limit"
            )));
        }
        std::fs::write(&path, bytes)?;
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
    fn authenticated_manifest_requires_the_configured_key() {
        let key = ManifestMacKey::from_hex(&"11".repeat(32)).unwrap();
        let wrong = ManifestMacKey::from_hex(&"22".repeat(32)).unwrap();
        let m = builder().build_with_mac(Some(&key)).unwrap();

        assert_eq!(m.version, VTOP_VERSION);
        assert_eq!(m.manifest.mac.as_deref().map(str::len), Some(64));
        m.verify_authentication(Some(&key))
            .expect("the writing key must authenticate the manifest");
        assert!(m.verify_authentication(Some(&wrong)).is_err());
    }

    #[test]
    fn recomputing_the_unkeyed_hash_cannot_hide_tampering() {
        let key = ManifestMacKey::from_hex(&"33".repeat(32)).unwrap();
        let mut m = builder().build_with_mac(Some(&key)).unwrap();

        // This is the exact attack the old self-hash could not detect: rewrite
        // content and then recompute the unkeyed value stored beside it.
        m.record_count = 9999;
        m.manifest.sha256 = m.self_hash().unwrap();
        m.verify_self_hash()
            .expect("the attacker can recompute an unkeyed self-hash");
        assert!(m.verify_authentication(Some(&key)).is_err());
    }

    #[test]
    fn enabling_a_key_rejects_unsigned_and_legacy_manifests() {
        let key = ManifestMacKey::from_hex(&"44".repeat(32)).unwrap();
        let unsigned = builder().build().unwrap();
        unsigned.verify_self_hash().unwrap();
        assert!(unsigned.verify_authentication(Some(&key)).is_err());

        // Version 0.1 JSON did not have `manifest.mac`; serde must still read
        // it so an operator without a key retains today's behavior.
        let mut json = serde_json::to_value(&unsigned).unwrap();
        json["version"] = serde_json::json!("0.1");
        json["manifest"].as_object_mut().unwrap().remove("mac");
        let mut legacy: VtopManifest = serde_json::from_value(json).unwrap();
        legacy.manifest.sha256 = legacy.self_hash().unwrap();
        legacy.verify_authentication(None).unwrap();
        assert!(legacy.verify_authentication(Some(&key)).is_err());
    }

    #[test]
    fn manifest_key_parser_rejects_wrong_length_and_non_hex() {
        assert!(ManifestMacKey::from_hex("11").is_err());
        assert!(ManifestMacKey::from_hex(&"zz".repeat(32)).is_err());
        let key_hex = "ab".repeat(32);
        let key = ManifestMacKey::from_hex(&key_hex).unwrap();
        let debug = format!("{key:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains(&key_hex));
    }

    #[test]
    fn json_includes_source_progress_marker() {
        let m = builder().build().unwrap();
        let json = String::from_utf8(m.to_json_bytes().unwrap()).unwrap();
        assert!(json.contains("source_progress"));
        assert!(json.contains("start_offset"));
        assert!(json.contains("481000"));
    }

    #[test]
    fn refuses_to_write_an_oversized_manifest() {
        let mut manifest = builder().build().unwrap();
        manifest.source_name = "x".repeat(MAX_MANIFEST_BYTES + 1);
        let dir = tempfile::tempdir().unwrap();
        let err = manifest.write_to_file(dir.path()).unwrap_err();
        assert!(err.to_string().contains("manifest exceeds"));
        assert!(!dir.path().join("vtop-test.manifest.json").exists());
    }
}

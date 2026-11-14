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

pub mod keyring;
pub use keyring::{
    Authentication, KeyOutcome, KeyResult, KeySourceKind, KeyringKey, ManifestKeyring,
    KEY_LABEL_NONE, KEY_LABEL_OTHER,
};

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
/// The manifest version a writer emits when `manifest.write_version` opts in
/// (#671 release B). It adds `verification_plan` and is hashed over the
/// stored document ([`HashScheme::Canonical`]), which only v0.8.0 and later
/// readers verify, so it is never the default.
pub const VTOP_VERSION_04: &str = "0.4";
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

    /// The keyed link of the audit table's hash chain (#694), under a key
    /// derived from this one for that purpose alone: an audit link can never
    /// be presented as a manifest MAC, nor a manifest MAC as a link.
    fn audit_link(&self, bytes: &[u8]) -> blake3::Hash {
        let key = blake3::derive_key(AUDIT_CHAIN_CONTEXT, &self.0);
        blake3::keyed_hash(&key, bytes)
    }
}

/// The BLAKE3 key-derivation context of the audit chain's key.
const AUDIT_CHAIN_CONTEXT: &str = "vtop-engine 2026 audit_events chain v1";

/// The chain's name for an unkeyed link.
pub const AUDIT_CHAIN_UNKEYED: &str = "blake3";
/// The chain's name for a link keyed with (a key derived from) a manifest
/// MAC key; `blake3-keyed:<id>` names the keyring key (#685) it used, plain
/// `blake3-keyed` the `manifest_mac_key_env` key, which has no id.
pub const AUDIT_CHAIN_KEYED: &str = "blake3-keyed";

/// One link of the audit table's hash chain (#694): the hash of the previous
/// row's link and this row's record, exactly as stored. Keyed with the
/// keyring's signing key when one is configured, so a writer who can edit
/// the table cannot recompute the chain without it. Returns the link
/// (lowercase hex) and the chain's name, which names the key's id.
pub fn audit_chain_link(
    previous: Option<&str>,
    record: &str,
    keyring: &ManifestKeyring,
) -> (String, String) {
    match keyring.signing_key() {
        Some((key, id)) => (
            audit_chain_digest(previous, record, Some(key)),
            match id {
                Some(id) => format!("{AUDIT_CHAIN_KEYED}:{id}"),
                None => AUDIT_CHAIN_KEYED.to_owned(),
            },
        ),
        None => (
            audit_chain_digest(previous, record, None),
            AUDIT_CHAIN_UNKEYED.to_owned(),
        ),
    }
}

/// Recompute a stored link: `chain` is the name [`audit_chain_link`]
/// returned. `Err` names why it cannot be checked here: a key this keyring
/// does not hold (retired, or never configured), or a chain from a newer
/// release.
pub fn audit_chain_recompute(
    previous: Option<&str>,
    record: &str,
    chain: &str,
    keyring: &ManifestKeyring,
) -> Result<String, String> {
    if chain == AUDIT_CHAIN_UNKEYED {
        return Ok(audit_chain_digest(previous, record, None));
    }
    let id = match chain.strip_prefix(AUDIT_CHAIN_KEYED) {
        Some("") => None,
        Some(rest) => match rest.strip_prefix(':') {
            Some(id) => Some(id),
            None => return Err(format!("chain {chain:?} is not one this build reads")),
        },
        None => return Err(format!("chain {chain:?} is not one this build reads")),
    };
    let key = keyring.chain_key(id).ok_or_else(|| match id {
        Some(id) => format!(
            "keyed with manifest key {id:?}, which this configuration does not hold \
             (retired or never configured in manifest.mac)"
        ),
        None => "keyed with the manifest MAC key, which is not configured here; set \
                 manifest_mac_key_env (or manifest.mac with a legacy key)"
            .to_owned(),
    })?;
    Ok(audit_chain_digest(previous, record, Some(key)))
}

fn audit_chain_digest(
    previous: Option<&str>,
    record: &str,
    key: Option<&ManifestMacKey>,
) -> String {
    let mut bytes = Vec::with_capacity(record.len() + 72);
    bytes.extend_from_slice(previous.unwrap_or("").as_bytes());
    bytes.push(b'\n');
    bytes.extend_from_slice(record.as_bytes());
    match key {
        Some(key) => key.audit_link(&bytes).to_hex().to_string(),
        None => blake3::hash(&bytes).to_hex().to_string(),
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

/// What a version 0.4 manifest says about how its object will be verified
/// (#671 release B). It is the plan in force when the manifest was written,
/// never an outcome: the manifest is uploaded before verification runs and
/// its digest is pinned, so the outcome lives in the ledger.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VerificationPlan {
    /// [`crate::effective_config::verification_method`] for the configured
    /// `upload.backend` and the object's algorithm: `store_checksum`,
    /// `read_back` or `size_only`, the value `validate --print-effective`
    /// and the startup log print. Native S3 may fall back from
    /// `store_checksum` to `read_back` for a multipart object, which is the
    /// same (strong) class. A string, not a closed enum, so a later writer's
    /// method still reads here.
    pub method: String,
    /// `upload.require_strong_verification` when the manifest was written.
    pub require_strong: bool,
}

impl VerificationPlan {
    /// The plan for an object of `algorithm` stored through the configured
    /// `backend`. A backend with no known method is refused, naming it,
    /// rather than written as `unknown`: a manifest must not promise a check
    /// nobody can say will run.
    pub fn for_backend(
        backend: &str,
        algorithm: crate::types::ChecksumAlgorithm,
        require_strong: bool,
    ) -> Result<Self, VtopError> {
        use crate::effective_config::{verification_method, VerificationMethod};
        match verification_method(backend, algorithm) {
            VerificationMethod::Unknown => Err(VtopError::Manifest(format!(
                "upload.backend {backend:?} has no known verification method, so a version \
                 {VTOP_VERSION_04} manifest cannot state its verification plan"
            ))),
            method => Ok(Self {
                method: method.as_str().to_owned(),
                require_strong,
            }),
        }
    }
}

/// The manifest version an engine writes: `manifest.write_version` in the
/// config (#671 release B). `0.3` unless the operator opts in, because a
/// reader before v0.8.0 reports a `0.4` manifest as tampered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ManifestWriteVersion {
    #[default]
    V0_3,
    V0_4,
}

impl ManifestWriteVersion {
    /// Every value `manifest.write_version` accepts, in the order the
    /// refusal names them.
    pub const ACCEPTED: [&'static str; 2] = [VTOP_VERSION, VTOP_VERSION_04];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::V0_3 => VTOP_VERSION,
            Self::V0_4 => VTOP_VERSION_04,
        }
    }
}

impl std::fmt::Display for ManifestWriteVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for ManifestWriteVersion {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ManifestWriteVersion {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct WriteVersionVisitor;
        impl serde::de::Visitor<'_> for WriteVersionVisitor {
            type Value = ManifestWriteVersion;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(r#""0.3" or "0.4""#)
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                match v {
                    VTOP_VERSION => Ok(ManifestWriteVersion::V0_3),
                    VTOP_VERSION_04 => Ok(ManifestWriteVersion::V0_4),
                    // Bounded: the value lands in an error message.
                    other => Err(E::custom(format_args!(
                        r#"must be "0.3" or "0.4", got {:?}"#,
                        other.chars().take(32).collect::<String>()
                    ))),
                }
            }

            // YAML reads an unquoted `0.4` as a number. It is refused, not
            // converted, so `0.40` or `4e-1` can never pass for a version.
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Self::Value, E> {
                Err(E::custom(format_args!(
                    r#"must be the string "0.3" or "0.4", got the number {v}; quote it"#
                )))
            }

            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
                self.visit_f64(v as f64)
            }

            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
                self.visit_f64(v as f64)
            }
        }
        deserializer.deserialize_any(WriteVersionVisitor)
    }
}

/// What [`ManifestBuilder::build_written`] writes: version 0.3, exactly as
/// every earlier release, or version 0.4 with its verification plan.
#[derive(Debug, Clone)]
pub enum ManifestFormat {
    V0_3,
    V0_4 { plan: VerificationPlan },
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
    /// The id of the key that made `mac` (#685). Version 0.4 only, for the
    /// same reason as `verification_plan`: a 0.1-0.3 reader re-encodes the
    /// decoded struct before hashing, so it would drop the field and report
    /// the manifest as tampered. Never decoded with the struct: in a legacy
    /// document the key is unknown and ignored, so such a manifest is
    /// verified with the legacy key, as v0.8.0 verifies it, and
    /// [`ParsedManifest`] fills it for 0.4 only. In 0.4 it is not blanked
    /// for the hash, so the hash and the MAC both cover it.
    #[serde(default, skip_deserializing, skip_serializing_if = "Option::is_none")]
    pub mac_key_id: Option<String>,
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
    /// Always `not_verified`: the manifest is written before verification
    /// (protocol draft §11.2). Kept in 0.4 because v0.8.0's reader requires
    /// it; `verification_plan` and the ledger say what it cannot.
    pub verification_status: VerificationStatus,
    /// Version 0.4 only (#671 release B). Never part of a 0.1-0.3 manifest:
    /// a legacy reader drops it before hashing, so in one it would be
    /// unauthenticated. Never decoded with the struct: a legacy document
    /// treats the key as unknown, whatever its value, exactly as v0.8.0 did
    /// (review of #698), and [`ParsedManifest`] fills it for 0.4 only.
    #[serde(default, skip_deserializing, skip_serializing_if = "Option::is_none")]
    pub verification_plan: Option<VerificationPlan>,
    pub created_at: String,
}

/// The newest manifest version this build reads. Every version comparison
/// and the `supported` field of [`VtopError::UnsupportedManifestVersion`] are
/// derived from it.
pub const MAX_READABLE_VERSION: &str = "0.4";

/// Versions verified with the legacy scheme, and with the canonical one.
const LEGACY_VERSIONS: [&str; 3] = ["0.1", "0.2", "0.3"];
const CANONICAL_VERSIONS: [&str; 1] = ["0.4"];

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

/// A version as dotted digits: `MAJOR.MINOR` or `MAJOR.MINOR.PATCH`, each
/// part all digits with no leading zero (`0` itself is fine). A missing PATCH
/// compares as 0. A part too large for u64 is still a larger number, so it
/// saturates. `None` is a malformed version.
fn parse_version(version: &str) -> Option<(u64, u64, u64)> {
    let part = |part: &str| {
        let digits = !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
        let canonical = part == "0" || !part.starts_with('0');
        (digits && canonical).then(|| part.parse::<u64>().unwrap_or(u64::MAX))
    };
    let parts: Vec<&str> = version.split('.').collect();
    match parts.as_slice() {
        [major, minor] => Some((part(major)?, part(minor)?, 0)),
        [major, minor, patch] => Some((part(major)?, part(minor)?, part(patch)?)),
        _ => None,
    }
}

/// A manifest read from storage, with the bytes its version's scheme
/// authenticates. [`ParsedManifest::fields`] gives the decoded fields this
/// build knows, which bind it to the batch and the ledger; for a 0.4 manifest
/// the hash and MAC also cover the fields it does not know.
///
/// Every reader of a stored manifest goes through [`ParsedManifest::from_slice`]
/// (recovery's re-check, the in-run keyed read-back, `vtopctl scrub` and
/// `vtopctl verify-manifest`): decoding into [`VtopManifest`] and re-encoding
/// drops a newer writer's fields, and the recomputed hash then reports a
/// newer manifest as tampered. It deliberately does not dereference to
/// [`VtopManifest`], whose hash methods know only the legacy scheme.
#[derive(Debug)]
pub struct ParsedManifest {
    decoded: VtopManifest,
    auth_bytes: Vec<u8>,
}

impl ParsedManifest {
    /// Parse a stored manifest. `protocol` and `version` are checked first:
    /// a version newer than [`MAX_READABLE_VERSION`] is
    /// [`VtopError::UnsupportedManifestVersion`] before any other field is
    /// decoded or any digest computed, so a newer writer never looks like a
    /// damaged older one. That is only what the document CLAIMS: nothing is
    /// authenticated yet, so a reader that acts on it must first prove the
    /// bytes are its fleet's (see [`claimed_binding`]). Anything else wrong is
    /// a malformed document ([`VtopError::Manifest`] or [`VtopError::Json`]).
    pub fn from_slice(bytes: &[u8]) -> Result<Self, VtopError> {
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(VtopError::Manifest(format!(
                "manifest exceeds the {MAX_MANIFEST_BYTES}-byte limit"
            )));
        }
        let header: ManifestHeader = serde_json::from_slice(bytes)?;
        match header.scheme()? {
            HashScheme::Legacy => {
                // Decoded straight from the bytes, as every earlier reader
                // did, and hashed by re-encoding the struct: unchanged. A
                // `verification_plan` in a legacy document is skipped as an
                // unknown key (the field is never deserialized), as v0.8.0
                // skipped it: no hash covers it there.
                let decoded: VtopManifest = serde_json::from_slice(bytes)?;
                let auth_bytes = decoded.legacy_auth_bytes()?;
                Ok(Self {
                    decoded,
                    auth_bytes,
                })
            }
            HashScheme::Canonical => {
                let StrictJson(value) = serde_json::from_slice(bytes)?;
                let mut decoded = VtopManifest::deserialize(&value)?;
                // Defined in 0.4, so a malformed plan is a malformed document.
                decoded.verification_plan = value
                    .get("verification_plan")
                    .map(VerificationPlan::deserialize)
                    .transpose()?;
                // Defined in 0.4 too (#685). Any string is accepted here:
                // an id the keyring does not hold is an authentication
                // outcome (unknown key), not a malformed document.
                decoded.manifest.mac_key_id = value
                    .get("manifest")
                    .and_then(|m| m.get("mac_key_id"))
                    .map(|id| {
                        id.as_str().map(str::to_owned).ok_or_else(|| {
                            VtopError::Manifest("manifest.mac_key_id must be a string".into())
                        })
                    })
                    .transpose()?;
                let auth_bytes = canonical_auth_bytes(value)?;
                Ok(Self {
                    decoded,
                    auth_bytes,
                })
            }
        }
    }

    /// The decoded fields this build knows. For the hash, use
    /// [`ParsedManifest::verify_self_hash`]: [`VtopManifest::self_hash`] and
    /// [`VtopManifest::to_json_bytes`] refuse a 0.4 manifest.
    pub fn fields(&self) -> &VtopManifest {
        &self.decoded
    }

    pub fn verify_self_hash(&self) -> Result<(), VtopError> {
        check_self_hash(&self.decoded, &self.auth_bytes)
    }

    /// [`ParsedManifest::authenticate`] with one key that has no id (or
    /// none): the `manifest_mac_key_env` behaviour.
    pub fn verify_authentication(&self, mac_key: Option<&ManifestMacKey>) -> Result<(), VtopError> {
        self.authenticate(&ManifestKeyring::from_key(mac_key))
            .into_result()
    }

    /// Verify the self-hash, then the MAC with the key the manifest names
    /// (#685), or the legacy key when it names none. The outcome says which
    /// key was used, for the caller to count ([`Authentication::count`]).
    pub fn authenticate(&self, keyring: &ManifestKeyring) -> Authentication {
        if let Err(e) = self.verify_self_hash() {
            return Authentication {
                key: None,
                result: Err(e),
            };
        }
        keyring.check(&self.decoded, &self.auth_bytes)
    }
}

/// What a stored manifest says about itself, read without trusting it: its
/// `batch_id` and embedded `manifest.sha256`, for a reader that met a version
/// it cannot authenticate and must decide whether the bytes are its fleet's.
/// `None` when the document does not name both (or names one twice).
pub fn claimed_binding(bytes: &[u8]) -> Option<(String, String)> {
    #[derive(Deserialize)]
    struct Metadata {
        sha256: String,
    }
    #[derive(Deserialize)]
    struct Claimed {
        batch_id: String,
        manifest: Metadata,
    }
    let claimed: Claimed = serde_json::from_slice(bytes).ok()?;
    Some((claimed.batch_id, claimed.manifest.sha256))
}

/// The two envelope fields every manifest version carries, read from a
/// document that must be a JSON object. Every other value is skipped
/// unparsed (`IgnoredAny`), so a newer writer's fields cannot fail this. A
/// second `protocol` or `version` is malformed: a last-wins reading would let
/// a document claim two versions (review of #674).
struct ManifestHeader {
    protocol: Option<serde_json::Value>,
    version: Option<serde_json::Value>,
}

impl<'de> Deserialize<'de> for ManifestHeader {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct HeaderVisitor;
        impl<'de> serde::de::Visitor<'de> for HeaderVisitor {
            type Value = ManifestHeader;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a manifest object")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut header = ManifestHeader {
                    protocol: None,
                    version: None,
                };
                while let Some(key) = map.next_key::<std::borrow::Cow<'de, str>>()? {
                    let slot = match key.as_ref() {
                        "protocol" => &mut header.protocol,
                        "version" => &mut header.version,
                        _ => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                            continue;
                        }
                    };
                    if slot.is_some() {
                        return Err(serde::de::Error::custom(format_args!(
                            "duplicate key {key:?} in a manifest"
                        )));
                    }
                    *slot = Some(map.next_value()?);
                }
                Ok(header)
            }
        }
        // A map only: a derived struct would also accept a positional array.
        deserializer.deserialize_map(HeaderVisitor)
    }
}

impl ManifestHeader {
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
    if LEGACY_VERSIONS.contains(&version) {
        return Ok(HashScheme::Legacy);
    }
    if CANONICAL_VERSIONS.contains(&version) {
        return Ok(HashScheme::Canonical);
    }
    // Bounded: the field comes from storage and lands in logs.
    let shown: String = if version.chars().count() > 32 {
        version.chars().take(32).chain("...".chars()).collect()
    } else {
        version.to_string()
    };
    let newest = parse_version(MAX_READABLE_VERSION).expect("MAX_READABLE_VERSION is well formed");
    match parse_version(version) {
        Some(parsed) if parsed > newest => Err(VtopError::UnsupportedManifestVersion {
            uri: None,
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
    // Explicit: serde_json's maps are sorted only while no crate in the build
    // enables its `preserve_order` feature; this sorts at every depth either way.
    value.sort_all_objects();
    Ok(serde_json::to_vec(&value)?)
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

    /// Build a version 0.3 manifest, authenticating it when a runtime key is
    /// configured. Byte for byte what every earlier release wrote.
    pub fn build_with_mac(
        self,
        mac_key: Option<&ManifestMacKey>,
    ) -> Result<VtopManifest, VtopError> {
        let mut manifest = self.assemble(VTOP_VERSION, None, mac_key);
        manifest.manifest.sha256 = manifest.self_hash()?;
        if let Some(key) = mac_key {
            manifest.manifest.mac = Some(
                key.authenticate(&manifest.legacy_auth_bytes()?)
                    .to_hex()
                    .to_string(),
            );
        }
        Ok(manifest)
    }

    /// Build the manifest `format` names and the exact bytes to store
    /// (#671 release B). Version 0.3 is [`ManifestBuilder::build_with_mac`]
    /// and [`VtopManifest::to_json_bytes`], unchanged. Version 0.4 is hashed
    /// and MAC'd over its canonical form (the document with `manifest.sha256`
    /// and `manifest.mac` blanked, keys sorted, compact), the bytes every
    /// v0.8.0 and later reader authenticates, and stored pretty-printed:
    /// both parse to one document, so they share that canonical form.
    pub fn build_written(
        self,
        format: ManifestFormat,
        mac_key: Option<&ManifestMacKey>,
    ) -> Result<WrittenManifest, VtopError> {
        self.build_written_with(format, mac_key, None)
    }

    /// Build the manifest `format` names, signed with the keyring's active
    /// key (#685). A version 0.4 manifest records that key's id in
    /// `manifest.mac_key_id`; the `manifest_mac_key_env` key has none and
    /// writes none. An id cannot be recorded in version 0.3 (a 0.3 reader
    /// would drop it and report tampering), so that combination is refused;
    /// config load refuses it first.
    pub fn build_signed(
        self,
        format: ManifestFormat,
        keyring: &ManifestKeyring,
    ) -> Result<WrittenManifest, VtopError> {
        let (key, key_id) = match keyring.signing_key() {
            Some((key, id)) => (Some(key), id),
            None => (None, None),
        };
        if let (ManifestFormat::V0_3, Some(id)) = (&format, key_id) {
            return Err(VtopError::Manifest(format!(
                "key id {id:?} can only be recorded in a version {VTOP_VERSION_04} manifest; \
                 manifest.mac needs manifest.write_version: \"{VTOP_VERSION_04}\""
            )));
        }
        self.build_written_with(format, key, key_id)
    }

    fn build_written_with(
        self,
        format: ManifestFormat,
        mac_key: Option<&ManifestMacKey>,
        mac_key_id: Option<&str>,
    ) -> Result<WrittenManifest, VtopError> {
        let manifest = match format {
            ManifestFormat::V0_3 => {
                let manifest = self.build_with_mac(mac_key)?;
                let bytes = manifest.to_json_bytes()?;
                return Ok(WrittenManifest { manifest, bytes });
            }
            ManifestFormat::V0_4 { plan } => self.assemble(VTOP_VERSION_04, Some(plan), mac_key),
        };
        let mut manifest = manifest;
        // Before the hash: the id is covered by both the hash and the MAC.
        manifest.manifest.mac_key_id = mac_key.and(mac_key_id).map(str::to_owned);
        let auth = canonical_auth_bytes(serde_json::to_value(&manifest)?)?;
        manifest.manifest.sha256 = sha256_bytes(&auth);
        if let Some(key) = mac_key {
            manifest.manifest.mac = Some(key.authenticate(&auth).to_hex().to_string());
        }
        let bytes = serde_json::to_vec_pretty(&manifest)?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(VtopError::Manifest(format!(
                "manifest exceeds the {MAX_MANIFEST_BYTES}-byte limit"
            )));
        }
        Ok(WrittenManifest { manifest, bytes })
    }

    /// The document with both embedded values still blank.
    fn assemble(
        self,
        version: &str,
        verification_plan: Option<VerificationPlan>,
        mac_key: Option<&ManifestMacKey>,
    ) -> VtopManifest {
        VtopManifest {
            protocol: VTOP_PROTOCOL.to_string(),
            version: version.to_string(),
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
                mac_key_id: None,
            },
            partitioning: PartitionMetadata {
                path_template: self.path_template,
                resolved_prefix: self.resolved_prefix,
            },
            upload_backend: self.upload_backend,
            state: BatchState::ManifestUploaded,
            // Written before verification runs, in every version (D3 of the
            // #671 plan): v0.8.0's reader requires the field.
            verification_status: VerificationStatus::NotVerified,
            verification_plan,
            created_at: self.created_at,
        }
    }
}

/// A manifest this build wrote, with the exact bytes to upload. The bytes
/// are kept, not re-encoded, so what is stored, hashed and verified is one
/// document; [`ParsedManifest`] is the reader-side counterpart.
#[derive(Debug, Clone)]
pub struct WrittenManifest {
    manifest: VtopManifest,
    bytes: Vec<u8>,
}

impl WrittenManifest {
    /// The fields written.
    pub fn fields(&self) -> &VtopManifest {
        &self.manifest
    }

    /// The bytes to store.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Verify the bytes about to be stored the way every reader will: parsed
    /// from those bytes, by their version's scheme.
    pub fn verify_authentication(&self, mac_key: Option<&ManifestMacKey>) -> Result<(), VtopError> {
        ParsedManifest::from_slice(&self.bytes)?.verify_authentication(mac_key)
    }

    /// [`WrittenManifest::verify_authentication`] against a keyring (#685).
    pub fn authenticate(&self, keyring: &ManifestKeyring) -> Authentication {
        match ParsedManifest::from_slice(&self.bytes) {
            Ok(parsed) => parsed.authenticate(keyring),
            Err(e) => Authentication {
                key: None,
                result: Err(e),
            },
        }
    }

    /// Persist the bytes to `work_dir` as `<batch_id>.manifest.json`,
    /// returning the local path.
    pub fn write_to_file(&self, work_dir: &Path) -> Result<PathBuf, VtopError> {
        write_manifest_file(work_dir, &self.manifest.batch_id, &self.bytes)
    }
}

fn write_manifest_file(
    work_dir: &Path,
    batch_id: &str,
    bytes: &[u8],
) -> Result<PathBuf, VtopError> {
    std::fs::create_dir_all(work_dir)?;
    let path = work_dir.join(format!("{batch_id}.manifest.json"));
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(VtopError::Manifest(format!(
            "manifest exceeds the {MAX_MANIFEST_BYTES}-byte limit"
        )));
    }
    std::fs::write(&path, bytes)?;
    Ok(path)
}

impl VtopManifest {
    /// The manifest as JSON (pretty, field order from the struct definition),
    /// for upload. Legacy versions only: a 0.4 document re-encoded from its
    /// decoded fields would lose the fields this build does not know.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, VtopError> {
        self.require_legacy_scheme()?;
        if self.verification_plan.is_some() {
            // The legacy hash cannot cover it (see `legacy_auth_bytes`).
            return Err(VtopError::Manifest(format!(
                "verification_plan is a version {VTOP_VERSION_04} field; a version {} manifest \
                 cannot carry it",
                self.version
            )));
        }
        if self.manifest.mac_key_id.is_some() {
            // Nor this one (#685): a legacy reader would drop it.
            return Err(VtopError::Manifest(format!(
                "manifest.mac_key_id is a version {VTOP_VERSION_04} field; a version {} manifest \
                 cannot carry it",
                self.version
            )));
        }
        Ok(serde_json::to_vec_pretty(self)?)
    }

    /// Canonical bytes shared by the unkeyed self-hash and keyed authenticator
    /// under the legacy scheme. Both embedded values are blanked so neither
    /// calculation is circular.
    fn legacy_auth_bytes(&self) -> Result<Vec<u8>, VtopError> {
        let mut clone = self.clone();
        // Never covered by the legacy scheme: v0.8.0 and earlier do not know
        // the field and drop it before hashing.
        clone.verification_plan = None;
        clone.manifest.mac_key_id = None;
        clone.manifest.sha256 = String::new();
        if clone.manifest.mac.is_some() {
            clone.manifest.mac = Some(String::new());
        }
        Ok(serde_json::to_vec(&clone)?)
    }

    /// Compute the manifest's own SHA-256 over its canonical serialization.
    /// Legacy versions only, as for [`VtopManifest::to_json_bytes`]: a 0.4
    /// manifest's hash covers its stored bytes ([`ParsedManifest`]).
    pub fn self_hash(&self) -> Result<String, VtopError> {
        self.require_legacy_scheme()?;
        Ok(sha256_bytes(&self.legacy_auth_bytes()?))
    }

    /// Recompute and compare the embedded manifest hash of a decoded manifest.
    ///
    /// Only the legacy scheme (0.1-0.3) can be recomputed from decoded
    /// fields: a 0.4 manifest's hash covers fields the struct may not hold,
    /// so it is verified from its stored bytes with [`ParsedManifest`].
    pub fn verify_self_hash(&self) -> Result<(), VtopError> {
        self.require_legacy_scheme()?;
        check_self_hash(self, &self.legacy_auth_bytes()?)
    }

    /// Verify the reproducible self-hash and, when configured, require a valid
    /// keyed BLAKE3 authenticator. Legacy scheme only, as for
    /// [`VtopManifest::verify_self_hash`].
    ///
    /// Supplying a key deliberately rejects legacy/unsigned manifests. This is
    /// the migration boundary: operators must verify their backlog before
    /// enabling authentication rather than silently grandfathering it forever.
    pub fn verify_authentication(&self, mac_key: Option<&ManifestMacKey>) -> Result<(), VtopError> {
        self.require_legacy_scheme()?;
        if self.manifest.mac_key_id.is_some() {
            // No legacy hash covers it, so it may not choose the key.
            return Err(VtopError::Manifest(format!(
                "manifest.mac_key_id is a version {VTOP_VERSION_04} field; a version {} manifest \
                 cannot carry it",
                self.version
            )));
        }
        let bytes = self.legacy_auth_bytes()?;
        check_self_hash(self, &bytes)?;
        ManifestKeyring::from_key(mac_key)
            .check(self, &bytes)
            .into_result()
    }

    fn require_legacy_scheme(&self) -> Result<(), VtopError> {
        let header = ManifestHeader {
            protocol: Some(self.protocol.clone().into()),
            version: Some(self.version.clone().into()),
        };
        match header.scheme()? {
            HashScheme::Legacy => Ok(()),
            HashScheme::Canonical => Err(VtopError::Manifest(format!(
                "a version {} manifest is hashed over its stored bytes, not re-encoded from decoded fields",
                self.version
            ))),
        }
    }

    /// Persist the manifest JSON to `work_dir`, returning the local path.
    pub fn write_to_file(&self, work_dir: &Path) -> Result<PathBuf, VtopError> {
        write_manifest_file(work_dir, &self.batch_id, &self.to_json_bytes()?)
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

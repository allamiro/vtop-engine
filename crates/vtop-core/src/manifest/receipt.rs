//! Verification receipts (#671 release B).
//!
//! A manifest is uploaded BEFORE its object is verified, and its digest is
//! pinned, so it can never say how the object was checked: it carries the
//! plan (`verification_plan` in 0.4), and the outcome lives in the ledger
//! (#664). A receipt is the outcome, in the archive: once a batch is
//! VERIFIED the engine writes `<batch_id>.receipt.json` beside the manifest,
//! naming the batch, the manifest's self-hash and version, the object, the
//! class the ledger recorded and the method the backend actually ran.
//!
//! It is evidence about verification, not a precondition for it: a receipt
//! that cannot be written never holds a batch back, and the ledger stays the
//! authority for commit state.
//!
//! Authentication reuses the 0.4 manifest scheme: the hash and the MAC cover
//! the STORED document with `receipt.sha256` and `receipt.mac` blanked, keys
//! sorted at every depth, compact, with the same strict parser (no duplicate
//! keys, integer numbers only). The receipt names the key that signed it in
//! `receipt.mac_key_id` (#685) and is verified with that key and no other.
//! It binds the manifest's self-hash, which the manifest's own MAC covers, so
//! a receipt cannot be moved to another manifest.
//!
//! What it does NOT prove: without a MAC key anyone who can write the bucket
//! can write a receipt with a correct hash; `verified_at` is the engine's
//! clock and `verified_by` its configured name. See `docs/SECURITY_MODEL.md`.

use super::{
    canonical_auth_bytes_of, Authentication, ManifestKeyring, StrictDoc, StrictJson, VTOP_PROTOCOL,
};
use crate::checksum::sha256_bytes;
use crate::errors::VtopError;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// The receipt format this build writes and reads.
pub const RECEIPT_VERSION: &str = "1";
/// The largest receipt read from storage. A receipt is two URIs and a few
/// short fields, normally well under 2 KiB.
pub const MAX_RECEIPT_BYTES: usize = 64 * 1024;
/// `origin` of a receipt the engine wrote in its verify phase.
pub const RECEIPT_ORIGIN_ENGINE: &str = "engine";
/// `origin` of a receipt recovery wrote for a VERIFIED row it found without
/// one, after its own re-check of the stored batch.
pub const RECEIPT_ORIGIN_RECOVERY: &str = "recovery";
/// `verification.method` when the backend did not say which check it ran:
/// written as that, never guessed from the configuration.
pub const RECEIPT_METHOD_UNSPECIFIED: &str = "unspecified";

/// A receipt's fields. Strings rather than closed enums for `class`,
/// `method` and `origin`, so a later writer's value still reads here.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VerificationReceipt {
    pub protocol: String,
    pub receipt_version: String,
    pub batch_id: String,
    pub manifest: ReceiptManifest,
    pub object: ReceiptObject,
    pub verification: ReceiptVerification,
    /// When the verification this receipt records passed (RFC 3339, the
    /// writer's clock).
    pub verified_at: String,
    /// The writing engine's `engine.name`.
    pub verified_by: String,
    /// [`RECEIPT_ORIGIN_ENGINE`] or [`RECEIPT_ORIGIN_RECOVERY`].
    pub origin: String,
    pub receipt: ReceiptAuth,
}

/// The manifest the receipt is about.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptManifest {
    pub uri: String,
    /// The manifest's own self-hash (`manifest.sha256`), as the ledger
    /// recorded it.
    pub sha256: String,
    /// The stored version the ledger pinned, when the store gave one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

/// The object the manifest names.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptObject {
    pub uri: String,
    pub size_bytes: u64,
    pub checksum_algorithm: String,
    pub checksum: String,
    /// The version the write returned, when this attempt wrote the object
    /// and the store gave one; absent for a reused object (#626) and for a
    /// receipt written by recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

/// What verification found.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptVerification {
    /// The class the ledger recorded (#664): `strong` or `size_only`.
    pub class: String,
    /// The check the backend ran on the object: `store_checksum`,
    /// `read_back`, `size_only`, or [`RECEIPT_METHOD_UNSPECIFIED`].
    pub method: String,
}

/// The receipt's own hash and MAC.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptAuth {
    pub sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mac: Option<String>,
    /// The id of the key that made `mac` (#685). Covered by the hash and
    /// the MAC, like a 0.4 manifest's `manifest.mac_key_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mac_key_id: Option<String>,
}

/// A verification receipt being parsed: the refusals name it.
pub(crate) struct ReceiptDoc;

impl StrictDoc for ReceiptDoc {
    const NAME: &'static str = "a verification receipt";
}

/// The inputs of a receipt.
#[derive(Debug, Clone)]
pub struct ReceiptBuilder {
    pub batch_id: String,
    pub manifest_uri: String,
    pub manifest_sha256: String,
    pub manifest_version_id: Option<String>,
    pub object_uri: String,
    pub object_size_bytes: u64,
    pub object_checksum_algorithm: String,
    pub object_checksum: String,
    pub object_version_id: Option<String>,
    pub class: String,
    pub method: String,
    pub verified_at: String,
    pub verified_by: String,
    pub origin: String,
}

/// A URI that may go into a receipt: an `s3://bucket/key` the engine built,
/// never one carrying a query string, a fragment or user info, which could
/// hold a signed request's credentials (vtop-observability's redaction
/// rules). The refusal does not quote it.
fn check_uri(what: &str, uri: &str) -> Result<(), VtopError> {
    let authority = uri
        .split_once("://")
        .map_or("", |(_, rest)| rest.split('/').next().unwrap_or(""));
    if uri.contains(['?', '#']) || authority.contains('@') {
        return Err(VtopError::Manifest(format!(
            "the {what} URI carries a query string, a fragment or user info, which a \
             verification receipt never records"
        )));
    }
    Ok(())
}

impl ReceiptBuilder {
    /// Build the receipt and the exact bytes to store, signed with the
    /// keyring's active key when one is configured. A key with an id writes
    /// it in `receipt.mac_key_id`; the `manifest_mac_key_env` key has none.
    pub fn build_signed(self, keyring: &ManifestKeyring) -> Result<WrittenReceipt, VtopError> {
        check_uri("manifest", &self.manifest_uri)?;
        check_uri("object", &self.object_uri)?;
        let (key, key_id) = match keyring.signing_key() {
            Some((key, id)) => (Some(key), id),
            None => (None, None),
        };
        let mut receipt = VerificationReceipt {
            protocol: VTOP_PROTOCOL.to_owned(),
            receipt_version: RECEIPT_VERSION.to_owned(),
            batch_id: self.batch_id,
            manifest: ReceiptManifest {
                uri: self.manifest_uri,
                sha256: self.manifest_sha256,
                version_id: self.manifest_version_id,
            },
            object: ReceiptObject {
                uri: self.object_uri,
                size_bytes: self.object_size_bytes,
                checksum_algorithm: self.object_checksum_algorithm,
                checksum: self.object_checksum,
                version_id: self.object_version_id,
            },
            verification: ReceiptVerification {
                class: self.class,
                method: self.method,
            },
            verified_at: self.verified_at,
            verified_by: self.verified_by,
            origin: self.origin,
            receipt: ReceiptAuth {
                sha256: String::new(),
                // Presence is authenticated: blanked to "" in the canonical
                // form of a signed receipt, absent from an unsigned one.
                mac: key.map(|_| String::new()),
                mac_key_id: key.and(key_id).map(str::to_owned),
            },
        };
        let auth = canonical_auth_bytes_of(serde_json::to_value(&receipt)?, "receipt")?;
        receipt.receipt.sha256 = sha256_bytes(&auth);
        if let Some(key) = key {
            receipt.receipt.mac = Some(key.authenticate(&auth).to_hex().to_string());
        }
        let bytes = serde_json::to_vec_pretty(&receipt)?;
        if bytes.len() > MAX_RECEIPT_BYTES {
            return Err(VtopError::Manifest(format!(
                "verification receipt exceeds the {MAX_RECEIPT_BYTES}-byte limit"
            )));
        }
        Ok(WrittenReceipt { receipt, bytes })
    }
}

/// A receipt this build wrote, with the exact bytes to upload.
#[derive(Debug, Clone)]
pub struct WrittenReceipt {
    receipt: VerificationReceipt,
    bytes: Vec<u8>,
}

impl WrittenReceipt {
    pub fn fields(&self) -> &VerificationReceipt {
        &self.receipt
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Persist the bytes to `work_dir` as `<batch_id>.receipt.json`,
    /// returning the local path.
    pub fn write_to_file(&self, work_dir: &Path) -> Result<PathBuf, VtopError> {
        std::fs::create_dir_all(work_dir)?;
        let path = work_dir.join(crate::partitioning::receipt_file_name(
            &self.receipt.batch_id,
        ));
        std::fs::write(&path, &self.bytes)?;
        Ok(path)
    }
}

/// A receipt read from storage, with the bytes its hash and MAC cover.
#[derive(Debug)]
pub struct ParsedReceipt {
    decoded: VerificationReceipt,
    auth_bytes: Vec<u8>,
}

impl ParsedReceipt {
    /// Parse a stored receipt: bounded, strict (no duplicate keys, integer
    /// numbers only), `protocol` and `receipt_version` checked before the
    /// rest is decoded. A newer `receipt_version` is reported as newer, not
    /// as damage. Fields a later release adds are kept in the canonical form,
    /// so they are authenticated, not dropped.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, VtopError> {
        if bytes.len() > MAX_RECEIPT_BYTES {
            return Err(VtopError::Manifest(format!(
                "verification receipt exceeds the {MAX_RECEIPT_BYTES}-byte limit"
            )));
        }
        let StrictJson(value, _) = serde_json::from_slice::<StrictJson<ReceiptDoc>>(bytes)
            .map_err(|e| {
                VtopError::Manifest(format!("verification receipt does not parse: {e}"))
            })?;
        if value.get("protocol").and_then(serde_json::Value::as_str) != Some(VTOP_PROTOCOL) {
            return Err(VtopError::Manifest(format!(
                "verification receipt protocol must be {VTOP_PROTOCOL}"
            )));
        }
        let version = value
            .get("receipt_version")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                VtopError::Manifest("verification receipt version must be a string".into())
            })?;
        if version != RECEIPT_VERSION {
            // Bounded: the value comes from storage and lands in logs.
            let shown: String = version.chars().take(32).collect();
            let newer = !version.is_empty()
                && version.bytes().all(|b| b.is_ascii_digit())
                && !version.starts_with('0')
                && !matches!(version.parse::<u64>(), Ok(v) if v <= 1);
            return Err(VtopError::Manifest(if newer {
                format!(
                    "verification receipt version {shown:?} is newer than this build reads \
                     ({RECEIPT_VERSION}); upgrade the reader"
                )
            } else {
                format!("malformed verification receipt version {shown:?}")
            }));
        }
        let decoded = VerificationReceipt::deserialize(&value)
            .map_err(|e| VtopError::Manifest(format!("verification receipt is malformed: {e}")))?;
        let auth_bytes = canonical_auth_bytes_of(value, "receipt")?;
        Ok(Self {
            decoded,
            auth_bytes,
        })
    }

    pub fn fields(&self) -> &VerificationReceipt {
        &self.decoded
    }

    /// The unkeyed self-hash over the canonical stored document.
    pub fn verify_self_hash(&self) -> Result<(), VtopError> {
        let expected = sha256_bytes(&self.auth_bytes);
        if expected.eq_ignore_ascii_case(&self.decoded.receipt.sha256) {
            Ok(())
        } else {
            Err(VtopError::Manifest(
                "verification receipt content does not match its embedded sha256".into(),
            ))
        }
    }

    /// The self-hash, then the MAC with the key the receipt names (#685), or
    /// the legacy key when it names none; never by trying every key. An
    /// unknown or retired key is unauthenticated, not corrupt
    /// ([`super::KeyResult`]).
    pub fn authenticate(&self, keyring: &ManifestKeyring) -> Authentication {
        if let Err(e) = self.verify_self_hash() {
            return Authentication {
                key: None,
                result: Err(e),
            };
        }
        keyring.check_document(
            "receipt",
            self.decoded.receipt.mac_key_id.as_deref(),
            self.decoded.receipt.mac.as_deref(),
            &self.auth_bytes,
        )
    }
}

//! Persisted row models for the state store.

use serde::{Deserialize, Serialize};
use vtop_core::manifest::VtopManifest;
use vtop_core::state_machine::BatchState;
use vtop_core::types::{ContentFingerprint, ProgressMarker, SourceType, TelemetryFormat};

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

/// Where a byte-offset source (file, syslog spool) stood after its most recent
/// committed batch: the cursor a restart resumes from.
///
/// The offset alone is a position in a file the ledger cannot name. After a
/// rotation it describes the OLD file; seeding it blindly resumed the NEW one
/// at the old one's high-water mark, so its first bytes were never archived
/// (#558). The inode lets the adapter tell the two apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedCursor {
    /// The source path the markers carry.
    pub path: String,
    /// The inode of the file the MOST RECENT committed batch was read from.
    /// `None` for a legacy marker that predates inode tracking, or on a
    /// platform that has none.
    pub inode: Option<u64>,
    /// The MOST RECENT committed batch's `end_byte` — not a maximum, across
    /// inodes or within one: after a `copytruncate` the inode is unchanged
    /// and the old content's higher offset would skip the new content's
    /// bytes (#558, review of #564).
    pub end_byte: u64,
    /// The length of the file as the MOST RECENT committed batch observed it
    /// (`file_size` in its marker); `None` where the marker records none (a
    /// row that predates the field - every syslog spool row before #578). It
    /// is the newest row's, not a maximum: after a `copytruncate` the file
    /// legitimately gets shorter, and a maximum would flag every restart as a
    /// truncation.
    pub observed_size: Option<u64>,
    /// The fingerprint of the file's first bytes that the MOST RECENT
    /// committed batch took - newest by `(created_at, batch_id)`, the order
    /// the windows were read, not `updated_at`, as for the fields above
    /// (#578): a file replaced at the same inode while
    /// the engine was down, already longer than the offset, is caught by it
    /// alone. `None` for a row that predates the field, or a file that was
    /// empty when read.
    pub content_fingerprint: Option<ContentFingerprint>,
}

/// A `content_fingerprint` read out of a stored marker as JSON text. Anything
/// that does not parse reads as "unknown", never as some other fingerprint:
/// an unknown fingerprint only forgoes a check, a wrong one would invent a
/// replacement.
///
/// Nor is a fingerprint no reader could have written (review of #578): a
/// zero length would hash nothing and match every file, and a digest that is
/// not 64 LOWERCASE hex digits - what every writer produces - can match none:
/// the comparisons are of hex text, so an uppercase digest would call the
/// unchanged file a replacement.
pub(crate) fn parse_content_fingerprint(json: &str) -> Option<ContentFingerprint> {
    let fingerprint: ContentFingerprint = serde_json::from_str(json).ok()?;
    let plausible = (1..=ContentFingerprint::MAX_LEN as u64).contains(&fingerprint.len)
        && fingerprint.sha256.len() == 64
        && fingerprint
            .sha256
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
    plausible.then_some(fingerprint)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Review of #578: a stored fingerprint no reader could have written is
    /// unknown, never a fingerprint - `len: 0` would match every file.
    #[test]
    fn an_implausible_stored_fingerprint_reads_as_unknown() {
        let good = ContentFingerprint::of(b"abc\n").unwrap();
        let json = serde_json::to_string(&good).unwrap();
        assert_eq!(parse_content_fingerprint(&json), Some(good.clone()));
        let empty_digest = vtop_core::checksum::sha256_bytes(b"");
        for bad in [
            format!(r#"{{"len":0,"sha256":"{empty_digest}"}}"#),
            format!(r#"{{"len":4097,"sha256":"{}"}}"#, good.sha256),
            r#"{"len":4,"sha256":"not-a-digest"}"#.to_owned(),
            format!(r#"{{"len":4,"sha256":"{}"}}"#, &good.sha256[..63]),
            format!(
                r#"{{"len":4,"sha256":"{}"}}"#,
                good.sha256.to_ascii_uppercase()
            ),
            "null".to_owned(),
            "{".to_owned(),
        ] {
            assert_eq!(parse_content_fingerprint(&bad), None, "{bad}");
        }
    }
}

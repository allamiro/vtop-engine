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
    /// Which class of verification the batch passed on its way to VERIFIED
    /// (#664), recorded with that transition. `None` for a row verified by
    /// a release before #664, or by one that wrote a class this build does
    /// not know: unknown, never assumed to be either class.
    ///
    /// Omitted from the serialized record when unknown, so a prune digest of
    /// a legacy row is byte-for-byte what an older release wrote; an older
    /// reader ignores the key when present.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_verification_class"
    )]
    pub verification_class: Option<VerificationClass>,
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

/// Which class of verification a batch passed when it became VERIFIED
/// (#664).
///
/// The recovery re-check of a VERIFIED row runs after a restart, against the
/// store as it is then, and can reach a different answer from the
/// verification that put the row in VERIFIED: a backend-limited result at
/// one and a digest match at the other. What the row passed is the
/// verification that admitted it, so it is recorded with the transition and
/// read back, not re-derived.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationClass {
    /// The stored content matched a digest: a checksum the store computed,
    /// or a read-back of the stored bytes.
    Strong,
    /// The store confirmed only size and existence (`backend_limited`),
    /// accepted under `upload.require_strong_verification: false`.
    SizeOnly,
}

impl VerificationClass {
    /// The text stored in the ledger's `verification_class` column.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strong => "strong",
            Self::SizeOnly => "size_only",
        }
    }

    /// The class a stored value names. NULL, and a value this build does not
    /// know (written by a newer release), are both `None`: unknown. An
    /// unknown class is decided the way a row from before #664 is, so a
    /// newer value can never be misread as one of these two.
    pub fn from_stored(stored: Option<&str>) -> Option<Self> {
        match stored? {
            "strong" => Some(Self::Strong),
            "size_only" => Some(Self::SizeOnly),
            _ => None,
        }
    }
}

/// Tells the operator that a migration added the verification class column
/// to a table that already held rows (#664): what happened, what it means for
/// those rows, and that nothing needs doing. Says nothing for an empty table:
/// no row is left without a class.
pub(crate) fn log_class_column_added(backend: &str, existing_rows: i64) {
    if existing_rows == 0 {
        return;
    }
    tracing::info!(
        backend,
        existing_rows,
        column = "verification_class",
        "state store: added the verification_class column; {existing_rows} existing rows left \
         NULL (verification class unknown). A VERIFIED row among them is counted, when recovery \
         commits it, by recovery's re-check of the stored object, as before this release. \
         Nothing to do"
    );
}

/// [`log_class_column_added`] when the rows could not be counted: the column
/// is there either way, so this is not a failure of the migration.
pub(crate) fn log_class_column_added_uncounted(backend: &str, error: &str) {
    tracing::warn!(
        backend,
        column = "verification_class",
        error,
        "state store: added the verification_class column, but could not count the existing \
         rows it left NULL (verification class unknown). The migration is complete; those rows' \
         recovered commits are counted by recovery's re-check, as before this release. Nothing \
         to do"
    );
}

/// Reads a serialized `verification_class` leniently: a value this build does
/// not know is unknown (`None`), never an error. A prune digest is read whole
/// or refused, so a strict enum would make one row from a newer release fail
/// the coverage audit of every row in its file.
fn deserialize_verification_class<'de, D>(d: D) -> Result<Option<VerificationClass>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let stored = Option::<String>::deserialize(d)?;
    Ok(VerificationClass::from_stored(stored.as_deref()))
}

impl BatchRecord {
    /// Whether this record still holds uncommitted source progress: it is in
    /// neither terminal state (SOURCE_COMMITTED, or SUPERSEDED, #567).
    pub fn is_incomplete(&self) -> bool {
        !self.state.is_terminal()
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
    /// The file's mtime as the MOST RECENT committed batch observed it (the
    /// marker's `mtime`, RFC 3339 as the file adapter writes it). The file
    /// source re-arms a deferred `delete_after_commit` after a restart only
    /// while the file still has this mtime (#619, review of #631): a
    /// same-inode, same-length rewrite while the engine was down agrees with
    /// every other field here. `None` where the marker records none or an
    /// empty one (a filesystem without mtimes, a syslog spool marker).
    pub observed_mtime: Option<String>,
    /// The fingerprint of the bytes ending at `end_byte` that the same row's
    /// marker took (#620): a file rewritten at the same inode while the
    /// engine was down, with the same first page, is caught by it. `None`
    /// for a row that predates the field (judged as before), a marker at
    /// byte 0, and every spool row. Read out of the stored JSON - the
    /// column already holds it, so no migration.
    pub tail_fingerprint: Option<ContentFingerprint>,
}

/// Where the ledger says a Kafka partition resumes, for one consumer group
/// (#614): the end offset of the NEWEST committed batch read from it.
///
/// The group's committed offset on the broker cannot answer this. The adapter
/// reads with `assign()`, so its commits carry no group generation, and Kafka
/// accepts one from an engine whose claim has already moved on - backwards or
/// forwards. The ledger's commit is fenced (#557); this is what it recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedKafkaOffset {
    pub topic: String,
    pub partition: i32,
    pub consumer_group: String,
    /// The newest committed batch's last offset; the partition resumes at the
    /// one after it. Newest by `(created_at, batch_id)` - the order the windows
    /// were read, and the row pruning keeps - not the highest: a topic that
    /// was deleted and recreated starts its offsets again, and the old topic's
    /// highest offset would skip the new one's first records.
    pub end_offset: i64,
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
    serde_json::from_str::<ContentFingerprint>(json)
        .ok()
        .filter(ContentFingerprint::is_plausible)
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
    /// The class of verification the batch passed (#664). Written only with
    /// the transition to VERIFIED; a store refuses it on any other.
    pub verification_class: Option<VerificationClass>,
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
            verification_class: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #664: a serialized record (a prune digest row) carries the class only
    /// when it is known, so a legacy row serializes as an older release wrote
    /// it, and a value this build does not know reads as unknown rather than
    /// failing the whole digest.
    #[test]
    fn a_serialized_verification_class_is_optional_and_read_leniently() {
        let mut rec = crate::test_battery::sample_record("serde-664");
        let legacy = serde_json::to_value(&rec).unwrap();
        assert!(
            legacy.get("verification_class").is_none(),
            "an unknown class must not be written: {legacy}"
        );
        assert_eq!(
            serde_json::from_value::<BatchRecord>(legacy.clone())
                .unwrap()
                .verification_class,
            None
        );
        rec.verification_class = Some(VerificationClass::SizeOnly);
        let known = serde_json::to_value(&rec).unwrap();
        assert_eq!(known["verification_class"], "size_only");
        assert_eq!(
            serde_json::from_value::<BatchRecord>(known)
                .unwrap()
                .verification_class,
            Some(VerificationClass::SizeOnly)
        );
        for stored in [serde_json::json!("quantum"), serde_json::Value::Null] {
            let mut newer = legacy.clone();
            newer["verification_class"] = stored.clone();
            assert_eq!(
                serde_json::from_value::<BatchRecord>(newer)
                    .unwrap_or_else(|e| panic!("{stored}: {e}"))
                    .verification_class,
                None,
                "{stored}"
            );
        }
    }

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

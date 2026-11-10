//! Common protocol-independent type aliases and enums for the VTOP Engine.

use serde::{Deserialize, Serialize};

/// Unique identifier for a telemetry batch.
pub type BatchId = String;
/// Logical tenant that owns a batch / stream.
pub type TenantId = String;
/// Human-readable source name (topic, file path, spool id, ...).
pub type SourceName = String;
/// Fully-qualified object storage URI (e.g. `s3://bucket/key`).
pub type ObjectUri = String;
/// Fully-qualified manifest storage URI.
pub type ManifestUri = String;

/// The category of telemetry source a batch was produced from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    Kafka,
    File,
    SyslogSpool,
}

impl SourceType {
    /// Stable string used in the state store and CLI.
    pub fn as_str(&self) -> &'static str {
        match self {
            SourceType::Kafka => "kafka",
            SourceType::File => "file",
            SourceType::SyslogSpool => "syslog_spool",
        }
    }
}

impl std::str::FromStr for SourceType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "kafka" => Ok(SourceType::Kafka),
            "file" => Ok(SourceType::File),
            "syslog_spool" | "syslog-spool" | "syslog" => Ok(SourceType::SyslogSpool),
            other => Err(format!("unknown source type: {other}")),
        }
    }
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The on-the-wire telemetry record format inside a batch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryFormat {
    Cef,
    Leef,
    Json,
    Jsonl,
    Syslog,
    Raw,
}

impl TelemetryFormat {
    /// Object file extension fragment for this format (no leading dot).
    pub fn extension(&self) -> &'static str {
        match self {
            TelemetryFormat::Cef => "cef",
            TelemetryFormat::Leef => "leef",
            TelemetryFormat::Json => "json",
            TelemetryFormat::Jsonl => "jsonl",
            TelemetryFormat::Syslog => "syslog",
            TelemetryFormat::Raw => "raw",
        }
    }

    pub fn as_str(&self) -> &'static str {
        self.extension()
    }
}

impl std::str::FromStr for TelemetryFormat {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "cef" => Ok(TelemetryFormat::Cef),
            "leef" => Ok(TelemetryFormat::Leef),
            "json" => Ok(TelemetryFormat::Json),
            "jsonl" | "ndjson" => Ok(TelemetryFormat::Jsonl),
            "syslog" => Ok(TelemetryFormat::Syslog),
            "raw" => Ok(TelemetryFormat::Raw),
            other => Err(format!("unknown telemetry format: {other}")),
        }
    }
}

impl std::fmt::Display for TelemetryFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Compression algorithm applied to a sealed batch.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompressionType {
    Gzip,
    Zstd,
    None,
}

impl CompressionType {
    /// File extension fragment for the compressed object (no leading dot),
    /// or `None` when compression is disabled.
    pub fn extension(&self) -> Option<&'static str> {
        match self {
            CompressionType::Gzip => Some("gz"),
            CompressionType::Zstd => Some("zst"),
            CompressionType::None => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionType::Gzip => "gzip",
            CompressionType::Zstd => "zstd",
            CompressionType::None => "none",
        }
    }
}

impl std::str::FromStr for CompressionType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "gzip" | "gz" => Ok(CompressionType::Gzip),
            "zstd" | "zst" => Ok(CompressionType::Zstd),
            "none" => Ok(CompressionType::None),
            other => Err(format!("unknown compression type: {other}")),
        }
    }
}

/// Checksum algorithm used for object integrity. `None` disables checksums
/// (verification falls back to size + existence only — a comparison mode).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChecksumAlgorithm {
    Sha256,
    Blake3,
    None,
}

impl ChecksumAlgorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChecksumAlgorithm::Sha256 => "sha256",
            ChecksumAlgorithm::Blake3 => "blake3",
            ChecksumAlgorithm::None => "none",
        }
    }

    /// Whether this algorithm produces a digest (false when disabled).
    pub fn is_enabled(&self) -> bool {
        !matches!(self, ChecksumAlgorithm::None)
    }
}

impl std::str::FromStr for ChecksumAlgorithm {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "sha256" | "sha-256" => Ok(ChecksumAlgorithm::Sha256),
            "blake3" | "blake-3" => Ok(ChecksumAlgorithm::Blake3),
            "none" | "disabled" | "off" => Ok(ChecksumAlgorithm::None),
            other => Err(format!("unknown checksum algorithm: {other}")),
        }
    }
}

impl std::fmt::Display for ChecksumAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Source-agnostic progress marker. This is the central object that binds a
/// physical source position to a batch, and ultimately to a verified object.
///
/// A marker MUST NOT be committed back to the source until the batch reaches
/// [`crate::state_machine::BatchState::Verified`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "source_type", rename_all = "snake_case")]
pub enum ProgressMarker {
    Kafka {
        topic: String,
        partition: i32,
        start_offset: i64,
        end_offset: i64,
        consumer_group: String,
    },
    File {
        path: String,
        inode: Option<u64>,
        start_byte: u64,
        end_byte: u64,
        file_size: u64,
        mtime: String,
        /// What the file's first bytes were when this read opened it (#578).
        /// Path, inode and length cannot tell a file from a replacement that
        /// reuses the inode and is already longer than the old read position;
        /// its content can. `None` on a row written before the field existed
        /// (judged by inode and length alone, as it always was) and for a
        /// file that was empty at the read. Omitted when `None`, so such a
        /// marker serializes byte for byte as it did before.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_fingerprint: Option<ContentFingerprint>,
    },
    SyslogSpool {
        spool_id: String,
        path: String,
        /// Identity of the spool file the bytes were read from (fstat on the
        /// open descriptor, #127). `serde(default)` keeps pre-fingerprint
        /// markers deserializable; `None` means "unknown", never "checked".
        #[serde(default)]
        inode: Option<u64>,
        start_byte: u64,
        end_byte: u64,
        received_time_start: Option<String>,
        received_time_end: Option<String>,
        /// The spool's length as the read saw it (#578): an append-only file
        /// never gets shorter, so a later read that saw it shorter read new
        /// content. `None` on a row written before the field existed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file_size: Option<u64>,
        /// See [`ProgressMarker::File`]'s field of the same name.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_fingerprint: Option<ContentFingerprint>,
    },
}

/// The identity of a file's content (#578): the SHA-256 of its first `len`
/// bytes, where `len` is the file's length when the read opened it, capped
/// at [`ContentFingerprint::MAX_LEN`].
///
/// A file shorter than the cap fingerprints all of itself, so the same file
/// fingerprints differently as it grows: two fingerprints of DIFFERENT
/// lengths say nothing about each other ([`ContentFingerprint::same_content`]
/// answers `None`). The adapter, which has the file, checks a shorter one
/// against the file's own first bytes ([`ContentFingerprint::matches_head`]).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContentFingerprint {
    /// How many leading bytes the hash covers.
    pub len: u64,
    /// Lowercase hex SHA-256 of those bytes.
    pub sha256: String,
}

impl ContentFingerprint {
    /// How much of a file's start is hashed: one page, read once per open.
    pub const MAX_LEN: usize = 4096;

    /// The fingerprint of a file whose first bytes are `head` (at most
    /// [`Self::MAX_LEN`] of them are used). `None` for an empty file: there
    /// is no content to identify, and a fingerprint of nothing would match
    /// every file.
    pub fn of(head: &[u8]) -> Option<Self> {
        let head = &head[..head.len().min(Self::MAX_LEN)];
        if head.is_empty() {
            return None;
        }
        Some(Self {
            len: head.len() as u64,
            sha256: crate::checksum::sha256_bytes(head),
        })
    }

    /// Whether a file whose first bytes are `head` is the file this
    /// fingerprint was taken from. A file now shorter than the fingerprint
    /// does not match: an append-only file never loses its start.
    pub fn matches_head(&self, head: &[u8]) -> bool {
        usize::try_from(self.len)
            .ok()
            .and_then(|len| head.get(..len))
            .is_some_and(|prefix| crate::checksum::sha256_bytes(prefix) == self.sha256)
    }

    /// Whether two fingerprints show the same content: `Some` when they
    /// cover the same number of bytes, `None` when they do not and so cannot
    /// be compared.
    pub fn same_content(&self, other: &Self) -> Option<bool> {
        (self.len == other.len).then(|| self.sha256 == other.sha256)
    }
}

impl ProgressMarker {
    /// The [`SourceType`] this marker belongs to.
    pub fn source_type(&self) -> SourceType {
        match self {
            ProgressMarker::Kafka { .. } => SourceType::Kafka,
            ProgressMarker::File { .. } => SourceType::File,
            ProgressMarker::SyslogSpool { .. } => SourceType::SyslogSpool,
        }
    }

    /// A short, stable, filesystem-safe token describing the position range.
    /// Used to build deterministic batch ids.
    pub fn range_token(&self) -> String {
        match self {
            ProgressMarker::Kafka {
                partition,
                start_offset,
                end_offset,
                ..
            } => format!("p{partition}-{start_offset}-{end_offset}"),
            ProgressMarker::File {
                start_byte,
                end_byte,
                ..
            } => format!("b{start_byte}-{end_byte}"),
            ProgressMarker::SyslogSpool {
                start_byte,
                end_byte,
                ..
            } => format!("b{start_byte}-{end_byte}"),
        }
    }

    /// This marker as a manifest carries it: without the fields #578 added.
    ///
    /// A manifest is verified by decoding it and hashing the re-encoded
    /// struct, and serde drops fields a reader does not know. An older
    /// `verify-manifest` would re-encode a manifest carrying them without
    /// them, and report a good manifest as a checksum mismatch. The ledger
    /// row keeps the full marker; the manifest stays the format every
    /// released reader can verify.
    pub fn for_manifest(&self) -> Self {
        let mut marker = self.clone();
        match &mut marker {
            ProgressMarker::Kafka { .. } => {}
            ProgressMarker::File {
                content_fingerprint,
                ..
            } => *content_fingerprint = None,
            ProgressMarker::SyslogSpool {
                file_size,
                content_fingerprint,
                ..
            } => {
                *file_size = None;
                *content_fingerprint = None;
            }
        }
        marker
    }

    /// The content fingerprint a file or spool marker carries, if any.
    pub fn content_fingerprint(&self) -> Option<&ContentFingerprint> {
        match self {
            ProgressMarker::Kafka { .. } => None,
            ProgressMarker::File {
                content_fingerprint,
                ..
            }
            | ProgressMarker::SyslogSpool {
                content_fingerprint,
                ..
            } => content_fingerprint.as_ref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Markers exactly as the release before #578 wrote them into the ledger:
    /// serialized by that code (origin/main at 5408a45), not by this one.
    const OLD_FILE_ROW: &str = r#"{"source_type":"file","path":"/var/log/app.log","inode":877,"start_byte":0,"end_byte":100,"file_size":8192,"mtime":"2026-01-01T00:00:30Z"}"#;
    const OLD_SPOOL_ROW: &str = r#"{"source_type":"syslog_spool","spool_id":"spool.log","path":"/var/spool/rsyslog/spool.log","inode":4242,"start_byte":0,"end_byte":100,"received_time_start":null,"received_time_end":null}"#;

    /// #578: rows written before the fingerprint decode, with no fingerprint
    /// and (spool) no size - "unknown", which every reader judges as before.
    #[test]
    fn markers_written_before_the_fingerprint_still_decode_as_unknown() {
        let file: ProgressMarker = serde_json::from_str(OLD_FILE_ROW).unwrap();
        assert_eq!(
            file,
            ProgressMarker::File {
                path: "/var/log/app.log".into(),
                inode: Some(877),
                start_byte: 0,
                end_byte: 100,
                file_size: 8192,
                mtime: "2026-01-01T00:00:30Z".into(),
                content_fingerprint: None,
            }
        );
        let spool: ProgressMarker = serde_json::from_str(OLD_SPOOL_ROW).unwrap();
        assert_eq!(
            spool,
            ProgressMarker::SyslogSpool {
                spool_id: "spool.log".into(),
                path: "/var/spool/rsyslog/spool.log".into(),
                inode: Some(4242),
                start_byte: 0,
                end_byte: 100,
                received_time_start: None,
                received_time_end: None,
                file_size: None,
                content_fingerprint: None,
            }
        );
        // And a marker with nothing new to say is written exactly as before:
        // a manifest built from it hashes the same, and a ledger row is
        // indistinguishable from an old one.
        assert_eq!(serde_json::to_string(&file).unwrap(), OLD_FILE_ROW);
        assert_eq!(serde_json::to_string(&spool).unwrap(), OLD_SPOOL_ROW);
    }

    #[test]
    fn a_fingerprinted_marker_round_trips_and_a_manifest_does_not_carry_it() {
        let marker = ProgressMarker::SyslogSpool {
            spool_id: "s".into(),
            path: "/s".into(),
            inode: Some(1),
            start_byte: 0,
            end_byte: 4,
            received_time_start: None,
            received_time_end: None,
            file_size: Some(4),
            content_fingerprint: ContentFingerprint::of(b"abc\n"),
        };
        let json = serde_json::to_string(&marker).unwrap();
        assert!(json.contains("content_fingerprint") && json.contains("file_size"));
        assert_eq!(
            serde_json::from_str::<ProgressMarker>(&json).unwrap(),
            marker
        );
        let manifest = serde_json::to_string(&marker.for_manifest()).unwrap();
        assert!(
            !manifest.contains("content_fingerprint") && !manifest.contains("file_size"),
            "an older verify-manifest drops what it does not know and would fail the \
             self-hash: {manifest}"
        );
    }

    #[test]
    fn a_fingerprint_covers_at_most_one_page_and_an_empty_file_has_none() {
        assert_eq!(ContentFingerprint::of(b""), None);
        let big = vec![7u8; 10_000];
        let fp = ContentFingerprint::of(&big).unwrap();
        assert_eq!(fp.len, ContentFingerprint::MAX_LEN as u64);
        assert_eq!(
            Some(fp),
            ContentFingerprint::of(&big[..ContentFingerprint::MAX_LEN])
        );
    }

    /// A file under one page fingerprints all of itself, so the fingerprint
    /// changes as it grows. A shorter fingerprint is still checked against
    /// the grown file's first bytes; two of different lengths cannot be
    /// compared with each other, and say so rather than guessing.
    #[test]
    fn a_growing_small_file_still_matches_its_earlier_fingerprint() {
        let early = ContentFingerprint::of(b"line one\n").unwrap();
        let grown = b"line one\nline two\n";
        assert!(early.matches_head(grown));
        assert!(!early.matches_head(b"line ONE\nline two\n"), "rewritten");
        assert!(!early.matches_head(b"line"), "now shorter than it was");
        let later = ContentFingerprint::of(grown).unwrap();
        assert_eq!(early.same_content(&later), None);
        assert_eq!(later.same_content(&later.clone()), Some(true));
        let other = ContentFingerprint::of(b"other on\nline two\n").unwrap();
        assert_eq!(later.same_content(&other), Some(false));
    }
}

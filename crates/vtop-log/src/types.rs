use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use thiserror::Error;
use uuid::Uuid;

pub const FORMAT_NAME: &str = "vtop-native-segment";
pub const FORMAT_VERSION: u16 = 1;
pub const DEFAULT_MAX_RECORD_BYTES: u32 = 16 * 1024 * 1024;
pub const DEFAULT_MAX_GROUP_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_MAX_SEGMENT_BYTES: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_MAX_SEGMENT_RECORDS: u64 = 1_000_000;
pub const MAX_RECORD_BYTES: u32 = 64 * 1024 * 1024;
pub const MAX_GROUP_BYTES: u64 = 256 * 1024 * 1024;
pub const MAX_SEGMENT_BYTES: u64 = 8 * 1024 * 1024 * 1024;
pub const MAX_SEGMENT_RECORDS: u64 = 10_000_000;
/// Bytes added around every key/value payload by the v1 record frame.
pub const RECORD_FRAME_OVERHEAD_BYTES: u64 = 12 + 8 + 16 + 8 + 8 + 4 + 4 + 32;
/// Format version written by the proof-carrying v2 segment envelope.
pub const FORMAT_VERSION_V2: u16 = 2;
/// Record schema carried inside v2 segments.
pub const RECORD_SCHEMA_VERSION_V2: u16 = 2;
pub const DEFAULT_CHUNK_SIZE_BYTES: u32 = 1024 * 1024;
pub const MIN_CHUNK_SIZE_BYTES: u32 = 64 * 1024;
pub const MAX_CHUNK_SIZE_BYTES: u32 = 16 * 1024 * 1024;
/// Chunk-tree scheme identifier recorded in v2 manifests.
pub const CHUNK_TREE_SCHEME_V1: &str = "vtop-b3tree-v1";
/// Magic prefix of the `.chunks` sidecar that stores v2 chunk-tree leaves.
///
/// The sidecar encoding itself ships with the v2 writer; only the constant is
/// frozen here so the on-disk name cannot drift between the two changes.
pub const CHUNK_SIDECAR_MAGIC: &[u8; 8] = b"VTOPCHK1";
/// Commit-statement scheme when a runtime commit key is configured.
pub const COMMIT_SCHEME_KEYED: &str = "blake3-keyed";
/// Commit-statement scheme when no commit key is configured.
pub const COMMIT_SCHEME_UNKEYED: &str = "unkeyed-digest";
/// Domain separator prefixed to canonical commit-statement bytes before
/// hashing, so the MAC can never collide with any other keyed use of BLAKE3.
const COMMIT_STATEMENT_DOMAIN: &[u8] = b"vtop-segment-v2 commit-statement v1\0";

pub type SegmentId = Uuid;

#[derive(Debug, Error)]
pub enum LogError {
    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid segment configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid segment descriptor: {0}")]
    InvalidDescriptor(String),
    #[error("corrupt segment at byte {position}: {reason}")]
    Corrupt { position: u64, reason: String },
    #[error("unsupported segment format version {0}")]
    UnsupportedVersion(u16),
    #[error("producer {producer_id} must start at sequence 0, got {actual}")]
    FirstSequence { producer_id: Uuid, actual: u64 },
    #[error("producer {producer_id} sequence gap: expected {expected}, got {actual}")]
    SequenceGap {
        producer_id: Uuid,
        expected: u64,
        actual: u64,
    },
    #[error("producer {producer_id} reused sequence {sequence} with different record content")]
    SequenceConflict { producer_id: Uuid, sequence: u64 },
    #[error("record is {actual} bytes; configured maximum is {maximum} bytes")]
    RecordTooLarge { actual: usize, maximum: u32 },
    #[error("append group is {actual} bytes; configured maximum is {maximum} bytes")]
    GroupTooLarge { actual: u64, maximum: u64 },
    #[error("append would grow segment from {current} to {attempted} bytes; maximum is {maximum}")]
    SegmentByteLimit {
        current: u64,
        attempted: u64,
        maximum: u64,
    },
    #[error("append would grow segment to {attempted} records; maximum is {maximum}")]
    SegmentRecordLimit { attempted: u64, maximum: u64 },
    #[error("segment is already sealed")]
    AlreadySealed,
    #[error("segment writer is poisoned after a partial write; recover it before appending")]
    WriterPoisoned,
    #[error("manifest does not match the segment: {0}")]
    ManifestMismatch(String),
    #[error("commit boundary does not match the segment: {0}")]
    CommitBoundaryMismatch(String),
    #[error("invalid segment cursor: {0}")]
    InvalidCursor(String),
    #[error("record field {0} is not supported by this segment format version")]
    UnsupportedRecordField(&'static str),
}

pub type VtopLogResult<T> = Result<T, LogError>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentRange {
    pub range_id: Uuid,
    pub generation: u64,
    pub key_range: KeyRange,
}

/// A buddy-aligned prefix in the unsigned 64-bit routing-key hash space.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRange {
    pub prefix: u64,
    pub prefix_bits: u8,
}

impl KeyRange {
    pub const fn full() -> Self {
        Self {
            prefix: 0,
            prefix_bits: 0,
        }
    }

    pub fn new(prefix: u64, prefix_bits: u8) -> VtopLogResult<Self> {
        let range = Self {
            prefix,
            prefix_bits,
        };
        range.validate()?;
        Ok(range)
    }

    pub fn contains(self, key_hash: u64) -> bool {
        self.validate().is_ok()
            && (self.prefix_bits == 0 || (key_hash & self.mask()) == self.prefix)
    }

    pub fn children(self) -> VtopLogResult<(Self, Self)> {
        self.validate()?;
        if self.prefix_bits == 64 {
            return Err(LogError::InvalidDescriptor(
                "a single-key range cannot be split".to_owned(),
            ));
        }
        let child_bits = self.prefix_bits + 1;
        let split_bit = 1_u64 << (64 - child_bits);
        Ok((
            Self {
                prefix: self.prefix,
                prefix_bits: child_bits,
            },
            Self {
                prefix: self.prefix | split_bit,
                prefix_bits: child_bits,
            },
        ))
    }

    pub fn buddy(self) -> VtopLogResult<Self> {
        self.validate()?;
        if self.prefix_bits == 0 {
            return Err(LogError::InvalidDescriptor(
                "the full keyspace has no buddy".to_owned(),
            ));
        }
        let buddy_bit = 1_u64 << (64 - self.prefix_bits);
        Ok(Self {
            prefix: self.prefix ^ buddy_bit,
            prefix_bits: self.prefix_bits,
        })
    }

    pub fn parent(self) -> VtopLogResult<Self> {
        self.validate()?;
        if self.prefix_bits == 0 {
            return Err(LogError::InvalidDescriptor(
                "the full keyspace has no parent".to_owned(),
            ));
        }
        let prefix_bits = self.prefix_bits - 1;
        let mask = if prefix_bits == 0 {
            0
        } else {
            u64::MAX << (64 - prefix_bits)
        };
        Ok(Self {
            prefix: self.prefix & mask,
            prefix_bits,
        })
    }

    fn mask(self) -> u64 {
        if self.prefix_bits == 0 {
            0
        } else {
            u64::MAX << (64 - self.prefix_bits)
        }
    }

    pub(crate) fn validate(self) -> VtopLogResult<()> {
        if self.prefix_bits > 64 {
            return Err(LogError::InvalidDescriptor(
                "key-range prefix length cannot exceed 64 bits".to_owned(),
            ));
        }
        if self.prefix & !self.mask() != 0 {
            return Err(LogError::InvalidDescriptor(
                "key-range prefix has non-zero bits outside its prefix length".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeLineage {
    pub range_id: Uuid,
    pub generation: u64,
    pub key_range: KeyRange,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parents: Vec<ParentRange>,
}

impl RangeLineage {
    pub fn root(range_id: Uuid) -> Self {
        Self {
            range_id,
            generation: 0,
            key_range: KeyRange::full(),
            parents: Vec::new(),
        }
    }

    pub(crate) fn validate(&self) -> VtopLogResult<()> {
        self.key_range.validate()?;
        if self.generation == 0 {
            if !self.parents.is_empty() || self.key_range != KeyRange::full() {
                return Err(LogError::InvalidDescriptor(
                    "generation-zero lineage must be the parentless full keyspace".to_owned(),
                ));
            }
            return Ok(());
        }
        if self.parents.is_empty() || self.parents.len() > 2 {
            return Err(LogError::InvalidDescriptor(
                "non-root range lineage must name one split parent or two merge parents".to_owned(),
            ));
        }
        for parent in &self.parents {
            parent.key_range.validate()?;
        }
        if self
            .parents
            .iter()
            .any(|parent| parent.range_id == self.range_id || parent.generation >= self.generation)
        {
            return Err(LogError::InvalidDescriptor(
                "range parents must be distinct older generations".to_owned(),
            ));
        }
        let mut ids: Vec<_> = self.parents.iter().map(|parent| parent.range_id).collect();
        ids.sort_unstable();
        ids.dedup();
        if ids.len() != self.parents.len() {
            return Err(LogError::InvalidDescriptor(
                "range lineage contains duplicate parents".to_owned(),
            ));
        }
        let expected_parent_generation = self.generation - 1;
        if self
            .parents
            .iter()
            .any(|parent| parent.generation != expected_parent_generation)
        {
            return Err(LogError::InvalidDescriptor(
                "range lineage must name direct parents from the previous generation".to_owned(),
            ));
        }
        match self.parents.as_slice() {
            [parent] => {
                let (left, right) = parent.key_range.children()?;
                if self.key_range != left && self.key_range != right {
                    return Err(LogError::InvalidDescriptor(
                        "single-parent lineage must be an exact buddy split child".to_owned(),
                    ));
                }
            }
            [left, right] => {
                if left.key_range.buddy()? != right.key_range
                    || left.key_range.parent()? != self.key_range
                    || right.key_range.parent()? != self.key_range
                {
                    return Err(LogError::InvalidDescriptor(
                        "two-parent lineage must merge exact buddy ranges into their parent"
                            .to_owned(),
                    ));
                }
            }
            _ => unreachable!("parent count was validated above"),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentDescriptor {
    pub segment_id: SegmentId,
    pub topic: String,
    pub topic_epoch: u64,
    pub lineage: RangeLineage,
    pub base_offset: u64,
}

impl SegmentDescriptor {
    pub(crate) fn validate(&self) -> VtopLogResult<()> {
        if self.topic.is_empty() || self.topic.len() > 249 {
            return Err(LogError::InvalidDescriptor(
                "topic length must be between 1 and 249 bytes".to_owned(),
            ));
        }
        if self
            .topic
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')))
        {
            return Err(LogError::InvalidDescriptor(
                "topic may contain only ASCII letters, digits, '.', '_' and '-'".to_owned(),
            ));
        }
        self.lineage.validate()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentConfig {
    pub max_record_bytes: u32,
    pub max_group_bytes: u64,
    pub max_segment_bytes: u64,
    pub max_segment_records: u64,
    pub index_stride: u32,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
            max_group_bytes: DEFAULT_MAX_GROUP_BYTES,
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            max_segment_records: DEFAULT_MAX_SEGMENT_RECORDS,
            index_stride: 256,
        }
    }
}

impl SegmentConfig {
    pub(crate) fn validate(self) -> VtopLogResult<Self> {
        if self.max_record_bytes == 0 || self.max_record_bytes > MAX_RECORD_BYTES {
            return Err(LogError::InvalidConfig(format!(
                "max_record_bytes must be in 1..={MAX_RECORD_BYTES}"
            )));
        }
        if self.max_group_bytes == 0 || self.max_group_bytes > MAX_GROUP_BYTES {
            return Err(LogError::InvalidConfig(format!(
                "max_group_bytes must be in 1..={MAX_GROUP_BYTES}"
            )));
        }
        let minimum_group_bytes = u64::from(self.max_record_bytes)
            .checked_add(RECORD_FRAME_OVERHEAD_BYTES)
            .expect("record limits are bounded constants");
        if self.max_group_bytes < minimum_group_bytes {
            return Err(LogError::InvalidConfig(
                "max_group_bytes must fit max_record_bytes plus v1 frame overhead".to_owned(),
            ));
        }
        if self.max_segment_bytes < self.max_group_bytes
            || self.max_segment_bytes > MAX_SEGMENT_BYTES
        {
            return Err(LogError::InvalidConfig(format!(
                "max_segment_bytes must be in max_group_bytes..={MAX_SEGMENT_BYTES}"
            )));
        }
        if self.max_segment_records == 0 || self.max_segment_records > MAX_SEGMENT_RECORDS {
            return Err(LogError::InvalidConfig(format!(
                "max_segment_records must be in 1..={MAX_SEGMENT_RECORDS}"
            )));
        }
        if self.index_stride == 0 {
            return Err(LogError::InvalidConfig(
                "index_stride must be greater than zero".to_owned(),
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    pub producer_id: Uuid,
    /// Producer fencing epoch. Persisted by schema v2; the v1 append path
    /// rejects nonzero values because the v1 frame cannot represent them.
    pub producer_epoch: u64,
    pub sequence: u64,
    pub timestamp_millis: i64,
    /// Record attribute bits. Reserved: must be zero in schema v2.
    pub attributes: u16,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Return after the bytes have been written to the operating system.
    Buffered,
    /// Flush file data and a checksummed commit boundary before acknowledging.
    Fsync,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendOutcome {
    Appended { offset: u64 },
    Duplicate { offset: u64 },
}

impl AppendOutcome {
    pub fn offset(self) -> u64 {
        match self {
            Self::Appended { offset } | Self::Duplicate { offset } => offset,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchedRecord {
    pub offset: u64,
    pub record: LogRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchBatch {
    pub records: Vec<FetchedRecord>,
    pub encoded_bytes: usize,
    pub next_offset: u64,
    pub high_watermark: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryReport {
    pub records: u64,
    pub recovered_bytes: u64,
    pub truncated_bytes: u64,
    pub next_offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Canonical metadata for the v1 sealed local segment.
///
/// `blake3_root` is the linear BLAKE3 digest of all encoded record frames in
/// order. It detects whole-segment mutation but is not a chunk tree and does
/// not support independent chunk proofs. A future proof-carrying format must
/// use a new version rather than silently changing this v1 contract.
pub struct SegmentManifest {
    pub format: String,
    pub version: u16,
    pub descriptor: SegmentDescriptor,
    pub record_count: u64,
    pub first_offset: Option<u64>,
    pub next_offset: u64,
    pub content_bytes: u64,
    pub blake3_root: String,
    pub index_stride: u32,
}

/// Tamper-evident progress within a sealed segment.
///
/// Binding progress to the topic epoch, range generation, segment identity,
/// and content root keeps a checkpoint unambiguous across future split/merge
/// topology changes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentCursor {
    pub topic: String,
    pub topic_epoch: u64,
    pub range_id: Uuid,
    pub range_generation: u64,
    pub segment_id: SegmentId,
    pub segment_root: String,
    pub offset: u64,
}

/// Identity of a v2 segment, extending v1 with generation and creator fencing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentDescriptorV2 {
    pub segment_id: SegmentId,
    pub topic: String,
    pub topic_epoch: u64,
    pub lineage: RangeLineage,
    pub base_offset: u64,
    pub segment_generation: u64,
    pub creation_node_id: Uuid,
    pub creation_fencing_epoch: u64,
}

impl SegmentDescriptorV2 {
    pub(crate) fn validate(&self) -> VtopLogResult<()> {
        if self.topic.is_empty() || self.topic.len() > 249 {
            return Err(LogError::InvalidDescriptor(
                "topic length must be between 1 and 249 bytes".to_owned(),
            ));
        }
        if self
            .topic
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')))
        {
            return Err(LogError::InvalidDescriptor(
                "topic may contain only ASCII letters, digits, '.', '_' and '-'".to_owned(),
            ));
        }
        self.lineage.validate()
    }
}

/// Limits for a v2 segment: the v1 limits plus the proof chunk size.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentConfigV2 {
    pub max_record_bytes: u32,
    pub max_group_bytes: u64,
    pub max_segment_bytes: u64,
    pub max_segment_records: u64,
    pub index_stride: u32,
    pub chunk_size: u32,
}

impl Default for SegmentConfigV2 {
    fn default() -> Self {
        Self {
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
            max_group_bytes: DEFAULT_MAX_GROUP_BYTES,
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            max_segment_records: DEFAULT_MAX_SEGMENT_RECORDS,
            index_stride: 256,
            chunk_size: DEFAULT_CHUNK_SIZE_BYTES,
        }
    }
}

impl SegmentConfigV2 {
    pub(crate) fn validate(self) -> VtopLogResult<Self> {
        if self.max_record_bytes == 0 || self.max_record_bytes > MAX_RECORD_BYTES {
            return Err(LogError::InvalidConfig(format!(
                "max_record_bytes must be in 1..={MAX_RECORD_BYTES}"
            )));
        }
        if self.max_group_bytes == 0 || self.max_group_bytes > MAX_GROUP_BYTES {
            return Err(LogError::InvalidConfig(format!(
                "max_group_bytes must be in 1..={MAX_GROUP_BYTES}"
            )));
        }
        let minimum_group_bytes = u64::from(self.max_record_bytes)
            .checked_add(crate::codec_v2::RECORD_FRAME_OVERHEAD_BYTES_V2)
            .expect("record limits are bounded constants");
        if self.max_group_bytes < minimum_group_bytes {
            return Err(LogError::InvalidConfig(
                "max_group_bytes must fit max_record_bytes plus v2 frame overhead".to_owned(),
            ));
        }
        if self.max_segment_bytes < self.max_group_bytes
            || self.max_segment_bytes > MAX_SEGMENT_BYTES
        {
            return Err(LogError::InvalidConfig(format!(
                "max_segment_bytes must be in max_group_bytes..={MAX_SEGMENT_BYTES}"
            )));
        }
        if self.max_segment_records == 0 || self.max_segment_records > MAX_SEGMENT_RECORDS {
            return Err(LogError::InvalidConfig(format!(
                "max_segment_records must be in 1..={MAX_SEGMENT_RECORDS}"
            )));
        }
        if self.index_stride == 0 {
            return Err(LogError::InvalidConfig(
                "index_stride must be greater than zero".to_owned(),
            ));
        }
        if !self.chunk_size.is_power_of_two()
            || !(MIN_CHUNK_SIZE_BYTES..=MAX_CHUNK_SIZE_BYTES).contains(&self.chunk_size)
        {
            return Err(LogError::InvalidConfig(format!(
                "chunk_size must be a power of two in {MIN_CHUNK_SIZE_BYTES}..={MAX_CHUNK_SIZE_BYTES}"
            )));
        }
        Ok(self)
    }
}

/// Per-(producer, epoch) sequence coverage recorded in a sealed v2 manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerSummaryEntry {
    pub producer_id: Uuid,
    pub producer_epoch: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub record_count: u64,
}

/// Placement evidence attached by replication and tiering; opaque to storage.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentEvidence {
    pub replicas: Vec<serde_json::Value>,
    pub tiers: Vec<serde_json::Value>,
}

/// Canonical metadata for a sealed v2 segment.
///
/// Unlike the v1 linear `blake3_root`, `chunk_tree_root` commits to a
/// domain-separated BLAKE3 chunk tree, so any chunk can later be verified
/// independently with a logarithmic proof (see [`crate::proof`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentManifestV2 {
    pub format: String,
    pub version: u16,
    pub record_schema_version: u16,
    pub descriptor: SegmentDescriptorV2,
    pub record_count: u64,
    pub first_offset: Option<u64>,
    pub next_offset: u64,
    pub content_bytes: u64,
    pub committed_high_watermark: u64,
    /// Sorted by `(producer_id, producer_epoch)`.
    pub producer_summary: Vec<ProducerSummaryEntry>,
    pub chunk_size: u32,
    pub chunk_count: u64,
    pub chunk_tree_scheme: String,
    /// Lowercase hex chunk-tree root over the encoded record frames.
    pub chunk_tree_root: String,
    pub index_stride: u32,
    pub sealing_node_id: Uuid,
    pub sealing_fencing_epoch: u64,
    pub evidence: SegmentEvidence,
    pub commit_statement: Option<CommitStatementV1>,
}

/// Runtime-only key used to authenticate v2 commit statements.
///
/// The key is deliberately opaque and has no serialization implementation: it
/// may be loaded from an environment variable, but it can never accidentally
/// become part of a config dump or manifest.
#[derive(Clone)]
pub struct SegmentCommitKey([u8; 32]);

impl std::fmt::Debug for SegmentCommitKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SegmentCommitKey([REDACTED])")
    }
}

impl SegmentCommitKey {
    /// Decode the required 32-byte key from its 64-character hex form.
    pub fn from_hex(value: &str) -> VtopLogResult<Self> {
        decode_hex_32(value).map(Self).ok_or_else(|| {
            LogError::InvalidConfig(
                "segment commit key must be exactly 32 bytes (64 hex characters)".to_owned(),
            )
        })
    }

    fn authenticate(&self, bytes: &[u8]) -> blake3::Hash {
        blake3::keyed_hash(&self.0, bytes)
    }
}

/// Signed statement that a sealed segment's content is committed.
///
/// The MAC covers the domain separator plus the canonical statement bytes
/// with `mac` blanked, so every identity, boundary, and digest field is
/// authenticated, including the scheme itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitStatementV1 {
    pub statement_version: u32,
    pub scheme: String,
    pub key_id: String,
    pub segment_id: SegmentId,
    pub segment_generation: u64,
    pub topic: String,
    pub topic_epoch: u64,
    pub range_id: Uuid,
    pub range_generation: u64,
    pub base_offset: u64,
    pub committed_high_watermark: u64,
    pub content_bytes: u64,
    pub chunk_tree_root: String,
    pub manifest_core_digest: String,
    pub mac: String,
}

impl CommitStatementV1 {
    /// Canonical statement bytes with the embedded MAC blanked so the
    /// calculation is not circular.
    fn auth_bytes(&self) -> VtopLogResult<Vec<u8>> {
        let mut clone = self.clone();
        clone.mac = String::new();
        let mut message = COMMIT_STATEMENT_DOMAIN.to_vec();
        message.extend_from_slice(&serde_json::to_vec(&clone).map_err(|error| {
            LogError::InvalidDescriptor(format!("cannot encode commit statement: {error}"))
        })?);
        Ok(message)
    }

    fn compute_mac(&self, key: Option<&SegmentCommitKey>) -> VtopLogResult<blake3::Hash> {
        let message = self.auth_bytes()?;
        Ok(match key {
            Some(key) => key.authenticate(&message),
            None => blake3::hash(&message),
        })
    }

    /// Fill in `scheme` and `mac` for the supplied key configuration.
    pub fn authenticate(&mut self, key: Option<&SegmentCommitKey>) -> VtopLogResult<()> {
        self.scheme = match key {
            Some(_) => COMMIT_SCHEME_KEYED,
            None => COMMIT_SCHEME_UNKEYED,
        }
        .to_owned();
        self.mac = self.compute_mac(key)?.to_hex().to_string();
        Ok(())
    }

    /// Recompute the MAC and require both scheme and value to match.
    ///
    /// Supplying a key deliberately rejects unkeyed statements: operators must
    /// re-sign their backlog rather than silently grandfathering it forever.
    pub fn verify(&self, key: Option<&SegmentCommitKey>) -> VtopLogResult<()> {
        let expected_scheme = match key {
            Some(_) => COMMIT_SCHEME_KEYED,
            None => COMMIT_SCHEME_UNKEYED,
        };
        if self.scheme != expected_scheme {
            return Err(LogError::ManifestMismatch(format!(
                "commit statement scheme {:?} does not match the configured key (expected {expected_scheme:?})",
                self.scheme
            )));
        }
        let stored = blake3::Hash::from_hex(&self.mac).map_err(|_| {
            LogError::ManifestMismatch(
                "commit statement MAC is not 32 hex-encoded bytes".to_owned(),
            )
        })?;
        // blake3::Hash equality is constant-time, so a remote verification
        // path does not leak how many prefix bytes were correct.
        if self.compute_mac(key)? != stored {
            return Err(LogError::ManifestMismatch(
                "commit statement MAC verification failed".to_owned(),
            ));
        }
        Ok(())
    }
}

fn decode_hex_32(value: &str) -> Option<[u8; 32]> {
    let raw = value.as_bytes();
    if raw.len() != 64 {
        return None;
    }
    let mut bytes = [0_u8; 32];
    for (byte, pair) in bytes.iter_mut().zip(raw.chunks_exact(2)) {
        *byte = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Some(bytes)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key_hex() -> String {
        (0..32).map(|byte| format!("{byte:02x}")).collect()
    }

    fn golden_manifest() -> SegmentManifestV2 {
        SegmentManifestV2 {
            format: FORMAT_NAME.to_owned(),
            version: FORMAT_VERSION_V2,
            record_schema_version: RECORD_SCHEMA_VERSION_V2,
            descriptor: SegmentDescriptorV2 {
                segment_id: Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
                topic: "audit.v1".to_owned(),
                topic_epoch: 3,
                lineage: RangeLineage::root(
                    Uuid::parse_str("ffeeddcc-bbaa-9988-7766-554433221100").unwrap(),
                ),
                base_offset: 42,
                segment_generation: 7,
                creation_node_id: Uuid::parse_str("12345678-9abc-def0-1234-56789abcdef0").unwrap(),
                creation_fencing_epoch: 5,
            },
            record_count: 9,
            first_offset: Some(42),
            next_offset: 51,
            content_bytes: 4096,
            committed_high_watermark: 51,
            producer_summary: vec![ProducerSummaryEntry {
                producer_id: Uuid::parse_str("01020304-0506-0708-090a-0b0c0d0e0f10").unwrap(),
                producer_epoch: 2,
                first_sequence: 0,
                last_sequence: 8,
                record_count: 9,
            }],
            chunk_size: 65_536,
            chunk_count: 1,
            chunk_tree_scheme: CHUNK_TREE_SCHEME_V1.to_owned(),
            chunk_tree_root: "aa".repeat(32),
            index_stride: 2,
            sealing_node_id: Uuid::parse_str("12345678-9abc-def0-1234-56789abcdef0").unwrap(),
            sealing_fencing_epoch: 6,
            evidence: SegmentEvidence::default(),
            commit_statement: None,
        }
    }

    fn golden_statement() -> CommitStatementV1 {
        CommitStatementV1 {
            statement_version: 1,
            scheme: String::new(),
            key_id: "test-key".to_owned(),
            segment_id: Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
            segment_generation: 7,
            topic: "audit.v1".to_owned(),
            topic_epoch: 3,
            range_id: Uuid::parse_str("ffeeddcc-bbaa-9988-7766-554433221100").unwrap(),
            range_generation: 0,
            base_offset: 42,
            committed_high_watermark: 51,
            content_bytes: 4096,
            chunk_tree_root: "aa".repeat(32),
            manifest_core_digest: "bb".repeat(32),
            mac: String::new(),
        }
    }

    #[test]
    fn v2_manifest_canonical_json_matches_golden_vector_and_round_trips() {
        let manifest = golden_manifest();
        let json = serde_json::to_string(&manifest).unwrap();
        assert_eq!(
            json,
            concat!(
                "{\"format\":\"vtop-native-segment\",\"version\":2,\"record_schema_version\":2,",
                "\"descriptor\":{\"segment_id\":\"00112233-4455-6677-8899-aabbccddeeff\",",
                "\"topic\":\"audit.v1\",\"topic_epoch\":3,\"lineage\":{",
                "\"range_id\":\"ffeeddcc-bbaa-9988-7766-554433221100\",\"generation\":0,",
                "\"key_range\":{\"prefix\":0,\"prefix_bits\":0}},\"base_offset\":42,",
                "\"segment_generation\":7,",
                "\"creation_node_id\":\"12345678-9abc-def0-1234-56789abcdef0\",",
                "\"creation_fencing_epoch\":5},\"record_count\":9,\"first_offset\":42,",
                "\"next_offset\":51,\"content_bytes\":4096,\"committed_high_watermark\":51,",
                "\"producer_summary\":[{\"producer_id\":\"01020304-0506-0708-090a-0b0c0d0e0f10\",",
                "\"producer_epoch\":2,\"first_sequence\":0,\"last_sequence\":8,\"record_count\":9}],",
                "\"chunk_size\":65536,\"chunk_count\":1,\"chunk_tree_scheme\":\"vtop-b3tree-v1\",",
                "\"chunk_tree_root\":",
                "\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",",
                "\"index_stride\":2,\"sealing_node_id\":\"12345678-9abc-def0-1234-56789abcdef0\",",
                "\"sealing_fencing_epoch\":6,\"evidence\":{\"replicas\":[],\"tiers\":[]},",
                "\"commit_statement\":null}"
            )
        );
        let decoded: SegmentManifestV2 = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, manifest);
    }

    #[test]
    fn keyed_commit_statement_matches_golden_mac_and_canonical_json() {
        let key = SegmentCommitKey::from_hex(&test_key_hex()).unwrap();
        let mut statement = golden_statement();
        statement.authenticate(Some(&key)).unwrap();
        assert_eq!(statement.scheme, COMMIT_SCHEME_KEYED);
        assert_eq!(
            statement.mac,
            "13d7aa6608779d47dfd87bdc19b8a92e785f733c25b1b0acba82ac89d2a6521f"
        );
        assert_eq!(
            serde_json::to_string(&statement).unwrap(),
            concat!(
                "{\"statement_version\":1,\"scheme\":\"blake3-keyed\",\"key_id\":\"test-key\",",
                "\"segment_id\":\"00112233-4455-6677-8899-aabbccddeeff\",\"segment_generation\":7,",
                "\"topic\":\"audit.v1\",\"topic_epoch\":3,",
                "\"range_id\":\"ffeeddcc-bbaa-9988-7766-554433221100\",\"range_generation\":0,",
                "\"base_offset\":42,\"committed_high_watermark\":51,\"content_bytes\":4096,",
                "\"chunk_tree_root\":",
                "\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",",
                "\"manifest_core_digest\":",
                "\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",",
                "\"mac\":\"13d7aa6608779d47dfd87bdc19b8a92e785f733c25b1b0acba82ac89d2a6521f\"}"
            )
        );
        statement.verify(Some(&key)).unwrap();

        // Wrong key, tampered field, malformed MAC, and scheme confusion all
        // fail verification.
        let wrong = SegmentCommitKey::from_hex(&"11".repeat(32)).unwrap();
        assert!(matches!(
            statement.verify(Some(&wrong)),
            Err(LogError::ManifestMismatch(_))
        ));
        let mut tampered = statement.clone();
        tampered.content_bytes += 1;
        assert!(tampered.verify(Some(&key)).is_err());
        let mut renamed = statement.clone();
        renamed.key_id = "other-key".to_owned();
        assert!(renamed.verify(Some(&key)).is_err());
        let mut malformed = statement.clone();
        malformed.mac = "zz".repeat(32);
        assert!(malformed.verify(Some(&key)).is_err());
        assert!(matches!(
            statement.verify(None),
            Err(LogError::ManifestMismatch(reason)) if reason.contains("scheme")
        ));
    }

    #[test]
    fn unkeyed_commit_statement_matches_golden_digest_and_rejects_keys() {
        let mut statement = golden_statement();
        statement.authenticate(None).unwrap();
        assert_eq!(statement.scheme, COMMIT_SCHEME_UNKEYED);
        assert_eq!(
            statement.mac,
            "9384477c56ef6324c1667634661f26bb124ce85a5976897c7f81c83ea2b1682a"
        );
        statement.verify(None).unwrap();

        // A configured key deliberately rejects unkeyed statements.
        let key = SegmentCommitKey::from_hex(&test_key_hex()).unwrap();
        assert!(matches!(
            statement.verify(Some(&key)),
            Err(LogError::ManifestMismatch(reason)) if reason.contains("scheme")
        ));
        let mut tampered = statement;
        tampered.committed_high_watermark += 1;
        assert!(tampered.verify(None).is_err());
    }

    #[test]
    fn segment_commit_key_round_trips_hex_and_redacts_debug_output() {
        let key = SegmentCommitKey::from_hex(&test_key_hex()).unwrap();
        assert_eq!(format!("{key:?}"), "SegmentCommitKey([REDACTED])");

        // The same hex, in either case, must authenticate identically.
        let upper = SegmentCommitKey::from_hex(&test_key_hex().to_uppercase()).unwrap();
        let mut first = golden_statement();
        first.authenticate(Some(&key)).unwrap();
        let mut second = golden_statement();
        second.authenticate(Some(&upper)).unwrap();
        assert_eq!(first.mac, second.mac);
        first.verify(Some(&upper)).unwrap();

        assert!(matches!(
            SegmentCommitKey::from_hex("11"),
            Err(LogError::InvalidConfig(_))
        ));
        assert!(SegmentCommitKey::from_hex(&"zz".repeat(32)).is_err());
        assert!(SegmentCommitKey::from_hex(&"11".repeat(33)).is_err());
    }

    #[test]
    fn v2_config_validation_accepts_and_rejects_the_expected_table() {
        let valid = SegmentConfigV2::default();
        assert!(valid.validate().is_ok());

        let table: &[(&str, SegmentConfigV2, bool)] = &[
            ("default", valid, true),
            (
                "smallest chunk",
                SegmentConfigV2 {
                    chunk_size: MIN_CHUNK_SIZE_BYTES,
                    ..valid
                },
                true,
            ),
            (
                "largest chunk",
                SegmentConfigV2 {
                    chunk_size: MAX_CHUNK_SIZE_BYTES,
                    ..valid
                },
                true,
            ),
            (
                "zero chunk",
                SegmentConfigV2 {
                    chunk_size: 0,
                    ..valid
                },
                false,
            ),
            (
                "non power of two chunk",
                SegmentConfigV2 {
                    chunk_size: 96 * 1024,
                    ..valid
                },
                false,
            ),
            (
                "power of two below minimum",
                SegmentConfigV2 {
                    chunk_size: 32 * 1024,
                    ..valid
                },
                false,
            ),
            (
                "power of two above maximum",
                SegmentConfigV2 {
                    chunk_size: 32 * 1024 * 1024,
                    ..valid
                },
                false,
            ),
            (
                "zero record bytes",
                SegmentConfigV2 {
                    max_record_bytes: 0,
                    ..valid
                },
                false,
            ),
            (
                "record limit above cap",
                SegmentConfigV2 {
                    max_record_bytes: MAX_RECORD_BYTES + 1,
                    ..valid
                },
                false,
            ),
            (
                "group cannot fit one framed record",
                SegmentConfigV2 {
                    max_record_bytes: 1024,
                    max_group_bytes: 1024,
                    ..valid
                },
                false,
            ),
            (
                "group exactly fits one framed record",
                SegmentConfigV2 {
                    max_record_bytes: 1024,
                    max_group_bytes: 1024 + crate::codec_v2::RECORD_FRAME_OVERHEAD_BYTES_V2,
                    ..valid
                },
                true,
            ),
            (
                "segment smaller than group",
                SegmentConfigV2 {
                    max_segment_bytes: DEFAULT_MAX_GROUP_BYTES - 1,
                    ..valid
                },
                false,
            ),
            (
                "zero segment records",
                SegmentConfigV2 {
                    max_segment_records: 0,
                    ..valid
                },
                false,
            ),
            (
                "zero index stride",
                SegmentConfigV2 {
                    index_stride: 0,
                    ..valid
                },
                false,
            ),
        ];
        for (name, config, expected) in table {
            assert_eq!(config.validate().is_ok(), *expected, "case {name}");
        }
    }

    #[test]
    fn v2_descriptor_validation_mirrors_v1_topic_and_lineage_rules() {
        let descriptor = golden_manifest().descriptor;
        descriptor.validate().unwrap();

        let mut empty_topic = descriptor.clone();
        empty_topic.topic = String::new();
        assert!(empty_topic.validate().is_err());

        let mut bad_byte = descriptor.clone();
        bad_byte.topic = "audit/v1".to_owned();
        assert!(bad_byte.validate().is_err());

        let mut bad_lineage = descriptor;
        bad_lineage.lineage.generation = 1;
        assert!(bad_lineage.validate().is_err());
    }
}

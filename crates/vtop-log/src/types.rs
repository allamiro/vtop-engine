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
    pub sequence: u64,
    pub timestamp_millis: i64,
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

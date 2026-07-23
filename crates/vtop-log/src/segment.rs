use crate::codec::{
    encode_header, encode_record, read_frame, read_header, record_content_hash, AnyHeader,
    FrameRead, SegmentHeader, INDEX_MAGIC,
};
use crate::codec_v2::{encode_header_v2, encode_record_v2, read_frame_v2, SegmentHeaderV2};
use crate::env::{Env, OpenMode, Storage, StorageFile};
use crate::proof;
use crate::types::{
    AppendOutcome, CommitStatementV1, Durability, FetchBatch, FetchedRecord, LogError, LogRecord,
    ProducerSummaryEntry, RecoveryReport, SegmentCommitKey, SegmentConfig, SegmentConfigV2,
    SegmentDescriptor, SegmentDescriptorV2, SegmentEvidence, SegmentManifest, SegmentManifestV2,
    VtopLogResult, CHUNK_SIDECAR_MAGIC, CHUNK_TREE_SCHEME_V1, FORMAT_NAME, FORMAT_VERSION,
    FORMAT_VERSION_V2, PRODUCER_SEQUENCE_WINDOW, RECORD_SCHEMA_VERSION_V2,
};
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

const COMMIT_MAGIC: &[u8; 8] = b"VTOPCMT1";
const COMMIT_VERSION: u16 = 1;
const COMMIT_BOUNDARY_LEN: usize = 8 + 2 + 16 + 8 + 8 + 32;
const CHUNK_SIDECAR_VERSION: u16 = 1;
/// magic8 + version u16 + chunk_size u32 + chunk_count u64 + checksum32.
const CHUNK_SIDECAR_FIXED_LEN: usize = 8 + 2 + 4 + 8 + 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CommitBoundary {
    segment_id: Uuid,
    committed_offset: u64,
    content_bytes: u64,
}

/// Producer states are keyed by `(producer_id, producer_epoch)`. The v1
/// format only ever observes epoch zero, so shared keying cannot change any
/// v1 decision.
type ProducerKey = (Uuid, u64);

/// `seen` is bounded: it holds only the most recent
/// `PRODUCER_SEQUENCE_WINDOW` accepted sequences, keyed sequences are dense
/// (in-order accepts, no gaps), and a `BTreeMap` lets eviction split off
/// everything below the window floor in one operation.
#[derive(Clone)]
struct ProducerState {
    latest_sequence: u64,
    seen: BTreeMap<u64, SeenRecord>,
}

struct ProducerDelta {
    latest_sequence: u64,
    seen: BTreeMap<u64, SeenRecord>,
}

#[derive(Clone)]
struct SeenRecord {
    offset: u64,
    content_hash: blake3::Hash,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct IndexEntry {
    offset: u64,
    position: u64,
}

impl AnyHeader {
    pub(crate) fn format_version(&self) -> u16 {
        match self {
            Self::V1(_) => FORMAT_VERSION,
            Self::V2(_) => FORMAT_VERSION_V2,
        }
    }

    fn segment_id(&self) -> Uuid {
        match self {
            Self::V1(header) => header.descriptor.segment_id,
            Self::V2(header) => header.descriptor.segment_id,
        }
    }

    fn base_offset(&self) -> u64 {
        match self {
            Self::V1(header) => header.descriptor.base_offset,
            Self::V2(header) => header.descriptor.base_offset,
        }
    }

    fn topic(&self) -> &str {
        match self {
            Self::V1(header) => &header.descriptor.topic,
            Self::V2(header) => &header.descriptor.topic,
        }
    }

    fn topic_epoch(&self) -> u64 {
        match self {
            Self::V1(header) => header.descriptor.topic_epoch,
            Self::V2(header) => header.descriptor.topic_epoch,
        }
    }

    fn range_id(&self) -> Uuid {
        match self {
            Self::V1(header) => header.descriptor.lineage.range_id,
            Self::V2(header) => header.descriptor.lineage.range_id,
        }
    }

    fn range_generation(&self) -> u64 {
        match self {
            Self::V1(header) => header.descriptor.lineage.generation,
            Self::V2(header) => header.descriptor.lineage.generation,
        }
    }

    fn max_record_bytes(&self) -> u32 {
        match self {
            Self::V1(header) => header.config.max_record_bytes,
            Self::V2(header) => header.config.max_record_bytes,
        }
    }

    fn max_group_bytes(&self) -> u64 {
        match self {
            Self::V1(header) => header.config.max_group_bytes,
            Self::V2(header) => header.config.max_group_bytes,
        }
    }

    fn max_segment_bytes(&self) -> u64 {
        match self {
            Self::V1(header) => header.config.max_segment_bytes,
            Self::V2(header) => header.config.max_segment_bytes,
        }
    }

    fn max_segment_records(&self) -> u64 {
        match self {
            Self::V1(header) => header.config.max_segment_records,
            Self::V2(header) => header.config.max_segment_records,
        }
    }

    fn index_stride(&self) -> u32 {
        match self {
            Self::V1(header) => header.config.index_stride,
            Self::V2(header) => header.config.index_stride,
        }
    }

    /// The v1-shaped identity of the segment; a v2 descriptor projects onto
    /// its common prefix. The extra v2 fields never affect slot identity.
    fn v1_descriptor_view(&self) -> SegmentDescriptor {
        match self {
            Self::V1(header) => header.descriptor.clone(),
            Self::V2(header) => SegmentDescriptor {
                segment_id: header.descriptor.segment_id,
                topic: header.descriptor.topic.clone(),
                topic_epoch: header.descriptor.topic_epoch,
                lineage: header.descriptor.lineage.clone(),
                base_offset: header.descriptor.base_offset,
            },
        }
    }
}

/// Version-selected commitment over the encoded record frames: the v1 linear
/// digest or the v2 chunk-tree builder. The hasher is boxed because its
/// in-place state dwarfs the builder's.
enum ContentAccumulator {
    V1(Box<blake3::Hasher>),
    V2(proof::ChunkTreeBuilder),
}

impl ContentAccumulator {
    fn for_header(header: &AnyHeader) -> Self {
        match header {
            AnyHeader::V1(_) => Self::V1(Box::new(blake3::Hasher::new())),
            AnyHeader::V2(header) => {
                Self::V2(proof::ChunkTreeBuilder::new(header.config.chunk_size))
            }
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::V1(hasher) => {
                hasher.update(bytes);
            }
            Self::V2(builder) => builder.update(bytes),
        }
    }

    /// Leaves plus root; the v1 linear digest has no leaves.
    fn finalize(self) -> (Vec<blake3::Hash>, blake3::Hash) {
        match self {
            Self::V1(hasher) => (Vec::new(), hasher.finalize()),
            Self::V2(builder) => builder.finalize(),
        }
    }
}

fn encode_record_any(
    header: &AnyHeader,
    record: &LogRecord,
    relative_offset: u64,
) -> VtopLogResult<Vec<u8>> {
    match header {
        AnyHeader::V1(v1) => encode_record(record, relative_offset, v1.config.max_record_bytes),
        AnyHeader::V2(v2) => encode_record_v2(record, relative_offset, v2.config.max_record_bytes),
    }
}

fn read_frame_any<R: Read>(
    format_version: u16,
    reader: &mut R,
    position: u64,
    maximum: u32,
) -> VtopLogResult<FrameRead> {
    if format_version == FORMAT_VERSION_V2 {
        read_frame_v2(reader, position, maximum)
    } else {
        read_frame(reader, position, maximum)
    }
}

struct ScanResult {
    records: u64,
    valid_end: u64,
    truncated_bytes: u64,
    next_offset: u64,
    producer_states: HashMap<ProducerKey, ProducerState>,
    producer_epochs: HashMap<Uuid, u64>,
    index: Vec<IndexEntry>,
    accumulator: ContentAccumulator,
}

/// A sealed manifest of any supported on-disk format version, boxed so the
/// enum stays small wherever it is embedded.
enum AnyManifest {
    V1(Box<SegmentManifest>),
    V2(Box<SegmentManifestV2>),
}

pub(crate) struct SegmentInspection {
    pub descriptor: SegmentDescriptor,
    pub format_version: u16,
    pub record_count: u64,
    pub next_offset: u64,
    pub content_bytes: u64,
    pub sealed_content_root: Option<String>,
}

struct ActiveFileInspection {
    header: AnyHeader,
    header_len: u64,
    actual_len: u64,
    scan: ScanResult,
}

struct SealedFileInspection {
    header: AnyHeader,
    header_len: u64,
    scan: ScanResult,
    manifest: AnyManifest,
    /// Finalized chunk-tree leaves from the scan; present only for v2.
    chunk_leaves: Option<Vec<blake3::Hash>>,
}

pub struct ActiveSegment {
    env: Env,
    path: PathBuf,
    file: Box<dyn StorageFile>,
    header: AnyHeader,
    header_len: u64,
    next_offset: u64,
    committed_offset: u64,
    record_count: u64,
    content_bytes: u64,
    producer_states: HashMap<ProducerKey, ProducerState>,
    producer_epochs: HashMap<Uuid, u64>,
    index: Vec<IndexEntry>,
    accumulator: ContentAccumulator,
    poisoned: bool,
    sealed: bool,
    recovery: RecoveryReport,
}

impl ActiveSegment {
    pub fn create(
        path: impl AsRef<Path>,
        descriptor: SegmentDescriptor,
        config: SegmentConfig,
    ) -> VtopLogResult<Self> {
        Self::create_in(&Env::real(), path, descriptor, config)
    }

    pub fn create_in(
        env: &Env,
        path: impl AsRef<Path>,
        descriptor: SegmentDescriptor,
        config: SegmentConfig,
    ) -> VtopLogResult<Self> {
        descriptor.validate()?;
        let config = config.validate()?;
        let header = SegmentHeader::new(descriptor, config);
        let encoded_header = encode_header(&header)?;
        Self::create_with_header(env, path, AnyHeader::V1(header), encoded_header)
    }

    /// Create a proof-carrying v2 segment.
    pub fn create_v2(
        path: impl AsRef<Path>,
        descriptor: SegmentDescriptorV2,
        config: SegmentConfigV2,
    ) -> VtopLogResult<Self> {
        Self::create_v2_in(&Env::real(), path, descriptor, config)
    }

    pub fn create_v2_in(
        env: &Env,
        path: impl AsRef<Path>,
        descriptor: SegmentDescriptorV2,
        config: SegmentConfigV2,
    ) -> VtopLogResult<Self> {
        descriptor.validate()?;
        let config = config.validate()?;
        let header = SegmentHeaderV2::new(descriptor, config);
        let encoded_header = encode_header_v2(&header)?;
        Self::create_with_header(env, path, AnyHeader::V2(header), encoded_header)
    }

    fn create_with_header(
        env: &Env,
        path: impl AsRef<Path>,
        header: AnyHeader,
        encoded_header: Vec<u8>,
    ) -> VtopLogResult<Self> {
        let path = path.as_ref().to_path_buf();
        let paths = SegmentPaths::from_active(&path)?;
        let mut file = env
            .storage
            .open(&path, OpenMode::CreateNew)
            .map_err(|source| io_error(&path, source))?;
        if let Err(source) = file
            .write_all(&encoded_header)
            .and_then(|()| file.sync_data())
        {
            return Err(io_error(&path, source));
        }
        sync_parent(env.storage.as_ref(), &path)?;
        let header_len = encoded_header.len() as u64;
        let base_offset = header.base_offset();
        write_commit_boundary_atomic(
            env,
            &paths.commit,
            CommitBoundary {
                segment_id: header.segment_id(),
                committed_offset: base_offset,
                content_bytes: 0,
            },
        )?;
        let accumulator = ContentAccumulator::for_header(&header);
        Ok(Self {
            env: env.clone(),
            path,
            file,
            header,
            header_len,
            next_offset: base_offset,
            committed_offset: base_offset,
            record_count: 0,
            content_bytes: 0,
            producer_states: HashMap::new(),
            producer_epochs: HashMap::new(),
            index: Vec::new(),
            accumulator,
            poisoned: false,
            sealed: false,
            recovery: RecoveryReport {
                records: 0,
                recovered_bytes: 0,
                truncated_bytes: 0,
                next_offset: base_offset,
            },
        })
    }

    /// Recover an active segment in place.
    ///
    /// An incomplete final frame is treated as a torn append and truncated.
    /// Invalid lengths, magic, or checksums are corruption and are never
    /// silently discarded.
    pub fn recover(path: impl AsRef<Path>) -> VtopLogResult<Self> {
        Self::recover_in(&Env::real(), path)
    }

    pub fn recover_in(env: &Env, path: impl AsRef<Path>) -> VtopLogResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = env
            .storage
            .open(&path, OpenMode::ReadWrite)
            .map_err(|source| io_error(&path, source))?;
        let inspection = inspect_active_file(env.storage.as_ref(), file.as_mut(), &path)?;
        let header = inspection.header;
        let header_len = inspection.header_len;
        let mut scan = inspection.scan;
        scan.truncated_bytes = inspection.actual_len.saturating_sub(scan.valid_end);
        if scan.truncated_bytes > 0 {
            file.set_len(scan.valid_end)
                .map_err(|source| io_error(&path, source))?;
        }
        file.sync_data().map_err(|source| io_error(&path, source))?;
        file.seek(SeekFrom::Start(scan.valid_end))
            .map_err(|source| io_error(&path, source))?;
        let report = RecoveryReport {
            records: scan.records,
            recovered_bytes: scan.valid_end - header_len,
            truncated_bytes: scan.truncated_bytes,
            next_offset: scan.next_offset,
        };
        Ok(Self {
            env: env.clone(),
            path,
            file,
            header,
            header_len,
            next_offset: scan.next_offset,
            committed_offset: scan.next_offset,
            record_count: scan.records,
            content_bytes: scan.valid_end - header_len,
            producer_states: scan.producer_states,
            producer_epochs: scan.producer_epochs,
            index: scan.index,
            accumulator: scan.accumulator,
            poisoned: false,
            sealed: false,
            recovery: report,
        })
    }

    /// The v1 descriptor.
    ///
    /// # Panics
    ///
    /// Panics on a v2 segment; use [`Self::descriptor_v2`] there.
    pub fn descriptor(&self) -> &SegmentDescriptor {
        match &self.header {
            AnyHeader::V1(header) => &header.descriptor,
            AnyHeader::V2(_) => panic!("descriptor() is a v1 accessor; use descriptor_v2()"),
        }
    }

    /// The v1 limits.
    ///
    /// # Panics
    ///
    /// Panics on a v2 segment; use [`Self::config_v2`] there.
    pub fn config(&self) -> SegmentConfig {
        match &self.header {
            AnyHeader::V1(header) => header.config,
            AnyHeader::V2(_) => panic!("config() is a v1 accessor; use config_v2()"),
        }
    }

    /// The v2 descriptor, if this segment uses the v2 format.
    pub fn descriptor_v2(&self) -> Option<&SegmentDescriptorV2> {
        match &self.header {
            AnyHeader::V1(_) => None,
            AnyHeader::V2(header) => Some(&header.descriptor),
        }
    }

    /// The v2 limits, if this segment uses the v2 format.
    pub fn config_v2(&self) -> Option<SegmentConfigV2> {
        match &self.header {
            AnyHeader::V1(_) => None,
            AnyHeader::V2(header) => Some(header.config),
        }
    }

    pub fn format_version(&self) -> u16 {
        self.header.format_version()
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// The first offset not yet durably committed on this node.
    pub fn committed_offset(&self) -> u64 {
        self.committed_offset
    }

    /// Durably commit every accepted append with one storage barrier.
    pub fn commit(&mut self) -> VtopLogResult<u64> {
        self.ensure_writable()?;
        self.persist_commit_boundary(self.next_offset, self.content_bytes)?;
        self.committed_offset = self.next_offset;
        Ok(self.committed_offset)
    }

    pub fn recovery_report(&self) -> &RecoveryReport {
        &self.recovery
    }

    pub fn append(
        &mut self,
        record: LogRecord,
        durability: Durability,
    ) -> VtopLogResult<AppendOutcome> {
        let mut outcomes = self.append_group(std::slice::from_ref(&record), durability)?;
        Ok(outcomes.remove(0))
    }

    /// Validate an entire producer group before writing any bytes.
    ///
    /// `Fsync` commits new and previously buffered bytes with one data barrier
    /// plus an atomic commit-boundary update. `Buffered` returns after writing
    /// to the operating system and leaves the group below the visible commit
    /// point until `commit` or a later `Fsync` append. Duplicate retries return
    /// their original offsets and are not written a second time.
    pub fn append_group(
        &mut self,
        records: &[LogRecord],
        durability: Durability,
    ) -> VtopLogResult<Vec<AppendOutcome>> {
        self.ensure_writable()?;
        for record in records {
            // The v1 frame cannot represent the schema-v2 record fields;
            // refusing nonzero values keeps the v1 on-disk contract
            // byte-for-byte frozen. Schema v2 persists the producer epoch but
            // still reserves every attribute bit.
            if matches!(self.header, AnyHeader::V1(_)) && record.producer_epoch != 0 {
                return Err(LogError::UnsupportedRecordField("producer_epoch"));
            }
            if record.attributes != 0 {
                return Err(LogError::UnsupportedRecordField("attributes"));
            }
        }
        let mut producer_deltas = HashMap::new();
        let mut epoch_deltas = HashMap::new();
        let mut prospective_next = self.next_offset;
        let mut outcomes = Vec::with_capacity(records.len());
        let mut pending = Vec::new();
        let mut group_bytes = 0_u64;

        for record in records {
            let hash = record_content_hash(record);
            match validate_sequence_with_delta(
                &self.producer_states,
                &self.producer_epochs,
                &producer_deltas,
                &epoch_deltas,
                record,
                hash,
            )? {
                SequenceDecision::Duplicate(offset) => {
                    outcomes.push(AppendOutcome::Duplicate { offset });
                }
                SequenceDecision::Append => {
                    let offset = prospective_next;
                    prospective_next = prospective_next.checked_add(1).ok_or_else(|| {
                        LogError::InvalidDescriptor("segment offset space exhausted".to_owned())
                    })?;
                    let relative_offset = offset - self.header.base_offset();
                    let encoded = encode_record_any(&self.header, record, relative_offset)?;
                    group_bytes = group_bytes.checked_add(encoded.len() as u64).ok_or(
                        LogError::GroupTooLarge {
                            actual: u64::MAX,
                            maximum: self.header.max_group_bytes(),
                        },
                    )?;
                    if group_bytes > self.header.max_group_bytes() {
                        return Err(LogError::GroupTooLarge {
                            actual: group_bytes,
                            maximum: self.header.max_group_bytes(),
                        });
                    }
                    remember_pending_sequence(
                        &mut producer_deltas,
                        &mut epoch_deltas,
                        record,
                        offset,
                        hash,
                    );
                    outcomes.push(AppendOutcome::Appended { offset });
                    pending.push((offset, encoded));
                }
            }
        }

        if pending.is_empty() {
            if matches!(durability, Durability::Fsync) {
                self.commit()?;
            }
            return Ok(outcomes);
        }
        let attempted_bytes =
            self.content_bytes
                .checked_add(group_bytes)
                .ok_or(LogError::SegmentByteLimit {
                    current: self.content_bytes,
                    attempted: u64::MAX,
                    maximum: self.header.max_segment_bytes(),
                })?;
        if attempted_bytes > self.header.max_segment_bytes() {
            return Err(LogError::SegmentByteLimit {
                current: self.content_bytes,
                attempted: attempted_bytes,
                maximum: self.header.max_segment_bytes(),
            });
        }
        let attempted_records = self.record_count.checked_add(pending.len() as u64).ok_or(
            LogError::SegmentRecordLimit {
                attempted: u64::MAX,
                maximum: self.header.max_segment_records(),
            },
        )?;
        if attempted_records > self.header.max_segment_records() {
            return Err(LogError::SegmentRecordLimit {
                attempted: attempted_records,
                maximum: self.header.max_segment_records(),
            });
        }
        let write_start = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(|source| io_error(&self.path, source))?;
        let mut position = write_start;
        for (offset, encoded) in &pending {
            if (*offset - self.header.base_offset())
                .is_multiple_of(u64::from(self.header.index_stride()))
            {
                self.index.push(IndexEntry {
                    offset: *offset,
                    position,
                });
            }
            if let Err(source) = self.file.write_all(encoded) {
                self.poisoned = true;
                return Err(io_error(&self.path, source));
            }
            position += encoded.len() as u64;
        }
        if matches!(durability, Durability::Fsync) {
            self.persist_commit_boundary(prospective_next, attempted_bytes)?;
        }
        for (_, encoded) in &pending {
            self.accumulator.update(encoded);
        }
        merge_producer_deltas(
            &mut self.producer_states,
            &mut self.producer_epochs,
            producer_deltas,
            epoch_deltas,
        );
        self.next_offset = prospective_next;
        self.record_count += pending.len() as u64;
        self.content_bytes = attempted_bytes;
        if matches!(durability, Durability::Fsync) {
            self.committed_offset = self.next_offset;
        }
        Ok(outcomes)
    }

    pub fn fetch(
        &mut self,
        start_offset: u64,
        max_bytes: usize,
        max_records: usize,
    ) -> VtopLogResult<FetchBatch> {
        self.fetch_through(start_offset, max_bytes, max_records, self.committed_offset)
    }

    /// Fetch records visible at or below `high_watermark`.
    ///
    /// The effective watermark is clamped to the local durable
    /// [`Self::committed_offset`] so callers cannot expose buffered tails or
    /// unrecovered bytes. Clustered brokers pass the quorum-committed point
    /// here so followers never serve above the cluster high-water mark.
    pub fn fetch_through(
        &mut self,
        start_offset: u64,
        max_bytes: usize,
        max_records: usize,
        high_watermark: u64,
    ) -> VtopLogResult<FetchBatch> {
        let high_watermark = high_watermark.min(self.committed_offset);
        let result = fetch_from_file(
            self.file.as_mut(),
            &self.path,
            &self.header,
            self.header_len,
            &self.index,
            high_watermark,
            start_offset,
            max_bytes,
            max_records,
        );
        if result.is_err() {
            self.poisoned = true;
            let _ = self.file.seek(SeekFrom::End(0));
        }
        result
    }

    /// Seal the segment and atomically publish its manifest and sparse index.
    ///
    /// The record bodies stay in one file; no WAL or second data copy is made.
    /// A v2 segment seals without a commit statement; use [`Self::seal_v2`]
    /// to attach one.
    pub fn seal(self) -> VtopLogResult<SegmentReader> {
        match self.header {
            AnyHeader::V1(_) => self.seal_v1(),
            AnyHeader::V2(_) => self.seal_v2(None),
        }
    }

    fn seal_v1(mut self) -> VtopLogResult<SegmentReader> {
        self.check_sealable()?;
        let AnyHeader::V1(header) = &self.header else {
            unreachable!("seal_v1 is only dispatched for v1 segments");
        };
        let (_, root) = std::mem::replace(
            &mut self.accumulator,
            ContentAccumulator::for_header(&self.header),
        )
        .finalize();
        let manifest = SegmentManifest {
            format: FORMAT_NAME.to_owned(),
            version: FORMAT_VERSION,
            descriptor: header.descriptor.clone(),
            record_count: self.record_count,
            first_offset: (self.record_count > 0).then_some(header.descriptor.base_offset),
            next_offset: self.next_offset,
            content_bytes: self.content_bytes,
            blake3_root: root.to_hex().to_string(),
            index_stride: header.config.index_stride,
        };
        let paths = self.check_unpublished()?;
        write_index_atomic(&self.env, &paths.index, &self.index)?;
        write_manifest_atomic(&self.env, &paths.manifest, &manifest)?;
        self.publish_sealed(paths)
    }

    /// Seal a v2 segment, optionally signing a [`CommitStatementV1`] with the
    /// supplied runtime key. Without a key the manifest carries no statement.
    pub fn seal_v2(mut self, key: Option<&SegmentCommitKey>) -> VtopLogResult<SegmentReader> {
        if matches!(self.header, AnyHeader::V1(_)) {
            return Err(LogError::InvalidDescriptor(
                "a v1 segment cannot carry a v2 commit statement; use seal()".to_owned(),
            ));
        }
        self.check_sealable()?;
        let AnyHeader::V2(header) = &self.header else {
            unreachable!("v1 segments were rejected above");
        };
        let (leaves, root) = std::mem::replace(
            &mut self.accumulator,
            ContentAccumulator::for_header(&self.header),
        )
        .finalize();
        let mut manifest = SegmentManifestV2 {
            format: FORMAT_NAME.to_owned(),
            version: FORMAT_VERSION_V2,
            record_schema_version: RECORD_SCHEMA_VERSION_V2,
            descriptor: header.descriptor.clone(),
            record_count: self.record_count,
            first_offset: (self.record_count > 0).then_some(header.descriptor.base_offset),
            next_offset: self.next_offset,
            content_bytes: self.content_bytes,
            // Sealing publishes only durably committed bytes, so the sealed
            // high watermark is exactly the next offset.
            committed_high_watermark: self.next_offset,
            producer_summary: producer_summary_from_states(&self.producer_states),
            chunk_size: header.config.chunk_size,
            chunk_count: leaves.len() as u64,
            chunk_tree_scheme: CHUNK_TREE_SCHEME_V1.to_owned(),
            chunk_tree_root: root.to_hex().to_string(),
            index_stride: header.config.index_stride,
            sealing_node_id: header.descriptor.creation_node_id,
            sealing_fencing_epoch: header.descriptor.creation_fencing_epoch,
            evidence: SegmentEvidence::default(),
            commit_statement: None,
        };
        if let Some(key) = key {
            let mut statement = commit_statement_core(&manifest)?;
            statement.authenticate(Some(key))?;
            manifest.commit_statement = Some(statement);
        }
        let paths = self.check_unpublished()?;
        write_chunk_sidecar_atomic(&self.env, &paths.chunks, header.config.chunk_size, &leaves)?;
        write_index_atomic(&self.env, &paths.index, &self.index)?;
        write_manifest_v2_atomic(&self.env, &paths.manifest, &manifest)?;
        self.publish_sealed(paths)
    }

    /// Commit outstanding appends and require the file to match the accepted
    /// append state before any manifest bytes are derived from it.
    fn check_sealable(&mut self) -> VtopLogResult<()> {
        self.ensure_writable()?;
        // A sealed reader exposes its complete contents. Advance a durable
        // commit boundary first so sealing can never publish buffered records
        // that were not committed on this node.
        self.commit()?;
        let actual_file_bytes = self
            .file
            .len()
            .map_err(|source| io_error(&self.path, source))?;
        let actual_content_bytes =
            actual_file_bytes
                .checked_sub(self.header_len)
                .ok_or_else(|| LogError::Corrupt {
                    position: actual_file_bytes,
                    reason: "active segment is shorter than its validated header".to_owned(),
                })?;
        if actual_content_bytes != self.content_bytes {
            return Err(LogError::Corrupt {
                position: self.header_len + self.content_bytes,
                reason: "active segment length differs from accepted append state".to_owned(),
            });
        }
        Ok(())
    }

    fn check_unpublished(&self) -> VtopLogResult<SegmentPaths> {
        let paths = SegmentPaths::from_active(&self.path)?;
        if self
            .env
            .storage
            .exists(&paths.segment)
            .map_err(|source| io_error(&paths.segment, source))?
        {
            return Err(LogError::InvalidDescriptor(format!(
                "refusing to replace existing sealed segment {}",
                paths.segment.display()
            )));
        }
        Ok(paths)
    }

    fn publish_sealed(mut self, paths: SegmentPaths) -> VtopLogResult<SegmentReader> {
        self.env
            .storage
            .rename(&self.path, &paths.segment)
            .map_err(|source| io_error(&self.path, source))?;
        sync_parent(self.env.storage.as_ref(), &paths.segment)?;
        self.sealed = true;
        SegmentReader::open_in(&self.env, paths.segment)
    }

    fn persist_commit_boundary(
        &mut self,
        committed_offset: u64,
        content_bytes: u64,
    ) -> VtopLogResult<()> {
        if let Err(source) = self.file.sync_data() {
            self.poisoned = true;
            return Err(io_error(&self.path, source));
        }
        let paths = SegmentPaths::from_active(&self.path)?;
        if let Err(error) = write_commit_boundary_atomic(
            &self.env,
            &paths.commit,
            CommitBoundary {
                segment_id: self.header.segment_id(),
                committed_offset,
                content_bytes,
            },
        ) {
            self.poisoned = true;
            return Err(error);
        }
        Ok(())
    }

    fn ensure_writable(&self) -> VtopLogResult<()> {
        if self.sealed {
            return Err(LogError::AlreadySealed);
        }
        if self.poisoned {
            return Err(LogError::WriterPoisoned);
        }
        Ok(())
    }
}

pub struct SegmentReader {
    path: PathBuf,
    file: Box<dyn StorageFile>,
    header: AnyHeader,
    header_len: u64,
    manifest: AnyManifest,
    index: Vec<IndexEntry>,
}

impl SegmentReader {
    pub fn open(path: impl AsRef<Path>) -> VtopLogResult<Self> {
        Self::open_in(&Env::real(), path)
    }

    pub fn open_in(env: &Env, path: impl AsRef<Path>) -> VtopLogResult<Self> {
        let path = path.as_ref().to_path_buf();
        let paths = SegmentPaths::from_segment(&path)?;
        let mut file = env
            .storage
            .open(&path, OpenMode::Read)
            .map_err(|source| io_error(&path, source))?;
        let inspection = inspect_sealed_file(env.storage.as_ref(), file.as_mut(), &path)?;
        let header = inspection.header;
        let header_len = inspection.header_len;
        let scan = inspection.scan;
        let manifest = inspection.manifest;

        // The `.chunks` sidecar is a rebuildable cache, exactly like the
        // sparse index: the manifest root is the authority and the scan above
        // already proved the segment folds to it.
        if let (AnyManifest::V2(manifest), Some(leaves)) = (&manifest, &inspection.chunk_leaves) {
            match read_chunk_sidecar(env.storage.as_ref(), &paths.chunks) {
                Ok((chunk_size, stored))
                    if chunk_size == manifest.chunk_size && &stored == leaves => {}
                Ok(_) | Err(_) => {
                    write_chunk_sidecar_atomic(env, &paths.chunks, manifest.chunk_size, leaves)?;
                }
            }
        }
        let index = match read_index(env.storage.as_ref(), &paths.index) {
            Ok(index) if index == scan.index => index,
            Ok(_) | Err(_) => {
                write_index_atomic(env, &paths.index, &scan.index)?;
                scan.index
            }
        };
        Ok(Self {
            path,
            file,
            header,
            header_len,
            manifest,
            index,
        })
    }

    /// The v1 manifest.
    ///
    /// # Panics
    ///
    /// Panics on a v2 segment; use [`Self::manifest_v2`] there.
    pub fn manifest(&self) -> &SegmentManifest {
        match &self.manifest {
            AnyManifest::V1(manifest) => manifest,
            AnyManifest::V2(_) => panic!("manifest() is a v1 accessor; use manifest_v2()"),
        }
    }

    /// The v2 manifest, if this segment uses the v2 format.
    pub fn manifest_v2(&self) -> Option<&SegmentManifestV2> {
        match &self.manifest {
            AnyManifest::V1(_) => None,
            AnyManifest::V2(manifest) => Some(manifest),
        }
    }

    /// The v2 descriptor, if this segment uses the v2 format.
    pub fn descriptor_v2(&self) -> Option<&SegmentDescriptorV2> {
        match &self.header {
            AnyHeader::V1(_) => None,
            AnyHeader::V2(header) => Some(&header.descriptor),
        }
    }

    pub fn format_version(&self) -> u16 {
        self.header.format_version()
    }

    fn manifest_next_offset(&self) -> u64 {
        match &self.manifest {
            AnyManifest::V1(manifest) => manifest.next_offset,
            AnyManifest::V2(manifest) => manifest.next_offset,
        }
    }

    /// A cursor binds progress to the sealed content root: the linear v1
    /// digest or the v2 chunk-tree root.
    pub fn cursor(&self, offset: u64) -> VtopLogResult<crate::SegmentCursor> {
        let next_offset = self.manifest_next_offset();
        if !(self.header.base_offset()..=next_offset).contains(&offset) {
            return Err(LogError::InvalidCursor(format!(
                "offset {offset} is outside segment interval {}..={}",
                self.header.base_offset(),
                next_offset
            )));
        }
        Ok(crate::SegmentCursor {
            topic: self.header.topic().to_owned(),
            topic_epoch: self.header.topic_epoch(),
            range_id: self.header.range_id(),
            range_generation: self.header.range_generation(),
            segment_id: self.header.segment_id(),
            segment_root: match &self.manifest {
                AnyManifest::V1(manifest) => manifest.blake3_root.clone(),
                AnyManifest::V2(manifest) => manifest.chunk_tree_root.clone(),
            },
            offset,
        })
    }

    pub fn fetch(
        &mut self,
        start_offset: u64,
        max_bytes: usize,
        max_records: usize,
    ) -> VtopLogResult<FetchBatch> {
        let high_watermark = self.manifest_next_offset();
        fetch_from_file(
            self.file.as_mut(),
            &self.path,
            &self.header,
            self.header_len,
            &self.index,
            high_watermark,
            start_offset,
            max_bytes,
            max_records,
        )
    }
}

pub fn rebuild_index(path: impl AsRef<Path>) -> VtopLogResult<()> {
    rebuild_index_in(&Env::real(), path)
}

pub fn rebuild_index_in(env: &Env, path: impl AsRef<Path>) -> VtopLogResult<()> {
    let path = path.as_ref();
    let paths = SegmentPaths::from_segment(path)?;
    let mut file = env
        .storage
        .open(path, OpenMode::Read)
        .map_err(|source| io_error(path, source))?;
    let (header, header_len) = read_header_with_path(file.as_mut(), path)?;
    let scan = scan_records(file.as_mut(), path, &header, header_len, None, false)?;
    write_index_atomic(env, &paths.index, &scan.index)
}

/// Rebuild the `.chunks` sidecar of a sealed v2 segment from its record
/// frames.
pub fn rebuild_chunk_index(path: impl AsRef<Path>) -> VtopLogResult<()> {
    rebuild_chunk_index_in(&Env::real(), path)
}

pub fn rebuild_chunk_index_in(env: &Env, path: impl AsRef<Path>) -> VtopLogResult<()> {
    let path = path.as_ref();
    let paths = SegmentPaths::from_segment(path)?;
    let mut file = env
        .storage
        .open(path, OpenMode::Read)
        .map_err(|source| io_error(path, source))?;
    let (header, header_len) = read_header_with_path(file.as_mut(), path)?;
    let AnyHeader::V2(v2_header) = &header else {
        return Err(LogError::InvalidDescriptor(
            "only v2 segments carry a chunk sidecar".to_owned(),
        ));
    };
    let chunk_size = v2_header.config.chunk_size;
    let scan = scan_records(file.as_mut(), path, &header, header_len, None, false)?;
    let (leaves, _) = scan.accumulator.finalize();
    write_chunk_sidecar_atomic(env, &paths.chunks, chunk_size, &leaves)
}

pub(crate) fn inspect_active_segment(env: &Env, path: &Path) -> VtopLogResult<SegmentInspection> {
    SegmentPaths::from_active(path)?;
    let mut file = env
        .storage
        .open(path, OpenMode::Read)
        .map_err(|source| io_error(path, source))?;
    let inspection = inspect_active_file(env.storage.as_ref(), file.as_mut(), path)?;
    Ok(SegmentInspection {
        descriptor: inspection.header.v1_descriptor_view(),
        format_version: inspection.header.format_version(),
        record_count: inspection.scan.records,
        next_offset: inspection.scan.next_offset,
        content_bytes: inspection.scan.valid_end - inspection.header_len,
        sealed_content_root: None,
    })
}

pub(crate) fn inspect_sealed_segment(env: &Env, path: &Path) -> VtopLogResult<SegmentInspection> {
    SegmentPaths::from_segment(path)?;
    let mut file = env
        .storage
        .open(path, OpenMode::Read)
        .map_err(|source| io_error(path, source))?;
    let inspection = inspect_sealed_file(env.storage.as_ref(), file.as_mut(), path)?;
    Ok(SegmentInspection {
        descriptor: inspection.header.v1_descriptor_view(),
        format_version: inspection.header.format_version(),
        record_count: inspection.scan.records,
        next_offset: inspection.scan.next_offset,
        content_bytes: inspection.scan.valid_end - inspection.header_len,
        sealed_content_root: Some(match inspection.manifest {
            AnyManifest::V1(manifest) => manifest.blake3_root,
            AnyManifest::V2(manifest) => manifest.chunk_tree_root,
        }),
    })
}

fn inspect_active_file(
    storage: &dyn Storage,
    file: &mut dyn StorageFile,
    path: &Path,
) -> VtopLogResult<ActiveFileInspection> {
    let paths = SegmentPaths::from_active(path)?;
    let (header, header_len) = read_header_with_path(file, path)?;
    // A missing marker is ambiguous: the file may contain acknowledged Fsync
    // appends whose boundary sidecar was deleted. Inspection and recovery must
    // never manufacture a boundary or promote the surviving bytes.
    let boundary = read_commit_boundary(storage, &paths.commit)?;
    if boundary.segment_id != header.segment_id() {
        return Err(LogError::CommitBoundaryMismatch(
            "segment id differs from the active segment header".to_owned(),
        ));
    }
    let committed_end = header_len
        .checked_add(boundary.content_bytes)
        .ok_or_else(|| {
            LogError::CommitBoundaryMismatch("committed byte length overflows".to_owned())
        })?;
    let actual_len = file.len().map_err(|source| io_error(path, source))?;
    if committed_end > actual_len {
        return Err(LogError::CommitBoundaryMismatch(format!(
            "committed byte end {committed_end} exceeds file length {actual_len}"
        )));
    }
    let scan = scan_records(file, path, &header, header_len, Some(committed_end), false)?;
    if scan.next_offset != boundary.committed_offset
        || scan.valid_end != committed_end
        || scan.valid_end - header_len != boundary.content_bytes
    {
        return Err(LogError::CommitBoundaryMismatch(
            "offset or byte boundary does not end on the validated record frontier".to_owned(),
        ));
    }
    Ok(ActiveFileInspection {
        header,
        header_len,
        actual_len,
        scan,
    })
}

fn inspect_sealed_file(
    storage: &dyn Storage,
    file: &mut dyn StorageFile,
    path: &Path,
) -> VtopLogResult<SealedFileInspection> {
    let paths = SegmentPaths::from_segment(path)?;
    let (header, header_len) = read_header_with_path(file, path)?;
    let mut scan = scan_records(file, path, &header, header_len, None, false)?;
    let manifest_bytes = storage
        .read(&paths.manifest)
        .map_err(|source| io_error(&paths.manifest, source))?;
    match &header {
        AnyHeader::V1(v1_header) => {
            let manifest: SegmentManifest =
                serde_json::from_slice(&manifest_bytes).map_err(|error| {
                    LogError::ManifestMismatch(format!("cannot decode manifest: {error}"))
                })?;
            if manifest_bytes != canonical_manifest_bytes(&manifest)? {
                return Err(LogError::ManifestMismatch(
                    "manifest is not in canonical VTOP JSON encoding".to_owned(),
                ));
            }
            validate_manifest(&manifest, v1_header, &scan, header_len)?;
            Ok(SealedFileInspection {
                header,
                header_len,
                scan,
                manifest: AnyManifest::V1(Box::new(manifest)),
                chunk_leaves: None,
            })
        }
        AnyHeader::V2(v2_header) => {
            let manifest: SegmentManifestV2 =
                serde_json::from_slice(&manifest_bytes).map_err(|error| {
                    LogError::ManifestMismatch(format!("cannot decode manifest: {error}"))
                })?;
            if manifest_bytes != canonical_manifest_v2_bytes(&manifest)? {
                return Err(LogError::ManifestMismatch(
                    "manifest is not in canonical VTOP JSON encoding".to_owned(),
                ));
            }
            let accumulator = std::mem::replace(
                &mut scan.accumulator,
                ContentAccumulator::V1(Box::new(blake3::Hasher::new())),
            );
            let (leaves, root) = accumulator.finalize();
            validate_manifest_v2(&manifest, v2_header, &scan, header_len, &leaves, &root)?;
            Ok(SealedFileInspection {
                header,
                header_len,
                scan,
                manifest: AnyManifest::V2(Box::new(manifest)),
                chunk_leaves: Some(leaves),
            })
        }
    }
}

enum SequenceDecision {
    Append,
    Duplicate(u64),
}

/// Reject an epoch older than the newest one this segment has observed for
/// the producer. A duplicate retry is recognized before this check runs, so
/// already-persisted old-epoch records still return their original offsets.
fn validate_epoch_not_fenced(latest_epoch: Option<u64>, record: &LogRecord) -> VtopLogResult<()> {
    if let Some(latest_epoch) = latest_epoch {
        if record.producer_epoch < latest_epoch {
            return Err(LogError::ProducerFenced {
                producer_id: record.producer_id,
                latest_epoch,
                actual_epoch: record.producer_epoch,
            });
        }
    }
    Ok(())
}

/// Lowest sequence whose idempotency is still verifiable for a producer
/// state whose newest accepted sequence is `latest_sequence`.
fn sequence_window_floor(latest_sequence: u64) -> u64 {
    latest_sequence.saturating_sub(PRODUCER_SEQUENCE_WINDOW - 1)
}

/// Evict every remembered sequence below the retry window floor; O(evicted).
fn evict_below_window(seen: &mut BTreeMap<u64, SeenRecord>, latest_sequence: u64) {
    let floor = sequence_window_floor(latest_sequence);
    if floor > 0 {
        *seen = seen.split_off(&floor);
    }
}

fn sequence_below_window(record: &LogRecord, latest_sequence: u64) -> Option<LogError> {
    let window_floor = sequence_window_floor(latest_sequence);
    (record.sequence < window_floor).then_some(LogError::SequenceBelowWindow {
        producer_id: record.producer_id,
        producer_epoch: record.producer_epoch,
        sequence: record.sequence,
        window_floor,
    })
}

fn validate_sequence(
    states: &HashMap<ProducerKey, ProducerState>,
    epochs: &HashMap<Uuid, u64>,
    record: &LogRecord,
    hash: blake3::Hash,
) -> VtopLogResult<SequenceDecision> {
    let key = (record.producer_id, record.producer_epoch);
    if let Some(seen) = states
        .get(&key)
        .and_then(|state| state.seen.get(&record.sequence))
    {
        return if hash == seen.content_hash {
            Ok(SequenceDecision::Duplicate(seen.offset))
        } else {
            Err(LogError::SequenceConflict {
                producer_id: record.producer_id,
                sequence: record.sequence,
            })
        };
    }
    validate_epoch_not_fenced(epochs.get(&record.producer_id).copied(), record)?;
    let Some(state) = states.get(&key) else {
        return if record.sequence == 0 {
            Ok(SequenceDecision::Append)
        } else {
            Err(LogError::FirstSequence {
                producer_id: record.producer_id,
                actual: record.sequence,
            })
        };
    };
    // The `seen` lookup above missed, so a sequence this old fell out of the
    // retry window; reject fail-closed rather than guess its idempotency.
    if let Some(error) = sequence_below_window(record, state.latest_sequence) {
        return Err(error);
    }
    let expected = state
        .latest_sequence
        .checked_add(1)
        .ok_or(LogError::SequenceGap {
            producer_id: record.producer_id,
            expected: u64::MAX,
            actual: record.sequence,
        })?;
    if record.sequence != expected {
        return Err(LogError::SequenceGap {
            producer_id: record.producer_id,
            expected,
            actual: record.sequence,
        });
    }
    Ok(SequenceDecision::Append)
}

fn validate_sequence_with_delta(
    states: &HashMap<ProducerKey, ProducerState>,
    epochs: &HashMap<Uuid, u64>,
    deltas: &HashMap<ProducerKey, ProducerDelta>,
    epoch_deltas: &HashMap<Uuid, u64>,
    record: &LogRecord,
    hash: blake3::Hash,
) -> VtopLogResult<SequenceDecision> {
    let key = (record.producer_id, record.producer_epoch);
    if let Some(seen) = deltas
        .get(&key)
        .and_then(|delta| delta.seen.get(&record.sequence))
        .or_else(|| {
            states
                .get(&key)
                .and_then(|state| state.seen.get(&record.sequence))
        })
    {
        return if hash == seen.content_hash {
            Ok(SequenceDecision::Duplicate(seen.offset))
        } else {
            Err(LogError::SequenceConflict {
                producer_id: record.producer_id,
                sequence: record.sequence,
            })
        };
    }

    validate_epoch_not_fenced(
        epoch_deltas
            .get(&record.producer_id)
            .or_else(|| epochs.get(&record.producer_id))
            .copied(),
        record,
    )?;
    let latest = deltas
        .get(&key)
        .map(|delta| delta.latest_sequence)
        .or_else(|| states.get(&key).map(|state| state.latest_sequence));
    let Some(latest) = latest else {
        return if record.sequence == 0 {
            Ok(SequenceDecision::Append)
        } else {
            Err(LogError::FirstSequence {
                producer_id: record.producer_id,
                actual: record.sequence,
            })
        };
    };
    // Same fail-closed rule as `validate_sequence`, against the group's
    // prospective frontier so a large group cannot widen the window.
    if let Some(error) = sequence_below_window(record, latest) {
        return Err(error);
    }
    let expected = latest.checked_add(1).ok_or(LogError::SequenceGap {
        producer_id: record.producer_id,
        expected: u64::MAX,
        actual: record.sequence,
    })?;
    if record.sequence != expected {
        return Err(LogError::SequenceGap {
            producer_id: record.producer_id,
            expected,
            actual: record.sequence,
        });
    }
    Ok(SequenceDecision::Append)
}

fn remember_pending_sequence(
    deltas: &mut HashMap<ProducerKey, ProducerDelta>,
    epoch_deltas: &mut HashMap<Uuid, u64>,
    record: &LogRecord,
    offset: u64,
    content_hash: blake3::Hash,
) {
    let latest_epoch = epoch_deltas
        .entry(record.producer_id)
        .or_insert(record.producer_epoch);
    *latest_epoch = (*latest_epoch).max(record.producer_epoch);
    deltas
        .entry((record.producer_id, record.producer_epoch))
        .and_modify(|delta| {
            delta.latest_sequence = record.sequence;
            delta.seen.insert(
                record.sequence,
                SeenRecord {
                    offset,
                    content_hash,
                },
            );
            evict_below_window(&mut delta.seen, record.sequence);
        })
        .or_insert_with(|| ProducerDelta {
            latest_sequence: record.sequence,
            seen: BTreeMap::from([(
                record.sequence,
                SeenRecord {
                    offset,
                    content_hash,
                },
            )]),
        });
}

fn merge_producer_deltas(
    states: &mut HashMap<ProducerKey, ProducerState>,
    epochs: &mut HashMap<Uuid, u64>,
    deltas: HashMap<ProducerKey, ProducerDelta>,
    epoch_deltas: HashMap<Uuid, u64>,
) {
    for (producer_id, epoch) in epoch_deltas {
        let latest = epochs.entry(producer_id).or_insert(epoch);
        *latest = (*latest).max(epoch);
    }
    for (key, delta) in deltas {
        let ProducerDelta {
            latest_sequence,
            seen,
        } = delta;
        if let Some(state) = states.get_mut(&key) {
            state.latest_sequence = latest_sequence;
            state.seen.extend(seen);
            evict_below_window(&mut state.seen, latest_sequence);
        } else {
            states.insert(
                key,
                ProducerState {
                    latest_sequence,
                    seen,
                },
            );
        }
    }
}

fn scan_records(
    mut file: &mut dyn StorageFile,
    path: &Path,
    header: &AnyHeader,
    header_len: u64,
    logical_end: Option<u64>,
    permit_torn_tail: bool,
) -> VtopLogResult<ScanResult> {
    file.seek(SeekFrom::Start(header_len))
        .map_err(|source| io_error(path, source))?;
    let actual_file_len = file.len().map_err(|source| io_error(path, source))?;
    let file_len = logical_end.unwrap_or(actual_file_len);
    if file_len < header_len || file_len > actual_file_len {
        return Err(LogError::CommitBoundaryMismatch(format!(
            "logical file end {file_len} is outside {header_len}..={actual_file_len}"
        )));
    }
    let mut position = header_len;
    let mut next_offset = header.base_offset();
    let mut records = 0_u64;
    let mut producer_states = HashMap::new();
    let mut producer_epochs = HashMap::new();
    let mut index = Vec::new();
    let mut accumulator = ContentAccumulator::for_header(header);

    loop {
        if position == file_len {
            break;
        }
        if position > file_len {
            return Err(LogError::CommitBoundaryMismatch(
                "commit boundary splits a record frame".to_owned(),
            ));
        }
        match read_frame_any(
            header.format_version(),
            &mut file,
            position,
            header.max_record_bytes(),
        )
        .map_err(|error| with_path(error, path))?
        {
            FrameRead::End => break,
            FrameRead::Torn if permit_torn_tail => break,
            FrameRead::Torn => {
                return Err(LogError::Corrupt {
                    position,
                    reason: "sealed segment ends with an incomplete record".to_owned(),
                });
            }
            FrameRead::Complete(frame) => {
                if position.saturating_add(frame.encoded_len as u64) > file_len {
                    return Err(LogError::CommitBoundaryMismatch(
                        "commit boundary splits a record frame".to_owned(),
                    ));
                }
                let attempted_bytes = position
                    .saturating_sub(header_len)
                    .saturating_add(frame.encoded_len as u64);
                if attempted_bytes > header.max_segment_bytes() {
                    return Err(LogError::Corrupt {
                        position,
                        reason: format!(
                            "segment exceeds configured byte limit {}",
                            header.max_segment_bytes()
                        ),
                    });
                }
                if records >= header.max_segment_records() {
                    return Err(LogError::Corrupt {
                        position,
                        reason: format!(
                            "segment exceeds configured record limit {}",
                            header.max_segment_records()
                        ),
                    });
                }
                if frame.relative_offset != records {
                    return Err(LogError::Corrupt {
                        position,
                        reason: format!(
                            "record carries relative offset {}, expected {records}",
                            frame.relative_offset
                        ),
                    });
                }
                let hash = record_content_hash(&frame.record);
                match validate_sequence(&producer_states, &producer_epochs, &frame.record, hash)
                    .map_err(|error| LogError::Corrupt {
                        position,
                        reason: error.to_string(),
                    })? {
                    SequenceDecision::Append => {}
                    SequenceDecision::Duplicate(_) => {
                        return Err(LogError::Corrupt {
                            position,
                            reason: "segment contains a duplicate producer sequence".to_owned(),
                        });
                    }
                }
                if records.is_multiple_of(u64::from(header.index_stride())) {
                    index.push(IndexEntry {
                        offset: next_offset,
                        position,
                    });
                }
                remember_sequence(
                    &mut producer_states,
                    &mut producer_epochs,
                    &frame.record,
                    next_offset,
                    hash,
                );
                accumulator.update(&frame.encoded);
                position += frame.encoded_len as u64;
                next_offset = next_offset
                    .checked_add(1)
                    .ok_or_else(|| LogError::Corrupt {
                        position,
                        reason: "segment offset space exhausted".to_owned(),
                    })?;
                records += 1;
            }
        }
    }
    Ok(ScanResult {
        records,
        valid_end: position,
        truncated_bytes: actual_file_len.saturating_sub(position),
        next_offset,
        producer_states,
        producer_epochs,
        index,
        accumulator,
    })
}

#[allow(clippy::too_many_arguments)]
fn fetch_from_file(
    mut file: &mut dyn StorageFile,
    path: &Path,
    header: &AnyHeader,
    header_len: u64,
    index: &[IndexEntry],
    high_watermark: u64,
    start_offset: u64,
    max_bytes: usize,
    max_records: usize,
) -> VtopLogResult<FetchBatch> {
    if max_bytes == 0 || max_records == 0 || start_offset >= high_watermark {
        return Ok(FetchBatch {
            records: Vec::new(),
            encoded_bytes: 0,
            next_offset: start_offset.max(header.base_offset()).min(high_watermark),
            high_watermark,
        });
    }
    let requested = start_offset.max(header.base_offset());
    let entry = index
        .iter()
        .rev()
        .find(|entry| entry.offset <= requested)
        .copied()
        .unwrap_or(IndexEntry {
            offset: header.base_offset(),
            position: header_len,
        });
    file.seek(SeekFrom::Start(entry.position))
        .map_err(|source| io_error(path, source))?;
    let mut offset = entry.offset;
    let mut position = entry.position;
    let mut records = Vec::new();
    let mut encoded_bytes = 0_usize;

    while offset < high_watermark && records.len() < max_records {
        let frame = match read_frame_any(
            header.format_version(),
            &mut file,
            position,
            header.max_record_bytes(),
        )
        .map_err(|error| with_path(error, path))?
        {
            FrameRead::Complete(frame) => frame,
            FrameRead::End | FrameRead::Torn => {
                return Err(LogError::Corrupt {
                    position,
                    reason: "record disappeared below the high watermark".to_owned(),
                });
            }
        };
        position += frame.encoded_len as u64;
        let expected_relative = offset - header.base_offset();
        if frame.relative_offset != expected_relative {
            return Err(LogError::Corrupt {
                position,
                reason: format!(
                    "record carries relative offset {}, expected {expected_relative}",
                    frame.relative_offset
                ),
            });
        }
        if offset < requested {
            offset += 1;
            continue;
        }
        if encoded_bytes + frame.encoded_len > max_bytes {
            break;
        }
        encoded_bytes += frame.encoded_len;
        records.push(FetchedRecord {
            offset,
            record: frame.record,
        });
        offset += 1;
    }
    file.seek(SeekFrom::End(0))
        .map_err(|source| io_error(path, source))?;
    Ok(FetchBatch {
        records,
        encoded_bytes,
        next_offset: offset,
        high_watermark,
    })
}

fn remember_sequence(
    states: &mut HashMap<ProducerKey, ProducerState>,
    epochs: &mut HashMap<Uuid, u64>,
    record: &LogRecord,
    offset: u64,
    content_hash: blake3::Hash,
) {
    let latest_epoch = epochs
        .entry(record.producer_id)
        .or_insert(record.producer_epoch);
    *latest_epoch = (*latest_epoch).max(record.producer_epoch);
    states
        .entry((record.producer_id, record.producer_epoch))
        .and_modify(|state| {
            state.latest_sequence = record.sequence;
            state.seen.insert(
                record.sequence,
                SeenRecord {
                    offset,
                    content_hash,
                },
            );
            evict_below_window(&mut state.seen, record.sequence);
        })
        .or_insert_with(|| ProducerState {
            latest_sequence: record.sequence,
            seen: BTreeMap::from([(
                record.sequence,
                SeenRecord {
                    offset,
                    content_hash,
                },
            )]),
        });
}

fn validate_manifest(
    manifest: &SegmentManifest,
    header: &SegmentHeader,
    scan: &ScanResult,
    header_len: u64,
) -> VtopLogResult<()> {
    let ContentAccumulator::V1(hasher) = &scan.accumulator else {
        return Err(LogError::ManifestMismatch(
            "v1 manifest paired with a non-v1 content accumulator".to_owned(),
        ));
    };
    let root = hasher.clone().finalize().to_hex().to_string();
    let expected_first = (scan.records > 0).then_some(header.descriptor.base_offset);
    let content_bytes = scan.valid_end - header_len;
    if manifest.format != FORMAT_NAME
        || manifest.version != FORMAT_VERSION
        || manifest.descriptor != header.descriptor
        || manifest.record_count != scan.records
        || manifest.first_offset != expected_first
        || manifest.next_offset != scan.next_offset
        || manifest.content_bytes != content_bytes
        || manifest.blake3_root != root
        || manifest.index_stride != header.config.index_stride
    {
        return Err(LogError::ManifestMismatch(
            "descriptor, offsets, length, index stride, or BLAKE3 root differs".to_owned(),
        ));
    }
    Ok(())
}

fn validate_manifest_v2(
    manifest: &SegmentManifestV2,
    header: &SegmentHeaderV2,
    scan: &ScanResult,
    header_len: u64,
    leaves: &[blake3::Hash],
    root: &blake3::Hash,
) -> VtopLogResult<()> {
    let expected_first = (scan.records > 0).then_some(header.descriptor.base_offset);
    let content_bytes = scan.valid_end - header_len;
    if manifest.format != FORMAT_NAME
        || manifest.version != FORMAT_VERSION_V2
        || manifest.record_schema_version != RECORD_SCHEMA_VERSION_V2
        || manifest.descriptor != header.descriptor
        || manifest.record_count != scan.records
        || manifest.first_offset != expected_first
        || manifest.next_offset != scan.next_offset
        || manifest.content_bytes != content_bytes
        // A sealed segment publishes only committed bytes, so its high
        // watermark must sit exactly on the validated record frontier.
        || manifest.committed_high_watermark != scan.next_offset
        || manifest.producer_summary != producer_summary_from_states(&scan.producer_states)
        || manifest.chunk_size != header.config.chunk_size
        || manifest.chunk_count != leaves.len() as u64
        || manifest.chunk_tree_scheme != CHUNK_TREE_SCHEME_V1
        || manifest.chunk_tree_root != root.to_hex().as_str()
        || manifest.index_stride != header.config.index_stride
    {
        return Err(LogError::ManifestMismatch(
            "descriptor, offsets, length, producer summary, index stride, or chunk tree differs"
                .to_owned(),
        ));
    }
    if let Some(statement) = &manifest.commit_statement {
        let expected = commit_statement_core(&SegmentManifestV2 {
            commit_statement: None,
            ..manifest.clone()
        })?;
        if statement.statement_version != expected.statement_version
            || statement.segment_id != expected.segment_id
            || statement.segment_generation != expected.segment_generation
            || statement.topic != expected.topic
            || statement.topic_epoch != expected.topic_epoch
            || statement.range_id != expected.range_id
            || statement.range_generation != expected.range_generation
            || statement.base_offset != expected.base_offset
            || statement.committed_high_watermark != expected.committed_high_watermark
            || statement.content_bytes != expected.content_bytes
            || statement.chunk_tree_root != expected.chunk_tree_root
            || statement.manifest_core_digest != expected.manifest_core_digest
        {
            return Err(LogError::ManifestMismatch(
                "commit statement does not restate the sealed manifest core".to_owned(),
            ));
        }
        // An unkeyed digest is verifiable without configuration; a keyed MAC
        // is verified by callers holding the runtime key.
        if statement.scheme == crate::types::COMMIT_SCHEME_UNKEYED {
            statement.verify(None)?;
        } else if statement.scheme != crate::types::COMMIT_SCHEME_KEYED {
            return Err(LogError::ManifestMismatch(format!(
                "unknown commit statement scheme {:?}",
                statement.scheme
            )));
        }
    }
    Ok(())
}

/// Per-(producer, epoch) coverage, sorted by `(producer_id, producer_epoch)`.
fn producer_summary_from_states(
    states: &HashMap<ProducerKey, ProducerState>,
) -> Vec<ProducerSummaryEntry> {
    let mut summary: Vec<ProducerSummaryEntry> = states
        .iter()
        .map(
            |((producer_id, producer_epoch), state)| ProducerSummaryEntry {
                producer_id: *producer_id,
                producer_epoch: *producer_epoch,
                first_sequence: state
                    .seen
                    .keys()
                    .copied()
                    .min()
                    .unwrap_or(state.latest_sequence),
                last_sequence: state.latest_sequence,
                record_count: state.seen.len() as u64,
            },
        )
        .collect();
    summary.sort_by_key(|entry| (entry.producer_id, entry.producer_epoch));
    summary
}

/// The commit statement implied by a sealed manifest whose statement slot is
/// still empty; `scheme` and `mac` are filled by authentication.
pub(crate) fn commit_statement_core(
    manifest: &SegmentManifestV2,
) -> VtopLogResult<CommitStatementV1> {
    let core_bytes = canonical_manifest_v2_bytes(manifest)?;
    Ok(CommitStatementV1 {
        statement_version: 1,
        scheme: String::new(),
        key_id: String::new(),
        segment_id: manifest.descriptor.segment_id,
        segment_generation: manifest.descriptor.segment_generation,
        topic: manifest.descriptor.topic.clone(),
        topic_epoch: manifest.descriptor.topic_epoch,
        range_id: manifest.descriptor.lineage.range_id,
        range_generation: manifest.descriptor.lineage.generation,
        base_offset: manifest.descriptor.base_offset,
        committed_high_watermark: manifest.committed_high_watermark,
        content_bytes: manifest.content_bytes,
        chunk_tree_root: manifest.chunk_tree_root.clone(),
        manifest_core_digest: blake3::hash(&core_bytes).to_hex().to_string(),
        mac: String::new(),
    })
}

fn write_manifest_atomic(env: &Env, path: &Path, manifest: &SegmentManifest) -> VtopLogResult<()> {
    let bytes = canonical_manifest_bytes(manifest)?;
    write_atomic(env, path, &bytes)
}

fn write_manifest_v2_atomic(
    env: &Env,
    path: &Path,
    manifest: &SegmentManifestV2,
) -> VtopLogResult<()> {
    let bytes = canonical_manifest_v2_bytes(manifest)?;
    write_atomic(env, path, &bytes)
}

fn write_commit_boundary_atomic(
    env: &Env,
    path: &Path,
    boundary: CommitBoundary,
) -> VtopLogResult<()> {
    write_atomic(env, path, &encode_commit_boundary(boundary))
}

fn encode_commit_boundary(boundary: CommitBoundary) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(COMMIT_BOUNDARY_LEN);
    bytes.extend_from_slice(COMMIT_MAGIC);
    bytes.extend_from_slice(&COMMIT_VERSION.to_be_bytes());
    bytes.extend_from_slice(boundary.segment_id.as_bytes());
    bytes.extend_from_slice(&boundary.committed_offset.to_be_bytes());
    bytes.extend_from_slice(&boundary.content_bytes.to_be_bytes());
    let checksum = blake3::hash(&bytes);
    bytes.extend_from_slice(checksum.as_bytes());
    debug_assert_eq!(bytes.len(), COMMIT_BOUNDARY_LEN);
    bytes
}

fn read_commit_boundary(storage: &dyn Storage, path: &Path) -> VtopLogResult<CommitBoundary> {
    let bytes = storage
        .read(path)
        .map_err(|source| io_error(path, source))?;
    if bytes.len() != COMMIT_BOUNDARY_LEN {
        return Err(LogError::CommitBoundaryMismatch(format!(
            "expected {COMMIT_BOUNDARY_LEN} bytes, found {}",
            bytes.len()
        )));
    }
    if &bytes[..8] != COMMIT_MAGIC {
        return Err(LogError::CommitBoundaryMismatch(
            "invalid commit marker magic".to_owned(),
        ));
    }
    let version = u16::from_be_bytes(bytes[8..10].try_into().expect("fixed slice"));
    if version != COMMIT_VERSION {
        return Err(LogError::CommitBoundaryMismatch(format!(
            "unsupported commit marker version {version}"
        )));
    }
    let checksum_start = COMMIT_BOUNDARY_LEN - 32;
    if blake3::hash(&bytes[..checksum_start]).as_bytes() != &bytes[checksum_start..] {
        return Err(LogError::CommitBoundaryMismatch(
            "commit marker checksum mismatch".to_owned(),
        ));
    }
    let segment_id = Uuid::from_slice(&bytes[10..26]).map_err(|error| {
        LogError::CommitBoundaryMismatch(format!("invalid segment id: {error}"))
    })?;
    Ok(CommitBoundary {
        segment_id,
        committed_offset: u64::from_be_bytes(bytes[26..34].try_into().expect("fixed slice")),
        content_bytes: u64::from_be_bytes(bytes[34..42].try_into().expect("fixed slice")),
    })
}

pub(crate) fn canonical_manifest_bytes(manifest: &SegmentManifest) -> VtopLogResult<Vec<u8>> {
    let mut bytes = serde_json::to_vec(manifest)
        .map_err(|error| LogError::ManifestMismatch(format!("cannot encode manifest: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn canonical_manifest_v2_bytes(manifest: &SegmentManifestV2) -> VtopLogResult<Vec<u8>> {
    let mut bytes = serde_json::to_vec(manifest)
        .map_err(|error| LogError::ManifestMismatch(format!("cannot encode manifest: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn encode_chunk_sidecar(chunk_size: u32, leaves: &[blake3::Hash]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(CHUNK_SIDECAR_FIXED_LEN + leaves.len() * 32);
    bytes.extend_from_slice(CHUNK_SIDECAR_MAGIC);
    bytes.extend_from_slice(&CHUNK_SIDECAR_VERSION.to_be_bytes());
    bytes.extend_from_slice(&chunk_size.to_be_bytes());
    bytes.extend_from_slice(&(leaves.len() as u64).to_be_bytes());
    for leaf in leaves {
        bytes.extend_from_slice(leaf.as_bytes());
    }
    let checksum = blake3::hash(&bytes);
    bytes.extend_from_slice(checksum.as_bytes());
    bytes
}

fn write_chunk_sidecar_atomic(
    env: &Env,
    path: &Path,
    chunk_size: u32,
    leaves: &[blake3::Hash],
) -> VtopLogResult<()> {
    write_atomic(env, path, &encode_chunk_sidecar(chunk_size, leaves))
}

pub(crate) fn read_chunk_sidecar(
    storage: &dyn Storage,
    path: &Path,
) -> VtopLogResult<(u32, Vec<blake3::Hash>)> {
    let bytes = storage
        .read(path)
        .map_err(|source| io_error(path, source))?;
    if bytes.len() < CHUNK_SIDECAR_FIXED_LEN || &bytes[..8] != CHUNK_SIDECAR_MAGIC {
        return Err(LogError::Corrupt {
            position: 0,
            reason: "invalid chunk-sidecar header".to_owned(),
        });
    }
    let version = u16::from_be_bytes(bytes[8..10].try_into().expect("fixed slice"));
    if version != CHUNK_SIDECAR_VERSION {
        return Err(LogError::UnsupportedVersion(version));
    }
    let chunk_size = u32::from_be_bytes(bytes[10..14].try_into().expect("fixed slice"));
    let chunk_count = u64::from_be_bytes(bytes[14..22].try_into().expect("fixed slice"));
    let expected = chunk_count
        .checked_mul(32)
        .and_then(|length| length.checked_add(CHUNK_SIDECAR_FIXED_LEN as u64))
        .and_then(|length| usize::try_from(length).ok())
        .ok_or_else(|| LogError::Corrupt {
            position: 14,
            reason: "chunk-sidecar leaf count overflows".to_owned(),
        })?;
    if bytes.len() != expected {
        return Err(LogError::Corrupt {
            position: 14,
            reason: "chunk-sidecar length does not match its leaf count".to_owned(),
        });
    }
    let checksum_start = bytes.len() - 32;
    if blake3::hash(&bytes[..checksum_start]).as_bytes() != &bytes[checksum_start..] {
        return Err(LogError::Corrupt {
            position: 0,
            reason: "chunk-sidecar checksum mismatch".to_owned(),
        });
    }
    let leaves = bytes[22..checksum_start]
        .chunks_exact(32)
        .map(|chunk| blake3::Hash::from_bytes(chunk.try_into().expect("fixed slice")))
        .collect();
    Ok((chunk_size, leaves))
}

fn write_index_atomic(env: &Env, path: &Path, entries: &[IndexEntry]) -> VtopLogResult<()> {
    let mut bytes = Vec::with_capacity(16 + entries.len() * 16);
    bytes.extend_from_slice(INDEX_MAGIC);
    bytes.extend_from_slice(&(entries.len() as u64).to_be_bytes());
    for entry in entries {
        bytes.extend_from_slice(&entry.offset.to_be_bytes());
        bytes.extend_from_slice(&entry.position.to_be_bytes());
    }
    write_atomic(env, path, &bytes)
}

fn read_index(storage: &dyn Storage, path: &Path) -> VtopLogResult<Vec<IndexEntry>> {
    let bytes = storage
        .read(path)
        .map_err(|source| io_error(path, source))?;
    if bytes.len() < 16 || &bytes[..8] != INDEX_MAGIC {
        return Err(LogError::Corrupt {
            position: 0,
            reason: "invalid sparse-index header".to_owned(),
        });
    }
    let count = u64::from_be_bytes(bytes[8..16].try_into().expect("fixed slice"));
    let expected = count
        .checked_mul(16)
        .and_then(|length| length.checked_add(16))
        .and_then(|length| usize::try_from(length).ok())
        .ok_or_else(|| LogError::Corrupt {
            position: 8,
            reason: "sparse-index entry count overflows".to_owned(),
        })?;
    if bytes.len() != expected {
        return Err(LogError::Corrupt {
            position: 8,
            reason: "sparse-index length does not match its entry count".to_owned(),
        });
    }
    Ok(bytes[16..]
        .chunks_exact(16)
        .map(|chunk| IndexEntry {
            offset: u64::from_be_bytes(chunk[..8].try_into().expect("fixed slice")),
            position: u64::from_be_bytes(chunk[8..].try_into().expect("fixed slice")),
        })
        .collect())
}

fn write_atomic(env: &Env, path: &Path, bytes: &[u8]) -> VtopLogResult<()> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            LogError::InvalidDescriptor("sidecar path has no UTF-8 filename".to_owned())
        })?;
    let temporary = path.with_file_name(format!(
        ".{file_name}.{}.tmp",
        Uuid::from_u128(env.rng.next_u128())
    ));
    let mut file = env
        .storage
        .open(&temporary, OpenMode::CreateNew)
        .map_err(|source| io_error(&temporary, source))?;
    let result = file.write_all(bytes).and_then(|()| file.sync_data());
    drop(file);
    if let Err(source) = result {
        let _ = env.storage.remove_file(&temporary);
        return Err(io_error(&temporary, source));
    }
    if let Err(source) = env.storage.rename(&temporary, path) {
        let _ = env.storage.remove_file(&temporary);
        return Err(io_error(path, source));
    }
    sync_parent(env.storage.as_ref(), path)
}

fn sync_parent(storage: &dyn Storage, path: &Path) -> VtopLogResult<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    storage
        .sync_dir(parent)
        .map_err(|source| io_error(parent, source))
}

fn read_header_with_path(
    mut file: &mut dyn StorageFile,
    path: &Path,
) -> VtopLogResult<(AnyHeader, u64)> {
    read_header(&mut file).map_err(|error| with_path(error, path))
}

fn with_path(error: LogError, path: &Path) -> LogError {
    match error {
        LogError::Io { source, .. } => io_error(path, source),
        other => other,
    }
}

fn io_error(path: &Path, source: std::io::Error) -> LogError {
    LogError::Io {
        path: path.to_path_buf(),
        source,
    }
}

pub(crate) struct SegmentPaths {
    pub(crate) segment: PathBuf,
    pub(crate) index: PathBuf,
    pub(crate) manifest: PathBuf,
    pub(crate) commit: PathBuf,
    pub(crate) chunks: PathBuf,
}

impl SegmentPaths {
    fn from_active(path: &Path) -> VtopLogResult<Self> {
        if path.extension().and_then(|value| value.to_str()) != Some("active") {
            return Err(LogError::InvalidDescriptor(
                "active segment path must end in .active".to_owned(),
            ));
        }
        Self::from_stem(path)
    }

    pub(crate) fn from_segment(path: &Path) -> VtopLogResult<Self> {
        if path.extension().and_then(|value| value.to_str()) != Some("segment") {
            return Err(LogError::InvalidDescriptor(
                "sealed segment path must end in .segment".to_owned(),
            ));
        }
        Self::from_stem(path)
    }

    fn from_stem(path: &Path) -> VtopLogResult<Self> {
        let stem = path
            .file_stem()
            .and_then(|value| value.to_str())
            .ok_or_else(|| {
                LogError::InvalidDescriptor("segment filename is not UTF-8".to_owned())
            })?;
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        Ok(Self {
            segment: parent.join(format!("{stem}.segment")),
            index: parent.join(format!("{stem}.index")),
            manifest: parent.join(format!("{stem}.manifest.json")),
            commit: parent.join(format!("{stem}.commit")),
            chunks: parent.join(format!("{stem}.chunks")),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, OpenOptions};
    use std::io::Seek;
    use tempfile::tempdir;

    fn real_storage() -> std::sync::Arc<dyn Storage> {
        Env::real().storage
    }

    fn descriptor() -> SegmentDescriptor {
        SegmentDescriptor {
            segment_id: Uuid::from_u128(1),
            topic: "events.v1".to_owned(),
            topic_epoch: 7,
            lineage: crate::RangeLineage::root(Uuid::from_u128(2)),
            base_offset: 40,
        }
    }

    fn config() -> SegmentConfig {
        SegmentConfig {
            max_record_bytes: 1024,
            max_group_bytes: 4096,
            max_segment_bytes: 16 * 1024,
            max_segment_records: 100,
            index_stride: 2,
        }
    }

    fn record(producer: Uuid, sequence: u64, value: &[u8]) -> LogRecord {
        LogRecord {
            producer_id: producer,
            producer_epoch: 0,
            sequence,
            timestamp_millis: 1_700_000_000_000 + sequence as i64,
            attributes: 0,
            key: b"key".to_vec(),
            value: value.to_vec(),
        }
    }

    #[test]
    fn v1_append_rejects_nonzero_schema_v2_record_fields() {
        let directory = tempdir().unwrap();
        let active_path = directory.path().join("v2-fields.active");
        let mut segment = ActiveSegment::create(&active_path, descriptor(), config()).unwrap();

        let mut epoch = record(Uuid::from_u128(3), 0, b"a");
        epoch.producer_epoch = 1;
        assert!(matches!(
            segment.append(epoch, Durability::Buffered),
            Err(LogError::UnsupportedRecordField("producer_epoch"))
        ));

        let mut attributes = record(Uuid::from_u128(3), 0, b"a");
        attributes.attributes = 1;
        assert!(matches!(
            segment.append(attributes, Durability::Buffered),
            Err(LogError::UnsupportedRecordField("attributes"))
        ));

        // The rejection happens before any state change; a clean record with
        // the same sequence still lands at the first offset.
        let outcome = segment
            .append(record(Uuid::from_u128(3), 0, b"a"), Durability::Fsync)
            .unwrap();
        assert!(matches!(outcome, AppendOutcome::Appended { offset: 40 }));
    }

    #[test]
    fn append_fetch_seal_and_reopen_round_trip() {
        let directory = tempdir().unwrap();
        let active_path = directory.path().join("one.active");
        let producer = Uuid::from_u128(3);
        let mut segment = ActiveSegment::create(&active_path, descriptor(), config()).unwrap();
        let outcomes = segment
            .append_group(
                &[
                    record(producer, 0, b"alpha"),
                    record(producer, 1, b"beta"),
                    record(producer, 2, b"gamma"),
                ],
                Durability::Fsync,
            )
            .unwrap();
        assert_eq!(outcomes[0], AppendOutcome::Appended { offset: 40 });
        let too_small = segment.fetch(40, 1, 10).unwrap();
        assert!(too_small.records.is_empty());
        assert_eq!(too_small.encoded_bytes, 0);
        assert_eq!(too_small.next_offset, 40);
        let first = segment.fetch(40, 1024, 1).unwrap();
        assert_eq!(first.records.len(), 1);
        assert_eq!(first.records[0].record.value, b"alpha");
        assert!(first.encoded_bytes <= 1024);
        assert_eq!(first.next_offset, 41);

        let mut sealed = segment.seal().unwrap();
        assert_eq!(sealed.manifest().record_count, 3);
        assert_eq!(sealed.manifest().first_offset, Some(40));
        assert_eq!(sealed.manifest().next_offset, 43);
        assert_eq!(sealed.manifest().blake3_root.len(), 64);
        let fetched = sealed.fetch(41, usize::MAX, 10).unwrap();
        assert_eq!(fetched.records.len(), 2);
        assert_eq!(fetched.records[0].record.value, b"beta");
        assert_eq!(fetched.records[1].record.value, b"gamma");
        let cursor = sealed.cursor(42).unwrap();
        assert_eq!(cursor.topic_epoch, 7);
        assert_eq!(cursor.range_id, Uuid::from_u128(2));
        assert_eq!(cursor.segment_root, sealed.manifest().blake3_root);
        assert!(matches!(sealed.cursor(39), Err(LogError::InvalidCursor(_))));
    }

    #[test]
    fn duplicate_retry_is_idempotent_but_conflicting_retry_is_rejected() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(4);
        let original = record(producer, 0, b"same");
        let mut segment = ActiveSegment::create(
            directory.path().join("duplicate.active"),
            descriptor(),
            config(),
        )
        .unwrap();
        assert_eq!(
            segment
                .append(original.clone(), Durability::Buffered)
                .unwrap(),
            AppendOutcome::Appended { offset: 40 }
        );
        assert_eq!(
            segment.append(original, Durability::Fsync).unwrap(),
            AppendOutcome::Duplicate { offset: 40 }
        );
        assert_eq!(segment.next_offset(), 41);
        assert_eq!(segment.committed_offset(), 41);
        let error = segment
            .append(record(producer, 0, b"different"), Durability::Buffered)
            .unwrap_err();
        assert!(matches!(error, LogError::SequenceConflict { .. }));
    }

    /// Limits wide enough to push a producer past `PRODUCER_SEQUENCE_WINDOW`
    /// accepted sequences within one segment.
    fn window_config() -> SegmentConfig {
        SegmentConfig {
            max_record_bytes: 1024,
            max_group_bytes: 16 * 1024 * 1024,
            max_segment_bytes: 64 * 1024 * 1024,
            max_segment_records: 2 * PRODUCER_SEQUENCE_WINDOW,
            index_stride: 4096,
        }
    }

    /// Append sequences `0..count` for `producer` in groups of `group_len`.
    fn fill_past_window(segment: &mut ActiveSegment, producer: Uuid, count: u64, group_len: u64) {
        let mut sequence = 0;
        while sequence < count {
            let batch: Vec<LogRecord> = (sequence..(sequence + group_len).min(count))
                .map(|sequence| record(producer, sequence, b"w"))
                .collect();
            sequence += batch.len() as u64;
            for outcome in segment.append_group(&batch, Durability::Fsync).unwrap() {
                assert!(matches!(outcome, AppendOutcome::Appended { .. }));
            }
        }
    }

    #[test]
    fn sequence_window_bounds_seen_state_and_rejects_evicted_retries() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(90);
        let mut segment = ActiveSegment::create(
            directory.path().join("window.active"),
            descriptor(),
            window_config(),
        )
        .unwrap();
        let total = PRODUCER_SEQUENCE_WINDOW + 10;
        // One giant group: the delta apply path must evict just like the
        // per-append path, so a single group cannot bypass the bound.
        fill_past_window(&mut segment, producer, total, total);
        let latest = total - 1;
        let floor = latest - (PRODUCER_SEQUENCE_WINDOW - 1);

        let state = segment.producer_states.get(&(producer, 0)).unwrap();
        assert_eq!(state.seen.len() as u64, PRODUCER_SEQUENCE_WINDOW);
        assert_eq!(state.seen.keys().next().copied(), Some(floor));

        // Retries inside the window keep their exact answers.
        assert_eq!(
            segment
                .append(record(producer, latest, b"w"), Durability::Buffered)
                .unwrap(),
            AppendOutcome::Duplicate {
                offset: 40 + latest
            }
        );
        assert_eq!(
            segment
                .append(record(producer, floor, b"w"), Durability::Buffered)
                .unwrap(),
            AppendOutcome::Duplicate { offset: 40 + floor }
        );
        assert!(matches!(
            segment.append(record(producer, floor, b"different"), Durability::Buffered),
            Err(LogError::SequenceConflict { .. })
        ));

        // The evicted sequence right below the floor is rejected fail-closed.
        match segment
            .append(record(producer, floor - 1, b"w"), Durability::Buffered)
            .unwrap_err()
        {
            LogError::SequenceBelowWindow {
                producer_id,
                producer_epoch,
                sequence,
                window_floor,
            } => {
                assert_eq!(producer_id, producer);
                assert_eq!(producer_epoch, 0);
                assert_eq!(sequence, floor - 1);
                assert_eq!(window_floor, floor);
            }
            other => panic!("expected SequenceBelowWindow, got {other:?}"),
        }
    }

    #[test]
    fn recovery_rebuilds_the_same_bounded_window_as_the_live_path() {
        let directory = tempdir().unwrap();
        let active_path = directory.path().join("window-recovery.active");
        let producer = Uuid::from_u128(91);
        let mut live = ActiveSegment::create(&active_path, descriptor(), window_config()).unwrap();
        let total = PRODUCER_SEQUENCE_WINDOW + 3;
        // Multiple groups: crossing the boundary exercises merge eviction.
        fill_past_window(&mut live, producer, total, 4096);
        let latest = total - 1;
        let floor = latest - (PRODUCER_SEQUENCE_WINDOW - 1);

        let mut recovered = ActiveSegment::recover(&active_path).unwrap();
        let live_state = live.producer_states.get(&(producer, 0)).unwrap();
        let recovered_state = recovered.producer_states.get(&(producer, 0)).unwrap();
        assert_eq!(recovered_state.latest_sequence, live_state.latest_sequence);
        assert_eq!(recovered_state.seen.len(), live_state.seen.len());
        assert_eq!(recovered_state.seen.len() as u64, PRODUCER_SEQUENCE_WINDOW);

        // Probes around the window boundary must decide identically on the
        // live instance and after recovery: rejected below the floor,
        // duplicate at the floor and at the latest sequence.
        for probe in [floor - 1, floor, latest] {
            let live_decision = live.append(record(producer, probe, b"w"), Durability::Buffered);
            let recovered_decision =
                recovered.append(record(producer, probe, b"w"), Durability::Buffered);
            assert_eq!(
                format!("{live_decision:?}"),
                format!("{recovered_decision:?}"),
                "live and recovered decisions diverged at sequence {probe}"
            );
            if probe < floor {
                assert!(matches!(
                    live_decision,
                    Err(LogError::SequenceBelowWindow { window_floor, .. }) if window_floor == floor
                ));
            } else {
                assert_eq!(
                    live_decision.unwrap(),
                    AppendOutcome::Duplicate { offset: 40 + probe }
                );
            }
        }
    }

    #[test]
    fn epoch_bump_starts_a_fresh_sequence_window() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(92);
        let mut segment = ActiveSegment::create_v2(
            directory.path().join("window-epoch.active"),
            descriptor_v2(),
            config_v2(),
        )
        .unwrap();
        for sequence in 0..3 {
            segment
                .append(
                    record_v2(producer, 0, sequence, b"old"),
                    Durability::Buffered,
                )
                .unwrap();
        }
        // The bumped epoch keys a fresh window that starts at sequence 0.
        assert!(matches!(
            segment.append(record_v2(producer, 1, 3, b"new"), Durability::Buffered),
            Err(LogError::FirstSequence { .. })
        ));
        assert_eq!(
            segment
                .append(record_v2(producer, 1, 0, b"new"), Durability::Buffered)
                .unwrap(),
            AppendOutcome::Appended { offset: 45 }
        );
        let fresh = segment.producer_states.get(&(producer, 1)).unwrap();
        assert_eq!(fresh.seen.len(), 1);
        assert_eq!(fresh.latest_sequence, 0);
    }

    #[test]
    fn buffered_records_stay_below_fetch_high_watermark_until_fsync() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(16);
        let original = record(producer, 0, b"buffered");
        let mut segment = ActiveSegment::create(
            directory.path().join("commit-point.active"),
            descriptor(),
            config(),
        )
        .unwrap();
        segment
            .append(original.clone(), Durability::Buffered)
            .unwrap();
        assert_eq!(segment.next_offset(), 41);
        assert_eq!(segment.committed_offset(), 40);
        let before_commit = segment.fetch(40, usize::MAX, 10).unwrap();
        assert!(before_commit.records.is_empty());
        assert_eq!(before_commit.high_watermark, 40);

        assert_eq!(
            segment.append(original, Durability::Fsync).unwrap(),
            AppendOutcome::Duplicate { offset: 40 }
        );
        let after_commit = segment.fetch(40, usize::MAX, 10).unwrap();
        assert_eq!(after_commit.records.len(), 1);
        assert_eq!(after_commit.high_watermark, 41);
    }

    #[test]
    fn recovery_discards_a_complete_uncommitted_tail() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("uncommitted-tail.active");
        let producer = Uuid::from_u128(18);
        let mut segment = ActiveSegment::create(&path, descriptor(), config()).unwrap();
        segment
            .append(record(producer, 0, b"committed"), Durability::Fsync)
            .unwrap();
        let committed_len = fs::metadata(&path).unwrap().len();
        segment
            .append(record(producer, 1, b"buffered"), Durability::Buffered)
            .unwrap();
        let accepted_len = fs::metadata(&path).unwrap().len();
        assert!(accepted_len > committed_len);
        drop(segment);

        let mut recovered = ActiveSegment::recover(&path).unwrap();
        assert_eq!(recovered.next_offset(), 41);
        assert_eq!(recovered.committed_offset(), 41);
        assert_eq!(
            recovered.recovery_report().truncated_bytes,
            accepted_len - committed_len
        );
        assert_eq!(fs::metadata(&path).unwrap().len(), committed_len);
        let visible = recovered.fetch(40, usize::MAX, 10).unwrap();
        assert_eq!(visible.records.len(), 1);
        assert_eq!(visible.records[0].record.value, b"committed");
        assert_eq!(visible.high_watermark, 41);

        assert_eq!(
            recovered
                .append(record(producer, 1, b"buffered"), Durability::Fsync)
                .unwrap(),
            AppendOutcome::Appended { offset: 41 }
        );
    }

    #[test]
    fn recovery_never_promotes_a_buffered_only_segment() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("buffered-only.active");
        let mut segment = ActiveSegment::create(&path, descriptor(), config()).unwrap();
        segment
            .append(
                record(Uuid::from_u128(19), 0, b"not committed"),
                Durability::Buffered,
            )
            .unwrap();
        drop(segment);

        let mut recovered = ActiveSegment::recover(&path).unwrap();
        assert_eq!(recovered.next_offset(), 40);
        assert_eq!(recovered.committed_offset(), 40);
        assert!(recovered
            .fetch(40, usize::MAX, 10)
            .unwrap()
            .records
            .is_empty());
    }

    #[test]
    fn seal_commits_buffered_records_before_publication() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("seal-commit.active");
        let mut segment = ActiveSegment::create(&path, descriptor(), config()).unwrap();
        segment
            .append(
                record(Uuid::from_u128(20), 0, b"seal me"),
                Durability::Buffered,
            )
            .unwrap();
        assert_eq!(segment.committed_offset(), 40);

        let mut sealed = segment.seal().unwrap();
        let paths = SegmentPaths::from_segment(&sealed.path).unwrap();
        let boundary = read_commit_boundary(real_storage().as_ref(), &paths.commit).unwrap();
        assert_eq!(boundary.committed_offset, 41);
        assert_eq!(sealed.fetch(40, usize::MAX, 10).unwrap().records.len(), 1);
    }

    #[test]
    fn recovery_rejects_a_corrupt_commit_boundary_without_changing_data() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("corrupt-commit.active");
        let mut segment = ActiveSegment::create(&path, descriptor(), config()).unwrap();
        segment
            .append(
                record(Uuid::from_u128(21), 0, b"committed"),
                Durability::Fsync,
            )
            .unwrap();
        drop(segment);
        let data_len = fs::metadata(&path).unwrap().len();
        let paths = SegmentPaths::from_active(&path).unwrap();
        let mut bytes = fs::read(&paths.commit).unwrap();
        *bytes.last_mut().unwrap() ^= 0xff;
        fs::write(&paths.commit, bytes).unwrap();

        let error = match ActiveSegment::recover(&path) {
            Ok(_) => panic!("corrupt commit boundary should fail recovery"),
            Err(error) => error,
        };
        assert!(matches!(error, LogError::CommitBoundaryMismatch(_)));
        assert_eq!(fs::metadata(&path).unwrap().len(), data_len);
    }

    #[test]
    fn recovery_rejects_a_missing_commit_boundary_without_changing_data() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("missing-commit.active");
        let mut segment = ActiveSegment::create(&path, descriptor(), config()).unwrap();
        segment
            .append(
                record(Uuid::from_u128(28), 0, b"acknowledged"),
                Durability::Fsync,
            )
            .unwrap();
        drop(segment);
        let data_len = fs::metadata(&path).unwrap().len();
        let paths = SegmentPaths::from_active(&path).unwrap();
        fs::remove_file(&paths.commit).unwrap();

        let error = match ActiveSegment::recover(&path) {
            Ok(_) => panic!("missing commit boundary should fail recovery"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            LogError::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound
        ));
        assert_eq!(fs::metadata(&path).unwrap().len(), data_len);
    }

    #[test]
    fn complete_group_retry_returns_original_offsets_without_rewriting() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("group-retry.active");
        let producer = Uuid::from_u128(14);
        let group = [
            record(producer, 0, b"one"),
            record(producer, 1, b"two"),
            record(producer, 2, b"three"),
        ];
        let mut segment = ActiveSegment::create(&path, descriptor(), config()).unwrap();
        segment.append_group(&group, Durability::Fsync).unwrap();
        let length = fs::metadata(&path).unwrap().len();

        let retry = segment.append_group(&group, Durability::Fsync).unwrap();
        assert_eq!(
            retry,
            vec![
                AppendOutcome::Duplicate { offset: 40 },
                AppendOutcome::Duplicate { offset: 41 },
                AppendOutcome::Duplicate { offset: 42 },
            ]
        );
        assert_eq!(fs::metadata(&path).unwrap().len(), length);
        assert_eq!(segment.next_offset(), 43);

        drop(segment);
        let mut recovered = ActiveSegment::recover(&path).unwrap();
        let retry_after_restart = recovered.append_group(&group, Durability::Fsync).unwrap();
        assert!(retry_after_restart
            .iter()
            .all(|outcome| matches!(outcome, AppendOutcome::Duplicate { .. })));
        assert_eq!(fs::metadata(&path).unwrap().len(), length);
    }

    #[test]
    fn append_group_handles_existing_and_in_group_duplicates() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(22);
        let first = record(producer, 0, b"first");
        let second = record(producer, 1, b"second");
        let mut segment = ActiveSegment::create(
            directory.path().join("delta-validation.active"),
            descriptor(),
            config(),
        )
        .unwrap();
        segment.append(first.clone(), Durability::Fsync).unwrap();

        let outcomes = segment
            .append_group(&[first, second.clone(), second], Durability::Fsync)
            .unwrap();
        assert_eq!(
            outcomes,
            vec![
                AppendOutcome::Duplicate { offset: 40 },
                AppendOutcome::Appended { offset: 41 },
                AppendOutcome::Duplicate { offset: 41 },
            ]
        );
        assert_eq!(segment.next_offset(), 42);
        assert_eq!(segment.fetch(40, usize::MAX, 10).unwrap().records.len(), 2);
    }

    #[test]
    fn group_is_fully_validated_before_writing() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("atomic-validation.active");
        let producer = Uuid::from_u128(5);
        let mut segment = ActiveSegment::create(&path, descriptor(), config()).unwrap();
        let before = fs::metadata(&path).unwrap().len();
        let error = segment
            .append_group(
                &[record(producer, 0, b"valid"), record(producer, 2, b"gap")],
                Durability::Buffered,
            )
            .unwrap_err();
        assert!(matches!(error, LogError::SequenceGap { expected: 1, .. }));
        assert_eq!(fs::metadata(&path).unwrap().len(), before);
        assert_eq!(segment.next_offset(), 40);
    }

    #[test]
    fn segment_capacity_is_hard_bounded_before_writing() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("bounded.active");
        let producer = Uuid::from_u128(17);
        let mut bounded = config();
        bounded.max_segment_records = 1;
        let mut segment = ActiveSegment::create(&path, descriptor(), bounded).unwrap();
        segment
            .append(record(producer, 0, b"first"), Durability::Fsync)
            .unwrap();
        let length = fs::metadata(&path).unwrap().len();
        let error = segment
            .append(record(producer, 1, b"second"), Durability::Fsync)
            .unwrap_err();
        assert!(matches!(error, LogError::SegmentRecordLimit { .. }));
        assert_eq!(fs::metadata(&path).unwrap().len(), length);
        assert_eq!(segment.next_offset(), 41);
    }

    #[test]
    fn recovery_truncates_every_torn_tail_boundary_and_preserves_idempotency() {
        let producer = Uuid::from_u128(6);
        let second = record(producer, 1, b"second-record");
        let encoded_second = encode_record(&second, 1, config().max_record_bytes).unwrap();

        for retained in 0..encoded_second.len() {
            let directory = tempdir().unwrap();
            let path = directory.path().join("torn.active");
            let mut segment = ActiveSegment::create(&path, descriptor(), config()).unwrap();
            segment
                .append(record(producer, 0, b"first"), Durability::Fsync)
                .unwrap();
            drop(segment);
            let stable_len = fs::metadata(&path).unwrap().len();
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&encoded_second[..retained]).unwrap();
            file.sync_all().unwrap();
            drop(file);

            let mut recovered = ActiveSegment::recover(&path).unwrap();
            assert_eq!(recovered.next_offset(), 41, "retained={retained}");
            assert_eq!(fs::metadata(&path).unwrap().len(), stable_len);
            assert_eq!(
                recovered.append(second.clone(), Durability::Fsync).unwrap(),
                AppendOutcome::Appended { offset: 41 }
            );
        }
    }

    #[test]
    fn recovery_rejects_checksum_corruption_without_truncating() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("corrupt.active");
        let mut segment = ActiveSegment::create(&path, descriptor(), config()).unwrap();
        segment
            .append(
                record(Uuid::from_u128(7), 0, b"protected"),
                Durability::Fsync,
            )
            .unwrap();
        drop(segment);
        let length = fs::metadata(&path).unwrap().len();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::End(-33)).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let error = match ActiveSegment::recover(&path) {
            Ok(_) => panic!("corruption should fail recovery"),
            Err(error) => error,
        };
        assert!(matches!(error, LogError::Corrupt { .. }));
        assert_eq!(fs::metadata(&path).unwrap().len(), length);
    }

    #[test]
    fn active_apis_reject_non_active_and_sealed_paths() {
        let directory = tempdir().unwrap();
        let invalid_path = directory.path().join("wrong.segment");
        let create_error = match ActiveSegment::create(&invalid_path, descriptor(), config()) {
            Ok(_) => panic!("non-active path should be rejected"),
            Err(error) => error,
        };
        assert!(matches!(create_error, LogError::InvalidDescriptor(_)));
        assert!(!invalid_path.exists());

        let active_path = directory.path().join("immutable.active");
        let mut active = ActiveSegment::create(&active_path, descriptor(), config()).unwrap();
        active
            .append(
                record(Uuid::from_u128(23), 0, b"immutable"),
                Durability::Fsync,
            )
            .unwrap();
        let sealed = active.seal().unwrap();
        let sealed_path = sealed.path.clone();
        drop(sealed);
        let before = fs::metadata(&sealed_path).unwrap().len();

        let recover_error = match ActiveSegment::recover(&sealed_path) {
            Ok(_) => panic!("sealed path should never become writable"),
            Err(error) => error,
        };
        assert!(matches!(recover_error, LogError::InvalidDescriptor(_)));
        assert_eq!(fs::metadata(&sealed_path).unwrap().len(), before);
        SegmentReader::open(&sealed_path).unwrap();
    }

    #[test]
    fn sealing_a_file_shorter_than_its_header_returns_corruption() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("short-header.active");
        let active = ActiveSegment::create(&path, descriptor(), config()).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(0)
            .unwrap();
        let error = match active.seal() {
            Ok(_) => panic!("short active file should fail sealing"),
            Err(error) => error,
        };
        assert!(matches!(error, LogError::Corrupt { .. }));
    }

    #[test]
    fn missing_or_damaged_sparse_index_is_rebuilt_from_segment_data() {
        let directory = tempdir().unwrap();
        let active_path = directory.path().join("rebuild.active");
        let producer = Uuid::from_u128(8);
        let mut active = ActiveSegment::create(&active_path, descriptor(), config()).unwrap();
        for sequence in 0..6 {
            active
                .append(
                    record(producer, sequence, &[sequence as u8]),
                    Durability::Buffered,
                )
                .unwrap();
        }
        let sealed = active.seal().unwrap();
        let segment_path = sealed.path.clone();
        drop(sealed);
        let paths = SegmentPaths::from_segment(&segment_path).unwrap();
        fs::write(&paths.index, b"broken").unwrap();

        let mut reopened = SegmentReader::open(&segment_path).unwrap();
        assert!(fs::metadata(&paths.index).unwrap().len() > 16);
        let fetched = reopened.fetch(44, usize::MAX, 10).unwrap();
        assert_eq!(
            fetched
                .records
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![44, 45]
        );
    }

    #[test]
    fn sealed_manifest_detects_segment_tampering() {
        let directory = tempdir().unwrap();
        let active_path = directory.path().join("tamper.active");
        let mut active = ActiveSegment::create(&active_path, descriptor(), config()).unwrap();
        active
            .append(
                record(Uuid::from_u128(9), 0, b"original"),
                Durability::Fsync,
            )
            .unwrap();
        let sealed = active.seal().unwrap();
        let segment_path = sealed.path.clone();
        drop(sealed);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&segment_path)
            .unwrap();
        file.seek(SeekFrom::End(-33)).unwrap();
        file.write_all(&[0xaa]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let error = match SegmentReader::open(&segment_path) {
            Ok(_) => panic!("tampering should fail verification"),
            Err(error) => error,
        };
        assert!(matches!(error, LogError::Corrupt { .. }));
    }

    #[test]
    fn sealed_reader_rejects_noncanonical_manifest_encoding() {
        let directory = tempdir().unwrap();
        let active_path = directory.path().join("manifest.active");
        let mut active = ActiveSegment::create(&active_path, descriptor(), config()).unwrap();
        active
            .append(
                record(Uuid::from_u128(15), 0, b"canonical"),
                Durability::Fsync,
            )
            .unwrap();
        let sealed = active.seal().unwrap();
        let segment_path = sealed.path.clone();
        drop(sealed);
        let paths = SegmentPaths::from_segment(&segment_path).unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(&fs::read(&paths.manifest).unwrap()).unwrap();
        fs::write(&paths.manifest, serde_json::to_vec_pretty(&value).unwrap()).unwrap();

        let error = match SegmentReader::open(&segment_path) {
            Ok(_) => panic!("noncanonical manifest should fail verification"),
            Err(error) => error,
        };
        assert!(matches!(error, LogError::ManifestMismatch(_)));
    }

    #[test]
    fn lineage_rejects_self_and_future_parents() {
        let range = Uuid::from_u128(10);
        let mut invalid = descriptor();
        invalid.lineage = crate::RangeLineage {
            range_id: range,
            generation: 1,
            key_range: crate::KeyRange::new(0, 1).unwrap(),
            parents: vec![crate::ParentRange {
                range_id: range,
                generation: 0,
                key_range: crate::KeyRange::full(),
            }],
        };
        let directory = tempdir().unwrap();
        let error =
            match ActiveSegment::create(directory.path().join("invalid.active"), invalid, config())
            {
                Ok(_) => panic!("invalid lineage should be rejected"),
                Err(error) => error,
            };
        assert!(matches!(error, LogError::InvalidDescriptor(_)));

        let mut missing_parent = descriptor();
        missing_parent.lineage.generation = 1;
        let error = match ActiveSegment::create(
            directory.path().join("missing-parent.active"),
            missing_parent,
            config(),
        ) {
            Ok(_) => panic!("non-root lineage without a parent should be rejected"),
            Err(error) => error,
        };
        assert!(matches!(error, LogError::InvalidDescriptor(_)));
    }

    #[test]
    fn lineage_accepts_exact_splits_and_buddy_merges_only() {
        let parent_id = Uuid::from_u128(24);
        let left_id = Uuid::from_u128(25);
        let right_id = Uuid::from_u128(26);
        let merged_id = Uuid::from_u128(27);
        let full = crate::KeyRange::full();
        let (left, right) = full.children().unwrap();

        let left_lineage = crate::RangeLineage {
            range_id: left_id,
            generation: 1,
            key_range: left,
            parents: vec![crate::ParentRange {
                range_id: parent_id,
                generation: 0,
                key_range: full,
            }],
        };
        left_lineage.validate().unwrap();

        let merged = crate::RangeLineage {
            range_id: merged_id,
            generation: 2,
            key_range: full,
            parents: vec![
                crate::ParentRange {
                    range_id: left_id,
                    generation: 1,
                    key_range: left,
                },
                crate::ParentRange {
                    range_id: right_id,
                    generation: 1,
                    key_range: right,
                },
            ],
        };
        merged.validate().unwrap();

        let mut unrelated = left_lineage;
        unrelated.key_range = right.children().unwrap().0;
        assert!(matches!(
            unrelated.validate(),
            Err(LogError::InvalidDescriptor(_))
        ));
    }

    #[test]
    fn buddy_key_ranges_split_cover_and_rejoin_the_parent_space() {
        let full = crate::KeyRange::full();
        let (low, high) = full.children().unwrap();
        assert_eq!(low, crate::KeyRange::new(0, 1).unwrap());
        assert_eq!(high, crate::KeyRange::new(1_u64 << 63, 1).unwrap());
        assert_eq!(low.buddy().unwrap(), high);
        assert_eq!(high.buddy().unwrap(), low);
        assert!(low.contains(0));
        assert!(low.contains((1_u64 << 63) - 1));
        assert!(!low.contains(1_u64 << 63));
        assert!(high.contains(u64::MAX));
        assert!(crate::KeyRange::new(1, 1).is_err());
        assert!(!crate::KeyRange {
            prefix: 0,
            prefix_bits: 65,
        }
        .contains(0));
    }

    #[test]
    fn group_limit_accounts_for_encoded_record_overhead() {
        let mut too_small = config();
        too_small.max_record_bytes = 1024;
        too_small.max_group_bytes = 1024;
        assert!(matches!(
            too_small.validate(),
            Err(LogError::InvalidConfig(_))
        ));

        too_small.max_group_bytes =
            u64::from(too_small.max_record_bytes) + crate::types::RECORD_FRAME_OVERHEAD_BYTES;
        too_small.validate().unwrap();
    }

    #[test]
    fn v1_commit_boundary_matches_golden_vector() {
        let encoded = encode_commit_boundary(CommitBoundary {
            segment_id: Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
            committed_offset: 43,
            content_bytes: 97,
        });
        assert_eq!(
            hex(&encoded),
            concat!(
                "56544f50434d5431000100112233445566778899aabbccddeeff000000000000002b0000000000000061",
                "ffcea3ae94b0a0d03671c2444bedf54ac68b306f03a4caeb34789ae36b6a827f"
            )
        );
    }

    #[test]
    fn v1_manifest_matches_canonical_golden_json() {
        let manifest = SegmentManifest {
            format: FORMAT_NAME.to_owned(),
            version: FORMAT_VERSION,
            descriptor: descriptor(),
            record_count: 2,
            first_offset: Some(40),
            next_offset: 42,
            content_bytes: 200,
            blake3_root: "00".repeat(32),
            index_stride: 2,
        };
        let encoded = canonical_manifest_bytes(&manifest).unwrap();
        assert_eq!(
            String::from_utf8(encoded).unwrap(),
            concat!(
                "{\"format\":\"vtop-native-segment\",\"version\":1,\"descriptor\":{",
                "\"segment_id\":\"00000000-0000-0000-0000-000000000001\",",
                "\"topic\":\"events.v1\",\"topic_epoch\":7,\"lineage\":{",
                "\"range_id\":\"00000000-0000-0000-0000-000000000002\",",
                "\"generation\":0,\"key_range\":{\"prefix\":0,\"prefix_bits\":0}},",
                "\"base_offset\":40},\"record_count\":2,\"first_offset\":40,",
                "\"next_offset\":42,\"content_bytes\":200,",
                "\"blake3_root\":\"0000000000000000000000000000000000000000000000000000000000000000\",",
                "\"index_stride\":2}\n"
            )
        );
    }

    #[test]
    fn v1_commit_boundary_rejects_magic_version_checksum_and_trailing_bytes() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("marker.commit");
        let golden = encode_commit_boundary(CommitBoundary {
            segment_id: Uuid::from_u128(1),
            committed_offset: 41,
            content_bytes: 100,
        });

        for mutation in 0..4 {
            let mut bytes = golden.clone();
            match mutation {
                0 => bytes[0] ^= 0xff,
                1 => bytes[8..10].copy_from_slice(&(COMMIT_VERSION + 1).to_be_bytes()),
                2 => *bytes.last_mut().unwrap() ^= 0xff,
                3 => bytes.push(0),
                _ => unreachable!(),
            }
            fs::write(&path, bytes).unwrap();
            assert!(matches!(
                read_commit_boundary(real_storage().as_ref(), &path),
                Err(LogError::CommitBoundaryMismatch(_))
            ));
        }
    }

    #[test]
    fn v1_manifest_rejects_trailing_bytes() {
        let directory = tempdir().unwrap();
        let active_path = directory.path().join("trailing-manifest.active");
        let mut active = ActiveSegment::create(&active_path, descriptor(), config()).unwrap();
        active
            .append(
                record(Uuid::from_u128(29), 0, b"manifest"),
                Durability::Fsync,
            )
            .unwrap();
        let sealed = active.seal().unwrap();
        let segment_path = sealed.path.clone();
        drop(sealed);
        let paths = SegmentPaths::from_segment(&segment_path).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&paths.manifest)
            .unwrap()
            .write_all(b"\n")
            .unwrap();

        assert!(matches!(
            SegmentReader::open(&segment_path),
            Err(LogError::ManifestMismatch(_))
        ));
    }

    fn hex(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
        encoded
    }

    fn descriptor_v2() -> SegmentDescriptorV2 {
        SegmentDescriptorV2 {
            segment_id: Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
            topic: "audit.v1".to_owned(),
            topic_epoch: 3,
            lineage: crate::RangeLineage::root(
                Uuid::parse_str("ffeeddcc-bbaa-9988-7766-554433221100").unwrap(),
            ),
            base_offset: 42,
            segment_generation: 7,
            creation_node_id: Uuid::parse_str("12345678-9abc-def0-1234-56789abcdef0").unwrap(),
            creation_fencing_epoch: 5,
        }
    }

    fn config_v2() -> SegmentConfigV2 {
        SegmentConfigV2 {
            max_record_bytes: 1024,
            max_group_bytes: 4096,
            max_segment_bytes: 16 * 1024,
            max_segment_records: 100,
            index_stride: 2,
            chunk_size: 64 * 1024,
        }
    }

    fn record_v2(producer: Uuid, epoch: u64, sequence: u64, value: &[u8]) -> LogRecord {
        LogRecord {
            producer_id: producer,
            producer_epoch: epoch,
            sequence,
            timestamp_millis: 1_700_000_000_000 + sequence as i64,
            attributes: 0,
            key: b"key".to_vec(),
            value: value.to_vec(),
        }
    }

    fn commit_key() -> SegmentCommitKey {
        let key_hex: String = (0..32).map(|byte| format!("{byte:02x}")).collect();
        SegmentCommitKey::from_hex(&key_hex).unwrap()
    }

    /// The fixed workload behind the v2 sealed-bundle golden vectors.
    fn golden_v2_bundle(directory: &Path) -> PathBuf {
        let active = directory.join("golden.active");
        let producer = Uuid::parse_str("01020304-0506-0708-090a-0b0c0d0e0f10").unwrap();
        let mut segment = ActiveSegment::create_v2(&active, descriptor_v2(), config_v2()).unwrap();
        for sequence in 0..3_u64 {
            segment
                .append(
                    LogRecord {
                        producer_id: producer,
                        producer_epoch: 2,
                        sequence,
                        timestamp_millis: 1_700_000_000_000 + sequence as i64,
                        attributes: 0,
                        key: format!("key-{sequence}").into_bytes(),
                        value: format!("value-{sequence}").into_bytes(),
                    },
                    Durability::Fsync,
                )
                .unwrap();
        }
        drop(segment.seal_v2(Some(&commit_key())).unwrap());
        directory.join("golden.segment")
    }

    #[test]
    fn v2_sealed_bundle_matches_golden_manifest_sidecar_and_statement_mac() {
        let directory = tempdir().unwrap();
        let sealed = golden_v2_bundle(directory.path());

        let manifest_json =
            String::from_utf8(fs::read(directory.path().join("golden.manifest.json")).unwrap())
                .unwrap();
        assert_eq!(
            manifest_json,
            concat!(
                "{\"format\":\"vtop-native-segment\",\"version\":2,\"record_schema_version\":2,",
                "\"descriptor\":{\"segment_id\":\"00112233-4455-6677-8899-aabbccddeeff\",",
                "\"topic\":\"audit.v1\",\"topic_epoch\":3,\"lineage\":{",
                "\"range_id\":\"ffeeddcc-bbaa-9988-7766-554433221100\",\"generation\":0,",
                "\"key_range\":{\"prefix\":0,\"prefix_bits\":0}},\"base_offset\":42,",
                "\"segment_generation\":7,",
                "\"creation_node_id\":\"12345678-9abc-def0-1234-56789abcdef0\",",
                "\"creation_fencing_epoch\":5},\"record_count\":3,\"first_offset\":42,",
                "\"next_offset\":45,\"content_bytes\":342,\"committed_high_watermark\":45,",
                "\"producer_summary\":[{\"producer_id\":\"01020304-0506-0708-090a-0b0c0d0e0f10\",",
                "\"producer_epoch\":2,\"first_sequence\":0,\"last_sequence\":2,\"record_count\":3}],",
                "\"chunk_size\":65536,\"chunk_count\":1,\"chunk_tree_scheme\":\"vtop-b3tree-v1\",",
                "\"chunk_tree_root\":",
                "\"784d2e353d3670f37821cd064ca122800b36707b8c0ba5c638a03b84d29007d8\",",
                "\"index_stride\":2,\"sealing_node_id\":\"12345678-9abc-def0-1234-56789abcdef0\",",
                "\"sealing_fencing_epoch\":5,\"evidence\":{\"replicas\":[],\"tiers\":[]},",
                "\"commit_statement\":{\"statement_version\":1,\"scheme\":\"blake3-keyed\",",
                "\"key_id\":\"\",\"segment_id\":\"00112233-4455-6677-8899-aabbccddeeff\",",
                "\"segment_generation\":7,\"topic\":\"audit.v1\",\"topic_epoch\":3,",
                "\"range_id\":\"ffeeddcc-bbaa-9988-7766-554433221100\",\"range_generation\":0,",
                "\"base_offset\":42,\"committed_high_watermark\":45,\"content_bytes\":342,",
                "\"chunk_tree_root\":",
                "\"784d2e353d3670f37821cd064ca122800b36707b8c0ba5c638a03b84d29007d8\",",
                "\"manifest_core_digest\":",
                "\"747d3e48fc3f74eb209a291231e92065bc4dfa2a9d4c1bfef73bd88bc001929b\",",
                "\"mac\":\"1bb2f9c894238bde46c3f9ea2b12531c0e04004ed5f09a8f36336ce0c953d59c\"}}\n"
            )
        );
        // One 342-byte chunk: the sidecar carries a single leaf that is also
        // the chunk-tree root pinned in the manifest.
        assert_eq!(
            hex(&fs::read(directory.path().join("golden.chunks")).unwrap()),
            concat!(
                "56544f5043484b310001000100000000000000000001",
                "784d2e353d3670f37821cd064ca122800b36707b8c0ba5c638a03b84d29007d8",
                "2bbd5785ec3faf653d5a957c241130d89edc210c540e4e8ce44749d7ce739443"
            )
        );
        let manifest: SegmentManifestV2 = serde_json::from_str(&manifest_json).unwrap();
        let statement = manifest.commit_statement.unwrap();
        assert_eq!(
            statement.mac,
            "1bb2f9c894238bde46c3f9ea2b12531c0e04004ed5f09a8f36336ce0c953d59c"
        );
        statement.verify(Some(&commit_key())).unwrap();
        drop(SegmentReader::open(&sealed).unwrap());
    }

    #[test]
    fn v2_round_trip_appends_recovers_seals_and_fetches_byte_identically() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("round-trip.active");
        let first = Uuid::from_u128(31);
        let second = Uuid::from_u128(32);
        let workload = [
            (record_v2(first, 0, 0, b"alpha"), Durability::Fsync),
            (record_v2(second, 1, 0, b"beta"), Durability::Buffered),
            (record_v2(first, 0, 1, b"gamma"), Durability::Fsync),
            (record_v2(second, 2, 0, b"delta"), Durability::Buffered),
            (record_v2(second, 2, 1, b"epsilon"), Durability::Fsync),
        ];
        let mut segment = ActiveSegment::create_v2(&path, descriptor_v2(), config_v2()).unwrap();
        assert_eq!(segment.format_version(), crate::FORMAT_VERSION_V2);
        assert_eq!(segment.descriptor_v2().unwrap(), &descriptor_v2());
        assert_eq!(segment.config_v2().unwrap(), config_v2());
        for (index, (record, durability)) in workload.iter().enumerate() {
            assert_eq!(
                segment.append(record.clone(), *durability).unwrap(),
                AppendOutcome::Appended {
                    offset: 42 + index as u64
                }
            );
        }
        drop(segment);

        let mut recovered = ActiveSegment::recover(&path).unwrap();
        assert_eq!(recovered.next_offset(), 47);
        assert_eq!(recovered.committed_offset(), 47);
        // Recovery rebuilds per-(producer, epoch) state: a fresh sequence
        // under the fenced epoch is rejected while the duplicate retry of an
        // already persisted old-epoch record still answers its offset.
        assert!(matches!(
            recovered.append(record_v2(second, 1, 1, b"stale"), Durability::Buffered),
            Err(LogError::ProducerFenced {
                latest_epoch: 2,
                actual_epoch: 1,
                ..
            })
        ));
        assert_eq!(
            recovered
                .append(record_v2(second, 1, 0, b"beta"), Durability::Buffered)
                .unwrap(),
            AppendOutcome::Duplicate { offset: 43 }
        );

        let mut sealed = recovered.seal_v2(None).unwrap();
        assert_eq!(sealed.format_version(), crate::FORMAT_VERSION_V2);
        let manifest = sealed.manifest_v2().unwrap().clone();
        assert_eq!(manifest.record_count, 5);
        assert_eq!(manifest.first_offset, Some(42));
        assert_eq!(manifest.next_offset, 47);
        assert_eq!(manifest.committed_high_watermark, 47);
        assert_eq!(manifest.chunk_count, 1);
        assert_eq!(manifest.chunk_size, 64 * 1024);
        assert_eq!(manifest.chunk_tree_scheme, crate::CHUNK_TREE_SCHEME_V1);
        assert!(manifest.commit_statement.is_none());
        let mut summaries = manifest.producer_summary.clone();
        summaries.sort_by_key(|entry| (entry.producer_id, entry.producer_epoch));
        assert_eq!(summaries, manifest.producer_summary);
        assert_eq!(
            manifest
                .producer_summary
                .iter()
                .map(|entry| (
                    entry.producer_epoch,
                    entry.first_sequence,
                    entry.last_sequence
                ))
                .collect::<Vec<_>>(),
            vec![(0, 0, 1), (1, 0, 0), (2, 0, 1)]
        );

        let fetched = sealed.fetch(42, usize::MAX, usize::MAX).unwrap();
        assert_eq!(fetched.records.len(), workload.len());
        for (index, fetched_record) in fetched.records.iter().enumerate() {
            assert_eq!(fetched_record.offset, 42 + index as u64);
            assert_eq!(fetched_record.record, workload[index].0);
        }
        let cursor = sealed.cursor(44).unwrap();
        assert_eq!(cursor.segment_root, manifest.chunk_tree_root);
        assert_eq!(cursor.topic_epoch, 3);

        // The reopened reader observes byte-identical records and metadata.
        let segment_path = directory.path().join("round-trip.segment");
        let mut reopened = SegmentReader::open(&segment_path).unwrap();
        assert_eq!(reopened.manifest_v2().unwrap(), &manifest);
        assert_eq!(reopened.descriptor_v2().unwrap(), &descriptor_v2());
        let refetched = reopened.fetch(42, usize::MAX, usize::MAX).unwrap();
        assert_eq!(refetched.records, fetched.records);
    }

    #[test]
    fn v2_producer_epoch_rules_enforce_fencing_scoping_and_cross_epoch_duplicates() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("epochs.active");
        let producer = Uuid::from_u128(33);
        let mut segment = ActiveSegment::create_v2(&path, descriptor_v2(), config_v2()).unwrap();

        assert_eq!(
            segment
                .append(record_v2(producer, 0, 0, b"x"), Durability::Fsync)
                .unwrap(),
            AppendOutcome::Appended { offset: 42 }
        );
        // Each (producer, epoch) starts at sequence zero.
        assert!(matches!(
            segment.append(record_v2(producer, 2, 3, b"late"), Durability::Buffered),
            Err(LogError::FirstSequence { actual: 3, .. })
        ));
        assert_eq!(
            segment
                .append(record_v2(producer, 1, 0, b"y"), Durability::Fsync)
                .unwrap(),
            AppendOutcome::Appended { offset: 43 }
        );
        assert!(matches!(
            segment.append(record_v2(producer, 1, 5, b"gap"), Durability::Buffered),
            Err(LogError::SequenceGap {
                expected: 1,
                actual: 5,
                ..
            })
        ));
        // A duplicate retry across epochs stays a duplicate; a conflicting
        // retry stays a conflict; only a fresh old-epoch sequence is fenced.
        assert_eq!(
            segment
                .append(record_v2(producer, 0, 0, b"x"), Durability::Buffered)
                .unwrap(),
            AppendOutcome::Duplicate { offset: 42 }
        );
        assert!(matches!(
            segment.append(record_v2(producer, 0, 0, b"z"), Durability::Buffered),
            Err(LogError::SequenceConflict { sequence: 0, .. })
        ));
        assert!(matches!(
            segment.append(record_v2(producer, 0, 1, b"fresh"), Durability::Buffered),
            Err(LogError::ProducerFenced {
                latest_epoch: 1,
                actual_epoch: 0,
                ..
            })
        ));

        // Schema v2 still reserves every attribute bit.
        let mut flagged = record_v2(producer, 1, 1, b"flagged");
        flagged.attributes = 1;
        assert!(matches!(
            segment.append(flagged, Durability::Buffered),
            Err(LogError::UnsupportedRecordField("attributes"))
        ));

        // A group is fully validated before any byte is written: an in-group
        // epoch regression rejects the whole group.
        let length = fs::metadata(&path).unwrap().len();
        assert!(matches!(
            segment.append_group(
                &[
                    record_v2(producer, 3, 0, b"newer"),
                    record_v2(producer, 2, 0, b"older"),
                ],
                Durability::Fsync,
            ),
            Err(LogError::ProducerFenced {
                latest_epoch: 3,
                actual_epoch: 2,
                ..
            })
        ));
        assert_eq!(fs::metadata(&path).unwrap().len(), length);
        assert_eq!(segment.next_offset(), 44);
    }

    #[test]
    fn v1_segments_reject_seal_v2_and_version_accessors_disagree_by_format() {
        let directory = tempdir().unwrap();
        let v1 = ActiveSegment::create(
            directory.path().join("v1-only.active"),
            descriptor(),
            config(),
        )
        .unwrap();
        assert_eq!(v1.format_version(), FORMAT_VERSION);
        assert!(v1.descriptor_v2().is_none());
        assert!(v1.config_v2().is_none());
        assert!(matches!(
            v1.seal_v2(Some(&commit_key())),
            Err(LogError::InvalidDescriptor(_))
        ));

        // seal() on a v2 segment publishes a v2 manifest without a statement.
        let v2 = ActiveSegment::create_v2(
            directory.path().join("v2-only.active"),
            descriptor_v2(),
            config_v2(),
        )
        .unwrap();
        let sealed = v2.seal().unwrap();
        let manifest = sealed.manifest_v2().unwrap();
        assert_eq!(manifest.version, crate::FORMAT_VERSION_V2);
        assert_eq!(manifest.record_count, 0);
        assert_eq!(manifest.chunk_count, 0);
        assert!(manifest.commit_statement.is_none());
    }

    #[test]
    fn v2_corruption_in_each_region_is_detected_and_never_silently_accepted() {
        let producer = Uuid::from_u128(34);
        let key = commit_key();
        let build = |directory: &Path| -> PathBuf {
            let active = directory.join("guarded.active");
            let mut segment =
                ActiveSegment::create_v2(&active, descriptor_v2(), config_v2()).unwrap();
            segment
                .append_group(
                    &[
                        record_v2(producer, 4, 0, b"guarded-a"),
                        record_v2(producer, 4, 1, b"guarded-b"),
                    ],
                    Durability::Fsync,
                )
                .unwrap();
            drop(segment.seal_v2(Some(&key)).unwrap());
            directory.join("guarded.segment")
        };
        let header_len =
            crate::codec_v2::encode_header_v2(&SegmentHeaderV2::new(descriptor_v2(), config_v2()))
                .unwrap()
                .len() as u64;

        let flip = |path: &Path, position: u64| {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            file.seek(SeekFrom::Start(position)).unwrap();
            let mut byte = [0_u8; 1];
            std::io::Read::read_exact(&mut file, &mut byte).unwrap();
            file.seek(SeekFrom::Start(position)).unwrap();
            file.write_all(&[byte[0] ^ 0xff]).unwrap();
            file.sync_all().unwrap();
        };

        // Header, record frame, and producer-epoch field mutations are all
        // corruption; the epoch bytes sit at frame offset 12 + 8 + 16.
        for position in [0, header_len + 20, header_len + 12 + 8 + 16] {
            let directory = tempdir().unwrap();
            let sealed = build(directory.path());
            flip(&sealed, position);
            assert!(
                matches!(SegmentReader::open(&sealed), Err(LogError::Corrupt { .. })),
                "byte {position} was accepted"
            );
        }

        // The chunk sidecar is a rebuildable cache: leaf corruption and
        // deletion are detected and repaired to canonical bytes on open.
        let directory = tempdir().unwrap();
        let sealed = build(directory.path());
        let chunks = directory.path().join("guarded.chunks");
        let pristine = fs::read(&chunks).unwrap();
        flip(&chunks, (pristine.len() - 40) as u64);
        drop(SegmentReader::open(&sealed).unwrap());
        assert_eq!(fs::read(&chunks).unwrap(), pristine);
        fs::remove_file(&chunks).unwrap();
        drop(SegmentReader::open(&sealed).unwrap());
        assert_eq!(fs::read(&chunks).unwrap(), pristine);
        rebuild_chunk_index(&sealed).unwrap();
        assert_eq!(fs::read(&chunks).unwrap(), pristine);

        // A manifest whose chunk-tree root no longer matches the bytes fails
        // even when the encoding stays canonical.
        let manifest_path = directory.path().join("guarded.manifest.json");
        let manifest_bytes = fs::read(&manifest_path).unwrap();
        let mut manifest: SegmentManifestV2 = serde_json::from_slice(&manifest_bytes).unwrap();
        let original = manifest.clone();
        manifest.chunk_tree_root = "ab".repeat(32);
        fs::write(
            &manifest_path,
            canonical_manifest_v2_bytes(&manifest).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            SegmentReader::open(&sealed),
            Err(LogError::ManifestMismatch(_))
        ));

        // A commit statement that no longer restates the manifest core fails
        // at open; a forged MAC survives structural checks but fails keyed
        // verification.
        let mut restated = original.clone();
        restated
            .commit_statement
            .as_mut()
            .unwrap()
            .committed_high_watermark += 1;
        fs::write(
            &manifest_path,
            canonical_manifest_v2_bytes(&restated).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            SegmentReader::open(&sealed),
            Err(LogError::ManifestMismatch(_))
        ));

        let mut forged = original.clone();
        forged.commit_statement.as_mut().unwrap().mac = "00".repeat(32);
        fs::write(
            &manifest_path,
            canonical_manifest_v2_bytes(&forged).unwrap(),
        )
        .unwrap();
        let reader = SegmentReader::open(&sealed).unwrap();
        let statement = reader
            .manifest_v2()
            .unwrap()
            .commit_statement
            .clone()
            .unwrap();
        assert!(matches!(
            statement.verify(Some(&key)),
            Err(LogError::ManifestMismatch(_))
        ));

        // The untampered statement verifies with the sealing key and rejects
        // the wrong key.
        fs::write(
            &manifest_path,
            canonical_manifest_v2_bytes(&original).unwrap(),
        )
        .unwrap();
        let reader = SegmentReader::open(&sealed).unwrap();
        let statement = reader
            .manifest_v2()
            .unwrap()
            .commit_statement
            .clone()
            .unwrap();
        statement.verify(Some(&key)).unwrap();
        let wrong = SegmentCommitKey::from_hex(&"11".repeat(32)).unwrap();
        assert!(statement.verify(Some(&wrong)).is_err());
    }

    #[test]
    fn v2_chunk_tree_crosses_boundaries_and_root_is_stable_across_recover_and_reopen() {
        let config = SegmentConfigV2 {
            max_record_bytes: 64 * 1024,
            max_group_bytes: 128 * 1024,
            max_segment_bytes: 1024 * 1024,
            max_segment_records: 100,
            index_stride: 2,
            chunk_size: 64 * 1024,
        };
        let producer = Uuid::from_u128(35);
        let values: Vec<Vec<u8>> = (0..5_u8).map(|index| vec![index; 40 * 1024]).collect();
        let workload = |path: &Path| {
            let mut segment = ActiveSegment::create_v2(path, descriptor_v2(), config).unwrap();
            for (sequence, value) in values.iter().enumerate() {
                segment
                    .append(
                        record_v2(producer, 1, sequence as u64, value),
                        Durability::Fsync,
                    )
                    .unwrap();
            }
            segment
        };

        let direct_directory = tempdir().unwrap();
        let direct_path = direct_directory.path().join("direct.active");
        let direct = workload(&direct_path).seal_v2(None).unwrap();
        let direct_manifest = direct.manifest_v2().unwrap().clone();
        // Five ~40 KiB frames span at least three 64 KiB chunks, so the root
        // commits across multiple chunk boundaries.
        assert!(
            direct_manifest.chunk_count >= 3,
            "{}",
            direct_manifest.chunk_count
        );

        let recovered_directory = tempdir().unwrap();
        let recovered_path = recovered_directory.path().join("recovered.active");
        drop(workload(&recovered_path));
        let recovered = ActiveSegment::recover(&recovered_path)
            .unwrap()
            .seal_v2(None)
            .unwrap();
        let recovered_manifest = recovered.manifest_v2().unwrap();
        assert_eq!(
            recovered_manifest.chunk_tree_root,
            direct_manifest.chunk_tree_root
        );
        assert_eq!(recovered_manifest.chunk_count, direct_manifest.chunk_count);

        let mut reopened =
            SegmentReader::open(recovered_directory.path().join("recovered.segment")).unwrap();
        assert_eq!(
            reopened.manifest_v2().unwrap().chunk_tree_root,
            direct_manifest.chunk_tree_root
        );
        let fetched = reopened.fetch(42, usize::MAX, usize::MAX).unwrap();
        assert_eq!(fetched.records.len(), values.len());
        for (index, fetched_record) in fetched.records.iter().enumerate() {
            assert_eq!(fetched_record.record.value, values[index]);
        }
    }
}

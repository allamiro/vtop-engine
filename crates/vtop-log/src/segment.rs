use crate::codec::{
    encode_header, encode_record, read_frame, read_header, record_content_hash, FrameRead,
    SegmentHeader, INDEX_MAGIC,
};
use crate::types::{
    AppendOutcome, Durability, FetchBatch, FetchedRecord, LogError, LogRecord, RecoveryReport,
    SegmentConfig, SegmentDescriptor, SegmentManifest, VtopLogResult, FORMAT_NAME, FORMAT_VERSION,
};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

const COMMIT_MAGIC: &[u8; 8] = b"VTOPCMT1";
const COMMIT_VERSION: u16 = 1;
const COMMIT_BOUNDARY_LEN: usize = 8 + 2 + 16 + 8 + 8 + 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CommitBoundary {
    segment_id: Uuid,
    committed_offset: u64,
    content_bytes: u64,
}

#[derive(Clone)]
struct ProducerState {
    latest_sequence: u64,
    seen: HashMap<u64, SeenRecord>,
}

struct ProducerDelta {
    latest_sequence: u64,
    seen: HashMap<u64, SeenRecord>,
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

struct ScanResult {
    records: u64,
    valid_end: u64,
    truncated_bytes: u64,
    next_offset: u64,
    producer_states: HashMap<Uuid, ProducerState>,
    index: Vec<IndexEntry>,
    content_hasher: blake3::Hasher,
}

pub struct ActiveSegment {
    path: PathBuf,
    file: File,
    header: SegmentHeader,
    header_len: u64,
    next_offset: u64,
    committed_offset: u64,
    record_count: u64,
    content_bytes: u64,
    producer_states: HashMap<Uuid, ProducerState>,
    index: Vec<IndexEntry>,
    content_hasher: blake3::Hasher,
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
        descriptor.validate()?;
        let config = config.validate()?;
        let path = path.as_ref().to_path_buf();
        let paths = SegmentPaths::from_active(&path)?;
        let header = SegmentHeader::new(descriptor, config);
        let encoded_header = encode_header(&header)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|source| io_error(&path, source))?;
        if let Err(source) = file
            .write_all(&encoded_header)
            .and_then(|()| file.sync_data())
        {
            return Err(io_error(&path, source));
        }
        sync_parent(&path)?;
        let header_len = encoded_header.len() as u64;
        let base_offset = header.descriptor.base_offset;
        write_commit_boundary_atomic(
            &paths.commit,
            CommitBoundary {
                segment_id: header.descriptor.segment_id,
                committed_offset: base_offset,
                content_bytes: 0,
            },
        )?;
        Ok(Self {
            path,
            file,
            header,
            header_len,
            next_offset: base_offset,
            committed_offset: base_offset,
            record_count: 0,
            content_bytes: 0,
            producer_states: HashMap::new(),
            index: Vec::new(),
            content_hasher: blake3::Hasher::new(),
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
        let path = path.as_ref().to_path_buf();
        let paths = SegmentPaths::from_active(&path)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| io_error(&path, source))?;
        let (header, header_len) = read_header_with_path(&mut file, &path)?;
        let boundary = match read_commit_boundary(&paths.commit) {
            Ok(boundary) => boundary,
            Err(LogError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
                // Pre-boundary active files are recovered conservatively: no
                // record is promoted merely because its complete bytes happen
                // to have survived a restart.
                let boundary = CommitBoundary {
                    segment_id: header.descriptor.segment_id,
                    committed_offset: header.descriptor.base_offset,
                    content_bytes: 0,
                };
                write_commit_boundary_atomic(&paths.commit, boundary)?;
                boundary
            }
            Err(error) => return Err(error),
        };
        if boundary.segment_id != header.descriptor.segment_id {
            return Err(LogError::CommitBoundaryMismatch(
                "segment id differs from the active segment header".to_owned(),
            ));
        }
        let committed_end = header_len
            .checked_add(boundary.content_bytes)
            .ok_or_else(|| {
                LogError::CommitBoundaryMismatch("committed byte length overflows".to_owned())
            })?;
        let actual_len = file
            .metadata()
            .map_err(|source| io_error(&path, source))?
            .len();
        if committed_end > actual_len {
            return Err(LogError::CommitBoundaryMismatch(format!(
                "committed byte end {committed_end} exceeds file length {actual_len}"
            )));
        }
        let mut scan = scan_records(
            &mut file,
            &path,
            &header,
            header_len,
            Some(committed_end),
            false,
        )?;
        if scan.next_offset != boundary.committed_offset
            || scan.valid_end != committed_end
            || scan.valid_end - header_len != boundary.content_bytes
        {
            return Err(LogError::CommitBoundaryMismatch(
                "offset or byte boundary does not end on the validated record frontier".to_owned(),
            ));
        }
        scan.truncated_bytes = actual_len.saturating_sub(scan.valid_end);
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
            path,
            file,
            header,
            header_len,
            next_offset: scan.next_offset,
            committed_offset: scan.next_offset,
            record_count: scan.records,
            content_bytes: scan.valid_end - header_len,
            producer_states: scan.producer_states,
            index: scan.index,
            content_hasher: scan.content_hasher,
            poisoned: false,
            sealed: false,
            recovery: report,
        })
    }

    pub fn descriptor(&self) -> &SegmentDescriptor {
        &self.header.descriptor
    }

    pub fn config(&self) -> SegmentConfig {
        self.header.config
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
        let mut producer_deltas = HashMap::new();
        let mut prospective_next = self.next_offset;
        let mut outcomes = Vec::with_capacity(records.len());
        let mut pending = Vec::new();
        let mut group_bytes = 0_u64;

        for record in records {
            let hash = record_content_hash(record);
            match validate_sequence_with_delta(
                &self.producer_states,
                &producer_deltas,
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
                    let relative_offset = offset - self.header.descriptor.base_offset;
                    let encoded = encode_record(
                        record,
                        relative_offset,
                        self.header.config.max_record_bytes,
                    )?;
                    group_bytes = group_bytes.checked_add(encoded.len() as u64).ok_or(
                        LogError::GroupTooLarge {
                            actual: u64::MAX,
                            maximum: self.header.config.max_group_bytes,
                        },
                    )?;
                    if group_bytes > self.header.config.max_group_bytes {
                        return Err(LogError::GroupTooLarge {
                            actual: group_bytes,
                            maximum: self.header.config.max_group_bytes,
                        });
                    }
                    remember_pending_sequence(&mut producer_deltas, record, offset, hash);
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
                    maximum: self.header.config.max_segment_bytes,
                })?;
        if attempted_bytes > self.header.config.max_segment_bytes {
            return Err(LogError::SegmentByteLimit {
                current: self.content_bytes,
                attempted: attempted_bytes,
                maximum: self.header.config.max_segment_bytes,
            });
        }
        let attempted_records = self.record_count.checked_add(pending.len() as u64).ok_or(
            LogError::SegmentRecordLimit {
                attempted: u64::MAX,
                maximum: self.header.config.max_segment_records,
            },
        )?;
        if attempted_records > self.header.config.max_segment_records {
            return Err(LogError::SegmentRecordLimit {
                attempted: attempted_records,
                maximum: self.header.config.max_segment_records,
            });
        }
        let write_start = self
            .file
            .seek(SeekFrom::End(0))
            .map_err(|source| io_error(&self.path, source))?;
        let mut position = write_start;
        for (offset, encoded) in &pending {
            if (*offset - self.header.descriptor.base_offset)
                .is_multiple_of(u64::from(self.header.config.index_stride))
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
            self.content_hasher.update(encoded);
        }
        merge_producer_deltas(&mut self.producer_states, producer_deltas);
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
        let result = fetch_from_file(
            &mut self.file,
            &self.path,
            &self.header,
            self.header_len,
            &self.index,
            self.committed_offset,
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
    pub fn seal(mut self) -> VtopLogResult<SegmentReader> {
        self.ensure_writable()?;
        // A sealed reader exposes its complete contents. Advance a durable
        // commit boundary first so sealing can never publish buffered records
        // that were not committed on this node.
        self.commit()?;
        let actual_file_bytes = self
            .file
            .metadata()
            .map_err(|source| io_error(&self.path, source))?
            .len();
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
        let manifest = SegmentManifest {
            format: FORMAT_NAME.to_owned(),
            version: FORMAT_VERSION,
            descriptor: self.header.descriptor.clone(),
            record_count: self.record_count,
            first_offset: (self.record_count > 0).then_some(self.header.descriptor.base_offset),
            next_offset: self.next_offset,
            content_bytes: self.content_bytes,
            blake3_root: self.content_hasher.finalize().to_hex().to_string(),
            index_stride: self.header.config.index_stride,
        };
        let paths = SegmentPaths::from_active(&self.path)?;
        if paths.segment.exists() {
            return Err(LogError::InvalidDescriptor(format!(
                "refusing to replace existing sealed segment {}",
                paths.segment.display()
            )));
        }
        write_index_atomic(&paths.index, &self.index)?;
        write_manifest_atomic(&paths.manifest, &manifest)?;
        fs::rename(&self.path, &paths.segment).map_err(|source| io_error(&self.path, source))?;
        sync_parent(&paths.segment)?;
        self.sealed = true;
        SegmentReader::open(paths.segment)
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
            &paths.commit,
            CommitBoundary {
                segment_id: self.header.descriptor.segment_id,
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
    file: File,
    header: SegmentHeader,
    header_len: u64,
    manifest: SegmentManifest,
    index: Vec<IndexEntry>,
}

impl SegmentReader {
    pub fn open(path: impl AsRef<Path>) -> VtopLogResult<Self> {
        let path = path.as_ref().to_path_buf();
        let paths = SegmentPaths::from_segment(&path)?;
        let mut file = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|source| io_error(&path, source))?;
        let (header, header_len) = read_header_with_path(&mut file, &path)?;
        let scan = scan_records(&mut file, &path, &header, header_len, None, false)?;
        let manifest_bytes =
            fs::read(&paths.manifest).map_err(|source| io_error(&paths.manifest, source))?;
        let manifest: SegmentManifest =
            serde_json::from_slice(&manifest_bytes).map_err(|error| {
                LogError::ManifestMismatch(format!("cannot decode manifest: {error}"))
            })?;
        if manifest_bytes != canonical_manifest_bytes(&manifest)? {
            return Err(LogError::ManifestMismatch(
                "manifest is not in canonical VTOP JSON encoding".to_owned(),
            ));
        }
        validate_manifest(&manifest, &header, &scan, header_len)?;

        let index = match read_index(&paths.index) {
            Ok(index) if index == scan.index => index,
            Ok(_) | Err(_) => {
                write_index_atomic(&paths.index, &scan.index)?;
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

    pub fn manifest(&self) -> &SegmentManifest {
        &self.manifest
    }

    pub fn cursor(&self, offset: u64) -> VtopLogResult<crate::SegmentCursor> {
        if !(self.header.descriptor.base_offset..=self.manifest.next_offset).contains(&offset) {
            return Err(LogError::InvalidCursor(format!(
                "offset {offset} is outside segment interval {}..={}",
                self.header.descriptor.base_offset, self.manifest.next_offset
            )));
        }
        Ok(crate::SegmentCursor {
            topic: self.header.descriptor.topic.clone(),
            topic_epoch: self.header.descriptor.topic_epoch,
            range_id: self.header.descriptor.lineage.range_id,
            range_generation: self.header.descriptor.lineage.generation,
            segment_id: self.header.descriptor.segment_id,
            segment_root: self.manifest.blake3_root.clone(),
            offset,
        })
    }

    pub fn fetch(
        &mut self,
        start_offset: u64,
        max_bytes: usize,
        max_records: usize,
    ) -> VtopLogResult<FetchBatch> {
        fetch_from_file(
            &mut self.file,
            &self.path,
            &self.header,
            self.header_len,
            &self.index,
            self.manifest.next_offset,
            start_offset,
            max_bytes,
            max_records,
        )
    }
}

pub fn rebuild_index(path: impl AsRef<Path>) -> VtopLogResult<()> {
    let path = path.as_ref();
    let paths = SegmentPaths::from_segment(path)?;
    let mut file = File::open(path).map_err(|source| io_error(path, source))?;
    let (header, header_len) = read_header_with_path(&mut file, path)?;
    let scan = scan_records(&mut file, path, &header, header_len, None, false)?;
    write_index_atomic(&paths.index, &scan.index)
}

enum SequenceDecision {
    Append,
    Duplicate(u64),
}

fn validate_sequence(
    states: &HashMap<Uuid, ProducerState>,
    record: &LogRecord,
    hash: blake3::Hash,
) -> VtopLogResult<SequenceDecision> {
    let Some(state) = states.get(&record.producer_id) else {
        return if record.sequence == 0 {
            Ok(SequenceDecision::Append)
        } else {
            Err(LogError::FirstSequence {
                producer_id: record.producer_id,
                actual: record.sequence,
            })
        };
    };
    if let Some(seen) = state.seen.get(&record.sequence) {
        return if hash == seen.content_hash {
            Ok(SequenceDecision::Duplicate(seen.offset))
        } else {
            Err(LogError::SequenceConflict {
                producer_id: record.producer_id,
                sequence: record.sequence,
            })
        };
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
    states: &HashMap<Uuid, ProducerState>,
    deltas: &HashMap<Uuid, ProducerDelta>,
    record: &LogRecord,
    hash: blake3::Hash,
) -> VtopLogResult<SequenceDecision> {
    if let Some(seen) = deltas
        .get(&record.producer_id)
        .and_then(|delta| delta.seen.get(&record.sequence))
        .or_else(|| {
            states
                .get(&record.producer_id)
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

    let latest = deltas
        .get(&record.producer_id)
        .map(|delta| delta.latest_sequence)
        .or_else(|| {
            states
                .get(&record.producer_id)
                .map(|state| state.latest_sequence)
        });
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
    deltas: &mut HashMap<Uuid, ProducerDelta>,
    record: &LogRecord,
    offset: u64,
    content_hash: blake3::Hash,
) {
    deltas
        .entry(record.producer_id)
        .and_modify(|delta| {
            delta.latest_sequence = record.sequence;
            delta.seen.insert(
                record.sequence,
                SeenRecord {
                    offset,
                    content_hash,
                },
            );
        })
        .or_insert_with(|| ProducerDelta {
            latest_sequence: record.sequence,
            seen: HashMap::from([(
                record.sequence,
                SeenRecord {
                    offset,
                    content_hash,
                },
            )]),
        });
}

fn merge_producer_deltas(
    states: &mut HashMap<Uuid, ProducerState>,
    deltas: HashMap<Uuid, ProducerDelta>,
) {
    for (producer_id, delta) in deltas {
        let ProducerDelta {
            latest_sequence,
            seen,
        } = delta;
        if let Some(state) = states.get_mut(&producer_id) {
            state.latest_sequence = latest_sequence;
            state.seen.extend(seen);
        } else {
            states.insert(
                producer_id,
                ProducerState {
                    latest_sequence,
                    seen,
                },
            );
        }
    }
}

fn scan_records(
    file: &mut File,
    path: &Path,
    header: &SegmentHeader,
    header_len: u64,
    logical_end: Option<u64>,
    permit_torn_tail: bool,
) -> VtopLogResult<ScanResult> {
    file.seek(SeekFrom::Start(header_len))
        .map_err(|source| io_error(path, source))?;
    let actual_file_len = file
        .metadata()
        .map_err(|source| io_error(path, source))?
        .len();
    let file_len = logical_end.unwrap_or(actual_file_len);
    if file_len < header_len || file_len > actual_file_len {
        return Err(LogError::CommitBoundaryMismatch(format!(
            "logical file end {file_len} is outside {header_len}..={actual_file_len}"
        )));
    }
    let mut position = header_len;
    let mut next_offset = header.descriptor.base_offset;
    let mut records = 0_u64;
    let mut producer_states = HashMap::new();
    let mut index = Vec::new();
    let mut content_hasher = blake3::Hasher::new();

    loop {
        if position == file_len {
            break;
        }
        if position > file_len {
            return Err(LogError::CommitBoundaryMismatch(
                "commit boundary splits a record frame".to_owned(),
            ));
        }
        match read_frame(file, position, header.config.max_record_bytes)
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
                if attempted_bytes > header.config.max_segment_bytes {
                    return Err(LogError::Corrupt {
                        position,
                        reason: format!(
                            "segment exceeds configured byte limit {}",
                            header.config.max_segment_bytes
                        ),
                    });
                }
                if records >= header.config.max_segment_records {
                    return Err(LogError::Corrupt {
                        position,
                        reason: format!(
                            "segment exceeds configured record limit {}",
                            header.config.max_segment_records
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
                match validate_sequence(&producer_states, &frame.record, hash).map_err(|error| {
                    LogError::Corrupt {
                        position,
                        reason: error.to_string(),
                    }
                })? {
                    SequenceDecision::Append => {}
                    SequenceDecision::Duplicate(_) => {
                        return Err(LogError::Corrupt {
                            position,
                            reason: "segment contains a duplicate producer sequence".to_owned(),
                        });
                    }
                }
                if records.is_multiple_of(u64::from(header.config.index_stride)) {
                    index.push(IndexEntry {
                        offset: next_offset,
                        position,
                    });
                }
                remember_sequence(&mut producer_states, &frame.record, next_offset, hash);
                content_hasher.update(&frame.encoded);
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
        index,
        content_hasher,
    })
}

#[allow(clippy::too_many_arguments)]
fn fetch_from_file(
    file: &mut File,
    path: &Path,
    header: &SegmentHeader,
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
            next_offset: start_offset
                .max(header.descriptor.base_offset)
                .min(high_watermark),
            high_watermark,
        });
    }
    let requested = start_offset.max(header.descriptor.base_offset);
    let entry = index
        .iter()
        .rev()
        .find(|entry| entry.offset <= requested)
        .copied()
        .unwrap_or(IndexEntry {
            offset: header.descriptor.base_offset,
            position: header_len,
        });
    file.seek(SeekFrom::Start(entry.position))
        .map_err(|source| io_error(path, source))?;
    let mut offset = entry.offset;
    let mut position = entry.position;
    let mut records = Vec::new();
    let mut encoded_bytes = 0_usize;

    while offset < high_watermark && records.len() < max_records {
        let frame = match read_frame(file, position, header.config.max_record_bytes)
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
        let expected_relative = offset - header.descriptor.base_offset;
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
    states: &mut HashMap<Uuid, ProducerState>,
    record: &LogRecord,
    offset: u64,
    content_hash: blake3::Hash,
) {
    states
        .entry(record.producer_id)
        .and_modify(|state| {
            state.latest_sequence = record.sequence;
            state.seen.insert(
                record.sequence,
                SeenRecord {
                    offset,
                    content_hash,
                },
            );
        })
        .or_insert_with(|| ProducerState {
            latest_sequence: record.sequence,
            seen: HashMap::from([(
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
    let root = scan.content_hasher.clone().finalize().to_hex().to_string();
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

fn write_manifest_atomic(path: &Path, manifest: &SegmentManifest) -> VtopLogResult<()> {
    let bytes = canonical_manifest_bytes(manifest)?;
    write_atomic(path, &bytes)
}

fn write_commit_boundary_atomic(path: &Path, boundary: CommitBoundary) -> VtopLogResult<()> {
    let mut bytes = Vec::with_capacity(COMMIT_BOUNDARY_LEN);
    bytes.extend_from_slice(COMMIT_MAGIC);
    bytes.extend_from_slice(&COMMIT_VERSION.to_be_bytes());
    bytes.extend_from_slice(boundary.segment_id.as_bytes());
    bytes.extend_from_slice(&boundary.committed_offset.to_be_bytes());
    bytes.extend_from_slice(&boundary.content_bytes.to_be_bytes());
    let checksum = blake3::hash(&bytes);
    bytes.extend_from_slice(checksum.as_bytes());
    debug_assert_eq!(bytes.len(), COMMIT_BOUNDARY_LEN);
    write_atomic(path, &bytes)
}

fn read_commit_boundary(path: &Path) -> VtopLogResult<CommitBoundary> {
    let bytes = fs::read(path).map_err(|source| io_error(path, source))?;
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

fn canonical_manifest_bytes(manifest: &SegmentManifest) -> VtopLogResult<Vec<u8>> {
    let mut bytes = serde_json::to_vec(manifest)
        .map_err(|error| LogError::ManifestMismatch(format!("cannot encode manifest: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn write_index_atomic(path: &Path, entries: &[IndexEntry]) -> VtopLogResult<()> {
    let mut bytes = Vec::with_capacity(16 + entries.len() * 16);
    bytes.extend_from_slice(INDEX_MAGIC);
    bytes.extend_from_slice(&(entries.len() as u64).to_be_bytes());
    for entry in entries {
        bytes.extend_from_slice(&entry.offset.to_be_bytes());
        bytes.extend_from_slice(&entry.position.to_be_bytes());
    }
    write_atomic(path, &bytes)
}

fn read_index(path: &Path) -> VtopLogResult<Vec<IndexEntry>> {
    let bytes = fs::read(path).map_err(|source| io_error(path, source))?;
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

fn write_atomic(path: &Path, bytes: &[u8]) -> VtopLogResult<()> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            LogError::InvalidDescriptor("sidecar path has no UTF-8 filename".to_owned())
        })?;
    let temporary = path.with_file_name(format!(".{file_name}.{}.tmp", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|source| io_error(&temporary, source))?;
    let result = file.write_all(bytes).and_then(|()| file.sync_data());
    if let Err(source) = result {
        let _ = fs::remove_file(&temporary);
        return Err(io_error(&temporary, source));
    }
    if let Err(source) = fs::rename(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(io_error(path, source));
    }
    sync_parent(path)
}

fn sync_parent(path: &Path) -> VtopLogResult<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let directory = File::open(parent).map_err(|source| io_error(parent, source))?;
    directory
        .sync_all()
        .map_err(|source| io_error(parent, source))
}

fn read_header_with_path(file: &mut File, path: &Path) -> VtopLogResult<(SegmentHeader, u64)> {
    read_header(file).map_err(|error| with_path(error, path))
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

struct SegmentPaths {
    segment: PathBuf,
    index: PathBuf,
    manifest: PathBuf,
    commit: PathBuf,
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

    fn from_segment(path: &Path) -> VtopLogResult<Self> {
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Seek;
    use tempfile::tempdir;

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
            sequence,
            timestamp_millis: 1_700_000_000_000 + sequence as i64,
            key: b"key".to_vec(),
            value: value.to_vec(),
        }
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
        let boundary = read_commit_boundary(&paths.commit).unwrap();
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
}

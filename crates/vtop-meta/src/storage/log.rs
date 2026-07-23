//! Chunked, append-only, checksummed raft log for metadata entries.
//!
//! Chunks are `log-{first_index:020}.vmlog` files with a checksummed header
//! binding cluster id, shard, and first index; entries are individually
//! framed and BLAKE3-checksummed. Appends buffer whole frames and `sync_data`
//! before returning, so a crash leaves the durable tail as a prefix of
//! acknowledged frames plus at most one torn frame.
//!
//! Recovery policy, frozen for v1: a torn or checksum-failing frame that
//! runs to the end of the final chunk is truncated (that is exactly the
//! crash-reachable state space); corruption anywhere else — bad magic, bad
//! lengths, mid-file checksum failures, index or term discontinuities — is
//! an error, never silently repaired. A tear always leaves a strict byte
//! prefix of real frames, so nothing outside that policy can be crash
//! fallout.

use super::{corrupt, io_error, MetaStoreError, MetaStoreResult};
use crate::command::MetadataCommand;
use crate::keys::MetaNodeId;
use crate::wire::{put_bounded_str, put_u16, put_u32, put_u64, put_u8, CodecError, Reader};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;
use vtop_log::env::{Env, OpenMode};

pub(crate) const CHUNK_MAGIC: &[u8; 8] = b"VTOPMLG1";
const CHUNK_VERSION: u16 = 1;
/// magic 8 + version 2 + cluster 16 + shard 2 + first_index 8 + checksum 32.
pub(crate) const CHUNK_HEADER_BYTES: u64 = 68;

pub(crate) const ENTRY_MAGIC: &[u8; 8] = b"VTOPMLE1";
/// magic 8 + frame_len 4.
const FRAME_PREFIX_BYTES: usize = 12;
/// term 8 + index 8 + kind 1 + payload_len 4 + checksum 32.
const FRAME_FIXED_BODY_BYTES: usize = 53;
const CHECKSUM_LEN: usize = 32;

const ENTRY_KIND_NORMAL: u8 = 1;
const ENTRY_KIND_MEMBERSHIP: u8 = 2;
const ENTRY_KIND_BLANK: u8 = 3;

/// Commands are small; membership lists are bounded. Half a mebibyte leaves
/// generous headroom while keeping a hostile frame length harmless.
const MAX_ENTRY_PAYLOAD_BYTES: usize = 512 * 1024;
const MAX_MEMBERSHIP_NODES: usize = 1024;
const MAX_LEARNER_ADDR_BYTES: usize = 256;

/// Chunk rotation threshold in production.
pub const DEFAULT_MAX_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
/// Floor for the configurable threshold, so even sweep-sized configurations
/// hold a header and make per-entry progress.
pub const MIN_MAX_CHUNK_BYTES: u64 = 128;

/// Log tuning. The rotation threshold is configurable solely so the crash
/// sweeps can drive multi-chunk workloads with tiny entries.
#[derive(Clone, Copy, Debug)]
pub struct MetaLogConfig {
    pub max_chunk_bytes: u64,
}

impl Default for MetaLogConfig {
    fn default() -> Self {
        Self {
            max_chunk_bytes: DEFAULT_MAX_CHUNK_BYTES,
        }
    }
}

/// Raft membership carried in the log and in snapshot headers. Hand-encoded
/// like everything else: bounded counts, bounded addresses.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetaMembership {
    pub voters: Vec<MetaNodeId>,
    pub learners: Vec<(MetaNodeId, String)>,
}

impl MetaMembership {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        if self.voters.len() > MAX_MEMBERSHIP_NODES {
            return Err(CodecError::BoundExceeded {
                what: "membership voters",
                actual: self.voters.len(),
                maximum: MAX_MEMBERSHIP_NODES,
            });
        }
        if self.learners.len() > MAX_MEMBERSHIP_NODES {
            return Err(CodecError::BoundExceeded {
                what: "membership learners",
                actual: self.learners.len(),
                maximum: MAX_MEMBERSHIP_NODES,
            });
        }
        let mut out = Vec::with_capacity(4 + self.voters.len() * 8 + self.learners.len() * 24);
        put_u16(&mut out, self.voters.len() as u16);
        for voter in &self.voters {
            put_u64(&mut out, voter.0);
        }
        put_u16(&mut out, self.learners.len() as u16);
        for (node, addr) in &self.learners {
            put_u64(&mut out, node.0);
            put_bounded_str(&mut out, addr, MAX_LEARNER_ADDR_BYTES, "learner address")?;
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let membership = Self::decode_from(&mut reader)?;
        reader.finish()?;
        Ok(membership)
    }

    pub(crate) fn decode_from(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        let voter_count = reader.u16("membership voter count")? as usize;
        if voter_count > MAX_MEMBERSHIP_NODES {
            return Err(CodecError::BoundExceeded {
                what: "membership voters",
                actual: voter_count,
                maximum: MAX_MEMBERSHIP_NODES,
            });
        }
        let mut voters = Vec::with_capacity(voter_count);
        for _ in 0..voter_count {
            voters.push(MetaNodeId(reader.u64("membership voter id")?));
        }
        let learner_count = reader.u16("membership learner count")? as usize;
        if learner_count > MAX_MEMBERSHIP_NODES {
            return Err(CodecError::BoundExceeded {
                what: "membership learners",
                actual: learner_count,
                maximum: MAX_MEMBERSHIP_NODES,
            });
        }
        let mut learners = Vec::with_capacity(learner_count);
        for _ in 0..learner_count {
            let node = MetaNodeId(reader.u64("membership learner id")?);
            let addr = reader.bounded_str(MAX_LEARNER_ADDR_BYTES, "learner address")?;
            learners.push((node, addr));
        }
        Ok(Self { voters, learners })
    }
}

/// The payload of one log entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetaLogPayload {
    Normal(MetadataCommand),
    Membership(MetaMembership),
    Blank,
}

/// One replicated log entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaLogEntry {
    pub term: u64,
    pub index: u64,
    pub payload: MetaLogPayload,
}

impl MetaLogEntry {
    /// Encode the durable frame for this entry, checksum included.
    pub fn encode_frame(&self) -> Result<Vec<u8>, CodecError> {
        let payload = match &self.payload {
            MetaLogPayload::Normal(command) => command.encode()?,
            MetaLogPayload::Membership(membership) => membership.encode()?,
            MetaLogPayload::Blank => Vec::new(),
        };
        if payload.len() > MAX_ENTRY_PAYLOAD_BYTES {
            return Err(CodecError::BoundExceeded {
                what: "log entry payload",
                actual: payload.len(),
                maximum: MAX_ENTRY_PAYLOAD_BYTES,
            });
        }
        let frame_len = (FRAME_FIXED_BODY_BYTES + payload.len()) as u32;
        let mut out = Vec::with_capacity(FRAME_PREFIX_BYTES + frame_len as usize);
        out.extend_from_slice(ENTRY_MAGIC);
        put_u32(&mut out, frame_len);
        put_u64(&mut out, self.term);
        put_u64(&mut out, self.index);
        put_u8(
            &mut out,
            match self.payload {
                MetaLogPayload::Normal(_) => ENTRY_KIND_NORMAL,
                MetaLogPayload::Membership(_) => ENTRY_KIND_MEMBERSHIP,
                MetaLogPayload::Blank => ENTRY_KIND_BLANK,
            },
        );
        put_u32(&mut out, payload.len() as u32);
        out.extend_from_slice(&payload);
        let checksum = blake3::hash(&out);
        out.extend_from_slice(checksum.as_bytes());
        Ok(out)
    }
}

enum FrameParse {
    Complete {
        term: u64,
        index: u64,
        payload: MetaLogPayload,
        total_bytes: usize,
    },
    /// The frame could be the durable image of one interrupted write: it is
    /// a byte prefix of a plausible frame, or a checksum-failing frame that
    /// runs exactly to the end of the bytes.
    Torn,
    /// Provably not crash fallout: bad magic, impossible lengths, or a
    /// checksum failure with more data behind it.
    Corrupt(String),
}

/// Parse one frame at `position`. A tear (crash mid-write) always leaves a
/// strict byte prefix of a real frame at the end of the file, so anything
/// else — wrong magic, an out-of-range length, a checksum failure that is
/// not the final bytes — is corruption, not a tear.
fn parse_frame(bytes: &[u8], position: usize) -> FrameParse {
    let remaining = &bytes[position..];
    if remaining.len() < FRAME_PREFIX_BYTES {
        let comparable = remaining.len().min(ENTRY_MAGIC.len());
        if remaining[..comparable] != ENTRY_MAGIC[..comparable] {
            return FrameParse::Corrupt("invalid entry magic in incomplete frame".to_owned());
        }
        return FrameParse::Torn;
    }
    if &remaining[..8] != ENTRY_MAGIC {
        return FrameParse::Corrupt("invalid entry magic".to_owned());
    }
    let frame_len = u32::from_be_bytes(remaining[8..12].try_into().expect("fixed slice")) as usize;
    if !(FRAME_FIXED_BODY_BYTES..=FRAME_FIXED_BODY_BYTES + MAX_ENTRY_PAYLOAD_BYTES)
        .contains(&frame_len)
    {
        return FrameParse::Corrupt(format!("invalid entry frame length {frame_len}"));
    }
    let total_bytes = FRAME_PREFIX_BYTES + frame_len;
    if remaining.len() < total_bytes {
        return FrameParse::Torn;
    }
    let frame = &remaining[..total_bytes];
    let (authenticated, stored_checksum) = frame.split_at(total_bytes - CHECKSUM_LEN);
    if blake3::hash(authenticated).as_bytes() != stored_checksum {
        // The final bytes of the file may be one torn write whose cut
        // happened to land inside the checksum; anywhere else this is rot.
        if remaining.len() == total_bytes {
            return FrameParse::Torn;
        }
        return FrameParse::Corrupt("entry checksum mismatch".to_owned());
    }
    let term = u64::from_be_bytes(frame[12..20].try_into().expect("fixed slice"));
    let index = u64::from_be_bytes(frame[20..28].try_into().expect("fixed slice"));
    let kind = frame[28];
    let payload_len = u32::from_be_bytes(frame[29..33].try_into().expect("fixed slice")) as usize;
    if payload_len != frame_len - FRAME_FIXED_BODY_BYTES {
        return FrameParse::Corrupt("payload length does not match the frame length".to_owned());
    }
    let payload_bytes = &frame[33..33 + payload_len];
    let payload = match kind {
        ENTRY_KIND_NORMAL => match MetadataCommand::decode(payload_bytes) {
            Ok(command) => MetaLogPayload::Normal(command),
            Err(error) => return FrameParse::Corrupt(format!("invalid command payload: {error}")),
        },
        ENTRY_KIND_MEMBERSHIP => match MetaMembership::decode(payload_bytes) {
            Ok(membership) => MetaLogPayload::Membership(membership),
            Err(error) => {
                return FrameParse::Corrupt(format!("invalid membership payload: {error}"));
            }
        },
        ENTRY_KIND_BLANK => {
            if payload_len != 0 {
                return FrameParse::Corrupt("blank entries must carry no payload".to_owned());
            }
            MetaLogPayload::Blank
        }
        other => return FrameParse::Corrupt(format!("unknown entry kind {other}")),
    };
    FrameParse::Complete {
        term,
        index,
        payload,
        total_bytes,
    }
}

pub(crate) fn encode_chunk_header(cluster_id: Uuid, first_index: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(CHUNK_HEADER_BYTES as usize);
    out.extend_from_slice(CHUNK_MAGIC);
    put_u16(&mut out, CHUNK_VERSION);
    out.extend_from_slice(cluster_id.as_bytes());
    put_u16(&mut out, crate::keys::META_SHARD_ID);
    put_u64(&mut out, first_index);
    let checksum = blake3::hash(&out);
    out.extend_from_slice(checksum.as_bytes());
    out
}

fn chunk_file_name(first_index: u64) -> String {
    format!("log-{first_index:020}.vmlog")
}

fn parse_chunk_file_name(name: &str) -> Option<u64> {
    let digits = name.strip_prefix("log-")?.strip_suffix(".vmlog")?;
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

#[derive(Clone, Debug)]
struct Chunk {
    first_index: u64,
    path: PathBuf,
    len: u64,
}

#[derive(Clone, Copy, Debug)]
struct EntryLocation {
    index: u64,
    term: u64,
    chunk_first_index: u64,
    offset: u64,
}

/// The recovered chunked log.
pub struct MetaLog {
    env: Env,
    dir: PathBuf,
    cluster_id: Uuid,
    config: MetaLogConfig,
    chunks: Vec<Chunk>,
    entries: Vec<EntryLocation>,
    poisoned: bool,
}

impl MetaLog {
    /// Recover the log from `dir`: order chunks by first index, validate
    /// headers and contiguity, scan every frame, truncate a torn tail frame,
    /// and reject corruption anywhere else.
    pub fn open_in(
        env: &Env,
        dir: impl AsRef<Path>,
        cluster_id: Uuid,
        config: MetaLogConfig,
    ) -> MetaStoreResult<Self> {
        if config.max_chunk_bytes < MIN_MAX_CHUNK_BYTES {
            return Err(MetaStoreError::InvalidConfig(format!(
                "max_chunk_bytes must be at least {MIN_MAX_CHUNK_BYTES}"
            )));
        }
        let dir = dir.as_ref().to_path_buf();
        let mut found: Vec<(u64, PathBuf)> = Vec::new();
        for entry in env
            .storage
            .read_dir(&dir)
            .map_err(|source| io_error(&dir, source))?
        {
            if !entry.is_regular_file {
                continue;
            }
            let Some(name) = entry.path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if super::is_atomic_temp_name(name) {
                continue;
            }
            if let Some(first_index) = parse_chunk_file_name(name) {
                found.push((first_index, entry.path.clone()));
            }
        }
        found.sort_by_key(|(first_index, _)| *first_index);

        let mut log = Self {
            env: env.clone(),
            dir,
            cluster_id,
            config,
            chunks: Vec::new(),
            entries: Vec::new(),
            poisoned: false,
        };
        let mut expected_index: Option<u64> = None;
        let total = found.len();
        for (ordinal, (name_first_index, path)) in found.into_iter().enumerate() {
            let is_last = ordinal + 1 == total;
            log.recover_chunk(&path, name_first_index, &mut expected_index, is_last)?;
        }
        Ok(log)
    }

    fn recover_chunk(
        &mut self,
        path: &Path,
        name_first_index: u64,
        expected_index: &mut Option<u64>,
        is_last: bool,
    ) -> MetaStoreResult<()> {
        let bytes = self
            .env
            .storage
            .read(path)
            .map_err(|source| io_error(path, source))?;
        // The header is written as one 68-byte write before any entry, so a
        // torn chunk creation is exactly "shorter than one header". Such a
        // file can hold no acknowledged entry and is deleted; a full-length
        // header that fails validation is corruption and must surface.
        if is_last && bytes.len() < CHUNK_HEADER_BYTES as usize {
            self.env
                .storage
                .remove_file(path)
                .map_err(|source| io_error(path, source))?;
            return self.sync_dir();
        }
        self.validate_chunk_header(path, &bytes, name_first_index)?;
        if let Some(expected) = *expected_index {
            if name_first_index != expected {
                return Err(corrupt(
                    path,
                    format!("chunk starts at {name_first_index}, expected {expected}"),
                ));
            }
        }

        let mut position = CHUNK_HEADER_BYTES as usize;
        let mut next_index = name_first_index;
        let mut previous_term = self.entries.last().map_or(0, |entry| entry.term);
        let mut scanned: Vec<EntryLocation> = Vec::new();
        let mut keep_bytes = bytes.len();
        while position < bytes.len() {
            match parse_frame(&bytes, position) {
                FrameParse::Complete {
                    term,
                    index,
                    total_bytes,
                    ..
                } => {
                    if index != next_index || term < previous_term {
                        return Err(corrupt(
                            path,
                            format!(
                                "entry at byte {position} has index {index} term {term}, \
                                 expected index {next_index} and term >= {previous_term}"
                            ),
                        ));
                    }
                    scanned.push(EntryLocation {
                        index,
                        term,
                        chunk_first_index: name_first_index,
                        offset: position as u64,
                    });
                    previous_term = term;
                    next_index += 1;
                    position += total_bytes;
                }
                FrameParse::Torn => {
                    if !is_last {
                        return Err(corrupt(
                            path,
                            format!("torn entry frame at byte {position} of a non-tail chunk"),
                        ));
                    }
                    keep_bytes = position;
                    break;
                }
                FrameParse::Corrupt(reason) => {
                    return Err(corrupt(path, format!("entry at byte {position}: {reason}")));
                }
            }
        }
        if keep_bytes < bytes.len() {
            let mut file = self
                .env
                .storage
                .open(path, OpenMode::ReadWrite)
                .map_err(|source| io_error(path, source))?;
            file.set_len(keep_bytes as u64)
                .and_then(|()| file.sync_data())
                .map_err(|source| io_error(path, source))?;
        }
        self.chunks.push(Chunk {
            first_index: name_first_index,
            path: path.to_path_buf(),
            len: keep_bytes as u64,
        });
        self.entries.extend(scanned);
        *expected_index = Some(next_index);
        Ok(())
    }

    fn validate_chunk_header(
        &self,
        path: &Path,
        bytes: &[u8],
        name_first_index: u64,
    ) -> MetaStoreResult<()> {
        if bytes.len() < CHUNK_HEADER_BYTES as usize {
            return Err(corrupt(path, "chunk header is incomplete"));
        }
        let header = &bytes[..CHUNK_HEADER_BYTES as usize];
        let (payload, stored_checksum) = header.split_at(header.len() - CHECKSUM_LEN);
        if blake3::hash(payload).as_bytes() != stored_checksum {
            return Err(corrupt(path, "chunk header checksum mismatch"));
        }
        if &payload[..8] != CHUNK_MAGIC {
            return Err(corrupt(path, "chunk header magic mismatch"));
        }
        let version = u16::from_be_bytes(payload[8..10].try_into().expect("fixed slice"));
        if version != CHUNK_VERSION {
            return Err(MetaStoreError::UnsupportedVersion {
                path: path.to_path_buf(),
                version,
            });
        }
        let cluster = Uuid::from_bytes(payload[10..26].try_into().expect("fixed slice"));
        if cluster != self.cluster_id {
            return Err(corrupt(
                path,
                format!(
                    "chunk belongs to cluster {cluster}, not {}",
                    self.cluster_id
                ),
            ));
        }
        let shard = u16::from_be_bytes(payload[26..28].try_into().expect("fixed slice"));
        if shard != crate::keys::META_SHARD_ID {
            return Err(corrupt(
                path,
                format!("chunk belongs to foreign shard {shard}"),
            ));
        }
        let header_first = u64::from_be_bytes(payload[28..36].try_into().expect("fixed slice"));
        if header_first != name_first_index {
            return Err(corrupt(
                path,
                format!(
                    "chunk header first index {header_first} disagrees with \
                     file name index {name_first_index}"
                ),
            ));
        }
        Ok(())
    }

    pub fn first_index(&self) -> Option<u64> {
        self.entries.first().map(|entry| entry.index)
    }

    pub fn last_index(&self) -> Option<u64> {
        self.entries.last().map(|entry| entry.index)
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    fn guard_poisoned(&self) -> MetaStoreResult<()> {
        if self.poisoned {
            return Err(MetaStoreError::Poisoned("metadata log"));
        }
        Ok(())
    }

    fn sync_dir(&self) -> MetaStoreResult<()> {
        self.env
            .storage
            .sync_dir(&self.dir)
            .map_err(|source| io_error(&self.dir, source))
    }

    /// Durably append contiguous entries with non-decreasing terms; buffered
    /// writes are synced before this returns. Rotates to a new chunk when
    /// the configured threshold would be exceeded.
    pub fn append(&mut self, entries: &[MetaLogEntry]) -> MetaStoreResult<()> {
        self.guard_poisoned()?;
        if entries.is_empty() {
            return Ok(());
        }
        let mut expected = self.last_index().map(|last| last + 1);
        let mut previous_term = self.entries.last().map_or(0, |entry| entry.term);
        let mut frames = Vec::with_capacity(entries.len());
        for entry in entries {
            if expected.is_some_and(|expected| entry.index != expected) {
                return Err(MetaStoreError::InvalidConfig(format!(
                    "append entry index {} breaks contiguity; expected {}",
                    entry.index,
                    expected.expect("checked above")
                )));
            }
            if entry.term < previous_term {
                return Err(MetaStoreError::InvalidConfig(format!(
                    "append entry term {} regresses below {previous_term}",
                    entry.term
                )));
            }
            frames.push(entry.encode_frame().map_err(|error| {
                MetaStoreError::InvalidConfig(format!("cannot encode log entry: {error}"))
            })?);
            expected = Some(entry.index + 1);
            previous_term = entry.term;
        }

        // A header-only tail chunk left by a crashed rotation must agree
        // with the index we are about to write; if not (possible only on a
        // log whose every entry was truncated away), it is stale and must go
        // before it can break the on-disk contiguity invariant.
        if self.entries.is_empty() {
            if let Some(chunk) = self.chunks.last() {
                if chunk.first_index != entries[0].index {
                    let path = chunk.path.clone();
                    if let Err(source) = self.env.storage.remove_file(&path) {
                        self.poisoned = true;
                        return Err(io_error(&path, source));
                    }
                    if let Err(error) = self.sync_dir() {
                        self.poisoned = true;
                        return Err(error);
                    }
                    self.chunks.pop();
                }
            }
        }
        if self.chunks.is_empty() {
            self.create_chunk(entries[0].index)?;
        }
        let mut chunk_ordinal = self.chunks.len() - 1;
        let mut write_offset = self.chunks[chunk_ordinal].len;
        let mut buffer: Vec<u8> = Vec::new();
        let mut new_locations: Vec<EntryLocation> = Vec::new();
        for (entry, frame) in entries.iter().zip(&frames) {
            let projected = self.chunks[chunk_ordinal].len + buffer.len() as u64;
            let holds_entries = projected > CHUNK_HEADER_BYTES;
            if holds_entries && projected + frame.len() as u64 > self.config.max_chunk_bytes {
                self.flush(chunk_ordinal, write_offset, &buffer)?;
                self.chunks[chunk_ordinal].len += buffer.len() as u64;
                buffer.clear();
                self.create_chunk(entry.index)?;
                chunk_ordinal = self.chunks.len() - 1;
                write_offset = self.chunks[chunk_ordinal].len;
            }
            new_locations.push(EntryLocation {
                index: entry.index,
                term: entry.term,
                chunk_first_index: self.chunks[chunk_ordinal].first_index,
                offset: self.chunks[chunk_ordinal].len + buffer.len() as u64,
            });
            buffer.extend_from_slice(frame);
        }
        if !buffer.is_empty() {
            self.flush(chunk_ordinal, write_offset, &buffer)?;
            self.chunks[chunk_ordinal].len += buffer.len() as u64;
        }
        self.entries.extend(new_locations);
        Ok(())
    }

    fn flush(&mut self, chunk_ordinal: usize, offset: u64, buffer: &[u8]) -> MetaStoreResult<()> {
        let path = self.chunks[chunk_ordinal].path.clone();
        let result = (|| {
            let mut file = self.env.storage.open(&path, OpenMode::ReadWrite)?;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(buffer)?;
            file.sync_data()
        })();
        if let Err(source) = result {
            self.poisoned = true;
            return Err(io_error(&path, source));
        }
        Ok(())
    }

    fn create_chunk(&mut self, first_index: u64) -> MetaStoreResult<()> {
        let path = self.dir.join(chunk_file_name(first_index));
        let header = encode_chunk_header(self.cluster_id, first_index);
        let result = (|| {
            let mut file = self.env.storage.open(&path, OpenMode::CreateNew)?;
            file.write_all(&header)?;
            file.sync_data()
        })();
        if let Err(source) = result {
            self.poisoned = true;
            return Err(io_error(&path, source));
        }
        if let Err(error) = self.sync_dir() {
            self.poisoned = true;
            return Err(error);
        }
        self.chunks.push(Chunk {
            first_index,
            path,
            len: CHUNK_HEADER_BYTES,
        });
        Ok(())
    }

    /// Read entries in `[start, end)`, re-verifying every frame checksum.
    pub fn read_range(&self, start: u64, end: u64) -> MetaStoreResult<Vec<MetaLogEntry>> {
        if start >= end {
            return Ok(Vec::new());
        }
        let (Some(first), Some(last)) = (self.first_index(), self.last_index()) else {
            return Err(MetaStoreError::InvalidConfig(format!(
                "cannot read [{start}, {end}) from an empty log"
            )));
        };
        if start < first || end - 1 > last {
            return Err(MetaStoreError::InvalidConfig(format!(
                "read range [{start}, {end}) is outside the log [{first}, {last}]"
            )));
        }
        let offset_in_entries = (start - first) as usize;
        let count = (end - start) as usize;
        let mut out = Vec::with_capacity(count);
        let mut cached: Option<(u64, Vec<u8>)> = None;
        for location in &self.entries[offset_in_entries..offset_in_entries + count] {
            let chunk = self
                .chunks
                .iter()
                .find(|chunk| chunk.first_index == location.chunk_first_index)
                .expect("every entry location points at a live chunk");
            if cached
                .as_ref()
                .is_none_or(|(first_index, _)| *first_index != chunk.first_index)
            {
                let bytes = self
                    .env
                    .storage
                    .read(&chunk.path)
                    .map_err(|source| io_error(&chunk.path, source))?;
                cached = Some((chunk.first_index, bytes));
            }
            let (_, bytes) = cached.as_ref().expect("cache was just filled");
            match parse_frame(bytes, location.offset as usize) {
                FrameParse::Complete {
                    term,
                    index,
                    payload,
                    ..
                } => {
                    if index != location.index || term != location.term {
                        return Err(corrupt(
                            &chunk.path,
                            format!("entry at byte {} changed identity on disk", location.offset),
                        ));
                    }
                    out.push(MetaLogEntry {
                        term,
                        index,
                        payload,
                    });
                }
                FrameParse::Torn | FrameParse::Corrupt(_) => {
                    return Err(corrupt(
                        &chunk.path,
                        format!("entry at byte {} failed re-validation", location.offset),
                    ));
                }
            }
        }
        Ok(out)
    }

    /// Delete every entry with index >= `index`: later whole chunks are
    /// removed (highest first, so a crash always preserves a contiguous
    /// prefix), then the boundary chunk is shortened in place and synced.
    pub fn truncate_since(&mut self, index: u64) -> MetaStoreResult<()> {
        self.guard_poisoned()?;
        if self.last_index().is_none_or(|last| index > last) {
            return Ok(());
        }
        let removed_chunks: Vec<Chunk> = self
            .chunks
            .iter()
            .filter(|chunk| chunk.first_index >= index)
            .cloned()
            .collect();
        let boundary_cut: Option<(usize, u64)> = self
            .entries
            .iter()
            .find(|entry| entry.index >= index && entry.chunk_first_index < index)
            .map(|entry| {
                let ordinal = self
                    .chunks
                    .iter()
                    .position(|chunk| chunk.first_index == entry.chunk_first_index)
                    .expect("boundary entry points at a live chunk");
                (ordinal, entry.offset)
            });
        for chunk in removed_chunks.iter().rev() {
            if let Err(source) = self.env.storage.remove_file(&chunk.path) {
                self.poisoned = true;
                return Err(io_error(&chunk.path, source));
            }
        }
        if !removed_chunks.is_empty() {
            if let Err(error) = self.sync_dir() {
                self.poisoned = true;
                return Err(error);
            }
        }
        if let Some((ordinal, keep)) = boundary_cut {
            let path = self.chunks[ordinal].path.clone();
            let result = (|| {
                let mut file = self.env.storage.open(&path, OpenMode::ReadWrite)?;
                file.set_len(keep)?;
                file.sync_data()
            })();
            if let Err(source) = result {
                self.poisoned = true;
                return Err(io_error(&path, source));
            }
            self.chunks[ordinal].len = keep;
        }
        self.chunks.retain(|chunk| chunk.first_index < index);
        self.entries.retain(|entry| entry.index < index);
        Ok(())
    }

    /// Delete whole chunks whose last entry is at or below `index`. Only
    /// whole chunks are ever purged, and the newest chunk is always kept so
    /// the log retains its position. Deletion runs lowest-first, so a crash
    /// leaves a contiguous suffix.
    /// Drop every log chunk and in-memory entry. Used after installing a
    /// snapshot whose frontier is ahead of the physical log tail: `purge_upto`
    /// always retains the newest chunk, which would otherwise block appends
    /// that must extend from the snapshot index.
    pub fn discard_all(&mut self) -> MetaStoreResult<()> {
        self.guard_poisoned()?;
        for chunk in &self.chunks {
            if let Err(source) = self.env.storage.remove_file(&chunk.path) {
                self.poisoned = true;
                return Err(io_error(&chunk.path, source));
            }
        }
        if let Err(error) = self.sync_dir() {
            self.poisoned = true;
            return Err(error);
        }
        self.chunks.clear();
        self.entries.clear();
        Ok(())
    }

    pub fn purge_upto(&mut self, index: u64) -> MetaStoreResult<()> {
        self.guard_poisoned()?;
        if self.chunks.len() < 2 {
            return Ok(());
        }
        // A non-final chunk's last index is the next chunk's first index
        // minus one, guaranteed by the contiguity invariant.
        let mut removable = 0;
        while removable + 1 < self.chunks.len()
            && self.chunks[removable + 1].first_index <= index + 1
        {
            removable += 1;
        }
        if removable == 0 {
            return Ok(());
        }
        for chunk in &self.chunks[..removable] {
            if let Err(source) = self.env.storage.remove_file(&chunk.path) {
                self.poisoned = true;
                return Err(io_error(&chunk.path, source));
            }
        }
        if let Err(error) = self.sync_dir() {
            self.poisoned = true;
            return Err(error);
        }
        let new_first = self.chunks[removable].first_index;
        self.chunks.drain(..removable);
        self.entries.retain(|entry| entry.index >= new_first);
        Ok(())
    }
}

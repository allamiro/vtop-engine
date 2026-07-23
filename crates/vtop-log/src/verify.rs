//! Independent offline verification of sealed segments.
//!
//! The verifier re-derives every claim from the immutable sealed artifacts
//! alone: the segment file, its manifest, and the `.chunks` sidecar. Mutable
//! broker metadata (commit boundaries, producer-epoch journals) is
//! deliberately never an input, so a verdict cannot be steered by state an
//! attacker could rewrite after sealing. Unlike [`crate::SegmentReader`],
//! which refuses to open a damaged segment at the first inconsistency, the
//! verifier collects every failed check into a [`VerifyReport`] so an
//! operator sees all of the damage at once; only an unreadable or
//! undecodable segment file itself is an error.

use crate::codec::{read_frame, read_header, AnyHeader, FrameRead};
use crate::codec_v2::read_frame_v2;
use crate::env::{Env, OpenMode, StorageFile};
use crate::proof::{self, ChunkParams, ChunkProof, ChunkTreeBuilder};
use crate::segment::{
    canonical_manifest_bytes, canonical_manifest_v2_bytes, commit_statement_core,
    read_chunk_sidecar, SegmentPaths,
};
use crate::types::{
    LogError, ProducerSummaryEntry, SegmentCommitKey, SegmentManifest, SegmentManifestV2,
    VtopLogResult, CHUNK_TREE_SCHEME_V1, COMMIT_SCHEME_KEYED, COMMIT_SCHEME_UNKEYED, FORMAT_NAME,
    FORMAT_VERSION, FORMAT_VERSION_V2, PRODUCER_SEQUENCE_WINDOW, RECORD_SCHEMA_VERSION_V2,
};
use std::collections::{BTreeMap, HashMap};
use std::io::{Read, SeekFrom};
use std::path::Path;
use uuid::Uuid;

/// Full frame scan: offsets, per-(producer, epoch) sequence rules, checksums.
pub const CHECK_FRAME_SCAN: &str = "frame-scan";
/// Recomputed content root (v2 chunk tree, v1 linear digest) vs the manifest.
pub const CHECK_CONTENT_ROOT: &str = "content-root";
/// `.chunks` sidecar leaves fold to the recomputed content root (v2 only).
pub const CHECK_CHUNK_SIDECAR: &str = "chunk-sidecar";
/// Manifest decodes and is byte-identical to its canonical VTOP encoding.
pub const CHECK_MANIFEST_CANONICAL: &str = "manifest-canonical";
/// Manifest fields agree with the header and the frame scan.
pub const CHECK_MANIFEST_CONSISTENCY: &str = "manifest-consistency";
/// Commit statement echoes the sealed manifest core fields.
pub const CHECK_STATEMENT_ECHO: &str = "statement-echo";
/// Commit statement's manifest core digest recomputes from the manifest.
pub const CHECK_STATEMENT_DIGEST: &str = "statement-digest";
/// Commit statement MAC (unkeyed digest, or keyed against the keyring).
pub const CHECK_STATEMENT_MAC: &str = "statement-mac";
/// Caller-pinned content root matches the recomputed root.
pub const CHECK_ROOT_PIN: &str = "root-pin";
/// Caller-pinned manifest core digest matches the recomputed digest.
pub const CHECK_MANIFEST_DIGEST_PIN: &str = "manifest-digest-pin";
/// The achieved level reaches the caller's required level.
pub const CHECK_REQUIRED_LEVEL: &str = "required-level";

/// How much a verification verdict may rely on beyond the artifacts alone.
///
/// The variants are ordered: `SelfConsistent < RootPinned < Authenticated`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum VerifyLevel {
    /// The artifacts agree with themselves; nothing external vouches for them.
    SelfConsistent,
    /// A caller-supplied pin (content root or manifest core digest) matched.
    RootPinned,
    /// A keyed commit statement verified against a caller-supplied key.
    Authenticated,
}

/// Caller-supplied trust anchors for [`verify_sealed_segment`].
pub struct VerifyExpectations {
    /// Expected content root: the v2 chunk-tree root, or for a v1 segment the
    /// linear BLAKE3 digest over its record frames.
    pub chunk_tree_root: Option<blake3::Hash>,
    /// Expected BLAKE3 digest of the canonical manifest bytes with the commit
    /// statement stripped (v2 only).
    pub manifest_core_digest: Option<blake3::Hash>,
    /// Commit keys by `key_id`; the empty string is a valid key id.
    pub keyring: BTreeMap<String, SegmentCommitKey>,
    /// Level the caller demands; the report records whether it was reached.
    pub require: VerifyLevel,
}

impl Default for VerifyExpectations {
    fn default() -> Self {
        Self {
            chunk_tree_root: None,
            manifest_core_digest: None,
            keyring: BTreeMap::new(),
            require: VerifyLevel::SelfConsistent,
        }
    }
}

/// One named verification step. `passed` means the step detected no problem;
/// a step that could not run against a tampered artifact fails with a detail
/// explaining why, while a step that was deliberately skipped (for example a
/// keyed MAC with no matching keyring entry) passes and says so in `detail`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckOutcome {
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
}

impl CheckOutcome {
    fn pass(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            passed: true,
            detail: detail.into(),
        }
    }

    fn fail(name: &'static str, detail: impl Into<String>) -> Self {
        Self {
            name,
            passed: false,
            detail: detail.into(),
        }
    }
}

/// Everything [`verify_sealed_segment`] established about one segment.
#[derive(Clone, Debug)]
pub struct VerifyReport {
    pub format_version: u16,
    pub segment_id: Uuid,
    pub record_count: u64,
    pub content_bytes: u64,
    pub chunk_count: u64,
    /// Highest level the artifacts and expectations support; failing checks
    /// are reported independently and always outweigh the achieved level.
    pub achieved: VerifyLevel,
    pub checks: Vec<CheckOutcome>,
}

impl VerifyReport {
    /// True when every check passed (the achieved level is judged separately
    /// by the `required-level` check, which is part of `checks`).
    pub fn passed(&self) -> bool {
        self.checks.iter().all(|check| check.passed)
    }
}

/// Verify a sealed segment (`.segment`) against `expectations`.
///
/// Returns `Err` only when the segment file itself cannot be read or its
/// header cannot be decoded; every other problem is a failing check inside
/// the returned report.
pub fn verify_sealed_segment(
    path: &Path,
    expectations: &VerifyExpectations,
) -> VtopLogResult<VerifyReport> {
    verify_sealed_segment_in(&Env::real(), path, expectations)
}

pub fn verify_sealed_segment_in(
    env: &Env,
    path: &Path,
    expectations: &VerifyExpectations,
) -> VtopLogResult<VerifyReport> {
    let paths = SegmentPaths::from_segment(path)?;
    let mut file = env
        .storage
        .open(path, OpenMode::Read)
        .map_err(|source| LogError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    let (header, header_len) = {
        let mut reader = file.as_mut();
        read_header(&mut reader)?
    };

    let mut checks = Vec::new();
    let scan = match scan_frames(file.as_mut(), &header, header_len) {
        Ok(outcome) => {
            checks.push(CheckOutcome::pass(
                CHECK_FRAME_SCAN,
                format!(
                    "{} records over {} content bytes",
                    outcome.records, outcome.content_bytes
                ),
            ));
            Some(outcome)
        }
        Err(detail) => {
            checks.push(CheckOutcome::fail(CHECK_FRAME_SCAN, detail));
            None
        }
    };

    let (achieved, record_count, content_bytes, chunk_count) = match &header {
        AnyHeader::V1(v1_header) => verify_v1(
            env,
            &paths,
            v1_header,
            scan.as_ref(),
            expectations,
            &mut checks,
        ),
        AnyHeader::V2(v2_header) => verify_v2(
            env,
            &paths,
            v2_header,
            scan.as_ref(),
            expectations,
            &mut checks,
        ),
    };

    if achieved >= expectations.require {
        checks.push(CheckOutcome::pass(
            CHECK_REQUIRED_LEVEL,
            format!(
                "achieved {} (required {})",
                level_name(achieved),
                level_name(expectations.require)
            ),
        ));
    } else {
        checks.push(CheckOutcome::fail(
            CHECK_REQUIRED_LEVEL,
            format!(
                "achieved only {} (required {})",
                level_name(achieved),
                level_name(expectations.require)
            ),
        ));
    }

    Ok(VerifyReport {
        format_version: header.format_version(),
        segment_id: match &header {
            AnyHeader::V1(header) => header.descriptor.segment_id,
            AnyHeader::V2(header) => header.descriptor.segment_id,
        },
        record_count,
        content_bytes,
        chunk_count,
        achieved,
        checks,
    })
}

/// Human-readable name of a verification level.
pub fn level_name(level: VerifyLevel) -> &'static str {
    match level {
        VerifyLevel::SelfConsistent => "self-consistent",
        VerifyLevel::RootPinned => "root-pinned",
        VerifyLevel::Authenticated => "authenticated",
    }
}

/// Build the inclusion proof for content chunk `index` of a sealed v2
/// segment, returning the chunking geometry, the proof, and the chunk bytes.
pub fn chunk_proof(path: &Path, index: u64) -> VtopLogResult<(ChunkParams, ChunkProof, Vec<u8>)> {
    chunk_proof_in(&Env::real(), path, index)
}

pub fn chunk_proof_in(
    env: &Env,
    path: &Path,
    index: u64,
) -> VtopLogResult<(ChunkParams, ChunkProof, Vec<u8>)> {
    // Opening the reader fully validates the segment against its manifest,
    // so the proof below is derived from bytes that fold to the sealed root.
    let reader = crate::SegmentReader::open_in(env, path)?;
    let manifest = reader
        .manifest_v2()
        .ok_or_else(|| {
            LogError::InvalidDescriptor(
                "chunk proofs require a v2 segment; v1 has no chunk tree".to_owned(),
            )
        })?
        .clone();
    drop(reader);
    if index >= manifest.chunk_count {
        return Err(LogError::InvalidDescriptor(format!(
            "chunk index {index} is out of range for {} chunks",
            manifest.chunk_count
        )));
    }
    // Segments run to gigabytes, so the proof must not materialize the file:
    // the tree comes from the sidecar leaves (validated against the sealed
    // root below and freshly repaired by the reader open above), and only
    // the requested chunk's byte range is read from the segment.
    let paths = SegmentPaths::from_segment(path)?;
    let (sidecar_chunk_size, leaves) = read_chunk_sidecar(env.storage.as_ref(), &paths.chunks)?;
    if sidecar_chunk_size != manifest.chunk_size
        || leaves.len() as u64 != manifest.chunk_count
        || proof::tree_root(&leaves).to_hex().as_str() != manifest.chunk_tree_root
    {
        return Err(LogError::ManifestMismatch(
            "chunk sidecar does not fold to the sealed chunk-tree root".to_owned(),
        ));
    }
    let io_error = |source: std::io::Error| LogError::Io {
        path: path.to_path_buf(),
        source,
    };
    let mut file = env.storage.open(path, OpenMode::Read).map_err(io_error)?;
    let (_, header_len) = read_header(&mut file)?;
    let start = index * u64::from(manifest.chunk_size);
    let length = manifest
        .content_bytes
        .saturating_sub(start)
        .min(u64::from(manifest.chunk_size));
    file.seek(SeekFrom::Start(header_len + start))
        .map_err(io_error)?;
    let mut chunk = vec![0_u8; length as usize];
    file.read_exact(&mut chunk).map_err(io_error)?;
    // One cheap hash guards the freshly read bytes against racing mutation
    // between the validating open and this read.
    if proof::leaf_hash(&chunk) != leaves[index as usize] {
        return Err(LogError::ManifestMismatch(format!(
            "chunk {index} bytes do not match the validated sidecar leaf"
        )));
    }
    let params = ChunkParams {
        chunk_size: manifest.chunk_size,
        chunk_count: manifest.chunk_count,
    };
    Ok((params, proof::prove_chunk(&leaves, index), chunk))
}

/// The frame scan re-derives everything a sealed manifest claims about the
/// content region, using only the record frames themselves.
struct ScanOutcome {
    records: u64,
    next_offset: u64,
    content_bytes: u64,
    /// Chunk-tree leaves; empty for a v1 segment.
    leaves: Vec<blake3::Hash>,
    /// Chunk-tree root (v2) or linear BLAKE3 digest (v1) over the frames.
    root: blake3::Hash,
    /// Sorted by `(producer_id, producer_epoch)`, like a sealed manifest.
    producer_summary: Vec<ProducerSummaryEntry>,
}

enum ContentDigest {
    Linear(Box<blake3::Hasher>),
    ChunkTree(ChunkTreeBuilder),
}

impl ContentDigest {
    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Linear(hasher) => {
                hasher.update(bytes);
            }
            Self::ChunkTree(builder) => builder.update(bytes),
        }
    }

    fn finalize(self) -> (Vec<blake3::Hash>, blake3::Hash) {
        match self {
            Self::Linear(hasher) => (Vec::new(), hasher.finalize()),
            Self::ChunkTree(builder) => builder.finalize(),
        }
    }
}

fn scan_frames(
    mut file: &mut dyn StorageFile,
    header: &AnyHeader,
    header_len: u64,
) -> Result<ScanOutcome, String> {
    let (base_offset, max_record_bytes, mut digest) = match header {
        AnyHeader::V1(header) => (
            header.descriptor.base_offset,
            header.config.max_record_bytes,
            ContentDigest::Linear(Box::new(blake3::Hasher::new())),
        ),
        AnyHeader::V2(header) => (
            header.descriptor.base_offset,
            header.config.max_record_bytes,
            ContentDigest::ChunkTree(ChunkTreeBuilder::new(header.config.chunk_size)),
        ),
    };
    file.seek(SeekFrom::Start(header_len))
        .map_err(|error| format!("cannot seek to content region: {error}"))?;
    let file_len = file
        .len()
        .map_err(|error| format!("cannot read file length: {error}"))?;

    let mut position = header_len;
    let mut records = 0_u64;
    // Latest sequence per (producer, epoch) and newest epoch per producer.
    let mut sequences: HashMap<(Uuid, u64), u64> = HashMap::new();
    let mut epochs: HashMap<Uuid, u64> = HashMap::new();
    while position < file_len {
        let frame = match header {
            AnyHeader::V1(_) => read_frame(&mut file, position, max_record_bytes),
            AnyHeader::V2(_) => read_frame_v2(&mut file, position, max_record_bytes),
        }
        .map_err(|error| error.to_string())?;
        let frame = match frame {
            FrameRead::Complete(frame) => frame,
            FrameRead::End => break,
            FrameRead::Torn => {
                return Err(format!(
                    "byte {position}: sealed segment ends with an incomplete record"
                ));
            }
        };
        if frame.relative_offset != records {
            return Err(format!(
                "byte {position}: record carries relative offset {}, expected {records}",
                frame.relative_offset
            ));
        }
        let record = &frame.record;
        // Schema v2 reserves every attribute bit; the codec enforces this on
        // decode, so reaching here with nonzero bits means a v1 frame, which
        // cannot carry them at all.
        debug_assert_eq!(record.attributes, 0);
        if let Some(latest_epoch) = epochs.get(&record.producer_id) {
            if record.producer_epoch < *latest_epoch {
                return Err(format!(
                    "byte {position}: producer {} epoch {} appears after newer epoch {latest_epoch}",
                    record.producer_id, record.producer_epoch
                ));
            }
        }
        let key = (record.producer_id, record.producer_epoch);
        match sequences.get(&key) {
            None if record.sequence != 0 => {
                return Err(format!(
                    "byte {position}: producer {} epoch {} starts at sequence {}, expected 0",
                    record.producer_id, record.producer_epoch, record.sequence
                ));
            }
            Some(latest) if record.sequence != latest + 1 => {
                return Err(format!(
                    "byte {position}: producer {} epoch {} carries sequence {}, expected {}",
                    record.producer_id,
                    record.producer_epoch,
                    record.sequence,
                    latest + 1
                ));
            }
            _ => {}
        }
        sequences.insert(key, record.sequence);
        epochs
            .entry(record.producer_id)
            .and_modify(|latest| *latest = (*latest).max(record.producer_epoch))
            .or_insert(record.producer_epoch);
        digest.update(&frame.encoded);
        position += frame.encoded_len as u64;
        records += 1;
    }

    let mut producer_summary: Vec<ProducerSummaryEntry> = sequences
        .iter()
        .map(|((producer_id, producer_epoch), latest)| {
            // Every (producer, epoch) run starts at sequence zero and
            // advances by one, as enforced above — but the seal path
            // derives the summary from the RETAINED duplicate-detection
            // window (at most PRODUCER_SEQUENCE_WINDOW most recent
            // sequences), so a run longer than the window reports the
            // window floor, not zero. Reproduce that arithmetic exactly.
            let first_sequence = latest.saturating_sub(PRODUCER_SEQUENCE_WINDOW - 1);
            ProducerSummaryEntry {
                producer_id: *producer_id,
                producer_epoch: *producer_epoch,
                first_sequence,
                last_sequence: *latest,
                record_count: latest - first_sequence + 1,
            }
        })
        .collect();
    producer_summary.sort_by_key(|entry| (entry.producer_id, entry.producer_epoch));
    let (leaves, root) = digest.finalize();
    Ok(ScanOutcome {
        records,
        next_offset: base_offset + records,
        content_bytes: position - header_len,
        leaves,
        root,
        producer_summary,
    })
}

fn verify_v1(
    env: &Env,
    paths: &SegmentPaths,
    header: &crate::codec::SegmentHeader,
    scan: Option<&ScanOutcome>,
    expectations: &VerifyExpectations,
    checks: &mut Vec<CheckOutcome>,
) -> (VerifyLevel, u64, u64, u64) {
    let manifest = read_manifest_checked::<SegmentManifest>(env, paths, checks, |manifest| {
        canonical_manifest_bytes(manifest)
    });

    match (scan, &manifest) {
        (Some(scan), Some(manifest)) => {
            let root = scan.root.to_hex();
            if manifest.blake3_root == root.as_str() {
                checks.push(CheckOutcome::pass(CHECK_CONTENT_ROOT, root.to_string()));
            } else {
                checks.push(CheckOutcome::fail(
                    CHECK_CONTENT_ROOT,
                    format!(
                        "recomputed linear root {root}; manifest pins {}",
                        manifest.blake3_root
                    ),
                ));
            }
            let mut mismatched = Vec::new();
            if manifest.format != FORMAT_NAME {
                mismatched.push("format");
            }
            if manifest.version != FORMAT_VERSION {
                mismatched.push("version");
            }
            if manifest.descriptor != header.descriptor {
                mismatched.push("descriptor");
            }
            if manifest.record_count != scan.records {
                mismatched.push("record_count");
            }
            if manifest.first_offset != (scan.records > 0).then_some(header.descriptor.base_offset)
            {
                mismatched.push("first_offset");
            }
            if manifest.next_offset != scan.next_offset {
                mismatched.push("next_offset");
            }
            if manifest.content_bytes != scan.content_bytes {
                mismatched.push("content_bytes");
            }
            if manifest.index_stride != header.config.index_stride {
                mismatched.push("index_stride");
            }
            push_field_consistency(checks, mismatched);
        }
        _ => {
            checks.push(CheckOutcome::fail(
                CHECK_CONTENT_ROOT,
                "frame scan or manifest unavailable".to_owned(),
            ));
            checks.push(CheckOutcome::fail(
                CHECK_MANIFEST_CONSISTENCY,
                "frame scan or manifest unavailable".to_owned(),
            ));
        }
    }

    let pinned = check_root_pin(scan, expectations, checks);
    if expectations.manifest_core_digest.is_some() {
        checks.push(CheckOutcome::fail(
            CHECK_MANIFEST_DIGEST_PIN,
            "v1 segments have no manifest core digest; only the linear root can be pinned"
                .to_owned(),
        ));
    }

    // A v1 segment carries no commit statement, so authentication is
    // unreachable and the maximum achievable level is a matching root pin.
    let achieved = if pinned {
        VerifyLevel::RootPinned
    } else {
        VerifyLevel::SelfConsistent
    };
    let (record_count, content_bytes) = match (scan, &manifest) {
        (Some(scan), _) => (scan.records, scan.content_bytes),
        (None, Some(manifest)) => (manifest.record_count, manifest.content_bytes),
        (None, None) => (0, 0),
    };
    (achieved, record_count, content_bytes, 0)
}

fn verify_v2(
    env: &Env,
    paths: &SegmentPaths,
    header: &crate::codec_v2::SegmentHeaderV2,
    scan: Option<&ScanOutcome>,
    expectations: &VerifyExpectations,
    checks: &mut Vec<CheckOutcome>,
) -> (VerifyLevel, u64, u64, u64) {
    let manifest = read_manifest_checked::<SegmentManifestV2>(env, paths, checks, |manifest| {
        canonical_manifest_v2_bytes(manifest)
    });

    match (scan, &manifest) {
        (Some(scan), Some(manifest)) => {
            let root = scan.root.to_hex();
            if manifest.chunk_tree_root == root.as_str() {
                checks.push(CheckOutcome::pass(CHECK_CONTENT_ROOT, root.to_string()));
            } else {
                checks.push(CheckOutcome::fail(
                    CHECK_CONTENT_ROOT,
                    format!(
                        "recomputed chunk-tree root {root}; manifest pins {}",
                        manifest.chunk_tree_root
                    ),
                ));
            }
        }
        _ => {
            checks.push(CheckOutcome::fail(
                CHECK_CONTENT_ROOT,
                "frame scan or manifest unavailable".to_owned(),
            ));
        }
    }

    match read_chunk_sidecar(env.storage.as_ref(), &paths.chunks) {
        Ok((chunk_size, stored)) => match scan {
            Some(_) if chunk_size != header.config.chunk_size => {
                checks.push(CheckOutcome::fail(
                    CHECK_CHUNK_SIDECAR,
                    format!(
                        "sidecar chunk size {chunk_size} differs from configured {}",
                        header.config.chunk_size
                    ),
                ));
            }
            Some(scan) if stored != scan.leaves => {
                checks.push(CheckOutcome::fail(
                    CHECK_CHUNK_SIDECAR,
                    "sidecar leaves do not fold to the recomputed content root".to_owned(),
                ));
            }
            Some(scan) => {
                checks.push(CheckOutcome::pass(
                    CHECK_CHUNK_SIDECAR,
                    format!("{} leaves fold to the content root", scan.leaves.len()),
                ));
            }
            None => {
                checks.push(CheckOutcome::fail(
                    CHECK_CHUNK_SIDECAR,
                    "frame scan failed; sidecar cannot be cross-checked".to_owned(),
                ));
            }
        },
        Err(error) => {
            checks.push(CheckOutcome::fail(
                CHECK_CHUNK_SIDECAR,
                format!("cannot read chunk sidecar: {error}"),
            ));
        }
    }

    match (scan, &manifest) {
        (Some(scan), Some(manifest)) => {
            let mut mismatched = Vec::new();
            if manifest.format != FORMAT_NAME {
                mismatched.push("format");
            }
            if manifest.version != FORMAT_VERSION_V2 {
                mismatched.push("version");
            }
            if manifest.record_schema_version != RECORD_SCHEMA_VERSION_V2 {
                mismatched.push("record_schema_version");
            }
            if manifest.descriptor != header.descriptor {
                mismatched.push("descriptor");
            }
            if manifest.record_count != scan.records {
                mismatched.push("record_count");
            }
            if manifest.first_offset != (scan.records > 0).then_some(header.descriptor.base_offset)
            {
                mismatched.push("first_offset");
            }
            if manifest.next_offset != scan.next_offset {
                mismatched.push("next_offset");
            }
            if manifest.content_bytes != scan.content_bytes {
                mismatched.push("content_bytes");
            }
            // A sealed segment publishes only committed bytes, so its high
            // watermark must sit exactly on the validated record frontier.
            if manifest.committed_high_watermark != scan.next_offset {
                mismatched.push("committed_high_watermark");
            }
            if manifest.producer_summary != scan.producer_summary {
                mismatched.push("producer_summary");
            }
            if manifest.chunk_size != header.config.chunk_size {
                mismatched.push("chunk_size");
            }
            if manifest.chunk_count != scan.leaves.len() as u64 {
                mismatched.push("chunk_count");
            }
            if manifest.chunk_tree_scheme != CHUNK_TREE_SCHEME_V1 {
                mismatched.push("chunk_tree_scheme");
            }
            if manifest.index_stride != header.config.index_stride {
                mismatched.push("index_stride");
            }
            push_field_consistency(checks, mismatched);
        }
        _ => {
            checks.push(CheckOutcome::fail(
                CHECK_MANIFEST_CONSISTENCY,
                "frame scan or manifest unavailable".to_owned(),
            ));
        }
    }

    let mut authenticated = false;
    if let Some(manifest) = &manifest {
        if let Some(statement) = &manifest.commit_statement {
            match commit_statement_core(&SegmentManifestV2 {
                commit_statement: None,
                ..manifest.clone()
            }) {
                Ok(expected) => {
                    let mut mismatched = Vec::new();
                    if statement.statement_version != expected.statement_version {
                        mismatched.push("statement_version");
                    }
                    if statement.segment_id != expected.segment_id {
                        mismatched.push("segment_id");
                    }
                    if statement.segment_generation != expected.segment_generation {
                        mismatched.push("segment_generation");
                    }
                    if statement.topic != expected.topic {
                        mismatched.push("topic");
                    }
                    if statement.topic_epoch != expected.topic_epoch {
                        mismatched.push("topic_epoch");
                    }
                    if statement.range_id != expected.range_id {
                        mismatched.push("range_id");
                    }
                    if statement.range_generation != expected.range_generation {
                        mismatched.push("range_generation");
                    }
                    if statement.base_offset != expected.base_offset {
                        mismatched.push("base_offset");
                    }
                    if statement.committed_high_watermark != expected.committed_high_watermark {
                        mismatched.push("committed_high_watermark");
                    }
                    if statement.content_bytes != expected.content_bytes {
                        mismatched.push("content_bytes");
                    }
                    if statement.chunk_tree_root != expected.chunk_tree_root {
                        mismatched.push("chunk_tree_root");
                    }
                    if mismatched.is_empty() {
                        checks.push(CheckOutcome::pass(
                            CHECK_STATEMENT_ECHO,
                            "statement restates the sealed manifest core".to_owned(),
                        ));
                    } else {
                        checks.push(CheckOutcome::fail(
                            CHECK_STATEMENT_ECHO,
                            format!("statement fields differ: {}", mismatched.join(", ")),
                        ));
                    }
                    if statement.manifest_core_digest == expected.manifest_core_digest {
                        checks.push(CheckOutcome::pass(
                            CHECK_STATEMENT_DIGEST,
                            expected.manifest_core_digest,
                        ));
                    } else {
                        checks.push(CheckOutcome::fail(
                            CHECK_STATEMENT_DIGEST,
                            format!(
                                "recomputed manifest core digest {}; statement carries {}",
                                expected.manifest_core_digest, statement.manifest_core_digest
                            ),
                        ));
                    }
                }
                Err(error) => {
                    checks.push(CheckOutcome::fail(CHECK_STATEMENT_ECHO, error.to_string()));
                    checks.push(CheckOutcome::fail(
                        CHECK_STATEMENT_DIGEST,
                        "manifest core digest could not be recomputed".to_owned(),
                    ));
                }
            }
            if statement.scheme == COMMIT_SCHEME_UNKEYED {
                match statement.verify(None) {
                    Ok(()) => checks.push(CheckOutcome::pass(
                        CHECK_STATEMENT_MAC,
                        "unkeyed digest recomputes".to_owned(),
                    )),
                    Err(error) => {
                        checks.push(CheckOutcome::fail(CHECK_STATEMENT_MAC, error.to_string()));
                    }
                }
            } else if statement.scheme == COMMIT_SCHEME_KEYED {
                match expectations.keyring.get(&statement.key_id) {
                    Some(key) => match statement.verify(Some(key)) {
                        Ok(()) => {
                            authenticated = true;
                            checks.push(CheckOutcome::pass(
                                CHECK_STATEMENT_MAC,
                                format!("keyed MAC verifies with key_id {:?}", statement.key_id),
                            ));
                        }
                        Err(error) => {
                            checks.push(CheckOutcome::fail(CHECK_STATEMENT_MAC, error.to_string()));
                        }
                    },
                    None => checks.push(CheckOutcome::pass(
                        CHECK_STATEMENT_MAC,
                        format!(
                            "skipped: keyring has no key for key_id {:?}; not authenticated",
                            statement.key_id
                        ),
                    )),
                }
            } else {
                checks.push(CheckOutcome::fail(
                    CHECK_STATEMENT_MAC,
                    format!("unknown commit statement scheme {:?}", statement.scheme),
                ));
            }
        }
    }

    let mut pinned = check_root_pin(scan, expectations, checks);
    if let Some(expected_digest) = &expectations.manifest_core_digest {
        match &manifest {
            Some(manifest) => {
                let recomputed = canonical_manifest_v2_bytes(&SegmentManifestV2 {
                    commit_statement: None,
                    ..manifest.clone()
                })
                .map(|bytes| blake3::hash(&bytes));
                match recomputed {
                    Ok(digest) if digest == *expected_digest => {
                        pinned = true;
                        checks.push(CheckOutcome::pass(
                            CHECK_MANIFEST_DIGEST_PIN,
                            digest.to_hex().to_string(),
                        ));
                    }
                    Ok(digest) => checks.push(CheckOutcome::fail(
                        CHECK_MANIFEST_DIGEST_PIN,
                        format!(
                            "recomputed manifest core digest {} does not match pinned {}",
                            digest.to_hex(),
                            expected_digest.to_hex()
                        ),
                    )),
                    Err(error) => checks.push(CheckOutcome::fail(
                        CHECK_MANIFEST_DIGEST_PIN,
                        error.to_string(),
                    )),
                }
            }
            None => checks.push(CheckOutcome::fail(
                CHECK_MANIFEST_DIGEST_PIN,
                "manifest unavailable".to_owned(),
            )),
        }
    }

    let achieved = if authenticated {
        VerifyLevel::Authenticated
    } else if pinned {
        VerifyLevel::RootPinned
    } else {
        VerifyLevel::SelfConsistent
    };
    let (record_count, content_bytes, chunk_count) = match (scan, &manifest) {
        (Some(scan), _) => (scan.records, scan.content_bytes, scan.leaves.len() as u64),
        (None, Some(manifest)) => (
            manifest.record_count,
            manifest.content_bytes,
            manifest.chunk_count,
        ),
        (None, None) => (0, 0, 0),
    };
    (achieved, record_count, content_bytes, chunk_count)
}

/// Read a manifest, requiring both a clean decode and canonical bytes; the
/// decoded manifest is still returned when only the canonical check failed so
/// later checks can report every field-level disagreement.
fn read_manifest_checked<M: serde::de::DeserializeOwned>(
    env: &Env,
    paths: &SegmentPaths,
    checks: &mut Vec<CheckOutcome>,
    canonical: impl Fn(&M) -> VtopLogResult<Vec<u8>>,
) -> Option<M> {
    let bytes = match env.storage.read(&paths.manifest) {
        Ok(bytes) => bytes,
        Err(error) => {
            checks.push(CheckOutcome::fail(
                CHECK_MANIFEST_CANONICAL,
                format!("cannot read manifest: {error}"),
            ));
            return None;
        }
    };
    match serde_json::from_slice::<M>(&bytes) {
        Ok(manifest) => {
            match canonical(&manifest) {
                Ok(expected) if expected == bytes => checks.push(CheckOutcome::pass(
                    CHECK_MANIFEST_CANONICAL,
                    "canonical VTOP JSON encoding".to_owned(),
                )),
                Ok(_) => checks.push(CheckOutcome::fail(
                    CHECK_MANIFEST_CANONICAL,
                    "manifest is not in canonical VTOP JSON encoding".to_owned(),
                )),
                Err(error) => checks.push(CheckOutcome::fail(
                    CHECK_MANIFEST_CANONICAL,
                    error.to_string(),
                )),
            }
            Some(manifest)
        }
        Err(error) => {
            checks.push(CheckOutcome::fail(
                CHECK_MANIFEST_CANONICAL,
                format!("cannot decode manifest: {error}"),
            ));
            None
        }
    }
}

fn push_field_consistency(checks: &mut Vec<CheckOutcome>, mismatched: Vec<&str>) {
    if mismatched.is_empty() {
        checks.push(CheckOutcome::pass(
            CHECK_MANIFEST_CONSISTENCY,
            "manifest agrees with the header and the frame scan".to_owned(),
        ));
    } else {
        checks.push(CheckOutcome::fail(
            CHECK_MANIFEST_CONSISTENCY,
            format!("manifest fields differ: {}", mismatched.join(", ")),
        ));
    }
}

fn check_root_pin(
    scan: Option<&ScanOutcome>,
    expectations: &VerifyExpectations,
    checks: &mut Vec<CheckOutcome>,
) -> bool {
    let Some(expected_root) = &expectations.chunk_tree_root else {
        return false;
    };
    match scan {
        // blake3::Hash equality is constant-time.
        Some(scan) if scan.root == *expected_root => {
            checks.push(CheckOutcome::pass(
                CHECK_ROOT_PIN,
                scan.root.to_hex().to_string(),
            ));
            true
        }
        Some(scan) => {
            checks.push(CheckOutcome::fail(
                CHECK_ROOT_PIN,
                format!(
                    "recomputed content root {} does not match pinned {}",
                    scan.root.to_hex(),
                    expected_root.to_hex()
                ),
            ));
            false
        }
        None => {
            checks.push(CheckOutcome::fail(
                CHECK_ROOT_PIN,
                "content root unavailable: frame scan failed".to_owned(),
            ));
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proof::verify_chunk;
    use crate::types::{
        Durability, LogRecord, SegmentConfig, SegmentConfigV2, SegmentDescriptor,
        SegmentDescriptorV2,
    };
    use crate::{ActiveSegment, AppendOutcome, RangeLineage};
    use std::fs;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn descriptor_v2() -> SegmentDescriptorV2 {
        SegmentDescriptorV2 {
            segment_id: Uuid::from_u128(0x51),
            topic: "audit.v1".to_owned(),
            topic_epoch: 3,
            lineage: RangeLineage::root(Uuid::from_u128(0x52)),
            base_offset: 40,
            segment_generation: 7,
            creation_node_id: Uuid::from_u128(0x53),
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

    fn record_v2(epoch: u64, sequence: u64, value: &[u8]) -> LogRecord {
        LogRecord {
            producer_id: Uuid::from_u128(0x54),
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

    fn wrong_key() -> SegmentCommitKey {
        SegmentCommitKey::from_hex(&"11".repeat(32)).unwrap()
    }

    /// Seal a small v2 bundle with the given key configuration and return the
    /// sealed segment path.
    fn seal_v2_bundle(directory: &Path, key: Option<&SegmentCommitKey>) -> PathBuf {
        let active = directory.join("bundle.active");
        let mut segment = ActiveSegment::create_v2(&active, descriptor_v2(), config_v2()).unwrap();
        segment
            .append_group(
                &[
                    record_v2(2, 0, b"alpha"),
                    record_v2(2, 1, b"beta"),
                    record_v2(3, 0, b"gamma"),
                ],
                Durability::Fsync,
            )
            .unwrap();
        drop(segment.seal_v2(key).unwrap());
        directory.join("bundle.segment")
    }

    fn seal_v1_bundle(directory: &Path) -> PathBuf {
        let active = directory.join("legacy.active");
        let descriptor = SegmentDescriptor {
            segment_id: Uuid::from_u128(0x61),
            topic: "events.v1".to_owned(),
            topic_epoch: 1,
            lineage: RangeLineage::root(Uuid::from_u128(0x62)),
            base_offset: 10,
        };
        let config = SegmentConfig {
            max_record_bytes: 1024,
            max_group_bytes: 4096,
            max_segment_bytes: 16 * 1024,
            max_segment_records: 100,
            index_stride: 2,
        };
        let mut segment = ActiveSegment::create(&active, descriptor, config).unwrap();
        let mut first = record_v2(0, 0, b"one");
        first.producer_epoch = 0;
        let mut second = record_v2(0, 1, b"two");
        second.producer_epoch = 0;
        segment
            .append_group(&[first, second], Durability::Fsync)
            .unwrap();
        drop(segment.seal().unwrap());
        directory.join("legacy.segment")
    }

    fn check(report: &VerifyReport, name: &str) -> CheckOutcome {
        report
            .checks
            .iter()
            .find(|check| check.name == name)
            .unwrap_or_else(|| panic!("missing check {name}"))
            .clone()
    }

    fn keyring(key_id: &str, key: SegmentCommitKey) -> BTreeMap<String, SegmentCommitKey> {
        BTreeMap::from([(key_id.to_owned(), key)])
    }

    fn manifest_of(sealed: &Path) -> SegmentManifestV2 {
        let manifest_path = sealed.with_file_name("bundle.manifest.json");
        serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap()
    }

    fn flip_byte(path: &Path, position: usize) {
        let mut bytes = fs::read(path).unwrap();
        bytes[position] ^= 0xff;
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn keyed_seal_verifies_at_every_level_with_the_right_key() {
        let directory = tempdir().unwrap();
        let sealed = seal_v2_bundle(directory.path(), Some(&commit_key()));
        let manifest = manifest_of(&sealed);
        let root = blake3::Hash::from_hex(&manifest.chunk_tree_root).unwrap();

        // The sealing key_id is the empty string; the keyring must accept it.
        let expectations = VerifyExpectations {
            chunk_tree_root: Some(root),
            keyring: keyring("", commit_key()),
            require: VerifyLevel::Authenticated,
            ..VerifyExpectations::default()
        };
        let report = verify_sealed_segment(&sealed, &expectations).unwrap();
        assert!(report.passed(), "{:?}", report.checks);
        assert_eq!(report.achieved, VerifyLevel::Authenticated);
        assert_eq!(report.format_version, FORMAT_VERSION_V2);
        assert_eq!(report.segment_id, descriptor_v2().segment_id);
        assert_eq!(report.record_count, 3);
        assert_eq!(report.chunk_count, 1);
        assert!(report.content_bytes > 0);
    }

    #[test]
    fn keyring_miss_caps_the_level_below_authenticated_without_failing_checks() {
        let directory = tempdir().unwrap();
        let sealed = seal_v2_bundle(directory.path(), Some(&commit_key()));

        let expectations = VerifyExpectations {
            keyring: keyring("other-key", commit_key()),
            require: VerifyLevel::Authenticated,
            ..VerifyExpectations::default()
        };
        let report = verify_sealed_segment(&sealed, &expectations).unwrap();
        assert_eq!(report.achieved, VerifyLevel::SelfConsistent);
        let mac = check(&report, CHECK_STATEMENT_MAC);
        assert!(mac.passed);
        assert!(mac.detail.contains("skipped"), "{}", mac.detail);
        assert!(!check(&report, CHECK_REQUIRED_LEVEL).passed);

        // The same miss at a lower requirement leaves the report clean.
        let relaxed = VerifyExpectations {
            keyring: keyring("other-key", commit_key()),
            ..VerifyExpectations::default()
        };
        let report = verify_sealed_segment(&sealed, &relaxed).unwrap();
        assert!(report.passed(), "{:?}", report.checks);
    }

    #[test]
    fn wrong_key_fails_the_mac_check_and_never_authenticates() {
        let directory = tempdir().unwrap();
        let sealed = seal_v2_bundle(directory.path(), Some(&commit_key()));

        let expectations = VerifyExpectations {
            keyring: keyring("", wrong_key()),
            ..VerifyExpectations::default()
        };
        let report = verify_sealed_segment(&sealed, &expectations).unwrap();
        assert_eq!(report.achieved, VerifyLevel::SelfConsistent);
        assert!(!check(&report, CHECK_STATEMENT_MAC).passed);
    }

    #[test]
    fn statementless_and_keyed_seals_verify_and_pin_to_root_pinned() {
        // `None` seals without any commit statement; `Some` attaches a keyed
        // statement that simply goes unverified when the keyring is empty.
        for key in [None, Some(commit_key())] {
            let directory = tempdir().unwrap();
            let sealed = seal_v2_bundle(directory.path(), key.as_ref());
            let manifest = manifest_of(&sealed);
            let root = blake3::Hash::from_hex(&manifest.chunk_tree_root).unwrap();

            let report = verify_sealed_segment(&sealed, &VerifyExpectations::default()).unwrap();
            assert!(report.passed(), "{:?}", report.checks);
            assert_eq!(report.achieved, VerifyLevel::SelfConsistent);

            let pinned = VerifyExpectations {
                chunk_tree_root: Some(root),
                require: VerifyLevel::RootPinned,
                ..VerifyExpectations::default()
            };
            let report = verify_sealed_segment(&sealed, &pinned).unwrap();
            assert!(report.passed(), "{:?}", report.checks);
            assert_eq!(report.achieved, VerifyLevel::RootPinned);
        }
    }

    #[test]
    fn unkeyed_statement_verifies_its_digest_but_cannot_authenticate() {
        let directory = tempdir().unwrap();
        let active = directory.path().join("bundle.active");
        let mut segment = ActiveSegment::create_v2(&active, descriptor_v2(), config_v2()).unwrap();
        segment
            .append(record_v2(1, 0, b"solo"), Durability::Fsync)
            .unwrap();
        // seal_v2(None) writes no statement; attach an unkeyed one by
        // resealing the manifest the way the seal path would with no key but
        // an explicit statement request is not exposed, so authenticate one
        // into the manifest manually via the manifest rewrite below.
        drop(segment.seal_v2(None).unwrap());
        let sealed = directory.path().join("bundle.segment");
        let manifest_path = directory.path().join("bundle.manifest.json");
        let mut manifest: SegmentManifestV2 =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        let mut statement = commit_statement_core(&manifest).unwrap();
        statement.authenticate(None).unwrap();
        manifest.commit_statement = Some(statement);
        fs::write(
            &manifest_path,
            canonical_manifest_v2_bytes(&manifest).unwrap(),
        )
        .unwrap();

        let expectations = VerifyExpectations {
            require: VerifyLevel::Authenticated,
            ..VerifyExpectations::default()
        };
        let report = verify_sealed_segment(&sealed, &expectations).unwrap();
        assert!(check(&report, CHECK_STATEMENT_MAC).passed);
        assert_eq!(report.achieved, VerifyLevel::SelfConsistent);
        assert!(!check(&report, CHECK_REQUIRED_LEVEL).passed);
    }

    #[test]
    fn manifest_core_digest_pin_reaches_root_pinned_and_rejects_mismatches() {
        let directory = tempdir().unwrap();
        let sealed = seal_v2_bundle(directory.path(), None);
        let manifest = manifest_of(&sealed);
        let digest = blake3::hash(&canonical_manifest_v2_bytes(&manifest).unwrap());

        let matching = VerifyExpectations {
            manifest_core_digest: Some(digest),
            require: VerifyLevel::RootPinned,
            ..VerifyExpectations::default()
        };
        let report = verify_sealed_segment(&sealed, &matching).unwrap();
        assert!(report.passed(), "{:?}", report.checks);
        assert_eq!(report.achieved, VerifyLevel::RootPinned);

        let mismatching = VerifyExpectations {
            manifest_core_digest: Some(blake3::hash(b"not the manifest")),
            ..VerifyExpectations::default()
        };
        let report = verify_sealed_segment(&sealed, &mismatching).unwrap();
        assert!(!check(&report, CHECK_MANIFEST_DIGEST_PIN).passed);
        assert_eq!(report.achieved, VerifyLevel::SelfConsistent);
    }

    #[test]
    fn wrong_root_pin_fails_only_the_pin_check() {
        let directory = tempdir().unwrap();
        let sealed = seal_v2_bundle(directory.path(), None);
        let expectations = VerifyExpectations {
            chunk_tree_root: Some(blake3::hash(b"not the root")),
            ..VerifyExpectations::default()
        };
        let report = verify_sealed_segment(&sealed, &expectations).unwrap();
        assert!(!check(&report, CHECK_ROOT_PIN).passed);
        assert!(check(&report, CHECK_FRAME_SCAN).passed);
        assert!(check(&report, CHECK_CONTENT_ROOT).passed);
        assert_eq!(report.achieved, VerifyLevel::SelfConsistent);
    }

    #[test]
    fn each_tamper_fails_its_own_check_and_is_reported_not_errored() {
        // Flipping a content byte breaks the frame checksum; everything the
        // scan feeds is reported as unavailable rather than early-returned.
        let directory = tempdir().unwrap();
        let sealed = seal_v2_bundle(directory.path(), Some(&commit_key()));
        let inside_last_frame = fs::read(&sealed).unwrap().len() - 10;
        flip_byte(&sealed, inside_last_frame);
        let report = verify_sealed_segment(&sealed, &VerifyExpectations::default()).unwrap();
        assert!(!check(&report, CHECK_FRAME_SCAN).passed);
        assert!(!check(&report, CHECK_CONTENT_ROOT).passed);
        assert!(check(&report, CHECK_MANIFEST_CANONICAL).passed);

        // A flipped sidecar leaf fails only the sidecar cross-check.
        let directory = tempdir().unwrap();
        let sealed = seal_v2_bundle(directory.path(), Some(&commit_key()));
        let chunks = directory.path().join("bundle.chunks");
        let sidecar_len = fs::read(&chunks).unwrap().len();
        flip_byte(&chunks, sidecar_len - 40);
        let report = verify_sealed_segment(
            &sealed,
            &VerifyExpectations {
                keyring: keyring("", commit_key()),
                ..VerifyExpectations::default()
            },
        )
        .unwrap();
        assert!(!check(&report, CHECK_CHUNK_SIDECAR).passed);
        assert!(check(&report, CHECK_FRAME_SCAN).passed);
        // The content and statement are intact, so authentication still
        // succeeds; the failing check is what condemns the bundle.
        assert_eq!(report.achieved, VerifyLevel::Authenticated);

        // A flipped manifest byte breaks the canonical encoding.
        let directory = tempdir().unwrap();
        let sealed = seal_v2_bundle(directory.path(), Some(&commit_key()));
        let manifest_path = directory.path().join("bundle.manifest.json");
        flip_byte(&manifest_path, 3);
        let report = verify_sealed_segment(&sealed, &VerifyExpectations::default()).unwrap();
        assert!(!check(&report, CHECK_MANIFEST_CANONICAL).passed);
        assert!(check(&report, CHECK_FRAME_SCAN).passed);

        // A forged MAC survives structural checks but fails verification.
        let directory = tempdir().unwrap();
        let sealed = seal_v2_bundle(directory.path(), Some(&commit_key()));
        let manifest_path = directory.path().join("bundle.manifest.json");
        let mut manifest: SegmentManifestV2 =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        manifest.commit_statement.as_mut().unwrap().mac = "00".repeat(32);
        fs::write(
            &manifest_path,
            canonical_manifest_v2_bytes(&manifest).unwrap(),
        )
        .unwrap();
        let report = verify_sealed_segment(
            &sealed,
            &VerifyExpectations {
                keyring: keyring("", commit_key()),
                ..VerifyExpectations::default()
            },
        )
        .unwrap();
        assert!(!check(&report, CHECK_STATEMENT_MAC).passed);
        assert!(check(&report, CHECK_STATEMENT_ECHO).passed);
        assert!(check(&report, CHECK_STATEMENT_DIGEST).passed);
        assert_eq!(report.achieved, VerifyLevel::SelfConsistent);
    }

    #[test]
    fn v1_segments_verify_and_top_out_at_root_pinned() {
        let directory = tempdir().unwrap();
        let sealed = seal_v1_bundle(directory.path());
        let manifest: SegmentManifest = serde_json::from_slice(
            &fs::read(directory.path().join("legacy.manifest.json")).unwrap(),
        )
        .unwrap();
        let linear_root = blake3::Hash::from_hex(&manifest.blake3_root).unwrap();

        let report = verify_sealed_segment(&sealed, &VerifyExpectations::default()).unwrap();
        assert!(report.passed(), "{:?}", report.checks);
        assert_eq!(report.format_version, FORMAT_VERSION);
        assert_eq!(report.record_count, 2);
        assert_eq!(report.chunk_count, 0);
        assert_eq!(report.achieved, VerifyLevel::SelfConsistent);

        // The root expectation pins the v1 linear digest.
        let pinned = VerifyExpectations {
            chunk_tree_root: Some(linear_root),
            require: VerifyLevel::RootPinned,
            ..VerifyExpectations::default()
        };
        let report = verify_sealed_segment(&sealed, &pinned).unwrap();
        assert!(report.passed(), "{:?}", report.checks);
        assert_eq!(report.achieved, VerifyLevel::RootPinned);

        // Authentication and manifest-core-digest pins are unreachable on v1.
        let unreachable = VerifyExpectations {
            chunk_tree_root: Some(linear_root),
            manifest_core_digest: Some(blake3::hash(b"anything")),
            require: VerifyLevel::Authenticated,
            ..VerifyExpectations::default()
        };
        let report = verify_sealed_segment(&sealed, &unreachable).unwrap();
        assert!(!check(&report, CHECK_MANIFEST_DIGEST_PIN).passed);
        assert!(!check(&report, CHECK_REQUIRED_LEVEL).passed);
        assert_eq!(report.achieved, VerifyLevel::RootPinned);
    }

    #[test]
    fn unreadable_or_headerless_segments_are_errors_not_reports() {
        let directory = tempdir().unwrap();
        assert!(matches!(
            verify_sealed_segment(
                &directory.path().join("missing.segment"),
                &VerifyExpectations::default()
            ),
            Err(LogError::Io { .. })
        ));
        let garbage = directory.path().join("garbage.segment");
        fs::write(&garbage, b"not a segment").unwrap();
        assert!(matches!(
            verify_sealed_segment(&garbage, &VerifyExpectations::default()),
            Err(LogError::Corrupt { .. })
        ));
        assert!(matches!(
            verify_sealed_segment(
                &directory.path().join("wrong-extension.active"),
                &VerifyExpectations::default()
            ),
            Err(LogError::InvalidDescriptor(_))
        ));
    }

    #[test]
    fn sealed_segment_past_the_producer_retry_window_still_verifies_self_consistent() {
        // The seal path derives producer summaries from the retained
        // PRODUCER_SEQUENCE_WINDOW; reconstructing 0..latest would disagree
        // with the sealed manifest and falsely report corruption.
        let directory = tempdir().unwrap();
        let active = directory.path().join("window.active");
        let config = SegmentConfigV2 {
            max_record_bytes: 256,
            max_group_bytes: 16 * 1024 * 1024,
            max_segment_bytes: 64 * 1024 * 1024,
            max_segment_records: 2 * PRODUCER_SEQUENCE_WINDOW,
            index_stride: 4096,
            chunk_size: 64 * 1024,
        };
        let mut segment = ActiveSegment::create_v2(&active, descriptor_v2(), config).unwrap();
        let producer = Uuid::from_u128(0x54);
        let total = PRODUCER_SEQUENCE_WINDOW + 10;
        let mut sequence = 0_u64;
        while sequence < total {
            let batch: Vec<LogRecord> = (sequence..(sequence + 4096).min(total))
                .map(|sequence| LogRecord {
                    producer_id: producer,
                    producer_epoch: 1,
                    sequence,
                    timestamp_millis: 1_700_000_000_000 + sequence as i64,
                    attributes: 0,
                    key: Vec::new(),
                    value: b"w".to_vec(),
                })
                .collect();
            sequence += batch.len() as u64;
            for outcome in segment.append_group(&batch, Durability::Fsync).unwrap() {
                assert!(matches!(outcome, AppendOutcome::Appended { .. }));
            }
        }
        drop(segment.seal_v2(None).unwrap());
        let sealed = directory.path().join("window.segment");
        let report = verify_sealed_segment(&sealed, &VerifyExpectations::default()).unwrap();
        assert!(report.passed(), "{:?}", report.checks);
        assert_eq!(report.achieved, VerifyLevel::SelfConsistent);
        assert!(check(&report, CHECK_MANIFEST_CONSISTENCY).passed);
        assert_eq!(report.record_count, total);
    }

    #[test]
    fn chunk_proofs_round_trip_across_chunk_boundaries_and_reject_tampering() {
        let directory = tempdir().unwrap();
        let active = directory.path().join("wide.active");
        let config = SegmentConfigV2 {
            max_record_bytes: 64 * 1024,
            max_group_bytes: 128 * 1024,
            max_segment_bytes: 1024 * 1024,
            max_segment_records: 100,
            index_stride: 2,
            chunk_size: 64 * 1024,
        };
        let mut segment = ActiveSegment::create_v2(&active, descriptor_v2(), config).unwrap();
        for sequence in 0..5_u64 {
            segment
                .append(
                    record_v2(1, sequence, &vec![sequence as u8; 40 * 1024]),
                    Durability::Fsync,
                )
                .unwrap();
        }
        drop(segment.seal_v2(None).unwrap());
        let sealed = directory.path().join("wide.segment");
        let manifest_root = blake3::Hash::from_hex(
            &serde_json::from_slice::<SegmentManifestV2>(
                &fs::read(directory.path().join("wide.manifest.json")).unwrap(),
            )
            .unwrap()
            .chunk_tree_root,
        )
        .unwrap();

        let (params, first_proof, _) = chunk_proof(&sealed, 0).unwrap();
        assert!(params.chunk_count >= 3, "{}", params.chunk_count);
        for index in 0..params.chunk_count {
            let (chunk_params, proof, chunk) = chunk_proof(&sealed, index).unwrap();
            assert_eq!(chunk_params, params);
            assert!(verify_chunk(&manifest_root, params, index, &chunk, &proof));

            let mut tampered = chunk.clone();
            tampered[0] ^= 0xff;
            assert!(!verify_chunk(
                &manifest_root,
                params,
                index,
                &tampered,
                &proof
            ));
        }
        // A proof only opens its own chunk.
        let (_, _, second_chunk) = chunk_proof(&sealed, 1).unwrap();
        assert!(!verify_chunk(
            &manifest_root,
            params,
            0,
            &second_chunk,
            &first_proof
        ));

        assert!(matches!(
            chunk_proof(&sealed, params.chunk_count),
            Err(LogError::InvalidDescriptor(_))
        ));
        let directory_v1 = tempdir().unwrap();
        let v1 = seal_v1_bundle(directory_v1.path());
        assert!(matches!(
            chunk_proof(&v1, 0),
            Err(LogError::InvalidDescriptor(_))
        ));
    }
}

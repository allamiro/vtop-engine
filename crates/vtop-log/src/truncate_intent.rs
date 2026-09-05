//! The durable record of a cross-segment truncation in flight.
//!
//! # Why this exists
//!
//! A cut below the tail deletes segment(s) and creates a replacement tail, and
//! neither ordering of those two is crash-safe on its own. Replacement-first
//! leaves two active segments, which discovery quarantines as
//! `MultipleActiveSegments`; delete-first leaves none, which nothing can
//! distinguish from a range whose tail was lost. Either way a crash strands a
//! range that will not reopen — the very failure truncation exists to repair.
//!
//! The fix is to make the INTENT durable before any file is touched. With the
//! intent on disk, an interrupted truncation is not an ambiguous layout: the
//! marker says exactly which segments were doomed and what was to replace
//! them, so the next open can FINISH the job instead of quarantining it.
//!
//! # The marker must survive the thing it protects
//!
//! It cannot live in a segment being deleted, so it is one file at the range
//! directory level, written atomically like every other sidecar. It also
//! embeds the producer frontier the replacement tail inherits, byte-for-byte,
//! rather than pointing at the doomed prefix's `.producers` sidecar — that
//! sidecar shares a filename stem with the doomed segment at the cut and is
//! deleted with it, so the marker is the only copy guaranteed to survive.
//! Without that frontier the replacement would begin empty and the next
//! append from a producer already in the retained prefix would be rejected as
//! `FirstSequence`, the exact failure `.producers` exists to prevent.
//!
//! # Determinism
//!
//! Encoding is byte-deterministic and checksummed. Recovery re-creates the
//! replacement from nothing but this file, so two runs of recovery — or a run
//! interrupted and repeated — must rebuild the identical segment.

use crate::producer_snapshot::ProducerSnapshot;
use crate::{
    KeyRange, LogError, ParentRange, RangeLineage, SegmentConfig, SegmentConfigV2,
    SegmentDescriptor, SegmentDescriptorV2, VtopLogResult,
};
use uuid::Uuid;

/// One marker per range directory, under a fixed name. The name is what lets
/// discovery and open find it without knowing anything else about the range.
pub(crate) const TRUNCATE_INTENT_FILE: &str = "range.truncate-intent";

const MAGIC: &[u8; 8] = b"VTOPTIN1";
/// A marker whose replacement is a v1 segment. Byte-for-byte the original
/// format: a marker written before this distinction existed decodes as this
/// version, which matters because a marker's whole purpose is to survive a
/// crash — including a crash the process upgrades across (#429).
const VERSION: u16 = 1;
/// A marker whose replacement is a v2 segment, carrying the descriptor
/// fields v1 has no room for: segment generation, creation node, creation
/// epoch, and the chunk size the config needs (#429). A separate version
/// rather than a flag inside v1's layout, so an older binary that finds one
/// refuses it whole — recovery deletes segments on this file's word, and a
/// partial understanding of it must not act.
const VERSION_V2: u16 = 2;

/// A segment the truncation removes: its identity, and the base offset that
/// names every file it owns via `segment_stem`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DoomedSegment {
    pub(crate) segment_id: Uuid,
    pub(crate) base_offset: u64,
}

/// The replacement tail's identity and limits, in the format of the tail it
/// replaces (#429). Keyed on the TAIL's format because the replacement is the
/// segment appends continue into: a v2 range must come back as a v2 segment
/// with its generation, creation node and creation epoch carried byte-exactly
/// — the same fields a rolled successor inherits unchanged — or the rebuilt
/// tail would have a different identity than the one the truncation replaced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReplacementTail {
    V1 {
        descriptor: SegmentDescriptor,
        config: SegmentConfig,
    },
    V2 {
        descriptor: SegmentDescriptorV2,
        config: SegmentConfigV2,
    },
}

impl ReplacementTail {
    pub(crate) fn base_offset(&self) -> u64 {
        match self {
            Self::V1 { descriptor, .. } => descriptor.base_offset,
            Self::V2 { descriptor, .. } => descriptor.base_offset,
        }
    }

    /// Test-only: the pins that stage a marker by hand assert the rebuilt
    /// tail carries the marker's identity, whichever arm holds it.
    #[cfg(test)]
    pub(crate) fn segment_id(&self) -> Uuid {
        match self {
            Self::V1 { descriptor, .. } => descriptor.segment_id,
            Self::V2 { descriptor, .. } => descriptor.segment_id,
        }
    }
}

/// Everything recovery needs to finish an interrupted cross-segment
/// truncation, with no other file consulted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TruncateIntent {
    /// First offset the range no longer holds once the truncation completes.
    pub(crate) target_offset: u64,
    /// The replacement tail's full identity and limits, inherited from the
    /// tail it replaces exactly as a rolled successor inherits them. Its
    /// `base_offset` must equal `target_offset`; the encoding stores the
    /// offset once and rebuilds the descriptor from it, so the two cannot
    /// drift apart on disk.
    pub(crate) replacement: ReplacementTail,
    /// Segments to delete, in ascending base-offset order. The first begins
    /// at the cut; the last is the old tail.
    pub(crate) doomed: Vec<DoomedSegment>,
    /// The producer frontier of the retained prefix, embedded whole because
    /// the sidecar holding it on disk is deleted with the doomed segment that
    /// owns it.
    pub(crate) inherited: ProducerSnapshot,
}

impl TruncateIntent {
    pub(crate) fn encode(&self) -> VtopLogResult<Vec<u8>> {
        // The descriptor's base offset is not stored separately, so a value
        // that disagrees with the target would be silently "repaired" on the
        // way back in. Refuse to write it instead.
        if self.replacement.base_offset() != self.target_offset {
            return Err(LogError::InvalidConfig(format!(
                "truncation intent replacement begins at {} but the cut is at {}",
                self.replacement.base_offset(),
                self.target_offset
            )));
        }
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(
            &match self.replacement {
                ReplacementTail::V1 { .. } => VERSION,
                ReplacementTail::V2 { .. } => VERSION_V2,
            }
            .to_be_bytes(),
        );
        out.extend_from_slice(&self.target_offset.to_be_bytes());

        // The identity core is laid out identically in both versions — the
        // version says what FOLLOWS the lineage, so a v1 marker's bytes stay
        // exactly what every earlier binary wrote and read.
        let (segment_id, topic, topic_epoch, lineage) = match &self.replacement {
            ReplacementTail::V1 { descriptor, .. } => (
                descriptor.segment_id,
                descriptor.topic.as_str(),
                descriptor.topic_epoch,
                &descriptor.lineage,
            ),
            ReplacementTail::V2 { descriptor, .. } => (
                descriptor.segment_id,
                descriptor.topic.as_str(),
                descriptor.topic_epoch,
                &descriptor.lineage,
            ),
        };
        out.extend_from_slice(segment_id.as_bytes());
        let topic = topic.as_bytes();
        // Topics are capped at 249 bytes by descriptor validation; u16 is
        // checked anyway so a hand-built descriptor cannot truncate silently.
        let topic_len = u16::try_from(topic.len()).map_err(|_| {
            LogError::InvalidConfig("truncation intent topic exceeds u16 length".to_owned())
        })?;
        out.extend_from_slice(&topic_len.to_be_bytes());
        out.extend_from_slice(topic);
        out.extend_from_slice(&topic_epoch.to_be_bytes());
        out.extend_from_slice(lineage.range_id.as_bytes());
        out.extend_from_slice(&lineage.generation.to_be_bytes());
        out.extend_from_slice(&lineage.key_range.prefix.to_be_bytes());
        out.push(lineage.key_range.prefix_bits);
        out.extend_from_slice(&(lineage.parents.len() as u32).to_be_bytes());
        for parent in &lineage.parents {
            out.extend_from_slice(parent.range_id.as_bytes());
            out.extend_from_slice(&parent.generation.to_be_bytes());
            out.extend_from_slice(&parent.key_range.prefix.to_be_bytes());
            out.push(parent.key_range.prefix_bits);
        }

        match &self.replacement {
            ReplacementTail::V1 { config, .. } => {
                out.extend_from_slice(&config.max_record_bytes.to_be_bytes());
                out.extend_from_slice(&config.max_group_bytes.to_be_bytes());
                out.extend_from_slice(&config.max_segment_bytes.to_be_bytes());
                out.extend_from_slice(&config.max_segment_records.to_be_bytes());
                out.extend_from_slice(&config.index_stride.to_be_bytes());
            }
            ReplacementTail::V2 { descriptor, config } => {
                // The fields v1 has no room for (#429): the identity a
                // rolled successor inherits unchanged, and the chunk size
                // without which a v2 header cannot be written.
                out.extend_from_slice(&descriptor.segment_generation.to_be_bytes());
                out.extend_from_slice(descriptor.creation_node_id.as_bytes());
                out.extend_from_slice(&descriptor.creation_fencing_epoch.to_be_bytes());
                out.extend_from_slice(&config.max_record_bytes.to_be_bytes());
                out.extend_from_slice(&config.max_group_bytes.to_be_bytes());
                out.extend_from_slice(&config.max_segment_bytes.to_be_bytes());
                out.extend_from_slice(&config.max_segment_records.to_be_bytes());
                out.extend_from_slice(&config.index_stride.to_be_bytes());
                out.extend_from_slice(&config.chunk_size.to_be_bytes());
            }
        }

        out.extend_from_slice(&(self.doomed.len() as u32).to_be_bytes());
        for doomed in &self.doomed {
            out.extend_from_slice(doomed.segment_id.as_bytes());
            out.extend_from_slice(&doomed.base_offset.to_be_bytes());
        }

        let frontier = self.inherited.encode()?;
        out.extend_from_slice(&(frontier.len() as u32).to_be_bytes());
        out.extend_from_slice(&frontier);

        let checksum = blake3::hash(&out);
        out.extend_from_slice(checksum.as_bytes());
        Ok(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> VtopLogResult<Self> {
        // Checksum before structure: recovery acts on this file by deleting
        // segments, so nothing below may run on bytes that were not written
        // whole by this code.
        if bytes.len() < MAGIC.len() + 2 + 32 {
            return Err(corrupt("too short to hold its own checksum"));
        }
        let checksum_start = bytes.len() - 32;
        if blake3::hash(&bytes[..checksum_start]).as_bytes() != &bytes[checksum_start..] {
            return Err(corrupt("checksum mismatch"));
        }
        let mut reader = Reader {
            bytes: &bytes[..checksum_start],
            at: 0,
        };
        if reader.take(8)? != MAGIC {
            return Err(corrupt("bad magic"));
        }
        let version = reader.u16()?;
        if version != VERSION && version != VERSION_V2 {
            return Err(corrupt(&format!("unsupported version {version}")));
        }
        let target_offset = reader.u64()?;

        let segment_id = reader.uuid()?;
        let topic_len = reader.u16()? as usize;
        let topic = String::from_utf8(reader.take(topic_len)?.to_vec())
            .map_err(|_| corrupt("topic is not UTF-8"))?;
        let topic_epoch = reader.u64()?;
        let range_id = reader.uuid()?;
        let generation = reader.u64()?;
        let key_range = KeyRange {
            prefix: reader.u64()?,
            prefix_bits: reader.u8()?,
        };
        let parent_count = reader.u32()? as usize;
        let mut parents = Vec::new();
        for _ in 0..parent_count {
            parents.push(ParentRange {
                range_id: reader.uuid()?,
                generation: reader.u64()?,
                key_range: KeyRange {
                    prefix: reader.u64()?,
                    prefix_bits: reader.u8()?,
                },
            });
        }

        let lineage = RangeLineage {
            range_id,
            generation,
            key_range,
            parents,
        };
        let replacement = if version == VERSION_V2 {
            let segment_generation = reader.u64()?;
            let creation_node_id = reader.uuid()?;
            let creation_fencing_epoch = reader.u64()?;
            ReplacementTail::V2 {
                descriptor: SegmentDescriptorV2 {
                    segment_id,
                    topic,
                    topic_epoch,
                    lineage,
                    base_offset: target_offset,
                    segment_generation,
                    creation_node_id,
                    creation_fencing_epoch,
                },
                config: SegmentConfigV2 {
                    max_record_bytes: reader.u32()?,
                    max_group_bytes: reader.u64()?,
                    max_segment_bytes: reader.u64()?,
                    max_segment_records: reader.u64()?,
                    index_stride: reader.u32()?,
                    chunk_size: reader.u32()?,
                },
            }
        } else {
            ReplacementTail::V1 {
                descriptor: SegmentDescriptor {
                    segment_id,
                    topic,
                    topic_epoch,
                    lineage,
                    base_offset: target_offset,
                },
                config: SegmentConfig {
                    max_record_bytes: reader.u32()?,
                    max_group_bytes: reader.u64()?,
                    max_segment_bytes: reader.u64()?,
                    max_segment_records: reader.u64()?,
                    index_stride: reader.u32()?,
                },
            }
        };

        let doomed_count = reader.u32()? as usize;
        let mut doomed: Vec<DoomedSegment> = Vec::new();
        for _ in 0..doomed_count {
            let entry = DoomedSegment {
                segment_id: reader.uuid()?,
                base_offset: reader.u64()?,
            };
            // Strictly ascending. Base offsets name the files recovery
            // deletes, and a repeated or rewinding entry means the writer and
            // reader disagree about which segments those are.
            if doomed
                .last()
                .is_some_and(|last| entry.base_offset <= last.base_offset)
            {
                return Err(corrupt(&format!(
                    "doomed segments do not ascend at base offset {}",
                    entry.base_offset
                )));
            }
            doomed.push(entry);
        }
        // The doomed list is what makes the truncation a truncation: an empty
        // one describes work with nothing to remove, which the writer cannot
        // have produced, and the cut must be the first doomed segment's base
        // or recovery would delete records below the offset it was told to
        // keep.
        let Some(first) = doomed.first() else {
            return Err(corrupt("no doomed segments"));
        };
        if first.base_offset != target_offset {
            return Err(corrupt(&format!(
                "first doomed segment begins at {} but the cut is at {target_offset}",
                first.base_offset
            )));
        }

        let frontier_len = reader.u32()? as usize;
        let inherited = ProducerSnapshot::decode(reader.take(frontier_len)?)?;
        if !reader.is_finished() {
            return Err(corrupt("trailing bytes after the inherited frontier"));
        }

        Ok(Self {
            target_offset,
            replacement,
            doomed,
            inherited,
        })
    }
}

fn corrupt(reason: &str) -> LogError {
    LogError::Corrupt {
        position: 0,
        reason: format!("truncation intent: {reason}"),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> VtopLogResult<&'a [u8]> {
        let end = self
            .at
            .checked_add(count)
            .ok_or_else(|| corrupt("length overflows"))?;
        if end > self.bytes.len() {
            return Err(corrupt("ends mid-field"));
        }
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self) -> VtopLogResult<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> VtopLogResult<u16> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("fixed width"),
        ))
    }

    fn u32(&mut self) -> VtopLogResult<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("fixed width"),
        ))
    }

    fn u64(&mut self) -> VtopLogResult<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("fixed width"),
        ))
    }

    fn uuid(&mut self) -> VtopLogResult<Uuid> {
        let bytes: [u8; 16] = self.take(16)?.try_into().expect("fixed width");
        Ok(Uuid::from_bytes(bytes))
    }

    fn is_finished(&self) -> bool {
        self.at == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::producer_snapshot::{SnapshotEntry, SnapshotSeen};
    use std::collections::BTreeMap;

    fn frontier() -> ProducerSnapshot {
        let mut producers = BTreeMap::new();
        producers.insert(
            (Uuid::from_u128(9), 0),
            SnapshotEntry {
                latest_sequence: 15,
                first_sequence: 0,
                record_count: 16,
                seen: vec![SnapshotSeen {
                    sequence: 15,
                    offset: 15,
                    content_hash: [7; 32],
                }],
            },
        );
        let mut epochs = BTreeMap::new();
        epochs.insert(Uuid::from_u128(9), 0);
        ProducerSnapshot { producers, epochs }
    }

    fn lineage() -> RangeLineage {
        RangeLineage {
            range_id: Uuid::from_u128(100),
            generation: 1,
            key_range: KeyRange {
                prefix: 0,
                prefix_bits: 1,
            },
            parents: vec![crate::ParentRange {
                range_id: Uuid::from_u128(99),
                generation: 0,
                key_range: KeyRange::full(),
            }],
        }
    }

    fn intent() -> TruncateIntent {
        TruncateIntent {
            target_offset: 16,
            replacement: ReplacementTail::V1 {
                descriptor: SegmentDescriptor {
                    segment_id: Uuid::from_u128(77),
                    topic: "events.v1".to_owned(),
                    topic_epoch: 7,
                    lineage: lineage(),
                    base_offset: 16,
                },
                config: SegmentConfig {
                    max_record_bytes: 256,
                    max_group_bytes: 512,
                    max_segment_bytes: 512,
                    max_segment_records: 100,
                    index_stride: 2,
                },
            },
            doomed: vec![
                DoomedSegment {
                    segment_id: Uuid::from_u128(3),
                    base_offset: 16,
                },
                DoomedSegment {
                    segment_id: Uuid::from_u128(4),
                    base_offset: 32,
                },
            ],
            inherited: frontier(),
        }
    }

    fn v2_intent() -> TruncateIntent {
        TruncateIntent {
            replacement: ReplacementTail::V2 {
                descriptor: SegmentDescriptorV2 {
                    segment_id: Uuid::from_u128(77),
                    topic: "events.v1".to_owned(),
                    topic_epoch: 7,
                    lineage: lineage(),
                    base_offset: 16,
                    segment_generation: 4,
                    creation_node_id: Uuid::from_u128(0xA1),
                    creation_fencing_epoch: 9,
                },
                config: SegmentConfigV2 {
                    max_record_bytes: 256,
                    max_group_bytes: 512,
                    max_segment_bytes: 512,
                    max_segment_records: 100,
                    index_stride: 2,
                    chunk_size: 128,
                },
            },
            ..intent()
        }
    }

    #[test]
    fn an_intent_round_trips() {
        let original = intent();
        let decoded = TruncateIntent::decode(&original.encode().unwrap()).unwrap();
        assert_eq!(decoded, original);
    }

    /// A v2 replacement carries the fields v1 has no room for — segment
    /// generation, creation node, creation epoch, chunk size — and every one
    /// must survive the round trip byte-exactly: recovery re-creates the
    /// replacement from these bytes alone, and a v2 tail rebuilt with a
    /// different identity than the one it replaced is the wrong-identity
    /// failure the old refusal existed to prevent (#429).
    #[test]
    fn a_v2_intent_round_trips_with_its_identity_fields() {
        let original = v2_intent();
        let decoded = TruncateIntent::decode(&original.encode().unwrap()).unwrap();
        assert_eq!(decoded, original);
    }

    /// Recovery rebuilds the replacement from nothing but these bytes, so the
    /// same intent must always produce the same bytes.
    #[test]
    fn encoding_is_deterministic() {
        assert_eq!(intent().encode().unwrap(), intent().encode().unwrap());
        assert_eq!(v2_intent().encode().unwrap(), v2_intent().encode().unwrap());
    }

    /// A v1 replacement still writes format version 1 — byte-for-byte the
    /// pre-#429 marker. A marker's purpose is to survive a crash, including
    /// one the process is UPGRADED across: an older binary must be able to
    /// finish a v1 truncation this binary staged, and this binary must
    /// finish one an older binary staged, which both reduce to the v1 bytes
    /// never changing. The v2 marker takes a new version for the inverse
    /// reason: an older binary that cannot understand it must refuse it
    /// whole rather than delete segments on a partial reading.
    #[test]
    fn a_v1_marker_keeps_the_original_version_and_a_v2_marker_declares_itself() {
        let v1 = intent().encode().unwrap();
        assert_eq!(&v1[8..10], &1u16.to_be_bytes(), "v1 markers must stay v1");
        let v2 = v2_intent().encode().unwrap();
        assert_eq!(
            &v2[8..10],
            &2u16.to_be_bytes(),
            "a v2 marker must announce a version older binaries refuse"
        );
    }

    /// An intent with an empty frontier is the cut-at-the-range-base case and
    /// must round-trip rather than being a special case.
    #[test]
    fn an_empty_frontier_round_trips() {
        let mut original = intent();
        original.inherited = ProducerSnapshot::default();
        let decoded = TruncateIntent::decode(&original.encode().unwrap()).unwrap();
        assert_eq!(decoded, original);
    }

    /// Every single-byte flip must be refused: this file authorizes deleting
    /// segments, and the checksum is what stands between a damaged marker and
    /// recovery deleting the wrong ones.
    #[test]
    fn every_single_byte_flip_is_refused() {
        for pristine in [intent().encode().unwrap(), v2_intent().encode().unwrap()] {
            for index in 0..pristine.len() {
                let mut damaged = pristine.clone();
                damaged[index] ^= 0xff;
                assert!(
                    matches!(
                        TruncateIntent::decode(&damaged),
                        Err(LogError::Corrupt { .. })
                    ),
                    "flip at byte {index} was accepted"
                );
            }
        }
    }

    /// A marker cut short at any byte is an incomplete write, never a shorter
    /// valid intent.
    #[test]
    fn every_truncated_prefix_is_refused() {
        let pristine = intent().encode().unwrap();
        for length in 0..pristine.len() {
            assert!(
                matches!(
                    TruncateIntent::decode(&pristine[..length]),
                    Err(LogError::Corrupt { .. })
                ),
                "prefix of {length} bytes was accepted"
            );
        }
    }

    /// Trailing bytes mean the writer and reader disagree about the format,
    /// and a partial understanding of a file that deletes segments must not
    /// be treated as a whole one.
    #[test]
    fn trailing_bytes_are_refused() {
        let mut bytes = intent().encode().unwrap();
        // Re-checksum so only the trailing byte is at fault.
        bytes.truncate(bytes.len() - 32);
        bytes.push(0);
        let checksum = blake3::hash(&bytes);
        bytes.extend_from_slice(checksum.as_bytes());
        assert!(matches!(
            TruncateIntent::decode(&bytes),
            Err(LogError::Corrupt { .. })
        ));
    }

    /// A truncation with nothing to remove cannot have been written by the
    /// live path, so recovery must not act on it.
    #[test]
    fn an_intent_with_no_doomed_segments_is_refused() {
        let mut broken = intent();
        broken.doomed.clear();
        assert!(matches!(
            TruncateIntent::decode(&broken.encode().unwrap()),
            Err(LogError::Corrupt { .. })
        ));
    }

    /// The doomed list names the files recovery deletes; an order that repeats
    /// or rewinds means writer and reader disagree about which those are.
    #[test]
    fn doomed_segments_that_do_not_ascend_are_refused() {
        let mut broken = intent();
        broken.doomed[1].base_offset = broken.doomed[0].base_offset;
        assert!(matches!(
            TruncateIntent::decode(&broken.encode().unwrap()),
            Err(LogError::Corrupt { .. })
        ));
    }

    /// The first doomed segment must begin AT the cut, or recovery would
    /// delete records below the offset it was told to keep.
    #[test]
    fn a_first_doomed_segment_off_the_cut_is_refused() {
        let mut broken = intent();
        broken.doomed[0].base_offset = broken.target_offset + 1;
        broken.doomed[1].base_offset = broken.target_offset + 2;
        assert!(matches!(
            TruncateIntent::decode(&broken.encode().unwrap()),
            Err(LogError::Corrupt { .. })
        ));
    }

    /// The replacement's base offset is derived from the cut on decode, so an
    /// intent where they disagree cannot be represented and must be refused
    /// on the way OUT rather than silently repaired on the way back in.
    #[test]
    fn a_replacement_off_the_cut_is_refused_at_encode() {
        let mut broken = intent();
        let ReplacementTail::V1 { descriptor, .. } = &mut broken.replacement else {
            unreachable!("the fixture is v1");
        };
        descriptor.base_offset = 99;
        assert!(matches!(broken.encode(), Err(LogError::InvalidConfig(_))));

        let mut broken = v2_intent();
        let ReplacementTail::V2 { descriptor, .. } = &mut broken.replacement else {
            unreachable!("the fixture is v2");
        };
        descriptor.base_offset = 99;
        assert!(matches!(broken.encode(), Err(LogError::InvalidConfig(_))));
    }
}

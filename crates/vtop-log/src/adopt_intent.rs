//! The durable record of a sealed-segment adoption in flight (#270).
//!
//! # Why this exists
//!
//! Adopting a transferred prefix means renaming received sealed segments into
//! the live range directory and replacing (or sealing) the tail they extend —
//! and no ordering of those file operations is crash-safe on its own. A crash
//! mid-move leaves sidecars without a primary, which discovery quarantines as
//! `OrphanSidecars`; deleting the tail first leaves a range with no active
//! segment, which `SegmentSet::open_in` refuses because it cannot tell an
//! interrupted adoption from a lost tail. This is the same shape cross-segment
//! truncation had (#276), and it gets the same fix: make the INTENT durable
//! before any file is touched, and let the next open FINISH the job instead of
//! quarantining it.
//!
//! # The staging directory is part of the contract
//!
//! Recovery must find the received segments without a running process to tell
//! it where they are, so adoption always stages in a fixed subdirectory of the
//! range (`adopt/`, see [`crate::ADOPT_STAGING_DIR`]) rather than a
//! caller-chosen path a marker would have to embed — a path that may not even
//! exist on the next boot of a relocated data directory. A subdirectory also
//! guarantees the renames never cross a filesystem.
//!
//! # The marker embeds the frontier, byte-for-byte
//!
//! The replacement tail begins where the adopted prefix ends, so it must
//! inherit that prefix's producer frontier or the next append from a producer
//! already mid-stream would be rejected as `FirstSequence` — the exact failure
//! `.producers` exists to prevent, and the exact failure a follower repaired
//! by adoption would hit on its first post-repair append. The frontier is
//! embedded whole, as the truncation marker embeds its own, so recovery
//! depends on nothing but this file and the staged bytes.
//!
//! # Determinism
//!
//! Encoding is byte-deterministic and checksummed. Recovery re-creates the
//! replacement tail from nothing but this file, so an interrupted recovery
//! repeated must rebuild the identical segment.

use crate::producer_snapshot::ProducerSnapshot;
use crate::{
    KeyRange, LogError, ParentRange, RangeLineage, SegmentConfig, SegmentDescriptor, VtopLogResult,
};
use uuid::Uuid;

/// One marker per range directory, under a fixed name, exactly like the
/// truncation marker: the name is what lets discovery and open find it
/// without knowing anything else about the range.
pub(crate) const ADOPT_INTENT_FILE: &str = "range.adopt-intent";

const MAGIC: &[u8; 8] = b"VTOPAIN1";
const VERSION: u16 = 1;

/// One received sealed segment to move from the staging subdirectory: its
/// identity, and the base offset that names every file it owns via
/// `segment_stem`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AdoptedSegment {
    pub(crate) segment_id: Uuid,
    pub(crate) base_offset: u64,
}

/// What happens to the tail the adopted prefix extends.
///
/// Two dispositions because the tail can stand in two relations to the
/// adoption point. An EMPTY tail beginning exactly where the first adopted
/// segment begins holds nothing and is replaced — its files share a stem with
/// the incoming segment and would otherwise collide as `ConflictingPrimaryFiles`.
/// A NON-EMPTY tail ending exactly where the first adopted segment begins is
/// real history and is sealed in place, becoming the last local sealed segment
/// before the adopted run. Anything else — overlap, gap — was refused before
/// the marker was written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TailDisposition {
    Replace { base_offset: u64 },
    Seal { base_offset: u64 },
}

/// Everything recovery needs to finish an interrupted adoption, with nothing
/// consulted but this file and the staged segments it names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AdoptIntent {
    /// First offset the range's NEW tail holds: the end of the adopted run.
    pub(crate) target_offset: u64,
    /// The replacement tail's full identity. Its `base_offset` must equal
    /// `target_offset`; the encoding stores the offset once and rebuilds the
    /// descriptor from it, so the two cannot drift apart on disk.
    pub(crate) replacement: SegmentDescriptor,
    /// The limits the replacement is created with, inherited from the tail it
    /// follows exactly as a rolled successor inherits them.
    pub(crate) config: SegmentConfig,
    /// What to do with the pre-adoption tail.
    pub(crate) tail: TailDisposition,
    /// Segments to move in from the staging subdirectory, ascending.
    pub(crate) adopted: Vec<AdoptedSegment>,
    /// The producer frontier at `target_offset`, embedded whole because the
    /// replacement must never exist without the frontier that makes it
    /// readable.
    pub(crate) inherited: ProducerSnapshot,
}

impl AdoptIntent {
    fn validate(&self) -> VtopLogResult<()> {
        if self.replacement.base_offset != self.target_offset {
            return Err(LogError::InvalidConfig(format!(
                "adoption intent replacement begins at {} but the adopted run ends at {}",
                self.replacement.base_offset, self.target_offset
            )));
        }
        let Some(first) = self.adopted.first() else {
            // An adoption with nothing to adopt cannot have been written by
            // the live path, and recovery acting on one would delete a tail
            // for nothing.
            return Err(corrupt("no adopted segments"));
        };
        for pair in self.adopted.windows(2) {
            // Strictly ascending. Base offsets name the files recovery moves,
            // and a repeated or rewinding entry means the writer and reader
            // disagree about which those are.
            if pair[1].base_offset <= pair[0].base_offset {
                return Err(corrupt(&format!(
                    "adopted segments do not ascend at base offset {}",
                    pair[1].base_offset
                )));
            }
        }
        let last = self.adopted.last().expect("first exists above");
        if last.base_offset >= self.target_offset {
            return Err(corrupt(&format!(
                "last adopted segment begins at {} but the replacement begins at {}",
                last.base_offset, self.target_offset
            )));
        }
        match self.tail {
            // A replaced tail is empty and shares its stem with the first
            // adopted segment; recovery deletes its files and expects the
            // adopted ones at the same names, so the two MUST agree.
            TailDisposition::Replace { base_offset } => {
                if base_offset != first.base_offset {
                    return Err(corrupt(&format!(
                        "replaced tail begins at {base_offset} but the first adopted segment \
                         begins at {}",
                        first.base_offset
                    )));
                }
            }
            // A sealed tail is real history BELOW the adopted run; a seal at
            // or above the first adopted base would collide with it.
            TailDisposition::Seal { base_offset } => {
                if base_offset >= first.base_offset {
                    return Err(corrupt(&format!(
                        "sealed tail begins at {base_offset}, not below the first adopted \
                         segment at {}",
                        first.base_offset
                    )));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn encode(&self) -> VtopLogResult<Vec<u8>> {
        // Refused on the way OUT rather than silently repaired on the way
        // back in, exactly as the truncation marker refuses.
        self.validate()?;
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_be_bytes());
        out.extend_from_slice(&self.target_offset.to_be_bytes());

        out.extend_from_slice(self.replacement.segment_id.as_bytes());
        let topic = self.replacement.topic.as_bytes();
        let topic_len = u16::try_from(topic.len()).map_err(|_| {
            LogError::InvalidConfig("adoption intent topic exceeds u16 length".to_owned())
        })?;
        out.extend_from_slice(&topic_len.to_be_bytes());
        out.extend_from_slice(topic);
        out.extend_from_slice(&self.replacement.topic_epoch.to_be_bytes());
        out.extend_from_slice(self.replacement.lineage.range_id.as_bytes());
        out.extend_from_slice(&self.replacement.lineage.generation.to_be_bytes());
        out.extend_from_slice(&self.replacement.lineage.key_range.prefix.to_be_bytes());
        out.push(self.replacement.lineage.key_range.prefix_bits);
        out.extend_from_slice(&(self.replacement.lineage.parents.len() as u32).to_be_bytes());
        for parent in &self.replacement.lineage.parents {
            out.extend_from_slice(parent.range_id.as_bytes());
            out.extend_from_slice(&parent.generation.to_be_bytes());
            out.extend_from_slice(&parent.key_range.prefix.to_be_bytes());
            out.push(parent.key_range.prefix_bits);
        }

        out.extend_from_slice(&self.config.max_record_bytes.to_be_bytes());
        out.extend_from_slice(&self.config.max_group_bytes.to_be_bytes());
        out.extend_from_slice(&self.config.max_segment_bytes.to_be_bytes());
        out.extend_from_slice(&self.config.max_segment_records.to_be_bytes());
        out.extend_from_slice(&self.config.index_stride.to_be_bytes());

        let (tag, tail_base): (u8, u64) = match self.tail {
            TailDisposition::Replace { base_offset } => (0, base_offset),
            TailDisposition::Seal { base_offset } => (1, base_offset),
        };
        out.push(tag);
        out.extend_from_slice(&tail_base.to_be_bytes());

        out.extend_from_slice(&(self.adopted.len() as u32).to_be_bytes());
        for adopted in &self.adopted {
            out.extend_from_slice(adopted.segment_id.as_bytes());
            out.extend_from_slice(&adopted.base_offset.to_be_bytes());
        }

        let frontier = self.inherited.encode()?;
        out.extend_from_slice(&(frontier.len() as u32).to_be_bytes());
        out.extend_from_slice(&frontier);

        let checksum = blake3::hash(&out);
        out.extend_from_slice(checksum.as_bytes());
        Ok(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> VtopLogResult<Self> {
        // Checksum before structure: recovery acts on this file by deleting a
        // tail and moving segments, so nothing below may run on bytes that
        // were not written whole by this code.
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
        if version != VERSION {
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

        let config = SegmentConfig {
            max_record_bytes: reader.u32()?,
            max_group_bytes: reader.u64()?,
            max_segment_bytes: reader.u64()?,
            max_segment_records: reader.u64()?,
            index_stride: reader.u32()?,
        };

        let tail_tag = reader.u8()?;
        let tail_base = reader.u64()?;
        let tail = match tail_tag {
            0 => TailDisposition::Replace {
                base_offset: tail_base,
            },
            1 => TailDisposition::Seal {
                base_offset: tail_base,
            },
            other => return Err(corrupt(&format!("unknown tail disposition {other}"))),
        };

        let adopted_count = reader.u32()? as usize;
        let mut adopted: Vec<AdoptedSegment> = Vec::new();
        for _ in 0..adopted_count {
            adopted.push(AdoptedSegment {
                segment_id: reader.uuid()?,
                base_offset: reader.u64()?,
            });
        }

        let frontier_len = reader.u32()? as usize;
        let inherited = ProducerSnapshot::decode(reader.take(frontier_len)?)?;
        if !reader.is_finished() {
            return Err(corrupt("trailing bytes after the inherited frontier"));
        }

        let intent = Self {
            target_offset,
            replacement: SegmentDescriptor {
                segment_id,
                topic,
                topic_epoch,
                lineage: RangeLineage {
                    range_id,
                    generation,
                    key_range,
                    parents,
                },
                base_offset: target_offset,
            },
            config,
            tail,
            adopted,
            inherited,
        };
        intent.validate()?;
        Ok(intent)
    }
}

fn corrupt(reason: &str) -> LogError {
    LogError::Corrupt {
        position: 0,
        reason: format!("adoption intent: {reason}"),
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
                latest_sequence: 47,
                first_sequence: 0,
                record_count: 48,
                seen: vec![SnapshotSeen {
                    sequence: 47,
                    offset: 47,
                    content_hash: [7; 32],
                }],
            },
        );
        let mut epochs = BTreeMap::new();
        epochs.insert(Uuid::from_u128(9), 0);
        ProducerSnapshot { producers, epochs }
    }

    fn intent() -> AdoptIntent {
        AdoptIntent {
            target_offset: 48,
            replacement: SegmentDescriptor {
                segment_id: Uuid::from_u128(77),
                topic: "events.v1".to_owned(),
                topic_epoch: 7,
                lineage: RangeLineage {
                    range_id: Uuid::from_u128(100),
                    generation: 1,
                    key_range: KeyRange::full(),
                    parents: Vec::new(),
                },
                base_offset: 48,
            },
            config: SegmentConfig {
                max_record_bytes: 256,
                max_group_bytes: 512,
                max_segment_bytes: 512,
                max_segment_records: 100,
                index_stride: 2,
            },
            tail: TailDisposition::Replace { base_offset: 0 },
            adopted: vec![
                AdoptedSegment {
                    segment_id: Uuid::from_u128(3),
                    base_offset: 0,
                },
                AdoptedSegment {
                    segment_id: Uuid::from_u128(4),
                    base_offset: 16,
                },
                AdoptedSegment {
                    segment_id: Uuid::from_u128(5),
                    base_offset: 32,
                },
            ],
            inherited: frontier(),
        }
    }

    #[test]
    fn an_intent_round_trips() {
        let original = intent();
        let decoded = AdoptIntent::decode(&original.encode().unwrap()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn a_seal_disposition_round_trips() {
        let mut original = intent();
        // The sealed tail is real history below the adopted run.
        original.adopted.remove(0);
        original.tail = TailDisposition::Seal { base_offset: 0 };
        let decoded = AdoptIntent::decode(&original.encode().unwrap()).unwrap();
        assert_eq!(decoded, original);
    }

    /// Recovery rebuilds the replacement from nothing but these bytes, so the
    /// same intent must always produce the same bytes.
    #[test]
    fn encoding_is_deterministic() {
        assert_eq!(intent().encode().unwrap(), intent().encode().unwrap());
    }

    /// Every single-byte flip must be refused: this file authorizes deleting
    /// a tail and renaming segments into a live range, and the checksum is
    /// what stands between a damaged marker and recovery acting on a guess.
    #[test]
    fn every_single_byte_flip_is_refused() {
        let pristine = intent().encode().unwrap();
        for index in 0..pristine.len() {
            let mut damaged = pristine.clone();
            damaged[index] ^= 0xff;
            assert!(
                AdoptIntent::decode(&damaged).is_err(),
                "flip at byte {index} was accepted"
            );
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
                    AdoptIntent::decode(&pristine[..length]),
                    Err(LogError::Corrupt { .. })
                ),
                "prefix of {length} bytes was accepted"
            );
        }
    }

    /// An adoption with nothing to adopt cannot have been written by the live
    /// path, so recovery must not act on it.
    #[test]
    fn an_intent_with_no_adopted_segments_is_refused() {
        let mut broken = intent();
        broken.adopted.clear();
        assert!(broken.encode().is_err());
    }

    /// A replaced tail shares its stem with the first adopted segment;
    /// recovery deletes one set of names and expects the other at the same
    /// stem, so an intent where they disagree must not exist.
    #[test]
    fn a_replaced_tail_off_the_first_adopted_base_is_refused() {
        let mut broken = intent();
        broken.tail = TailDisposition::Replace { base_offset: 7 };
        assert!(broken.encode().is_err());
    }

    /// A sealed tail at or above the first adopted base would collide with
    /// the segment being moved in.
    #[test]
    fn a_sealed_tail_at_the_first_adopted_base_is_refused() {
        let mut broken = intent();
        broken.tail = TailDisposition::Seal { base_offset: 0 };
        assert!(broken.encode().is_err());
    }

    /// Adopted bases name the files recovery moves; an order that repeats or
    /// rewinds means writer and reader disagree about which those are.
    #[test]
    fn adopted_segments_that_do_not_ascend_are_refused() {
        let mut broken = intent();
        broken.adopted[1].base_offset = broken.adopted[0].base_offset;
        assert!(broken.encode().is_err());
    }

    /// The replacement must begin ABOVE every adopted segment, or recovery
    /// would create a tail inside the run it just moved in.
    #[test]
    fn a_replacement_inside_the_adopted_run_is_refused() {
        let mut broken = intent();
        broken.target_offset = 32;
        broken.replacement.base_offset = 32;
        assert!(broken.encode().is_err());
    }
}

//! Durable marker for an in-flight sealed-prefix retention (#290).
//!
//! Retention deletes whole sealed segments from the FRONT of a range. Each
//! segment owns seven files, so a crash mid-deletion would otherwise leave a
//! front bundle with some artifacts missing — which discovery quarantines,
//! turning a routine reclamation into a range that will not open. The marker
//! is written before the first unlink, exactly as the cross-segment
//! truncation marker (#276) is, so recovery FINISHES the retention instead
//! of judging its debris.
//!
//! It is deliberately smaller than [`crate::truncate_intent::TruncateIntent`]:
//! retention needs no replacement segment (the tail is untouched), no config,
//! and no producer snapshot — a sealed segment's frontier is inherited by its
//! successor's `.producers` sidecar at roll time, so the surviving front
//! segment already carries everything the deleted prefix contributed.

use crate::{LogError, VtopLogResult};
use uuid::Uuid;

pub(crate) const RETENTION_INTENT_FILE: &str = "range.retention-intent";

const MAGIC: &[u8; 8] = b"VTOPRIN1";
const VERSION: u16 = 1;

/// A segment the retention removes: its identity, and the base offset that
/// names every file it owns via `segment_stem`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RetainedSegment {
    pub(crate) segment_id: Uuid,
    pub(crate) base_offset: u64,
}

/// Everything recovery needs to finish an interrupted retention, with no
/// other file consulted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RetentionIntent {
    /// First offset the range still holds once the retention completes —
    /// the base of the oldest surviving segment. Recorded so recovery can
    /// assert it is deleting a strict prefix and nothing else.
    pub(crate) new_base: u64,
    /// Segments to delete, in ascending base-offset order, all strictly
    /// below `new_base`.
    pub(crate) doomed: Vec<RetainedSegment>,
}

fn corrupt(reason: &str) -> LogError {
    LogError::InvalidDescriptor(format!("retention intent: {reason}"))
}

impl RetentionIntent {
    pub(crate) fn encode(&self) -> VtopLogResult<Vec<u8>> {
        if self.doomed.is_empty() {
            return Err(corrupt("dooms nothing; an empty retention is not written"));
        }
        let mut previous: Option<u64> = None;
        for doomed in &self.doomed {
            if doomed.base_offset >= self.new_base {
                return Err(corrupt(
                    "dooms a segment at or above the surviving base; retention is a strict \
                     prefix drop",
                ));
            }
            if previous.is_some_and(|last| doomed.base_offset <= last) {
                return Err(corrupt("doomed segments are not strictly ascending"));
            }
            previous = Some(doomed.base_offset);
        }
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_be_bytes());
        out.extend_from_slice(&self.new_base.to_be_bytes());
        out.extend_from_slice(&(self.doomed.len() as u32).to_be_bytes());
        for doomed in &self.doomed {
            out.extend_from_slice(doomed.segment_id.as_bytes());
            out.extend_from_slice(&doomed.base_offset.to_be_bytes());
        }
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
        let body = &bytes[..checksum_start];
        let mut at = 0_usize;
        let take = |at: &mut usize, len: usize| -> VtopLogResult<&[u8]> {
            let end = at
                .checked_add(len)
                .filter(|end| *end <= body.len())
                .ok_or_else(|| corrupt("truncated"))?;
            let slice = &body[*at..end];
            *at = end;
            Ok(slice)
        };
        if take(&mut at, 8)? != MAGIC {
            return Err(corrupt("bad magic"));
        }
        let version = u16::from_be_bytes(take(&mut at, 2)?.try_into().expect("fixed width"));
        if version != VERSION {
            return Err(corrupt(&format!("unsupported version {version}")));
        }
        let new_base = u64::from_be_bytes(take(&mut at, 8)?.try_into().expect("fixed width"));
        let count = u32::from_be_bytes(take(&mut at, 4)?.try_into().expect("fixed width")) as usize;
        if count == 0 {
            return Err(corrupt("dooms nothing"));
        }
        let mut doomed = Vec::with_capacity(count.min(1024));
        let mut previous: Option<u64> = None;
        for _ in 0..count {
            let segment_id = Uuid::from_slice(take(&mut at, 16)?)
                .map_err(|_| corrupt("segment id is not a UUID"))?;
            let base_offset =
                u64::from_be_bytes(take(&mut at, 8)?.try_into().expect("fixed width"));
            if base_offset >= new_base {
                return Err(corrupt("dooms a segment at or above the surviving base"));
            }
            if previous.is_some_and(|last| base_offset <= last) {
                return Err(corrupt("doomed segments are not strictly ascending"));
            }
            previous = Some(base_offset);
            doomed.push(RetainedSegment {
                segment_id,
                base_offset,
            });
        }
        if at != body.len() {
            return Err(corrupt("trailing bytes after the doomed list"));
        }
        Ok(Self { new_base, doomed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent() -> RetentionIntent {
        RetentionIntent {
            new_base: 400,
            doomed: vec![
                RetainedSegment {
                    segment_id: Uuid::from_u128(1),
                    base_offset: 0,
                },
                RetainedSegment {
                    segment_id: Uuid::from_u128(2),
                    base_offset: 200,
                },
            ],
        }
    }

    #[test]
    fn round_trips() {
        let encoded = intent().encode().unwrap();
        assert_eq!(RetentionIntent::decode(&encoded).unwrap(), intent());
    }

    #[test]
    fn a_flipped_bit_is_refused_before_structure_is_read() {
        let mut encoded = intent().encode().unwrap();
        encoded[12] ^= 0x01;
        assert!(RetentionIntent::decode(&encoded).is_err());
    }

    #[test]
    fn an_empty_doom_list_is_not_writable() {
        let empty = RetentionIntent {
            new_base: 100,
            doomed: Vec::new(),
        };
        assert!(empty.encode().is_err());
    }

    #[test]
    fn dooming_at_or_above_the_surviving_base_is_refused() {
        let overreach = RetentionIntent {
            new_base: 200,
            doomed: vec![RetainedSegment {
                segment_id: Uuid::from_u128(1),
                base_offset: 200,
            }],
        };
        assert!(
            overreach.encode().is_err(),
            "retention is a strict prefix drop, never the survivor"
        );
    }
}

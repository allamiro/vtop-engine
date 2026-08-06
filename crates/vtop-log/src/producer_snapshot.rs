//! The producer sequence frontier a segment inherits from its predecessor.
//!
//! # Why this exists
//!
//! Idempotent producer sequences belong to a RANGE; validation of them is
//! per SEGMENT. Those two facts are compatible only while a range is a single
//! file, which is what it has been until now.
//!
//! The moment a range rolls, a segment can begin part-way through a producer's
//! sequence — and both places that validate sequences reject exactly that:
//!
//! * the append path, because a fresh segment holds no producer state and so
//!   requires every producer to start at sequence 0;
//! * the SCAN path, which re-derives producer state from a segment's own bytes
//!   during recovery and discovery, and therefore cannot see a sequence that
//!   began in a file it does not contain.
//!
//! The second is the one that matters. Carrying state in memory across a roll
//! fixes appends but does nothing for a process that is starting up with only
//! the bytes on disk: it would open the range, work, and then refuse to open
//! after the next restart. This sidecar is what makes a rolled segment
//! self-describing.
//!
//! # What it holds, and why it is bounded
//!
//! Per `(producer_id, producer_epoch)`: the sequence frontier, plus the
//! bounded retry window of `(sequence → offset, content hash)` that duplicate
//! detection consults.
//!
//! Carrying the window matters and is not free-looking: duplicate detection
//! compares record CONTENT, and a successor does not hold the bytes of records
//! its predecessor wrote. Without the hashes, a legitimate producer retry
//! spanning a roll could not be answered — it would be neither confirmed as a
//! duplicate nor safely re-appended.
//!
//! It costs nothing new, though, because the window is already bounded at
//! [`PRODUCER_SEQUENCE_WINDOW`] entries in memory. Persisting it is the same
//! order of magnitude as holding it.
//!
//! # Which segment owns it
//!
//! The sidecar belongs to the segment that INHERITS the state — the successor
//! — not to the predecessor that produced it. So opening any segment reads its
//! own sidecar to learn where it starts, then scans its records forward. No
//! cross-segment lookup, and a segment remains verifiable on its own.
//!
//! A first segment has no predecessor and no sidecar, which reads as an empty
//! frontier: exactly today's behaviour, so existing ranges keep working
//! untouched.
//!
//! # Determinism
//!
//! Encoding and decoding are byte-deterministic and depend on nothing but the
//! map contents; entries are emitted in sorted key order so the same frontier
//! always produces the same bytes.

use crate::types::PRODUCER_SEQUENCE_WINDOW;
use crate::{LogError, VtopLogResult};
use std::collections::BTreeMap;
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"VTOPPRD1";
const VERSION: u16 = 1;

/// One producer's retry-window entry: where the record landed, and what it
/// contained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotSeen {
    pub(crate) sequence: u64,
    pub(crate) offset: u64,
    pub(crate) content_hash: [u8; 32],
}

/// The frontier for one `(producer_id, producer_epoch)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SnapshotEntry {
    pub(crate) latest_sequence: u64,
    pub(crate) first_sequence: u64,
    pub(crate) record_count: u64,
    pub(crate) seen: Vec<SnapshotSeen>,
}

/// Everything a segment needs to validate sequences that began before it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProducerSnapshot {
    /// Keyed by `(producer_id, producer_epoch)`, sorted so encoding is stable.
    pub(crate) producers: BTreeMap<(Uuid, u64), SnapshotEntry>,
    /// Highest epoch observed per producer, which is what fences an older one.
    pub(crate) epochs: BTreeMap<Uuid, u64>,
}

impl ProducerSnapshot {
    pub(crate) fn is_empty(&self) -> bool {
        self.producers.is_empty() && self.epochs.is_empty()
    }

    pub(crate) fn encode(&self) -> VtopLogResult<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_be_bytes());

        out.extend_from_slice(&(self.epochs.len() as u32).to_be_bytes());
        for (producer, epoch) in &self.epochs {
            out.extend_from_slice(producer.as_bytes());
            out.extend_from_slice(&epoch.to_be_bytes());
        }

        out.extend_from_slice(&(self.producers.len() as u32).to_be_bytes());
        for ((producer, epoch), entry) in &self.producers {
            // Bounded on the way OUT as well as in. A window longer than the
            // rule allows cannot have been produced by this code, and writing
            // it would create a file the reader is obliged to reject.
            if entry.seen.len() as u64 > PRODUCER_SEQUENCE_WINDOW {
                return Err(LogError::InvalidConfig(format!(
                    "producer {producer} epoch {epoch} has {} retry-window entries; \
                     the bound is {PRODUCER_SEQUENCE_WINDOW}",
                    entry.seen.len()
                )));
            }
            out.extend_from_slice(producer.as_bytes());
            out.extend_from_slice(&epoch.to_be_bytes());
            out.extend_from_slice(&entry.latest_sequence.to_be_bytes());
            out.extend_from_slice(&entry.first_sequence.to_be_bytes());
            out.extend_from_slice(&entry.record_count.to_be_bytes());
            out.extend_from_slice(&(entry.seen.len() as u32).to_be_bytes());
            for seen in &entry.seen {
                out.extend_from_slice(&seen.sequence.to_be_bytes());
                out.extend_from_slice(&seen.offset.to_be_bytes());
                out.extend_from_slice(&seen.content_hash);
            }
        }
        Ok(out)
    }

    pub(crate) fn decode(bytes: &[u8]) -> VtopLogResult<Self> {
        let mut reader = Reader { bytes, at: 0 };
        if reader.take(8)? != MAGIC {
            return Err(corrupt("bad magic"));
        }
        let version = u16::from_be_bytes(reader.take(2)?.try_into().expect("fixed width"));
        if version != VERSION {
            return Err(corrupt(&format!("unsupported version {version}")));
        }

        let mut epochs = BTreeMap::new();
        let epoch_count = reader.u32()? as usize;
        for _ in 0..epoch_count {
            let producer = reader.uuid()?;
            let epoch = reader.u64()?;
            if epochs.insert(producer, epoch).is_some() {
                return Err(corrupt(&format!("producer {producer} listed twice")));
            }
        }

        let mut producers = BTreeMap::new();
        let producer_count = reader.u32()? as usize;
        for _ in 0..producer_count {
            let producer = reader.uuid()?;
            let epoch = reader.u64()?;
            let latest_sequence = reader.u64()?;
            let first_sequence = reader.u64()?;
            let record_count = reader.u64()?;
            let seen_count = reader.u32()? as usize;
            // Bounded BEFORE allocating: the length comes off a file this
            // process did not necessarily write.
            if seen_count as u64 > PRODUCER_SEQUENCE_WINDOW {
                return Err(corrupt(&format!(
                    "producer {producer} claims {seen_count} retry-window entries; \
                     the bound is {PRODUCER_SEQUENCE_WINDOW}"
                )));
            }
            let mut seen = Vec::with_capacity(seen_count);
            let mut previous: Option<u64> = None;
            for _ in 0..seen_count {
                let sequence = reader.u64()?;
                // Strictly ascending. A window that repeats or rewinds a
                // sequence would give duplicate detection two answers for one
                // sequence, and the reader must not have to pick.
                if previous.is_some_and(|last| sequence <= last) {
                    return Err(corrupt(&format!(
                        "producer {producer} retry window does not ascend at sequence {sequence}"
                    )));
                }
                previous = Some(sequence);
                let offset = reader.u64()?;
                let mut content_hash = [0_u8; 32];
                content_hash.copy_from_slice(reader.take(32)?);
                seen.push(SnapshotSeen {
                    sequence,
                    offset,
                    content_hash,
                });
            }
            if first_sequence > latest_sequence {
                return Err(corrupt(&format!(
                    "producer {producer} first sequence {first_sequence} is above its latest \
                     {latest_sequence}"
                )));
            }
            // Every remembered sequence must lie inside the frontier it claims.
            // A `seen` entry above `latest_sequence` would answer a retry for a
            // record the producer has not reached, and one below
            // `first_sequence` for a record this run never wrote — either way
            // duplicate detection returns an offset that is not the record's.
            if let Some(first) = seen.first() {
                if first.sequence < first_sequence {
                    return Err(corrupt(&format!(
                        "producer {producer} remembers sequence {} below its first {first_sequence}",
                        first.sequence
                    )));
                }
            }
            if let Some(last) = seen.last() {
                if last.sequence > latest_sequence {
                    return Err(corrupt(&format!(
                        "producer {producer} remembers sequence {} above its latest \
                         {latest_sequence}",
                        last.sequence
                    )));
                }
            }
            // `record_count` is a summary of the same run, and the append path
            // increments it. A count exceeding the sequence span it describes
            // cannot have been produced by that path, and carrying it forward
            // corrupts every sealed manifest summary computed from it after.
            let span = latest_sequence
                .checked_sub(first_sequence)
                .and_then(|span| span.checked_add(1))
                .ok_or_else(|| corrupt("producer sequence span overflows"))?;
            // EQUAL, not merely within. Producer sequences are gap-free — the
            // append path refuses a gap with `SequenceGap` — so a run from
            // `first_sequence` to `latest_sequence` contains exactly `span`
            // records. A count below it describes a run with a hole, which this
            // code cannot have written, and recovery continuing from it would
            // carry that hole into every sealed manifest summary derived from
            // it afterwards.
            if record_count != span {
                return Err(corrupt(&format!(
                    "producer {producer} claims {record_count} records across sequences \
                     {first_sequence}..={latest_sequence}, which spans exactly {span}"
                )));
            }
            if producers
                .insert(
                    (producer, epoch),
                    SnapshotEntry {
                        latest_sequence,
                        first_sequence,
                        record_count,
                        seen,
                    },
                )
                .is_some()
            {
                return Err(corrupt(&format!(
                    "producer {producer} epoch {epoch} listed twice"
                )));
            }
        }
        if !reader.is_finished() {
            return Err(corrupt("trailing bytes after the last producer"));
        }
        // The epoch map is what fences a stale producer. A frontier naming an
        // epoch the map does not cover would let a producer whose epoch had
        // been superseded keep appending, because the check that would have
        // stopped it consults a map that never heard of it.
        for (producer, epoch) in producers.keys() {
            match epochs.get(producer) {
                None => {
                    return Err(corrupt(&format!(
                        "producer {producer} has state at epoch {epoch} but no epoch entry"
                    )))
                }
                Some(latest) if latest < epoch => {
                    return Err(corrupt(&format!(
                        "producer {producer} has state at epoch {epoch}, above its recorded \
                         latest epoch {latest}"
                    )))
                }
                Some(_) => {}
            }
        }
        Ok(Self { producers, epochs })
    }
}

fn corrupt(reason: &str) -> LogError {
    LogError::Corrupt {
        position: 0,
        reason: format!("producer snapshot: {reason}"),
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

    fn entry(latest: u64, seen: &[(u64, u64)]) -> SnapshotEntry {
        SnapshotEntry {
            latest_sequence: latest,
            first_sequence: 0,
            record_count: latest + 1,
            seen: seen
                .iter()
                .map(|(sequence, offset)| SnapshotSeen {
                    sequence: *sequence,
                    offset: *offset,
                    content_hash: [*sequence as u8; 32],
                })
                .collect(),
        }
    }

    fn snapshot() -> ProducerSnapshot {
        let mut producers = BTreeMap::new();
        producers.insert((Uuid::from_u128(1), 0), entry(3, &[(2, 20), (3, 30)]));
        producers.insert((Uuid::from_u128(2), 7), entry(9, &[(9, 90)]));
        let mut epochs = BTreeMap::new();
        epochs.insert(Uuid::from_u128(1), 0);
        epochs.insert(Uuid::from_u128(2), 7);
        ProducerSnapshot { producers, epochs }
    }

    #[test]
    fn a_snapshot_round_trips() {
        let original = snapshot();
        let decoded = ProducerSnapshot::decode(&original.encode().unwrap()).unwrap();
        assert_eq!(decoded, original);
    }

    /// The same frontier must always produce the same bytes: a snapshot is
    /// compared and copied between replicas, and an encoding that depended on
    /// map iteration order would make identical state look different.
    #[test]
    fn encoding_is_deterministic() {
        assert_eq!(snapshot().encode().unwrap(), snapshot().encode().unwrap());
    }

    /// An empty snapshot is the state of a range's FIRST segment, and must
    /// round-trip rather than being a special case.
    #[test]
    fn an_empty_snapshot_round_trips() {
        let empty = ProducerSnapshot::default();
        assert!(empty.is_empty());
        assert_eq!(
            ProducerSnapshot::decode(&empty.encode().unwrap()).unwrap(),
            empty
        );
    }

    /// The retry window is bounded before allocating. The count comes off a
    /// file, so a reader that reserved on it could be made to allocate by
    /// anything that can write to the data directory.
    #[test]
    fn an_oversized_retry_window_is_refused_before_allocating() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&VERSION.to_be_bytes());
        bytes.extend_from_slice(&0_u32.to_be_bytes()); // no epochs
        bytes.extend_from_slice(&1_u32.to_be_bytes()); // one producer
        bytes.extend_from_slice(Uuid::from_u128(1).as_bytes());
        bytes.extend_from_slice(&0_u64.to_be_bytes()); // epoch
        bytes.extend_from_slice(&0_u64.to_be_bytes()); // latest
        bytes.extend_from_slice(&0_u64.to_be_bytes()); // first
        bytes.extend_from_slice(&0_u64.to_be_bytes()); // count
        bytes.extend_from_slice(&u32::MAX.to_be_bytes()); // absurd window
        assert!(matches!(
            ProducerSnapshot::decode(&bytes),
            Err(LogError::Corrupt { .. })
        ));
    }

    /// A retry window that repeats or rewinds a sequence would give duplicate
    /// detection two answers for one sequence.
    #[test]
    fn a_non_ascending_retry_window_is_refused() {
        let mut broken = snapshot();
        let key = (Uuid::from_u128(1), 0);
        broken.producers.get_mut(&key).unwrap().seen = vec![
            SnapshotSeen {
                sequence: 5,
                offset: 50,
                content_hash: [0; 32],
            },
            SnapshotSeen {
                sequence: 5,
                offset: 51,
                content_hash: [1; 32],
            },
        ];
        assert!(matches!(
            ProducerSnapshot::decode(&broken.encode().unwrap()),
            Err(LogError::Corrupt { .. })
        ));
    }

    /// Trailing bytes mean the writer and reader disagree about the format, and
    /// a snapshot decides whether a producer's retry is a duplicate — so a
    /// partial understanding of it must not be treated as a whole one.
    #[test]
    fn trailing_bytes_are_refused() {
        let mut bytes = snapshot().encode().unwrap();
        bytes.push(0);
        assert!(matches!(
            ProducerSnapshot::decode(&bytes),
            Err(LogError::Corrupt { .. })
        ));
    }

    #[test]
    fn a_first_sequence_above_the_latest_is_refused() {
        let mut broken = snapshot();
        let key = (Uuid::from_u128(1), 0);
        broken.producers.get_mut(&key).unwrap().first_sequence = 99;
        assert!(matches!(
            ProducerSnapshot::decode(&broken.encode().unwrap()),
            Err(LogError::Corrupt { .. })
        ));
    }

    /// Producer sequences are gap-free, so a count BELOW its span describes a
    /// run with a hole — which the append path cannot produce. Recovery
    /// continuing from it would carry that hole into every sealed manifest
    /// summary derived from it afterwards.
    #[test]
    fn a_record_count_below_its_sequence_span_is_refused() {
        let mut broken = snapshot();
        let key = (Uuid::from_u128(1), 0);
        // Sequences 0..=3 is four records; claim three.
        broken.producers.get_mut(&key).unwrap().record_count = 3;
        assert!(matches!(
            ProducerSnapshot::decode(&broken.encode().unwrap()),
            Err(LogError::Corrupt { .. })
        ));
    }

    #[test]
    fn a_record_count_above_its_sequence_span_is_refused() {
        let mut broken = snapshot();
        let key = (Uuid::from_u128(1), 0);
        broken.producers.get_mut(&key).unwrap().record_count = 99;
        assert!(matches!(
            ProducerSnapshot::decode(&broken.encode().unwrap()),
            Err(LogError::Corrupt { .. })
        ));
    }

    /// A remembered sequence outside the frontier answers a retry for a record
    /// the producer never wrote at that point, so duplicate detection would
    /// return an offset that is not the record's.
    #[test]
    fn a_retry_entry_outside_the_frontier_is_refused() {
        let mut broken = snapshot();
        let key = (Uuid::from_u128(1), 0);
        broken
            .producers
            .get_mut(&key)
            .unwrap()
            .seen
            .push(SnapshotSeen {
                sequence: 99,
                offset: 990,
                content_hash: [9; 32],
            });
        assert!(matches!(
            ProducerSnapshot::decode(&broken.encode().unwrap()),
            Err(LogError::Corrupt { .. })
        ));
    }

    /// The epoch map is what fences a stale producer. State at an epoch the map
    /// does not cover would let a superseded producer keep appending, because
    /// the check that would stop it consults a map that never heard of it.
    #[test]
    fn state_at_an_epoch_the_map_does_not_cover_is_refused() {
        let mut broken = snapshot();
        broken.epochs.remove(&Uuid::from_u128(1));
        assert!(matches!(
            ProducerSnapshot::decode(&broken.encode().unwrap()),
            Err(LogError::Corrupt { .. })
        ));
    }
}

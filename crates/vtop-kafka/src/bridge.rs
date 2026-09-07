//! The seam between the Kafka protocol and the engine (#225).
//!
//! Everything the listener needs from a backend, and nothing about how a
//! backend does it. Phase 1 backs ONE partition per topic — the engine has no
//! partition concept; a range is a log — so the trait is partition-free and
//! the listener refuses every partition but zero by name. Offsets are the
//! backend's to assign, and it keeps whatever it keeps.
//!
//! [`MemoryBridge`] is the in-crate backend: the tests' and a lab's, never a
//! deployment's. It assigns offsets and serves batches back, and it keeps
//! Kafka's five-set window per producer (#457): a set carrying an identity
//! (`sequenced: Some`) is deduplicated against that window, and only a set
//! without one appends twice when retried. The native backend over
//! `LocalBroker` — which carries producer epochs and sequences in the log
//! itself — is wired where brokers are wired, and this trait is what it
//! implements.

use crate::messages::ErrorCode;
use crate::records::{Record, RecordBatch};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// What a produce appended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Appended {
    pub base_offset: i64,
    /// `-1` when the backend keeps the producer's timestamps (create time),
    /// which is what every phase-1 backend does.
    pub log_append_time_ms: i64,
    /// The earliest offset still held after the append (review): what the
    /// produce response advertises, from the backend's own floor.
    pub log_start_offset: i64,
}

/// What a fetch found: zero or more encoded v2 batches, back to back, as
/// Kafka carries them, and the watermarks the response reports beside them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    pub records: Vec<u8>,
    /// The next offset to be assigned: everything below it is readable.
    pub high_watermark: i64,
    pub log_start_offset: i64,
}

/// What an idempotent producer's set carries beside its records (#457): the
/// producer id and epoch every batch in the set shares, and the first batch's
/// base sequence — the set's records are sequenced contiguously from it, and
/// the listener has judged that before the backend sees the set. A backend
/// answers a set it has already appended under this identity and sequence
/// with the offset it gave the first time, and appends nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sequenced {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub first_sequence: i32,
}

pub trait Bridge: Send + Sync + 'static {
    /// Every topic this backend serves, for a Metadata request naming none.
    fn topics(&self) -> Vec<String>;

    /// Append a set of decoded batches. A produce set is one acknowledgement,
    /// and within one native append the backend is ALL OR NOTHING (review).
    /// A backend whose plane cannot frame the whole set in one append (the
    /// native replica plane frames 4 096 records) appends it in order across
    /// several. Without `sequenced`, a failure after the first leaves that
    /// prefix durable and the client told the set failed, which a retry
    /// duplicates — the same limitation a timeout has. WITH it (#457), the
    /// set is an idempotent producer's: a retry of a set already appended is
    /// answered with the offset it got the first time and appends nothing,
    /// a retry after a partial failure appends only what is missing, a
    /// sequence that is not the next one is refused (`OutOfOrderSequenceNumber`),
    /// and a retry too old to verify is `DuplicateSequenceNumber`, which a
    /// client treats as delivered. The backend assigns the base offset;
    /// every batch carries at least one record (the listener judges that),
    /// and offsets run contiguously across the set.
    fn produce(
        &self,
        topic: &str,
        batches: &[RecordBatch],
        sequenced: Option<Sequenced>,
    ) -> Result<Appended, ErrorCode>;

    /// Batches from `offset` on, up to about `max_bytes` — always at least
    /// the first batch, so a client's buffer never starves on a big one.
    fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode>;

    /// `(log_start_offset, high_watermark)`: the earliest offset still held
    /// and the next to be assigned. What ListOffsets LATEST answers, and the
    /// range a fetch offset must fall in to be served rather than refused.
    fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode>;

    /// The next offset to be assigned, which is what ListOffsets LATEST is.
    fn high_watermark(&self, topic: &str) -> Result<i64, ErrorCode> {
        self.bounds(topic).map(|(_, high_watermark)| high_watermark)
    }
}

struct StoredBatch {
    base_offset: i64,
    last_offset: i64,
    bytes: Vec<u8>,
}

/// One idempotent producer's place in a memory log: the sequence it is
/// expected to send next, and the last few sets it sent, so a retry of one
/// of them is answered with the offset it got the first time.
#[derive(Default)]
struct ProducerWindow {
    next_sequence: i64,
    /// `(first sequence, record count, base offset, fingerprint of the
    /// records)`, newest last. The fingerprint is what makes a retry a
    /// retry (review): the same sequence with other bytes is a client's
    /// bug, refused as the native log refuses it, never acknowledged as
    /// stored.
    recent: VecDeque<(i64, i64, i64, u64)>,
}

/// What a remembered set's records were: their timestamps, keys, values and
/// headers, hashed. The memory bridge is a test double, so a 64-bit hash is enough.
pub(crate) fn set_fingerprint(batches: &[RecordBatch]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for batch in batches {
        for record in &batch.records {
            record.timestamp_millis.hash(&mut hasher);
            record.key.hash(&mut hasher);
            record.value.hash(&mut hasher);
            record.headers.hash(&mut hasher);
        }
    }
    hasher.finish()
}

/// How many sets a memory log remembers per producer — Kafka's own five.
const MEMORY_DEDUP_WINDOW: usize = 5;

#[derive(Default)]
struct MemoryLog {
    batches: Vec<StoredBatch>,
    next_offset: i64,
    producers: HashMap<(i64, i16), ProducerWindow>,
}

/// An in-memory backend: offsets assigned, batches kept, nothing durable.
#[derive(Default)]
pub struct MemoryBridge {
    logs: Mutex<HashMap<String, MemoryLog>>,
}

impl MemoryBridge {
    pub fn with_topics<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let bridge = Self::default();
        for name in names {
            bridge.create_topic(name);
        }
        bridge
    }

    pub fn create_topic(&self, name: impl Into<String>) {
        self.logs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(name.into())
            .or_default();
    }
}

impl Bridge for MemoryBridge {
    fn topics(&self) -> Vec<String> {
        let logs = self
            .logs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut names: Vec<String> = logs.keys().cloned().collect();
        names.sort();
        names
    }

    fn produce(
        &self,
        topic: &str,
        batches: &[RecordBatch],
        sequenced: Option<Sequenced>,
    ) -> Result<Appended, ErrorCode> {
        if batches.is_empty() || batches.iter().any(|batch| batch.records.is_empty()) {
            // Nothing to acknowledge: an empty set, or an empty batch in it,
            // is refused rather than answered with an offset it never took.
            return Err(ErrorCode::InvalidRecord);
        }
        let mut logs = self
            .logs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let log = logs
            .get_mut(topic)
            .ok_or(ErrorCode::UnknownTopicOrPartition)?;
        // An idempotent set is judged against its producer's window first
        // (#457), under the same lock as the append: a retry of a remembered
        // set is its original offset and no append; the next sequence
        // appends; anything else is refused by the code a client acts on.
        let total: i64 = batches.iter().map(|batch| batch.records.len() as i64).sum();
        // Computed only for a set that carries an identity (review): nothing
        // reads it on the shared path.
        let fingerprint = sequenced.map(|_| set_fingerprint(batches));
        if let Some(sequenced) = sequenced {
            let first = i64::from(sequenced.first_sequence);
            let window = log
                .producers
                .entry((sequenced.producer_id, sequenced.producer_epoch))
                .or_default();
            if let Some(&(_, _, base_offset, seen_fingerprint)) = window
                .recent
                .iter()
                .find(|(seen_first, seen_count, _, _)| *seen_first == first && *seen_count == total)
            {
                if Some(seen_fingerprint) != fingerprint {
                    // The same sequence, other bytes: not a retry.
                    return Err(ErrorCode::InvalidRecord);
                }
                return Ok(Appended {
                    base_offset,
                    log_append_time_ms: -1,
                    log_start_offset: log.batches.first().map_or(0, |b| b.base_offset),
                });
            }
            if first != window.next_sequence {
                // Behind the window: delivered once, unverifiable now. Ahead
                // of it, or a new producer not starting at zero: a gap.
                return Err(if first < window.next_sequence {
                    ErrorCode::DuplicateSequenceNumber
                } else {
                    ErrorCode::OutOfOrderSequenceNumber
                });
            }
        }
        // Under ONE lock, so the set lands whole and contiguous — and STAGED
        // first (review): every batch is encoded before any is committed, so
        // a batch the encoder refuses leaves the log untouched rather than
        // half a set behind it.
        let set_base = log.next_offset;
        let mut next_offset = log.next_offset;
        let mut staged = Vec::with_capacity(batches.len());
        for batch in batches {
            let base_offset = next_offset;
            // Re-based onto the offsets THIS log assigns: the producer's own
            // base offset is meaningless here, and a fetch must hand back
            // batches whose offsets are the ones it advertised.
            let records: Vec<Record> = batch
                .records
                .iter()
                .enumerate()
                .map(|(i, record)| Record {
                    offset: base_offset + i as i64,
                    ..record.clone()
                })
                .collect();
            let bytes = RecordBatch::encode(
                base_offset,
                batch.producer_id,
                batch.producer_epoch,
                batch.base_sequence,
                &records,
            );
            let count = records.len() as i64;
            staged.push(StoredBatch {
                base_offset,
                last_offset: base_offset + count - 1,
                bytes,
            });
            next_offset = base_offset + count;
        }
        log.batches.extend(staged);
        log.next_offset = next_offset;
        if let Some(sequenced) = sequenced {
            let window = log
                .producers
                .entry((sequenced.producer_id, sequenced.producer_epoch))
                .or_default();
            let first = i64::from(sequenced.first_sequence);
            window.next_sequence = first + total;
            window
                .recent
                .push_back((first, total, set_base, fingerprint.unwrap_or_default()));
            while window.recent.len() > MEMORY_DEDUP_WINDOW {
                window.recent.pop_front();
            }
        }
        Ok(Appended {
            base_offset: set_base,
            log_append_time_ms: -1,
            log_start_offset: log.batches.first().map_or(0, |b| b.base_offset),
        })
    }

    fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode> {
        let logs = self
            .logs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let log = logs.get(topic).ok_or(ErrorCode::UnknownTopicOrPartition)?;
        if offset < 0 || offset > log.next_offset {
            return Err(ErrorCode::OffsetOutOfRange);
        }
        let mut records = Vec::new();
        for stored in log
            .batches
            .iter()
            .filter(|stored| stored.last_offset >= offset)
        {
            if !records.is_empty() && records.len() + stored.bytes.len() > max_bytes {
                break;
            }
            records.extend_from_slice(&stored.bytes);
        }
        Ok(Fetched {
            records,
            high_watermark: log.next_offset,
            log_start_offset: log.batches.first().map_or(0, |b| b.base_offset),
        })
    }

    fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
        let logs = self
            .logs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        logs.get(topic)
            .map(|log| {
                (
                    log.batches.first().map_or(0, |b| b.base_offset),
                    log.next_offset,
                )
            })
            .ok_or(ErrorCode::UnknownTopicOrPartition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(values: &[&str]) -> RecordBatch {
        RecordBatch {
            base_offset: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: values
                .iter()
                .enumerate()
                .map(|(i, v)| Record {
                    offset: i as i64,
                    timestamp_millis: 1_000 + i as i64,
                    key: None,
                    value: Some(v.as_bytes().to_vec()),
                    headers: Vec::new(),
                })
                .collect(),
        }
    }

    #[test]
    fn offsets_are_assigned_contiguously_across_batches_and_fetched_back() {
        let bridge = MemoryBridge::with_topics(["events"]);
        assert_eq!(
            bridge
                .produce("events", &[batch(&["a", "b"])], None)
                .unwrap()
                .base_offset,
            0
        );
        assert_eq!(
            bridge
                .produce("events", &[batch(&["c"])], None)
                .unwrap()
                .base_offset,
            2
        );
        assert_eq!(bridge.high_watermark("events").unwrap(), 3);

        let fetched = bridge.fetch("events", 0, 1 << 20).unwrap();
        assert_eq!(fetched.high_watermark, 3);
        let first = RecordBatch::decode(&fetched.records).unwrap();
        assert_eq!(first.base_offset, 0);
        assert_eq!(
            first.records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(first.records[1].value.as_deref(), Some(b"b".as_slice()));

        // From the middle: the batch holding offset 1 comes whole, then the next.
        let from_one = bridge.fetch("events", 1, 1 << 20).unwrap();
        assert_eq!(from_one.records.len(), fetched.records.len());
        // From 2: only the second batch.
        let from_two = bridge.fetch("events", 2, 1 << 20).unwrap();
        assert_eq!(
            RecordBatch::decode(&from_two.records).unwrap().base_offset,
            2
        );
        // At the watermark: nothing, and no error — that is a caught-up consumer.
        assert!(bridge
            .fetch("events", 3, 1 << 20)
            .unwrap()
            .records
            .is_empty());
        assert_eq!(
            bridge.fetch("events", 4, 1 << 20).unwrap_err(),
            ErrorCode::OffsetOutOfRange
        );
    }

    #[test]
    fn a_fetch_never_starves_on_a_batch_bigger_than_its_budget() {
        let bridge = MemoryBridge::with_topics(["events"]);
        bridge
            .produce("events", &[batch(&["0123456789"])], None)
            .unwrap();
        bridge.produce("events", &[batch(&["x"])], None).unwrap();
        let fetched = bridge.fetch("events", 0, 1).unwrap();
        // The first batch alone, whole, despite a one-byte budget.
        assert_eq!(
            RecordBatch::decode(&fetched.records).unwrap().base_offset,
            0
        );
        let first_len = fetched.records.len();
        assert!(
            bridge
                .fetch("events", 0, first_len + 1)
                .unwrap()
                .records
                .len()
                == first_len
        );
        assert!(bridge.fetch("events", 0, 1 << 20).unwrap().records.len() > first_len);
    }

    /// A set lands whole (review): two batches in one produce are contiguous
    /// and acknowledged once, and an empty batch anywhere refuses the set.
    #[test]
    fn a_produce_set_lands_whole_or_not_at_all() {
        let bridge = MemoryBridge::with_topics(["events"]);
        let appended = bridge
            .produce("events", &[batch(&["a", "b"]), batch(&["c"])], None)
            .unwrap();
        assert_eq!(appended.base_offset, 0);
        assert_eq!(bridge.bounds("events").unwrap(), (0, 3));
        let mut empty = batch(&["d"]);
        empty.records.clear();
        assert_eq!(
            bridge
                .produce("events", &[batch(&["d"]), empty], None)
                .unwrap_err(),
            ErrorCode::InvalidRecord
        );
        assert_eq!(
            bridge.bounds("events").unwrap(),
            (0, 3),
            "nothing of the refused set landed"
        );
        assert_eq!(
            bridge.produce("events", &[], None).unwrap_err(),
            ErrorCode::InvalidRecord
        );
    }

    /// An idempotent producer's retry is its original offset and no append;
    /// the next sequence appends; a gap and a too-old retry are refused by
    /// the code a client acts on (#457).
    #[test]
    fn a_sequenced_retry_is_answered_with_its_original_offset_and_appends_nothing() {
        let bridge = MemoryBridge::with_topics(["t"]);
        let at = |first_sequence: i32| {
            Some(Sequenced {
                producer_id: 7,
                producer_epoch: 0,
                first_sequence,
            })
        };
        let set = [batch(&["a", "b"])];
        assert_eq!(bridge.produce("t", &set, at(0)).unwrap().base_offset, 0);
        assert_eq!(
            bridge.produce("t", &set, at(0)).unwrap().base_offset,
            0,
            "a retry"
        );
        assert_eq!(bridge.bounds("t").unwrap(), (0, 2), "appended once");
        assert_eq!(
            bridge.produce("t", &[batch(&["a", "B"])], at(0)),
            Err(ErrorCode::InvalidRecord),
            "the same sequence with other bytes is not a retry"
        );
        let mut with_header = batch(&["a", "b"]);
        with_header.records[0].headers = vec![("h".to_owned(), None)];
        assert_eq!(
            bridge.produce("t", &[with_header], at(0)),
            Err(ErrorCode::InvalidRecord),
            "nor with other headers"
        );
        assert_eq!(
            bridge
                .produce("t", &[batch(&["c"])], at(2))
                .unwrap()
                .base_offset,
            2
        );
        assert_eq!(
            bridge.produce("t", &[batch(&["x"])], at(5)),
            Err(ErrorCode::OutOfOrderSequenceNumber),
            "a gap"
        );
        assert_eq!(
            bridge.produce("t", &set, at(0)).unwrap().base_offset,
            0,
            "an older retry still in the window"
        );
        for sequence in [3, 4, 5, 6, 7] {
            bridge.produce("t", &[batch(&["y"])], at(sequence)).unwrap();
        }
        assert_eq!(
            bridge.produce("t", &set, at(0)),
            Err(ErrorCode::DuplicateSequenceNumber),
            "a retry the window no longer holds: delivered, unverifiable"
        );
        assert_eq!(
            bridge.produce(
                "t",
                &set,
                Some(Sequenced {
                    producer_id: 8,
                    producer_epoch: 0,
                    first_sequence: 1
                })
            ),
            Err(ErrorCode::OutOfOrderSequenceNumber),
            "a new producer starts at zero"
        );
        assert_eq!(bridge.bounds("t").unwrap(), (0, 8));
        assert_eq!(
            bridge.produce("t", &set, None).unwrap().base_offset,
            8,
            "a set without an identity is appended as before"
        );
    }

    #[test]
    fn an_unknown_topic_is_unknown_everywhere() {
        let bridge = MemoryBridge::with_topics(["events"]);
        assert_eq!(bridge.topics(), vec!["events".to_owned()]);
        assert_eq!(
            bridge.produce("nope", &[batch(&["a"])], None).unwrap_err(),
            ErrorCode::UnknownTopicOrPartition
        );
        assert_eq!(
            bridge.fetch("nope", 0, 1).unwrap_err(),
            ErrorCode::UnknownTopicOrPartition
        );
        assert_eq!(
            bridge.high_watermark("nope").unwrap_err(),
            ErrorCode::UnknownTopicOrPartition
        );
    }
}

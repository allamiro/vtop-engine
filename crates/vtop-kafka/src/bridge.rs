//! The seam between the Kafka protocol and the engine (#225).
//!
//! Everything the listener needs from a backend, and nothing about how a
//! backend does it. Phase 1 backs ONE partition per topic — the engine has no
//! partition concept; a range is a log — so the trait is partition-free and
//! the listener refuses every partition but zero by name. Offsets are the
//! backend's to assign, and it keeps whatever it keeps.
//!
//! [`MemoryBridge`] is the in-crate backend: the tests' and a lab's, never a
//! deployment's. It assigns offsets and serves batches back; it does NOT
//! bridge idempotence, so a retried batch appends twice. The native backend
//! over `LocalBroker` — which does carry producer epochs and sequences — is
//! wired where brokers are wired, and this trait is what it implements.

use crate::messages::ErrorCode;
use crate::records::{Record, RecordBatch};
use std::collections::HashMap;
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

pub trait Bridge: Send + Sync + 'static {
    /// Every topic this backend serves, for a Metadata request naming none.
    fn topics(&self) -> Vec<String>;

    /// Append a set of decoded batches. A produce set is one acknowledgement,
    /// and within one native append the backend is ALL OR NOTHING (review).
    /// A backend whose plane cannot frame the whole set in one append (the
    /// native replica plane frames 4 096 records) appends it in order across
    /// several; a failure after the first leaves that prefix durable and the
    /// client told the set failed, which a retry duplicates — the same
    /// limitation a timeout has on a bridge without idempotence, and what
    /// InitProducerId (phase 2) is for. The backend assigns the base offset;
    /// every batch carries at least one record (the listener judges that),
    /// and offsets run contiguously across the set.
    fn produce(&self, topic: &str, batches: &[RecordBatch]) -> Result<Appended, ErrorCode>;

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

#[derive(Default)]
struct MemoryLog {
    batches: Vec<StoredBatch>,
    next_offset: i64,
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

    fn produce(&self, topic: &str, batches: &[RecordBatch]) -> Result<Appended, ErrorCode> {
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
                .produce("events", &[batch(&["a", "b"])])
                .unwrap()
                .base_offset,
            0
        );
        assert_eq!(
            bridge
                .produce("events", &[batch(&["c"])])
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
        bridge.produce("events", &[batch(&["0123456789"])]).unwrap();
        bridge.produce("events", &[batch(&["x"])]).unwrap();
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
            .produce("events", &[batch(&["a", "b"]), batch(&["c"])])
            .unwrap();
        assert_eq!(appended.base_offset, 0);
        assert_eq!(bridge.bounds("events").unwrap(), (0, 3));
        let mut empty = batch(&["d"]);
        empty.records.clear();
        assert_eq!(
            bridge
                .produce("events", &[batch(&["d"]), empty])
                .unwrap_err(),
            ErrorCode::InvalidRecord
        );
        assert_eq!(
            bridge.bounds("events").unwrap(),
            (0, 3),
            "nothing of the refused set landed"
        );
        assert_eq!(
            bridge.produce("events", &[]).unwrap_err(),
            ErrorCode::InvalidRecord
        );
    }

    #[test]
    fn an_unknown_topic_is_unknown_everywhere() {
        let bridge = MemoryBridge::with_topics(["events"]);
        assert_eq!(bridge.topics(), vec!["events".to_owned()]);
        assert_eq!(
            bridge.produce("nope", &[batch(&["a"])]).unwrap_err(),
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

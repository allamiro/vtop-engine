//! The native backend (#225): [`Bridge`] over a `LocalBroker`, the same
//! entry the native session loop uses.
//!
//! One gateway, one native identity: every Kafka producer behind this bridge
//! appends as the configured `producer_id`/`producer_epoch` with one shared
//! sequence space, so the native idempotence machinery (contiguous sequences,
//! duplicate detection) protects the bridge's own retries and NOT a Kafka
//! client's — a Kafka retry after a lost acknowledgement appends again. That
//! is the single-writer limitation the surface map records; lifting it needs
//! a producer-id allocation service the engine does not have.
//!
//! Behind the `native` feature: the crate's codecs and listener need no
//! broker, and a lab that only wants the in-memory backend should not build
//! one.

use crate::bridge::{Appended, Bridge, Fetched};
use crate::messages::ErrorCode;
use crate::records::{Record, RecordBatch};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;
use vtop_broker::LocalBroker;
use vtop_protocol::{
    Durability, ErrorCode as NativeCode, FetchRequest, Message, ProduceRecord, ProduceRequest,
    Role, WireFrame,
};

/// How the bridge appends: the identity it appends as, and the durability the
/// range can honour (the broker refuses `Quorum` on a standalone range and
/// `LocalFsync` on a replicated one, so the node that wires this says which).
///
/// There is no producer epoch here on purpose: the bridge MINTS one when it
/// is built (see [`NativeBridge::new`]), because a sequence space is a
/// property of one live bridge and a recreated one must not inherit a
/// frontier it did not see.
#[derive(Debug, Clone)]
pub struct NativeBridgeConfig {
    pub topic: String,
    pub producer_id: Uuid,
    pub durability: Durability,
    /// The most records one fetch asks the broker for.
    pub fetch_max_records: u32,
}

/// The sequence space of one live bridge: the next sequence to reserve, and
/// the lock that ORDERS reservations against appends (review). Two sessions
/// reserving adjacent sequences and reaching the broker in the other order
/// would trip its contiguity check, and a reservation the broker refused
/// would leave a hole every later append trips over — so a reservation is
/// taken and spent under one lock, and stands only once the broker accepted
/// it.
struct SequenceSpace {
    next: u64,
}

pub struct NativeBridge {
    broker: Arc<LocalBroker>,
    config: NativeBridgeConfig,
    producer_epoch: u64,
    sequences: std::sync::Mutex<SequenceSpace>,
    request_id: AtomicU64,
}

/// One epoch per bridge built, strictly increasing within a process and
/// across restarts on any sane clock: microseconds since the epoch, bumped
/// past the previous mint when two bridges are built in the same instant.
fn mint_producer_epoch() -> u64 {
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(1);
    let mut previous = LAST.load(Ordering::SeqCst);
    loop {
        let candidate = now.max(previous + 1);
        match LAST.compare_exchange(previous, candidate, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return candidate,
            Err(seen) => previous = seen,
        }
    }
}

impl NativeBridge {
    /// Build over `broker`, minting a fresh producer epoch: a recreated
    /// bridge does not know where the previous one's sequences ended, and
    /// the broker keeps per-epoch sequence state, so a new epoch — sequences
    /// from zero, as a restarted Kafka producer's own — is the honest start.
    /// The old epoch's state stays behind it, as it should.
    pub fn new(broker: Arc<LocalBroker>, config: NativeBridgeConfig) -> Self {
        Self::with_producer_epoch(broker, config, mint_producer_epoch())
    }

    /// Build with a chosen epoch. For a caller that allocates epochs itself
    /// (or a test that needs two bridges to share one); an epoch the broker
    /// has already seen sequences for must be resumed by that caller.
    pub fn with_producer_epoch(
        broker: Arc<LocalBroker>,
        config: NativeBridgeConfig,
        producer_epoch: u64,
    ) -> Self {
        Self {
            broker,
            config,
            producer_epoch,
            sequences: std::sync::Mutex::new(SequenceSpace { next: 0 }),
            request_id: AtomicU64::new(1),
        }
    }

    /// The producer epoch this bridge appends under.
    pub fn producer_epoch(&self) -> u64 {
        self.producer_epoch
    }

    /// The next sequence this bridge would reserve.
    pub fn next_sequence(&self) -> u64 {
        self.sequences
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .next
    }

    fn frame(&self, message: Message) -> WireFrame {
        WireFrame {
            request_id: self.request_id.fetch_add(1, Ordering::Relaxed),
            stream_id: 0,
            message,
        }
    }

    fn known(&self, topic: &str) -> Result<(), ErrorCode> {
        if topic == self.config.topic {
            Ok(())
        } else {
            Err(ErrorCode::UnknownTopicOrPartition)
        }
    }
}

/// The native refusal a Kafka client can act on.
///
/// Only truthful mappings: a fenced or wrong-range broker is not this
/// partition's leader any more; a storage or overload refusal is a timeout
/// the client retries; a sequence conflict or a malformed request is a bad
/// record the client must not retry blindly.
fn kafka_code(code: NativeCode) -> ErrorCode {
    match code {
        NativeCode::Fenced | NativeCode::WrongRange | NativeCode::WrongLineage => {
            ErrorCode::NotLeaderOrFollower
        }
        NativeCode::Overloaded | NativeCode::Storage => ErrorCode::RequestTimedOut,
        NativeCode::OffsetRetained => ErrorCode::OffsetOutOfRange,
        _ => ErrorCode::InvalidRecord,
    }
}

impl Bridge for NativeBridge {
    fn topics(&self) -> Vec<String> {
        vec![self.config.topic.clone()]
    }

    fn produce(&self, topic: &str, batches: &[RecordBatch]) -> Result<Appended, ErrorCode> {
        self.known(topic)?;
        if batches.is_empty() || batches.iter().any(|batch| batch.records.is_empty()) {
            return Err(ErrorCode::InvalidRecord);
        }
        // The native record has no null (review), and a shape the log cannot
        // hold is refused rather than bent: a null VALUE is a tombstone, and
        // storing it as empty bytes would read back as a real empty message;
        // a present-but-empty KEY would read back as null, since an empty
        // native key is how a null key is kept. A null key and an empty
        // native key are one shape and round-trip as null; everything else
        // round-trips exactly.
        for (which, batch) in batches.iter().enumerate() {
            for (index, record) in batch.records.iter().enumerate() {
                if record.value.is_none() {
                    tracing::warn!(
                        which,
                        index,
                        "native produce refused: a null value (tombstone) has no representation in \
                         the native log; send an empty value, or none of this record"
                    );
                    return Err(ErrorCode::InvalidRecord);
                }
                if matches!(&record.key, Some(key) if key.is_empty()) {
                    tracing::warn!(
                        which,
                        index,
                        "native produce refused: an empty key would read back as null; send a null \
                         key (no key) instead"
                    );
                    return Err(ErrorCode::InvalidRecord);
                }
            }
        }
        // The whole set is ONE native append (review): the broker takes a
        // request's records atomically, so a set is acknowledged whole or
        // refused whole, never half durable for a client to retry into.
        let records: Vec<ProduceRecord> = batches
            .iter()
            .flat_map(|batch| batch.records.iter())
            .map(|record| ProduceRecord {
                timestamp_millis: record.timestamp_millis,
                key: record.key.clone().unwrap_or_default(),
                value: record.value.clone().unwrap_or_default(),
            })
            .collect();
        let count = records.len() as u64;
        // Reserved and spent under ONE lock (review): the broker requires
        // contiguous sequences in arrival order, so no other produce may
        // reach it between this reservation and this append — and the
        // reservation stands only once the broker took it, so a refused
        // append leaves no hole for the next one to trip over.
        let mut sequences = self
            .sequences
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let first_sequence = sequences.next;
        let request = ProduceRequest {
            range: self.broker.range().clone(),
            fencing_epoch: self.broker.held_fencing_epoch(),
            producer_id: self.config.producer_id,
            producer_epoch: self.producer_epoch,
            first_sequence,
            durability: self.config.durability,
            records,
        };
        let reply = self
            .broker
            .handle(Role::Producer, self.frame(Message::ProduceRequest(request)));
        match reply.message {
            Message::ProduceResponse(response) => {
                let base_offset = response
                    .outcomes
                    .first()
                    .map(|outcome| outcome.offset as i64)
                    .ok_or(ErrorCode::InvalidRecord)?;
                sequences.next = first_sequence + count;
                Ok(Appended {
                    base_offset,
                    log_append_time_ms: -1,
                    log_start_offset: self.broker.earliest_offset() as i64,
                })
            }
            Message::Error(error) => {
                tracing::warn!(code = ?error.code, message = %error.message, "native produce refused");
                Err(kafka_code(error.code))
            }
            other => {
                tracing::warn!(?other, "native produce answered with an unexpected message");
                Err(ErrorCode::InvalidRecord)
            }
        }
    }

    fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode> {
        self.known(topic)?;
        let start_offset = u64::try_from(offset).map_err(|_| ErrorCode::OffsetOutOfRange)?;
        let request = FetchRequest {
            range: self.broker.range().clone(),
            fencing_epoch: self.broker.held_fencing_epoch(),
            start_offset,
            max_bytes: u32::try_from(max_bytes).unwrap_or(u32::MAX),
            max_records: self.config.fetch_max_records,
        };
        let reply = self
            .broker
            .handle(Role::Consumer, self.frame(Message::FetchRequest(request)));
        match reply.message {
            Message::FetchResponse(response) => {
                let high_watermark = response.committed_high_watermark as i64;
                if start_offset > response.committed_high_watermark {
                    return Err(ErrorCode::OffsetOutOfRange);
                }
                let records: Vec<Record> = response
                    .records
                    .iter()
                    .map(|record| Record {
                        offset: record.offset as i64,
                        timestamp_millis: record.timestamp_millis,
                        key: (!record.key.is_empty()).then(|| record.key.clone()),
                        value: Some(record.value.clone()),
                        headers: Vec::new(),
                    })
                    .collect();
                let encoded = match records.first() {
                    None => Vec::new(),
                    // One batch, at the first record's offset, under the
                    // bridge's identity: the producer's own is not kept by
                    // the native log, and a consumer does not need it.
                    Some(first) => RecordBatch::encode(first.offset, -1, -1, -1, &records),
                };
                Ok(Fetched {
                    records: encoded,
                    high_watermark,
                    // The floor retention left (review), not zero: a
                    // consumer below it must learn where the log now starts.
                    log_start_offset: self.broker.earliest_offset() as i64,
                })
            }
            Message::Error(error) => {
                tracing::warn!(code = ?error.code, message = %error.message, "native fetch refused");
                Err(kafka_code(error.code))
            }
            other => {
                tracing::warn!(?other, "native fetch answered with an unexpected message");
                Err(ErrorCode::InvalidRecord)
            }
        }
    }

    fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
        self.known(topic)?;
        // The watermark asked from the log's END (review), never from offset
        // zero: once retention has reclaimed the prefix, a fetch from zero
        // is refused as retained, and the watermark would vanish with it. A
        // fetch at the next offset carries the committed high watermark and
        // no records, and the broker accepts it. The floor is the broker's
        // own. `local_offsets` queues behind an append the way a request
        // handler may.
        let (_, next_offset) = self.broker.local_offsets();
        let request = FetchRequest {
            range: self.broker.range().clone(),
            fencing_epoch: self.broker.held_fencing_epoch(),
            start_offset: next_offset,
            max_bytes: 1,
            max_records: 1,
        };
        match self
            .broker
            .handle(Role::Consumer, self.frame(Message::FetchRequest(request)))
            .message
        {
            Message::FetchResponse(response) => Ok((
                self.broker.earliest_offset() as i64,
                response.committed_high_watermark as i64,
            )),
            Message::Error(error) => Err(kafka_code(error.code)),
            _ => Err(ErrorCode::InvalidRecord),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use vtop_broker::ProducerEpochJournal;
    use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
    use vtop_protocol::RangeIdentity;

    fn broker() -> (TempDir, Arc<LocalBroker>) {
        let dir = tempfile::tempdir().unwrap();
        let range_id = Uuid::from_u128(10);
        let range = RangeIdentity {
            topic: "events".to_owned(),
            topic_epoch: 1,
            range_id,
            range_generation: 0,
        };
        let descriptor = SegmentDescriptor {
            segment_id: Uuid::from_u128(11),
            topic: range.topic.clone(),
            topic_epoch: range.topic_epoch,
            lineage: RangeLineage {
                range_id,
                generation: 0,
                key_range: KeyRange::full(),
                parents: Vec::new(),
            },
            base_offset: 0,
        };
        let segment = ActiveSegment::create(
            dir.path().join("events.active"),
            descriptor,
            SegmentConfig::default(),
        )
        .unwrap();
        let epochs = ProducerEpochJournal::open(dir.path().join("events.epochs")).unwrap();
        let broker = Arc::new(LocalBroker::new(segment, epochs, range, 7).unwrap());
        (dir, broker)
    }

    fn config(durability: Durability) -> NativeBridgeConfig {
        NativeBridgeConfig {
            topic: "events".to_owned(),
            producer_id: Uuid::from_u128(0xabc),
            durability,
            fetch_max_records: 1024,
        }
    }

    fn bridge(broker: Arc<LocalBroker>) -> NativeBridge {
        NativeBridge::new(broker, config(Durability::LocalFsync))
    }

    /// A refused append leaves no hole (review): the reservation stands only
    /// once the broker took it, so the next produce under the same epoch
    /// starts where the broker expects.
    #[test]
    fn a_refused_append_does_not_spend_a_sequence() {
        let (_dir, broker) = broker();
        // Quorum on a standalone range is refused by the broker before any
        // append: the one refusal a fixture can produce on demand.
        let refused =
            NativeBridge::with_producer_epoch(Arc::clone(&broker), config(Durability::Quorum), 77);
        assert!(refused.produce("events", &[batch(&[("a", None)])]).is_err());
        assert_eq!(
            refused.next_sequence(),
            0,
            "nothing reserved for a refused append"
        );
        // The same epoch, from a bridge that appends: the broker still
        // expects sequence 0, and gets it.
        let accepted = NativeBridge::with_producer_epoch(
            Arc::clone(&broker),
            config(Durability::LocalFsync),
            77,
        );
        assert_eq!(
            accepted
                .produce("events", &[batch(&[("a", None), ("b", None)])])
                .unwrap()
                .base_offset,
            0
        );
        assert_eq!(accepted.next_sequence(), 2);
        assert_eq!(
            accepted
                .produce("events", &[batch(&[("c", None)])])
                .unwrap()
                .base_offset,
            2
        );
    }

    /// A recreated bridge mints its own epoch (review): sequences from zero
    /// under a new epoch, the way a restarted producer's are, so the old
    /// frontier is never guessed at.
    #[test]
    fn a_recreated_bridge_starts_a_new_producer_epoch() {
        let (_dir, broker) = broker();
        let first = bridge(Arc::clone(&broker));
        first
            .produce("events", &[batch(&[("a", None), ("b", None)])])
            .unwrap();
        let second = bridge(Arc::clone(&broker));
        assert!(
            second.producer_epoch() > first.producer_epoch(),
            "strictly later"
        );
        assert_eq!(second.next_sequence(), 0);
        assert_eq!(
            second
                .produce("events", &[batch(&[("c", None)])])
                .unwrap()
                .base_offset,
            2
        );
        assert_eq!(second.high_watermark("events").unwrap(), 3);
        let epochs: Vec<u64> = (0..64).map(|_| mint_producer_epoch()).collect();
        assert!(
            epochs.windows(2).all(|pair| pair[0] < pair[1]),
            "minted epochs are strictly increasing: {epochs:?}"
        );
    }

    fn batch(values: &[(&str, Option<&str>)]) -> RecordBatch {
        RecordBatch {
            base_offset: 0,
            producer_id: 42,
            producer_epoch: 3,
            base_sequence: 100,
            records: values
                .iter()
                .enumerate()
                .map(|(i, (value, key))| Record {
                    offset: i as i64,
                    timestamp_millis: 1_700_000_000_000 + i as i64,
                    key: key.map(|k| k.as_bytes().to_vec()),
                    value: Some(value.as_bytes().to_vec()),
                    headers: Vec::new(),
                })
                .collect(),
        }
    }

    #[test]
    fn a_produce_lands_in_the_native_log_and_reads_back_as_one_batch() {
        let (_dir, broker) = broker();
        let bridge = bridge(broker);
        assert_eq!(bridge.topics(), vec!["events".to_owned()]);
        let first = bridge
            .produce("events", &[batch(&[("a", Some("k1")), ("b", None)])])
            .unwrap();
        assert_eq!(first.base_offset, 0);
        let second = bridge.produce("events", &[batch(&[("c", None)])]).unwrap();
        assert_eq!(
            second.base_offset, 2,
            "the native log assigns contiguous offsets"
        );
        assert_eq!(bridge.high_watermark("events").unwrap(), 3);
        assert_eq!(bridge.bounds("events").unwrap(), (0, 3));

        let fetched = bridge.fetch("events", 0, 1 << 20).unwrap();
        assert_eq!((fetched.high_watermark, fetched.log_start_offset), (3, 0));
        let decoded = RecordBatch::decode(&fetched.records).unwrap();
        assert_eq!(decoded.base_offset, 0);
        let values: Vec<&[u8]> = decoded
            .records
            .iter()
            .map(|r| r.value.as_deref().unwrap())
            .collect();
        assert_eq!(values, vec![b"a".as_slice(), b"b", b"c"]);
        assert_eq!(decoded.records[0].key.as_deref(), Some(b"k1".as_slice()));
        assert_eq!(
            decoded.records[1].key, None,
            "an empty native key reads back as null"
        );
        assert_eq!(decoded.records[0].timestamp_millis, 1_700_000_000_000);

        // From the middle, and at the watermark.
        let tail =
            RecordBatch::decode(&bridge.fetch("events", 2, 1 << 20).unwrap().records).unwrap();
        assert_eq!((tail.base_offset, tail.records.len()), (2, 1));
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

    /// The shapes the native log cannot hold are refused, not bent (review):
    /// a null value and an empty key; a null key round-trips as null.
    #[test]
    fn a_tombstone_and_an_empty_key_are_refused_and_a_null_key_round_trips() {
        let (_dir, broker) = broker();
        let bridge = bridge(broker);
        let mut tombstone = batch(&[("a", None)]);
        tombstone.records[0].value = None;
        assert_eq!(
            bridge.produce("events", &[tombstone]).unwrap_err(),
            ErrorCode::InvalidRecord
        );
        assert_eq!(
            bridge
                .produce("events", &[batch(&[("a", Some(""))])])
                .unwrap_err(),
            ErrorCode::InvalidRecord
        );
        assert_eq!(bridge.bounds("events").unwrap(), (0, 0), "nothing landed");
        bridge
            .produce("events", &[batch(&[("a", None), ("b", Some("k"))])])
            .unwrap();
        let decoded =
            RecordBatch::decode(&bridge.fetch("events", 0, 1 << 20).unwrap().records).unwrap();
        assert_eq!(decoded.records[0].key, None);
        assert_eq!(decoded.records[1].key.as_deref(), Some(b"k".as_slice()));
        assert_eq!(decoded.records[0].value.as_deref(), Some(b"a".as_slice()));
    }

    /// A two-batch set is one native append (review): contiguous, one
    /// acknowledgement, and the sequence space advances by the whole set.
    #[test]
    fn a_produce_set_is_one_native_append() {
        let (_dir, broker) = broker();
        let bridge = bridge(broker);
        let appended = bridge
            .produce(
                "events",
                &[batch(&[("a", None), ("b", None)]), batch(&[("c", None)])],
            )
            .unwrap();
        assert_eq!(appended.base_offset, 0);
        assert_eq!(bridge.next_sequence(), 3);
        assert_eq!(bridge.bounds("events").unwrap(), (0, 3));
        let mut empty = batch(&[("d", None)]);
        empty.records.clear();
        assert_eq!(
            bridge
                .produce("events", &[batch(&[("d", None)]), empty])
                .unwrap_err(),
            ErrorCode::InvalidRecord
        );
        assert_eq!(bridge.bounds("events").unwrap(), (0, 3), "nothing landed");
    }

    #[test]
    fn the_bridge_serves_its_own_topic_only() {
        let (_dir, broker) = broker();
        let bridge = bridge(broker);
        assert_eq!(
            bridge
                .produce("other", &[batch(&[("a", None)])])
                .unwrap_err(),
            ErrorCode::UnknownTopicOrPartition
        );
        assert_eq!(
            bridge.fetch("other", 0, 1).unwrap_err(),
            ErrorCode::UnknownTopicOrPartition
        );
        assert_eq!(
            bridge.high_watermark("other").unwrap_err(),
            ErrorCode::UnknownTopicOrPartition
        );
    }

    #[test]
    fn native_refusals_map_to_codes_a_client_can_act_on() {
        assert_eq!(
            kafka_code(NativeCode::Fenced),
            ErrorCode::NotLeaderOrFollower
        );
        assert_eq!(
            kafka_code(NativeCode::WrongRange),
            ErrorCode::NotLeaderOrFollower
        );
        assert_eq!(
            kafka_code(NativeCode::Overloaded),
            ErrorCode::RequestTimedOut
        );
        assert_eq!(kafka_code(NativeCode::Storage), ErrorCode::RequestTimedOut);
        assert_eq!(
            kafka_code(NativeCode::SequenceConflict),
            ErrorCode::InvalidRecord
        );
        assert_eq!(
            kafka_code(NativeCode::OffsetRetained),
            ErrorCode::OffsetOutOfRange
        );
    }
}

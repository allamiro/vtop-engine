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
    /// The most records one native append carries. The replica plane frames
    /// an append whole, and a follower refuses a frame over its limit
    /// (`DEFAULT_MAX_RECORDS`) — silently, from the leader's side, as a
    /// quorum that never arrives. A Kafka batch is up to 10 000 records by
    /// a stock client's default, so a set is appended in as many native
    /// appends as this allows, in order.
    pub max_append_records: usize,
    /// The most record bytes one native append carries, for the same
    /// reason against the plane's frame limit. One record above it is
    /// refused as too large.
    pub max_append_bytes: usize,
}

/// Bytes a record costs on the native wire, roughly: its key, its value,
/// and the framing around them.
fn record_wire_bytes(record: &ProduceRecord) -> usize {
    record.key.len() + record.value.len() + 32
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

/// The producer-epoch space has no room above what the journal or this
/// process already holds: the bridge cannot be built with an epoch the
/// broker would take as new.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochsExhausted;

impl std::fmt::Display for EpochsExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the producer-epoch space is exhausted: no epoch above what the journal holds")
    }
}

impl std::error::Error for EpochsExhausted {}

pub struct NativeBridge {
    broker: Arc<LocalBroker>,
    config: NativeBridgeConfig,
    producer_epoch: u64,
    sequences: std::sync::Mutex<SequenceSpace>,
    request_id: AtomicU64,
}

/// One epoch per bridge built, strictly increasing within a process and
/// across restarts on any sane clock: microseconds since the epoch, bumped
/// past the previous mint when two bridges are built in the same instant —
/// and held ABOVE an epoch the journal already holds for this producer
/// (review): the journal replicates with the log, so a replica
/// promoted after a failover carries the former leader's epoch, and a clock
/// behind that leader's would otherwise mint below it — every append fenced
/// until the clock caught up. Durable state orders the mint; the clock only
/// keeps two bridges in one process apart.
fn mint_producer_epoch_above(journal: Option<u64>) -> Result<u64, EpochsExhausted> {
    static LAST: AtomicU64 = AtomicU64::new(0);
    // An epoch space with no room above what the journal or this process
    // holds is refused (review), never reused: a reused epoch with a fresh
    // sequence space would be the broker's duplicate check defeated.
    let above_journal = match journal {
        Some(u64::MAX) => return Err(EpochsExhausted),
        Some(held) => held + 1,
        None => 0,
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(1)
        .max(above_journal);
    let mut previous = LAST.load(Ordering::SeqCst);
    loop {
        if previous == u64::MAX {
            return Err(EpochsExhausted);
        }
        let candidate = now.max(previous + 1);
        match LAST.compare_exchange(previous, candidate, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return Ok(candidate),
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
    pub fn new(
        broker: Arc<LocalBroker>,
        config: NativeBridgeConfig,
    ) -> Result<Self, EpochsExhausted> {
        let held = broker.producer_epoch_of(config.producer_id);
        let epoch = mint_producer_epoch_above(held)?;
        Ok(Self::with_producer_epoch(broker, config, epoch))
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
/// The set cut into appends the plane can frame: at most `max_records`
/// records and about `max_bytes` bytes each, in order. A record that alone
/// exceeds the byte bound has no append that can carry it.
fn split_for_the_plane(
    records: Vec<ProduceRecord>,
    max_records: usize,
    max_bytes: usize,
) -> Result<Vec<Vec<ProduceRecord>>, ErrorCode> {
    let mut appends: Vec<Vec<ProduceRecord>> = Vec::new();
    let mut current: Vec<ProduceRecord> = Vec::new();
    let mut current_bytes = 0;
    for record in records {
        let bytes = record_wire_bytes(&record);
        if bytes > max_bytes {
            tracing::warn!(
                bytes,
                max_bytes,
                "native produce refused: one record exceeds what a native append can carry"
            );
            return Err(ErrorCode::MessageTooLarge);
        }
        if !current.is_empty()
            && (current.len() >= max_records || current_bytes + bytes > max_bytes)
        {
            appends.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes += bytes;
        current.push(record);
    }
    if !current.is_empty() {
        appends.push(current);
    }
    Ok(appends)
}

/// The records that fit `max_bytes` on Kafka's wire, roughly, and at least
/// the first: what a widened hop found, cut back to what the client asked.
fn within_budget(
    records: Vec<vtop_protocol::FetchedRecord>,
    max_bytes: usize,
) -> Vec<vtop_protocol::FetchedRecord> {
    let mut spent = 0_usize;
    let mut kept = Vec::with_capacity(records.len());
    for record in records {
        let cost = record.key.len() + record.value.len() + 24;
        if !kept.is_empty() && spent + cost > max_bytes {
            break;
        }
        spent += cost;
        kept.push(record);
    }
    kept
}

/// The byte budget a fetch widens to after a stretch with nothing visible:
/// enough to cross a run of markers in a few hops, whatever the client's own
/// partition budget was.
const INVISIBLE_HOP_BUDGET: u32 = 1 << 20;

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
        let records: Vec<ProduceRecord> = batches
            .iter()
            .flat_map(|batch| batch.records.iter())
            .map(|record| ProduceRecord {
                timestamp_millis: record.timestamp_millis,
                key: record.key.clone().unwrap_or_default(),
                value: record.value.clone().unwrap_or_default(),
            })
            .collect();
        // The set becomes native appends the replica plane can frame
        // (review): at most `max_append_records` records and
        // `max_append_bytes` bytes each, in the set's order. Within one
        // append the broker is atomic — acknowledged whole or refused whole.
        // Across appends it is not: a failure after the first leaves the
        // earlier ones durable and the client told the set failed, and a
        // retry then duplicates them — the same retry-duplicates limitation
        // a bridge without idempotence has on a timeout, and the reason the
        // split is as coarse as the plane allows.
        let appends = split_for_the_plane(
            records,
            self.config.max_append_records.max(1),
            self.config.max_append_bytes.max(1),
        )?;
        // Reserved and spent under ONE lock (review): the broker requires
        // contiguous sequences in arrival order, so no other produce may
        // reach it between this reservation and this append — and the
        // reservation stands only once the broker took it, so a refused
        // append leaves no hole for the next one to trip over.
        let mut sequences = self
            .sequences
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut base_offset = None;
        for (which, records) in appends.into_iter().enumerate() {
            let count = records.len() as u64;
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
                    let offset = response
                        .outcomes
                        .first()
                        .map(|outcome| outcome.offset as i64)
                        .ok_or(ErrorCode::InvalidRecord)?;
                    base_offset.get_or_insert(offset);
                    sequences.next = first_sequence + count;
                }
                Message::Error(error) => {
                    tracing::warn!(
                        code = ?error.code,
                        message = %error.message,
                        append = which,
                        "native produce refused"
                    );
                    return Err(kafka_code(error.code));
                }
                other => {
                    tracing::warn!(?other, "native produce answered with an unexpected message");
                    return Err(ErrorCode::InvalidRecord);
                }
            }
        }
        Ok(Appended {
            base_offset: base_offset.ok_or(ErrorCode::InvalidRecord)?,
            log_append_time_ms: -1,
            log_start_offset: self.broker.earliest_offset() as i64,
        })
    }

    fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode> {
        self.known(topic)?;
        let start_offset = u64::try_from(offset).map_err(|_| ErrorCode::OffsetOutOfRange)?;
        let mut from = start_offset;
        let mut budget = u32::try_from(max_bytes).unwrap_or(u32::MAX);
        let response = loop {
            let request = FetchRequest {
                range: self.broker.range().clone(),
                fencing_epoch: self.broker.held_fencing_epoch(),
                start_offset: from,
                max_bytes: budget,
                max_records: self.config.fetch_max_records,
            };
            let reply = self
                .broker
                .handle(Role::Consumer, self.frame(Message::FetchRequest(request)));
            match reply.message {
                Message::FetchResponse(response) => {
                    if start_offset > response.committed_high_watermark {
                        return Err(ErrorCode::OffsetOutOfRange);
                    }
                    // A stretch with nothing visible but a moved cursor — a
                    // promotion marker the broker filters from consumer output
                    // (review): follow the cursor, until a visible record or
                    // the watermark. A Kafka client has no cursor of its own
                    // to move, and no cap belongs here either: an answer
                    // short of visible data parks the client on this offset
                    // for good. The cursor moves forward on every hop, so
                    // the walk ends with the log; after the first hop the
                    // budget widens so a run of markers is crossed in a few.
                    if response.records.is_empty()
                        && response.next_offset > from
                        && response.next_offset < response.committed_high_watermark
                    {
                        from = response.next_offset;
                        budget = budget.max(INVISIBLE_HOP_BUDGET);
                        continue;
                    }
                    break response;
                }
                Message::Error(error) => {
                    tracing::warn!(code = ?error.code, message = %error.message, "native fetch refused");
                    return Err(kafka_code(error.code));
                }
                other => {
                    tracing::warn!(?other, "native fetch answered with an unexpected message");
                    return Err(ErrorCode::InvalidRecord);
                }
            }
        };
        let high_watermark = response.committed_high_watermark as i64;
        // A hop widened the budget to cross markers, never to hand the client
        // more than it asked (review): the visible records found are cut
        // back to the client's own budget, at least one kept — the
        // at-least-one-batch rule a Kafka broker keeps too.
        let records = within_budget(response.records, max_bytes);
        {
            {
                let records: Vec<Record> = records
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
        }
    }

    fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
        self.known(topic)?;
        // One snapshot of the log (review): the retained floor and the
        // committed watermark read under one lock, never a fetch at an
        // offset read a moment earlier — under appends with retention, two
        // segment rolls between the two could reclaim that offset and turn
        // a healthy topic's bounds into OFFSET_OUT_OF_RANGE. The watermark
        // is the same one a fetch reports.
        let (floor, high_watermark) = self.broker.retained_bounds();
        Ok((floor as i64, high_watermark as i64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use vtop_broker::ProducerEpochJournal;
    use vtop_log::{
        ActiveSegment, KeyRange, LogRecord, RangeLineage, SegmentConfig, SegmentDescriptor,
    };
    use vtop_protocol::RangeIdentity;

    fn broker() -> (TempDir, Arc<LocalBroker>) {
        broker_over(Vec::new())
    }

    /// A broker opened over a log that already holds `records`, committed.
    fn broker_over(records: Vec<LogRecord>) -> (TempDir, Arc<LocalBroker>) {
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
        let mut segment = ActiveSegment::create(
            dir.path().join("events.active"),
            descriptor,
            SegmentConfig::default(),
        )
        .unwrap();
        for record in records {
            segment
                .append(record, vtop_log::Durability::Buffered)
                .unwrap();
        }
        if segment.next_offset() > 0 {
            segment.commit().unwrap();
        }
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
            max_append_records: 4096,
            max_append_bytes: 4 << 20,
        }
    }

    /// A set larger than one native append is appended in order across
    /// several (review): a stock client's default batch is 10 000 records
    /// and the replica plane frames 4 096.
    #[test]
    fn a_large_set_is_appended_in_order_across_several_native_appends() {
        let (_dir, broker) = broker();
        let bridge = NativeBridge::new(
            Arc::clone(&broker),
            NativeBridgeConfig {
                max_append_records: 2,
                ..config(Durability::LocalFsync)
            },
        )
        .unwrap();
        let values = ["a", "b", "c", "d", "e"];
        let records: Vec<(&str, Option<&str>)> = values.iter().map(|v| (*v, None)).collect();
        let appended = bridge.produce("events", &[batch(&records)]).unwrap();
        assert_eq!(appended.base_offset, 0);
        assert_eq!(bridge.bounds("events").unwrap(), (0, 5));
        assert_eq!(
            bridge.next_sequence(),
            5,
            "one sequence per record across the appends"
        );
        let all =
            RecordBatch::decode(&bridge.fetch("events", 0, 1 << 20).unwrap().records).unwrap();
        let got: Vec<&[u8]> = all
            .records
            .iter()
            .map(|r| r.value.as_deref().unwrap())
            .collect();
        assert_eq!(got, vec![b"a".as_slice(), b"b", b"c", b"d", b"e"]);
        // By bytes, the same way; and one record too large for any append
        // is refused as such.
        let bridge = NativeBridge::new(
            Arc::clone(&broker),
            NativeBridgeConfig {
                max_append_bytes: 40,
                ..config(Durability::LocalFsync)
            },
        )
        .unwrap();
        assert!(bridge.produce("events", &[batch(&records)]).is_ok());
        assert_eq!(bridge.bounds("events").unwrap(), (0, 10));
        let big = "x".repeat(64);
        assert_eq!(
            bridge
                .produce("events", &[batch(&[(big.as_str(), None)])])
                .unwrap_err(),
            ErrorCode::MessageTooLarge
        );
    }

    fn bridge(broker: Arc<LocalBroker>) -> NativeBridge {
        NativeBridge::new(broker, config(Durability::LocalFsync)).unwrap()
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
        let epochs: Vec<u64> = (0..64)
            .map(|_| mint_producer_epoch_above(None).unwrap())
            .collect();
        assert!(
            epochs.windows(2).all(|pair| pair[0] < pair[1]),
            "minted epochs are strictly increasing: {epochs:?}"
        );
    }

    /// A journal that already holds a higher epoch for the bridge's producer
    /// — a replica promoted after a failover, its clock behind the former
    /// leader's — orders the mint (review): the new bridge appends, it is
    /// not fenced until the clock catches up.
    #[test]
    fn a_bridge_mints_above_the_epoch_the_journal_holds() {
        let (_dir, broker) = broker();
        let far_ahead = 1 << 62; // an epoch no clock reaches
        let former = NativeBridge::with_producer_epoch(
            Arc::clone(&broker),
            config(Durability::LocalFsync),
            far_ahead,
        );
        former.produce("events", &[batch(&[("a", None)])]).unwrap();
        assert_eq!(
            broker.producer_epoch_of(Uuid::from_u128(0xabc)),
            Some(far_ahead)
        );
        let promoted = bridge(Arc::clone(&broker));
        assert!(
            promoted.producer_epoch() > far_ahead,
            "above the journal, not the clock"
        );
        assert_eq!(
            promoted
                .produce("events", &[batch(&[("b", None)])])
                .unwrap()
                .base_offset,
            1,
            "appends instead of being fenced"
        );
        // No room above the journal: refused, never the same epoch again.
        assert_eq!(
            mint_producer_epoch_above(Some(u64::MAX)),
            Err(EpochsExhausted)
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

    /// A promotion marker the broker filters from consumer output is not a
    /// place a Kafka consumer can be parked (review): the fetch follows the
    /// cursor past it to the next visible record. The marker is written
    /// into the log directly — a standalone broker publishes none — under
    /// the reserved producer the consumer path filters on.
    #[test]
    fn a_fetch_follows_the_cursor_past_a_filtered_marker() {
        let record = |producer_id: Uuid, sequence: u64, value: &str| LogRecord {
            producer_id,
            producer_epoch: 0,
            sequence,
            timestamp_millis: 0,
            attributes: 0,
            key: Vec::new(),
            value: value.as_bytes().to_vec(),
        };
        let writer = Uuid::from_u128(0xabc);
        let (_dir, broker) = broker_over(vec![
            record(writer, 0, "a"),
            record(
                vtop_broker::PROMOTION_MARKER_PRODUCER,
                0,
                "promotion-boundary",
            ),
            record(writer, 1, "b"),
        ]);
        let bridge = bridge(Arc::clone(&broker));
        assert_eq!(bridge.bounds("events").unwrap(), (0, 3));
        // From the marker's offset, with a budget that admits nothing past
        // the marker itself: the record after it, not an empty set.
        let fetched = bridge.fetch("events", 1, 1).unwrap();
        let decoded = RecordBatch::decode(&fetched.records).unwrap();
        assert_eq!(decoded.base_offset, 2, "the record after the marker");
        assert_eq!(decoded.records[0].value.as_deref(), Some(b"b".as_slice()));
        assert_eq!(
            decoded.records.len(),
            1,
            "the widened hop hands back the client's budget: one record, not the log"
        );
        // From the start: both visible records, the marker absent.
        let all =
            RecordBatch::decode(&bridge.fetch("events", 0, 1 << 20).unwrap().records).unwrap();
        assert_eq!(all.records.len(), 2);
        assert_eq!(all.base_offset + all.records[1].offset, 2);
        // At the watermark: nothing, and no error.
        assert!(bridge
            .fetch("events", 3, 1 << 20)
            .unwrap()
            .records
            .is_empty());
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

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

use crate::bridge::{Appended, Bridge, Fetched, Sequenced};
use crate::messages::ErrorCode;
use crate::records::{Record, RecordBatch};
use crate::turnstile::Turnstile;
use std::collections::HashMap;
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
    /// One turnstile per derived producer with an append in flight or queued
    /// (review): a sequenced set that the plane splits into several native
    /// appends holds its producer's turn for ALL of them. The gateway
    /// answers a timed-out produce and reads the session's next request
    /// while this task is still appending; a later set of the same producer
    /// that reached the broker between two chunks would meet a gap in its
    /// own sequences — fatal to an idempotent client — so it queues behind
    /// the whole append instead. And it queues IN ARRIVAL ORDER (review): a
    /// mutex hands its lock to waiters in no order, so two sets queued
    /// behind a running append could reach the broker reversed — a
    /// reordering the client never made, and one it would read as fatal. A
    /// ticket is taken on arrival and served in ticket order instead. Keyed
    /// by the full derived id, so no two producers wait on each other, and an
    /// entry is removed once no ticket is outstanding, so the map is bounded
    /// by the appends under way and not by the producers ever seen.
    producer_serials: std::sync::Mutex<HashMap<Uuid, Arc<Turnstile>>>,
    /// Producers whose last set failed part-way; see [`ProducerGaps`].
    gaps: std::sync::Mutex<ProducerGaps>,
    request_id: AtomicU64,
}

/// Producers whose last set failed for a reason a client retries (review):
/// the first sequence that did NOT land, per derived producer — whether the
/// failure struck before any chunk landed or after some did. A later set of
/// that producer arriving before the retry would meet a gap the broker calls
/// out of order — a code a client reads as fatal unless its own bookkeeping
/// explains it — so until the retry lands, such a set is told to try again
/// instead. A permanent refusal (a malformed record, a sequence the client
/// got wrong) remembers nothing: the client will not retry it, and holding
/// its later sets would be a hang where a clear refusal is owed. Bounded: a
/// producer that never retries (a client that gave up and bumped its epoch,
/// which is a new identity here) is evicted oldest-first past the cap.
#[derive(Default)]
struct ProducerGaps {
    at: HashMap<Uuid, u64>,
    order: std::collections::VecDeque<Uuid>,
}

/// Producers remembered as broken, at most.
const MAX_REMEMBERED_GAPS: usize = 4096;

impl ProducerGaps {
    /// `producer`'s set failed with `first_missing` the first sequence that
    /// did not land.
    fn note(&mut self, producer: Uuid, first_missing: u64) {
        // A producer failing again is the NEWEST, not the oldest (review): it
        // is still retrying, and the eviction must fall on one that stopped.
        if self.at.insert(producer, first_missing).is_some() {
            self.order.retain(|p| *p != producer);
        }
        self.order.push_back(producer);
        while self.order.len() > MAX_REMEMBERED_GAPS {
            if let Some(oldest) = self.order.pop_front() {
                self.at.remove(&oldest);
            }
        }
    }

    /// The sequence a set starting at `first_sequence` would leave unfilled,
    /// if `producer` is broken before it.
    fn blocks(&self, producer: Uuid, first_sequence: u64) -> Option<u64> {
        self.at
            .get(&producer)
            .copied()
            .filter(|missing| first_sequence > *missing)
    }

    /// A set covering `[first_sequence, end)` landed: a gap inside it is
    /// filled.
    fn settle(&mut self, producer: Uuid, first_sequence: u64, end: u64) {
        let filled = self
            .at
            .get(&producer)
            .is_some_and(|missing| (first_sequence..end).contains(missing));
        if filled {
            self.at.remove(&producer);
            self.order.retain(|p| *p != producer);
        }
    }
}

/// A chunk the broker refused: the code for the client, and the first
/// sequence that did not land.
struct ChunkFailure {
    code: ErrorCode,
    first_missing: u64,
}

/// The codes a producer retries with the same sequences (review): the
/// transient ones this bridge answers. A gap is remembered for exactly these
/// — the retry is coming, and the sets behind it must wait for it — and for
/// nothing else.
fn a_client_retries(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::RequestTimedOut | ErrorCode::NotLeaderOrFollower
    )
}

/// What appending a set's chunks came to.
struct ChunkOutcome {
    /// The first chunk's offset — `None` when every chunk was below the log's
    /// retry window, which answers with no offset at all.
    base_offset: Option<i64>,
    /// Chunks the log already held, whole.
    duplicates: usize,
    /// A chunk the log answered as below its retry window: persisted, and
    /// the set can only be acknowledged as delivered, without an offset.
    below_window: bool,
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
            producer_serials: std::sync::Mutex::new(HashMap::new()),
            gaps: std::sync::Mutex::new(ProducerGaps::default()),
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

/// The native identity an idempotent Kafka producer appends as (#457): a
/// UUID derived from the RANGE (the namespace) and the client's id and
/// epoch, under a domain tag, so it can never collide with the principal's or
/// with the shared space — and it is the same on every replica of the range
/// (review): a client keeps its id, epoch and next sequence across a leader
/// failover, and the new leader must find the same producer in the log it
/// inherited, not a stranger whose first sequence is not zero. A gateway
/// restart changes nothing the log remembers for the same reason. Its
/// native epoch is zero: a Kafka client's epoch is part of the name, not a
/// fence to raise.
fn derived_producer(range_id: Uuid, sequenced: Sequenced) -> Uuid {
    let mut name = [0_u8; 26];
    name[..16].copy_from_slice(b"kafka-idempotent");
    name[16..24].copy_from_slice(&sequenced.producer_id.to_be_bytes());
    name[24..].copy_from_slice(&sequenced.producer_epoch.to_be_bytes());
    Uuid::new_v5(&range_id, &name)
}

/// The broker answers every sequence fault with one code and the log's own
/// words; a Kafka client needs them apart. Below the retry window is a set
/// the log persisted and can no longer verify — delivered, to the client.
/// A reused sequence with other bytes is a client's bug, refused as the
/// record it is. Everything else — a gap, a first sequence not zero — is
/// out of order, and fatal for that producer as it should be.
fn sequence_code(message: &str) -> ErrorCode {
    if message.contains("retry window") {
        ErrorCode::DuplicateSequenceNumber
    } else if message.contains("different record content") {
        ErrorCode::InvalidRecord
    } else {
        ErrorCode::OutOfOrderSequenceNumber
    }
}

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

impl NativeBridge {
    /// The producer's turnstile entry goes once no ticket is outstanding.
    /// Under the map lock, so a producer arriving now either finds the entry
    /// and takes a ticket (and it stays) or finds none and makes a fresh one
    /// after ours is gone.
    fn release_serial(&self, serial: Option<(Uuid, Arc<Turnstile>, u64)>) {
        if let Some((derived, entry, _)) = serial {
            let mut serials = self
                .producer_serials
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if entry.idle() {
                serials.remove(&derived);
            }
        }
    }

    /// The set's chunks, in order, as native appends under one identity.
    /// A chunk the log answers as below its retry window (review) is NOT the
    /// end of the set: the producer's sequences are contiguous and the log's
    /// frontier is a whole window past that chunk, so the chunk was
    /// persisted — the loop notes it and goes on, so a later chunk that is
    /// still missing (a retry after a failure part-way) is appended now
    /// rather than left behind an acknowledgement that called it delivered.
    /// Any other refusal fails the set, and the client's retry reaches the
    /// suffix.
    fn append_chunks(
        &self,
        appends: Vec<Vec<ProduceRecord>>,
        producer_id: Uuid,
        producer_epoch: u64,
        mut first_sequence: u64,
        shared: &mut Option<std::sync::MutexGuard<'_, SequenceSpace>>,
        sequenced: Option<Sequenced>,
    ) -> Result<ChunkOutcome, ChunkFailure> {
        let mut outcome = ChunkOutcome {
            base_offset: None,
            duplicates: 0,
            below_window: false,
        };
        for (which, records) in appends.into_iter().enumerate() {
            let count = records.len() as u64;
            let request = ProduceRequest {
                range: self.broker.range().clone(),
                fencing_epoch: self.broker.held_fencing_epoch(),
                producer_id,
                producer_epoch,
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
                        .ok_or(ChunkFailure {
                            code: ErrorCode::InvalidRecord,
                            first_missing: first_sequence,
                        })?;
                    if response.outcomes.iter().all(|outcome| outcome.duplicate) {
                        outcome.duplicates += 1;
                    }
                    outcome.base_offset.get_or_insert(offset);
                    first_sequence += count;
                    if let Some(guard) = shared.as_mut() {
                        guard.next = first_sequence;
                    }
                }
                Message::Error(error) => {
                    let code = match (sequenced, error.code) {
                        (Some(_), NativeCode::SequenceConflict) => sequence_code(&error.message),
                        (_, code) => kafka_code(code),
                    };
                    if code == ErrorCode::DuplicateSequenceNumber {
                        tracing::debug!(
                            which,
                            first_sequence,
                            "native append below the retry window: persisted; the set goes on"
                        );
                        outcome.below_window = true;
                        first_sequence += count;
                        continue;
                    }
                    tracing::warn!(
                        code = ?error.code,
                        message = %error.message,
                        which,
                        sequenced = sequenced.is_some(),
                        "native produce refused"
                    );
                    return Err(ChunkFailure {
                        code,
                        first_missing: first_sequence,
                    });
                }
                other => {
                    tracing::warn!(?other, "native produce answered with an unexpected message");
                    return Err(ChunkFailure {
                        code: ErrorCode::InvalidRecord,
                        first_missing: first_sequence,
                    });
                }
            }
        }
        Ok(outcome)
    }
}

impl Bridge for NativeBridge {
    fn topics(&self) -> Vec<String> {
        vec![self.config.topic.clone()]
    }

    fn produce(
        &self,
        topic: &str,
        batches: &[RecordBatch],
        sequenced: Option<Sequenced>,
    ) -> Result<Appended, ErrorCode> {
        self.known(topic)?;
        if let Some(sequenced) = sequenced {
            if sequenced.producer_epoch < 0 {
                return Err(ErrorCode::InvalidProducerEpoch);
            }
            if sequenced.first_sequence < 0 {
                return Err(ErrorCode::OutOfOrderSequenceNumber);
            }
        }
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
        // Two sequence spaces (#457). WITHOUT an identity, the set is the
        // bridge's own producer's: reserved and spent under ONE lock
        // (review) — the broker requires contiguous sequences in arrival
        // order, so no other produce may reach it between this reservation
        // and this append, and the reservation stands only once the broker
        // took it, so a refused append leaves no hole for the next one to
        // trip over. WITH one, the set is an idempotent Kafka producer's: it
        // appends as an identity derived from that producer, with the
        // client's own sequences, and the log's per-record duplicate check
        // is the idempotence — a retry, whole or after a partial failure,
        // is answered with the offsets the records already have. The client
        // orders its own sequences; what this side must order is its own
        // chunked appends against the client's NEXT set (review): the
        // producer's stripe lock is held for every chunk of this set, so a
        // set that arrives while a timed-out one is still appending queues
        // behind it rather than meeting a gap.
        let mut shared = None;
        let mut serial = None;
        let (producer_id, producer_epoch, first_sequence) = match sequenced {
            None => {
                let guard = self
                    .sequences
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let next = guard.next;
                shared = Some(guard);
                (self.config.producer_id, self.producer_epoch, next)
            }
            Some(sequenced) => {
                let derived = derived_producer(self.broker.range().range_id, sequenced);
                // The ticket is taken under the map lock, so arrival at the
                // map is arrival at the turnstile.
                let (entry, ticket) = {
                    let mut serials = self
                        .producer_serials
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let entry = Arc::clone(serials.entry(derived).or_default());
                    let ticket = entry.enter();
                    (entry, ticket)
                };
                serial = Some((derived, entry, ticket));
                (derived, 0, sequenced.first_sequence as u64)
            }
        };
        let turn = serial
            .as_ref()
            .map(|(_, entry, ticket)| entry.wait_turn(*ticket));
        let total: u64 = appends.iter().map(|chunk| chunk.len() as u64).sum();
        // A producer whose last set failed part-way (review): a set past the
        // missing sequence is told to try again rather than handed to the
        // broker to be called out of order; the retry of the broken set is
        // not past it, and lands.
        if let Some(sequenced) = sequenced {
            let blocked = self
                .gaps
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .blocks(producer_id, first_sequence);
            if let Some(missing) = blocked {
                tracing::warn!(
                    producer_id = sequenced.producer_id,
                    producer_epoch = sequenced.producer_epoch,
                    first_sequence = sequenced.first_sequence,
                    missing,
                    "native produce held: an earlier set of this producer failed part-way at \
                     sequence {missing}; sets past it are told to retry until its retry lands"
                );
                drop(turn);
                self.release_serial(serial);
                return Err(ErrorCode::RequestTimedOut);
            }
        }
        let outcome = self.append_chunks(
            appends,
            producer_id,
            producer_epoch,
            first_sequence,
            &mut shared,
            sequenced,
        );
        if sequenced.is_some() {
            let mut gaps = self
                .gaps
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match &outcome {
                Ok(_) => gaps.settle(producer_id, first_sequence, first_sequence + total),
                // A retryable failure, with or without a landed prefix
                // (review): the retry is coming, and the sets queued behind
                // it wait. A permanent one remembers nothing.
                Err(failure) if a_client_retries(failure.code) => {
                    gaps.note(producer_id, failure.first_missing)
                }
                Err(_) => {}
            }
        }
        drop(turn);
        self.release_serial(serial);
        let outcome = outcome.map_err(|failure| failure.code)?;
        if let Some(sequenced) = sequenced {
            if outcome.duplicates > 0 || outcome.below_window {
                tracing::info!(
                    producer_id = sequenced.producer_id,
                    producer_epoch = sequenced.producer_epoch,
                    first_sequence = sequenced.first_sequence,
                    appends = outcome.duplicates,
                    below_window = outcome.below_window,
                    base_offset = outcome.base_offset.unwrap_or(-1),
                    "idempotent retry acknowledged with its original offset (#457)"
                );
            }
            if outcome.below_window {
                // Every chunk is persisted now — the ones below the window
                // were, the rest just landed or were held — and the set is
                // delivered, which is what this code says to a client. It
                // carries no offset, and the window left us none to give.
                return Err(ErrorCode::DuplicateSequenceNumber);
            }
        }
        Ok(Appended {
            base_offset: outcome.base_offset.ok_or(ErrorCode::InvalidRecord)?,
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
        let appended = bridge.produce("events", &[batch(&records)], None).unwrap();
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
        assert!(bridge.produce("events", &[batch(&records)], None).is_ok());
        assert_eq!(bridge.bounds("events").unwrap(), (0, 10));
        let big = "x".repeat(64);
        assert_eq!(
            bridge
                .produce("events", &[batch(&[(big.as_str(), None)])], None)
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
    /// An idempotent producer's set is appended once however often it is
    /// retried (#457): whole, across the chunks the plane forces, and by a
    /// bridge built later over the same log — the identity and the
    /// sequences are the client's, the duplicate check is the log's. A gap,
    /// a first sequence not zero, and a reused sequence with other bytes
    /// are refused by the code a client acts on; the shared space is
    /// untouched beside it.
    #[test]
    fn an_idempotent_set_is_appended_once_however_often_it_is_retried() {
        let (_dir, broker) = broker();
        let bridge = NativeBridge::new(
            Arc::clone(&broker),
            NativeBridgeConfig {
                max_append_records: 2,
                ..config(Durability::LocalFsync)
            },
        )
        .unwrap();
        let of = |producer_id: i64, first_sequence: i32| {
            Some(Sequenced {
                producer_id,
                producer_epoch: 0,
                first_sequence,
            })
        };
        let values = ["a", "b", "c", "d", "e"];
        let records: Vec<(&str, Option<&str>)> = values.iter().map(|v| (*v, None)).collect();
        let set = [batch(&records)];
        assert_eq!(
            bridge
                .produce("events", &set, of(9, 0))
                .unwrap()
                .base_offset,
            0
        );
        assert_eq!(
            bridge.bounds("events").unwrap(),
            (0, 5),
            "three native appends"
        );
        assert_eq!(
            bridge
                .produce("events", &set, of(9, 0))
                .unwrap()
                .base_offset,
            0,
            "the retry is the original offset"
        );
        assert_eq!(
            bridge.bounds("events").unwrap(),
            (0, 5),
            "and appended nothing"
        );
        assert_eq!(
            bridge
                .produce("events", &[batch(&[("f", None)])], of(9, 5))
                .unwrap()
                .base_offset,
            5,
            "the next sequence appends"
        );
        assert_eq!(
            bridge.produce("events", &[batch(&[("g", None)])], of(9, 9)),
            Err(ErrorCode::OutOfOrderSequenceNumber),
            "a gap"
        );
        assert_eq!(
            bridge.produce("events", &[batch(&[("h", None)])], of(10, 3)),
            Err(ErrorCode::OutOfOrderSequenceNumber),
            "a new producer starts at zero"
        );
        assert_eq!(
            bridge
                .produce("events", &[batch(&[("h", None)])], of(10, 0))
                .unwrap()
                .base_offset,
            6,
            "another producer's own space"
        );
        assert_eq!(
            bridge.produce("events", &[batch(&[("H", None)])], of(10, 0)),
            Err(ErrorCode::InvalidRecord),
            "the same sequence with other bytes is the client's bug"
        );
        assert_eq!(
            bridge
                .produce("events", &[batch(&[("i", None)])], None)
                .unwrap()
                .base_offset,
            7,
            "the shared space beside them"
        );
        assert_eq!(
            bridge.produce("events", &set, of(9, -1)),
            Err(ErrorCode::OutOfOrderSequenceNumber)
        );
        assert_eq!(
            bridge.produce(
                "events",
                &set,
                Some(Sequenced {
                    producer_id: 9,
                    producer_epoch: -1,
                    first_sequence: 0
                })
            ),
            Err(ErrorCode::InvalidProducerEpoch)
        );
        // A bridge built later over the same log — the gateway restarted —
        // knows the same producers: the state is the log's, not the bridge's.
        drop(bridge);
        let rebuilt =
            NativeBridge::new(Arc::clone(&broker), config(Durability::LocalFsync)).unwrap();
        assert_eq!(
            rebuilt
                .produce("events", &set, of(9, 0))
                .unwrap()
                .base_offset,
            0,
            "a retry after a restart is still the original offset"
        );
        assert_eq!(rebuilt.bounds("events").unwrap(), (0, 8));
    }

    /// A set the plane splits is one producer's appends, whole (review): a
    /// later set of the same producer that arrives while the chunks are
    /// still going in waits for all of them, and lands after — never between
    /// two chunks, where its own sequences would be a gap. One-record chunks
    /// and a racing second set, many times: the second set always lands at
    /// the offset after the first, and is never refused.
    #[test]
    fn a_later_set_of_the_same_producer_waits_for_a_chunked_append_to_finish() {
        let (_dir, broker) = broker();
        let bridge = Arc::new(
            NativeBridge::new(
                Arc::clone(&broker),
                NativeBridgeConfig {
                    max_append_records: 1,
                    ..config(Durability::LocalFsync)
                },
            )
            .unwrap(),
        );
        let mut next_sequence = 0_i32;
        let mut next_offset = 0_i64;
        for round in 0..20_i64 {
            let first: Vec<(String, Option<&str>)> =
                (0..40).map(|i| (format!("r{round}-{i}"), None)).collect();
            let first: Vec<(&str, Option<&str>)> =
                first.iter().map(|(k, v)| (k.as_str(), *v)).collect();
            let first_set = [batch(&first)];
            let second_set = [batch(&[("tail", None)])];
            let racer = Arc::clone(&bridge);
            let second_sequence = next_sequence + 40;
            let a = std::thread::spawn({
                let bridge = Arc::clone(&bridge);
                let first_sequence = next_sequence;
                move || {
                    bridge.produce(
                        "events",
                        &first_set,
                        Some(Sequenced {
                            producer_id: 9,
                            producer_epoch: 0,
                            first_sequence,
                        }),
                    )
                }
            });
            let b = std::thread::spawn(move || {
                // Arrives while the first set's forty appends are under way
                // (or just before: then it is a gap of its own making, and
                // the retry below is what a client would do).
                racer.produce(
                    "events",
                    &second_set,
                    Some(Sequenced {
                        producer_id: 9,
                        producer_epoch: 0,
                        first_sequence: second_sequence,
                    }),
                )
            });
            let first = a.join().unwrap().unwrap();
            let second = b.join().unwrap();
            assert_eq!(first.base_offset, next_offset);
            match second {
                Ok(appended) => assert_eq!(
                    appended.base_offset,
                    next_offset + 40,
                    "round {round}: the second set landed after the whole first set"
                ),
                Err(ErrorCode::OutOfOrderSequenceNumber) => {
                    // It reached the broker before the first set's FIRST
                    // chunk took the lock — a gap in the client's own order,
                    // which the lock cannot and should not repair. Its
                    // retry, as a client's would, lands after.
                    let retried = bridge
                        .produce(
                            "events",
                            &[batch(&[("tail", None)])],
                            Some(Sequenced {
                                producer_id: 9,
                                producer_epoch: 0,
                                first_sequence: second_sequence,
                            }),
                        )
                        .unwrap();
                    assert_eq!(retried.base_offset, next_offset + 40);
                }
                Err(other) => panic!("round {round}: refused with {other:?}"),
            }
            assert_eq!(
                bridge.bounds("events").unwrap().1,
                next_offset + 41,
                "round {round}: forty-one records, none twice"
            );
            next_sequence += 41;
            next_offset += 41;
        }
    }

    /// A retry whose first chunks fell below the log's retry window still
    /// lands the suffix it was retrying for (review): one set of a window
    /// plus 464 records is appended; the same producer then retries a set of
    /// a window plus 4 464 records from sequence zero — as a client would
    /// after a failure part-way through the bigger set. Its first chunks are
    /// below the window, the middle ones duplicates, the last 4 000 records
    /// are missing and land; the set answers "delivered", and the log ends
    /// at the bigger set's length, not the smaller one's.
    #[test]
    fn a_retry_that_fell_below_the_window_still_lands_its_missing_suffix() {
        let window = vtop_log::PRODUCER_SEQUENCE_WINDOW as usize;
        let (_dir, broker) = broker();
        let bridge = NativeBridge::new(
            Arc::clone(&broker),
            NativeBridgeConfig {
                max_append_records: 4096,
                max_append_bytes: 64 << 20,
                // The broker acknowledges only fsync or quorum produces: a
                // chunk is one fsync, and there are a few dozen of them here.
                ..config(Durability::LocalFsync)
            },
        )
        .unwrap();
        let values: Vec<String> = (0..window + 4_464).map(|i| format!("r{i}")).collect();
        let of = |first_sequence: i32| {
            Some(Sequenced {
                producer_id: 9,
                producer_epoch: 0,
                first_sequence,
            })
        };
        let prefix: Vec<(&str, Option<&str>)> = values[..window + 464]
            .iter()
            .map(|v| (v.as_str(), None))
            .collect();
        assert_eq!(
            bridge
                .produce("events", &[batch(&prefix)], of(0))
                .unwrap()
                .base_offset,
            0
        );
        assert_eq!(bridge.bounds("events").unwrap().1, (window + 464) as i64);
        let whole: Vec<(&str, Option<&str>)> = values.iter().map(|v| (v.as_str(), None)).collect();
        assert_eq!(
            bridge.produce("events", &[batch(&whole)], of(0)),
            Err(ErrorCode::DuplicateSequenceNumber),
            "delivered — and only now true"
        );
        assert_eq!(
            bridge.bounds("events").unwrap().1,
            (window + 4_464) as i64,
            "the missing suffix landed"
        );
        assert_eq!(
            bridge.produce("events", &[batch(&whole)], of(0)),
            Err(ErrorCode::DuplicateSequenceNumber),
            "and again: nothing more appended"
        );
        assert_eq!(bridge.bounds("events").unwrap().1, (window + 4_464) as i64);
    }

    /// The same client is the same identity to a bridge on another node
    /// (review): after a leader failover the client keeps its id, epoch and
    /// next sequence, and the new leader's bridge — a different gateway
    /// producer over the same replicated log — must find the same producer
    /// in it. A retry is its original offset; the client's next set appends.
    #[test]
    fn the_same_client_is_the_same_identity_to_a_bridge_on_another_node() {
        let (_dir, broker) = broker();
        let of = |first_sequence: i32| {
            Some(Sequenced {
                producer_id: 9,
                producer_epoch: 0,
                first_sequence,
            })
        };
        let set = [batch(&[("a", None), ("b", None)])];
        let old_leader =
            NativeBridge::new(Arc::clone(&broker), config(Durability::LocalFsync)).unwrap();
        assert_eq!(
            old_leader
                .produce("events", &set, of(0))
                .unwrap()
                .base_offset,
            0
        );
        drop(old_leader);
        let new_leader = NativeBridge::new(
            Arc::clone(&broker),
            NativeBridgeConfig {
                producer_id: Uuid::from_u128(0xdef),
                ..config(Durability::LocalFsync)
            },
        )
        .unwrap();
        assert_eq!(
            new_leader
                .produce("events", &set, of(0))
                .unwrap()
                .base_offset,
            0,
            "the retry is the original offset on the new leader"
        );
        assert_eq!(
            new_leader
                .produce("events", &[batch(&[("c", None)])], of(2))
                .unwrap()
                .base_offset,
            2,
            "the client's next set appends"
        );
        assert_eq!(new_leader.bounds("events").unwrap(), (0, 3));
    }

    /// A producer broken part-way holds its later sets and frees them when
    /// the retry lands (review): a set past the missing sequence is blocked,
    /// the retry of the broken set and anything before the gap is not, a
    /// landed set covering the gap settles it, and the memory is bounded.
    #[test]
    fn a_producer_broken_part_way_holds_its_later_sets_until_the_retry_lands() {
        let mut gaps = ProducerGaps::default();
        let p = Uuid::from_u128(1);
        assert_eq!(gaps.blocks(p, 100), None, "nothing broken");
        gaps.note(p, 40);
        assert_eq!(gaps.blocks(p, 100), Some(40), "past the gap: held");
        assert_eq!(
            gaps.blocks(p, 40),
            None,
            "the retry of the broken set itself"
        );
        assert_eq!(gaps.blocks(p, 0), None, "a retry from before the gap");
        assert_eq!(
            gaps.blocks(Uuid::from_u128(2), 100),
            None,
            "another producer"
        );
        gaps.settle(p, 60, 80);
        assert_eq!(
            gaps.blocks(p, 100),
            Some(40),
            "a set that did not cover the gap settles nothing"
        );
        gaps.settle(p, 0, 50);
        assert_eq!(gaps.blocks(p, 100), None, "the gap is filled");
        for i in 0..(MAX_REMEMBERED_GAPS as u128 + 10) {
            gaps.note(Uuid::from_u128(1000 + i), 1);
        }
        assert_eq!(gaps.at.len(), MAX_REMEMBERED_GAPS, "bounded");
        assert_eq!(
            gaps.blocks(Uuid::from_u128(1000), 5),
            None,
            "the oldest was evicted"
        );
        assert_eq!(
            gaps.blocks(Uuid::from_u128(1000 + 10), 5),
            Some(1),
            "the newer ones stay"
        );
        // A producer still failing is re-noted and becomes the newest: the
        // cap falls on one that stopped, never on it.
        let retrying = Uuid::from_u128(1000 + 11);
        gaps.note(retrying, 1);
        for i in 0..MAX_REMEMBERED_GAPS as u128 {
            gaps.note(Uuid::from_u128(50_000 + i), 1);
            if i % 512 == 0 {
                gaps.note(retrying, 2);
            }
        }
        assert_eq!(
            gaps.blocks(retrying, 9),
            Some(2),
            "re-noted through the churn, still remembered"
        );
        assert_eq!(gaps.at.len(), MAX_REMEMBERED_GAPS);
        // Only a failure the client retries is a gap worth remembering.
        assert!(a_client_retries(ErrorCode::RequestTimedOut));
        assert!(a_client_retries(ErrorCode::NotLeaderOrFollower));
        assert!(!a_client_retries(ErrorCode::InvalidRecord));
        assert!(!a_client_retries(ErrorCode::OutOfOrderSequenceNumber));
        assert!(!a_client_retries(ErrorCode::MessageTooLarge));
    }

    #[test]
    fn a_refused_append_does_not_spend_a_sequence() {
        let (_dir, broker) = broker();
        // Quorum on a standalone range is refused by the broker before any
        // append: the one refusal a fixture can produce on demand.
        let refused =
            NativeBridge::with_producer_epoch(Arc::clone(&broker), config(Durability::Quorum), 77);
        assert!(refused
            .produce("events", &[batch(&[("a", None)])], None)
            .is_err());
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
                .produce("events", &[batch(&[("a", None), ("b", None)])], None)
                .unwrap()
                .base_offset,
            0
        );
        assert_eq!(accepted.next_sequence(), 2);
        assert_eq!(
            accepted
                .produce("events", &[batch(&[("c", None)])], None)
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
            .produce("events", &[batch(&[("a", None), ("b", None)])], None)
            .unwrap();
        let second = bridge(Arc::clone(&broker));
        assert!(
            second.producer_epoch() > first.producer_epoch(),
            "strictly later"
        );
        assert_eq!(second.next_sequence(), 0);
        assert_eq!(
            second
                .produce("events", &[batch(&[("c", None)])], None)
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
        former
            .produce("events", &[batch(&[("a", None)])], None)
            .unwrap();
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
                .produce("events", &[batch(&[("b", None)])], None)
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
            .produce("events", &[batch(&[("a", Some("k1")), ("b", None)])], None)
            .unwrap();
        assert_eq!(first.base_offset, 0);
        let second = bridge
            .produce("events", &[batch(&[("c", None)])], None)
            .unwrap();
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
            bridge.produce("events", &[tombstone], None).unwrap_err(),
            ErrorCode::InvalidRecord
        );
        assert_eq!(
            bridge
                .produce("events", &[batch(&[("a", Some(""))])], None)
                .unwrap_err(),
            ErrorCode::InvalidRecord
        );
        assert_eq!(bridge.bounds("events").unwrap(), (0, 0), "nothing landed");
        bridge
            .produce("events", &[batch(&[("a", None), ("b", Some("k"))])], None)
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
                None,
            )
            .unwrap();
        assert_eq!(appended.base_offset, 0);
        assert_eq!(bridge.next_sequence(), 3);
        assert_eq!(bridge.bounds("events").unwrap(), (0, 3));
        let mut empty = batch(&[("d", None)]);
        empty.records.clear();
        assert_eq!(
            bridge
                .produce("events", &[batch(&[("d", None)]), empty], None)
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
                .produce("other", &[batch(&[("a", None)])], None)
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

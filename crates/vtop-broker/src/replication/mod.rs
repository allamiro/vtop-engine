//! Leader→follower replication for quorum durability.
//!
//! This slice implements the Stage-6 produce path from the architecture:
//! leader local append → replica append with fencing epoch → quorum durable
//! acknowledgements → advance and propagate the cluster committed high-water
//! mark.
//!
//! [`InProcessReplicaSet`] is the deterministic harness backend. Production
//! wiring uses [`network::NetworkedReplicaSet`]: persistent mTLS streams,
//! pipelined batches, per-follower flow-control windows, reconnect, and a
//! bounded retransmission buffer for basic catch-up. Full sealed-segment
//! transfer / repair remains a follow-up.
//!
//! [`fault::FaultInjectingReplicaSet`] layers controllable network delivery
//! faults (loss / duplicate / reorder / delay) over the in-process set for
//! the distributed data-plane fault harness (#188). Disk faults stay on
//! [`vtop_log::sim`] and are injected independently.

pub mod fault;
pub mod network;

use crate::{
    storage_producer_id, BrokerError, BrokerResult, MetaFencingEpoch, MetaLeaseState,
    ProducerEpochJournal, SegmentFormat,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use vtop_log::{ActiveSegment, Durability, FetchBatch, LogRecord};
use vtop_protocol::{
    CommittedHwmUpdate, ErrorCode, ProduceRecord, RangeIdentity, ReplicaAppendRequest,
    ReplicaAppendResponse,
};

pub use fault::{
    FaultInjectingReplicaSet, FollowerNetworkFault, NetworkFaultPlan, PendingDeliveryStats,
};
pub use network::{
    FlowControlConfig, NetworkFollowerConfig, NetworkedReplicaSet, ReplicaPeerHandler,
    ReplicaPeerServer, ReplicaStatusClient, ReplicaTlsMaterial,
};

/// Shared quorum-committed high-water mark for a range.
///
/// Advanced only after a majority of replicas (including the leader) report
/// local durability through the offset. Fetch paths clamp visibility here.
#[derive(Clone, Debug)]
pub struct ClusterCommittedOffset {
    state: Arc<Mutex<u64>>,
}

impl ClusterCommittedOffset {
    pub fn new(offset: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(offset)),
        }
    }

    pub fn get(&self) -> u64 {
        *self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Monotonically advance the watermark. Returns the resulting value.
    pub fn advance_to(&self, offset: u64) -> u64 {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if offset > *state {
            *state = offset;
        }
        *state
    }
}

/// Result of fanning a locally durable leader append out to followers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaQuorumResult {
    /// Followers that durably applied through the leader's local commit point.
    pub follower_acks: usize,
    /// Replication factor including the leader.
    pub replication_factor: usize,
}

impl ReplicaQuorumResult {
    pub fn majority(&self) -> usize {
        self.replication_factor / 2 + 1
    }

    /// Leader local durability counts as one ack.
    pub fn has_quorum(&self) -> bool {
        1 + self.follower_acks >= self.majority()
    }
}

/// Fan-out surface used by the leader produce path.
pub trait ReplicaSet: Send + Sync {
    fn replication_factor(&self) -> usize;

    /// Replicate `request` to followers and count durable acks that cover
    /// `leader_committed_offset`.
    fn replicate_append(
        &self,
        request: &ReplicaAppendRequest,
        leader_committed_offset: u64,
    ) -> ReplicaQuorumResult {
        self.replicate_append_batch(std::slice::from_ref(request), leader_committed_offset)
    }

    /// Replicate an ordered multi-producer commit group with one durability
    /// barrier per follower when the implementation can do so.
    fn replicate_append_batch(
        &self,
        requests: &[ReplicaAppendRequest],
        leader_committed_offset: u64,
    ) -> ReplicaQuorumResult;

    fn propagate_committed_hwm(&self, update: &CommittedHwmUpdate);
}

struct FollowerState {
    segment: ActiveSegment,
    producer_epochs: ProducerEpochJournal,
}

/// Deterministic in-process follower replica.
pub struct InProcessFollower {
    node_id: Uuid,
    range: RangeIdentity,
    /// The epoch this follower currently serves.
    ///
    /// Atomic and monotonic rather than a fixed config value (#239): a
    /// follower whose epoch could never change was fenced out of its own
    /// quorum the moment metadata granted the range at a new epoch, so a
    /// lease-driven leader could not reach durability without every follower
    /// being restarted. It only ever moves forward — see
    /// [`Self::adopt_fencing_epoch`].
    held_fencing_epoch: AtomicU64,
    /// Which epoch wrote each stretch of this replica's log (#240). See
    /// [`crate::LocalBroker::epoch_starts`] for why absence and breakage are
    /// deliberately the same answer.
    fencing_epoch_journal: Mutex<Option<crate::fencing_epochs::FencingEpochJournal>>,
    fencing_epoch_history_broken: AtomicBool,
    meta_fencing_epoch: MetaFencingEpoch,
    segment_format: SegmentFormat,
    cluster_committed: ClusterCommittedOffset,
    state: Mutex<FollowerState>,
    online: AtomicBool,
    /// When set, appends land with [`Durability::Buffered`] and are not
    /// committed until [`Self::flush_held_fsync`]. Used by the data-plane
    /// fault harness to delay follower durability independently of network
    /// delivery faults.
    hold_fsync: AtomicBool,
}

impl InProcessFollower {
    pub fn new(
        node_id: Uuid,
        segment: ActiveSegment,
        producer_epochs: ProducerEpochJournal,
        range: RangeIdentity,
        held_fencing_epoch: u64,
        meta_fencing_epoch: MetaFencingEpoch,
        cluster_committed: ClusterCommittedOffset,
    ) -> BrokerResult<Self> {
        // Validate that the segment's embedded identity matches the range
        // this follower is being constructed for. A mismatch means the
        // caller supplied a segment for a different range, which would
        // silently accept appends under the wrong identity.
        let (seg_topic, seg_topic_epoch, seg_range_id, seg_generation) =
            if let Some(desc) = segment.descriptor_v2() {
                (
                    desc.topic.as_str(),
                    desc.topic_epoch,
                    desc.lineage.range_id,
                    desc.lineage.generation,
                )
            } else {
                let desc = segment.descriptor();
                (
                    desc.topic.as_str(),
                    desc.topic_epoch,
                    desc.lineage.range_id,
                    desc.lineage.generation,
                )
            };
        if seg_topic != range.topic
            || seg_topic_epoch != range.topic_epoch
            || seg_range_id != range.range_id
            || seg_generation != range.range_generation
        {
            return Err(BrokerError::InvalidConfig(format!(
                "follower segment identity ({seg_topic}, epoch {seg_topic_epoch}, \
                 {seg_range_id}, generation {seg_generation}) does not match range \
                 ({}, epoch {}, {}, generation {})",
                range.topic, range.topic_epoch, range.range_id, range.range_generation,
            )));
        }

        let segment_format = if segment.format_version() == vtop_log::FORMAT_VERSION_V2 {
            SegmentFormat::V2
        } else {
            SegmentFormat::V1
        };
        Ok(Self {
            node_id,
            range,
            held_fencing_epoch: AtomicU64::new(held_fencing_epoch),
            fencing_epoch_journal: Mutex::new(None),
            fencing_epoch_history_broken: AtomicBool::new(false),
            meta_fencing_epoch,
            segment_format,
            cluster_committed,
            state: Mutex::new(FollowerState {
                segment,
                producer_epochs,
            }),
            online: AtomicBool::new(true),
            hold_fsync: AtomicBool::new(false),
        })
    }

    pub fn node_id(&self) -> Uuid {
        self.node_id
    }

    pub fn cluster_committed(&self) -> &ClusterCommittedOffset {
        &self.cluster_committed
    }

    pub fn meta_fencing_epoch(&self) -> &MetaFencingEpoch {
        &self.meta_fencing_epoch
    }

    /// The epoch this follower currently serves.
    pub fn held_fencing_epoch(&self) -> u64 {
        self.held_fencing_epoch.load(Ordering::SeqCst)
    }

    /// Move this follower to `epoch`; returns whether it advanced.
    ///
    /// `fetch_max` makes adoption monotonic by construction, which is the
    /// safety property that matters: a follower that could be walked BACKWARD
    /// to a superseded epoch would start accepting appends from a leader
    /// metadata has already fenced, which is exactly the split-brain write the
    /// epoch exists to prevent. A stale observation is therefore a no-op
    /// rather than a regression, so the watcher driving this needs no ordering
    /// guarantees of its own.
    pub fn adopt_fencing_epoch(&self, epoch: u64) -> bool {
        let advanced = self.held_fencing_epoch.fetch_max(epoch, Ordering::SeqCst) < epoch;
        if advanced {
            // Durable before this replica writes anything under the epoch, for
            // the same reason as on a leader: a vector that loses its newest
            // entry in a crash is back to bare offsets exactly when the answer
            // matters.
            let next_offset = self.next_offset();
            let mut guard = self
                .fencing_epoch_journal
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(journal) = guard.as_mut() {
                if journal.record(epoch, next_offset).is_err() {
                    self.fencing_epoch_history_broken
                        .store(true, Ordering::SeqCst);
                }
            }
        }
        advanced
    }

    /// Install the durable epoch→offset vector for this replica (#240).
    pub fn set_fencing_epoch_journal(&self, journal: crate::fencing_epochs::FencingEpochJournal) {
        *self
            .fencing_epoch_journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(journal);
    }

    /// This replica's epoch history; empty means "unknown", never "no changes".
    pub fn epoch_starts(&self) -> Vec<crate::fencing_epochs::EpochStart> {
        if self.fencing_epoch_history_broken.load(Ordering::SeqCst) {
            return Vec::new();
        }
        self.fencing_epoch_journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|journal| journal.entries().to_vec())
            .unwrap_or_default()
    }

    pub fn set_online(&self, online: bool) {
        self.online.store(online, Ordering::SeqCst);
    }

    pub fn is_online(&self) -> bool {
        self.online.load(Ordering::SeqCst)
    }

    /// Delay promoting appended bytes to local durability until
    /// [`Self::flush_held_fsync`]. Orthogonal to [`Self::set_online`] and to
    /// network delivery faults on [`FaultInjectingReplicaSet`].
    pub fn set_hold_fsync(&self, hold: bool) {
        self.hold_fsync.store(hold, Ordering::SeqCst);
    }

    pub fn hold_fsync(&self) -> bool {
        self.hold_fsync.load(Ordering::SeqCst)
    }

    /// Commit any bytes held by [`Self::set_hold_fsync`].
    pub fn flush_held_fsync(&self) -> Result<u64, (ErrorCode, String)> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.segment.commit().map_err(|problem| {
            (
                ErrorCode::Storage,
                format!("follower {} flush_held_fsync: {problem}", self.node_id),
            )
        })
    }

    /// Replace on-disk state after a simulated crash/reboot recovery.
    ///
    /// Returns the previous handles so the caller can drop them before
    /// recovering the same paths. The follower Arc stays in the replica set.
    pub fn swap_storage(
        &self,
        segment: ActiveSegment,
        producer_epochs: ProducerEpochJournal,
    ) -> BrokerResult<(ActiveSegment, ProducerEpochJournal)> {
        let (seg_topic, seg_topic_epoch, seg_range_id, seg_generation) =
            if let Some(desc) = segment.descriptor_v2() {
                (
                    desc.topic.as_str(),
                    desc.topic_epoch,
                    desc.lineage.range_id,
                    desc.lineage.generation,
                )
            } else {
                let desc = segment.descriptor();
                (
                    desc.topic.as_str(),
                    desc.topic_epoch,
                    desc.lineage.range_id,
                    desc.lineage.generation,
                )
            };
        if seg_topic != self.range.topic
            || seg_topic_epoch != self.range.topic_epoch
            || seg_range_id != self.range.range_id
            || seg_generation != self.range.range_generation
        {
            return Err(BrokerError::InvalidConfig(format!(
                "recovered follower segment identity does not match range {}",
                self.range.topic
            )));
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let old_segment = std::mem::replace(&mut state.segment, segment);
        let old_epochs = std::mem::replace(&mut state.producer_epochs, producer_epochs);
        Ok((old_segment, old_epochs))
    }

    pub fn local_committed_offset(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .segment
            .committed_offset()
    }

    pub fn next_offset(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .segment
            .next_offset()
    }

    pub fn range(&self) -> &RangeIdentity {
        &self.range
    }

    /// Non-blocking `(local_committed_offset, next_offset)`, for observation
    /// only (#224).
    ///
    /// `None` while the replica append path holds the state lock. See
    /// [`crate::LocalBroker::try_local_offsets`] for why a metrics read must
    /// never block behind an fsync.
    pub fn try_local_offsets(&self) -> Option<(u64, u64)> {
        let state = match self.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        Some((
            state.segment.committed_offset(),
            state.segment.next_offset(),
        ))
    }

    /// Fetch capped at `min(local_committed, cluster_committed)`.
    pub fn fetch(
        &self,
        start_offset: u64,
        max_bytes: usize,
        max_records: usize,
    ) -> BrokerResult<FetchBatch> {
        let meta = self.meta_fencing_epoch.lock();
        check_follower_lease(&meta, self.held_fencing_epoch())?;
        let hwm = self.cluster_committed.get();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .segment
            .fetch_through(start_offset, max_bytes, max_records, hwm)
            .map_err(|source| BrokerError::InvalidConfig(source.to_string()))
    }

    pub fn apply_append(
        &self,
        request: &ReplicaAppendRequest,
    ) -> Result<ReplicaAppendResponse, (ErrorCode, String)> {
        if !self.is_online() {
            return Err((
                ErrorCode::Overloaded,
                format!("follower {} is offline", self.node_id),
            ));
        }
        if request.range != self.range {
            return Err((
                ErrorCode::WrongRange,
                "replica append range identity does not match this follower".to_owned(),
            ));
        }
        let meta = self.meta_fencing_epoch.lock();
        if let Err((code, message)) =
            check_follower_fencing(&meta, self.held_fencing_epoch(), request.fencing_epoch)
        {
            return Err((code, message.to_owned()));
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let tip = state.segment.next_offset();
        if tip > request.expected_base_offset {
            // Idempotent retry / catch-up: this follower already applied through
            // the batch. Ack when local durability covers the batch end.
            let batch_end = request
                .expected_base_offset
                .checked_add(request.records.len() as u64)
                .ok_or((
                    ErrorCode::InvalidRequest,
                    "replica append batch end overflows u64".to_owned(),
                ))?;
            if state.segment.committed_offset() >= batch_end {
                return Ok(ReplicaAppendResponse {
                    local_committed_offset: state.segment.committed_offset(),
                });
            }
            return Err((
                ErrorCode::InvalidRequest,
                format!(
                    "follower next_offset {tip} is ahead of expected_base_offset {} but not durable through {batch_end}",
                    request.expected_base_offset
                ),
            ));
        }
        if tip != request.expected_base_offset {
            return Err((
                ErrorCode::InvalidRequest,
                format!(
                    "follower next_offset {tip} does not match expected_base_offset {}",
                    request.expected_base_offset
                ),
            ));
        }
        if let Err(problem) = state
            .producer_epochs
            .accept(request.producer_id, request.producer_epoch)
        {
            return Err(match problem {
                BrokerError::ProducerFenced { .. } => (ErrorCode::Fenced, problem.to_string()),
                other => (ErrorCode::Storage, other.to_string()),
            });
        }
        let (stored_id, stored_epoch) = match self.segment_format {
            SegmentFormat::V1 => (
                storage_producer_id(request.producer_id, request.producer_epoch),
                0,
            ),
            SegmentFormat::V2 => (request.producer_id, request.producer_epoch),
        };
        let records = match records_from_wire(
            &request.records,
            stored_id,
            stored_epoch,
            request.first_sequence,
        ) {
            Ok(records) => records,
            Err(message) => return Err((ErrorCode::InvalidRequest, message.to_owned())),
        };
        let durability = if self.hold_fsync.load(Ordering::SeqCst) {
            Durability::Buffered
        } else {
            Durability::Fsync
        };
        match state.segment.append_group(&records, durability) {
            Ok(_) => Ok(ReplicaAppendResponse {
                local_committed_offset: state.segment.committed_offset(),
            }),
            Err(problem) => Err((
                match problem {
                    vtop_log::LogError::FirstSequence { .. }
                    | vtop_log::LogError::SequenceGap { .. }
                    | vtop_log::LogError::SequenceConflict { .. }
                    | vtop_log::LogError::SequenceBelowWindow { .. } => ErrorCode::SequenceConflict,
                    vtop_log::LogError::ProducerFenced { .. } => ErrorCode::Fenced,
                    _ => ErrorCode::Storage,
                },
                problem.to_string(),
            )),
        }
    }

    /// Apply an ordered commit group with one local durability barrier.
    ///
    /// Members are appended with [`Durability::Buffered`] and then committed
    /// once so concurrent producer sessions share the follower fsync.
    pub fn apply_append_batch(
        &self,
        requests: &[ReplicaAppendRequest],
    ) -> Result<ReplicaAppendResponse, (ErrorCode, String)> {
        if requests.is_empty() {
            return Ok(ReplicaAppendResponse {
                local_committed_offset: self.local_committed_offset(),
            });
        }
        if requests.len() == 1 {
            return self.apply_append(&requests[0]);
        }
        if !self.is_online() {
            return Err((
                ErrorCode::Overloaded,
                format!("follower {} is offline", self.node_id),
            ));
        }
        let meta = self.meta_fencing_epoch.lock();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for request in requests {
            if request.range != self.range {
                return Err((
                    ErrorCode::WrongRange,
                    "replica append range identity does not match this follower".to_owned(),
                ));
            }
            if let Err((code, message)) =
                check_follower_fencing(&meta, self.held_fencing_epoch(), request.fencing_epoch)
            {
                return Err((code, message.to_owned()));
            }
            let tip = state.segment.next_offset();
            if tip > request.expected_base_offset {
                let batch_end = request
                    .expected_base_offset
                    .checked_add(request.records.len() as u64)
                    .ok_or((
                        ErrorCode::InvalidRequest,
                        "replica append batch end overflows u64".to_owned(),
                    ))?;
                if state.segment.committed_offset() >= batch_end {
                    continue;
                }
                return Err((
                    ErrorCode::InvalidRequest,
                    format!(
                        "follower next_offset {tip} is ahead of expected_base_offset {} but not durable through {batch_end}",
                        request.expected_base_offset
                    ),
                ));
            }
            if tip != request.expected_base_offset {
                return Err((
                    ErrorCode::InvalidRequest,
                    format!(
                        "follower next_offset {tip} does not match expected_base_offset {}",
                        request.expected_base_offset
                    ),
                ));
            }
            if let Err(problem) = state
                .producer_epochs
                .accept(request.producer_id, request.producer_epoch)
            {
                return Err(match problem {
                    BrokerError::ProducerFenced { .. } => (ErrorCode::Fenced, problem.to_string()),
                    other => (ErrorCode::Storage, other.to_string()),
                });
            }
            let (stored_id, stored_epoch) = match self.segment_format {
                SegmentFormat::V1 => (
                    storage_producer_id(request.producer_id, request.producer_epoch),
                    0,
                ),
                SegmentFormat::V2 => (request.producer_id, request.producer_epoch),
            };
            let records = match records_from_wire(
                &request.records,
                stored_id,
                stored_epoch,
                request.first_sequence,
            ) {
                Ok(records) => records,
                Err(message) => return Err((ErrorCode::InvalidRequest, message.to_owned())),
            };
            if let Err(problem) = state.segment.append_group(&records, Durability::Buffered) {
                return Err((
                    match problem {
                        vtop_log::LogError::FirstSequence { .. }
                        | vtop_log::LogError::SequenceGap { .. }
                        | vtop_log::LogError::SequenceConflict { .. }
                        | vtop_log::LogError::SequenceBelowWindow { .. } => {
                            ErrorCode::SequenceConflict
                        }
                        vtop_log::LogError::ProducerFenced { .. } => ErrorCode::Fenced,
                        _ => ErrorCode::Storage,
                    },
                    problem.to_string(),
                ));
            }
        }
        if self.hold_fsync.load(Ordering::SeqCst) {
            return Ok(ReplicaAppendResponse {
                local_committed_offset: state.segment.committed_offset(),
            });
        }
        match state.segment.commit() {
            Ok(local_committed_offset) => Ok(ReplicaAppendResponse {
                local_committed_offset,
            }),
            Err(problem) => Err((ErrorCode::Storage, problem.to_string())),
        }
    }

    pub fn observe_hwm(&self, update: &CommittedHwmUpdate) -> Result<(), (ErrorCode, String)> {
        if update.range != self.range {
            return Err((
                ErrorCode::WrongRange,
                "committed HWM update range identity does not match this follower".to_owned(),
            ));
        }
        let meta = self.meta_fencing_epoch.lock();
        if let Err((code, message)) =
            check_follower_fencing(&meta, self.held_fencing_epoch(), update.fencing_epoch)
        {
            return Err((code, message.to_owned()));
        }
        // Never advertise above local durability.
        let local = self.local_committed_offset();
        let visible = update.committed_high_watermark.min(local);
        self.cluster_committed.advance_to(visible);
        Ok(())
    }
}

/// Deterministic RF=N in-process replica set (leader is external).
pub struct InProcessReplicaSet {
    followers: Vec<Arc<InProcessFollower>>,
}

impl InProcessReplicaSet {
    pub fn new(followers: Vec<Arc<InProcessFollower>>) -> Self {
        Self { followers }
    }

    pub fn followers(&self) -> &[Arc<InProcessFollower>] {
        &self.followers
    }
}

impl ReplicaSet for InProcessReplicaSet {
    fn replication_factor(&self) -> usize {
        1 + self.followers.len()
    }

    fn replicate_append_batch(
        &self,
        requests: &[ReplicaAppendRequest],
        leader_committed_offset: u64,
    ) -> ReplicaQuorumResult {
        let mut follower_acks = 0;
        for follower in &self.followers {
            let applied = if requests.len() <= 1 {
                requests
                    .first()
                    .map(|request| follower.apply_append(request))
                    .unwrap_or_else(|| {
                        Ok(ReplicaAppendResponse {
                            local_committed_offset: follower.local_committed_offset(),
                        })
                    })
            } else {
                follower.apply_append_batch(requests)
            };
            if let Ok(response) = applied {
                if response.local_committed_offset >= leader_committed_offset {
                    follower_acks += 1;
                }
            }
        }
        ReplicaQuorumResult {
            follower_acks,
            replication_factor: self.replication_factor(),
        }
    }

    fn propagate_committed_hwm(&self, update: &CommittedHwmUpdate) {
        for follower in &self.followers {
            let _ = follower.observe_hwm(update);
        }
    }
}

fn check_follower_lease(meta: &MetaLeaseState, held_fencing_epoch: u64) -> Result<(), BrokerError> {
    if !meta.lease_active || meta.fencing_epoch != held_fencing_epoch {
        return Err(BrokerError::InvalidConfig(
            "follower lease is inactive or fenced by a newer metadata grant".to_owned(),
        ));
    }
    Ok(())
}

fn check_follower_fencing(
    meta: &MetaLeaseState,
    held_fencing_epoch: u64,
    request_epoch: u64,
) -> Result<(), (ErrorCode, &'static str)> {
    if request_epoch != held_fencing_epoch {
        return Err((
            ErrorCode::Fenced,
            "replica request fencing epoch does not match this follower's lease",
        ));
    }
    if !meta.lease_active || meta.fencing_epoch != held_fencing_epoch {
        return Err((
            ErrorCode::Fenced,
            "follower lease is inactive or fenced by a newer metadata grant",
        ));
    }
    Ok(())
}

fn records_from_wire(
    records: &[ProduceRecord],
    stored_id: Uuid,
    stored_epoch: u64,
    first_sequence: u64,
) -> Result<Vec<LogRecord>, &'static str> {
    records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            let sequence = first_sequence
                .checked_add(index as u64)
                .ok_or("producer sequence range overflows u64")?;
            Ok(LogRecord {
                producer_id: stored_id,
                producer_epoch: stored_epoch,
                sequence,
                timestamp_millis: record.timestamp_millis,
                attributes: 0,
                key: record.key.clone(),
                value: record.value.clone(),
            })
        })
        .collect()
}

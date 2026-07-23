//! In-process leader→follower replication for quorum durability.
//!
//! This slice implements the Stage-6 produce path from the architecture:
//! leader local append → replica append with fencing epoch → quorum durable
//! acknowledgements → advance and propagate the cluster committed high-water
//! mark. Peer TCP transport and catch-up repair remain follow-ups; the wire
//! message types live in `vtop-protocol` so a later transport can reuse them.

use crate::{
    storage_producer_id, BrokerError, BrokerResult, MetaFencingEpoch, MetaLeaseState,
    ProducerEpochJournal, SegmentFormat,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use vtop_log::{ActiveSegment, Durability, FetchBatch, LogRecord};
use vtop_protocol::{
    CommittedHwmUpdate, ErrorCode, ProduceRecord, RangeIdentity, ReplicaAppendRequest,
    ReplicaAppendResponse,
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
    held_fencing_epoch: u64,
    meta_fencing_epoch: MetaFencingEpoch,
    segment_format: SegmentFormat,
    cluster_committed: ClusterCommittedOffset,
    state: Mutex<FollowerState>,
    online: AtomicBool,
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
        let segment_format = if segment.format_version() == vtop_log::FORMAT_VERSION_V2 {
            SegmentFormat::V2
        } else {
            SegmentFormat::V1
        };
        Ok(Self {
            node_id,
            range,
            held_fencing_epoch,
            meta_fencing_epoch,
            segment_format,
            cluster_committed,
            state: Mutex::new(FollowerState {
                segment,
                producer_epochs,
            }),
            online: AtomicBool::new(true),
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

    pub fn set_online(&self, online: bool) {
        self.online.store(online, Ordering::SeqCst);
    }

    pub fn is_online(&self) -> bool {
        self.online.load(Ordering::SeqCst)
    }

    pub fn local_committed_offset(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .segment
            .committed_offset()
    }

    /// Fetch capped at `min(local_committed, cluster_committed)`.
    pub fn fetch(
        &self,
        start_offset: u64,
        max_bytes: usize,
        max_records: usize,
    ) -> BrokerResult<FetchBatch> {
        let meta = self.meta_fencing_epoch.lock();
        check_follower_lease(&meta, self.held_fencing_epoch)?;
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
            check_follower_fencing(&meta, self.held_fencing_epoch, request.fencing_epoch)
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
        match state.segment.append_group(&records, Durability::Fsync) {
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

    pub fn observe_hwm(&self, update: &CommittedHwmUpdate) -> Result<(), (ErrorCode, String)> {
        if update.range != self.range {
            return Err((
                ErrorCode::WrongRange,
                "committed HWM update range identity does not match this follower".to_owned(),
            ));
        }
        let meta = self.meta_fencing_epoch.lock();
        if let Err((code, message)) =
            check_follower_fencing(&meta, self.held_fencing_epoch, update.fencing_epoch)
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

    fn replicate_append(
        &self,
        request: &ReplicaAppendRequest,
        leader_committed_offset: u64,
    ) -> ReplicaQuorumResult {
        let mut follower_acks = 0;
        for follower in &self.followers {
            if let Ok(response) = follower.apply_append(request) {
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

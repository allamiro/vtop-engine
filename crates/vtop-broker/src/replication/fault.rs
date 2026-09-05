//! Deterministic network-delivery fault layer for the data-plane harness.
//!
//! This module extends [`super::InProcessReplicaSet`] with controllable
//! loss / duplication / reorder / delay of replica append and HWM RPCs.
//! Disk faults remain on [`vtop_log::sim`] (`FaultPlan`, crash/reboot) and
//! are configured per node independently of these network faults — see the
//! `data_plane_fault_harness` integration test (#188).
//!
//! Time is a logical tick advanced by produce fan-out and explicit
//! [`FaultInjectingReplicaSet::advance_tick`] calls so failed runs replay
//! exactly from the same seed and scenario script.

use super::{InProcessFollower, InProcessReplicaSet, ReplicaQuorumResult, ReplicaSet};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use vtop_protocol::{CommittedHwmUpdate, ReplicaAppendRequest};

/// Per-follower network delivery controls.
///
/// Counters are consumed as deliveries are attempted. Delay and reorder
/// apply to newly enqueued RPCs until cleared.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FollowerNetworkFault {
    /// Drop the next N append/HWM deliveries after they become due.
    pub drop_next: usize,
    /// Deliver the next N due append batches twice (duplicate RPC).
    pub duplicate_next: usize,
    /// Hold each newly enqueued append for this many logical ticks.
    pub delay_ticks: u64,
    /// Buffer up to this many due appends, then release them in reverse order.
    /// `0` disables reordering.
    pub reorder_window: usize,
    /// A thin pipe toward this follower (#403): appends are released as a
    /// token bucket refilled by this many record bytes (key plus value)
    /// per logical tick, in order — a rate, not a loss, so every queue
    /// behind it becomes a growth question the harness can ask. Zero is
    /// unlimited. Committed-HWM updates are not metered: they are a few
    /// bytes that a real link carries beside the appends it starves.
    pub bytes_per_tick: u64,
    /// The bucket's capacity; zero means one tick's worth. A bucket that is
    /// FULL releases the append at the head whatever its size, so a batch
    /// larger than the burst crosses a starved link eventually rather than
    /// never — the link is slow, not closed.
    pub burst_bytes: u64,
}

impl FollowerNetworkFault {
    fn burst(&self) -> u64 {
        if self.burst_bytes == 0 {
            self.bytes_per_tick
        } else {
            self.burst_bytes
        }
    }
}

/// Snapshot of pending network deliveries (for assertions / debugging).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PendingDeliveryStats {
    pub tick: u64,
    pub delayed_appends: usize,
    pub reorder_buffered: usize,
    pub delayed_hwm: usize,
    /// Record bytes waiting behind delay or a thin pipe (#403): the size of
    /// the retransmission the leader would owe, which the pins bound.
    pub delayed_bytes: u64,
}

/// Full network fault plan keyed by follower index (0-based).
#[derive(Clone, Debug, Default)]
pub struct NetworkFaultPlan {
    pub followers: Vec<FollowerNetworkFault>,
}

struct PendingAppend {
    deliver_at: u64,
    requests: Vec<ReplicaAppendRequest>,
}

struct PendingHwm {
    deliver_at: u64,
    update: CommittedHwmUpdate,
}

struct FollowerNetState {
    fault: FollowerNetworkFault,
    delayed_appends: VecDeque<PendingAppend>,
    reorder_buf: VecDeque<Vec<ReplicaAppendRequest>>,
    delayed_hwm: VecDeque<PendingHwm>,
    /// The token bucket's current fill, in record bytes (#403).
    budget: u64,
}

/// The bytes a link carries for these appends: every record's key and value.
fn append_bytes(requests: &[ReplicaAppendRequest]) -> u64 {
    requests
        .iter()
        .flat_map(|request| request.records.iter())
        .map(|record| (record.key.len() + record.value.len()) as u64)
        .sum()
}

struct FaultState {
    tick: u64,
    seed: u64,
    followers: Vec<FollowerNetState>,
}

/// [`ReplicaSet`] wrapper that injects deterministic network faults.
pub struct FaultInjectingReplicaSet {
    inner: Arc<InProcessReplicaSet>,
    state: Mutex<FaultState>,
}

impl FaultInjectingReplicaSet {
    pub fn new(inner: Arc<InProcessReplicaSet>, seed: u64) -> Self {
        let followers = inner
            .followers()
            .iter()
            .map(|_| FollowerNetState {
                fault: FollowerNetworkFault::default(),
                delayed_appends: VecDeque::new(),
                reorder_buf: VecDeque::new(),
                delayed_hwm: VecDeque::new(),
                budget: 0,
            })
            .collect();
        Self {
            inner,
            state: Mutex::new(FaultState {
                tick: 0,
                seed,
                followers,
            }),
        }
    }

    pub fn seed(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .seed
    }

    pub fn tick(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .tick
    }

    pub fn inner(&self) -> &Arc<InProcessReplicaSet> {
        &self.inner
    }

    pub fn followers(&self) -> &[Arc<InProcessFollower>] {
        self.inner.followers()
    }

    /// Replace the network fault configuration for `follower_index`.
    pub fn set_follower_fault(&self, follower_index: usize, fault: FollowerNetworkFault) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let slot = state
            .followers
            .get_mut(follower_index)
            .unwrap_or_else(|| panic!("follower_index {follower_index} out of range"));
        // A newly set pipe starts with a full bucket: the first append after
        // the fault crosses at once, and starvation shows from the second.
        slot.budget = fault.burst();
        slot.fault = fault;
    }

    pub fn clear_follower_fault(&self, follower_index: usize) {
        self.set_follower_fault(follower_index, FollowerNetworkFault::default());
    }

    pub fn apply_plan(&self, plan: &NetworkFaultPlan) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (index, fault) in plan.followers.iter().enumerate() {
            if let Some(slot) = state.followers.get_mut(index) {
                slot.budget = fault.burst();
                slot.fault = fault.clone();
            }
        }
    }

    /// Advance logical time and deliver any due RPCs.
    pub fn advance_tick(&self, ticks: u64) {
        if ticks == 0 {
            return;
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.tick = state.tick.saturating_add(ticks);
        Self::refill(&mut state, ticks);
        let tick = state.tick;
        Self::drain_due(&self.inner, &mut state, tick);
    }

    /// Refill every follower's bucket for `ticks` elapsed, capped at the
    /// burst (#403): budget does not accrue past what the pipe can hold.
    fn refill(state: &mut FaultState, ticks: u64) {
        for slot in &mut state.followers {
            if slot.fault.bytes_per_tick == 0 {
                continue;
            }
            let accrued = slot.fault.bytes_per_tick.saturating_mul(ticks);
            slot.budget = slot.budget.saturating_add(accrued).min(slot.fault.burst());
        }
    }

    /// Deliver every buffered RPC immediately (heal / catch-up helper).
    pub fn drain_all(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Force every pending item due now.
        for slot in &mut state.followers {
            for pending in &mut slot.delayed_appends {
                pending.deliver_at = 0;
            }
            for pending in &mut slot.delayed_hwm {
                pending.deliver_at = 0;
            }
            // "Deliver everything now" opens the pipe as well as the clock;
            // the bucket is left full for whatever the fault still holds.
            slot.budget = u64::MAX;
        }
        let tick = state.tick;
        Self::drain_due(&self.inner, &mut state, tick);
        for slot in &mut state.followers {
            slot.budget = slot.fault.burst();
        }
        // Flush any incomplete reorder windows.
        for (index, slot) in state.followers.iter_mut().enumerate() {
            while let Some(requests) = slot.reorder_buf.pop_back() {
                Self::deliver_append(&self.inner, index, &mut slot.fault, &requests);
            }
        }
    }

    pub fn pending_stats(&self) -> PendingDeliveryStats {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        PendingDeliveryStats {
            tick: state.tick,
            delayed_appends: state
                .followers
                .iter()
                .map(|f| f.delayed_appends.len())
                .sum(),
            reorder_buffered: state.followers.iter().map(|f| f.reorder_buf.len()).sum(),
            delayed_hwm: state.followers.iter().map(|f| f.delayed_hwm.len()).sum(),
            delayed_bytes: state
                .followers
                .iter()
                .flat_map(|f| f.delayed_appends.iter())
                .map(|pending| append_bytes(&pending.requests))
                .sum(),
        }
    }

    fn drain_due(inner: &InProcessReplicaSet, state: &mut FaultState, tick: u64) {
        for (index, slot) in state.followers.iter_mut().enumerate() {
            while let Some(front) = slot.delayed_appends.front() {
                if front.deliver_at > tick {
                    break;
                }
                // The pipe (#403): an append due by the clock still waits for
                // its bytes, in order — head-of-line, as a link is. A full
                // bucket releases the head whatever its size.
                if slot.fault.bytes_per_tick > 0 {
                    let cost = append_bytes(&front.requests);
                    if slot.budget < cost && slot.budget < slot.fault.burst() {
                        break;
                    }
                    slot.budget = slot.budget.saturating_sub(cost);
                }
                let pending = slot.delayed_appends.pop_front().expect("front existed");
                Self::accept_due_append(inner, index, slot, pending.requests);
            }
            while let Some(front) = slot.delayed_hwm.front() {
                if front.deliver_at > tick {
                    break;
                }
                let pending = slot.delayed_hwm.pop_front().expect("front existed");
                Self::deliver_hwm(inner, index, &mut slot.fault, &pending.update);
            }
        }
    }

    fn accept_due_append(
        inner: &InProcessReplicaSet,
        index: usize,
        slot: &mut FollowerNetState,
        requests: Vec<ReplicaAppendRequest>,
    ) {
        let window = slot.fault.reorder_window;
        if window == 0 {
            Self::deliver_append(inner, index, &mut slot.fault, &requests);
            return;
        }
        slot.reorder_buf.push_back(requests);
        if slot.reorder_buf.len() >= window {
            while let Some(batch) = slot.reorder_buf.pop_back() {
                Self::deliver_append(inner, index, &mut slot.fault, &batch);
            }
        }
    }

    fn deliver_append(
        inner: &InProcessReplicaSet,
        index: usize,
        fault: &mut FollowerNetworkFault,
        requests: &[ReplicaAppendRequest],
    ) {
        if fault.drop_next > 0 {
            fault.drop_next -= 1;
            return;
        }
        let duplicate = if fault.duplicate_next > 0 {
            fault.duplicate_next -= 1;
            true
        } else {
            false
        };
        let follower = &inner.followers()[index];
        let _ = apply_to_follower(follower, requests);
        if duplicate {
            let _ = apply_to_follower(follower, requests);
        }
    }

    fn deliver_hwm(
        inner: &InProcessReplicaSet,
        index: usize,
        fault: &mut FollowerNetworkFault,
        update: &CommittedHwmUpdate,
    ) {
        if fault.drop_next > 0 {
            fault.drop_next -= 1;
            return;
        }
        let follower = &inner.followers()[index];
        let _ = follower.observe_hwm(update);
    }

    fn enqueue_appends(state: &mut FaultState, requests: &[ReplicaAppendRequest]) {
        let tick = state.tick;
        for slot in &mut state.followers {
            let deliver_at = tick.saturating_add(slot.fault.delay_ticks);
            slot.delayed_appends.push_back(PendingAppend {
                deliver_at,
                requests: requests.to_vec(),
            });
        }
    }

    fn enqueue_hwm(state: &mut FaultState, update: &CommittedHwmUpdate) {
        let tick = state.tick;
        for slot in &mut state.followers {
            let deliver_at = tick.saturating_add(slot.fault.delay_ticks);
            slot.delayed_hwm.push_back(PendingHwm {
                deliver_at,
                update: update.clone(),
            });
        }
    }
}

fn apply_to_follower(
    follower: &InProcessFollower,
    requests: &[ReplicaAppendRequest],
) -> Result<(), (vtop_protocol::ErrorCode, String)> {
    if requests.len() <= 1 {
        if let Some(request) = requests.first() {
            follower.apply_append(request).map(|_| ())
        } else {
            Ok(())
        }
    } else {
        follower.apply_append_batch(requests).map(|_| ())
    }
}

impl ReplicaSet for FaultInjectingReplicaSet {
    fn replication_factor(&self) -> usize {
        self.inner.replication_factor()
    }

    fn replicate_append_batch(
        &self,
        requests: &[ReplicaAppendRequest],
        leader_committed_offset: u64,
    ) -> ReplicaQuorumResult {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // One logical tick per fan-out attempt keeps delivery deterministic.
        state.tick = state.tick.saturating_add(1);
        Self::refill(&mut state, 1);
        Self::enqueue_appends(&mut state, requests);
        let tick = state.tick;
        Self::drain_due(&self.inner, &mut state, tick);
        drop(state);

        let mut follower_acks = 0;
        for follower in self.inner.followers() {
            if follower.local_committed_offset() >= leader_committed_offset {
                follower_acks += 1;
            }
        }
        ReplicaQuorumResult {
            follower_acks,
            replication_factor: self.replication_factor(),
        }
    }

    fn propagate_committed_hwm(&self, update: &CommittedHwmUpdate) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self::enqueue_hwm(&mut state, update);
        let tick = state.tick;
        Self::drain_due(&self.inner, &mut state, tick);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::{ClusterCommittedOffset, InProcessFollower};
    use crate::{MetaFencingEpoch, ProducerEpochJournal};
    use tempfile::TempDir;
    use uuid::Uuid;
    use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
    use vtop_protocol::{ProduceRecord, RangeIdentity};

    const EPOCH: u64 = 7;

    fn range() -> RangeIdentity {
        RangeIdentity {
            topic: "fault.v1".to_owned(),
            topic_epoch: 1,
            range_id: Uuid::from_u128(0xF1),
            range_generation: 0,
        }
    }

    fn open_segment(dir: &TempDir, id: u128, range: &RangeIdentity) -> ActiveSegment {
        let descriptor = SegmentDescriptor {
            segment_id: Uuid::from_u128(id),
            topic: range.topic.clone(),
            topic_epoch: range.topic_epoch,
            lineage: RangeLineage {
                range_id: range.range_id,
                generation: range.range_generation,
                key_range: KeyRange::full(),
                parents: Vec::new(),
            },
            base_offset: 0,
        };
        ActiveSegment::create(
            dir.path().join("range.active"),
            descriptor,
            SegmentConfig::default(),
        )
        .unwrap()
    }

    fn follower(
        dir: &TempDir,
        id: u128,
        node: Uuid,
        meta: &MetaFencingEpoch,
    ) -> Arc<InProcessFollower> {
        let range = range();
        let segment = open_segment(dir, id, &range);
        let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
        Arc::new(
            InProcessFollower::new(
                node,
                segment,
                epochs,
                range,
                EPOCH,
                meta.clone(),
                ClusterCommittedOffset::new(0),
            )
            .unwrap(),
        )
    }

    fn append_req(base: u64, seq: u64) -> ReplicaAppendRequest {
        ReplicaAppendRequest {
            range: range(),
            fencing_epoch: EPOCH,
            leader_node_id: Uuid::from_u128(0xA1),
            expected_base_offset: base,
            producer_id: Uuid::from_u128(0xB1),
            producer_epoch: 1,
            first_sequence: seq,
            records: vec![ProduceRecord {
                timestamp_millis: 1,
                key: b"k".to_vec(),
                value: format!("v{seq}").into_bytes(),
            }],
        }
    }

    #[test]
    fn drop_prevents_follower_ack() {
        let meta = MetaFencingEpoch::new(EPOCH);
        let d0 = tempfile::tempdir().unwrap();
        let d1 = tempfile::tempdir().unwrap();
        let f0 = follower(&d0, 1, Uuid::from_u128(0xA2), &meta);
        let f1 = follower(&d1, 2, Uuid::from_u128(0xA3), &meta);
        let inner = Arc::new(InProcessReplicaSet::new(vec![f0.clone(), f1.clone()]));
        let faulty = FaultInjectingReplicaSet::new(inner, 0x5eed_0188);
        faulty.set_follower_fault(
            0,
            FollowerNetworkFault {
                drop_next: 1,
                ..FollowerNetworkFault::default()
            },
        );

        let result = faulty.replicate_append(&append_req(0, 0), 1);
        assert_eq!(result.follower_acks, 1);
        assert_eq!(f0.local_committed_offset(), 0);
        assert_eq!(f1.local_committed_offset(), 1);
    }

    #[test]
    fn delay_then_advance_delivers() {
        let meta = MetaFencingEpoch::new(EPOCH);
        let d0 = tempfile::tempdir().unwrap();
        let d1 = tempfile::tempdir().unwrap();
        let f0 = follower(&d0, 1, Uuid::from_u128(0xA2), &meta);
        let f1 = follower(&d1, 2, Uuid::from_u128(0xA3), &meta);
        let inner = Arc::new(InProcessReplicaSet::new(vec![f0.clone(), f1.clone()]));
        let faulty = FaultInjectingReplicaSet::new(inner, 0x5eed_0188);
        faulty.set_follower_fault(
            0,
            FollowerNetworkFault {
                delay_ticks: 2,
                ..FollowerNetworkFault::default()
            },
        );

        let result = faulty.replicate_append(&append_req(0, 0), 1);
        assert_eq!(result.follower_acks, 1);
        assert_eq!(f0.local_committed_offset(), 0);

        faulty.advance_tick(2);
        assert_eq!(f0.local_committed_offset(), 1);
    }

    #[test]
    fn reorder_window_releases_in_reverse() {
        let meta = MetaFencingEpoch::new(EPOCH);
        let d0 = tempfile::tempdir().unwrap();
        let f0 = follower(&d0, 1, Uuid::from_u128(0xA2), &meta);
        // Single follower so RF=2; we only care about delivery order on f0.
        let inner = Arc::new(InProcessReplicaSet::new(vec![f0.clone()]));
        let faulty = FaultInjectingReplicaSet::new(inner, 0x5eed_0188);
        faulty.set_follower_fault(
            0,
            FollowerNetworkFault {
                reorder_window: 2,
                ..FollowerNetworkFault::default()
            },
        );

        // First append stays buffered in the reorder window.
        let _ = faulty.replicate_append(&append_req(0, 0), 1);
        assert_eq!(f0.local_committed_offset(), 0);

        // Second append fills the window; LIFO release applies seq1 then seq0.
        // seq1 expects base 1 which fails; seq0 then applies. Net: only offset 0
        // is durable — reorder without leader retransmission leaves a gap.
        let _ = faulty.replicate_append(&append_req(1, 1), 2);
        assert_eq!(
            f0.local_committed_offset(),
            1,
            "LIFO applies later append first (fails) then earlier append"
        );
    }
}

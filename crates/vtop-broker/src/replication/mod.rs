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
//! bounded retransmission buffer for basic catch-up.
//!
//! [`transfer`] is what catches a follower that fell BELOW that buffer, and so
//! has no way back through the append path at all: the leader serves its
//! immutable sealed prefix — `.segment`, `.manifest.json`, `.producers`,
//! verbatim — over the same peer plane, and the receiver rebuilds the derived
//! sidecars itself and verifies before anything is published. Adoption of the
//! received set by a running follower is the remaining follow-up.
//!
//! [`fault::FaultInjectingReplicaSet`] layers controllable network delivery
//! faults (loss / duplicate / reorder / delay) over the in-process set for
//! the distributed data-plane fault harness (#188). Disk faults stay on
//! [`vtop_log::sim`] and are injected independently.

pub mod fault;
pub mod network;
pub mod transfer;

use crate::{
    storage_producer_id, BrokerError, BrokerResult, MetaFencingEpoch, MetaLeaseState,
    ProducerEpochJournal, SegmentFormat, PROMOTION_MARKER_PRODUCER,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use vtop_log::{Durability, FetchBatch, LogRecord, SegmentSet};
use vtop_protocol::{
    CommittedHwmUpdate, ErrorCode, ProduceRecord, RangeIdentity, ReplicaAppendRequest,
    ReplicaAppendResponse,
};

pub use fault::{
    FaultInjectingReplicaSet, FollowerNetworkFault, NetworkFaultPlan, PendingDeliveryStats,
};
pub use network::{
    address_now, FlowControlConfig, NetworkFollowerConfig, NetworkedReplicaSet, ReplicaPeerHandler,
    ReplicaPeerServer, ReplicaStatusClient, ReplicaTlsMaterial,
};
pub use transfer::{LeaderSegmentTransferHandler, SegmentTransferClient, TransferredPrefix};

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
    /// A follower holds the same shape as its leader (#270): a segment SET
    /// whose tail rolls at its own configured bound. It must — the leader
    /// replicates by offset, not by file, so a follower that could not roll
    /// would refuse the append that pushes it past the bound and drop out of
    /// every quorum from then on.
    segment: SegmentSet,
    producer_epochs: ProducerEpochJournal,
}

/// What a replica held at the instant it was fenced (#240).
///
/// Distinct from a status read: a status is a measurement of something that may
/// still be moving, this is a measurement of something that has been stopped.
/// Only the second can be counted toward a promotion boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FenceOutcome {
    pub fencing_epoch: u64,
    pub local_committed_offset: u64,
    pub next_offset: u64,
    /// Empty means UNKNOWN, never "no leadership changes".
    pub epoch_starts: Vec<crate::fencing_epochs::EpochStart>,
    /// Records discarded to agree with the caller, if any.
    pub truncated_records: u64,
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
    /// Durable shadow of `cluster_committed` (#240): read at open to seed the
    /// cell, advanced at the commit barrier. `None` for harnesses that never
    /// attach one — absent means the guard starts at zero, the pre-floor
    /// behaviour every older test pins.
    committed_floor: Mutex<Option<crate::committed_floor::CommittedFloorFile>>,
    /// Whether arming is SETTLED — a nonzero floor is durable, no file is
    /// attached, or the file gave up (#240). The fast-path guard so the
    /// dispatch loop's per-frame [`Self::arm_committed_floor`] is a single
    /// atomic load once arming is done, never a lock. Only while this is
    /// false does arming take the file lock, and only in the transient
    /// window before the first observed HWM is durable.
    committed_floor_armed: AtomicBool,
    /// Sealed-prefix retention bound in bytes; 0 = disabled (#290).
    retention_max_total_bytes: std::sync::atomic::AtomicU64,
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
        segment: impl Into<SegmentSet>,
        producer_epochs: ProducerEpochJournal,
        range: RangeIdentity,
        held_fencing_epoch: u64,
        meta_fencing_epoch: MetaFencingEpoch,
        cluster_committed: ClusterCommittedOffset,
    ) -> BrokerResult<Self> {
        let segment: SegmentSet = segment.into();
        // Validate that the tail's embedded identity matches the range this
        // follower is being constructed for. A mismatch means the caller
        // supplied a segment for a different range, which would silently
        // accept appends under the wrong identity. The tail vouches for its
        // sealed prefix: discovery quarantines a directory whose segments
        // disagree about their lineage before a set can be opened from it.
        let (seg_topic, seg_topic_epoch, seg_range_id, seg_generation) =
            if let Some(desc) = segment.active().descriptor_v2() {
                (
                    desc.topic.as_str(),
                    desc.topic_epoch,
                    desc.lineage.range_id,
                    desc.lineage.generation,
                )
            } else {
                let desc = segment.active().descriptor();
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

        let segment_format = if segment.active().format_version() == vtop_log::FORMAT_VERSION_V2 {
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
            committed_floor: Mutex::new(None),
            committed_floor_armed: AtomicBool::new(false),
            retention_max_total_bytes: std::sync::atomic::AtomicU64::new(0),
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
        // Serialized under the meta lock (review, #439): adoption is what
        // reopens a fenced log — it is the move that lets a newer leader's
        // appends pass `check_follower_fencing` — so it takes the same
        // lock the probe fence measures under. Lock-free, a rival's grant
        // could overtake the fence between its held re-check and its vote,
        // and no number of re-checks closes a race the other side never
        // synchronizes with. Every caller reaches adoption without holding
        // that lock; the fence, which does hold it, uses the locked
        // variant below.
        let _meta = self.meta_fencing_epoch.lock();
        self.adopt_fencing_epoch_locked(epoch)
    }

    /// [`Self::adopt_fencing_epoch`]'s body, for callers already inside the
    /// meta-locked critical section.
    fn adopt_fencing_epoch_locked(&self, epoch: u64) -> bool {
        // Durable before the epoch is visible, for the same reason as on a
        // leader: `fetch_max` is what admits appends under the new epoch, so
        // recording after it can name a start above the first record that
        // epoch actually accepted.
        if epoch > self.held_fencing_epoch.load(Ordering::SeqCst) {
            let next_offset = self.next_offset();
            let mut guard = self
                .fencing_epoch_journal
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(journal) = guard.as_mut() {
                if journal.record_adoption(epoch, next_offset).is_err() {
                    self.fencing_epoch_history_broken
                        .store(true, Ordering::SeqCst);
                }
            }
        }
        self.held_fencing_epoch.fetch_max(epoch, Ordering::SeqCst) < epoch
    }

    /// Bound this replica's disk in bytes; `None` disables retention (#290).
    /// Followers reclaim by their own policy exactly as they roll at their
    /// own bound: the leader replicates offsets, not files. A `Some` policy
    /// with a zero bound is treated as disabled at this layer; node
    /// configuration rejects zero outright.
    pub fn set_retention(&self, policy: Option<vtop_log::RetentionPolicy>) {
        self.retention_max_total_bytes.store(
            policy.map(|policy| policy.max_total_bytes).unwrap_or(0),
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    /// One retention pass under the held state lock; failures reported, not
    /// returned, for the same reason as on the leader — the append was
    /// already durable, and the next open finishes an interrupted pass.
    fn run_retention(&self, segment: &mut vtop_log::SegmentSet) {
        let max_total_bytes = self
            .retention_max_total_bytes
            .load(std::sync::atomic::Ordering::SeqCst);
        if max_total_bytes == 0 {
            return;
        }
        let floor = self.cluster_committed.get().min(segment.committed_offset());
        if let Err(problem) = segment.retain(&vtop_log::RetentionPolicy { max_total_bytes }, floor)
        {
            eprintln!(
                "follower retention failed and will be retried after the next append: {problem}"
            );
        }
    }

    /// Durably commit the tail's boundary for an orderly shutdown (#280).
    /// Same contract as [`crate::LocalBroker::quiesce`]: loses nothing if
    /// skipped, spares the next open a torn-tail truncation. Also the
    /// floor's final persist (#240): `observe_hwm` does no I/O, so the HWM
    /// frames that arrive after the last committed append would otherwise be
    /// exactly the lag the next restart starts with.
    pub fn quiesce(&self) -> BrokerResult<u64> {
        let committed = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.segment.commit().map_err(BrokerError::from)?
        };
        self.persist_committed_floor();
        Ok(committed)
    }

    /// Install the durable epoch→offset vector for this replica (#240).
    ///
    /// Seeds the held epoch only when both the vector and the log are empty;
    /// see [`crate::LocalBroker::set_fencing_epoch_journal`] for why a replica
    /// that already holds records must report "unknown" instead. Also completes
    /// a truncation interrupted by a crash, per
    /// [`crate::LocalBroker::attach_epoch_journal_to_log`].
    pub fn set_fencing_epoch_journal(
        &self,
        mut journal: crate::fencing_epochs::FencingEpochJournal,
    ) {
        let epoch = self.held_fencing_epoch.load(Ordering::SeqCst);
        let next_offset = self.next_offset();
        if !crate::LocalBroker::attach_epoch_journal_to_log(&mut journal, next_offset) {
            self.fencing_epoch_history_broken
                .store(true, Ordering::SeqCst);
        }
        // Epoch 0 is the "no grant yet" sentinel, never a writing epoch.
        if journal.latest().is_none()
            && next_offset == 0
            && epoch > 0
            && journal.record(epoch, 0).is_err()
        {
            self.fencing_epoch_history_broken
                .store(true, Ordering::SeqCst);
        }
        *self
            .fencing_epoch_journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(journal);
    }

    /// Attach the durable floor beneath `cluster_committed` (#240).
    ///
    /// Injected after construction like the fencing-epoch journal: the
    /// deterministic harness builds followers with no disk at all, and the
    /// wiring that has a data directory is the wiring that can supply the
    /// file. Seeding the CELL from the file is deliberately the constructor's
    /// job — `ClusterCommittedOffset::new(file.floor())` — not this setter's:
    /// the cell is shared state a caller may already have cloned, and a
    /// setter that silently advanced it would move a value other components
    /// were told was theirs to observe.
    pub fn set_committed_floor(&self, file: crate::committed_floor::CommittedFloorFile) {
        // A file that opened carrying a floor is already armed — the guard it
        // seeds is live from the first fence, so arming never needs to run.
        // A fresh (zero) file waits for the first observed HWM. Set the file
        // before the flag so an arm racing the wiring sees the file it will
        // read under the lock.
        let already_armed = file.floor() != 0;
        *self
            .committed_floor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(file);
        self.committed_floor_armed
            .store(already_armed, Ordering::Release);
    }

    /// Make the guard's floor durable when it advanced (#240); a no-op when
    /// no file was attached.
    ///
    /// Called only where a durability barrier already exists — after a
    /// committed append, single or batch, after [`Self::flush_held_fsync`]
    /// releases held bytes, and from [`Self::quiesce`] — always AFTER the
    /// state lock drops, because this needs only the shared cell and its own
    /// file, and deliberately NOT from [`Self::observe_hwm`], which runs in
    /// the per-connection dispatch loop that also carries append frames and
    /// must stay I/O-free in the steady state ([`Self::arm_committed_floor`]
    /// is the one bounded exception). The durable floor therefore lags the
    /// cell by at most one append batch plus the quiet tail before quiesce.
    /// That lag is safe
    /// by the asymmetry [`crate::committed_floor`] states: a floor too low
    /// only weakens the guard toward the pre-floor behaviour; it never
    /// blocks legitimate work.
    ///
    /// A failed write is reported ONCE and persistence stops: the floor is
    /// protection, not a liveness dependency, and failing the append path
    /// over it would convert a damaged sidecar into an outage.
    fn persist_committed_floor(&self) {
        let mut guard = self
            .committed_floor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // The cell is read UNDER the file lock: barriers overlap now that
        // persists run outside the state lock, and a snapshot taken before
        // the lock could arrive after a newer save — a regression the file
        // rightly refuses, which the disable path would then read as damage
        // and stop persistence for good. Read-in-lock makes every saved
        // value at least as fresh as the save it follows.
        let floor = self.cluster_committed.get();
        Self::save_floor_or_disable(&mut guard, floor);
        self.settle_arming_if_done(floor, guard.is_none());
    }

    /// Make the first observed HWM durable without waiting for a later
    /// barrier (#240): the difference between an UNARMED guard and an armed
    /// one is whether a crash on the quiet tail recovers a floor at all, and
    /// that is the exact window this file exists to close. Every later frame
    /// only sharpens an already-armed guard and defers to the next barrier,
    /// so this is a single atomic load once arming has settled — the
    /// steady-state dispatch loop pays nothing.
    fn arm_committed_floor(&self) {
        if self.committed_floor_armed.load(Ordering::Acquire) {
            return;
        }
        // Not yet settled, so take the lock — BLOCKING, not `try_lock`. An
        // earlier revision skipped on contention, and both reviews of this
        // PR found the race: the only thing that holds this lock for any
        // time is a barrier persist mid-fsync, and if that persist read the
        // cell before this HWM advanced it, skipping drops the one arming
        // chance and a quiet-tail crash recovers zero. Blocking is bounded
        // and self-limiting: a barrier running means appends are committing,
        // so the cell is advancing and arming completes; and once it does,
        // the atomic above means this lock is never taken from the dispatch
        // path again.
        let mut guard = self
            .committed_floor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match guard.as_ref() {
            // A file already carrying a floor (opened non-empty, or armed by
            // a barrier between the load above and this lock) needs nothing;
            // no file at all is a harness follower with nothing to arm.
            Some(file) if file.floor() != 0 => {
                self.committed_floor_armed.store(true, Ordering::Release);
                return;
            }
            None => {
                self.committed_floor_armed.store(true, Ordering::Release);
                return;
            }
            Some(_) => {}
        }
        let floor = self.cluster_committed.get();
        if floor == 0 {
            // Nothing acknowledged yet, so nothing to protect and no barrier
            // to contend with; a later frame retries once the cell moves.
            return;
        }
        Self::save_floor_or_disable(&mut guard, floor);
        self.settle_arming_if_done(floor, guard.is_none());
    }

    /// Arming is settled — the atomic fast path may short-circuit forever —
    /// once a nonzero floor is durable OR the file has given up. A no-op
    /// save of zero settles nothing: the guard still has nothing to recover.
    fn settle_arming_if_done(&self, saved_floor: u64, file_disabled: bool) {
        if saved_floor != 0 || file_disabled {
            self.committed_floor_armed.store(true, Ordering::Release);
        }
    }

    /// The one disable path for a floor that cannot persist: report once,
    /// keep serving, stop claiming durability until the next open. Shared by
    /// the barrier persists and the arming save so the two can never drift
    /// on what a failed write means.
    fn save_floor_or_disable(
        guard: &mut Option<crate::committed_floor::CommittedFloorFile>,
        floor: u64,
    ) {
        let Some(file) = guard.as_mut() else {
            return;
        };
        if let Err(problem) = file.save(floor) {
            eprintln!(
                "committed-floor persist failed; the in-memory guard keeps serving and the \
                 durable floor stops advancing until the next open: {problem}"
            );
            *guard = None;
        }
    }

    /// Discard every record at or above `offset` and drop the epoch entries
    /// that described them (#240).
    ///
    /// The follower is the replica this repair exists for: it fsyncs the
    /// leader's appends before that leader has a quorum, so a leader deposed
    /// mid-flight leaves records here that no quorum ever agreed to. Until they
    /// are gone this follower refuses every append from the new leader and is
    /// stranded — retrying forever against a mismatch that retrying cannot fix.
    ///
    /// See [`crate::LocalBroker::truncate_to`] for the acknowledged-records
    /// bound and why the log is truncated before the vector.
    pub fn truncate_to(&self, offset: u64) -> BrokerResult<vtop_log::TruncateOutcome> {
        let high_watermark = self.cluster_committed.get();
        if offset < high_watermark {
            return Err(BrokerError::TruncationBelowAcknowledged {
                requested: offset,
                high_watermark,
            });
        }

        let outcome = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state
                .segment
                .truncate_to(offset)
                .map_err(|source| BrokerError::InvalidConfig(source.to_string()))?
        };

        let mut guard = self
            .fencing_epoch_journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(journal) = guard.as_mut() {
            let repaired = journal.truncate_to(offset).and_then(|_| {
                journal.record_held_epoch_at(self.held_fencing_epoch.load(Ordering::SeqCst), offset)
            });
            if repaired.is_err() {
                self.fencing_epoch_history_broken
                    .store(true, Ordering::SeqCst);
            }
        }
        Ok(outcome)
    }

    /// This replica's epoch history; empty means "unknown", never "no changes".
    pub fn epoch_starts(&self) -> Vec<crate::fencing_epochs::EpochStart> {
        // Flag read UNDER the lock; see LocalBroker::epoch_starts.
        let guard = self
            .fencing_epoch_journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.fencing_epoch_history_broken.load(Ordering::SeqCst) {
            return Vec::new();
        }
        guard
            .as_ref()
            .map(|journal| journal.entries().to_vec())
            .unwrap_or_default()
    }

    /// Fence this replica at `epoch` and report what it holds at that instant
    /// (#240).
    ///
    /// A promotion probe that merely READS a replica is reading a moving
    /// target: the deposed leader may still be appending here while the new
    /// leader takes its measurement, so the boundary a quorum "proves" can be
    /// stale before it is published. Fencing first stops the old leader — its
    /// appends carry the previous epoch and are refused — and the reply is
    /// taken from a log that can no longer move underneath it.
    ///
    /// Fencing and reading are one operation for the same reason. Offsets and
    /// the epoch history are what a truncation target is computed from, and
    /// taking them in two calls would describe two different moments.
    ///
    /// # What this refuses, and why it is not paranoia
    ///
    /// **An epoch metadata has not granted, as far as this replica knows.** The
    /// caller's claim is not evidence. A replica that adopted a bare claim
    /// could be fenced to any epoch by anything that can reach its port, and
    /// would then refuse every append until metadata reached a number that may
    /// never arrive — a permanent outage from one compromised or buggy peer.
    /// So the ceiling is this replica's own metadata view, which the lease
    /// watcher maintains (#239). A caller that has genuinely just been granted
    /// the epoch only has to wait for this replica to see the same grant.
    ///
    /// **An epoch below the one already held.** Fencing moves forward only;
    /// otherwise a stale leader could un-fence a replica it had already lost.
    /// Fence this replica from its OWN process (#410): stop the log at
    /// `epoch` so the promotion probe can count an offset nothing is still
    /// moving.
    ///
    /// Not [`Self::fence`], deliberately, on both of that method's guards.
    /// The not-yet-observed-grant refusal protects against a peer's bare
    /// claim over the wire; this epoch is no claim — it is this process's
    /// own authenticated read of its grant, and the shared metadata view
    /// only learns it after the promotion the probe serves. And no
    /// reconciliation: the caller IS this replica, and a log has nothing
    /// to reconcile against itself.
    ///
    /// THE META LOCK IS THE APPEND'S CRITICAL SECTION (review). Both
    /// append paths hold it from their fencing check through the applied
    /// records, so taking it here waits out any append already past its
    /// check and blocks the next one until the epoch has risen — after
    /// which the check itself refuses. Without it, adoption is a bare
    /// atomic maximum that an in-flight append can straddle: checked at
    /// the old epoch, applied after the "stopped" offset was read.
    ///
    /// Fencing moves forward only, exactly as the wire fence rules: a held
    /// epoch that stands above the one being fenced — before this call or
    /// raced into it — means the grant this probe acts on was superseded,
    /// and the refusal is the caller's cue to abstain rather than vouch
    /// for a boundary metadata has moved past. On success the return value
    /// IS the vote: the committed offset read inside the same critical
    /// section that stopped the log.
    pub fn fence_locally(&self, epoch: u64) -> Result<u64, (ErrorCode, String)> {
        let _meta = self.meta_fencing_epoch.lock();
        self.adopt_fencing_epoch_locked(epoch);
        // RE-READ AFTER ADOPTING, and DECISIVE for the whole critical
        // section (review, twice): adoption everywhere else now takes the
        // meta lock this fence already holds, so between this read and the
        // vote below nothing can adopt past it — a pre-check was a
        // measurement a concurrent adopt could invalidate, and re-checks
        // alone could never close a race the other side did not
        // synchronize with. Held above `epoch` means the grant this probe
        // acts on was superseded while it was being fenced, and the moment
        // the rival's adoption lands its appends are admissible again — so
        // an offset counted here would be a vote for a dead grant from a
        // log about to move.
        let held = self.held_fencing_epoch();
        if held != epoch {
            return Err((
                ErrorCode::Fenced,
                format!(
                    "local fence at epoch {epoch} was overtaken by this replica's held \
                     epoch {held}"
                ),
            ));
        }
        // THE VOTE IS READ INSIDE THE SAME CRITICAL SECTION. Returned from
        // here rather than sampled by the caller afterwards (review): the
        // meta lock is what holds rival adoption's appends out, and any
        // read taken after it drops is a measurement of something that may
        // already be moving again — the exact distinction `FenceOutcome`'s
        // doc draws for the wire fence.
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(state.segment.committed_offset())
    }

    pub fn fence(
        &self,
        epoch: u64,
        leader_epoch_starts: &[crate::fencing_epochs::EpochStart],
    ) -> Result<FenceOutcome, (ErrorCode, String)> {
        let held = self.held_fencing_epoch();
        if epoch < held {
            return Err((
                ErrorCode::Fenced,
                format!("fence request at epoch {epoch} is below this replica's held epoch {held}"),
            ));
        }
        let granted = self.meta_fencing_epoch.lock().fencing_epoch;
        if epoch > granted {
            return Err((
                ErrorCode::Fenced,
                format!(
                    "this replica has not observed a grant for epoch {epoch}; its metadata view \
                     is at {granted}. Retry once it catches up — a replica must not fence itself \
                     on a caller's word alone"
                ),
            ));
        }
        // Records the start durably before the epoch becomes visible, exactly
        // as the append path does. A replica holding records with NO history
        // stays that way: `record_adoption` refuses to fabricate a first
        // entry over a non-empty log (#315), so the reconciliation below
        // sees an empty vector, compares as Unknown, and truncates nothing —
        // on this fence and every later one, and in the `epoch_starts` this
        // fence returns to the caller.
        self.adopt_fencing_epoch(epoch);

        // Reconcile WHILE STOPPED. This is the only moment it is sound: the
        // replica has just been fenced, so nothing can append between the
        // comparison and the truncation, and the numbers the caller acts on
        // describe the log it will actually follow.
        //
        // Doing it here rather than per-append is what closes the ack-on-
        // divergence hole (#261) at its cause. That hole exists because the
        // catch-up path asks "am I durable through this offset?" when the
        // question is "do I hold the SAME record there?" — a question a single
        // append cannot answer. After this, a diverged replica has already
        // discarded what the caller disagrees with, so the catch-up path is
        // never reached in a diverged state and never has to answer it.
        let truncated_records = self.reconcile_with(leader_epoch_starts)?;

        // Offsets and history read together, under the state lock, so the pair
        // describes one instant of one log.
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let local_committed_offset = state.segment.committed_offset();
        let next_offset = state.segment.next_offset();
        drop(state);
        Ok(FenceOutcome {
            fencing_epoch: self.held_fencing_epoch(),
            local_committed_offset,
            next_offset,
            epoch_starts: self.epoch_starts(),
            truncated_records,
        })
    }

    /// Discard anything written under a leadership `leader_epoch_starts` does
    /// not share, and report how many records that cost.
    ///
    /// Returns 0 without touching the log whenever the answer is not provable:
    ///
    /// * The caller's history is EMPTY — it cannot vouch for its own lineage.
    ///   Truncating here would delete data to satisfy a claim nobody made.
    /// * The two vectors share no common prefix at all (`divergence_point`
    ///   yields `None`). That is a lineage fault — two replicas of what should
    ///   be the same range with no provably identical history — and it is not
    ///   something truncation can repair. It needs an operator, not a deletion.
    /// * The divergence point is at or above this replica's tail: nothing it
    ///   holds is in dispute.
    ///
    /// A truncation that would cross the acknowledged high-water mark is an
    /// ERROR, not a silent no-op. Everything below that was acknowledged to a
    /// producer, so a caller asking for it is either wrong or reconciling
    /// against a log that is not this range's — and the fence must fail rather
    /// than let promotion proceed believing this replica agreed.
    fn reconcile_with(
        &self,
        leader_epoch_starts: &[crate::fencing_epochs::EpochStart],
    ) -> Result<u64, (ErrorCode, String)> {
        if leader_epoch_starts.is_empty() {
            return Ok(0);
        }
        let verdict = {
            let guard = self
                .fencing_epoch_journal
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if self.fencing_epoch_history_broken.load(Ordering::SeqCst) {
                crate::fencing_epochs::Lineage::Unknown
            } else {
                guard
                    .as_ref()
                    .map_or(crate::fencing_epochs::Lineage::Unknown, |journal| {
                        journal.compare_lineage(leader_epoch_starts)
                    })
            }
        };
        let divergence = match verdict {
            // Agreed and Unknown are different facts with the same action:
            // touch nothing. One says there is nothing to discard, the other
            // says we cannot prove there is.
            crate::fencing_epochs::Lineage::Agreed | crate::fencing_epochs::Lineage::Unknown => {
                return Ok(0)
            }
            crate::fencing_epochs::Lineage::DivergesAt(offset) => offset,
        };
        if divergence >= self.next_offset() {
            return Ok(0);
        }
        // A divergence verdict BELOW this replica's earliest offset points at
        // records retention reclaimed (#290): the epoch entries that produced
        // it describe history whose records neither party can compare any
        // more, and the truncation it would mandate is below the acknowledged
        // floor by construction — retention never reclaims above it. Failing
        // the fence over that would exclude a valid replica from every
        // promotion for a dispute about data it no longer holds. The honest
        // verdict is the journal-less one: unprovable, touch nothing. (The
        // journal is deliberately NOT compacted at retention: re-anchoring
        // surviving epochs at each replica's own retained base would give the
        // same epoch different start offsets on different replicas and
        // manufacture exactly the false divergence this guards against.)
        let base_offset = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.segment.base_offset()
        };
        if divergence < base_offset {
            // ...but "began below the base" is not "confined below the base"
            // (#408): DivergesAt marks everything at or above it suspect,
            // which includes every retained record. Whether the RETAINED
            // stretch is actually disputed is answerable — clamp both
            // histories to exactly the retained window [base, tail) and
            // compare those (review: bounded on BOTH sides, so the fencing
            // epoch's own tail adoption and a peer's idle-epoch future
            // cannot manufacture disagreements about records nobody holds,
            // and the comparison is exact equality with no asymmetric
            // prefix tolerance to excuse a real one). Equal clamps confine
            // the disagreement to records neither party holds any more:
            // unprovable, touch nothing, exactly the #290 reasoning above.
            // Unequal clamps mean retained records are attributed to
            // DIFFERENT leaderships, and admitting that silently would hand
            // back the split-brain read this vector exists to prevent — the
            // fence fails loudly and the replica needs repair. An empty
            // clamp on either side is "cannot vouch": unknown, touch
            // nothing, as everywhere else. An empty WINDOW is nothing to
            // dispute at all.
            let tail = self.next_offset();
            if tail <= base_offset {
                return Ok(0);
            }
            let confined = {
                let guard = self
                    .fencing_epoch_journal
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                guard.as_ref().is_none_or(|journal| {
                    let mine =
                        crate::fencing_epochs::clamp_lineage(journal.entries(), base_offset, tail);
                    let theirs = crate::fencing_epochs::clamp_lineage(
                        leader_epoch_starts,
                        base_offset,
                        tail,
                    );
                    mine.is_empty() || theirs.is_empty() || mine == theirs
                })
            };
            if confined {
                return Ok(0);
            }
            return Err((
                ErrorCode::WrongLineage,
                format!(
                    "the caller's history and this replica's disagree about who wrote records \
                     this replica still retains (base offset {base_offset}); the dispute is not \
                     confined to reclaimed records, so nothing can be silently admitted — this \
                     replica needs repair"
                ),
            ));
        }
        match self.truncate_to(divergence) {
            Ok(outcome) => Ok(outcome.records_removed),
            Err(BrokerError::TruncationBelowAcknowledged {
                requested,
                high_watermark,
            }) => Err((
                ErrorCode::InvalidRequest,
                format!(
                    "reconciling with the caller would discard acknowledged records: it puts \
                     divergence at {requested}, below this replica's high-water mark \
                     {high_watermark}"
                ),
            )),
            Err(problem) => Err((ErrorCode::Storage, problem.to_string())),
        }
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
        let committed = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let committed = state.segment.commit().map_err(|problem| {
                (
                    ErrorCode::Storage,
                    format!("follower {} flush_held_fsync: {problem}", self.node_id),
                )
            })?;
            // The flush is the moment held appends become durable, and it may
            // have rolled the tail — reclaim here too, or a follower flushed
            // after a hold keeps its now-durable sealed prefix until the next
            // append happens to arrive (#290).
            self.run_retention(&mut state.segment);
            committed
        };
        // A durability barrier like any committed append, so the floor rides
        // it too (#240) — an HWM observed during the hold would otherwise
        // wait for the next append or quiesce to become durable.
        self.persist_committed_floor();
        Ok(committed)
    }

    /// Replace on-disk state after a simulated crash/reboot recovery.
    ///
    /// Returns the previous handles so the caller can drop them before
    /// recovering the same paths. The follower Arc stays in the replica set.
    pub fn swap_storage(
        &self,
        segment: impl Into<SegmentSet>,
        producer_epochs: ProducerEpochJournal,
    ) -> BrokerResult<(SegmentSet, ProducerEpochJournal)> {
        let segment: SegmentSet = segment.into();
        let (seg_topic, seg_topic_epoch, seg_range_id, seg_generation) =
            if let Some(desc) = segment.active().descriptor_v2() {
                (
                    desc.topic.as_str(),
                    desc.topic_epoch,
                    desc.lineage.range_id,
                    desc.lineage.generation,
                )
            } else {
                let desc = segment.active().descriptor();
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

    /// Where this follower's sealed prefix ends (#306): the next offset of
    /// its last sealed segment, `None` with nothing sealed. For the
    /// transition record a promotion from this replica reports (#240).
    pub fn sealed_prefix_end(&self) -> Option<u64> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .segment
            .sealed()
            .last()
            .map(|reader| reader.next_offset())
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
        let mut fetched = state
            .segment
            .fetch_through(start_offset, max_bytes, max_records, hwm);
        if let Ok(batch) = &fetched {
            // The byte budget excluded even the first committed record —
            // the leader's wire path refetches exactly that record so the
            // consumer always makes progress, and the two fetch surfaces
            // must agree on when a cursor can move. Like the leader's
            // guard, this reads the RAW batch before the visibility filter
            // below: an all-marker batch arrives with an ADVANCED
            // next_offset, so it cannot re-trigger the refetch.
            if batch.records.is_empty()
                && batch.next_offset == start_offset
                && batch.next_offset < batch.high_watermark
            {
                fetched = state
                    .segment
                    .fetch_through(start_offset, usize::MAX, 1, hwm);
            }
        }
        fetched
            .map(|mut batch| {
                // The same visibility rule as the leader's wire mapping —
                // one predicate for both fetch surfaces (#240): the fault
                // harness compares the two views record-for-record across
                // failovers, and a marker visible on one side only would
                // read as divergence. The batch's next_offset still steps
                // over what the filter removed.
                batch
                    .records
                    .retain(|record| crate::consumer_visible(&record.record));
                batch
            })
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
            // NOTE (#240): durability through the batch end is not the same
            // question as holding the SAME records, and this path cannot yet
            // tell them apart — see #261. An earlier attempt compared the epoch that wrote
            // the record against the request's fencing epoch and was wrong: a
            // newly promoted leader inherits its predecessor's prefix and
            // legitimately retransmits it under its own, newer epoch, so the
            // two differ constantly during ordinary catch-up. Live chaos
            // scenario 09 caught it — every post-failover produce lost its
            // quorum. The real comparison needs the epoch each record was
            // ORIGINALLY written under, which the request does not carry.
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
        // A v1 frame stores the producer identity MERGED with the epoch, so
        // a promotion marker written here would lose the reserved identity
        // that keeps it invisible — and would surface as a phantom record if
        // this follower ever serves a fetch or is promoted. Refusing keeps a
        // mixed-format set honest: the marker stays unacked here and the
        // leader's quorum count says so, instead of a hidden visibility leak
        // saying nothing (#240).
        if self.segment_format == SegmentFormat::V1
            && request.producer_id == PROMOTION_MARKER_PRODUCER
        {
            return Err((
                ErrorCode::InvalidRequest,
                "a v1-format follower cannot store the promotion marker under its \
                 recognizable identity, so it refuses the marker rather than hide the loss"
                    .to_owned(),
            ));
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
        // Rolls at the follower's own bound, mirroring the leader: the leader
        // replicates offsets, not files, so where each replica's boundaries
        // fall is local to that replica. EXCEPT under an fsync hold — a roll
        // seals the tail, and sealing makes bytes durable, which would
        // silently commit records the fault injection promised were still at
        // risk. Under a hold the bound refuses instead, the pre-roll
        // behaviour, so the harness's crash-loss guarantee stays exact.
        let appended = if self.hold_fsync() {
            state.segment.append_group_tail_only(&records, durability)
        } else {
            state.segment.append_group_minting(&records, durability)
        };
        match appended {
            Ok(_) => {
                // Not under an fsync hold: the branch above already refused
                // to roll there, and retention deletes files, which is even
                // less compatible with "held bytes die with a crash". The
                // floor persist rides the same condition — this append was a
                // durability barrier, held bytes were not — but runs after
                // the state lock drops: it reads only the shared cell and
                // its own file, and holding the follower's lock across a
                // sidecar fsync would serialize every other lock user.
                let barrier = !self.hold_fsync();
                if barrier {
                    self.run_retention(&mut state.segment);
                }
                let response = ReplicaAppendResponse {
                    local_committed_offset: state.segment.committed_offset(),
                };
                drop(state);
                if barrier {
                    self.persist_committed_floor();
                }
                Ok(response)
            }
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
            // Same refusal as the single-request path: a v1 frame cannot
            // store the marker's reserved identity, and hiding that loss
            // would be worse than failing the ack (#240).
            if self.segment_format == SegmentFormat::V1
                && request.producer_id == PROMOTION_MARKER_PRODUCER
            {
                return Err((
                    ErrorCode::InvalidRequest,
                    "a v1-format follower cannot store the promotion marker under its \
                     recognizable identity, so it refuses the marker rather than hide the loss"
                        .to_owned(),
                ));
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
            // A roll mid-batch commits the sealed tail early, which
            // strengthens the shared durability barrier for a HEALTHY
            // follower: nothing is acknowledged before the final commit
            // below. Under an fsync hold that same early commit would break
            // the injection's promise that held bytes die with a crash, so
            // the bound refuses instead of rolling there.
            let appended = if self.hold_fsync() {
                state
                    .segment
                    .append_group_tail_only(&records, Durability::Buffered)
            } else {
                state
                    .segment
                    .append_group_minting(&records, Durability::Buffered)
            };
            if let Err(problem) = appended {
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
            Ok(local_committed_offset) => {
                // One pass per batch, after the commit, and never under an
                // fsync hold — the hold path returned above (#290). The floor
                // persist rides the same barrier (#240) but AFTER the state
                // lock drops: it reads only the shared cell and its own file,
                // and holding the follower's lock across a sidecar fsync
                // would serialize every other lock user behind it.
                self.run_retention(&mut state.segment);
                drop(state);
                self.persist_committed_floor();
                Ok(ReplicaAppendResponse {
                    local_committed_offset,
                })
            }
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
        self.arm_committed_floor();
        // The floor this update just advanced can make sealed segments
        // eligible, and on a range that then goes idle no later append
        // would ever reclaim them — the follower half of the leader's
        // post-publish pass (#408 review). NOT under an fsync hold, for
        // the same reason the apply barrier refuses there: deleting files
        // breaks the injection's promise that held bytes die with a crash.
        // (The floor-persist doctrine is untouched: nothing here fsyncs
        // the sidecar; retention is its own bounded I/O with its own
        // failure reporting.)
        if !self.hold_fsync() {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.run_retention(&mut state.segment);
        }
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

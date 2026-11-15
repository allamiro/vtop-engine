//! Where a produce's time goes (#752): the leader's local fsync, the
//! replication round, and the commit, each timed apart so the cost of a
//! request can be split before the path is changed.
//!
//! Measurement only: nothing here may change what is acknowledged or when.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;
use vtop_broker::group_commit::GroupCommitConfig;
use vtop_broker::produce_phases::ProducePhase;
use vtop_broker::replication::{
    ClusterCommittedOffset, InProcessFollower, InProcessReplicaSet, ReplicaQuorumResult, ReplicaSet,
};
use vtop_broker::{LocalBroker, MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
use vtop_protocol::{
    CommittedHwmUpdate, Durability as WireDurability, Message, ProduceRecord, ProduceRequest,
    RangeIdentity, ReplicaAppendRequest, Role, WireFrame,
};

const LEADER: Uuid = Uuid::from_u128(0xA1);
const EPOCH: u64 = 18;
// Far enough apart that a sleep overshooting on a loaded runner cannot make
// one phase's floor look like the other's.
const REPLICATION_DELAY: Duration = Duration::from_millis(100);
const PROPAGATE_DELAY: Duration = Duration::from_millis(300);

/// Real followers behind a fixed delay on each side of the commit: the
/// replication round takes at least `REPLICATION_DELAY`, the mark's
/// propagation at least `PROPAGATE_DELAY`, so each phase has a floor the
/// totals must reach and a phase that took the other's time would show.
struct DelayedReplicaSet {
    inner: InProcessReplicaSet,
    rounds: AtomicU64,
}

impl ReplicaSet for DelayedReplicaSet {
    fn replication_factor(&self) -> usize {
        self.inner.replication_factor()
    }

    fn replicate_append_batch(
        &self,
        requests: &[ReplicaAppendRequest],
        leader_committed_offset: u64,
    ) -> ReplicaQuorumResult {
        self.rounds.fetch_add(1, Ordering::SeqCst);
        thread::sleep(REPLICATION_DELAY);
        self.inner
            .replicate_append_batch(requests, leader_committed_offset)
    }

    fn propagate_committed_hwm(&self, update: &CommittedHwmUpdate) {
        thread::sleep(PROPAGATE_DELAY);
        self.inner.propagate_committed_hwm(update);
    }
}

fn range() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

fn open_segment(dir: &TempDir, segment_id: u128) -> ActiveSegment {
    let range = range();
    ActiveSegment::create(
        dir.path().join("range.active"),
        SegmentDescriptor {
            segment_id: Uuid::from_u128(segment_id),
            topic: range.topic.clone(),
            topic_epoch: range.topic_epoch,
            lineage: RangeLineage {
                range_id: range.range_id,
                generation: range.range_generation,
                key_range: KeyRange::full(),
                parents: Vec::new(),
            },
            base_offset: 0,
        },
        SegmentConfig::default(),
    )
    .unwrap()
}

fn replicated_leader(group_commit: Option<GroupCommitConfig>) -> (Arc<LocalBroker>, Vec<TempDir>) {
    let meta = MetaFencingEpoch::new(EPOCH);
    let mut dirs = Vec::new();
    let mut followers = Vec::new();
    for index in 0..2u128 {
        let dir = tempfile::tempdir().unwrap();
        let segment = open_segment(&dir, 0xE1 + index);
        let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
        followers.push(Arc::new(
            InProcessFollower::new(
                Uuid::from_u128(0xF1 + index),
                segment,
                epochs,
                range(),
                EPOCH,
                meta.clone(),
                ClusterCommittedOffset::new(0),
            )
            .unwrap(),
        ));
        dirs.push(dir);
    }
    let leader_dir = tempfile::tempdir().unwrap();
    let segment = open_segment(&leader_dir, 0xD1);
    let epochs = ProducerEpochJournal::open(leader_dir.path().join("epochs")).unwrap();
    let replicas = Arc::new(DelayedReplicaSet {
        inner: InProcessReplicaSet::new(followers),
        rounds: AtomicU64::new(0),
    });
    let mut broker = LocalBroker::with_replication(
        segment,
        epochs,
        range(),
        EPOCH,
        meta,
        LEADER,
        Some(ClusterCommittedOffset::new(0)),
        Some(replicas as Arc<dyn ReplicaSet>),
    )
    .unwrap();
    if let Some(config) = group_commit {
        broker = broker.with_group_commit(config).unwrap();
    }
    dirs.push(leader_dir);
    (Arc::new(broker), dirs)
}

fn produce(producer: u128, durability: WireDurability) -> WireFrame {
    WireFrame {
        request_id: 1,
        stream_id: 1,
        message: Message::ProduceRequest(ProduceRequest {
            range: range(),
            fencing_epoch: EPOCH,
            producer_id: Uuid::from_u128(producer),
            producer_epoch: 1,
            first_sequence: 0,
            durability,
            records: vec![ProduceRecord {
                timestamp_millis: 1,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        }),
    }
}

fn expect_ack(frame: WireFrame) {
    assert!(
        matches!(frame.message, Message::ProduceResponse(_)),
        "the produce must be acknowledged: {frame:?}"
    );
}

fn spent(broker: &LocalBroker, phase: ProducePhase) -> Duration {
    Duration::from_nanos(broker.produce_phases().nanos(phase))
}

/// A quorum produce pays all three phases, and each total reaches the floor
/// the delays set for it: the replication round's time is not booked to the
/// commit or the other way round. Together they fit inside the request.
#[test]
fn a_quorum_produce_splits_its_time_into_fsync_replicate_and_commit() {
    let (broker, _dirs) = replicated_leader(None);
    let started = Instant::now();
    expect_ack(broker.handle(Role::Producer, produce(0xB1, WireDurability::Quorum)));
    let request = started.elapsed();

    let fsync = spent(&broker, ProducePhase::LocalFsync);
    let replicate = spent(&broker, ProducePhase::Replicate);
    let commit = spent(&broker, ProducePhase::Commit);
    assert!(fsync > Duration::ZERO, "the leader fsynced: {fsync:?}");
    assert!(
        replicate >= REPLICATION_DELAY && replicate < REPLICATION_DELAY + PROPAGATE_DELAY,
        "the replication round took at least {REPLICATION_DELAY:?} and must not hold the \
         propagation's {PROPAGATE_DELAY:?}: {replicate:?}"
    );
    assert!(
        commit >= PROPAGATE_DELAY && commit < PROPAGATE_DELAY + REPLICATION_DELAY,
        "the commit holds the mark's propagation ({PROPAGATE_DELAY:?}) and not the round: \
         {commit:?}"
    );
    assert!(
        fsync + replicate + commit <= request,
        "the phases are parts of the request ({request:?}): {fsync:?} + {replicate:?} + {commit:?}"
    );
}

/// A standalone LocalFsync produce has no replication round and no cluster
/// commit: everything it spends is the local fsync.
#[test]
fn a_local_produce_spends_only_the_local_fsync() {
    let dir = tempfile::tempdir().unwrap();
    let segment = open_segment(&dir, 0xD1);
    let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
    let broker = LocalBroker::new(segment, epochs, range(), EPOCH).unwrap();
    expect_ack(broker.handle(Role::Producer, produce(0xB1, WireDurability::LocalFsync)));
    assert!(spent(&broker, ProducePhase::LocalFsync) > Duration::ZERO);
    assert_eq!(spent(&broker, ProducePhase::Replicate), Duration::ZERO);
    assert_eq!(spent(&broker, ProducePhase::Commit), Duration::ZERO);
}

/// A request refused at admission reaches no phase: the totals measure the
/// path, not the refusals.
#[test]
fn a_refused_produce_is_not_timed() {
    let (broker, _dirs) = replicated_leader(None);
    // LocalFsync on a replicated range is refused before the append.
    let refused = broker.handle(Role::Producer, produce(0xB1, WireDurability::LocalFsync));
    assert!(matches!(refused.message, Message::Error(_)), "{refused:?}");
    for phase in ProducePhase::ALL {
        assert_eq!(spent(&broker, phase), Duration::ZERO, "{phase:?}");
    }
}

/// With group commit on, a group is ONE flush: its members share the phases
/// and the totals count the group's round once, not once per member.
#[test]
fn a_commit_group_is_timed_once_not_once_per_member() {
    let (broker, _dirs) = replicated_leader(Some(GroupCommitConfig {
        // Seals on the fourth request; the delay is a backstop.
        max_delay: Duration::from_secs(10),
        max_records: 4,
        max_bytes: 1024 * 1024,
        max_pending_requests: 32,
    }));
    let barrier = Arc::new(Barrier::new(4));
    let handles: Vec<_> = (0..4u128)
        .map(|index| {
            let broker = Arc::clone(&broker);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                expect_ack(broker.handle(
                    Role::Producer,
                    produce(0xB0 + index, WireDurability::Quorum),
                ));
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(broker.group_commit().unwrap().metrics().commits_total(), 1);
    let replicate = spent(&broker, ProducePhase::Replicate);
    assert!(
        replicate >= REPLICATION_DELAY && replicate < REPLICATION_DELAY * 2,
        "one group is one replication round, timed once: {replicate:?}"
    );
}

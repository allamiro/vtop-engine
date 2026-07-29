//! Adaptive cross-session group commit (#185).
//!
//! Concurrent producer sessions must share one local durability / quorum
//! barrier, never ack before that barrier, bound queue wait, and fail closed
//! when the queue is full.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;
use vtop_broker::group_commit::GroupCommitConfig;
use vtop_broker::replication::{
    ClusterCommittedOffset, InProcessFollower, InProcessReplicaSet, ReplicaQuorumResult, ReplicaSet,
};
use vtop_broker::{FlushReason, LocalBroker, MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
use vtop_protocol::{
    CommittedHwmUpdate, Durability as WireDurability, ErrorCode, ErrorResponse, Message,
    ProduceRecord, ProduceRequest, ProduceResponse, RangeIdentity, ReplicaAppendRequest, Role,
    WireFrame,
};

const LEADER: Uuid = Uuid::from_u128(0xA1);
const FOLLOWER_1: Uuid = Uuid::from_u128(0xA2);
const FOLLOWER_2: Uuid = Uuid::from_u128(0xA3);
const FENCING_EPOCH: u64 = 18;

struct CountingReplicaSet {
    inner: Arc<InProcessReplicaSet>,
    batch_calls: AtomicU64,
}

impl CountingReplicaSet {
    fn new(inner: Arc<InProcessReplicaSet>) -> Self {
        Self {
            inner,
            batch_calls: AtomicU64::new(0),
        }
    }
}

impl ReplicaSet for CountingReplicaSet {
    fn replication_factor(&self) -> usize {
        self.inner.replication_factor()
    }

    fn replicate_append_batch(
        &self,
        requests: &[ReplicaAppendRequest],
        leader_committed_offset: u64,
    ) -> ReplicaQuorumResult {
        self.batch_calls.fetch_add(1, Ordering::SeqCst);
        self.inner
            .replicate_append_batch(requests, leader_committed_offset)
    }

    fn propagate_committed_hwm(&self, update: &CommittedHwmUpdate) {
        self.inner.propagate_committed_hwm(update);
    }
}

struct FailingReplicaSet {
    replication_factor: usize,
    calls: AtomicU64,
}

impl ReplicaSet for FailingReplicaSet {
    fn replication_factor(&self) -> usize {
        self.replication_factor
    }

    fn replicate_append_batch(
        &self,
        _requests: &[ReplicaAppendRequest],
        _leader_committed_offset: u64,
    ) -> ReplicaQuorumResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ReplicaQuorumResult {
            follower_acks: 0,
            replication_factor: self.replication_factor,
        }
    }

    fn propagate_committed_hwm(&self, _update: &CommittedHwmUpdate) {}
}

struct HoldingReplicaSet {
    inner: Arc<InProcessReplicaSet>,
    gate: Arc<Mutex<()>>,
}

impl ReplicaSet for HoldingReplicaSet {
    fn replication_factor(&self) -> usize {
        self.inner.replication_factor()
    }

    fn replicate_append_batch(
        &self,
        requests: &[ReplicaAppendRequest],
        leader_committed_offset: u64,
    ) -> ReplicaQuorumResult {
        let _hold = self.gate.lock().unwrap();
        self.inner
            .replicate_append_batch(requests, leader_committed_offset)
    }

    fn propagate_committed_hwm(&self, update: &CommittedHwmUpdate) {
        self.inner.propagate_committed_hwm(update);
    }
}

fn range_identity() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

fn open_segment(dir: &TempDir, segment_id: u128, range: &RangeIdentity) -> ActiveSegment {
    let descriptor = SegmentDescriptor {
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
    };
    ActiveSegment::create(
        dir.path().join("range.active"),
        descriptor,
        SegmentConfig::default(),
    )
    .unwrap()
}

fn group_config() -> GroupCommitConfig {
    GroupCommitConfig {
        max_delay: Duration::from_millis(40),
        max_records: 64,
        max_bytes: 1024 * 1024,
        max_pending_requests: 32,
    }
}

fn build_followers(
    range: &RangeIdentity,
    meta: &MetaFencingEpoch,
) -> (Vec<TempDir>, Vec<Arc<InProcessFollower>>) {
    let mut dirs = Vec::new();
    let mut followers = Vec::new();
    for (index, node_id) in [FOLLOWER_1, FOLLOWER_2].into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let segment = open_segment(&dir, 0xE1 + index as u128, range);
        let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
        followers.push(Arc::new(
            InProcessFollower::new(
                node_id,
                segment,
                epochs,
                range.clone(),
                FENCING_EPOCH,
                meta.clone(),
                ClusterCommittedOffset::new(0),
            )
            .unwrap(),
        ));
        dirs.push(dir);
    }
    (dirs, followers)
}

fn produce_frame(
    range: RangeIdentity,
    producer_id: Uuid,
    sequence: u64,
    request_id: u64,
) -> WireFrame {
    WireFrame {
        request_id,
        stream_id: 1,
        message: Message::ProduceRequest(ProduceRequest {
            range,
            fencing_epoch: FENCING_EPOCH,
            producer_id,
            producer_epoch: 1,
            first_sequence: sequence,
            durability: WireDurability::Quorum,
            records: vec![ProduceRecord {
                timestamp_millis: 1_000,
                key: b"k".to_vec(),
                value: format!("v{sequence}").into_bytes(),
            }],
        }),
    }
}

fn expect_ok(frame: WireFrame) -> ProduceResponse {
    match frame.message {
        Message::ProduceResponse(value) => value,
        Message::Error(ErrorResponse { code, message, .. }) => {
            panic!("produce failed: {code:?}: {message}")
        }
        other => panic!("unexpected response: {other:?}"),
    }
}

#[test]
fn concurrent_sessions_share_one_quorum_barrier() {
    let range = range_identity();
    let meta = MetaFencingEpoch::new(FENCING_EPOCH);
    let cluster_committed = ClusterCommittedOffset::new(0);
    let leader_dir = tempfile::tempdir().unwrap();
    let leader_segment = open_segment(&leader_dir, 0xD1, &range);
    let leader_epochs = ProducerEpochJournal::open(leader_dir.path().join("epochs")).unwrap();
    let (follower_dirs, followers) = build_followers(&range, &meta);
    let counting = Arc::new(CountingReplicaSet::new(Arc::new(InProcessReplicaSet::new(
        followers,
    ))));
    let leader = Arc::new(
        LocalBroker::with_replication(
            leader_segment,
            leader_epochs,
            range.clone(),
            FENCING_EPOCH,
            meta,
            LEADER,
            Some(cluster_committed.clone()),
            Some(counting.clone() as Arc<dyn ReplicaSet>),
        )
        .unwrap()
        // Test-local override: the eight records produced below hit the count
        // threshold exactly, so the batch seals deterministically instead of
        // depending on all eight threads enqueueing inside a wall-clock delay
        // window (flaky on loaded CI runners). The 10s delay is a backstop.
        .with_group_commit(GroupCommitConfig {
            max_delay: Duration::from_secs(10),
            max_records: 8,
            max_bytes: 1024 * 1024,
            max_pending_requests: 32,
        })
        .unwrap(),
    );
    let _dirs = (leader_dir, follower_dirs);

    let barrier = Arc::new(Barrier::new(8));
    let mut handles = Vec::new();
    for index in 0..8u64 {
        let broker = Arc::clone(&leader);
        let range = range.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let producer = Uuid::from_u128(0xB0 + u128::from(index));
            expect_ok(broker.handle(Role::Producer, produce_frame(range, producer, 0, index + 1)))
        }));
    }
    let started = Instant::now();
    for handle in handles {
        let response = handle.join().unwrap();
        assert_eq!(response.outcomes.len(), 1);
        assert!(!response.outcomes[0].duplicate);
    }
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "group commit wait must stay bounded"
    );
    assert_eq!(
        counting.batch_calls.load(Ordering::SeqCst),
        1,
        "concurrent sessions must share one quorum replicate batch"
    );
    assert_eq!(cluster_committed.get(), 8);
    let sample = leader.group_commit().unwrap().metrics().last_sample();
    assert_eq!(sample.requests, 8);
    assert_eq!(sample.records, 8);
    assert!(matches!(
        sample.flush_reason,
        Some(FlushReason::MaxDelay | FlushReason::MaxRecords | FlushReason::MaxPending)
    ));
}

#[test]
fn failed_quorum_never_acks_partial_group() {
    let range = range_identity();
    let meta = MetaFencingEpoch::new(FENCING_EPOCH);
    let cluster_committed = ClusterCommittedOffset::new(0);
    let leader_dir = tempfile::tempdir().unwrap();
    let leader_segment = open_segment(&leader_dir, 0xD1, &range);
    let leader_epochs = ProducerEpochJournal::open(leader_dir.path().join("epochs")).unwrap();
    let failing = Arc::new(FailingReplicaSet {
        replication_factor: 3,
        calls: AtomicU64::new(0),
    });
    let leader = Arc::new(
        LocalBroker::with_replication(
            leader_segment,
            leader_epochs,
            range.clone(),
            FENCING_EPOCH,
            meta,
            LEADER,
            Some(cluster_committed.clone()),
            Some(failing.clone() as Arc<dyn ReplicaSet>),
        )
        .unwrap()
        .with_group_commit(group_config())
        .unwrap(),
    );
    let _dir = leader_dir;

    let barrier = Arc::new(Barrier::new(4));
    let errors = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for index in 0..4u64 {
        let broker = Arc::clone(&leader);
        let range = range.clone();
        let barrier = Arc::clone(&barrier);
        let errors = Arc::clone(&errors);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let producer = Uuid::from_u128(0xC0 + u128::from(index));
            let frame = broker.handle(Role::Producer, produce_frame(range, producer, 0, index + 1));
            match frame.message {
                Message::Error(error) => {
                    assert_eq!(error.code, ErrorCode::Overloaded);
                    errors.lock().unwrap().push(error.message);
                }
                Message::ProduceResponse(_) => panic!("partial group must not be acknowledged"),
                other => panic!("unexpected {other:?}"),
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(errors.lock().unwrap().len(), 4);
    assert_eq!(
        cluster_committed.get(),
        0,
        "HWM must not advance without quorum"
    );
    assert_eq!(failing.calls.load(Ordering::SeqCst), 1);
}

#[test]
fn full_queue_fails_closed_without_silent_drop() {
    let range = range_identity();
    let meta = MetaFencingEpoch::new(FENCING_EPOCH);
    let cluster_committed = ClusterCommittedOffset::new(0);
    let leader_dir = tempfile::tempdir().unwrap();
    let leader_segment = open_segment(&leader_dir, 0xD1, &range);
    let leader_epochs = ProducerEpochJournal::open(leader_dir.path().join("epochs")).unwrap();
    let (follower_dirs, followers) = build_followers(&range, &meta);
    let gate = Arc::new(Mutex::new(()));
    let flush_hold = gate.lock().unwrap();
    let holding = Arc::new(HoldingReplicaSet {
        inner: Arc::new(InProcessReplicaSet::new(followers)),
        gate: Arc::clone(&gate),
    });
    let leader = Arc::new(
        LocalBroker::with_replication(
            leader_segment,
            leader_epochs,
            range.clone(),
            FENCING_EPOCH,
            meta,
            LEADER,
            Some(cluster_committed),
            Some(holding as Arc<dyn ReplicaSet>),
        )
        .unwrap()
        .with_group_commit(GroupCommitConfig {
            max_delay: Duration::from_millis(200),
            max_records: 1_024,
            max_bytes: 1024 * 1024,
            // One queued request beyond the in-flight flush group.
            max_pending_requests: 1,
        })
        .unwrap(),
    );
    let _dirs = (leader_dir, follower_dirs);

    let first = {
        let broker = Arc::clone(&leader);
        let range = range.clone();
        thread::spawn(move || {
            expect_ok(broker.handle(
                Role::Producer,
                produce_frame(range, Uuid::from_u128(0xB1), 0, 1),
            ))
        })
    };
    // First request seals immediately (queue ceiling) and blocks in quorum fan-out.
    thread::sleep(Duration::from_millis(30));
    let second = {
        let broker = Arc::clone(&leader);
        let range = range.clone();
        thread::spawn(move || {
            expect_ok(broker.handle(
                Role::Producer,
                produce_frame(range, Uuid::from_u128(0xB2), 0, 2),
            ))
        })
    };
    thread::sleep(Duration::from_millis(20));
    let rejected = leader.handle(
        Role::Producer,
        produce_frame(range, Uuid::from_u128(0xB3), 0, 3),
    );
    match rejected.message {
        Message::Error(error) => assert_eq!(error.code, ErrorCode::Overloaded),
        other => panic!("expected overloaded, got {other:?}"),
    }
    drop(flush_hold);
    first.join().unwrap();
    second.join().unwrap();
}

#[test]
fn local_fsync_group_commit_shares_one_commit_metric() {
    let dir = tempfile::tempdir().unwrap();
    let range = range_identity();
    let segment = open_segment(&dir, 0xD1, &range);
    let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
    let broker = Arc::new(
        LocalBroker::new(segment, epochs, range.clone(), FENCING_EPOCH)
            .unwrap()
            .with_group_commit(group_config())
            .unwrap(),
    );
    let barrier = Arc::new(Barrier::new(6));
    let mut handles = Vec::new();
    for index in 0..6u64 {
        let broker = Arc::clone(&broker);
        let range = range.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let frame = WireFrame {
                request_id: index + 1,
                stream_id: 1,
                message: Message::ProduceRequest(ProduceRequest {
                    range,
                    fencing_epoch: FENCING_EPOCH,
                    producer_id: Uuid::from_u128(0xB0 + u128::from(index)),
                    producer_epoch: 1,
                    first_sequence: 0,
                    durability: WireDurability::LocalFsync,
                    records: vec![ProduceRecord {
                        timestamp_millis: 1,
                        key: Vec::new(),
                        value: b"x".to_vec(),
                    }],
                }),
            };
            expect_ok(broker.handle(Role::Producer, frame))
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let metrics = broker.group_commit().unwrap().metrics();
    assert_eq!(metrics.commits_total(), 1);
    assert_eq!(metrics.requests_total(), 6);
    assert!(metrics.sync_nanos_total() > 0);
}

//! Adaptive cross-session group commit (#185).
//!
//! Concurrent producer sessions must share one local durability / quorum
//! barrier, never ack before that barrier, bound queue wait, and fail closed
//! when the queue is full.
//!
//! The overlapped stages (#752, E3) are at the end: the next group's local
//! append and fsync run while the previous group replicates, the replica
//! rounds still run one at a time in offset order, and nothing about what is
//! acknowledged moves. System model: one leader process and two in-process
//! followers, crash-free; faults are a held or missed replica round, a held
//! or failed leader fsync, and a lease taken from the leader. Every check
//! there is a safety check, read back from the broker's answers, its log
//! offsets and the followers' logs.

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
use vtop_log::{
    ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor, SegmentSet,
};
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
    open_segment_with(dir, segment_id, range, SegmentConfig::default())
}

fn open_segment_with(
    dir: &TempDir,
    segment_id: u128,
    range: &RangeIdentity,
    config: SegmentConfig,
) -> ActiveSegment {
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
    ActiveSegment::create(dir.path().join("range.active"), descriptor, config).unwrap()
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
    // First request seals immediately (queue ceiling) and blocks in quorum
    // fan-out. The second then leads its own group: its local stage runs
    // while the first replicates, and it waits for the first's round (#752,
    // E3), so two groups are in flight and neither is queued. The third is
    // the one request the queue holds; the fourth finds it full.
    let coordinator = Arc::clone(leader.group_commit().unwrap());
    await_until("the first group to hold its replica round", || {
        coordinator.groups_in_flight() == 1 && coordinator.queued_requests() == 0
    });
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
    await_until("the second group to be in flight beside the first", || {
        coordinator.groups_in_flight() == 2 && coordinator.queued_requests() == 0
    });
    let third = {
        let broker = Arc::clone(&leader);
        let range = range.clone();
        thread::spawn(move || {
            expect_ok(broker.handle(
                Role::Producer,
                produce_frame(range, Uuid::from_u128(0xB3), 0, 3),
            ))
        })
    };
    await_until("the third request to be queued", || {
        coordinator.queued_requests() == 1
    });
    let rejected = leader.handle(
        Role::Producer,
        produce_frame(range, Uuid::from_u128(0xB4), 0, 4),
    );
    match rejected.message {
        Message::Error(error) => assert_eq!(error.code, ErrorCode::Overloaded),
        other => panic!("expected overloaded, got {other:?}"),
    }
    drop(flush_hold);
    first.join().unwrap();
    second.join().unwrap();
    third.join().unwrap();
}

/// Poll `condition` for up to 10 s, failing with `what` if it never holds.
fn await_until(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "waited 10 s for {what}");
        thread::sleep(Duration::from_millis(1));
    }
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

fn replicated_leader(
    config: GroupCommitConfig,
    wrap: impl FnOnce(Arc<InProcessReplicaSet>, MetaFencingEpoch) -> Arc<dyn ReplicaSet>,
) -> (Arc<LocalBroker>, ClusterCommittedOffset, Vec<TempDir>) {
    replicated_leader_on(config, SegmentConfig::default(), wrap)
}

/// [`replicated_leader`] whose own segment has `segment` limits.
fn replicated_leader_on(
    config: GroupCommitConfig,
    segment: SegmentConfig,
    wrap: impl FnOnce(Arc<InProcessReplicaSet>, MetaFencingEpoch) -> Arc<dyn ReplicaSet>,
) -> (Arc<LocalBroker>, ClusterCommittedOffset, Vec<TempDir>) {
    let range = range_identity();
    let meta = MetaFencingEpoch::new(FENCING_EPOCH);
    let cluster_committed = ClusterCommittedOffset::new(0);
    let leader_dir = tempfile::tempdir().unwrap();
    let leader_segment = open_segment_with(&leader_dir, 0xD1, &range, segment);
    let leader_epochs = ProducerEpochJournal::open(leader_dir.path().join("epochs")).unwrap();
    let (mut dirs, followers) = build_followers(&range, &meta);
    let replicas = wrap(Arc::new(InProcessReplicaSet::new(followers)), meta.clone());
    let leader = Arc::new(
        LocalBroker::with_replication(
            leader_segment,
            leader_epochs,
            range,
            FENCING_EPOCH,
            meta,
            LEADER,
            Some(cluster_committed.clone()),
            Some(replicas),
        )
        .unwrap()
        .with_group_commit(config)
        .unwrap(),
    );
    dirs.push(leader_dir);
    (leader, cluster_committed, dirs)
}

/// Seals on the `n`th request queued; the 10 s window is only a backstop, so
/// which requests share a group does not depend on thread timing.
fn sealing_at(n: usize) -> GroupCommitConfig {
    GroupCommitConfig {
        max_delay: Duration::from_secs(10),
        max_records: n,
        max_bytes: 1024 * 1024,
        max_pending_requests: 32,
    }
}

/// Run `frames` concurrently, released together, and return each answer in
/// the order given.
fn concurrently(broker: &Arc<LocalBroker>, frames: Vec<WireFrame>) -> Vec<WireFrame> {
    let barrier = Arc::new(Barrier::new(frames.len()));
    let handles: Vec<_> = frames
        .into_iter()
        .map(|frame| {
            let broker = Arc::clone(broker);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                broker.handle(Role::Producer, frame)
            })
        })
        .collect();
    handles.into_iter().map(|h| h.join().unwrap()).collect()
}

/// Defect 2 (#752, D6): one member's bad sequence must fail that member
/// only. The log validates a whole group before writing a byte and refuses
/// it whole, and the broker used to give every member that refusal; a
/// `SequenceConflict` is not retryable for an idempotent Kafka producer, so
/// one client's bad sequence would have failed another client's request
/// for good.
#[test]
fn one_members_sequence_conflict_does_not_fail_the_others() {
    let (leader, cluster_committed, _dirs) =
        replicated_leader(sealing_at(2), |inner, _| inner as Arc<dyn ReplicaSet>);
    let range = range_identity();
    let good = Uuid::from_u128(0xB1);
    let bad = Uuid::from_u128(0xB2);
    // A new producer's first sequence must be 0; this one starts at 5.
    let answers = concurrently(
        &leader,
        vec![
            produce_frame(range.clone(), good, 0, 1),
            produce_frame(range, bad, 5, 2),
        ],
    );
    let metrics = leader.group_commit().unwrap().metrics();
    assert_eq!(
        metrics.last_sample().requests,
        2,
        "the two requests must have shared a group for this test to mean anything"
    );
    let response = expect_ok(answers[0].clone());
    assert_eq!(
        response.outcomes.len(),
        1,
        "the good member is acknowledged"
    );
    match &answers[1].message {
        Message::Error(ErrorResponse { code, .. }) => {
            assert_eq!(*code, ErrorCode::SequenceConflict, "{:?}", answers[1])
        }
        other => panic!("the bad member must be refused: {other:?}"),
    }
    assert_eq!(
        cluster_committed.get(),
        1,
        "exactly the good member's record is committed"
    );
}

/// `members`, each a `(producer, epoch)` request with one 700-byte record,
/// queued in this order into one group that the log refuses as too large
/// (one such record fits a group, two do not), so the group is split and
/// each member re-run alone. Returns the answers, in order, and the
/// cluster's committed offset.
fn split_group(members: &[(u128, u64)]) -> (Vec<WireFrame>, u64) {
    let segment = SegmentConfig {
        max_record_bytes: 1024,
        max_group_bytes: 1024 + 128,
        ..SegmentConfig::default()
    };
    let (leader, cluster_committed, _dirs) =
        replicated_leader_on(sealing_at(members.len()), segment, |inner, _| {
            inner as Arc<dyn ReplicaSet>
        });
    let coordinator = leader.group_commit().unwrap();
    let mut handles = Vec::new();
    for (index, &(producer, producer_epoch)) in members.iter().enumerate() {
        let frame = WireFrame {
            request_id: index as u64 + 1,
            stream_id: 1,
            message: Message::ProduceRequest(ProduceRequest {
                range: range_identity(),
                fencing_epoch: FENCING_EPOCH,
                producer_id: Uuid::from_u128(producer),
                producer_epoch,
                first_sequence: 0,
                durability: WireDurability::Quorum,
                records: vec![ProduceRecord {
                    timestamp_millis: 1_000,
                    key: b"k".to_vec(),
                    value: vec![b'v'; 700],
                }],
            }),
        };
        let sender = Arc::clone(&leader);
        handles.push(thread::spawn(move || sender.handle(Role::Producer, frame)));
        // Each request is queued before the next is sent, so the group's
        // order does not depend on thread timing. The last one seals the
        // group and is taken at once, so it is never seen queued.
        if index + 1 == members.len() {
            break;
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while coordinator.queued_requests() <= index {
            assert!(
                Instant::now() < deadline,
                "request {} was not queued within 10 s",
                index + 1
            );
            thread::sleep(Duration::from_millis(1));
        }
    }
    let answers = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let metrics = coordinator.metrics();
    assert_eq!(
        (metrics.last_sample().requests, metrics.split_groups_total()),
        (members.len() as u64, 1),
        "the requests must have shared a group and been split for this test to mean anything"
    );
    (answers, cluster_committed.get())
}

fn expect_acked(answer: &WireFrame, epoch: u64) {
    match &answer.message {
        Message::ProduceResponse(response) => assert_eq!(
            response.outcomes.len(),
            1,
            "the epoch-{epoch} request is acknowledged with its record"
        ),
        other => {
            panic!("the epoch-{epoch} request was refused after its group was split: {other:?}")
        }
    }
}

/// Review of #780: a group split request by request must keep the
/// admission its first pass gave each member. One producer's epoch-1
/// request and then its epoch-2 request share a group; the first pass
/// admits both in queue order, durably advancing the producer's epoch
/// journal to 2. The log refuses the group whole as too large and each
/// member is re-run alone. The re-run judged the epoch-1 request against
/// the journal its own group had just advanced and fenced it: a permanent
/// refusal for a request that, sent alone ahead of the epoch-2 one, is
/// accepted.
#[test]
fn a_split_group_keeps_its_first_passes_producer_admissions() {
    let (answers, committed) = split_group(&[(0xB5, 1), (0xB5, 2)]);
    expect_acked(&answers[0], 1);
    expect_acked(&answers[1], 2);
    assert_eq!(committed, 2, "both records are committed, epoch 1 first");
}

/// A guard for the fix above: keeping a split group's admissions must not
/// weaken a fence. Producer B5's epoch-2 request, another producer's
/// request, then B5's epoch-1 request: the first pass fences the epoch-1
/// request behind epoch 2, the other two are too large together, and after
/// the split the epoch-1 request is still fenced, exactly as it is sent
/// alone after the epoch-2 one.
#[test]
fn a_split_group_still_fences_an_older_epoch_queued_behind_a_newer() {
    let (answers, committed) = split_group(&[(0xB5, 2), (0xB6, 1), (0xB5, 1)]);
    expect_acked(&answers[0], 2);
    expect_acked(&answers[1], 1);
    match &answers[2].message {
        Message::Error(ErrorResponse { code, .. }) => assert_eq!(
            *code,
            ErrorCode::Fenced,
            "the epoch-1 request is fenced: {:?}",
            answers[2]
        ),
        other => panic!("the epoch-1 request must be fenced behind epoch 2: {other:?}"),
    }
    assert_eq!(committed, 2, "only the two admitted records are committed");
}

/// A replica set that loses the lease in the middle of the round: the
/// followers ack (a quorum), but metadata has moved on before the leader
/// can publish the commit.
struct StealingReplicaSet {
    inner: Arc<InProcessReplicaSet>,
    meta: MetaFencingEpoch,
}

impl ReplicaSet for StealingReplicaSet {
    fn replication_factor(&self) -> usize {
        self.inner.replication_factor()
    }

    fn replicate_append_batch(
        &self,
        requests: &[ReplicaAppendRequest],
        leader_committed_offset: u64,
    ) -> ReplicaQuorumResult {
        let result = self
            .inner
            .replicate_append_batch(requests, leader_committed_offset);
        self.meta.set(FENCING_EPOCH + 1);
        result
    }

    fn propagate_committed_hwm(&self, update: &CommittedHwmUpdate) {
        self.inner.propagate_committed_hwm(update);
    }
}

/// D5 (#752), a guard: a lease lost between a group's fsync and its commit
/// fences EVERY member, and the group's records are never published as
/// committed, although a quorum held them.
#[test]
fn a_lease_lost_between_fsync_and_commit_fences_every_member() {
    let (leader, cluster_committed, _dirs) = replicated_leader(sealing_at(3), |inner, meta| {
        Arc::new(StealingReplicaSet { inner, meta }) as Arc<dyn ReplicaSet>
    });
    let range = range_identity();
    let answers = concurrently(
        &leader,
        (0..3u128)
            .map(|index| produce_frame(range.clone(), Uuid::from_u128(0xB0 + index), 0, 1))
            .collect(),
    );
    assert_eq!(
        leader
            .group_commit()
            .unwrap()
            .metrics()
            .last_sample()
            .requests,
        3
    );
    for answer in &answers {
        match &answer.message {
            Message::Error(ErrorResponse { code, .. }) => assert_eq!(*code, ErrorCode::Fenced),
            other => panic!("a member was acknowledged past a lost lease: {other:?}"),
        }
    }
    assert_eq!(
        cluster_committed.get(),
        0,
        "nothing is published as committed"
    );
}

/// Storage whose data barrier can be held: `sync_data` blocks while the
/// gate is armed, and says when it has started.
struct HoldingStorage {
    inner: vtop_log::env::RealStorage,
    gate: Arc<SyncGate>,
}

#[derive(Default)]
struct SyncGate {
    armed: std::sync::atomic::AtomicBool,
    /// Fail the next data or directory sync, once, without holding it.
    fail_next: std::sync::atomic::AtomicBool,
    entered: Mutex<bool>,
    entered_cv: std::sync::Condvar,
    released: Mutex<bool>,
    released_cv: std::sync::Condvar,
}

impl SyncGate {
    fn pass(&self) -> std::io::Result<()> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(std::io::Error::other("injected fsync failure"));
        }
        if !self.armed.load(Ordering::SeqCst) {
            return Ok(());
        }
        *self.entered.lock().unwrap() = true;
        self.entered_cv.notify_all();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.released_cv.wait(released).unwrap();
        }
        Ok(())
    }

    fn await_entered(&self) {
        let mut entered = self.entered.lock().unwrap();
        while !*entered {
            let (guard, timeout) = self
                .entered_cv
                .wait_timeout(entered, Duration::from_secs(10))
                .unwrap();
            entered = guard;
            assert!(
                !timeout.timed_out() || *entered,
                "the group never reached its fsync"
            );
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.released_cv.notify_all();
    }
}

struct HoldingFile {
    inner: Box<dyn vtop_log::env::StorageFile>,
    gate: Arc<SyncGate>,
}

impl std::io::Read for HoldingFile {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buffer)
    }
}

impl std::io::Write for HoldingFile {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buffer)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl std::io::Seek for HoldingFile {
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(position)
    }
}

impl vtop_log::env::StorageFile for HoldingFile {
    fn sync_data(&mut self) -> std::io::Result<()> {
        self.gate.pass()?;
        self.inner.sync_data()
    }
    fn set_len(&mut self, len: u64) -> std::io::Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> std::io::Result<u64> {
        self.inner.len()
    }
}

impl vtop_log::env::Storage for HoldingStorage {
    fn open(
        &self,
        path: &std::path::Path,
        mode: vtop_log::env::OpenMode,
    ) -> std::io::Result<Box<dyn vtop_log::env::StorageFile>> {
        Ok(Box::new(HoldingFile {
            inner: self.inner.open(path, mode)?,
            gate: Arc::clone(&self.gate),
        }))
    }
    fn read(&self, path: &std::path::Path) -> std::io::Result<Vec<u8>> {
        self.inner.read(path)
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
        self.inner.rename(from, to)
    }
    fn remove_file(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.inner.remove_file(path)
    }
    fn exists(&self, path: &std::path::Path) -> std::io::Result<bool> {
        self.inner.exists(path)
    }
    fn read_dir(
        &self,
        path: &std::path::Path,
    ) -> std::io::Result<Vec<vtop_log::env::DirEntryInfo>> {
        self.inner.read_dir(path)
    }
    fn sync_dir(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.gate.pass()?;
        self.inner.sync_dir(path)
    }
}

/// D5 (#752), a guard: no member of a group is answered before the group's
/// fsync returns. The data barrier is held mid-group; while it is, nothing
/// may come back, and once it returns every member is acknowledged.
#[test]
fn no_member_is_acknowledged_before_the_groups_fsync_returns() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(SyncGate::default());
    let env = vtop_log::env::Env {
        storage: Arc::new(HoldingStorage {
            inner: vtop_log::env::RealStorage,
            gate: Arc::clone(&gate),
        }),
        ..vtop_log::env::Env::real()
    };
    let range = range_identity();
    let segment = ActiveSegment::create_in(
        &env,
        dir.path().join("range.active"),
        SegmentDescriptor {
            segment_id: Uuid::from_u128(0xD1),
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
    .unwrap();
    let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
    let broker = Arc::new(
        LocalBroker::new(segment, epochs, range.clone(), FENCING_EPOCH)
            .unwrap()
            .with_group_commit(sealing_at(3))
            .unwrap(),
    );
    gate.armed.store(true, Ordering::SeqCst);
    let (answered_tx, answered) = std::sync::mpsc::channel();
    let handles: Vec<_> = (0..3u128)
        .map(|index| {
            let broker = Arc::clone(&broker);
            let range = range.clone();
            let answered_tx = answered_tx.clone();
            thread::spawn(move || {
                let mut frame = produce_frame(range, Uuid::from_u128(0xB0 + index), 0, 1);
                if let Message::ProduceRequest(request) = &mut frame.message {
                    request.durability = WireDurability::LocalFsync;
                }
                answered_tx
                    .send(broker.handle(Role::Producer, frame))
                    .unwrap();
            })
        })
        .collect();
    gate.await_entered();
    thread::sleep(Duration::from_millis(100));
    assert!(
        answered.try_recv().is_err(),
        "a member was answered while its group's fsync was still held"
    );
    gate.release();
    for handle in handles {
        handle.join().unwrap();
    }
    let answers: Vec<_> = answered.try_iter().collect();
    assert_eq!(answers.len(), 3);
    for answer in answers {
        expect_ok(answer);
    }
    assert_eq!(broker.group_commit().unwrap().metrics().commits_total(), 1);
}

// ---------------------------------------------------------------------------
// Overlapped stages (#752, E3).
// ---------------------------------------------------------------------------

/// Each request is its own group, sealed the moment it is queued, so which
/// requests share a group never depends on thread timing.
fn one_request_groups() -> GroupCommitConfig {
    GroupCommitConfig {
        max_delay: Duration::ZERO,
        max_records: 1,
        max_bytes: 1024 * 1024,
        max_pending_requests: 32,
    }
}

#[derive(Default)]
struct Rounds {
    /// `(base offset of the round's first request, leader committed offset
    /// it must cover)` per round, in the order the rounds were called.
    calls: Vec<(u64, u64)>,
    /// Rounds allowed through to the followers so far.
    released: usize,
    /// Rounds answered as a quorum miss without reaching the followers.
    missed: Vec<usize>,
}

/// In-process followers behind a gate: every round is recorded when it is
/// called and waits until the test releases it.
struct GatedReplicaSet {
    inner: InProcessReplicaSet,
    rounds: Mutex<Rounds>,
    changed: std::sync::Condvar,
}

impl GatedReplicaSet {
    fn new(inner: InProcessReplicaSet, released: usize) -> Self {
        Self {
            inner,
            rounds: Mutex::new(Rounds {
                released,
                ..Rounds::default()
            }),
            changed: std::sync::Condvar::new(),
        }
    }

    /// Let rounds through until `released` have been.
    fn release(&self, released: usize) {
        self.rounds.lock().unwrap().released = released;
        self.changed.notify_all();
    }

    fn miss(&self, round: usize) {
        self.rounds.lock().unwrap().missed.push(round);
    }

    fn calls(&self) -> Vec<(u64, u64)> {
        self.rounds.lock().unwrap().calls.clone()
    }

    /// Wait until `count` rounds have been called.
    fn await_called(&self, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut rounds = self.rounds.lock().unwrap();
        while rounds.calls.len() < count {
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(
                !left.is_zero(),
                "{} replica round(s) were called in 10 s, expected {count}: {:?}",
                rounds.calls.len(),
                rounds.calls
            );
            rounds = self.changed.wait_timeout(rounds, left).unwrap().0;
        }
    }

    fn followers(&self) -> &[Arc<InProcessFollower>] {
        self.inner.followers()
    }
}

impl ReplicaSet for GatedReplicaSet {
    fn replication_factor(&self) -> usize {
        self.inner.replication_factor()
    }

    fn replicate_append_batch(
        &self,
        requests: &[ReplicaAppendRequest],
        leader_committed_offset: u64,
    ) -> ReplicaQuorumResult {
        let missed = {
            let mut rounds = self.rounds.lock().unwrap();
            let round = rounds.calls.len();
            rounds.calls.push((
                requests
                    .first()
                    .map(|request| request.expected_base_offset)
                    .unwrap_or(u64::MAX),
                leader_committed_offset,
            ));
            self.changed.notify_all();
            while rounds.released <= round {
                rounds = self.changed.wait(rounds).unwrap();
            }
            rounds.missed.contains(&round)
        };
        if missed {
            return ReplicaQuorumResult {
                follower_acks: 0,
                replication_factor: self.replication_factor(),
            };
        }
        self.inner
            .replicate_append_batch(requests, leader_committed_offset)
    }

    fn propagate_committed_hwm(&self, update: &CommittedHwmUpdate) {
        self.inner.propagate_committed_hwm(update);
    }
}

struct Staged {
    leader: Arc<LocalBroker>,
    cluster_committed: ClusterCommittedOffset,
    rounds: Arc<GatedReplicaSet>,
    /// The leader's view of its lease; the followers hold their own, as on
    /// separate nodes, so a test can take the lease from the leader alone.
    meta: MetaFencingEpoch,
    /// The leader's data and directory syncs pass through this gate.
    sync: Arc<SyncGate>,
    _dirs: Vec<TempDir>,
}

/// A replicated leader with group commit on, whose log is a segment set on
/// gated storage and whose replica rounds wait for `released`.
fn staged_leader(config: GroupCommitConfig, segment: SegmentConfig, released: usize) -> Staged {
    let range = range_identity();
    let meta = MetaFencingEpoch::new(FENCING_EPOCH);
    let (mut dirs, followers) = build_followers(&range, &MetaFencingEpoch::new(FENCING_EPOCH));
    let rounds = Arc::new(GatedReplicaSet::new(
        InProcessReplicaSet::new(followers),
        released,
    ));
    let sync = Arc::new(SyncGate::default());
    let env = vtop_log::env::Env {
        storage: Arc::new(HoldingStorage {
            inner: vtop_log::env::RealStorage,
            gate: Arc::clone(&sync),
        }),
        ..vtop_log::env::Env::real()
    };
    let leader_dir = tempfile::tempdir().unwrap();
    // A directory of its own, so a tail rebuilt after a failed write (#881)
    // reopens exactly the segments.
    let segments = leader_dir.path().join("segments");
    std::fs::create_dir_all(&segments).unwrap();
    let set = SegmentSet::create_in(
        &env,
        &segments,
        SegmentDescriptor {
            segment_id: Uuid::from_u128(0xD1),
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
        segment,
    )
    .unwrap();
    let epochs = ProducerEpochJournal::open(leader_dir.path().join("epochs")).unwrap();
    let cluster_committed = ClusterCommittedOffset::new(0);
    let leader = Arc::new(
        LocalBroker::with_replication(
            set,
            epochs,
            range,
            FENCING_EPOCH,
            meta.clone(),
            LEADER,
            Some(cluster_committed.clone()),
            Some(Arc::clone(&rounds) as Arc<dyn ReplicaSet>),
        )
        .unwrap()
        .with_group_commit(config)
        .unwrap(),
    );
    dirs.push(leader_dir);
    Staged {
        leader,
        cluster_committed,
        rounds,
        meta,
        sync,
        _dirs: dirs,
    }
}

/// [`staged_leader`] with one request a group, default segments, and every
/// round held until the test releases it.
fn held_rounds() -> Staged {
    staged_leader(one_request_groups(), SegmentConfig::default(), 0)
}

/// Send one quorum produce from its own thread; the answer arrives on the
/// returned channel.
fn send(
    leader: &Arc<LocalBroker>,
    producer: u128,
    sequence: u64,
) -> std::sync::mpsc::Receiver<WireFrame> {
    let (answered, answer) = std::sync::mpsc::channel();
    let leader = Arc::clone(leader);
    thread::spawn(move || {
        let frame = produce_frame(range_identity(), Uuid::from_u128(producer), sequence, 1);
        let _ = answered.send(leader.handle(Role::Producer, frame));
    });
    answer
}

fn answer_of(answer: &std::sync::mpsc::Receiver<WireFrame>, what: &str) -> WireFrame {
    answer
        .recv_timeout(Duration::from_secs(10))
        .unwrap_or_else(|_| panic!("{what} was not answered within 10 s"))
}

/// Nothing has come back on `answer` after a pause long enough for an
/// answer that was owed to arrive.
fn still_unanswered(answer: &std::sync::mpsc::Receiver<WireFrame>, what: &str) {
    thread::sleep(Duration::from_millis(100));
    if let Ok(frame) = answer.try_recv() {
        panic!("{what} was answered too early: {frame:?}");
    }
}

/// Wait until the leader's log holds `next` records, all fsynced. Before the
/// overlap the second group's append waited for the first group's whole
/// flush, its replica round included, so with that round held this fails.
fn await_appended(leader: &LocalBroker, next: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let offsets = leader.local_offsets();
        if offsets == (next, next) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the leader's log held (committed, next) = {offsets:?} after 10 s, expected \
             ({next}, {next}): the next group's local append did not run while the previous \
             group's replica round was held"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

fn error_code(frame: &WireFrame) -> ErrorCode {
    match &frame.message {
        Message::Error(ErrorResponse { code, .. }) => *code,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// E3 (#752): group 2's local append and fsync run while group 1's replica
/// round is held, and group 2 then waits for its turn: its own round is not
/// called, and neither member is answered, until group 1 has finished.
///
/// Red before the change: the coordinator handed the lead on only after a
/// group was answered, so group 2's append waited for group 1's round, and
/// `await_appended` timed out with the log at (1, 1).
#[test]
fn the_next_group_starts_its_local_append_while_the_previous_replicates() {
    let staged = held_rounds();
    let first = send(&staged.leader, 0xB1, 0);
    staged.rounds.await_called(1);
    let second = send(&staged.leader, 0xB2, 0);
    await_appended(&staged.leader, 2);
    still_unanswered(&first, "group 1, its round held,");
    still_unanswered(&second, "group 2, behind group 1's round,");
    assert_eq!(
        staged.rounds.calls().len(),
        1,
        "group 2's round began before group 1's had finished"
    );
    let coordinator = staged.leader.group_commit().unwrap();
    assert_eq!(coordinator.groups_in_flight(), 2);
    assert_eq!(
        coordinator.metrics().overlapped_total(),
        1,
        "group 2 was sealed while group 1 replicated"
    );

    staged.rounds.release(usize::MAX);
    let first = expect_ok(answer_of(&first, "group 1"));
    let second = expect_ok(answer_of(&second, "group 2"));
    assert_eq!(first.outcomes[0].offset, 0);
    assert_eq!(second.outcomes[0].offset, 1);
    assert_eq!(staged.cluster_committed.get(), 2);
    assert_eq!(
        staged.rounds.calls(),
        [(0u64, 1u64), (1, 2)],
        "one round a group, in offset order, each covering its own records"
    );
    assert_eq!(coordinator.groups_in_flight(), 0);
}

/// E3 (#752), with two groups in flight: no member is answered before its
/// own group's fsync returns AND a quorum covering its offsets, and the
/// later group is never answered before the earlier group's round. Group 1
/// is held in its round; group 2's fsync is held; then each barrier is
/// released in turn, and only then is the member behind it answered.
///
/// Red before the change: group 2 never reached its fsync while group 1's
/// round was held, and `await_entered` failed with "the group never reached
/// its fsync".
#[test]
fn no_member_is_acknowledged_before_its_own_fsync_and_quorum_with_two_groups_in_flight() {
    let staged = held_rounds();
    let first = send(&staged.leader, 0xB1, 0);
    staged.rounds.await_called(1);
    staged.sync.armed.store(true, Ordering::SeqCst);
    let second = send(&staged.leader, 0xB2, 0);
    staged.sync.await_entered();
    still_unanswered(&first, "group 1, its round held,");
    still_unanswered(&second, "group 2, its fsync held,");

    // Group 2's records are durable on the leader; neither group has a
    // quorum yet.
    staged.sync.release();
    await_appended(&staged.leader, 2);
    still_unanswered(&first, "group 1, its round held,");
    still_unanswered(&second, "group 2, fsynced but with no quorum,");
    assert_eq!(staged.cluster_committed.get(), 0);

    // Group 1's quorum: group 1 is answered, and group 2's round begins.
    staged.rounds.release(1);
    let first = expect_ok(answer_of(&first, "group 1"));
    assert_eq!(
        (first.outcomes[0].offset, first.committed_next_offset),
        (0, 1)
    );
    staged.rounds.await_called(2);
    still_unanswered(&second, "group 2, its own round held,");
    assert_eq!(staged.cluster_committed.get(), 1);

    staged.rounds.release(usize::MAX);
    let second = expect_ok(answer_of(&second, "group 2"));
    assert_eq!(
        (second.outcomes[0].offset, second.committed_next_offset),
        (1, 2)
    );
    assert_eq!(staged.rounds.calls(), [(0u64, 1u64), (1, 2)]);
}

/// E3 (#752): under concurrent load (eight producers, four requests each),
/// every replica round starts where the one before it ended
/// (a follower receives the groups in the order its base-offset check
/// requires), every request is acknowledged once, and each follower ends
/// with the leader's whole log.
///
/// A guard, not red before the change: serial groups are ordered too. It
/// fails on an overlap that lets a later group's round start first.
#[test]
fn replication_rounds_run_in_offset_order() {
    const PRODUCERS: u128 = 8;
    const REQUESTS: u64 = 4;
    let staged = staged_leader(
        GroupCommitConfig {
            max_delay: Duration::ZERO,
            max_records: 4,
            max_bytes: 1024 * 1024,
            max_pending_requests: 64,
        },
        SegmentConfig::default(),
        usize::MAX,
    );
    let threads: Vec<_> = (0..PRODUCERS)
        .map(|producer| {
            let leader = Arc::clone(&staged.leader);
            thread::spawn(move || {
                for sequence in 0..REQUESTS {
                    let frame = produce_frame(
                        range_identity(),
                        Uuid::from_u128(0xB0 + producer),
                        sequence,
                        sequence + 1,
                    );
                    expect_ok(leader.handle(Role::Producer, frame));
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let total = PRODUCERS as u64 * REQUESTS;
    let calls = staged.rounds.calls();
    let mut expected_base = 0;
    for (base, covers) in &calls {
        assert_eq!(
            *base, expected_base,
            "a round started at {base} where the previous ended at {expected_base}: {calls:?}"
        );
        assert!(covers > base, "a round covers its own records: {calls:?}");
        expected_base = *covers;
    }
    assert_eq!(expected_base, total, "{calls:?}");
    assert_eq!(staged.cluster_committed.get(), total);
    for follower in staged.rounds.followers() {
        assert_eq!(follower.local_committed_offset(), total);
    }
}

/// E3 (#752): group 1's round misses its quorum while group 2 has already
/// appended. Nothing the quorum does not hold is acknowledged: group 1 is
/// answered `Overloaded` (retryable), and group 2's round, which the
/// followers refuse because they never received group 1, misses too. Both
/// producers retry with the same sequence and each retry is a duplicate of
/// the record already in the leader's log, never a second append: the log
/// stays at two records, and the retries commit them.
///
/// Red before the change: group 2's append waited for group 1's round, so
/// `await_appended` timed out.
#[test]
fn a_quorum_miss_on_group_one_while_group_two_appends_acknowledges_nothing_and_retries_dedupe() {
    let staged = held_rounds();
    staged.rounds.miss(0);
    let first = send(&staged.leader, 0xB1, 0);
    staged.rounds.await_called(1);
    let second = send(&staged.leader, 0xB2, 0);
    await_appended(&staged.leader, 2);
    staged.rounds.release(usize::MAX);
    assert_eq!(
        error_code(&answer_of(&first, "group 1")),
        ErrorCode::Overloaded
    );
    assert_eq!(
        error_code(&answer_of(&second, "group 2")),
        ErrorCode::Overloaded
    );
    assert_eq!(staged.cluster_committed.get(), 0, "nothing was committed");
    for follower in staged.rounds.followers() {
        assert_eq!(follower.local_committed_offset(), 0);
    }

    // The retries, one at a time. B1's goes to the followers (they now hold
    // offset 0) but cannot cover the leader's two records; B2's then
    // completes the prefix and commits both; B1's next retry is answered
    // from the committed mark.
    let retry = |producer: u128| {
        staged.leader.handle(
            Role::Producer,
            produce_frame(range_identity(), Uuid::from_u128(producer), 0, 9),
        )
    };
    assert_eq!(error_code(&retry(0xB1)), ErrorCode::Overloaded);
    assert_eq!(staged.leader.local_offsets(), (2, 2), "a retry appended");
    let b2 = expect_ok(retry(0xB2));
    assert!(b2.outcomes[0].duplicate, "{b2:?}");
    assert_eq!(b2.outcomes[0].offset, 1);
    let b1 = expect_ok(retry(0xB1));
    assert!(b1.outcomes[0].duplicate, "{b1:?}");
    assert_eq!(b1.outcomes[0].offset, 0);
    assert_eq!(staged.leader.local_offsets(), (2, 2), "a retry appended");
    assert_eq!(staged.cluster_committed.get(), 2);
}

/// E3 (#752), D5 with two groups in flight: the lease is taken from the
/// leader after both groups' local stages. The followers, which have not
/// seen the new grant, ack both rounds; the lease re-check at each group's
/// commit fences every member of both, and nothing is published as
/// committed.
///
/// Red before the change: group 2's append waited for group 1's round, so
/// `await_appended` timed out.
#[test]
fn a_lease_lost_after_both_local_stages_fences_every_member_of_both_groups() {
    let staged = held_rounds();
    let first = send(&staged.leader, 0xB1, 0);
    staged.rounds.await_called(1);
    let second = send(&staged.leader, 0xB2, 0);
    await_appended(&staged.leader, 2);
    staged.meta.set(FENCING_EPOCH + 1);
    staged.rounds.release(usize::MAX);
    assert_eq!(error_code(&answer_of(&first, "group 1")), ErrorCode::Fenced);
    assert_eq!(
        error_code(&answer_of(&second, "group 2")),
        ErrorCode::Fenced
    );
    assert_eq!(
        staged.rounds.calls().len(),
        2,
        "both rounds ran: the fence is the commit's re-check, not a missing quorum"
    );
    assert_eq!(
        staged.cluster_committed.get(),
        0,
        "nothing is published as committed"
    );
}

/// E3 (#752), scenario 05b's rule with two groups in flight: group 2's
/// fsync fails while group 1 replicates. Group 2 fails closed at once with a
/// storage error and nothing of it stays in the log; the storage layer
/// poisons the tail before advancing its offsets, and the next append
/// rebuilds the tail from disk (#881), cutting the failed write. So group 3
/// is appended where group 2 would have been, right after group 1, never
/// past a hole, and group 2's producer, retrying, is appended fresh rather
/// than deduplicated against records that do not exist.
///
/// Red before the change: group 2 waited behind group 1's held round and was
/// not answered within 10 s.
#[test]
fn a_failed_fsync_in_the_next_group_fails_it_closed_and_the_group_after_is_not_appended_past_it() {
    let staged = held_rounds();
    let first = send(&staged.leader, 0xB1, 0);
    staged.rounds.await_called(1);
    staged.sync.fail_next.store(true, Ordering::SeqCst);
    let second = send(&staged.leader, 0xB2, 0);
    assert_eq!(
        error_code(&answer_of(&second, "group 2, whose fsync failed,")),
        ErrorCode::Storage
    );
    assert_eq!(
        staged.leader.local_offsets(),
        (1, 1),
        "nothing of the failed group is in the log"
    );
    let third = send(&staged.leader, 0xB3, 0);
    await_appended(&staged.leader, 2);
    staged.rounds.release(usize::MAX);
    let first = expect_ok(answer_of(&first, "group 1"));
    let third = expect_ok(answer_of(&third, "group 3"));
    assert_eq!(first.outcomes[0].offset, 0);
    assert_eq!(
        third.outcomes[0].offset, 1,
        "group 3 took the offset the failed group did not keep"
    );
    let retried = expect_ok(staged.leader.handle(
        Role::Producer,
        produce_frame(range_identity(), Uuid::from_u128(0xB2), 0, 9),
    ));
    assert!(
        !retried.outcomes[0].duplicate && retried.outcomes[0].offset == 2,
        "the failed group's retry is a fresh append after group 3: {retried:?}"
    );
    assert_eq!(staged.cluster_committed.get(), 3);
    assert_eq!(staged.rounds.calls(), [(0u64, 1u64), (1, 2), (2, 3)]);
    for follower in staged.rounds.followers() {
        assert_eq!(follower.local_committed_offset(), 3);
    }
}

/// E3 (#752): a group the log refuses whole and re-runs request by request
/// (D6) does not overlap. Group 1 is held in its round; the two requests of
/// group 2 are each too large to share a group, so group 2 is split, and its
/// re-run waits until group 1 has finished before it appends anything.
///
/// A guard, not red before the change (serial groups never overlap): it
/// fails on an overlap that lets the re-run append, and then replicate,
/// beside an earlier group's round.
#[test]
fn a_split_group_is_re_run_only_after_the_earlier_group_has_finished() {
    let staged = staged_leader(
        GroupCommitConfig {
            max_delay: Duration::ZERO,
            max_records: 2,
            max_bytes: 1024 * 1024,
            max_pending_requests: 32,
        },
        SegmentConfig {
            max_record_bytes: 1024,
            max_group_bytes: 1024 + 128,
            ..SegmentConfig::default()
        },
        0,
    );
    let large = |producer: u128| WireFrame {
        request_id: 1,
        stream_id: 1,
        message: Message::ProduceRequest(ProduceRequest {
            range: range_identity(),
            fencing_epoch: FENCING_EPOCH,
            producer_id: Uuid::from_u128(producer),
            producer_epoch: 1,
            first_sequence: 0,
            durability: WireDurability::Quorum,
            records: vec![ProduceRecord {
                timestamp_millis: 1_000,
                key: b"k".to_vec(),
                value: vec![b'v'; 700],
            }],
        }),
    };
    let coordinator = Arc::clone(staged.leader.group_commit().unwrap());
    // Group 1's fsync is held, so the next two requests queue behind it and
    // are sealed together.
    staged.sync.armed.store(true, Ordering::SeqCst);
    let first = send(&staged.leader, 0xB1, 0);
    staged.sync.await_entered();
    let pair: Vec<_> = [0xB2u128, 0xB3]
        .into_iter()
        .map(|producer| {
            let leader = Arc::clone(&staged.leader);
            let frame = large(producer);
            thread::spawn(move || leader.handle(Role::Producer, frame))
        })
        .collect();
    await_until("the pair to be queued behind group 1", || {
        coordinator.queued_requests() == 2
    });
    staged.sync.release();
    staged.rounds.await_called(1);
    // The pair is now one group, refused whole and waiting to re-run.
    await_until("the pair to be taken as one group", || {
        coordinator.queued_requests() == 0 && coordinator.groups_in_flight() == 2
    });
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        staged.leader.local_offsets(),
        (1, 1),
        "the split group's re-run appended while group 1 was replicating"
    );
    assert_eq!(staged.rounds.calls().len(), 1);

    staged.rounds.release(usize::MAX);
    expect_ok(answer_of(&first, "group 1"));
    for request in pair {
        expect_ok(request.join().unwrap());
    }
    assert_eq!(coordinator.metrics().split_groups_total(), 1);
    assert_eq!(staged.rounds.calls(), [(0u64, 1u64), (1, 2), (2, 3)]);
    assert_eq!(staged.cluster_committed.get(), 3);
}

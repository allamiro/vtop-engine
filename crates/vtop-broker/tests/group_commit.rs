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

fn replicated_leader(
    config: GroupCommitConfig,
    wrap: impl FnOnce(Arc<InProcessReplicaSet>, MetaFencingEpoch) -> Arc<dyn ReplicaSet>,
) -> (Arc<LocalBroker>, ClusterCommittedOffset, Vec<TempDir>) {
    let range = range_identity();
    let meta = MetaFencingEpoch::new(FENCING_EPOCH);
    let cluster_committed = ClusterCommittedOffset::new(0);
    let leader_dir = tempfile::tempdir().unwrap();
    let leader_segment = open_segment(&leader_dir, 0xD1, &range);
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
    entered: Mutex<bool>,
    entered_cv: std::sync::Condvar,
    released: Mutex<bool>,
    released_cv: std::sync::Condvar,
}

impl SyncGate {
    fn pass(&self) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        *self.entered.lock().unwrap() = true;
        self.entered_cv.notify_all();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.released_cv.wait(released).unwrap();
        }
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
        self.gate.pass();
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
        self.gate.pass();
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

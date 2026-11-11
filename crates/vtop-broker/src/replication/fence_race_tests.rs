//! A fence against a high-water mark that lands while it runs.

use super::*;
use crate::fencing_epochs::{EpochStart, FencingEpochJournal};
use std::time::Duration;
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
use vtop_protocol::ProduceRecord;

const OLD_EPOCH: u64 = 18;
const NEW_EPOCH: u64 = 19;
const NEWER_EPOCH: u64 = 20;

fn range() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

fn append(epoch: u64, offset: u64) -> ReplicaAppendRequest {
    ReplicaAppendRequest {
        range: range(),
        fencing_epoch: epoch,
        leader_node_id: Uuid::from_u128(0xA1),
        expected_base_offset: offset,
        producer_id: Uuid::from_u128(0xB1),
        producer_epoch: 1,
        first_sequence: offset,
        records: vec![ProduceRecord {
            timestamp_millis: 1_000,
            key: b"k".to_vec(),
            value: format!("v{offset}").into_bytes(),
        }],
    }
}

/// A follower over a fresh directory, with a fencing-epoch journal.
fn follower(dir: &tempfile::TempDir, meta: &MetaFencingEpoch) -> Arc<InProcessFollower> {
    let segment = ActiveSegment::create(
        dir.path().join("range.active"),
        SegmentDescriptor {
            segment_id: Uuid::from_u128(0xE1),
            topic: range().topic,
            topic_epoch: 1,
            lineage: RangeLineage {
                range_id: range().range_id,
                generation: 0,
                key_range: KeyRange::full(),
                parents: Vec::new(),
            },
            base_offset: 0,
        },
        SegmentConfig::default(),
    )
    .unwrap();
    let node = Arc::new(
        InProcessFollower::new(
            Uuid::from_u128(0xA2),
            segment,
            ProducerEpochJournal::open(dir.path().join("epochs")).unwrap(),
            range(),
            OLD_EPOCH,
            meta.clone(),
            ClusterCommittedOffset::new(0),
        )
        .unwrap(),
    );
    node.set_fencing_epoch_journal(
        FencingEpochJournal::open(dir.path().join("fencing-epochs")).unwrap(),
    );
    node
}

/// A race left by #616 (#649 review, cubic 4532895325). A fence read this
/// replica's high-water mark and then kept a dispute from it, without the
/// meta lock that `observe_hwm` holds from its settle through its advance.
/// A mark landing in between saw no dispute to settle, advanced past the
/// dispute's start once it was installed, and left it below the mark. The
/// next settle's truncation was then refused as below the acknowledged mark,
/// and the replica refused every append from the new leader.
///
/// The shape is #616's below-the-mark path: epoch-18 records to 5, then a
/// catch-up 5..11 this replica filed under the epoch 19 it adopted at 5, a
/// mark of 9, and an epoch-20 caller whose own tail is 10. The fence keeps
/// 10 in dispute. The hook lands the leader's mark of 11 exactly between the
/// fence's mark read and that install.
///
/// No timer decides the interleaving (#660 review). The hook starts the mark
/// on another thread. If this fence does not hold the meta lock there (the
/// bug), the hook waits until the mark has passed its settle, which saw no
/// dispute; its advance then waits for the install's state lock and lands
/// after it. If the fence holds the meta lock (the fix), the mark cannot
/// even start its settle until the fence is done, and the hook returns at
/// once.
///
/// Invariant: no kept dispute starts below the mark, and the leader's next
/// record lands.
#[test]
fn a_mark_landing_while_a_fence_keeps_a_dispute_never_leaves_it_below_the_mark() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaFencingEpoch::new(OLD_EPOCH);
    let node = follower(&dir, &meta);
    for offset in 0..5 {
        node.apply_append(&append(OLD_EPOCH, offset)).unwrap();
    }
    meta.set(NEW_EPOCH);
    node.adopt_fencing_epoch(NEW_EPOCH);
    for offset in 5..11 {
        node.apply_append(&append(NEW_EPOCH, offset)).unwrap();
    }
    node.cluster_committed().advance_to(9);
    meta.set(NEWER_EPOCH);

    let (settled, past_settle) = std::sync::mpsc::channel::<()>();
    let settled = Mutex::new(settled);
    *node.after_mark_settle.lock().unwrap() = Some(Arc::new(move || {
        let _ = settled.lock().unwrap().send(());
    }));
    let past_settle = Mutex::new(past_settle);

    type Marking = std::thread::JoinHandle<Result<(), (ErrorCode, String)>>;
    let marking: Arc<Mutex<Option<Marking>>> = Arc::new(Mutex::new(None));
    {
        let leader_mark = Arc::clone(&node);
        let marking = Arc::clone(&marking);
        let meta = meta.clone();
        *node.before_dispute_install.lock().unwrap() = Some(Arc::new(move || {
            let leader_mark = Arc::clone(&leader_mark);
            *marking.lock().unwrap() = Some(std::thread::spawn(move || {
                leader_mark.observe_hwm(&CommittedHwmUpdate {
                    range: range(),
                    fencing_epoch: NEWER_EPOCH,
                    committed_high_watermark: 11,
                })
            }));
            // A free meta lock means the mark can run its settle now: wait
            // until it has. A held one (by this fence) means it cannot.
            if meta.try_snapshot().is_some() {
                past_settle
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(30))
                    .expect("the leader's mark never passed its settle");
            }
        }));
    }
    node.fence(
        NEWER_EPOCH,
        &[
            EpochStart {
                epoch: OLD_EPOCH,
                start_offset: 0,
            },
            EpochStart {
                epoch: NEWER_EPOCH,
                start_offset: 10,
            },
        ],
    )
    .expect("fenced");
    // Drop the hooks, and the replica they hold, before judging.
    *node.before_dispute_install.lock().unwrap() = None;
    *node.after_mark_settle.lock().unwrap() = None;
    let marked = marking
        .lock()
        .unwrap()
        .take()
        .expect("the fence kept a dispute, so the hook ran")
        .join()
        .unwrap();
    marked.expect("the leader's mark is accepted");

    let (_, reported) = node.replication_status();
    let mark = node.cluster_committed().get();
    assert!(
        reported == node.next_offset() || reported >= mark,
        "a dispute from {reported} was left below the mark {mark} (next offset {})",
        node.next_offset()
    );
    let next = node.next_offset();
    node.apply_append(&append(NEWER_EPOCH, next))
        .expect("the leader's next record must land, not be refused below the mark");
}

/// #660 review (cubic P3). A test hook ran while its slot's lock was held,
/// so a hook that cleared or replaced its own slot deadlocked. It is now
/// cloned out of the slot first.
#[test]
fn a_test_hook_may_clear_its_own_slot() {
    let slot: Arc<Mutex<Option<TestHook>>> = Arc::new(Mutex::new(None));
    let own = Arc::clone(&slot);
    *slot.lock().unwrap() = Some(Arc::new(move || {
        *own.lock().unwrap() = None;
    }));
    let (done, finished) = std::sync::mpsc::channel();
    let running = Arc::clone(&slot);
    std::thread::spawn(move || {
        InProcessFollower::run_test_hook(&running);
        let _ = done.send(());
    });
    finished
        .recv_timeout(Duration::from_secs(10))
        .expect("a hook that cleared its own slot deadlocked");
    assert!(slot.lock().unwrap().is_none());
}

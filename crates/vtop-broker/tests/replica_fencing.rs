//! Fencing a replica before it is read (#240).
//!
//! A promotion probe that merely reads a replica reads a moving target: the
//! deposed leader may still be appending to a follower while the new leader
//! measures it, so the boundary a quorum "proves" can be stale before it is
//! published. BookKeeper fences the ensemble before ledger recovery for exactly
//! this reason.
//!
//! These tests pin the things that make the fence worth having: it stops the
//! old leader, it refuses to be talked into an epoch metadata never granted,
//! and it never moves backwards.
//!
//! They pass an EMPTY caller history throughout, except where reconciliation is
//! the subject. Empty means "the caller cannot vouch for its own lineage", under
//! which a replica truncates nothing — so these stay tests of fencing alone,
//! with reconciliation held out as a separate variable.

use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;
use vtop_broker::fencing_epochs::{EpochStart, FencingEpochJournal};
use vtop_broker::replication::{ClusterCommittedOffset, InProcessFollower};
use vtop_broker::{MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
use vtop_protocol::{ErrorCode, ProduceRecord, RangeIdentity, ReplicaAppendRequest};

const FOLLOWER: Uuid = Uuid::from_u128(0xA2);
const OLD_LEADER: Uuid = Uuid::from_u128(0xA1);
const PRODUCER: Uuid = Uuid::from_u128(0xB1);
const OLD_EPOCH: u64 = 18;
const NEW_EPOCH: u64 = 19;

fn range_identity() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

struct Harness {
    _dir: TempDir,
    range: RangeIdentity,
    node: Arc<InProcessFollower>,
    meta: MetaFencingEpoch,
}

fn follower() -> Harness {
    let range = range_identity();
    let dir = tempfile::tempdir().unwrap();
    let descriptor = SegmentDescriptor {
        segment_id: Uuid::from_u128(0xE1),
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
    let segment = ActiveSegment::create(
        dir.path().join("range.active"),
        descriptor,
        SegmentConfig::default(),
    )
    .unwrap();
    let meta = MetaFencingEpoch::new(OLD_EPOCH);
    let node = Arc::new(
        InProcessFollower::new(
            FOLLOWER,
            segment,
            ProducerEpochJournal::open(dir.path().join("epochs")).unwrap(),
            range.clone(),
            OLD_EPOCH,
            meta.clone(),
            ClusterCommittedOffset::new(0),
        )
        .unwrap(),
    );
    node.set_fencing_epoch_journal(
        FencingEpochJournal::open(dir.path().join("fencing-epochs")).unwrap(),
    );
    Harness {
        _dir: dir,
        range,
        node,
        meta,
    }
}

fn append_at(h: &Harness, epoch: u64, leader: Uuid, offset: u64) -> Result<(), ErrorCode> {
    let request = ReplicaAppendRequest {
        range: h.range.clone(),
        fencing_epoch: epoch,
        leader_node_id: leader,
        expected_base_offset: offset,
        producer_id: PRODUCER,
        producer_epoch: 1,
        first_sequence: offset,
        records: vec![ProduceRecord {
            timestamp_millis: 1_000,
            key: b"k".to_vec(),
            value: format!("v{offset}").into_bytes(),
        }],
    };
    h.node.apply_append(&request).map(|_| ()).map_err(|e| e.0)
}

/// The point of the whole thing: after fencing, the deposed leader's appends
/// are refused. Its offset therefore cannot move while the new leader reads it.
#[test]
fn fencing_stops_the_deposed_leader_from_writing() {
    let h = follower();
    append_at(&h, OLD_EPOCH, OLD_LEADER, 0).expect("the old leader may write before the fence");

    // Metadata grants the new epoch, and the new leader fences.
    h.meta.set(NEW_EPOCH);
    let fenced = h.node.fence(NEW_EPOCH, &[]).expect("fence should succeed");
    assert_eq!(fenced.fencing_epoch, NEW_EPOCH);
    assert_eq!(fenced.next_offset, 1);

    // The old leader is now shut out.
    assert_eq!(
        append_at(&h, OLD_EPOCH, OLD_LEADER, 1),
        Err(ErrorCode::Fenced),
        "after fencing, a write under the previous epoch must be refused"
    );

    // And the measurement still describes the log: nothing moved.
    let again = h.node.fence(NEW_EPOCH, &[]).unwrap();
    assert_eq!(
        again.next_offset, fenced.next_offset,
        "a fenced replica must not move under the new leader's feet"
    );
}

/// A replica must not fence itself on a caller's word alone.
///
/// The claim is not evidence. A replica that adopted a bare claim could be
/// fenced to any epoch by anything that reaches its port, and would then refuse
/// every append until metadata reached a number that may never arrive — a
/// permanent outage caused by one compromised or buggy peer.
#[test]
fn a_fence_above_the_granted_epoch_is_refused() {
    let h = follower();
    // Metadata has granted nothing beyond OLD_EPOCH.
    let refused = h
        .node
        .fence(9_999, &[])
        .expect_err("must not adopt an epoch metadata never granted");
    assert_eq!(refused.0, ErrorCode::Fenced);
    assert!(
        refused.1.contains("has not observed a grant"),
        "the refusal should say why, so an operator can tell it from a stale \
         leader: {}",
        refused.1
    );

    // And the replica is untouched — still serving its real epoch.
    append_at(&h, OLD_EPOCH, OLD_LEADER, 0)
        .expect("a refused fence must not have disturbed the replica");
}

/// The same request succeeds once this replica has seen the grant. This is the
/// retry a legitimately promoted leader makes while the follower's lease
/// watcher catches up, and it must not need anything else to change.
#[test]
fn a_fence_succeeds_once_the_grant_is_visible() {
    let h = follower();
    assert!(h.node.fence(NEW_EPOCH, &[]).is_err());

    h.meta.set(NEW_EPOCH);
    let fenced = h.node.fence(NEW_EPOCH, &[]).expect("now it is granted");
    assert_eq!(fenced.fencing_epoch, NEW_EPOCH);
}

/// Fencing only ever moves forward: a stale leader must not un-fence a replica
/// it has already lost.
#[test]
fn a_fence_below_the_held_epoch_is_refused() {
    let h = follower();
    h.meta.set(NEW_EPOCH);
    h.node.fence(NEW_EPOCH, &[]).unwrap();

    let refused = h
        .node
        .fence(OLD_EPOCH, &[])
        .expect_err("a stale leader must not roll the fence back");
    assert_eq!(refused.0, ErrorCode::Fenced);
    assert_eq!(
        h.node.held_fencing_epoch(),
        NEW_EPOCH,
        "the refused request must not have lowered anything"
    );
}

/// Re-fencing at the epoch already held is a success, not a refusal: a leader
/// retrying after a lost reply must not be told it has been deposed.
#[test]
fn re_fencing_at_the_held_epoch_succeeds() {
    let h = follower();
    h.meta.set(NEW_EPOCH);
    h.node.fence(NEW_EPOCH, &[]).unwrap();

    let again = h
        .node
        .fence(NEW_EPOCH, &[])
        .expect("a retry at the same epoch is idempotent, not a rollback");
    assert_eq!(again.fencing_epoch, NEW_EPOCH);
}

/// The fence reports the epoch history alongside the offsets, taken together.
///
/// They are what a truncation target is computed from, and reading them in two
/// calls would describe two different moments of a log that may have moved in
/// between.
#[test]
fn the_fence_reports_offsets_and_history_from_one_instant() {
    let h = follower();
    append_at(&h, OLD_EPOCH, OLD_LEADER, 0).unwrap();
    append_at(&h, OLD_EPOCH, OLD_LEADER, 1).unwrap();

    h.meta.set(NEW_EPOCH);
    let fenced = h.node.fence(NEW_EPOCH, &[]).unwrap();

    assert_eq!(fenced.next_offset, 2);
    assert_eq!(
        fenced.epoch_starts.last().map(|entry| entry.epoch),
        Some(NEW_EPOCH),
        "adopting the epoch during the fence must record where it begins"
    );
    assert_eq!(
        fenced.epoch_starts.last().map(|entry| entry.start_offset),
        Some(2),
        "the new epoch begins at the tail as it stood when the fence landed"
    );
}

/// THE POINT OF THE WHOLE ARC (#261). A replica that took records from a leader
/// deposed before those records reached a quorum discards them during the
/// fence, so it is already in agreement by the time the new leader reads it.
///
/// Before this, that replica would ACK the new leader's writes as duplicates —
/// answering "I have something at offset 5" when the question was "do you have
/// THAT record at offset 5?" — and be counted toward a quorum for bytes it did
/// not hold. Reconciling while stopped means the catch-up path is never reached
/// in a diverged state and never has to answer a question it cannot.
#[test]
fn a_diverged_replica_discards_the_deposed_leaders_records_while_fenced() {
    let h = follower();
    for offset in 0..10 {
        append_at(&h, OLD_EPOCH, OLD_LEADER, offset).unwrap();
    }
    // Only the first five reached a quorum.
    h.node.cluster_committed().advance_to(5);

    // The new leader shares epoch 18 from 0, but its own epoch 19 began at 5 —
    // it never saw the five records this replica took after the quorum stopped
    // following.
    let leader_history = [
        EpochStart {
            epoch: OLD_EPOCH,
            start_offset: 0,
        },
        EpochStart {
            epoch: NEW_EPOCH,
            start_offset: 5,
        },
    ];

    h.meta.set(NEW_EPOCH);
    let fenced = h.node.fence(NEW_EPOCH, &leader_history).unwrap();

    assert_eq!(fenced.truncated_records, 5, "the disputed tail is gone");
    assert_eq!(
        fenced.next_offset, 5,
        "and the offset the caller counts is the reconciled one, not the \
         pre-truncation one"
    );

    // The new leader's write at 5 now lands as a genuine append rather than
    // being swallowed as a duplicate.
    assert!(append_at(&h, NEW_EPOCH, Uuid::from_u128(0xA9), 5).is_ok());
    assert_eq!(h.node.next_offset(), 6);
}

/// A caller that cannot vouch for its own history truncates nothing.
///
/// Deleting records to satisfy a claim nobody made is the failure mode this
/// whole mechanism exists to avoid, so "unknown" must mean "leave it alone"
/// here exactly as it does everywhere else.
#[test]
fn an_unknown_caller_history_truncates_nothing() {
    let h = follower();
    for offset in 0..10 {
        append_at(&h, OLD_EPOCH, OLD_LEADER, offset).unwrap();
    }

    h.meta.set(NEW_EPOCH);
    let fenced = h.node.fence(NEW_EPOCH, &[]).unwrap();

    assert_eq!(fenced.truncated_records, 0);
    assert_eq!(fenced.next_offset, 10, "the log is untouched");
}

/// A replica that agrees with the caller keeps everything.
///
/// REGRESSION. The first version of this wiped the log: the replica adopts the
/// new epoch during the fence, so its vector gains an entry the caller's
/// shorter vector does not have, and the comparison then reported the start of
/// the last common epoch — offset 0 — as though it were a divergence point.
/// Four records both replicas held were discarded to "reconcile" two logs that
/// did not disagree about anything.
#[test]
fn an_agreeing_replica_is_not_truncated() {
    let h = follower();
    for offset in 0..4 {
        append_at(&h, OLD_EPOCH, OLD_LEADER, offset).unwrap();
    }

    h.meta.set(NEW_EPOCH);
    let fenced = h
        .node
        .fence(
            NEW_EPOCH,
            &[EpochStart {
                epoch: OLD_EPOCH,
                start_offset: 0,
            }],
        )
        .unwrap();

    assert_eq!(fenced.truncated_records, 0);
    assert_eq!(fenced.next_offset, 4);
}

/// A reconciliation that would cross the acknowledged high-water mark FAILS the
/// fence rather than silently doing nothing.
///
/// Those records were acknowledged to a producer. A caller asking for them is
/// either wrong or reconciling against a log that is not this range's, and
/// either way promotion must not go on believing this replica agreed. Failing
/// the fence takes it out of the quorum, which is the honest outcome.
#[test]
fn a_reconciliation_below_the_acknowledged_mark_fails_the_fence() {
    let h = follower();
    for offset in 0..10 {
        append_at(&h, OLD_EPOCH, OLD_LEADER, offset).unwrap();
    }
    // Everything through 8 was acknowledged.
    h.node.cluster_committed().advance_to(8);

    // A caller claiming its own epoch 19 began at 2 puts divergence below that.
    let leader_history = [
        EpochStart {
            epoch: OLD_EPOCH,
            start_offset: 0,
        },
        EpochStart {
            epoch: NEW_EPOCH,
            start_offset: 2,
        },
    ];

    h.meta.set(NEW_EPOCH);
    let refused = h
        .node
        .fence(NEW_EPOCH, &leader_history)
        .expect_err("must not discard acknowledged records");
    assert!(
        refused.1.contains("acknowledged"),
        "the refusal should name what it is protecting: {}",
        refused.1
    );
    assert_eq!(
        h.node.next_offset(),
        10,
        "and nothing may have been discarded on the way to refusing"
    );
}

/// The marker is an empty append: acking it asserts "I hold everything below
/// this offset, durably, under the epoch you sent". Nothing is written.
#[test]
fn a_new_epoch_marker_acks_when_the_replica_holds_the_boundary() {
    let h = follower();
    for offset in 0..5 {
        append_at(&h, OLD_EPOCH, OLD_LEADER, offset).unwrap();
    }

    let marker = ReplicaAppendRequest {
        range: h.range.clone(),
        fencing_epoch: OLD_EPOCH,
        leader_node_id: OLD_LEADER,
        expected_base_offset: 5,
        producer_id: Uuid::nil(),
        producer_epoch: 0,
        first_sequence: 0,
        records: Vec::new(),
    };
    let ack = h
        .node
        .apply_append(&marker)
        .expect("the replica holds 0..5");
    assert_eq!(ack.local_committed_offset, 5);
    assert_eq!(
        h.node.next_offset(),
        5,
        "a marker proves a fact; it must not add a record"
    );
}

/// A replica that does NOT hold the boundary refuses the marker, which is what
/// keeps the quorum honest — the count would otherwise include a replica that
/// cannot vouch for the records below it.
#[test]
fn a_new_epoch_marker_is_refused_by_a_replica_short_of_the_boundary() {
    let h = follower();
    for offset in 0..3 {
        append_at(&h, OLD_EPOCH, OLD_LEADER, offset).unwrap();
    }

    let marker = ReplicaAppendRequest {
        range: h.range.clone(),
        fencing_epoch: OLD_EPOCH,
        leader_node_id: OLD_LEADER,
        expected_base_offset: 5,
        producer_id: Uuid::nil(),
        producer_epoch: 0,
        first_sequence: 0,
        records: Vec::new(),
    };
    assert!(
        h.node.apply_append(&marker).is_err(),
        "a replica two records short of the boundary must not vouch for it"
    );
}

/// The marker is fenced like any other append: a stale leader cannot use it to
/// prove a quorum for an epoch it no longer holds.
#[test]
fn a_new_epoch_marker_from_a_stale_epoch_is_refused() {
    let h = follower();
    append_at(&h, OLD_EPOCH, OLD_LEADER, 0).unwrap();
    h.meta.set(NEW_EPOCH);
    h.node.fence(NEW_EPOCH, &[]).unwrap();

    let marker = ReplicaAppendRequest {
        range: h.range.clone(),
        fencing_epoch: OLD_EPOCH,
        leader_node_id: OLD_LEADER,
        expected_base_offset: 1,
        producer_id: Uuid::nil(),
        producer_epoch: 0,
        first_sequence: 0,
        records: Vec::new(),
    };
    assert_eq!(
        h.node.apply_append(&marker).map(|_| ()).map_err(|e| e.0),
        Err(ErrorCode::Fenced)
    );
}

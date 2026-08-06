//! Fencing a replica before it is read (#240).
//!
//! A promotion probe that merely reads a replica reads a moving target: the
//! deposed leader may still be appending to a follower while the new leader
//! measures it, so the boundary a quorum "proves" can be stale before it is
//! published. BookKeeper fences the ensemble before ledger recovery for exactly
//! this reason.
//!
//! These tests pin the three things that make the fence worth having: it stops
//! the old leader, it refuses to be talked into an epoch metadata never granted,
//! and it never moves backwards.

use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;
use vtop_broker::fencing_epochs::FencingEpochJournal;
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
    let fenced = h.node.fence(NEW_EPOCH).expect("fence should succeed");
    assert_eq!(fenced.fencing_epoch, NEW_EPOCH);
    assert_eq!(fenced.next_offset, 1);

    // The old leader is now shut out.
    assert_eq!(
        append_at(&h, OLD_EPOCH, OLD_LEADER, 1),
        Err(ErrorCode::Fenced),
        "after fencing, a write under the previous epoch must be refused"
    );

    // And the measurement still describes the log: nothing moved.
    let again = h.node.fence(NEW_EPOCH).unwrap();
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
        .fence(9_999)
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
    assert!(h.node.fence(NEW_EPOCH).is_err());

    h.meta.set(NEW_EPOCH);
    let fenced = h.node.fence(NEW_EPOCH).expect("now it is granted");
    assert_eq!(fenced.fencing_epoch, NEW_EPOCH);
}

/// Fencing only ever moves forward: a stale leader must not un-fence a replica
/// it has already lost.
#[test]
fn a_fence_below_the_held_epoch_is_refused() {
    let h = follower();
    h.meta.set(NEW_EPOCH);
    h.node.fence(NEW_EPOCH).unwrap();

    let refused = h
        .node
        .fence(OLD_EPOCH)
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
    h.node.fence(NEW_EPOCH).unwrap();

    let again = h
        .node
        .fence(NEW_EPOCH)
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
    let fenced = h.node.fence(NEW_EPOCH).unwrap();

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

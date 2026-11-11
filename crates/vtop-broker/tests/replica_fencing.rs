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
use vtop_broker::committed_floor::CommittedFloorFile;
use vtop_broker::fencing_epochs::{EpochStart, FencingEpochJournal};
use vtop_broker::replication::{ClusterCommittedOffset, InProcessFollower};
use vtop_broker::{MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::{
    ActiveSegment, Durability, KeyRange, LogRecord, RangeLineage, RetentionPolicy, SegmentConfig,
    SegmentDescriptor, SegmentSet,
};
use vtop_protocol::{
    CommittedHwmUpdate, ErrorCode, ProduceRecord, RangeIdentity, ReplicaAppendRequest,
};

const FOLLOWER: Uuid = Uuid::from_u128(0xA2);
const OLD_LEADER: Uuid = Uuid::from_u128(0xA1);
const PRODUCER: Uuid = Uuid::from_u128(0xB1);
const OLD_EPOCH: u64 = 18;
const NEW_EPOCH: u64 = 19;
const NEWER_EPOCH: u64 = 20;
const NEWEST_EPOCH: u64 = 21;

fn range_identity() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

fn descriptor_for(range: &RangeIdentity, segment_id: u128) -> SegmentDescriptor {
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
    let segment = ActiveSegment::create(
        dir.path().join("range.active"),
        descriptor_for(&range, 0xE1),
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

fn append_request(
    range: &RangeIdentity,
    epoch: u64,
    leader: Uuid,
    offset: u64,
) -> ReplicaAppendRequest {
    ReplicaAppendRequest {
        range: range.clone(),
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
    }
}

fn append_at(h: &Harness, epoch: u64, leader: Uuid, offset: u64) -> Result<(), ErrorCode> {
    h.node
        .apply_append(&append_request(&h.range, epoch, leader, offset))
        .map(|_| ())
        .map_err(|e| e.0)
}

/// Rebuild a follower over `dir` exactly as `data_node` does after a
/// restart: journals reopened from disk, the cluster-committed cell seeded
/// from the committed-floor file, and the file handed back for further
/// persists. This is the injection point the restart tests below exercise —
/// nothing in memory survives into what this returns.
fn follower_via_the_real_open_path(
    dir: &TempDir,
    range: &RangeIdentity,
    meta: &MetaFencingEpoch,
    segment: impl Into<SegmentSet>,
) -> Arc<InProcessFollower> {
    let committed_floor = CommittedFloorFile::open(dir.path().join("committed-floor"));
    let node = Arc::new(
        InProcessFollower::new(
            FOLLOWER,
            segment,
            ProducerEpochJournal::open(dir.path().join("epochs")).unwrap(),
            range.clone(),
            OLD_EPOCH,
            meta.clone(),
            ClusterCommittedOffset::new(committed_floor.floor()),
        )
        .unwrap(),
    );
    node.set_fencing_epoch_journal(
        FencingEpochJournal::open(dir.path().join("fencing-epochs")).unwrap(),
    );
    node.set_committed_floor(committed_floor);
    node
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

/// THE POINT OF THE WHOLE ARC (#261), and its bound (#611). A replica that
/// took records from a leader deposed before those records reached a quorum
/// discards them during the fence. It is then already in agreement by the
/// time the new leader reads it.
///
/// Before this, that replica would ACK the new leader's writes as duplicates.
/// It answered "I have something at offset 5" when the question was "do you
/// have THAT record at offset 5?", and was counted toward a quorum for bytes
/// it did not hold. Reconciling while stopped means the catch-up path is never
/// reached in a diverged state and never has to answer a question it cannot.
///
/// The discard needs proof that the records were never acknowledged (#611).
/// Here the caller holds records from offset 5 written under epoch 19. That
/// leadership was established after 18 and holds something else at 5, so
/// epoch 18's records from 5 never reached a quorum.
#[test]
fn a_diverged_replica_discards_records_a_newer_leadership_superseded_while_fenced() {
    let h = follower();
    for offset in 0..10 {
        append_at(&h, OLD_EPOCH, OLD_LEADER, offset).unwrap();
    }
    // Only the first five reached a quorum.
    h.node.cluster_committed().advance_to(5);

    // The caller shares epoch 18 from 0. Under epoch 19 it holds its own
    // records from 5 to 8, and it adopted 20, the epoch it fences at, at 8.
    let leader_history = [
        EpochStart {
            epoch: OLD_EPOCH,
            start_offset: 0,
        },
        EpochStart {
            epoch: NEW_EPOCH,
            start_offset: 5,
        },
        EpochStart {
            epoch: NEWER_EPOCH,
            start_offset: 8,
        },
    ];

    h.meta.set(NEWER_EPOCH);
    let fenced = h.node.fence(NEWER_EPOCH, &leader_history).unwrap();

    assert_eq!(fenced.truncated_records, 5, "the disputed tail is gone");
    assert_eq!(
        fenced.next_offset, 5,
        "and the offset the caller counts is the reconciled one, not the \
         pre-truncation one"
    );

    // The new leader's write at 5 now lands as a genuine append rather than
    // being swallowed as a duplicate.
    assert!(append_at(&h, NEWER_EPOCH, Uuid::from_u128(0xA9), 5).is_ok());
    assert_eq!(h.node.next_offset(), 6);
}

/// #611. The same replica, fenced by a caller that never received 5..10 and
/// adopted the new epoch at its own tail, 5. The divergence point is that
/// adoption, which owns no record, so nothing proves 5..10 unacknowledged.
/// In a three-replica range they were acknowledged: the deposed leader and
/// this replica are a quorum. The replica keeps them and answers 10, and
/// the vote restriction refuses the caller.
///
/// This used to discard them, since the high-water mark the replica knew was
/// only 5. A test of this name pinned that loss as the point of the arc.
///
/// If the caller is established anyway, by a majority that outvoted this
/// replica, the suffix is provably unacknowledged. Its first append over it
/// then discards the suffix before applying, instead of acking a record this
/// replica does not hold (#261).
#[test]
fn a_replica_ahead_of_the_caller_in_its_own_epoch_keeps_its_records_through_the_fence() {
    let h = follower();
    for offset in 0..10 {
        append_at(&h, OLD_EPOCH, OLD_LEADER, offset).unwrap();
    }
    // The leader learned that 5..10 committed; this replica never did.
    h.node.cluster_committed().advance_to(5);

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
    assert_eq!(
        fenced.truncated_records, 0,
        "records the caller merely lacks are not evidence against them"
    );
    assert_eq!(
        fenced.next_offset, 10,
        "the answer carries everything held, which is the vote against the caller"
    );

    // A retry of the same fence changes nothing.
    let again = h.node.fence(NEW_EPOCH, &leader_history).unwrap();
    assert_eq!((again.truncated_records, again.next_offset), (0, 10));

    // Outvoted: the epoch-19 leader appends its own record at 5. Acking it
    // as a duplicate would count this replica for a record it does not
    // hold; instead the kept suffix goes and the record lands.
    let ack = h
        .node
        .apply_append(&append_request(
            &h.range,
            NEW_EPOCH,
            Uuid::from_u128(0xA9),
            5,
        ))
        .expect("the established leader's append lands once the kept suffix is gone");
    assert_eq!(ack.local_committed_offset, 6);
    assert_eq!(
        h.node.next_offset(),
        6,
        "the append was applied, not acked over the old record at 5"
    );
}

/// #611 through the group-commit path: a kept suffix goes before a GROUP is
/// applied over it, exactly as before a single append. A group the fencing
/// check refuses (a stale epoch) discards nothing.
#[test]
fn a_kept_suffix_goes_before_a_group_append_over_it_and_not_for_a_stale_epoch() {
    let h = follower();
    for offset in 0..10 {
        append_at(&h, OLD_EPOCH, OLD_LEADER, offset).unwrap();
    }
    h.node.cluster_committed().advance_to(5);
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
    assert_eq!((fenced.truncated_records, fenced.next_offset), (0, 10));

    let stale = h.node.apply_append_batch(&[
        append_request(&h.range, OLD_EPOCH, OLD_LEADER, 5),
        append_request(&h.range, OLD_EPOCH, OLD_LEADER, 6),
    ]);
    assert_eq!(stale.map_err(|error| error.0), Err(ErrorCode::Fenced));
    assert_eq!(
        h.node.next_offset(),
        10,
        "a refused group must not discard the kept suffix"
    );

    let leader = Uuid::from_u128(0xA9);
    let ack = h
        .node
        .apply_append_batch(&[
            append_request(&h.range, NEW_EPOCH, leader, 5),
            append_request(&h.range, NEW_EPOCH, leader, 6),
        ])
        .expect("the established leader's group lands once the kept suffix is gone");
    assert_eq!(ack.local_committed_offset, 7);
    assert_eq!(h.node.next_offset(), 7);
}

/// Fence a harness follower holding ten epoch-18 records (high-water mark 5)
/// for a caller that never received 5..10: the #611 shape, which keeps them.
fn kept_extension() -> Harness {
    let h = follower();
    for offset in 0..10 {
        append_at(&h, OLD_EPOCH, OLD_LEADER, offset).unwrap();
    }
    h.node.cluster_committed().advance_to(5);
    h.meta.set(NEW_EPOCH);
    let fenced = h
        .node
        .fence(
            NEW_EPOCH,
            &[
                EpochStart {
                    epoch: OLD_EPOCH,
                    start_offset: 0,
                },
                EpochStart {
                    epoch: NEW_EPOCH,
                    start_offset: 5,
                },
            ],
        )
        .unwrap();
    assert_eq!((fenced.truncated_records, fenced.next_offset), (0, 10));
    h
}

/// #616 review (codex P1, cubic P1). An outvoted follower reconnects after
/// the new leader committed past the kept suffix's start, and the session
/// sends the current high-water mark BEFORE any append. That mark must
/// neither expose the old records as committed nor make the suffix
/// impossible to discard. The epoch-19 leader committing past 5 proves it
/// was established and holds its own record at 5, so the suffix is
/// discarded before the mark is applied, and the mark never exceeds what
/// this replica holds in agreement. The leader's append at 5 then lands.
#[test]
fn a_high_water_mark_past_a_kept_suffix_discards_it_before_it_is_applied() {
    let h = kept_extension();
    h.node
        .observe_hwm(&CommittedHwmUpdate {
            range: h.range.clone(),
            fencing_epoch: NEW_EPOCH,
            committed_high_watermark: 7,
        })
        .expect("the leader's mark on reconnect");
    assert!(
        h.node.cluster_committed().get() <= 5,
        "the mark exposed the kept epoch-18 records as committed: cluster \
         committed {} over a suffix disputed from 5",
        h.node.cluster_committed().get()
    );
    let ack = h
        .node
        .apply_append(&append_request(
            &h.range,
            NEW_EPOCH,
            Uuid::from_u128(0xA9),
            5,
        ))
        .expect("the follower must still be able to catch up after the mark");
    assert_eq!(ack.local_committed_offset, 6);
    assert_eq!(h.node.next_offset(), 6);
}

/// The same mark at or below the dispute's start proves nothing about the
/// suffix and discards nothing.
#[test]
fn a_high_water_mark_at_a_kept_suffix_start_discards_nothing() {
    let h = kept_extension();
    h.node
        .observe_hwm(&CommittedHwmUpdate {
            range: h.range.clone(),
            fencing_epoch: NEW_EPOCH,
            committed_high_watermark: 5,
        })
        .unwrap();
    assert_eq!(h.node.next_offset(), 10);
    assert_eq!(h.node.cluster_committed().get(), 5);
}

/// #616 review (cubic P1). A request whose batch end overflows is malformed
/// and refused; it must be refused BEFORE the kept suffix is discarded, not
/// after.
#[test]
fn an_overflowing_append_is_refused_without_discarding_a_kept_suffix() {
    let h = kept_extension();
    let mut request = append_request(&h.range, NEW_EPOCH, Uuid::from_u128(0xA9), 5);
    request.expected_base_offset = u64::MAX;
    let refused = h
        .node
        .apply_append(&request)
        .expect_err("an overflowing batch end is malformed");
    assert_eq!(refused.0, ErrorCode::InvalidRequest);
    assert_eq!(
        h.node.next_offset(),
        10,
        "the malformed request discarded the kept suffix before it was refused"
    );

    let refused = h
        .node
        .apply_append_batch(&[request.clone(), request])
        .expect_err("an overflowing group is malformed");
    assert_eq!(refused.0, ErrorCode::InvalidRequest);
    assert_eq!(h.node.next_offset(), 10);
}

/// #616 review (codex P1). A retry of a batch in the AGREED prefix does not
/// reach the kept dispute, so it settles nothing, and the duplicate path
/// acks it. That ack must not report durability at or past the dispute's
/// start. The leader counts an ack against its whole tail, not against the
/// retried batch, so an ack of 10 would certify the new leader's record at
/// 5, which this replica does not hold.
#[test]
fn a_retry_of_the_agreed_prefix_is_not_acked_past_a_kept_dispute() {
    let h = kept_extension();
    let leader = Uuid::from_u128(0xA9);
    let ack = h
        .node
        .apply_append(&append_request(&h.range, NEW_EPOCH, leader, 3))
        .expect("a duplicate of an agreed record is acked");
    assert!(
        ack.local_committed_offset <= 5,
        "a retry of offset 3 was acked through {} over records disputed from 5",
        ack.local_committed_offset
    );
    let ack = h
        .node
        .apply_append_batch(&[
            append_request(&h.range, NEW_EPOCH, leader, 2),
            append_request(&h.range, NEW_EPOCH, leader, 3),
        ])
        .expect("a duplicate group of agreed records is acked");
    assert!(
        ack.local_committed_offset <= 5,
        "a group retry was acked through {} over records disputed from 5",
        ack.local_committed_offset
    );
    assert_eq!(h.node.next_offset(), 10, "and nothing was discarded");
}

/// #616 review (cubic P2). A newer fence whose caller HOLDS the kept records
/// (it agrees with this replica through 10) resolves the old dispute, which
/// must stop capping what the replica reports. Left in place it made every
/// reconnect re-send records already held in agreement, and capped this
/// replica's acks below records it holds.
#[test]
fn a_newer_fence_that_agrees_with_the_kept_records_retires_the_old_dispute() {
    let h = kept_extension();
    assert_eq!(h.node.replication_status(), (5, 5), "precondition");
    h.meta.set(NEWER_EPOCH);
    let fenced = h
        .node
        .fence(
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
        .unwrap();
    assert_eq!((fenced.truncated_records, fenced.next_offset), (0, 10));
    assert_eq!(
        h.node.replication_status(),
        (10, 10),
        "a dispute the epoch-20 fence resolved still capped the status"
    );
}

/// The #640 shape on one replica: epoch-18 records to 5, then the epoch-19
/// leader's catch-up 5..11 (epoch-18 records and its own marker), which this
/// replica adopted 19 at offset 5 for and so files under 19. Its high-water
/// mark is `mark`. The caller fencing at 20 files 5..10 under 18, so the two
/// histories disagree from 5.
fn caught_up_under_its_own_epoch(mark: u64) -> Harness {
    let h = follower();
    for offset in 0..5 {
        append_at(&h, OLD_EPOCH, OLD_LEADER, offset).unwrap();
    }
    h.meta.set(NEW_EPOCH);
    h.node.adopt_fencing_epoch(NEW_EPOCH);
    for offset in 5..11 {
        append_at(&h, NEW_EPOCH, Uuid::from_u128(0xA9), offset).unwrap();
    }
    h.node.cluster_committed().advance_to(mark);
    h.meta.set(NEWER_EPOCH);
    h
}

fn fence_from_the_epoch_19_leader(h: &Harness) -> Result<u64, (ErrorCode, String)> {
    h.node
        .fence(
            NEWER_EPOCH,
            &[
                EpochStart {
                    epoch: OLD_EPOCH,
                    start_offset: 0,
                },
                EpochStart {
                    epoch: NEW_EPOCH,
                    start_offset: 10,
                },
                EpochStart {
                    epoch: NEWER_EPOCH,
                    start_offset: 11,
                },
            ],
        )
        .map(|fenced| fenced.truncated_records)
}

/// #616 regression, found by the promotion crash sweep (#640 shape). The
/// histories disagree from 5, below this replica's own high-water mark of
/// 11. Everything below the mark is committed, and a committed record is
/// never in dispute. #616 kept a dispute from 5 anyway. Every later append
/// tried to settle it, the truncation guard refused, and the replica refused
/// every append for good. Now nothing at or below the mark is kept, and
/// here the mark is the tail, so nothing is kept at all. The replica takes
/// the next leader's appends.
#[test]
fn a_fence_never_keeps_a_dispute_below_the_replicas_high_water_mark() {
    let h = caught_up_under_its_own_epoch(11);
    assert_eq!(
        fence_from_the_epoch_19_leader(&h),
        Ok(0),
        "nothing is discarded"
    );
    assert_eq!(h.node.next_offset(), 11);
    assert_eq!(
        h.node.replication_status(),
        (11, 11),
        "a dispute was kept at or below the high-water mark"
    );
    let ack = h
        .node
        .apply_append(&append_request(
            &h.range,
            NEWER_EPOCH,
            Uuid::from_u128(0xAA),
            11,
        ))
        .expect("the replica refused the next leader's append");
    assert_eq!(ack.local_committed_offset, 12);
}

/// The same shape with the mark at 9, so 9..11 are not known committed.
/// Only that stretch is kept in dispute, not the committed 5..9. It is the
/// stretch the crash sweep caught being acked over when the fence refused
/// instead (a refused fence still adopts the epoch and leaves the replica
/// unreconciled). The next leader's first append over it discards it,
/// which the guard can always do, because it starts at the mark.
#[test]
fn a_dispute_below_the_mark_is_kept_only_from_the_mark() {
    let h = caught_up_under_its_own_epoch(9);
    assert_eq!(fence_from_the_epoch_19_leader(&h), Ok(0));
    assert_eq!(
        h.node.replication_status(),
        (9, 9),
        "kept from the mark, not from 5"
    );
    let ack = h
        .node
        .apply_append(&append_request(
            &h.range,
            NEWER_EPOCH,
            Uuid::from_u128(0xAA),
            9,
        ))
        .expect("the next leader's append over the kept stretch lands");
    assert_eq!(ack.local_committed_offset, 10);
    assert_eq!(h.node.next_offset(), 10);
}

/// #616 regression, also found by the promotion crash sweep (its no-fault
/// run). R kept 5..10 in dispute against the epoch-19 candidate, which was
/// refused, and R then won epoch 20 itself. A candidate fencing ITSELF at a
/// newer epoch makes its own log the reference, so a dispute kept for an
/// older epoch is retired there exactly as a newer peer fence retires it.
/// Left in place, the dispute capped R's acknowledgement of its own appends
/// at 5, and the new leader's marker never committed.
#[test]
fn a_candidate_fencing_itself_at_a_newer_epoch_retires_its_old_dispute() {
    let h = kept_extension();
    h.meta.set(NEWER_EPOCH);
    assert_eq!(
        h.node
            .fence_locally(NEWER_EPOCH)
            .expect("R fences itself at 20"),
        10
    );
    assert_eq!(
        h.node.replication_status(),
        (10, 10),
        "an epoch-19 dispute outlived R's own fence at 20"
    );
    let ack = h
        .node
        .apply_append(&append_request(
            &h.range,
            NEWER_EPOCH,
            Uuid::from_u128(0xA2),
            10,
        ))
        .expect("the new leader's own marker lands");
    assert_eq!(
        ack.local_committed_offset, 11,
        "the new leader's own append was acked only through {}",
        ack.local_committed_offset
    );
}

/// #611, the other way a fence can be told to discard records that may be
/// acknowledged: the replica's disputed records come from a leadership NEWER
/// than the caller's at the divergence point. Here the caller holds epoch-19
/// records from 5, but this replica's records from 5 were written under
/// epoch 20, a leadership the caller never followed. Nothing shows those were
/// unacknowledged, so the replica keeps them.
#[test]
fn a_replica_holding_a_newer_leaderships_records_keeps_them_through_the_fence() {
    let h = follower();
    for offset in 0..5 {
        append_at(&h, OLD_EPOCH, OLD_LEADER, offset).unwrap();
    }
    h.meta.set(NEWER_EPOCH);
    h.node.adopt_fencing_epoch(NEWER_EPOCH);
    for offset in 5..7 {
        append_at(&h, NEWER_EPOCH, Uuid::from_u128(0xA7), offset).unwrap();
    }
    h.node.cluster_committed().advance_to(5);

    let leader_history = [
        EpochStart {
            epoch: OLD_EPOCH,
            start_offset: 0,
        },
        EpochStart {
            epoch: NEW_EPOCH,
            start_offset: 5,
        },
        EpochStart {
            epoch: NEWEST_EPOCH,
            start_offset: 9,
        },
    ];
    h.meta.set(NEWEST_EPOCH);
    let fenced = h.node.fence(NEWEST_EPOCH, &leader_history).unwrap();
    assert_eq!(fenced.truncated_records, 0);
    assert_eq!(fenced.next_offset, 7);
}

/// A divergence verdict BELOW the retained base is unprovable, not fatal
/// (#290). The epoch entries that produced it describe records retention
/// reclaimed, and the truncation it would mandate is below the acknowledged
/// floor by construction — so failing the fence over it would exclude a
/// valid replica from every promotion for a dispute about data it no longer
/// holds. The honest verdict is the journal-less one: touch nothing.
#[test]
fn a_divergence_below_the_retained_base_does_not_fail_the_fence() {
    let range = range_identity();
    let dir = tempfile::tempdir().unwrap();
    let descriptor = descriptor_for(&range, 0xE7);
    let config = SegmentConfig {
        max_record_bytes: 256,
        max_group_bytes: 512,
        max_segment_bytes: 512,
        max_segment_records: 100,
        index_stride: 2,
    };
    let mut set =
        SegmentSet::create_in(&vtop_log::env::Env::real(), dir.path(), descriptor, config).unwrap();
    for sequence in 0..40_u64 {
        set.append_group(
            &[LogRecord {
                producer_id: PRODUCER,
                producer_epoch: 0,
                sequence,
                timestamp_millis: 1_000,
                attributes: 0,
                key: b"k".to_vec(),
                value: format!("v{sequence:04}").into_bytes(),
            }],
            Durability::Fsync,
            Uuid::from_u128(50_000 + sequence as u128),
        )
        .unwrap();
    }
    let next = set.next_offset();
    set.retain(
        &RetentionPolicy {
            max_total_bytes: 600,
        },
        next,
    )
    .unwrap();
    let base = set.base_offset();
    assert!(base > 2, "the front must have moved for this test to bite");

    let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
    let meta = MetaFencingEpoch::new(OLD_EPOCH);
    let node = Arc::new(
        InProcessFollower::new(
            FOLLOWER,
            set,
            epochs,
            range,
            OLD_EPOCH,
            meta.clone(),
            ClusterCommittedOffset::new(next),
        )
        .unwrap(),
    );
    // Histories that differ ONLY below the retained base: same epochs, with
    // the two sides recording the later epoch's start two records apart — a
    // legitimate skew of adoption timing whose records are now reclaimed on
    // this side.
    let mut journal = FencingEpochJournal::open(dir.path().join("fencing-epochs")).unwrap();
    journal.record(OLD_EPOCH, 0).unwrap();
    journal.record(NEW_EPOCH, base - 2).unwrap();
    node.set_fencing_epoch_journal(journal);

    meta.set(NEW_EPOCH + 1);
    let fenced = node
        .fence(
            NEW_EPOCH + 1,
            &[
                EpochStart {
                    epoch: OLD_EPOCH,
                    start_offset: 0,
                },
                EpochStart {
                    epoch: NEW_EPOCH,
                    start_offset: base - 1,
                },
            ],
        )
        .expect("a dispute about reclaimed records must not fail the fence");
    assert_eq!(
        fenced.truncated_records, 0,
        "nothing this replica holds is provably in dispute"
    );
    assert_eq!(fenced.next_offset, next, "nothing held was discarded");
}

/// "Began below the retained base" is not "confined below it" (#408): a
/// divergence verdict whose disagreement extends into records this replica
/// still RETAINS must fail the fence, not be waved through as unprovable.
/// The histories here disagree about who wrote the retained stretch itself —
/// this side says the newer epoch took over before the base, the caller says
/// the older epoch kept writing well past it — and silently admitting that
/// would hand back the split-brain read the epoch vector exists to prevent.
#[test]
fn a_dispute_that_reaches_retained_records_fails_the_fence() {
    let range = range_identity();
    let dir = tempfile::tempdir().unwrap();
    let descriptor = descriptor_for(&range, 0xE8);
    let config = SegmentConfig {
        max_record_bytes: 256,
        max_group_bytes: 512,
        max_segment_bytes: 512,
        max_segment_records: 100,
        index_stride: 2,
    };
    let mut set =
        SegmentSet::create_in(&vtop_log::env::Env::real(), dir.path(), descriptor, config).unwrap();
    for sequence in 0..40_u64 {
        set.append_group(
            &[LogRecord {
                producer_id: PRODUCER,
                producer_epoch: 0,
                sequence,
                timestamp_millis: 1_000,
                attributes: 0,
                key: b"k".to_vec(),
                value: format!("v{sequence:04}").into_bytes(),
            }],
            Durability::Fsync,
            Uuid::from_u128(60_000 + sequence as u128),
        )
        .unwrap();
    }
    let next = set.next_offset();
    set.retain(
        &RetentionPolicy {
            max_total_bytes: 600,
        },
        next,
    )
    .unwrap();
    let base = set.base_offset();
    assert!(base > 2, "the front must have moved for this test to bite");

    let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
    let meta = MetaFencingEpoch::new(OLD_EPOCH);
    let node = Arc::new(
        InProcessFollower::new(
            FOLLOWER,
            set,
            epochs,
            range,
            OLD_EPOCH,
            meta.clone(),
            ClusterCommittedOffset::new(next),
        )
        .unwrap(),
    );
    // This replica: the newer epoch began just below the base, so it owns
    // every retained record. The caller: the OLDER epoch kept writing
    // through base+10. compare_lineage puts the divergence below the base —
    // the old blanket shortcut's territory — but the retained records
    // themselves are attributed to different leaderships.
    let mut journal = FencingEpochJournal::open(dir.path().join("fencing-epochs")).unwrap();
    journal.record(OLD_EPOCH, 0).unwrap();
    journal.record(NEW_EPOCH, base - 2).unwrap();
    node.set_fencing_epoch_journal(journal);

    meta.set(NEW_EPOCH + 1);
    let refused = node
        .fence(
            NEW_EPOCH + 1,
            &[
                EpochStart {
                    epoch: OLD_EPOCH,
                    start_offset: 0,
                },
                EpochStart {
                    epoch: NEW_EPOCH,
                    start_offset: base + 10,
                },
            ],
        )
        .expect_err("retained records attributed to different leaderships must fail the fence");
    assert!(
        refused.1.contains("still retains"),
        "the refusal must say the dispute reaches retained records, so the operator reads \
         'repair', not 'storage fault': {}",
        refused.1
    );
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

/// A replica holding records with NO history — the shape `vtopctl node
/// repair` left behind before the transfer carried the lineage, and the shape
/// any replica is in after losing its journal file — must not be truncated by
/// a fence carrying a real history (#315).
///
/// REGRESSION. The fence adopts the new epoch before reconciling, which
/// manufactured such a replica's FIRST journal entry at its current tail; a
/// lone `(epoch, tail)` entry zipped against the caller's real history
/// compared as divergence at offset zero, and the whole log — a completed
/// repair — was discarded. The honest answer for records-without-history is
/// "unknown", the same verdict `set_fencing_epoch_journal` deliberately
/// reports for this state, and unknown truncates nothing.
#[test]
fn a_replica_with_records_but_no_history_is_not_truncated() {
    let h = follower();
    for offset in 0..10 {
        append_at(&h, OLD_EPOCH, OLD_LEADER, offset).unwrap();
    }
    // Restart with an empty journal over a log that has records.
    let journal_dir = tempfile::tempdir().unwrap();
    h.node.set_fencing_epoch_journal(
        FencingEpochJournal::open(journal_dir.path().join("fencing-epochs")).unwrap(),
    );
    assert!(
        h.node.epoch_starts().is_empty(),
        "precondition: records without history"
    );

    let real_history = [
        EpochStart {
            epoch: OLD_EPOCH,
            start_offset: 0,
        },
        EpochStart {
            epoch: NEW_EPOCH,
            start_offset: 10,
        },
    ];
    h.meta.set(NEW_EPOCH);
    let fenced = h.node.fence(NEW_EPOCH, &real_history).unwrap();

    assert_eq!(
        fenced.truncated_records, 0,
        "a claim this replica cannot check must not delete what it holds"
    );
    assert_eq!(fenced.next_offset, 10, "the records survive the fence");
    // The unknown-ness is DURABLE, not a one-fence grace: the adoption must
    // not have fabricated a first entry, so the caller sees an empty vector
    // (the documented "unknown" signal) rather than a lone (epoch, tail)
    // entry it could compute divergence-at-zero from.
    assert!(
        fenced.epoch_starts.is_empty(),
        "records without history must REPORT unknown, got {:?}",
        fenced.epoch_starts
    );

    // And a second transition reaches the same verdict — the first fence's
    // adoption must not have armed the truncation it skipped.
    h.meta.set(NEW_EPOCH + 1);
    let fenced_again = h.node.fence(NEW_EPOCH + 1, &real_history).unwrap();
    assert_eq!(
        fenced_again.truncated_records, 0,
        "unknown must persist across fences until a real history is installed"
    );
    assert_eq!(fenced_again.next_offset, 10);
    assert!(fenced_again.epoch_starts.is_empty());
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

    // A caller holding its own epoch-19 records from 2 (fencing at 20) puts
    // divergence below that, and claims to supersede them (#611: a caller
    // that merely LACKS them would not be obeyed at all).
    let leader_history = [
        EpochStart {
            epoch: OLD_EPOCH,
            start_offset: 0,
        },
        EpochStart {
            epoch: NEW_EPOCH,
            start_offset: 2,
        },
        EpochStart {
            epoch: NEWER_EPOCH,
            start_offset: 4,
        },
    ];

    h.meta.set(NEWER_EPOCH);
    let refused = h
        .node
        .fence(NEWER_EPOCH, &leader_history)
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

/// THE POINT OF #240's FLOOR. The truncation-below-acknowledged guard used
/// to compare against a cell rebuilt at ZERO on every restart, role flip,
/// and rebuild — vacuous precisely across the window in which a new
/// leader's fence-and-reconcile arrives, because `observe_hwm` cannot
/// re-arm the cell until the lease watcher adopts the current epoch. With
/// the floor persisted at the commit barrier and read back at open, the
/// same fence is REFUSED.
#[test]
fn a_restarted_follower_still_refuses_to_discard_what_it_acknowledged() {
    let range = range_identity();
    let dir = tempfile::tempdir().unwrap();

    // First life: ten records under epoch 18, eight acknowledged by the
    // cluster through the real HWM-frame path, floor persisted by the
    // orderly shutdown.
    {
        let segment = ActiveSegment::create(
            dir.path().join("range.active"),
            descriptor_for(&range, 0xE1),
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
        node.set_committed_floor(CommittedFloorFile::open(dir.path().join("committed-floor")));
        for offset in 0..10 {
            node.apply_append(&append_request(&range, OLD_EPOCH, OLD_LEADER, offset))
                .unwrap();
        }
        node.observe_hwm(&CommittedHwmUpdate {
            range: range.clone(),
            fencing_epoch: OLD_EPOCH,
            committed_high_watermark: 8,
        })
        .expect("the leader's HWM frame");
        node.quiesce().expect("orderly shutdown persists the floor");
    }

    // Second life over the SAME directory. Nothing in memory survives; the
    // guard is whatever the open path can recover.
    let meta = MetaFencingEpoch::new(OLD_EPOCH);
    let node = follower_via_the_real_open_path(
        &dir,
        &range,
        &meta,
        ActiveSegment::recover(dir.path().join("range.active")).unwrap(),
    );
    assert_eq!(
        node.cluster_committed().get(),
        8,
        "precondition: the floor survived the restart and seeded the cell"
    );

    // A new leader whose epoch-19 records from 2 put divergence there — beneath the eight
    // acknowledged records.
    let leader_history = [
        EpochStart {
            epoch: OLD_EPOCH,
            start_offset: 0,
        },
        EpochStart {
            epoch: NEW_EPOCH,
            start_offset: 2,
        },
        EpochStart {
            epoch: NEWER_EPOCH,
            start_offset: 4,
        },
    ];
    meta.set(NEWER_EPOCH);
    let refused = node.fence(NEWER_EPOCH, &leader_history).expect_err(
        "before the floor was persisted, this exact sequence rebuilt the guard at zero and \
         the fence SILENTLY TRUNCATED eight acknowledged records; it must refuse instead",
    );
    assert!(
        refused.1.contains("acknowledged"),
        "the refusal must name what it protects: {}",
        refused.1
    );
    assert_eq!(
        node.next_offset(),
        10,
        "and nothing may have been discarded on the way to refusing"
    );
}

/// The floor is protection, not a prerequisite. A directory with no
/// committed-floor file — every directory written before the file existed,
/// and every freshly repaired replica, since repair deliberately seeds no
/// floor (nothing on the wire carries the source's cluster HWM safely: a
/// source's `local_committed_offset` may legitimately EXCEED the quorum HWM
/// and must never become a floor) — reconciles exactly as before. Pins the
/// backward compatibility and the residual gap in one place.
#[test]
fn a_replica_without_a_floor_file_reconciles_exactly_as_before() {
    let range = range_identity();
    let dir = tempfile::tempdir().unwrap();

    // An older node's first life: the same appends and acknowledgement, but
    // no floor file was ever written.
    {
        let segment = ActiveSegment::create(
            dir.path().join("range.active"),
            descriptor_for(&range, 0xE1),
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
        for offset in 0..10 {
            node.apply_append(&append_request(&range, OLD_EPOCH, OLD_LEADER, offset))
                .unwrap();
        }
        node.observe_hwm(&CommittedHwmUpdate {
            range: range.clone(),
            fencing_epoch: OLD_EPOCH,
            committed_high_watermark: 8,
        })
        .unwrap();
        node.quiesce().unwrap();
    }

    let meta = MetaFencingEpoch::new(OLD_EPOCH);
    let node = follower_via_the_real_open_path(
        &dir,
        &range,
        &meta,
        ActiveSegment::recover(dir.path().join("range.active")).unwrap(),
    );
    assert_eq!(
        node.cluster_committed().get(),
        0,
        "no file, no floor — the pre-floor state, not an error"
    );

    let leader_history = [
        EpochStart {
            epoch: OLD_EPOCH,
            start_offset: 0,
        },
        EpochStart {
            epoch: NEW_EPOCH,
            start_offset: 2,
        },
        EpochStart {
            epoch: NEWER_EPOCH,
            start_offset: 4,
        },
    ];
    meta.set(NEWER_EPOCH);
    let fenced = node
        .fence(NEWER_EPOCH, &leader_history)
        .expect("an absent floor must not refuse anything the old behaviour allowed");
    assert_eq!(
        fenced.truncated_records, 8,
        "without a floor the divergent fence truncates through the acknowledged mark — the \
         documented pre-floor behaviour, and the residual for a freshly repaired replica"
    );
    assert_eq!(fenced.next_offset, 2);
}

/// `run_retention`'s floor input is `min(cluster_committed, local)`; at
/// zero it protects EVERY sealed segment from reclaim, so a restarted
/// follower used to hold its whole disk until the first HWM frame arrived.
/// The persisted floor makes reclaim live again from the first post-restart
/// append.
#[test]
fn a_reopened_followers_retention_floor_starts_at_the_persisted_value_not_zero() {
    let range = range_identity();
    let dir = tempfile::tempdir().unwrap();
    let config = SegmentConfig {
        max_record_bytes: 256,
        max_group_bytes: 512,
        max_segment_bytes: 512,
        max_segment_records: 100,
        index_stride: 2,
    };

    // First life: a rolled range, everything acknowledged, floor persisted.
    // Retention is OFF here so the sealed prefix survives intact into the
    // restarts below.
    {
        let set = SegmentSet::create_in(
            &vtop_log::env::Env::real(),
            dir.path(),
            descriptor_for(&range, 0xE8),
            config,
        )
        .unwrap();
        let meta = MetaFencingEpoch::new(OLD_EPOCH);
        let node = Arc::new(
            InProcessFollower::new(
                FOLLOWER,
                set,
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
        node.set_committed_floor(CommittedFloorFile::open(dir.path().join("committed-floor")));
        for offset in 0..40 {
            node.apply_append(&append_request(&range, OLD_EPOCH, OLD_LEADER, offset))
                .unwrap();
        }
        node.observe_hwm(&CommittedHwmUpdate {
            range: range.clone(),
            fencing_epoch: OLD_EPOCH,
            committed_high_watermark: 40,
        })
        .unwrap();
        node.quiesce().unwrap();
    }

    // Restarted WITHOUT the floor — the pre-floor shape: the cell sits at
    // zero until the first HWM frame, and the append's retention pass
    // reclaims nothing however far the policy is exceeded.
    {
        let set = SegmentSet::open_in(&vtop_log::env::Env::real(), dir.path())
            .unwrap()
            .expect("the range exists");
        assert_eq!(set.base_offset(), 0, "precondition: nothing reclaimed yet");
        let meta = MetaFencingEpoch::new(OLD_EPOCH);
        let node = Arc::new(
            InProcessFollower::new(
                FOLLOWER,
                set,
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
        node.set_retention(Some(RetentionPolicy {
            max_total_bytes: 600,
        }));
        node.apply_append(&append_request(&range, OLD_EPOCH, OLD_LEADER, 40))
            .unwrap();
        node.quiesce().unwrap();
    }
    {
        let set = SegmentSet::open_in(&vtop_log::env::Env::real(), dir.path())
            .unwrap()
            .expect("the range exists");
        assert_eq!(
            set.base_offset(),
            0,
            "a floor of zero blocks every reclaim — the starvation the persisted floor \
             exists to end"
        );
    }

    // Restarted through the real open path: the recovered floor is 40, and
    // the very next append's retention pass reclaims the acknowledged front.
    {
        let set = SegmentSet::open_in(&vtop_log::env::Env::real(), dir.path())
            .unwrap()
            .expect("the range exists");
        let meta = MetaFencingEpoch::new(OLD_EPOCH);
        let node = follower_via_the_real_open_path(&dir, &range, &meta, set);
        assert_eq!(
            node.cluster_committed().get(),
            40,
            "precondition: the floor survived the restart"
        );
        node.set_retention(Some(RetentionPolicy {
            max_total_bytes: 600,
        }));
        node.apply_append(&append_request(&range, OLD_EPOCH, OLD_LEADER, 41))
            .unwrap();
        node.quiesce().unwrap();
    }
    let set = SegmentSet::open_in(&vtop_log::env::Env::real(), dir.path())
        .unwrap()
        .expect("the range exists");
    assert!(
        set.base_offset() > 0,
        "reclaim below the recovered floor must proceed: a reopened follower's retention \
         floor is the persisted value, not zero"
    );
}

/// The cadence rule, both halves. The FIRST observed HWM arms the guard
/// immediately — an unarmed floor plus a crash before the next append
/// barrier would recover nothing, the exact window the file exists to
/// close. Every LATER frame only sharpens an armed guard and waits for the
/// commit barrier, where an fsync already exists: `observe_hwm` runs in
/// the per-connection dispatch loop that also carries append frames and
/// must stay I/O-free in the steady state. `quiesce` covers the quiet tail.
#[test]
fn the_first_hwm_arms_the_floor_and_later_frames_wait_for_the_barrier() {
    let range = range_identity();
    let dir = tempfile::tempdir().unwrap();
    let floor_path = dir.path().join("committed-floor");
    let segment = ActiveSegment::create(
        dir.path().join("range.active"),
        descriptor_for(&range, 0xE9),
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
    node.set_committed_floor(CommittedFloorFile::open(&floor_path));
    let batch = |base: u64| {
        [
            append_request(&range, OLD_EPOCH, OLD_LEADER, base),
            append_request(&range, OLD_EPOCH, OLD_LEADER, base + 1),
        ]
    };

    node.apply_append_batch(&batch(0)).expect("first batch");
    assert_eq!(
        CommittedFloorFile::open(&floor_path).floor(),
        0,
        "no HWM has been observed yet; a barrier with nothing to protect saves nothing"
    );
    node.observe_hwm(&CommittedHwmUpdate {
        range: range.clone(),
        fencing_epoch: OLD_EPOCH,
        committed_high_watermark: 2,
    })
    .unwrap();
    assert_eq!(
        CommittedFloorFile::open(&floor_path).floor(),
        2,
        "the FIRST observed HWM must arm the floor immediately: unarmed plus a crash \
         before the next barrier would recover nothing, the exact window the file \
         exists to close"
    );

    node.apply_append_batch(&batch(2)).expect("second batch");
    node.observe_hwm(&CommittedHwmUpdate {
        range: range.clone(),
        fencing_epoch: OLD_EPOCH,
        committed_high_watermark: 4,
    })
    .unwrap();
    assert_eq!(
        CommittedFloorFile::open(&floor_path).floor(),
        2,
        "a LATER frame must not do floor I/O — the guard is already armed, and the \
         dispatch loop carrying the frame also carries append frames"
    );

    node.apply_append_batch(&batch(4)).expect("third batch");
    assert_eq!(
        CommittedFloorFile::open(&floor_path).floor(),
        4,
        "the commit barrier is where a sharpened floor becomes durable — one write per \
         batch, and only when it advanced"
    );

    node.observe_hwm(&CommittedHwmUpdate {
        range: range.clone(),
        fencing_epoch: OLD_EPOCH,
        committed_high_watermark: 6,
    })
    .unwrap();
    node.quiesce().expect("orderly shutdown");
    assert_eq!(
        CommittedFloorFile::open(&floor_path).floor(),
        6,
        "quiesce persists the quiet tail the last commit barrier could not see"
    );
}

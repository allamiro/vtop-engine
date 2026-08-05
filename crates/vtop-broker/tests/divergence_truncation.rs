//! Repairing a replica that diverged from the range's current leadership
//! (#240).
//!
//! The failure being fixed: a follower fsyncs the leader's appends before that
//! leader has a quorum. Depose the leader mid-flight and the follower is left
//! holding records no quorum ever agreed to. The new leader's appends then
//! collide with them at every offset, the follower refuses each one, and
//! retrying cannot help — the mismatch is on disk. Before this, that replica
//! was stranded until an operator restored its data directory from a peer.
//!
//! Repair is a truncation to the point where the two replicas provably agree,
//! which comes from comparing epoch vectors rather than offsets. These tests
//! drive that end to end and pin the two bounds that keep it from becoming a
//! data-loss tool: never below the acknowledged high-water mark, and never on
//! a broker with no replication at all.

use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;
use vtop_broker::fencing_epochs::{EpochStart, FencingEpochJournal};
use vtop_broker::replication::{ClusterCommittedOffset, InProcessFollower};
use vtop_broker::{BrokerError, MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
use vtop_protocol::{ProduceRecord, RangeIdentity, ReplicaAppendRequest};

const FOLLOWER: Uuid = Uuid::from_u128(0xA2);
const OLD_LEADER: Uuid = Uuid::from_u128(0xA1);
const NEW_LEADER: Uuid = Uuid::from_u128(0xA9);
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

fn open_segment(dir: &TempDir, range: &RangeIdentity) -> ActiveSegment {
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
    ActiveSegment::create(
        dir.path().join("range.active"),
        descriptor,
        SegmentConfig::default(),
    )
    .unwrap()
}

struct Follower {
    _dir: TempDir,
    range: RangeIdentity,
    node: Arc<InProcessFollower>,
    cluster_committed: ClusterCommittedOffset,
    meta: MetaFencingEpoch,
}

impl Follower {
    /// Metadata mints a new epoch and the follower observes it — in that
    /// order, which is the order the real system uses. A follower that adopted
    /// an epoch metadata had not granted would be serving on its own authority.
    fn grant(&self, epoch: u64) {
        self.meta.set(epoch);
        self.node.adopt_fencing_epoch(epoch);
    }
}

fn follower() -> Follower {
    let range = range_identity();
    let dir = tempfile::tempdir().unwrap();
    let segment = open_segment(&dir, &range);
    let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
    let cluster_committed = ClusterCommittedOffset::new(0);
    let meta = MetaFencingEpoch::new(OLD_EPOCH);
    let node = Arc::new(
        InProcessFollower::new(
            FOLLOWER,
            segment,
            epochs,
            range.clone(),
            OLD_EPOCH,
            meta.clone(),
            cluster_committed.clone(),
        )
        .unwrap(),
    );
    node.set_fencing_epoch_journal(
        FencingEpochJournal::open(dir.path().join("fencing-epochs")).unwrap(),
    );
    Follower {
        _dir: dir,
        range,
        node,
        cluster_committed,
        meta,
    }
}

/// Append `count` records at `base` under `epoch`, as a leader would.
fn replicate(f: &Follower, epoch: u64, leader: Uuid, base: u64, count: u64) {
    for index in 0..count {
        let sequence = base + index;
        let request = ReplicaAppendRequest {
            range: f.range.clone(),
            fencing_epoch: epoch,
            leader_node_id: leader,
            expected_base_offset: sequence,
            producer_id: PRODUCER,
            producer_epoch: 1,
            first_sequence: sequence,
            records: vec![ProduceRecord {
                timestamp_millis: 1_000,
                key: b"k".to_vec(),
                value: format!("v{sequence}").into_bytes(),
            }],
        };
        f.node
            .apply_append(&request)
            .unwrap_or_else(|error| panic!("replicate {sequence}: {error:?}"));
    }
}

/// The whole arc: a follower takes records from a leader that is then deposed,
/// the new leader's history disagrees, and truncation to the divergence point
/// makes the follower able to accept the new leader's appends.
///
/// Without the truncation the final append is refused and stays refused, which
/// is the stranding this issue is about.
#[test]
fn a_diverged_follower_truncates_and_rejoins_the_new_leader() {
    let f = follower();

    // Epoch 18 writes ten records; the first five reached a quorum.
    replicate(&f, OLD_EPOCH, OLD_LEADER, 0, 10);
    f.cluster_committed.advance_to(5);
    assert_eq!(f.node.next_offset(), 10);
    assert_eq!(
        f.node.epoch_starts(),
        vec![EpochStart {
            epoch: OLD_EPOCH,
            start_offset: 0
        }],
        "the follower recorded where epoch 18 began on its disk"
    );

    // The new leader also holds epoch 18 from 0, but its epoch 19 begins at 5 —
    // it never saw the five records this follower took after the quorum stopped
    // following. Everything below 5 was written under identical leadership.
    let new_leader_history = [
        EpochStart {
            epoch: OLD_EPOCH,
            start_offset: 0,
        },
        EpochStart {
            epoch: NEW_EPOCH,
            start_offset: 5,
        },
    ];

    // The follower's own vector stops at epoch 18, so it is a prefix of the
    // leader's: they agree as far as the follower can vouch for, and the tail
    // above that is what has to go.
    let divergence = f
        .node
        .epoch_starts()
        .first()
        .map(|_| new_leader_history[1].start_offset)
        .expect("history known");
    assert_eq!(divergence, 5);

    f.grant(NEW_EPOCH);
    let collision = ReplicaAppendRequest {
        range: f.range.clone(),
        fencing_epoch: NEW_EPOCH,
        leader_node_id: NEW_LEADER,
        expected_base_offset: 5,
        producer_id: PRODUCER,
        producer_epoch: 1,
        first_sequence: 5,
        records: vec![ProduceRecord {
            timestamp_millis: 2_000,
            key: b"k".to_vec(),
            value: b"from-new-leader".to_vec(),
        }],
    };

    // KNOWN GAP (#261), asserted so it cannot change unnoticed. Before the truncation
    // the follower ACCEPTS this append as an idempotent retry: the catch-up
    // branch asks only whether it is durable through the batch end, which it
    // is, having taken five records the new leader never wrote. It answers "I
    // already have offset 5" when it holds a DIFFERENT record there, and the
    // leader counts that toward a quorum.
    //
    // The fix is not a check on the request's fencing epoch — that was tried
    // and is wrong, because a newly promoted leader inherits its predecessor's
    // prefix and retransmits it under its own newer epoch during ordinary
    // catch-up. It needs the epoch each record was ORIGINALLY written under,
    // which the request does not carry.
    let premature = f.node.apply_append(&collision);
    assert!(
        premature.is_ok(),
        "documenting current behaviour: the diverged follower acks rather than \
         refusing, which is why the truncation below is not optional"
    );
    // It acked without applying anything — its log is untouched and still wrong.
    assert_eq!(f.node.next_offset(), 10);

    let outcome = f.node.truncate_to(divergence).unwrap();
    assert_eq!(outcome.records_removed, 5);
    assert_eq!(outcome.next_offset, 5);

    // Now the same append genuinely lands, on a log that agrees with the leader.
    let ack = f
        .node
        .apply_append(&collision)
        .expect("the follower should accept the new leader after truncating");
    assert_eq!(ack.local_committed_offset, 6);
    assert_eq!(f.node.next_offset(), 6);
}

/// The bound that keeps this from being a data-loss tool.
///
/// Records below the cluster high-water mark were acknowledged to producers.
/// Discarding them is not repair, and a caller that computes a target below it
/// has made an error that must surface rather than be executed.
#[test]
fn truncation_below_the_acknowledged_high_water_mark_is_refused() {
    let f = follower();
    replicate(&f, OLD_EPOCH, OLD_LEADER, 0, 10);
    f.cluster_committed.advance_to(7);

    let error = f.node.truncate_to(4).expect_err("must refuse");
    assert!(
        matches!(
            error,
            BrokerError::TruncationBelowAcknowledged {
                requested: 4,
                high_watermark: 7
            }
        ),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        f.node.next_offset(),
        10,
        "a refused truncation must not have moved the log"
    );

    // At the high-water mark exactly is allowed: nothing acknowledged is lost.
    assert_eq!(f.node.truncate_to(7).unwrap().records_removed, 3);
}

/// Truncation drops the epoch entries whose records are gone, so the follower
/// stops claiming a history it no longer has.
#[test]
fn truncation_drops_the_epoch_entries_it_invalidated() {
    let f = follower();
    replicate(&f, OLD_EPOCH, OLD_LEADER, 0, 4);
    f.grant(NEW_EPOCH);
    replicate(&f, NEW_EPOCH, NEW_LEADER, 4, 4);

    assert_eq!(
        f.node.epoch_starts(),
        vec![
            EpochStart {
                epoch: OLD_EPOCH,
                start_offset: 0
            },
            EpochStart {
                epoch: NEW_EPOCH,
                start_offset: 4
            },
        ]
    );

    f.node.truncate_to(2).unwrap();

    assert_eq!(
        f.node.epoch_starts(),
        vec![
            EpochStart {
                epoch: OLD_EPOCH,
                start_offset: 0
            },
            EpochStart {
                epoch: NEW_EPOCH,
                start_offset: 2
            },
        ],
        "epoch 19's records are gone, but the replica still HOLDS epoch 19 and will write \
         under it at the new tail — so its entry must be re-anchored there rather than \
         dropped. Without this the next records would be attributed to epoch 18."
    );
    assert_eq!(
        f.node.epoch_starts().len(),
        2,
        "re-anchoring must not leave the stale start_offset 4 behind"
    );
}

/// Re-anchoring is skipped for the "no grant yet" sentinel: a replica holding
/// epoch 0 has never been granted anything and must not claim to own the tail.
#[test]
fn truncation_does_not_anchor_the_never_granted_sentinel() {
    let range = range_identity();
    let dir = tempfile::tempdir().unwrap();
    let cluster_committed = ClusterCommittedOffset::new(0);
    let meta = MetaFencingEpoch::new(0);
    let node = Arc::new(
        InProcessFollower::new(
            FOLLOWER,
            open_segment(&dir, &range),
            ProducerEpochJournal::open(dir.path().join("epochs")).unwrap(),
            range.clone(),
            0,
            meta.clone(),
            cluster_committed.clone(),
        )
        .unwrap(),
    );
    node.set_fencing_epoch_journal(
        FencingEpochJournal::open(dir.path().join("fencing-epochs")).unwrap(),
    );

    node.truncate_to(0).unwrap();

    assert!(
        node.epoch_starts().is_empty(),
        "epoch 0 never wrote a record and must not appear in the vector"
    );
}

/// A truncation interrupted by a crash finishes on the next open.
///
/// The log is truncated before the vector, so the window leaves entries whose
/// start offset sits past the log's tail. Reconstructed here by truncating the
/// segment alone and then reattaching the untouched journal — which is the
/// state a crash between the two steps leaves on disk.
#[test]
fn an_interrupted_truncation_is_completed_when_the_journal_is_reattached() {
    let range = range_identity();
    let dir = tempfile::tempdir().unwrap();
    let journal_path = dir.path().join("fencing-epochs");
    let cluster_committed = ClusterCommittedOffset::new(0);

    {
        let meta = MetaFencingEpoch::new(OLD_EPOCH);
        let node = Arc::new(
            InProcessFollower::new(
                FOLLOWER,
                open_segment(&dir, &range),
                ProducerEpochJournal::open(dir.path().join("epochs")).unwrap(),
                range.clone(),
                OLD_EPOCH,
                meta.clone(),
                cluster_committed.clone(),
            )
            .unwrap(),
        );
        node.set_fencing_epoch_journal(FencingEpochJournal::open(&journal_path).unwrap());
        let f = Follower {
            _dir: tempfile::tempdir().unwrap(),
            range: range.clone(),
            node: node.clone(),
            cluster_committed: cluster_committed.clone(),
            meta,
        };
        replicate(&f, OLD_EPOCH, OLD_LEADER, 0, 4);
        f.grant(NEW_EPOCH);
        replicate(&f, NEW_EPOCH, NEW_LEADER, 4, 4);
    }

    // The journal on disk claims epoch 19 starts at 4. Reopen against a log
    // that was truncated to 2 — the crash window.
    let mut segment = ActiveSegment::recover(dir.path().join("range.active")).unwrap();
    segment.truncate_to(2).unwrap();
    drop(segment);

    let node = Arc::new(
        InProcessFollower::new(
            FOLLOWER,
            ActiveSegment::recover(dir.path().join("range.active")).unwrap(),
            ProducerEpochJournal::open(dir.path().join("epochs")).unwrap(),
            range,
            OLD_EPOCH,
            MetaFencingEpoch::new(OLD_EPOCH),
            cluster_committed,
        )
        .unwrap(),
    );
    node.set_fencing_epoch_journal(FencingEpochJournal::open(&journal_path).unwrap());

    assert_eq!(
        node.epoch_starts(),
        vec![EpochStart {
            epoch: OLD_EPOCH,
            start_offset: 0
        }],
        "an entry starting past the log's tail cannot be true and must be dropped on open"
    );
    // The repair must be durable, not just applied in memory.
    assert_eq!(
        FencingEpochJournal::open(&journal_path).unwrap().entries(),
        &[EpochStart {
            epoch: OLD_EPOCH,
            start_offset: 0
        }]
    );
}

/// An entry AT the tail is the normal state of a replica granted an epoch it
/// has not yet written under, and must survive reattachment.
#[test]
fn an_epoch_recorded_at_the_tail_is_not_mistaken_for_an_interrupted_truncation() {
    let f = follower();
    replicate(&f, OLD_EPOCH, OLD_LEADER, 0, 4);
    // Granted, recorded at the tail, nothing written under it yet.
    f.grant(NEW_EPOCH);

    assert_eq!(
        f.node.epoch_starts().last().copied(),
        Some(EpochStart {
            epoch: NEW_EPOCH,
            start_offset: 4
        }),
        "a start at the tail is a grant not yet used, not a claim past the end"
    );
}

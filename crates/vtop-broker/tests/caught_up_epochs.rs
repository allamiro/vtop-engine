//! A follower files the records a new leader catches it up on under the
//! epochs that WROTE them, so its epoch history equals the leader's (#640).
//!
//! The failure: a follower adopted a new epoch at its own tail. When it was
//! behind, the records the new leader then sent it were filed under the new
//! epoch, while the leader filed the same bytes under the epoch that wrote
//! them. The two histories diverged below both tails, so since #577 every
//! later fence answer between them was Unknown and never counted, and after a
//! second failover no candidate could be established.
//!
//! The sequence is the #611 one the promotion crash sweep found it in:
//!
//! 1. Epoch 18 writes 0..5 to R and C, and 5..10 to R only.
//! 2. Metadata grants 19 to C. C fences itself and R; R keeps 5..10 and
//!    refuses C its vote.
//! 3. Metadata grants 20 to R. R fences itself and C, which adopted 20 at its
//!    tail, 5.
//! 4. R catches C up on 5..10 and writes its first epoch-20 record at 10.

use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;
use vtop_broker::fencing_epochs::{
    clamp_lineage, compare_lineages, EpochStart, FencingEpochJournal, Lineage,
};
use vtop_broker::replication::{ClusterCommittedOffset, InProcessFollower};
use vtop_broker::{MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
use vtop_protocol::{ProduceRecord, RangeIdentity, ReplicaAppendRequest};

const OLD_EPOCH: u64 = 18;
const C_EPOCH: u64 = 19;
const R_EPOCH: u64 = 20;
const SHARED: u64 = 5;
const R_TAIL: u64 = 10;

fn range_identity() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

fn segment_at(dir: &Path) -> ActiveSegment {
    let path = dir.join("range.active");
    if path.exists() {
        return ActiveSegment::recover(path).unwrap();
    }
    let range = range_identity();
    ActiveSegment::create(
        path,
        SegmentDescriptor {
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
        },
        SegmentConfig::default(),
    )
    .unwrap()
}

struct Replica {
    dir: TempDir,
    id: Uuid,
    meta: MetaFencingEpoch,
    node: Arc<InProcessFollower>,
}

impl Replica {
    fn new(id: u128) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let id = Uuid::from_u128(id);
        let (meta, node) = Self::open(dir.path(), id);
        Self {
            dir,
            id,
            meta,
            node,
        }
    }

    /// Open the replica from its directory the way `data_node` does.
    fn open(dir: &Path, id: Uuid) -> (MetaFencingEpoch, Arc<InProcessFollower>) {
        Self::open_holding(dir, id, OLD_EPOCH)
    }

    /// Open already holding `held`, as a process does that reads its grant
    /// before it constructs the replica: no adoption runs.
    fn open_holding(dir: &Path, id: Uuid, held: u64) -> (MetaFencingEpoch, Arc<InProcessFollower>) {
        let meta = MetaFencingEpoch::new(held);
        let node = Arc::new(
            InProcessFollower::new(
                id,
                segment_at(dir),
                ProducerEpochJournal::open(dir.join("epochs")).unwrap(),
                range_identity(),
                held,
                meta.clone(),
                ClusterCommittedOffset::new(0),
            )
            .unwrap(),
        );
        node.set_fencing_epoch_journal(
            FencingEpochJournal::open(dir.join("fencing-epochs")).unwrap(),
        );
        (meta, node)
    }

    /// A process death and restart: nothing in memory survives, and the
    /// lease watcher adopts metadata's current grant.
    fn restart(self, granted: u64) -> Self {
        let replica = self.reopen();
        replica.grant(granted);
        replica
    }

    /// Reopen from disk with nothing adopted yet: what the replica answers
    /// before its lease watcher has run.
    fn reopen(self) -> Self {
        let Replica {
            dir,
            id,
            meta,
            node,
        } = self;
        // The old handles go before the same files are reopened.
        drop(node);
        drop(meta);
        let (meta, node) = Self::open(dir.path(), id);
        Replica {
            dir,
            id,
            meta,
            node,
        }
    }

    /// Metadata grants `epoch` and this replica's lease watcher adopts it.
    fn grant(&self, epoch: u64) {
        self.meta.set(epoch);
        self.node.adopt_fencing_epoch(epoch);
    }

    fn append(&self, epoch: u64, offset: u64) {
        self.node
            .apply_append(&record(epoch, offset))
            .unwrap_or_else(|error| panic!("append at {offset} under {epoch}: {error:?}"));
    }
}

/// The record at `offset`, sent under `epoch`. One producer per record, so a
/// record resent under a newer epoch is the same record.
fn record(epoch: u64, offset: u64) -> ReplicaAppendRequest {
    ReplicaAppendRequest {
        range: range_identity(),
        fencing_epoch: epoch,
        leader_node_id: Uuid::from_u128(0xA0),
        expected_base_offset: offset,
        producer_id: Uuid::from_u128(0x1000 + u128::from(offset)),
        producer_epoch: 1,
        first_sequence: 0,
        records: vec![ProduceRecord {
            timestamp_millis: 1_000,
            key: b"k".to_vec(),
            value: format!("o{offset}").into_bytes(),
        }],
    }
}

fn starts(pairs: &[(u64, u64)]) -> Vec<EpochStart> {
    pairs
        .iter()
        .map(|&(epoch, start_offset)| EpochStart {
            epoch,
            start_offset,
        })
        .collect()
}

/// Steps 1 to 3 of the module docs: R is fenced at 20 and C has been fenced
/// by it, C still at 5.
fn up_to_the_fence_at_20() -> (Replica, Replica) {
    let r = Replica::new(0xB1);
    let c = Replica::new(0xB2);
    for offset in 0..SHARED {
        r.append(OLD_EPOCH, offset);
        c.append(OLD_EPOCH, offset);
    }
    for offset in SHARED..R_TAIL {
        r.append(OLD_EPOCH, offset);
    }

    // Epoch 19 goes to C, which is refused: R keeps 5..10.
    r.grant(C_EPOCH);
    c.meta.set(C_EPOCH);
    c.node.fence_locally(C_EPOCH).unwrap();
    let answer = r.node.fence(C_EPOCH, &c.node.epoch_starts()).unwrap();
    assert_eq!(
        answer.truncated_records, 0,
        "R keeps the records C lacks (#611)"
    );
    assert_eq!(answer.next_offset, R_TAIL);

    // Epoch 20 goes to R. C's watcher adopts it first, at C's tail.
    c.grant(R_EPOCH);
    r.meta.set(R_EPOCH);
    r.node.fence_locally(R_EPOCH).unwrap();
    c.node.fence(R_EPOCH, &r.node.epoch_starts()).unwrap();
    (r, c)
}

/// Step 4: R catches C up on 5..10, then writes its first record at 10.
fn catch_up_and_write(r: &Replica, c: &Replica) {
    for offset in SHARED..R_TAIL {
        c.append(R_EPOCH, offset);
    }
    r.append(R_EPOCH, R_TAIL);
    c.append(R_EPOCH, R_TAIL);
}

fn assert_c_files_like_r(r: &Replica, c: &Replica) {
    let leader = r.node.epoch_starts();
    let follower = c.node.epoch_starts();
    assert_eq!(
        leader,
        starts(&[(OLD_EPOCH, 0), (C_EPOCH, R_TAIL), (R_EPOCH, R_TAIL)]),
        "R files 5..10 under epoch 18, which wrote them"
    );
    assert_eq!(
        follower, leader,
        "after the catch-up C's history must equal the leader's: C holds the same bytes, and \
         before #640 it filed 5..10 under the epochs it adopted at its tail, 5"
    );
    let tail = c.node.next_offset();
    assert_eq!(tail, r.node.next_offset());
    assert_eq!(
        clamp_lineage(&follower, 0, tail),
        clamp_lineage(&leader, 0, tail),
        "every record either holds is attributed to the same epoch"
    );
    // The next failover compares exactly these. Diverged below both tails,
    // the answer would be Unknown and never counted (#577).
    assert_eq!(compare_lineages(&follower, &leader), Lineage::Agreed);
    assert_eq!(compare_lineages(&leader, &follower), Lineage::Agreed);
}

#[test]
fn after_a_catch_up_the_followers_history_equals_the_leaders() {
    let (r, c) = up_to_the_fence_at_20();
    catch_up_and_write(&r, &c);
    assert_c_files_like_r(&r, &c);
}

#[test]
fn at_the_fence_the_followers_adoptions_at_its_tail_own_nothing() {
    let (_r, c) = up_to_the_fence_at_20();
    assert_eq!(
        c.node.epoch_starts(),
        starts(&[(OLD_EPOCH, 0)]),
        "at the fence C owns nothing under 19 or 20: its adoptions at its tail were \
         provisional, and the leader's history says neither epoch wrote at 5"
    );
}

#[test]
fn a_restart_between_the_fence_and_the_catch_up_keeps_the_leaders_lineage() {
    let (r, c) = up_to_the_fence_at_20();
    let c = c.restart(R_EPOCH);
    assert_eq!(
        c.node.epoch_starts(),
        starts(&[(OLD_EPOCH, 0)]),
        "the restarted watcher's adoption of 20 at the tail is filed by the lineage the \
         fence left on disk, not recorded at 5"
    );
    catch_up_and_write(&r, &c);
    assert_c_files_like_r(&r, &c);
}

#[test]
fn a_catch_up_cut_short_by_a_restart_resumes_under_the_same_lineage() {
    let (r, c) = up_to_the_fence_at_20();
    for offset in SHARED..8 {
        c.append(R_EPOCH, offset);
    }
    assert_eq!(c.node.epoch_starts(), starts(&[(OLD_EPOCH, 0)]));
    let c = c.restart(R_EPOCH);
    for offset in 8..R_TAIL {
        c.append(R_EPOCH, offset);
    }
    r.append(R_EPOCH, R_TAIL);
    c.append(R_EPOCH, R_TAIL);
    assert_c_files_like_r(&r, &c);
}

/// The replica that kept a dispute takes nothing from the caller: its records
/// past the divergence are its own, and the caller's history says nothing
/// about who wrote them.
#[test]
fn a_replica_that_keeps_a_dispute_files_nothing_by_the_callers_history() {
    let (r, _c) = up_to_the_fence_at_20();
    assert_eq!(
        r.node.epoch_starts(),
        starts(&[(OLD_EPOCH, 0), (C_EPOCH, R_TAIL), (R_EPOCH, R_TAIL)]),
        "R's own records stay under 18; C's history (19 from 5) was not taken"
    );
}

/// A caller whose history does not name the epoch it fences at (#648
/// review: a lease-driven broker adopts its grant only after the probe)
/// says where every epoch up to its own began, but not where its new one
/// begins. Taken as the lineage, it would drop the follower's adoption of
/// that epoch and replace it with nothing, filing the new leader's records
/// under the old epoch. It is not taken: the follower keeps the adoption at
/// its tail, as before #640.
#[test]
fn a_fence_whose_history_lacks_its_own_epoch_is_not_taken_as_the_lineage() {
    let r = Replica::new(0xB1);
    let c = Replica::new(0xB2);
    for offset in 0..SHARED {
        r.append(OLD_EPOCH, offset);
        c.append(OLD_EPOCH, offset);
    }
    for offset in SHARED..R_TAIL {
        r.append(OLD_EPOCH, offset);
    }
    c.grant(R_EPOCH);
    let without_its_epoch = r.node.epoch_starts();
    assert_eq!(without_its_epoch, starts(&[(OLD_EPOCH, 0)]));
    c.node.fence(R_EPOCH, &without_its_epoch).unwrap();
    assert_eq!(
        c.node.epoch_starts(),
        starts(&[(OLD_EPOCH, 0), (R_EPOCH, SHARED)]),
        "the adoption at the tail stays: the caller's history says nothing about where \
         its epoch begins"
    );
}

/// A follower already at the leader's tail is unchanged: the leader's epoch
/// begins exactly at its tail, which is what adoption recorded before #640.
#[test]
fn an_in_sync_follower_records_the_epoch_at_its_tail_as_before() {
    let r = Replica::new(0xB1);
    let c = Replica::new(0xB2);
    for offset in 0..SHARED {
        r.append(OLD_EPOCH, offset);
        c.append(OLD_EPOCH, offset);
    }
    c.grant(R_EPOCH);
    r.meta.set(R_EPOCH);
    r.node.fence_locally(R_EPOCH).unwrap();
    c.node.fence(R_EPOCH, &r.node.epoch_starts()).unwrap();
    assert_eq!(
        c.node.epoch_starts(),
        starts(&[(OLD_EPOCH, 0), (R_EPOCH, SHARED)])
    );
    r.append(R_EPOCH, SHARED);
    c.append(R_EPOCH, SHARED);
    assert_eq!(c.node.epoch_starts(), r.node.epoch_starts());
}

/// A crash after an append is durable but before the journal is re-derived
/// from the lineage (#648 review). C holds R's first epoch-20 record at 10,
/// but its journal still ends with epoch 18: the follow that records 19 and
/// 20 at 10 never ran. Metadata moves on to 21 while C is down, so neither
/// its restart's adoption nor any append is under 20, the lineage's epoch.
/// The open must replay the lineage before the replica answers anything, or
/// C reports the record at 10 as written by 18, and keeps that.
#[test]
fn the_open_replays_the_lineage_over_records_a_crash_left_unfiled() {
    let (r, c) = up_to_the_fence_at_20();
    catch_up_and_write(&r, &c);
    let (dir, id) = crash_in_the_window(c);
    let (meta, node) = Replica::open(dir.path(), id);
    let c = Replica {
        dir,
        id,
        meta,
        node,
    };

    assert_eq!(
        c.node.epoch_starts(),
        r.node.epoch_starts(),
        "reopened, C must answer with the leader's history for the records it holds, before \
         anything is adopted or appended"
    );
    // Metadata moved on while C was down.
    c.grant(R_EPOCH + 1);
    let mut expected = r.node.epoch_starts();
    expected.push(EpochStart {
        epoch: R_EPOCH + 1,
        start_offset: R_TAIL + 1,
    });
    assert_eq!(c.node.epoch_starts(), expected);
    let c = c.reopen();
    assert_eq!(c.node.epoch_starts(), expected, "and it is durable");
}

/// Kill C and leave its directory in the crash window: the leader's record
/// at 10 durable, the journal as the fence left it (the follow that records
/// 19 and 20 at 10 never ran), and the lineage sidecar intact.
fn crash_in_the_window(c: Replica) -> (TempDir, Uuid) {
    let Replica {
        dir,
        id,
        meta,
        node,
    } = c;
    drop(node);
    drop(meta);
    let mut journal = FencingEpochJournal::open(dir.path().join("fencing-epochs")).unwrap();
    journal.truncate_to(1).unwrap();
    assert_eq!(journal.entries(), starts(&[(OLD_EPOCH, 0)]).as_slice());
    assert_eq!(journal.leader_lineage_epoch(), Some(R_EPOCH));
    drop(journal);
    (dir, id)
}

/// #648 review (cubic, P1): in the crash window, metadata moved on to 21
/// while C was down, and the first thing C does is adopt it. That adoption
/// is not under the lineage's epoch, so it never re-derived anything and
/// recorded 21 at the tail over records still filed under 18. Only the
/// adoption is checked here, not the open before it.
#[test]
fn adopting_a_newer_epoch_after_the_crash_window_keeps_the_leaders_history() {
    let (r, c) = up_to_the_fence_at_20();
    catch_up_and_write(&r, &c);
    let (dir, id) = crash_in_the_window(c);
    let (meta, node) = Replica::open(dir.path(), id);
    let c = Replica {
        dir,
        id,
        meta,
        node,
    };
    c.grant(R_EPOCH + 1);
    let mut expected = r.node.epoch_starts();
    expected.push(EpochStart {
        epoch: R_EPOCH + 1,
        start_offset: R_TAIL + 1,
    });
    assert_eq!(
        c.node.epoch_starts(),
        expected,
        "the records at 10 are epoch 20's, as R files them; 21 begins at the tail"
    );
}

/// #648 review (cubic, P1): in the crash window, C restarts already holding
/// the lineage's epoch (no adoption runs), and the leader retransmits the
/// batch C already holds. That append takes the committed-duplicate path,
/// which acks without appending and so without following the lineage. The
/// history must still be the leader's, from the replay at open.
#[test]
fn a_committed_duplicate_after_the_crash_window_answers_the_leaders_history() {
    let (r, c) = up_to_the_fence_at_20();
    catch_up_and_write(&r, &c);
    let (dir, id) = crash_in_the_window(c);
    let (meta, node) = Replica::open_holding(dir.path(), id, R_EPOCH);
    let c = Replica {
        dir,
        id,
        meta,
        node,
    };
    c.append(R_EPOCH, R_TAIL);
    assert_eq!(
        c.node.next_offset(),
        R_TAIL + 1,
        "the retransmission was a duplicate"
    );
    assert_eq!(c.node.epoch_starts(), r.node.epoch_starts());
    // And the fence the next candidate sends sees the same.
    c.meta.set(R_EPOCH + 1);
    let answer = c.node.fence(R_EPOCH + 1, &r.node.epoch_starts()).unwrap();
    assert_eq!(
        clamp_lineage(&answer.epoch_starts, 0, answer.next_offset),
        clamp_lineage(&r.node.epoch_starts(), 0, answer.next_offset),
        "the fence answer attributes every record C holds as R does"
    );
}

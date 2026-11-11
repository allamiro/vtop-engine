//! Deterministic crash sweep over a kept dispute's lifetime (#635).
//!
//! A replica fenced by a candidate that merely lacks its suffix keeps the
//! suffix (#611) and announces it in the fence reply. If that candidate is
//! established anyway, its first append or committed mark past the
//! dispute's start discards the suffix before anything is acked over it.
//! Before #635 the replica remembered the dispute in memory only. Killed
//! between the fence reply and that settlement, it came back holding the old
//! records with no record that they were disputed, and the catch-up path
//! acked the new leader's record at the disputed offset as a duplicate of a
//! DIFFERENT record (#261).
//!
//! The sweep runs the same harness shape as `epoch_journal_crash_sweep.rs`:
//! a workload over [`SimStorage`], a crash before every storage operation
//! and inside every write at every byte cut, then a reboot through the same
//! open path the data node uses. The workload starts from a durable fixture
//! (ten epoch-18 records, cluster mark 5) and covers the fence that keeps
//! 5..10 and the epoch-19 leader's append at 5 that settles it.
//!
//! What must hold after every crash whose fence reply was delivered:
//! * the replica reports its log as ending at the dispute's start for as
//!   long as it still holds the old suffix, so the leader re-sends from there;
//! * the leader's record at 5 replaces the old one; it is never acked as a
//!   duplicate of it;
//! * the five acknowledged records below the mark are untouched;
//! * once settled, the fencing-epoch journal is back at version 1, so an
//!   older binary is refused only while a dispute is pending.
//!
//! A crash BEFORE the reply leaves a replica the candidate never counted. It
//! is in the position of any replica absent from the fence, which is #261's
//! documented catch-up gap and not this test's subject; those runs are
//! recovered and counted but not judged.

use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;
use vtop_broker::committed_floor::CommittedFloorFile;
use vtop_broker::fencing_epochs::{EpochStart, FencingEpochJournal};
use vtop_broker::replication::{ClusterCommittedOffset, InProcessFollower};
use vtop_broker::{MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::env::Env;
use vtop_log::sim::{FaultPlan, SimDiskState, SimStorage, TraceKind};
use vtop_log::{KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor, SegmentSet};
use vtop_protocol::{CommittedHwmUpdate, ProduceRecord, RangeIdentity, ReplicaAppendRequest};

const SEED: u64 = 0x5eed_0635;
const DIR: &str = "/range";
const FOLLOWER: Uuid = Uuid::from_u128(0xA2);
const OLD_LEADER: Uuid = Uuid::from_u128(0xA1);
const NEW_LEADER: Uuid = Uuid::from_u128(0xA9);
const PRODUCER: Uuid = Uuid::from_u128(0xB1);
const OLD_EPOCH: u64 = 18;
const NEW_EPOCH: u64 = 19;
/// The cluster mark the replica learned before the fence, and the first
/// offset the fence keeps in dispute.
const DISPUTED_FROM: u64 = 5;
const HELD: u64 = 10;

fn range_identity() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

fn descriptor(range: &RangeIdentity) -> SegmentDescriptor {
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
    }
}

fn path(name: &str) -> String {
    format!("{DIR}/{name}")
}

/// Values tell the two leaderships' records apart: an ack over the wrong
/// one is invisible to offsets alone.
fn value(leader: Uuid, offset: u64) -> Vec<u8> {
    let who = if leader == NEW_LEADER { "new" } else { "old" };
    format!("{who}-{offset}").into_bytes()
}

fn append_request(epoch: u64, leader: Uuid, offset: u64) -> ReplicaAppendRequest {
    ReplicaAppendRequest {
        range: range_identity(),
        fencing_epoch: epoch,
        leader_node_id: leader,
        expected_base_offset: offset,
        producer_id: PRODUCER,
        producer_epoch: 1,
        first_sequence: offset,
        records: vec![ProduceRecord {
            timestamp_millis: 1_000,
            key: b"k".to_vec(),
            value: value(leader, offset),
        }],
    }
}

/// The epoch-19 candidate never received 5..10 and adopted 19 at its own
/// tail, 5: the #611 shape, which proves nothing against them.
fn candidate_history() -> [EpochStart; 2] {
    [
        EpochStart {
            epoch: OLD_EPOCH,
            start_offset: 0,
        },
        EpochStart {
            epoch: NEW_EPOCH,
            start_offset: DISPUTED_FROM,
        },
    ]
}

/// Open a replica over `env` exactly as `data_node` does after a restart:
/// segments, both journals and the committed floor reopened from disk,
/// nothing carried over in memory. `None` when the disk refuses (only
/// expected while a crash is in progress).
fn open_replica(env: &Env, meta: &MetaFencingEpoch) -> Option<Arc<InProcessFollower>> {
    let segment = SegmentSet::open_in(env, DIR).ok()??;
    let producer_epochs = ProducerEpochJournal::open_in(env, path("epochs")).ok()?;
    let floor = CommittedFloorFile::open_in(env, path("committed-floor"));
    let node = Arc::new(
        InProcessFollower::new(
            FOLLOWER,
            segment,
            producer_epochs,
            range_identity(),
            OLD_EPOCH,
            meta.clone(),
            ClusterCommittedOffset::new(floor.floor()),
        )
        .ok()?,
    );
    node.set_fencing_epoch_journal(FencingEpochJournal::open_in(env, path("fencing-epochs")).ok()?);
    node.set_committed_floor(floor);
    Some(node)
}

/// The durable starting point: ten epoch-18 records, the cluster mark at 5
/// (persisted by the floor), shut down in order.
fn fixture() -> SimDiskState {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new(DIR));
    let env = sim.env(SEED);
    let meta = MetaFencingEpoch::new(OLD_EPOCH);
    let segment = SegmentSet::create_in(
        &env,
        DIR,
        descriptor(&range_identity()),
        SegmentConfig::default(),
    )
    .expect("create the range");
    let node = InProcessFollower::new(
        FOLLOWER,
        segment,
        ProducerEpochJournal::open_in(&env, path("epochs")).expect("producer journal"),
        range_identity(),
        OLD_EPOCH,
        meta,
        ClusterCommittedOffset::new(0),
    )
    .expect("follower");
    node.set_fencing_epoch_journal(
        FencingEpochJournal::open_in(&env, path("fencing-epochs")).expect("epoch journal"),
    );
    node.set_committed_floor(CommittedFloorFile::open_in(&env, path("committed-floor")));
    for offset in 0..HELD {
        node.apply_append(&append_request(OLD_EPOCH, OLD_LEADER, offset))
            .expect("fixture append");
    }
    node.observe_hwm(&CommittedHwmUpdate {
        range: range_identity(),
        fencing_epoch: OLD_EPOCH,
        committed_high_watermark: DISPUTED_FROM,
    })
    .expect("fixture mark");
    node.quiesce().expect("fixture shutdown");
    sim.snapshot()
}

/// How far the workload got before the crash, as the outside world saw it.
#[derive(Clone, Copy, Debug, Default)]
struct Progress {
    /// The fence returned a reply the candidate counts, announcing 5..10.
    announced: bool,
    /// The leader's append at 5 was acked.
    settled: bool,
}

/// Fence (keeps 5..10), then the established leader appends at 5 (settles).
fn run_workload(env: &Env) -> Progress {
    let mut progress = Progress::default();
    let meta = MetaFencingEpoch::new(NEW_EPOCH);
    let Some(node) = open_replica(env, &meta) else {
        return progress;
    };
    match node.fence(NEW_EPOCH, &candidate_history()) {
        Ok(outcome) => {
            assert_eq!(
                (outcome.truncated_records, outcome.next_offset),
                (0, HELD),
                "precondition: the fence keeps the suffix the candidate lacks"
            );
            // A reply with no history is an Unknown answer: the candidate
            // records it but never counts it (#577), so it announced
            // nothing. That is the shape of a crash that broke the journal
            // while the fence adopted its epoch.
            progress.announced = !outcome.epoch_starts.is_empty();
        }
        Err(_) => return progress,
    }
    if node
        .apply_append(&append_request(NEW_EPOCH, NEW_LEADER, DISPUTED_FROM))
        .is_ok()
    {
        progress.settled = true;
    }
    progress
}

fn journal_version(sim: &SimStorage) -> u16 {
    let disk = sim.snapshot();
    let bytes = &disk.files[&Path::new(&path("fencing-epochs")).to_path_buf()];
    u16::from_be_bytes([bytes[8], bytes[9]])
}

/// What recovery found, for the coverage assertions.
#[derive(Clone, Copy, Debug, Default)]
struct Recovered {
    /// The reopened replica still held the old suffix.
    held_old_suffix: bool,
}

/// Reboot, reopen, let the lease watcher adopt 19, and let the epoch-19
/// leader do what it does on reconnect: read the status, re-send what the
/// status says is missing from 5, and publish its mark. Judges announced runs.
fn recover_and_check(sim: &SimStorage, env: &Env, progress: Progress, context: &str) -> Recovered {
    sim.reboot();
    let meta = MetaFencingEpoch::new(NEW_EPOCH);
    let node = open_replica(env, &meta)
        .unwrap_or_else(|| panic!("the replica must reopen after any crash ({context})"));
    node.adopt_fencing_epoch(NEW_EPOCH);

    let held_old_suffix = node.next_offset() > DISPUTED_FROM + 1;
    let (_, reported_next) = node.replication_status();
    if progress.announced && held_old_suffix {
        assert!(
            reported_next <= DISPUTED_FROM,
            "the reopened replica holds the suffix its fence reply kept in dispute from \
             {DISPUTED_FROM} but reports its log as ending at {reported_next}: the leader would \
             drop the records it must send there ({context})"
        );
    }

    let ack = node.apply_append(&append_request(NEW_EPOCH, NEW_LEADER, DISPUTED_FROM));
    if progress.announced {
        ack.as_ref().unwrap_or_else(|error| {
            panic!("the established leader's append at {DISPUTED_FROM} was refused: {error:?} ({context})")
        });
    }
    if ack.is_err() {
        return Recovered { held_old_suffix };
    }
    node.observe_hwm(&CommittedHwmUpdate {
        range: range_identity(),
        fencing_epoch: NEW_EPOCH,
        committed_high_watermark: DISPUTED_FROM + 1,
    })
    .unwrap_or_else(|error| panic!("the leader's mark: {error:?} ({context})"));

    let fetched = node
        .fetch(0, usize::MAX, 64)
        .unwrap_or_else(|error| panic!("fetch after recovery: {error} ({context})"));
    for record in fetched.records.iter().filter(|r| r.offset < DISPUTED_FROM) {
        assert_eq!(
            record.record.value,
            value(OLD_LEADER, record.offset),
            "an acknowledged record below the mark changed ({context})"
        );
    }
    if progress.announced {
        assert_eq!(
            node.next_offset(),
            DISPUTED_FROM + 1,
            "the leader's append at {DISPUTED_FROM} was acked over the old suffix instead of \
             replacing it: this replica was counted for a record it does not hold (#261, \
             #635) ({context})"
        );
        let at = fetched
            .records
            .iter()
            .find(|r| r.offset == DISPUTED_FROM)
            .unwrap_or_else(|| panic!("no record at {DISPUTED_FROM} after recovery ({context})"));
        assert_eq!(
            at.record.value,
            value(NEW_LEADER, DISPUTED_FROM),
            "the record at {DISPUTED_FROM} is not the established leader's ({context})"
        );
        assert_eq!(
            journal_version(sim),
            1,
            "a settled dispute left the epoch journal gated against older binaries ({context})"
        );
    }
    Recovered { held_old_suffix }
}

/// #635, the named case: the replica dies right after its fence reply,
/// before the new leader's first append. Before the fix it came back with
/// the old suffix and no dispute, reported its full log, and acked the
/// leader's record at 5 as a duplicate of a different one.
#[test]
fn a_replica_killed_between_its_fence_reply_and_settlement_still_replaces_the_kept_suffix() {
    let fixture = fixture();
    let sim = SimStorage::new();
    sim.restore(&fixture);
    let env = sim.env(SEED);
    let meta = MetaFencingEpoch::new(NEW_EPOCH);
    let node = open_replica(&env, &meta).expect("open the fixture");
    let outcome = node
        .fence(NEW_EPOCH, &candidate_history())
        .expect("the fence keeps the suffix");
    assert_eq!((outcome.truncated_records, outcome.next_offset), (0, HELD));
    drop(node);
    // kill -9: everything not yet synced is gone.
    sim.crash();

    let recovered = recover_and_check(
        &sim,
        &env,
        Progress {
            announced: true,
            settled: false,
        },
        "killed after the fence reply",
    );
    assert!(
        recovered.held_old_suffix,
        "precondition: the suffix survived"
    );
}

#[test]
fn a_crash_before_every_operation_never_acks_over_an_announced_kept_suffix() {
    let fixture = fixture();
    let clean = SimStorage::new();
    clean.restore(&fixture);
    let clean_progress = run_workload(&clean.env(SEED));
    assert!(
        clean_progress.announced && clean_progress.settled,
        "the clean run must fence and settle: {clean_progress:?}"
    );
    let total = clean.op_count();

    let mut kept_across_a_restart = 0;
    for op in 0..total {
        let context = format!("crash-before op={op} seed={SEED:#x}");
        let sim = SimStorage::new();
        sim.restore(&fixture);
        let env = sim.env(SEED);
        sim.set_fault(FaultPlan::CrashBefore(op));
        let progress = run_workload(&env);
        assert!(sim.has_crashed(), "{context}");
        let recovered = recover_and_check(&sim, &env, progress, &context);
        if progress.announced && recovered.held_old_suffix {
            kept_across_a_restart += 1;
        }
    }
    assert!(
        kept_across_a_restart > 0,
        "no crash point fell between the fence reply and settlement; the sweep never reached \
         #635's window"
    );
}

#[test]
fn a_torn_write_anywhere_in_the_fence_or_settlement_never_acks_over_an_announced_kept_suffix() {
    let fixture = fixture();
    let clean = SimStorage::new();
    clean.restore(&fixture);
    run_workload(&clean.env(SEED));
    let trace = clean.trace();

    let mut runs = 0;
    for entry in trace
        .iter()
        .filter(|entry| entry.kind == TraceKind::HandleWrite)
    {
        for cut in 0..=entry.len as usize {
            let context = format!(
                "torn-write op={} cut={cut} len={} seed={SEED:#x}",
                entry.index, entry.len
            );
            let sim = SimStorage::new();
            sim.restore(&fixture);
            let env = sim.env(SEED);
            sim.set_fault(FaultPlan::CrashDuringWrite {
                op: entry.index,
                byte_cut: cut,
            });
            let progress = run_workload(&env);
            assert!(sim.has_crashed(), "{context}");
            recover_and_check(&sim, &env, progress, &context);
            runs += 1;
        }
    }
    assert!(runs > 0, "the workload made no writes to tear");
}

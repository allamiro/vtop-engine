//! Deterministic crash sweep over the #611 promotion sequence.
//!
//! #611: a replica that held a quorum-acknowledged record the candidate lacked
//! truncated it at the fence, then granted the candidate its vote. #616 makes
//! the fence keep such a suffix (`caller_supersedes_suffix`), makes the
//! candidate count the replica's full answer (`fenced_lineage` with the
//! candidate's tail), and makes an outvoted replica discard a kept suffix on
//! the established leader's first append or high-water mark over it
//! (`KeptDispute`). This sweep crashes every replica at every durable step of
//! that sequence and checks, after restart and completion, that nothing
//! acknowledged was lost.
//!
//! # System model
//!
//! Crash-recovery processes, honest (non-Byzantine) nodes, and a network that
//! delivers or drops whole requests. Three replicas L, R and C, RF=3, each on
//! its own simulated disk (`vtop_log::sim`), each opened through the same
//! steps `data_node` runs at startup: `SegmentSet::open_in`, the producer epoch
//! journal, the fencing-epoch journal and the committed-floor file. A crash is
//! a power loss on that disk (unsynced bytes vanish, a write in flight may be
//! torn) together with the death of the process, so nothing held in memory
//! survives. A crashed node restarts at the next step, the way a supervisor
//! restarts it, and adopts metadata's current grant as its lease watcher does.
//!
//! What runs the real code and what is modelled:
//!
//! * Real: every replica's append, fence and reconciliation, kept-dispute
//!   settlement, high-water-mark handling, both journals, the floor file, the
//!   segment log and its recovery; the candidate's local fence
//!   (`fence_locally`), lineage verdict (`promotion::fenced_lineage`) and
//!   vote (`promotion::establish`).
//! * Modelled: the leader's write path is the leader's own replica taking the
//!   append (the same records, durably, under the leader's epoch), with the
//!   producer acknowledged once the leader's copy and one follower's copy are
//!   both durable. The candidate's probe calls each follower's `fence`
//!   in-process with the history `ReplicaPlaneProbe` would send and judges the
//!   answer the way it does; the TLS path itself is covered by the
//!   `lease_agent` tests over a real replica-plane server. Metadata is a
//!   counter that grants the next epoch to the next candidate. The new
//!   leader's boundary marker is an ordinary record from its own producer.
//!   A leader's session with a follower opens with the fence
//!   `NetworkedReplicaSet` sends (#641): the leader's epoch and history,
//!   before the status read, any record, or any mark. A session lasts while
//!   both the follower's process and the leader's epoch do; a restarted
//!   follower gets a new one.
//!
//! # The sequence (the #611 issue body)
//!
//! 1. L leads at epoch 18. Offsets 0..5 reach all three replicas and every
//!    replica learns the mark 5 (setup; not swept).
//! 2. L appends 5..10. R stores each one durably before L does; C never
//!    receives them. A record is acknowledged when L's and R's copies are
//!    both durable.
//! 3. L dies before R learns the mark moved past 5.
//! 4. Metadata grants the next epoch to C. C fences itself, then fences R
//!    (and L, when it is up) with its epoch history.
//! 5. R reconciles, keeps the disputed suffix, and answers. C counts the
//!    answer and is refused, or is established.
//! 6. Whoever is established catches the followers up, appends its marker
//!    and one new record, and publishes its high-water mark. An outvoted
//!    replica settles its kept dispute here.
//! 7. Every node that is down restarts, catches up, and learns the final mark.
//!
//! Three shapes of that sequence:
//!
//! * `AckedLeaderStaysDown`: 5..10 acknowledged, L stays down until a new
//!   leader has published. The issue's scenario.
//! * `AckedLeaderReturns`: the same, but L restarts before the election.
//! * `UnackedOutvoted`: L's own copies of 5..10 are not durable when it dies,
//!   so nothing past 5 was acknowledged. L restarts with 5, C and L outvote
//!   R, and R must discard its kept suffix rather than ack C's records over
//!   it. This is the settlement path.
//!
//! # Faults swept, one per run
//!
//! For each shape and each node's disk: a crash before every storage
//! operation that node performs after setup, and a crash during every write
//! at its first byte, its middle byte and its last byte. Separately, the
//! death and restart of every node after every step (a step is one append,
//! fence, mark or restart on one node), with unsynced bytes lost.
//!
//! # Invariants (safety)
//!
//! * No acknowledged record is lost: every record a leader acknowledged to a
//!   producer, and every record under a high-water mark a leader published,
//!   is at its offset, with its bytes, on the final leader.
//! * No replica acks a record it does not hold: whenever a follower acks an
//!   append, its durable log holds that request's bytes at that offset.
//! * Replicas agree below the final high-water mark.
//!
//! And liveness: some candidate is established, and every node catches up,
//! within a bounded number of elections once faults stop.
//!
//! # Known gaps: #635, #640
//!
//! Each failing run is matched against these by signature
//! (`Outcome::known_issues`) and printed with its issue number. A failure
//! that matches none of them fails the test, so the sweep stays a guard.
//!
//! * #635. A kept dispute is held in memory. A replica restarted between
//!   its fence and the settlement forgets it, and the established leader's
//!   first append over the suffix is acked as a duplicate of a record the
//!   replica does not hold (#261). Signature: the violation is on a node
//!   restarted while it held a kept dispute that its fence reply had
//!   announced, at or above the dispute's start.
//! * #640. A follower's fencing-epoch journal records an adopted epoch at its
//!   own tail, so records a new leader later catches it up on are filed
//!   under that epoch rather than the one that wrote them. After a second
//!   failover the replicas cannot prove each other's lineage, every answer
//!   is Unknown, and no candidate is established again. Signature: no leader
//!   finished and the last election failed with answers judged Unknown;
//!   every acknowledged record must still be durable on a majority.
//!   Reproduction:
//!   `reproduction_two_caught_up_replicas_cannot_prove_a_quorum_after_the_leader_is_lost`.
//!
//! Fixed and no longer matched: #641, a deposed leader absent from the
//! election that replaced it, which came back unfenced and kept its
//! unacknowledged suffix under the new leader's mark. Every session now opens
//! with a fence, so such a run fails the sweep. Reproduction, kept as a
//! guard:
//! `reproduction_a_returning_deposed_leader_keeps_its_unacknowledged_suffix_under_the_mark`.

// A node is an index into the cluster, `NAMES` and the trace arrays at once.
#![allow(clippy::needless_range_loop)]

use crate::promotion::{establish, fenced_lineage, Promotion, ReplicaProbe};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;
use vtop_broker::committed_floor::CommittedFloorFile;
use vtop_broker::fencing_epochs::FencingEpochJournal;
use vtop_broker::replication::{ClusterCommittedOffset, InProcessFollower};
use vtop_broker::{MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::env::Env;
use vtop_log::sim::{FaultPlan, SimDiskState, SimStorage, TraceKind};
use vtop_log::{KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor, SegmentSet};
use vtop_meta::AnswerLineage;
use vtop_protocol::{CommittedHwmUpdate, ProduceRecord, RangeIdentity, ReplicaAppendRequest};

const SEED: u64 = 0x5eed_0611;
const DATA_DIR: &str = "/node";
const PRODUCER_EPOCHS: &str = "/node/epochs";
const FENCING_EPOCHS: &str = "/node/fencing-epochs";
const COMMITTED_FLOOR: &str = "/node/committed-floor";
const OLD_EPOCH: u64 = 18;
const REPLICATION_FACTOR: usize = 3;
/// Records every replica holds and knows committed before the sweep starts.
const SHARED: u64 = 5;
/// Records L appends in step 2.
const DISPUTED: std::ops::Range<u64> = 5..10;
/// Elections a run may take before it counts as a liveness failure.
const MAX_ELECTIONS: usize = 12;

const L: usize = 0;
const R: usize = 1;
const C: usize = 2;
const NAMES: [&str; 3] = ["L", "R", "C"];
/// Candidates in the order metadata grants them: C first, as in the issue.
const CANDIDATE_ORDER: [usize; 3] = [C, R, L];

fn node_id(node: usize) -> Uuid {
    Uuid::from_u128(node as u128 + 1)
}

fn range() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

fn descriptor() -> SegmentDescriptor {
    let range = range();
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    AckedLeaderStaysDown,
    AckedLeaderReturns,
    UnackedOutvoted,
}

const SHAPES: [Shape; 3] = [
    Shape::AckedLeaderStaysDown,
    Shape::AckedLeaderReturns,
    Shape::UnackedOutvoted,
];

#[derive(Clone, Copy, Debug)]
enum Fault {
    None,
    Storage {
        node: usize,
        plan: FaultPlan,
    },
    /// Kill the node after the driver's `step`-th step and restart it.
    ProcessDeath {
        node: usize,
        step: u64,
    },
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Fault::None => write!(f, "no fault"),
            Fault::Storage { node, plan } => write!(f, "{} disk {plan:?}", NAMES[*node]),
            Fault::ProcessDeath { node, step } => {
                write!(f, "{} killed and restarted after step {step}", NAMES[*node])
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Violation {
    /// A leader acknowledged or published different bytes at an offset that
    /// already holds an acknowledged record: the earlier one was lost, at
    /// the moment the later one replaced it.
    AcknowledgedRecordReplaced {
        offset: u64,
        acknowledged: String,
        replaced_by: String,
    },
    /// A record a leader acknowledged or published is not on the final
    /// leader at its offset.
    AckedRecordLost {
        offset: u64,
        expected: String,
        found: Option<String>,
    },
    /// A follower acked an append while its durable log held other bytes at
    /// that offset (#261).
    AckWithoutTheRecord {
        node: usize,
        offset: u64,
        sent: String,
        holds: Option<String>,
    },
    /// A replica disagrees with the final leader below the final mark.
    DivergedBelowMark {
        node: usize,
        offset: u64,
        leader: String,
        holds: Option<String>,
    },
    Liveness(String),
}

/// How a run ended.
#[derive(Debug)]
struct Outcome {
    violations: Vec<Violation>,
    /// `(node, dispute start)` for every restart of a node that held a kept
    /// dispute at the time: the #635 exposure.
    forgotten_disputes: Vec<(usize, u64)>,
    /// What the run did, for the failure message.
    log: Vec<String>,
    /// Ops each disk performed after setup, and the trace of each (clean
    /// runs only).
    traces: Vec<Vec<vtop_log::sim::TraceEntry>>,
    first_op: [u64; 3],
    steps: u64,
    /// The first election's result, for the no-fault assertions.
    first_promotion: Option<(usize, Promotion)>,
    final_leader: Option<usize>,
    acknowledged: BTreeMap<u64, String>,
    /// Nodes whose open refused after the crash, and why.
    refused_to_open: Vec<(usize, String)>,
    /// The last election's result, for the #640 signature.
    last_promotion: Option<Promotion>,
}

impl Outcome {
    /// The known issues this run's violations match, one per violation, or
    /// `None` when any violation matches none of them: a new failure, which
    /// fails the sweep.
    ///
    /// * #635: an ack or disagreement on a node that was restarted while it
    ///   held a kept dispute its fence reply had announced, at or above the
    ///   dispute's start. A lost acknowledged record counts only inside the
    ///   range of such a dispute, on a node that acked falsely there.
    /// * #640: no leader finished, and the last election failed for want of
    ///   a provable quorum: answers came back but were judged Unknown. Every
    ///   acknowledged record must still be durable on a majority, or the run
    ///   reports a lost record, which matches nothing.
    ///
    /// #641 (a returning deposed leader never fenced) is fixed and no longer
    /// matched: a run that shows it fails the sweep.
    fn known_issues(&self) -> Option<BTreeSet<u32>> {
        let after_a_forgotten_dispute = |node: usize, offset: u64| {
            self.forgotten_disputes
                .iter()
                .any(|(forgot, from)| *forgot == node && offset >= *from)
        };
        // A lost record is #635's only inside the range a forgotten dispute
        // covered, on a node that then acked a record it did not hold there
        // (#646 review): the false ack is how #635 loses a record. A loss
        // anywhere else is independent of it and stays a new failure.
        let disputes_that_acked_falsely: Vec<(usize, u64)> = self
            .forgotten_disputes
            .iter()
            .copied()
            .filter(|(node, from)| {
                self.violations.iter().any(|violation| {
                    matches!(
                        violation,
                        Violation::AckWithoutTheRecord { node: acked, offset, .. }
                            if acked == node && offset >= from
                    )
                })
            })
            .collect();
        let lost_to_635 = |offset: u64| {
            disputes_that_acked_falsely
                .iter()
                .any(|(_, from)| offset >= *from)
        };
        let mut issues = BTreeSet::new();
        for violation in &self.violations {
            let issue = match violation {
                Violation::AckWithoutTheRecord { node, offset, .. }
                | Violation::DivergedBelowMark { node, offset, .. } => {
                    if after_a_forgotten_dispute(*node, *offset) {
                        635
                    } else {
                        return None;
                    }
                }
                Violation::AckedRecordLost { offset, .. }
                | Violation::AcknowledgedRecordReplaced { offset, .. }
                    if lost_to_635(*offset) =>
                {
                    635
                }
                Violation::AckedRecordLost { .. }
                | Violation::AcknowledgedRecordReplaced { .. } => return None,
                Violation::Liveness(_)
                    if self.final_leader.is_none()
                        && matches!(
                            self.last_promotion,
                            Some(Promotion::QuorumUnavailable { unproven, .. }) if unproven > 0
                        ) =>
                {
                    640
                }
                Violation::Liveness(_) => return None,
            };
            issues.insert(issue);
        }
        Some(issues)
    }

    fn report(&self, shape: Shape, fault: &Fault) -> String {
        format!(
            "shape {shape:?}, {fault}, seed {SEED:#x}\n  violations: {:#?}\n  forgotten \
             disputes (node, from): {:?}\n  run:\n    {}",
            self.violations,
            self.forgotten_disputes
                .iter()
                .map(|(node, from)| (NAMES[*node], *from))
                .collect::<Vec<_>>(),
            self.log.join("\n    ")
        )
    }
}

struct Node {
    disk: SimStorage,
    env: Env,
    meta: MetaFencingEpoch,
    replica: Option<Arc<InProcessFollower>>,
    /// Kept down by the scenario (L after it dies), not restarted on the
    /// next step.
    held_down: bool,
    /// Why this node's open refused after a crash; it stays down.
    refuses_to_open: Option<String>,
}

struct Cluster {
    shape: Shape,
    fault: Fault,
    nodes: Vec<Node>,
    granted: u64,
    published: u64,
    /// The node that last completed an election, until it is deposed.
    leader: Option<usize>,
    steps: u64,
    elections: usize,
    /// Offset -> bytes of every record a leader acknowledged to a producer or
    /// put under a published high-water mark.
    acknowledged: BTreeMap<u64, String>,
    /// The request each record value was first sent in, for resending it.
    templates: HashMap<String, ReplicaAppendRequest>,
    next_producer: u128,
    violations: Vec<Violation>,
    forgotten_disputes: Vec<(usize, u64)>,
    log: Vec<String>,
    first_promotion: Option<(usize, Promotion)>,
    last_promotion: Option<Promotion>,
    /// Per node: a fence reply that announced a kept suffix was sent, and
    /// the dispute it announced is still held (#635's precondition).
    announced_dispute: [bool; 3],
    /// Per follower: the leader, epoch and replica process of its current
    /// session (#641). A session opens with a fence, so a follower whose
    /// process or leader changed is fenced before it is sent anything.
    sessions: [Option<Session>; 3],
}

/// One leader-to-follower session, as `NetworkedReplicaSet` holds it.
#[derive(Clone)]
struct Session {
    leader: usize,
    epoch: u64,
    /// The leader's process: a restarted leader's connections are gone too
    /// (#649 review).
    leader_process: Arc<InProcessFollower>,
    follower: Arc<InProcessFollower>,
}

/// The durable records on `disk`, read from a copy so the read itself
/// changes nothing and counts no operation.
fn durable_records(disk: &SimStorage) -> BTreeMap<u64, String> {
    let copy = SimStorage::new();
    copy.restore(&disk.snapshot());
    let env = copy.env(SEED);
    let mut set = match SegmentSet::open_in(&env, DATA_DIR) {
        Ok(Some(set)) => set,
        Ok(None) => return BTreeMap::new(),
        Err(error) => panic!("a durable log the sweep reads back does not open: {error}"),
    };
    let base = set.base_offset();
    set.fetch_through(base, usize::MAX, usize::MAX, u64::MAX)
        .expect("read the durable log back")
        .records
        .into_iter()
        .map(|fetched| {
            (
                fetched.offset,
                String::from_utf8_lossy(&fetched.record.value).into_owned(),
            )
        })
        .collect()
}

/// Open a replica from `env` exactly as `data_node` does after a restart.
fn open_replica(
    node: usize,
    env: &Env,
    meta: &MetaFencingEpoch,
) -> Result<Arc<InProcessFollower>, String> {
    let set = match SegmentSet::open_in(env, DATA_DIR).map_err(|error| error.to_string())? {
        Some(set) => set,
        None => SegmentSet::create_in(env, DATA_DIR, descriptor(), SegmentConfig::default())
            .map_err(|error| error.to_string())?,
    };
    let floor = CommittedFloorFile::open_in(env, COMMITTED_FLOOR);
    let replica = InProcessFollower::new(
        node_id(node),
        set,
        ProducerEpochJournal::open_in(env, PRODUCER_EPOCHS).map_err(|error| error.to_string())?,
        range(),
        OLD_EPOCH,
        meta.clone(),
        ClusterCommittedOffset::new(floor.floor()),
    )
    .map_err(|error| error.to_string())?;
    replica.set_fencing_epoch_journal(
        FencingEpochJournal::open_in(env, FENCING_EPOCHS).map_err(|error| error.to_string())?,
    );
    replica.set_committed_floor(floor);
    Ok(Arc::new(replica))
}

/// The three disks after setup (step 1), built once.
fn setup_disks() -> Vec<SimDiskState> {
    let disks: Vec<SimStorage> = (0..3).map(|_| SimStorage::new()).collect();
    let mut cluster = Cluster {
        shape: Shape::AckedLeaderStaysDown,
        fault: Fault::None,
        nodes: disks
            .iter()
            .map(|disk| {
                disk.create_dir_all(Path::new(DATA_DIR));
                Node {
                    disk: disk.clone(),
                    env: disk.env(SEED),
                    meta: MetaFencingEpoch::new(OLD_EPOCH),
                    replica: None,
                    held_down: false,
                    refuses_to_open: None,
                }
            })
            .collect(),
        granted: OLD_EPOCH,
        published: 0,
        leader: None,
        steps: 0,
        elections: 0,
        acknowledged: BTreeMap::new(),
        templates: HashMap::new(),
        next_producer: 0,
        violations: Vec::new(),
        forgotten_disputes: Vec::new(),
        log: Vec::new(),
        first_promotion: None,
        last_promotion: None,
        announced_dispute: [false; 3],
        sessions: [None, None, None],
    };
    for node in 0..3 {
        let opened = open_replica(node, &cluster.nodes[node].env, &cluster.nodes[node].meta)
            .expect("a fresh replica opens");
        cluster.nodes[node].replica = Some(opened);
    }
    cluster.open_setup_sessions();
    for offset in 0..SHARED {
        let request = cluster.new_record(format!("e{OLD_EPOCH}-o{offset}"), OLD_EPOCH, L, offset);
        assert!(
            cluster.replicate(L, &request, &[R, C]),
            "setup record {offset} must reach a quorum"
        );
        cluster.acknowledged.insert(offset, value_of(&request));
    }
    cluster.publish_mark(L, OLD_EPOCH, SHARED, &[L, R, C]);
    for node in 0..3 {
        cluster.nodes[node]
            .replica
            .as_ref()
            .expect("up")
            .quiesce()
            .expect("an orderly stop after setup");
    }
    assert!(cluster.violations.is_empty(), "{:?}", cluster.violations);
    disks.iter().map(SimStorage::snapshot).collect()
}

fn value_of(request: &ReplicaAppendRequest) -> String {
    String::from_utf8_lossy(&request.records[0].value).into_owned()
}

impl Cluster {
    fn note(&mut self, line: String) {
        self.log.push(format!("[step {}] {line}", self.steps));
    }

    fn up(&self, node: usize) -> Option<Arc<InProcessFollower>> {
        self.nodes[node].replica.clone()
    }

    fn new_record(
        &mut self,
        value: String,
        epoch: u64,
        leader: usize,
        offset: u64,
    ) -> ReplicaAppendRequest {
        // One producer per record: no record's sequence depends on another's,
        // so a resend after truncation is never a sequence conflict.
        self.next_producer += 1;
        let request = ReplicaAppendRequest {
            range: range(),
            fencing_epoch: epoch,
            leader_node_id: node_id(leader),
            expected_base_offset: offset,
            producer_id: Uuid::from_u128(0xB000_0000 + self.next_producer),
            producer_epoch: 1,
            first_sequence: 0,
            records: vec![ProduceRecord {
                timestamp_millis: 1_000,
                key: b"k".to_vec(),
                value: value.clone().into_bytes(),
            }],
        };
        self.templates.insert(value, request.clone());
        request
    }

    /// The step that just ran touched `node`. Count it, apply a process
    /// death scheduled after it, and restart any node whose disk crashed.
    fn after_step(&mut self, _node: usize) {
        self.steps += 1;
        if let Fault::ProcessDeath { node: victim, step } = self.fault {
            if step == self.steps && self.nodes[victim].replica.is_some() {
                self.note(format!("{} killed (fault)", NAMES[victim]));
                self.kill(victim);
                self.nodes[victim].disk.reboot();
                if !self.nodes[victim].held_down {
                    self.restart(victim);
                }
            }
        }
        for index in 0..3 {
            if self.nodes[index].disk.has_crashed() {
                self.note(format!("{}'s disk crashed (fault)", NAMES[index]));
                self.kill(index);
                self.nodes[index].disk.reboot();
                if !self.nodes[index].held_down {
                    self.restart(index);
                }
            }
        }
    }

    /// Process death with power loss: memory and unsynced bytes are gone.
    fn kill(&mut self, node: usize) {
        if let Some(replica) = self.nodes[node].replica.take() {
            let (_, reported) = replica.replication_status();
            let held = replica.next_offset();
            if reported < held && self.announced_dispute[node] {
                self.forgotten_disputes.push((node, reported));
                self.note(format!(
                    "{} dies holding a kept dispute from {reported} (held {held}) that its \
                     fence reply announced (#635)",
                    NAMES[node]
                ));
            }
        }
        self.announced_dispute[node] = false;
        self.nodes[node].disk.crash();
    }

    /// Restart `node` from its disk and adopt metadata's grant, as the lease
    /// watcher does. A restart whose own storage crashes is retried once; a
    /// fault fires once.
    fn restart(&mut self, node: usize) {
        if self.nodes[node].refuses_to_open.is_some() {
            return;
        }
        for _ in 0..2 {
            let meta = MetaFencingEpoch::new(OLD_EPOCH);
            let opened = open_replica(node, &self.nodes[node].env, &meta);
            let crashed = self.nodes[node].disk.has_crashed();
            match opened {
                Ok(replica) if !crashed => {
                    if self.granted > OLD_EPOCH {
                        replica.adopt_fencing_epoch(self.granted);
                        meta.set(self.granted);
                    }
                    if self.nodes[node].disk.has_crashed() {
                        self.nodes[node].disk.reboot();
                        continue;
                    }
                    self.note(format!(
                        "{} restarted: next {}, committed {}",
                        NAMES[node],
                        replica.next_offset(),
                        replica.local_committed_offset()
                    ));
                    self.nodes[node].meta = meta;
                    self.nodes[node].replica = Some(replica);
                    return;
                }
                Ok(_) => {
                    self.nodes[node].disk.reboot();
                }
                Err(error) if crashed => {
                    self.note(format!("{} restart crashed: {error}", NAMES[node]));
                    self.nodes[node].disk.reboot();
                }
                Err(error) => {
                    // The open detected damage and refused, which is the
                    // storage layer's deliberate answer to a torn journal
                    // entry (see `epoch_journal_crash_sweep`). The node is
                    // out until an operator repairs it; the other two must
                    // still keep every acknowledged record.
                    self.note(format!(
                        "{} refuses to reopen and stays down: {error}",
                        NAMES[node]
                    ));
                    self.nodes[node].refuses_to_open = Some(error);
                    return;
                }
            }
        }
        self.violations.push(Violation::Liveness(format!(
            "{} could not be restarted",
            NAMES[node]
        )));
    }

    /// The sessions L held with R and C at epoch 18 before the sweep began.
    fn open_setup_sessions(&mut self) {
        let Some(leader_process) = self.up(L) else {
            return;
        };
        for follower in [R, C] {
            self.sessions[follower] = self.up(follower).map(|replica| Session {
                leader: L,
                epoch: OLD_EPOCH,
                leader_process: Arc::clone(&leader_process),
                follower: replica,
            });
        }
    }

    /// Whether `leader` holds a live session with `follower` at `epoch`: the
    /// same leader process and the same follower process that opened it.
    /// Either one restarting drops its connections, so the next send opens
    /// a new, fenced session.
    fn has_session(&self, leader: usize, epoch: u64, follower: usize) -> bool {
        let (Some(replica), Some(leader_process)) = (
            self.nodes[follower].replica.as_ref(),
            self.nodes[leader].replica.as_ref(),
        ) else {
            return false;
        };
        self.sessions[follower].as_ref().is_some_and(|session| {
            session.leader == leader
                && session.epoch == epoch
                && Arc::ptr_eq(&session.leader_process, leader_process)
                && Arc::ptr_eq(&session.follower, replica)
        })
    }

    /// Make sure `leader` has a session with `follower` at `epoch`, opening
    /// one with the fence `NetworkedReplicaSet` sends (#641) if the follower
    /// restarted or the leader changed since the last. False when the
    /// follower is down or refused the fence: nothing is sent to it.
    fn session(&mut self, leader: usize, epoch: u64, follower: usize) -> bool {
        let Some(replica) = self.up(follower) else {
            return false;
        };
        if self.has_session(leader, epoch, follower) {
            return true;
        }
        self.sessions[follower] = None;
        let Some(own) = self.up(leader) else {
            return false;
        };
        let history = own.epoch_starts();
        let answer = replica.fence(epoch, &history);
        let dead = self.nodes[follower].disk.has_crashed();
        let opened = match answer {
            Ok(outcome) if !dead && outcome.fencing_epoch == epoch => {
                let (_, reported) = replica.replication_status();
                self.announced_dispute[follower] = reported < replica.next_offset();
                self.note(format!(
                    "{} opens a session with {} at {epoch}: next {}, reports {reported}, \
                     truncated {}",
                    NAMES[leader], NAMES[follower], outcome.next_offset, outcome.truncated_records
                ));
                true
            }
            Ok(_) => false,
            Err((code, reason)) => {
                self.note(format!(
                    "{} refused {}'s session fence at {epoch}: {code:?} {reason}",
                    NAMES[follower], NAMES[leader]
                ));
                false
            }
        };
        self.after_step(follower);
        if opened && self.still_the_same(follower, &replica) && self.still_the_same(leader, &own) {
            self.sessions[follower] = Some(Session {
                leader,
                epoch,
                leader_process: own,
                follower: replica,
            });
            return true;
        }
        false
    }

    /// Send `request` to follower `node` over the request's leader's
    /// session. True when it acked; an ack is checked against the bytes the
    /// follower durably holds.
    fn send(&mut self, node: usize, request: &ReplicaAppendRequest) -> bool {
        let leader = (request.leader_node_id.as_u128() - 1) as usize;
        if !self.session(leader, request.fencing_epoch, node) {
            return false;
        }
        let Some(replica) = self.up(node) else {
            return false;
        };
        let answer = replica.apply_append(request);
        let crashed = self.nodes[node].disk.has_crashed();
        let end = request.expected_base_offset + request.records.len() as u64;
        let acked = !crashed
            && answer
                .as_ref()
                .is_ok_and(|response| response.local_committed_offset >= end);
        if acked {
            let held = durable_records(&self.nodes[node].disk);
            let offset = request.expected_base_offset;
            let sent = value_of(request);
            let holds = held.get(&offset).cloned();
            if holds.as_deref() != Some(sent.as_str()) {
                self.note(format!(
                    "{} ACKED {sent} at {offset} while holding {holds:?}",
                    NAMES[node]
                ));
                self.violations.push(Violation::AckWithoutTheRecord {
                    node,
                    offset,
                    sent,
                    holds,
                });
            }
        } else if let Err((code, reason)) = &answer {
            self.note(format!(
                "{} refused {} at {}: {code:?} {reason}",
                NAMES[node],
                value_of(request),
                request.expected_base_offset
            ));
        }
        self.after_step(node);
        acked
    }

    /// The leader's write: followers first, then the leader's own copy.
    /// Committed when the leader and one follower hold it durably.
    fn replicate(
        &mut self,
        leader: usize,
        request: &ReplicaAppendRequest,
        followers: &[usize],
    ) -> bool {
        let Some(own) = self.up(leader) else {
            return false;
        };
        let mut follower_acks = 0;
        for follower in followers {
            if self.send(*follower, request) {
                follower_acks += 1;
            }
        }
        if !self.still_the_same(leader, &own) {
            return false;
        }
        let end = request.expected_base_offset + request.records.len() as u64;
        let answer = own.apply_append(request);
        let local = !self.nodes[leader].disk.has_crashed()
            && answer
                .as_ref()
                .is_ok_and(|response| response.local_committed_offset >= end);
        if let Err((code, reason)) = &answer {
            self.note(format!(
                "leader {} refused its own {}: {code:?} {reason}",
                NAMES[leader],
                value_of(request)
            ));
        }
        self.after_step(leader);
        local && follower_acks >= 1 && self.still_the_same(leader, &own)
    }

    fn still_the_same(&self, node: usize, replica: &Arc<InProcessFollower>) -> bool {
        self.nodes[node]
            .replica
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, replica))
    }

    /// Record that `value` at `offset` was acknowledged or published. The
    /// first acknowledgement of an offset is the oracle and is never
    /// replaced (#646 review): different bytes acknowledged there later mean
    /// the first record was lost, and comparing the final leader against
    /// the replacement would hide exactly that.
    fn acknowledge(&mut self, offset: u64, value: String) {
        match self.acknowledged.get(&offset) {
            None => {
                self.acknowledged.insert(offset, value);
            }
            Some(first) if *first == value => {}
            Some(first) => {
                let first = first.clone();
                let already = self.violations.iter().any(|violation| {
                    matches!(
                        violation,
                        Violation::AcknowledgedRecordReplaced { offset: at, replaced_by, .. }
                            if *at == offset && *replaced_by == value
                    )
                });
                if already {
                    return;
                }
                self.note(format!(
                    "LOSS: {value} acknowledged at {offset}, where {first} was acknowledged"
                ));
                self.violations.push(Violation::AcknowledgedRecordReplaced {
                    offset,
                    acknowledged: first,
                    replaced_by: value,
                });
            }
        }
    }

    /// Publish `mark` to `to` (the leader included) at `epoch`.
    fn publish_mark(&mut self, leader: usize, epoch: u64, mark: u64, to: &[usize]) {
        let leader_log = durable_records(&self.nodes[leader].disk);
        for offset in 0..mark {
            if let Some(value) = leader_log.get(&offset) {
                self.acknowledge(offset, value.clone());
            }
        }
        for node in to {
            // A mark reaches a follower only over a session (#641).
            let fresh = *node != leader && !self.has_session(leader, epoch, *node);
            if *node != leader && !self.session(leader, epoch, *node) {
                continue;
            }
            let Some(replica) = self.up(*node) else {
                continue;
            };
            if let Err((code, reason)) = replica.observe_hwm(&CommittedHwmUpdate {
                range: range(),
                fencing_epoch: epoch,
                committed_high_watermark: mark,
            }) {
                self.note(format!(
                    "{} refused the mark {mark}: {code:?} {reason}",
                    NAMES[*node]
                ));
            }
            self.after_step(*node);
            // A session the mark opened goes on as the driver's does: it
            // sends the mark first, then retransmits from where the
            // follower's status said it ends.
            if fresh {
                self.catch_up(leader, epoch, *node);
            }
        }
    }

    /// Step 2: L appends 5..10, R first, C partitioned away.
    fn leader_appends_the_disputed_suffix(&mut self) {
        let Some(leader) = self.up(L) else {
            return;
        };
        if self.shape == Shape::UnackedOutvoted {
            // L's own copies stay in its page cache: they die with it.
            leader.set_hold_fsync(true);
        }
        for offset in DISPUTED {
            if !self.still_the_same(L, &leader) {
                self.note("L is gone; nothing more is appended under epoch 18".to_owned());
                return;
            }
            let request = self.new_record(format!("e{OLD_EPOCH}-o{offset}"), OLD_EPOCH, L, offset);
            if self.replicate(L, &request, &[R]) {
                self.note(format!("acknowledged {} at {offset}", value_of(&request)));
                self.acknowledge(offset, value_of(&request));
            }
        }
    }

    /// Metadata grants `epoch` to `candidate`; every other up node's lease
    /// watcher adopts it.
    fn grant(&mut self, candidate: usize, epoch: u64) {
        self.granted = epoch;
        self.note(format!(
            "metadata grants epoch {epoch} to {}",
            NAMES[candidate]
        ));
        for node in 0..3 {
            let Some(replica) = self.up(node) else {
                continue;
            };
            if node != candidate {
                replica.adopt_fencing_epoch(epoch);
            }
            self.nodes[node].meta.set(epoch);
            self.after_step(node);
        }
    }

    /// One election: fence, probe, judge and vote, as `ReplicaPlaneProbe`
    /// and `establish` do.
    fn elect(&mut self, candidate: usize, epoch: u64) -> Option<Promotion> {
        self.grant(candidate, epoch);
        let own = self.up(candidate)?;
        let vote = own.fence_locally(epoch).ok();
        let lineage = own.epoch_starts();
        let candidate_tail = match vote {
            Some(committed) => own.next_offset().max(committed),
            None => u64::MAX,
        };
        self.after_step(candidate);
        if !self.still_the_same(candidate, &own) {
            return None;
        }
        let mut probes = vec![ReplicaProbe {
            node_id: node_id(candidate),
            local_committed_offset: vote,
            lineage: if lineage.is_empty() && vote != Some(0) {
                AnswerLineage::Unknown
            } else {
                AnswerLineage::Agreed
            },
        }];
        for node in 0..3 {
            if node == candidate {
                continue;
            }
            let Some(replica) = self.up(node) else {
                probes.push(ReplicaProbe {
                    node_id: node_id(node),
                    local_committed_offset: None,
                    lineage: AnswerLineage::Unknown,
                });
                continue;
            };
            let answer = replica.fence(epoch, &lineage);
            // A process that died during the fence sent no reply.
            let dead = self.nodes[node].disk.has_crashed();
            let probe = match answer {
                Ok(outcome) if !dead => {
                    let verdict = fenced_lineage(
                        &lineage,
                        &outcome.epoch_starts,
                        outcome.next_offset,
                        outcome.truncated_records,
                        candidate_tail,
                    );
                    // The reply leaves the process here. Only a dispute a
                    // reply announced can be forgotten in #635's sense; one
                    // installed by a fence that died before replying leaves
                    // the replica absent from the election, and the next
                    // leader's session fences it (#641).
                    let (_, reported) = replica.replication_status();
                    self.announced_dispute[node] = reported < replica.next_offset();
                    self.note(format!(
                        "{} fenced at {epoch}: committed {}, next {}, truncated {}, {verdict:?}",
                        NAMES[node],
                        outcome.local_committed_offset,
                        outcome.next_offset,
                        outcome.truncated_records
                    ));
                    ReplicaProbe {
                        node_id: node_id(node),
                        local_committed_offset: Some(outcome.local_committed_offset),
                        lineage: verdict,
                    }
                }
                Ok(_) => ReplicaProbe {
                    node_id: node_id(node),
                    local_committed_offset: None,
                    lineage: AnswerLineage::Unknown,
                },
                Err((code, reason)) => {
                    self.note(format!(
                        "{} refused the fence at {epoch}: {code:?} {reason}",
                        NAMES[node]
                    ));
                    ReplicaProbe {
                        node_id: node_id(node),
                        local_committed_offset: None,
                        lineage: AnswerLineage::Unknown,
                    }
                }
            };
            probes.push(probe);
            self.after_step(node);
        }
        if !self.still_the_same(candidate, &own) {
            return None;
        }
        let promotion = establish(
            &probes,
            REPLICATION_FACTOR,
            node_id(candidate),
            Some(self.published),
        );
        self.note(format!(
            "{} at epoch {epoch} (own vote {vote:?}): {promotion:?}",
            NAMES[candidate]
        ));
        if self.first_promotion.is_none() {
            self.first_promotion = Some((candidate, promotion.clone()));
        }
        self.last_promotion = Some(promotion.clone());
        Some(promotion)
    }

    /// Bring `follower` up to the leader's log, from where its reconnect
    /// status says it ends.
    fn catch_up(&mut self, leader: usize, epoch: u64, follower: usize) -> bool {
        if self.nodes[follower].refuses_to_open.is_some() {
            return true;
        }
        if !self.session(leader, epoch, follower) {
            return false;
        }
        let Some(replica) = self.up(follower) else {
            return false;
        };
        let (_, from) = replica.replication_status();
        let leader_log = durable_records(&self.nodes[leader].disk);
        let tail = leader_log.keys().next_back().map_or(0, |last| last + 1);
        for offset in from..tail {
            let value = leader_log
                .get(&offset)
                .expect("a leader's durable log has no holes")
                .clone();
            let mut request = self.templates[&value].clone();
            request.fencing_epoch = epoch;
            request.leader_node_id = node_id(leader);
            request.expected_base_offset = offset;
            if !self.send(follower, &request) {
                return false;
            }
        }
        true
    }

    /// A new record from `leader`, replicated to every up follower once each
    /// is caught up. Returns whether it committed and was published.
    fn produce(&mut self, leader: usize, epoch: u64, value: String) -> bool {
        let Some(own) = self.up(leader) else {
            return false;
        };
        let followers: Vec<usize> = (0..3).filter(|node| *node != leader).collect();
        for follower in &followers {
            self.catch_up(leader, epoch, *follower);
        }
        if !self.still_the_same(leader, &own) {
            return false;
        }
        let offset = own.next_offset();
        let request = self.new_record(value, epoch, leader, offset);
        let up: Vec<usize> = followers
            .iter()
            .copied()
            .filter(|node| self.nodes[*node].replica.is_some())
            .collect();
        if !self.replicate(leader, &request, &up) {
            self.note(format!("{} did not commit", value_of(&request)));
            return false;
        }
        self.note(format!("acknowledged {} at {offset}", value_of(&request)));
        self.acknowledge(offset, value_of(&request));
        self.publish_mark(leader, epoch, offset + 1, &[0, 1, 2]);
        self.still_the_same(leader, &own)
    }

    /// The established leader's first writes: its marker, then one new
    /// record. False when it lost leadership or could not commit.
    fn lead(&mut self, leader: usize, epoch: u64) -> bool {
        // Leadership belongs to this process. A leader that restarted is a
        // follower until an election says otherwise.
        let Some(own) = self.up(leader) else {
            return false;
        };
        for attempt in 0..3 {
            if self.produce(leader, epoch, format!("marker-e{epoch}-{attempt}")) {
                return self.produce(leader, epoch, format!("post-e{epoch}"))
                    && self.still_the_same(leader, &own);
            }
            if !self.still_the_same(leader, &own) {
                return false;
            }
        }
        false
    }

    /// An orderly reopen of `node`'s directory: commit, drop, open.
    /// True when the node came out of the flip still holding the role: the
    /// same process reopened its directory. A death or crash during the flip
    /// restarts it as a follower, and leadership then needs a new election
    /// (#646 review).
    fn role_flip(&mut self, node: usize) -> bool {
        let Some(replica) = self.up(node) else {
            return false;
        };
        self.note(format!(
            "{} reopens its directory for the role flip",
            NAMES[node]
        ));
        if let Err(error) = replica.quiesce() {
            self.note(format!("{} pre-flip commit failed: {error}", NAMES[node]));
        }
        // The step is counted while the process still holds its replica, so
        // a death scheduled here fires (#646 review: taken first, the
        // replica was already gone and the kill was skipped). A death or a
        // crash restarts the node from its directory as a follower: the
        // process that won the election is gone, and with it the role.
        self.after_step(node);
        if !self.still_the_same(node, &replica) {
            self.note(format!(
                "{} died during the role flip; its leadership died with it",
                NAMES[node]
            ));
            return false;
        }
        self.nodes[node].replica = None;
        drop(replica);
        self.restart(node);
        self.nodes[node].replica.is_some()
    }

    /// Restart the nodes the scenario held down. True if there were any.
    fn release_held_down(&mut self) -> bool {
        let mut released = false;
        for node in 0..3 {
            if self.nodes[node].held_down {
                self.nodes[node].held_down = false;
                self.nodes[node].disk.reboot();
                self.restart(node);
                released = true;
            }
        }
        released
    }

    fn run(mut self, first_op: [u64; 3]) -> Outcome {
        // Step 2.
        self.leader_appends_the_disputed_suffix();
        // Step 3: L dies before R learns the mark moved past 5.
        if self.nodes[L].replica.is_some() {
            self.note("L dies before R learns the mark".to_owned());
            self.kill(L);
            self.nodes[L].disk.reboot();
        }
        self.nodes[L].replica = None;
        if self.shape == Shape::AckedLeaderStaysDown {
            self.nodes[L].held_down = true;
        } else {
            self.restart(L);
        }

        // Steps 4-7.
        let mut final_leader = None;
        let mut round = 0;
        while self.elections < MAX_ELECTIONS {
            let candidate = CANDIDATE_ORDER[round % 3];
            round += 1;
            if self.nodes[candidate].replica.is_none() {
                if (0..3).all(|node| self.nodes[node].replica.is_none())
                    && !self.release_held_down()
                {
                    break;
                }
                continue;
            }
            self.elections += 1;
            let epoch = self.granted + 1;
            let Some(promotion) = self.elect(candidate, epoch) else {
                continue;
            };
            let Promotion::Established {
                committed_offset, ..
            } = promotion
            else {
                // Two refusals in a row with L held down: let it back.
                if self.elections >= 3 {
                    self.release_held_down();
                }
                continue;
            };
            self.published = self.published.max(committed_offset);
            // The role flip `data_node` performs: the deposed leader and the
            // new one each reopen their directory, so nothing held in memory
            // (a kept dispute included) crosses the flip.
            if let Some(deposed) = self.leader.take() {
                if deposed != candidate {
                    self.role_flip(deposed);
                }
            }
            if !self.role_flip(candidate) {
                continue;
            }
            let Some(leading) = self.up(candidate) else {
                continue;
            };
            self.leader = Some(candidate);
            if !self.lead(candidate, epoch) {
                continue;
            }
            // Step 7: everyone comes back, catches up and learns the final
            // mark. Retried, since a fault can land here too.
            self.release_held_down();
            for _ in 0..3 {
                for node in 0..3 {
                    if self.nodes[node].replica.is_none() && !self.nodes[node].held_down {
                        self.nodes[node].disk.reboot();
                        self.restart(node);
                    }
                }
                let own = Arc::clone(&leading);
                if !self.still_the_same(candidate, &own) {
                    break;
                }
                let mut finished = true;
                for follower in (0..3).filter(|node| *node != candidate) {
                    finished &= self.catch_up(candidate, epoch, follower);
                }
                if !self.still_the_same(candidate, &own) {
                    break;
                }
                if finished {
                    let mark = own.next_offset();
                    self.publish_mark(candidate, epoch, mark, &[0, 1, 2]);
                    if self.still_the_same(candidate, &own) {
                        final_leader = Some(candidate);
                    }
                    break;
                }
            }
            if final_leader.is_some() {
                break;
            }
        }

        match final_leader {
            Some(leader) => self.check(leader),
            None => self.violations.push(Violation::Liveness(format!(
                "no leader finished within {MAX_ELECTIONS} elections"
            ))),
        }
        if final_leader.is_none() {
            self.check_majority_durability();
        }
        Outcome {
            violations: self.violations,
            last_promotion: self.last_promotion,
            forgotten_disputes: self.forgotten_disputes,
            log: self.log,
            traces: self.nodes.iter().map(|node| node.disk.trace()).collect(),
            first_op,
            steps: self.steps,
            first_promotion: self.first_promotion,
            final_leader,
            acknowledged: self.acknowledged,
            refused_to_open: self
                .nodes
                .iter()
                .enumerate()
                .filter_map(|(node, state)| state.refuses_to_open.clone().map(|why| (node, why)))
                .collect(),
        }
    }

    /// With no leader to read back from, the safety invariant is that every
    /// acknowledged record is still durable, with its bytes, on a majority.
    fn check_majority_durability(&mut self) {
        let logs: Vec<BTreeMap<u64, String>> = (0..3)
            .map(|node| durable_records(&self.nodes[node].disk))
            .collect();
        for (offset, expected) in &self.acknowledged {
            let holders = logs
                .iter()
                .filter(|log| log.get(offset) == Some(expected))
                .count();
            if holders < REPLICATION_FACTOR / 2 + 1 {
                self.violations.push(Violation::AckedRecordLost {
                    offset: *offset,
                    expected: expected.clone(),
                    found: None,
                });
            }
        }
    }

    /// The safety invariants, read back from each disk.
    fn check(&mut self, leader: usize) {
        let leader_log = durable_records(&self.nodes[leader].disk);
        for (offset, expected) in &self.acknowledged {
            let found = leader_log.get(offset).cloned();
            if found.as_deref() != Some(expected.as_str()) {
                self.violations.push(Violation::AckedRecordLost {
                    offset: *offset,
                    expected: expected.clone(),
                    found,
                });
            }
        }
        let mark = self
            .up(leader)
            .map_or(0, |replica| replica.cluster_committed().get());
        for node in (0..3).filter(|node| *node != leader) {
            if self.nodes[node].refuses_to_open.is_some() {
                continue;
            }
            let held = durable_records(&self.nodes[node].disk);
            for offset in 0..mark {
                let holds = held.get(&offset).cloned();
                let expected = leader_log.get(&offset).cloned().unwrap_or_default();
                if holds.as_deref() != Some(expected.as_str()) {
                    self.violations.push(Violation::DivergedBelowMark {
                        node,
                        offset,
                        leader: expected,
                        holds,
                    });
                    break;
                }
            }
        }
    }
}

/// Run one shape under one fault, from the setup disks.
fn run(shape: Shape, fault: Fault, setup: &[SimDiskState]) -> Outcome {
    let mut nodes = Vec::new();
    for disk_state in setup {
        let disk = SimStorage::new();
        disk.restore(disk_state);
        nodes.push(Node {
            env: disk.env(SEED),
            disk,
            meta: MetaFencingEpoch::new(OLD_EPOCH),
            replica: None,
            held_down: false,
            refuses_to_open: None,
        });
    }
    let mut cluster = Cluster {
        shape,
        fault: Fault::None,
        nodes,
        granted: OLD_EPOCH,
        published: 0,
        leader: None,
        steps: 0,
        elections: 0,
        acknowledged: (0..SHARED)
            .map(|offset| (offset, format!("e{OLD_EPOCH}-o{offset}")))
            .collect(),
        templates: HashMap::new(),
        next_producer: 1_000,
        violations: Vec::new(),
        forgotten_disputes: Vec::new(),
        log: Vec::new(),
        first_promotion: None,
        last_promotion: None,
        announced_dispute: [false; 3],
        sessions: [None, None, None],
    };
    // The shared records' requests, for resending them in a catch-up.
    for offset in 0..SHARED {
        cluster.new_record(format!("e{OLD_EPOCH}-o{offset}"), OLD_EPOCH, L, offset);
    }
    let mut first_op = [0; 3];
    for node in 0..3 {
        let opened = open_replica(node, &cluster.nodes[node].env, &cluster.nodes[node].meta)
            .expect("a setup disk opens");
        cluster.nodes[node].replica = Some(opened);
        first_op[node] = cluster.nodes[node].disk.op_count();
    }
    cluster.open_setup_sessions();
    if let Fault::Storage { node, plan } = fault {
        cluster.nodes[node].disk.set_fault(plan);
    }
    cluster.fault = fault;
    cluster.run(first_op)
}

/// Every fault the sweep runs for `shape`, derived from its clean run.
fn faults_for(clean: &Outcome) -> Vec<Fault> {
    let mut faults = Vec::new();
    for node in 0..3 {
        for entry in clean.traces[node]
            .iter()
            .filter(|entry| entry.index >= clean.first_op[node])
        {
            faults.push(Fault::Storage {
                node,
                plan: FaultPlan::CrashBefore(entry.index),
            });
            if entry.kind == TraceKind::HandleWrite && entry.len > 0 {
                let len = entry.len as usize;
                let mut cuts = vec![1, len / 2, len - 1];
                cuts.sort_unstable();
                cuts.dedup();
                for byte_cut in cuts.into_iter().filter(|cut| *cut > 0 && *cut < len) {
                    faults.push(Fault::Storage {
                        node,
                        plan: FaultPlan::CrashDuringWrite {
                            op: entry.index,
                            byte_cut,
                        },
                    });
                }
            }
        }
    }
    for step in 1..=clean.steps {
        for node in 0..3 {
            faults.push(Fault::ProcessDeath { node, step });
        }
    }
    faults
}

/// What the sweep of one shape found.
struct Sweep {
    runs: usize,
    passed: usize,
    /// Passed with a node out because its open refused after the crash.
    passed_with_a_node_refusing_to_open: Vec<String>,
    /// Failures matching a known issue: the issue numbers, then the case.
    known: Vec<(BTreeSet<u32>, String)>,
    new_failures: Vec<(String, String)>,
}

fn issue_title(issue: u32) -> &'static str {
    match issue {
        635 => "a kept dispute is forgotten on restart",
        640 => "no provable quorum after a catch-up",
        _ => "unknown",
    }
}

fn issue_labels(issues: &BTreeSet<u32>) -> String {
    issues
        .iter()
        .map(|issue| format!("#{issue}, {}", issue_title(*issue)))
        .collect::<Vec<_>>()
        .join("; ")
}

/// One line per violation, for the summary.
fn summary(outcome: &Outcome) -> String {
    outcome
        .violations
        .iter()
        .map(|violation| match violation {
            Violation::AckedRecordLost {
                offset,
                expected,
                found,
            } => format!("acked {expected} at {offset} lost (final leader holds {found:?})"),
            Violation::AcknowledgedRecordReplaced {
                offset,
                acknowledged,
                replaced_by,
            } => format!("acked {acknowledged} at {offset} replaced by {replaced_by}"),
            Violation::AckWithoutTheRecord {
                node,
                offset,
                sent,
                holds,
            } => format!(
                "{} acked {sent} at {offset} holding {holds:?}",
                NAMES[*node]
            ),
            Violation::DivergedBelowMark {
                node,
                offset,
                leader,
                holds,
            } => format!(
                "{} holds {holds:?} at {offset} below the mark, leader {leader}",
                NAMES[*node]
            ),
            Violation::Liveness(why)
                if outcome.log.iter().any(|line| {
                    line.contains("kept in dispute") && line.contains("below its high-water mark")
                }) =>
            {
                format!(
                    "liveness: {why}; a replica refused every append over a dispute it kept \
                     below its own high-water mark"
                )
            }
            Violation::Liveness(why) => format!("liveness: {why}"),
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn sweep(shape: Shape, setup: &[SimDiskState]) -> Sweep {
    let clean = run(shape, Fault::None, setup);
    assert!(
        clean.violations.is_empty(),
        "the clean run of {shape:?} must pass before faults are swept:\n{}",
        clean.report(shape, &Fault::None)
    );
    let mut result = Sweep {
        runs: 0,
        passed: 0,
        passed_with_a_node_refusing_to_open: Vec::new(),
        known: Vec::new(),
        new_failures: Vec::new(),
    };
    for fault in faults_for(&clean) {
        let outcome = run(shape, fault, setup);
        result.runs += 1;
        let known = outcome.known_issues();
        if std::env::var_os("VTOP_SWEEP_VERBOSE").is_some() {
            let verdict = match (&known, outcome.violations.is_empty()) {
                (_, true) => "pass".to_owned(),
                (Some(issues), false) => {
                    format!("{}: {}", issue_labels(issues), summary(&outcome))
                }
                (None, false) => format!("NEW: {}", summary(&outcome)),
            };
            eprintln!("RESULT {shape:?} | {fault} | {verdict}");
        }
        if outcome.violations.is_empty() {
            result.passed += 1;
            for (node, why) in &outcome.refused_to_open {
                result
                    .passed_with_a_node_refusing_to_open
                    .push(format!("{fault}: {} stayed down: {why}", NAMES[*node]));
            }
        } else if let Some(issues) = known {
            result
                .known
                .push((issues, format!("{fault}: {}", summary(&outcome))));
        } else {
            result.new_failures.push((
                format!("{fault}: {}", summary(&outcome)),
                outcome.report(shape, &fault),
            ));
        }
    }
    result
}

fn assert_sweep(shape: Shape) {
    let setup = setup_disks();
    let result = sweep(shape, &setup);
    let count = |issue: u32| {
        result
            .known
            .iter()
            .filter(|(issues, _)| issues.contains(&issue))
            .count()
    };
    eprintln!(
        "{shape:?}: {} runs, {} passed ({} of them with a node whose open refused after the \
         crash), expected failures: {} #635, {} #640; {} new failures",
        result.runs,
        result.passed,
        result.passed_with_a_node_refusing_to_open.len(),
        count(635),
        count(640),
        result.new_failures.len()
    );
    for case in &result.passed_with_a_node_refusing_to_open {
        eprintln!("  passed, node out: {case}");
    }
    for (issues, case) in &result.known {
        eprintln!("  EXPECTED FAILURE ({}): {case}", issue_labels(issues));
    }
    for (case, _) in &result.new_failures {
        eprintln!("  NEW FAILURE: {case}");
    }
    assert!(
        result.new_failures.is_empty(),
        "{} of {} crash points lost an acknowledged record, acked a record a replica \
         does not hold, or never finished, for a reason #635 and #640 do not explain. \
         First:\n{}",
        result.new_failures.len(),
        result.runs,
        result.new_failures[0].1
    );
}

/// The issue's own sequence with no fault: C is refused because R kept
/// 5..10 and answered 10, R is established, and nothing acknowledged is
/// lost. Before #616, R truncated 5..10 at the fence and C was established
/// at 5, losing five acknowledged records.
#[test]
fn the_611_sequence_without_a_fault_loses_no_acknowledged_record() {
    let setup = setup_disks();
    for shape in SHAPES {
        let outcome = run(shape, Fault::None, &setup);
        assert!(
            outcome.violations.is_empty(),
            "{}",
            outcome.report(shape, &Fault::None)
        );
        let (candidate, promotion) = outcome.first_promotion.clone().expect("an election ran");
        assert_eq!(candidate, C, "metadata grants C first");
        match shape {
            Shape::AckedLeaderStaysDown | Shape::AckedLeaderReturns => {
                assert!(
                    DISPUTED
                        .clone()
                        .all(|offset| outcome.acknowledged.contains_key(&offset)),
                    "precondition: 5..10 were acknowledged\n{}",
                    outcome.report(shape, &Fault::None)
                );
                // Refused by R's answer alone (L down), or by L's and R's
                // together, which put the floor itself above C.
                let refused = match shape {
                    Shape::AckedLeaderStaysDown => matches!(
                        promotion,
                        Promotion::CandidateBehindVoters {
                            candidate_offset: SHARED,
                            votes: 1,
                            ..
                        }
                    ),
                    _ => matches!(
                        promotion,
                        Promotion::LeaderBehind {
                            leader_committed_offset: Some(SHARED),
                            ..
                        }
                    ),
                };
                assert!(
                    refused,
                    "C lacks acknowledged 5..10 and must be refused: {promotion:?}\n{}",
                    outcome.report(shape, &Fault::None)
                );
                assert_ne!(outcome.final_leader, Some(C));
            }
            Shape::UnackedOutvoted => {
                assert!(
                    !DISPUTED.clone().any(|offset| outcome
                        .acknowledged
                        .get(&offset)
                        .is_some_and(|value| value.starts_with(&format!("e{OLD_EPOCH}-")))),
                    "precondition: nothing past 5 was acknowledged under epoch 18"
                );
                assert!(
                    matches!(
                        promotion,
                        Promotion::Established {
                            committed_offset: SHARED,
                            ..
                        }
                    ),
                    "C and L outvote R: {promotion:?}\n{}",
                    outcome.report(shape, &Fault::None)
                );
                assert_eq!(outcome.final_leader, Some(C));
            }
        }
    }
}

#[test]
fn a_crash_at_every_step_of_the_611_sequence_loses_no_acknowledged_record() {
    assert_sweep(Shape::AckedLeaderStaysDown);
}

#[test]
fn a_crash_at_every_step_with_the_deposed_leader_back_loses_no_acknowledged_record() {
    assert_sweep(Shape::AckedLeaderReturns);
}

#[test]
fn a_crash_at_every_step_of_an_outvoted_dispute_loses_no_acknowledged_record() {
    assert_sweep(Shape::UnackedOutvoted);
}

/// Was the first #640 reproduction; passes since #616's 786fb07 and stays
/// as a guard. The established leader R dies after it caught C up on 5..10
/// and committed its marker. C files 5..10 under the epoch it adopted at its
/// own tail, R under 18, so they still disagree about who wrote them (#640),
/// but L restarts, agrees with R, and R is established again with L. Before
/// 786fb07, C kept the "disputed" 5..12 below its own high-water mark and
/// refused every append from the next leader, so no leader ever finished.
#[test]
fn a_second_failover_after_a_catch_up_still_finishes_with_two_agreeing_replicas() {
    let setup = setup_disks();
    let fault = Fault::ProcessDeath { node: R, step: 30 };
    let outcome = run(Shape::AckedLeaderStaysDown, fault, &setup);
    assert!(
        outcome.violations.is_empty(),
        "{}",
        outcome.report(Shape::AckedLeaderStaysDown, &fault)
    );
}

/// The index of `node`'s `ordinal`-th (from 1) `kind` operation on `file`
/// in the clean run of `shape`. Reproductions name a crash point this way,
/// not by bare index, so a change that adds a storage operation elsewhere
/// (#640 added the lineage sidecar's probe at open) moves the index without
/// moving the crash.
fn nth_op(
    shape: Shape,
    setup: &[SimDiskState],
    node: usize,
    kind: TraceKind,
    file: &str,
    ordinal: usize,
) -> u64 {
    let clean = run(shape, Fault::None, setup);
    clean.traces[node]
        .iter()
        .filter(|entry| entry.kind == kind && entry.path == Path::new(file))
        .nth(ordinal - 1)
        .unwrap_or_else(|| {
            panic!(
                "the clean {shape:?} run has no {kind:?} #{ordinal} on {}'s {file}",
                NAMES[node]
            )
        })
        .index
}

/// #640. R leads at 20 after catching C up on 5..10, and L, back before the
/// election, has caught up too. R's producer-epoch journal entry is torn
/// as it takes its marker, so R refuses to reopen and stays out (that
/// journal's designed answer to a torn entry). L and C hold identical
/// bytes, but each filed the records it caught up on under the epoch it
/// adopted at its own tail, so each judges the other's answer Unknown and
/// neither is ever established. The range is unavailable with two healthy
/// replicas.
#[test]
#[ignore = "#640: caught-up records are filed under the adopted epoch, so two healthy replicas cannot prove a quorum"]
fn reproduction_two_caught_up_replicas_cannot_prove_a_quorum_after_the_leader_is_lost() {
    let setup = setup_disks();
    let op = nth_op(
        Shape::AckedLeaderReturns,
        &setup,
        R,
        TraceKind::HandleWrite,
        PRODUCER_EPOCHS,
        6,
    );
    let fault = Fault::Storage {
        node: R,
        plan: FaultPlan::CrashDuringWrite { op, byte_cut: 28 },
    };
    let outcome = run(Shape::AckedLeaderReturns, fault, &setup);
    assert!(
        outcome.violations.is_empty(),
        "{}",
        outcome.report(Shape::AckedLeaderReturns, &fault)
    );
}

/// #641, found by the sweep: the "absent replica" gap `promotion.rs` listed.
/// R's disk dies before it acks L's first record of 5..10, so L holds 5..10
/// unacknowledged. L dies, C and R establish C at 5, and L returns without
/// having been fenced by that election. It used to report its full tail and
/// keep its epoch-18 records under the new leader's published mark (the
/// leader's appends there would be acked as duplicates, #261). C's session
/// with L now opens with C's fence: L keeps 5..10 in dispute, reports 5, and
/// discards them on C's first record there, then catches up.
#[test]
fn reproduction_a_returning_deposed_leader_keeps_its_unacknowledged_suffix_under_the_mark() {
    let setup = setup_disks();
    // R's second length probe of its producer-epoch journal (op 67 when the
    // sweep found it), named by kind, file and ordinal so a storage
    // operation added elsewhere cannot move the crash.
    let op = nth_op(
        Shape::AckedLeaderStaysDown,
        &setup,
        R,
        TraceKind::HandleLen,
        PRODUCER_EPOCHS,
        2,
    );
    let fault = Fault::Storage {
        node: R,
        plan: FaultPlan::CrashBefore(op),
    };
    let outcome = run(Shape::AckedLeaderStaysDown, fault, &setup);
    assert!(
        outcome.violations.is_empty(),
        "{}",
        outcome.report(Shape::AckedLeaderStaysDown, &fault)
    );
}

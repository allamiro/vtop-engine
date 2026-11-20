//! Applied audit records are delivered at least once (#706).
//!
//! System model: one metadata replica, crash-recovery (a crash drops
//! everything not synced), an audit sink that can refuse a record and later
//! accept it. The Raft adapter is driven directly - entries are appended
//! through the store, then applied through the state machine - so a crash
//! can land between any two storage operations of an apply.
//!
//! Invariants (safety unless marked):
//! - every audited entry at or below the applied frontier is delivered at
//!   least once, or named by an `audit.gap` record;
//! - nothing at or below the persisted audit frontier is delivered again;
//! - the audit frontier never passes the applied frontier;
//! - (liveness) a sink that recovers gets every refused record, marked
//!   `replayed`.

use openraft::storage::RaftStateMachine;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, SnapshotMeta, StoredMembership};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use vtop_log::env::Env;
use vtop_log::sim::{FaultPlan, SimStorage};
use vtop_meta::audit::{AuditAction, AuditOutcome, AuditRecord, Auditor};
use vtop_meta::raft::{MetaRaftStateMachine, MetaRaftStore, MetaRaftTypeConfig, ReplayReason};
use vtop_meta::{
    CommandEnvelope, MetaLogEntry, MetaLogPayload, MetaMembership, MetaNodeId, MetaStorage,
    MetadataCommand,
};

const NODE_ID: u64 = 2;

fn cluster() -> Uuid {
    Uuid::from_u128(0x0706_a0d1)
}

fn envelope(request: u128) -> CommandEnvelope {
    CommandEnvelope {
        request_id: Uuid::from_u128(request),
        issued_at_ms: 1_750_000_000_000 + request as i64,
    }
}

/// An audited operator command (`RegisterNode`) or an unaudited,
/// node-initiated one (`ReportNodeVersion`).
fn entry(term: u64, index: u64, audited: bool) -> MetaLogEntry {
    let seed = u128::from(index) + 0x700;
    let command = if audited {
        MetadataCommand::RegisterNode {
            env: envelope(seed),
            node_uuid: Uuid::from_u128(seed),
            addr: format!("10.0.0.{index}:9200"),
            expected_generation: None,
        }
    } else {
        // Node-initiated: never audited (`command_audit` is `None`).
        MetadataCommand::ReportNodeVersion {
            env: envelope(seed),
            node_uuid: Uuid::from_u128(seed),
            release: "0.10.0".to_owned(),
            max_replica_protocol: 2,
            supported_feature_level: 8,
        }
    };
    MetaLogEntry {
        term,
        index,
        payload: MetaLogPayload::Normal(command),
    }
}

/// Entries 1..=n: every third one is not audited.
fn workload(n: u64) -> Vec<MetaLogEntry> {
    (1..=n)
        .map(|index| entry(1, index, index % 3 != 0))
        .collect()
}

fn audited_indexes(entries: &[MetaLogEntry]) -> BTreeSet<u64> {
    debug_assert!(
        entries.iter().any(|entry| matches!(
            &entry.payload,
            MetaLogPayload::Normal(command) if vtop_meta::audit::command_audit(command).is_none()
        )) || entries.len() < 3
    );
    entries
        .iter()
        .filter(|entry| match &entry.payload {
            MetaLogPayload::Normal(command) => vtop_meta::audit::command_audit(command).is_some(),
            _ => true,
        })
        .map(|entry| entry.index)
        .collect()
}

fn raft_entry(entry: &MetaLogEntry) -> Entry<MetaRaftTypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(entry.term, 0), entry.index - 1),
        payload: match &entry.payload {
            MetaLogPayload::Normal(command) => EntryPayload::Normal(command.clone()),
            MetaLogPayload::Blank => EntryPayload::Blank,
            MetaLogPayload::Membership(_) => unreachable!("not in these workloads"),
        },
    }
}

/// A sink that can be told to refuse, and keeps what it accepted.
#[derive(Default)]
struct Sink {
    refusing: AtomicBool,
    accepted: Mutex<Vec<AuditRecord>>,
}

impl Sink {
    fn refusing() -> Arc<Self> {
        let sink = Arc::new(Sink::default());
        sink.refusing.store(true, Ordering::SeqCst);
        sink
    }

    fn accepted(&self) -> Vec<AuditRecord> {
        self.accepted.lock().unwrap().clone()
    }

    /// Applied records by log index (gap records have none).
    fn indexes(&self) -> Vec<u64> {
        self.accepted().iter().filter_map(|r| r.log_index).collect()
    }

    fn gaps(&self) -> Vec<AuditRecord> {
        self.accepted()
            .into_iter()
            .filter(|r| r.action == AuditAction::AuditGap)
            .collect()
    }
}

impl Auditor for Sink {
    fn record(&self, record: &AuditRecord) -> Result<(), String> {
        if self.refusing.load(Ordering::SeqCst) {
            return Err("the audit file is full".to_owned());
        }
        self.accepted.lock().unwrap().push(record.clone());
        Ok(())
    }
}

fn open(env: &Env, sink: &Arc<Sink>) -> (MetaRaftStore, MetaRaftStateMachine) {
    let store = MetaRaftStore::open_in(env, "/n", cluster()).unwrap();
    store.set_audit_sink(sink.clone());
    let machine = MetaRaftStateMachine::new(store.clone()).with_node_id(NODE_ID);
    (store, machine)
}

/// Append `entries` durably, as the log store does before they commit.
fn append(env: &Env, entries: &[MetaLogEntry]) {
    let mut storage = MetaStorage::open_in(env, "/n", cluster()).unwrap();
    // Pinned as the Raft adapter pins it, so the appended entries are not
    // read as committed at the next open (legacy full-log replay).
    storage.ensure_raft_applied_frontier().unwrap();
    storage.append(entries).unwrap();
}

fn sim() -> (SimStorage, Env) {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new("/n"));
    let env = sim.env(0x706);
    (sim, env)
}

fn audited_frontier(store: &MetaRaftStore) -> Option<u64> {
    store.with_storage(MetaStorage::audited_frontier)
}

/// A sink that refuses leaves the frontier behind, apply goes on, and the
/// records are delivered - marked replayed, from the log - by the retry once
/// the sink takes them again. Before #706 they were gone. Within the retry
/// interval an apply does not retry, so a sink that stays down costs each
/// apply nothing (review of #767).
#[tokio::test]
async fn a_refused_record_is_delivered_again_by_the_retry() {
    let (_sim, env) = sim();
    let entries = workload(6);
    append(&env, &entries);
    let sink = Sink::refusing();
    let (store, mut machine) = open(&env, &sink);

    let responses = machine
        .apply(entries[..4].iter().map(raft_entry).collect::<Vec<_>>())
        .await
        .expect("a refusing sink does not fail apply");
    assert_eq!(responses.len(), 4);
    assert!(sink.accepted().is_empty());
    let progress = store.audit_progress();
    assert_eq!(progress.applied_index, 4);
    assert_eq!(progress.delivered_index, 0, "nothing was accepted");
    assert_eq!(progress.lag(), 4);
    assert_eq!(audited_frontier(&store), Some(0));

    sink.refusing.store(false, Ordering::SeqCst);
    machine
        .apply(entries[4..].iter().map(raft_entry).collect::<Vec<_>>())
        .await
        .unwrap();
    assert!(
        sink.accepted().is_empty(),
        "an apply within the retry interval does not re-read the backlog"
    );
    assert!(store.deliver_pending(ReplayReason::Retry));
    assert_eq!(
        sink.indexes(),
        audited_indexes(&entries).into_iter().collect::<Vec<_>>(),
        "every audited entry, once, in order"
    );
    assert!(sink.accepted().iter().all(|r| r.replayed));
    assert!(sink
        .accepted()
        .iter()
        .all(|r| r.actor == vtop_meta::audit::AuditActor::Replica(Some(NODE_ID))));
    assert_eq!(audited_frontier(&store), Some(6));
    let progress = store.audit_progress();
    assert_eq!(progress.lag(), 0);
    assert_eq!(
        progress.replayed[1],
        audited_indexes(&entries).len() as u64,
        "counted as retries"
    );
}

/// The #705 crash window: the applied frontier is durable, the record never
/// reached the sink. The restart delivers it, marked replayed, and the
/// records carry the entry's own time, term and index.
#[tokio::test]
async fn a_crash_after_the_applied_frontier_redelivers_the_record_at_restart() {
    let (sim, env) = sim();
    let entries = workload(3);
    append(&env, &entries);
    let refusing = Sink::refusing();
    let (store, mut machine) = open(&env, &refusing);
    machine
        .apply(entries.iter().map(raft_entry).collect::<Vec<_>>())
        .await
        .unwrap();
    assert_eq!(store.with_storage(MetaStorage::last_applied), 3);
    drop((store, machine));
    sim.crash();
    sim.reboot();

    let sink = Arc::new(Sink::default());
    let (store, _machine) = open(&env, &sink);
    assert!(store.deliver_pending(ReplayReason::Restart));
    let records = sink.accepted();
    assert_eq!(
        records
            .iter()
            .filter_map(|r| r.log_index)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    for record in &records {
        assert!(record.replayed, "{record:?}");
        assert_eq!(record.outcome, AuditOutcome::Success);
        assert_eq!(record.log_term, Some(1));
        let index = record.log_index.unwrap();
        assert_eq!(
            record.issued_at_ms,
            Some(1_750_000_000_000 + i64::try_from(index).unwrap() + 0x700),
            "the entry's issued_at_ms, never the replica's clock"
        );
    }
    assert_eq!(audited_frontier(&store), Some(3));
    assert_eq!(store.audit_progress().replayed[0], 2, "counted as restart");
}

/// A clean stop leaves nothing to deliver: a restart writes no record again.
#[tokio::test]
async fn a_clean_restart_delivers_nothing_again() {
    let (_sim, env) = sim();
    let entries = workload(5);
    append(&env, &entries);
    let sink = Arc::new(Sink::default());
    let (store, mut machine) = open(&env, &sink);
    machine
        .apply(entries.iter().map(raft_entry).collect::<Vec<_>>())
        .await
        .unwrap();
    let delivered = sink.indexes();
    assert_eq!(
        delivered,
        audited_indexes(&entries).into_iter().collect::<Vec<_>>()
    );
    assert!(
        sink.accepted().iter().all(|r| !r.replayed),
        "live, not replayed"
    );
    drop((store, machine));

    let again = Arc::new(Sink::default());
    let (store, _machine) = open(&env, &again);
    assert!(store.deliver_pending(ReplayReason::Restart));
    assert!(again.accepted().is_empty(), "{:?}", again.accepted());
}

/// A snapshot install brings entries this replica never applied one by one:
/// one gap record names them. While the sink refuses it the frontier stays
/// where it was; once it is delivered the frontier moves to the snapshot.
#[tokio::test]
async fn a_snapshot_install_writes_one_gap_record_and_moves_the_frontier() {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new("/leader"));
    sim.create_dir_all(Path::new("/n"));
    let env = sim.env(0x707);
    let mut leader = MetaStorage::open_in(&env, "/leader", cluster()).unwrap();
    let mut entries = workload(10);
    entries[0] = MetaLogEntry {
        term: 1,
        index: 1,
        payload: MetaLogPayload::Membership(MetaMembership {
            voters: vec![MetaNodeId(1), MetaNodeId(2)],
            learners: vec![],
            joint_outgoing: None,
        }),
    };
    leader.append(&entries).unwrap();
    leader.apply_through(10).unwrap();
    let snapshot = leader.write_snapshot().unwrap();
    let bytes = env.storage.read(&snapshot.path).unwrap();

    let sink = Sink::refusing();
    let (store, mut machine) = open(&env, &sink);
    let membership = openraft::Membership::new(
        vec![std::collections::BTreeSet::from([1u64, 2u64])],
        None::<BTreeSet<u64>>,
    );
    let meta = SnapshotMeta {
        last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 0), 9)),
        last_membership: StoredMembership::new(
            Some(LogId::new(CommittedLeaderId::new(1, 0), 0)),
            membership,
        ),
        snapshot_id: snapshot.snapshot_id.clone(),
    };
    machine
        .install_snapshot(&meta, Box::new(Cursor::new(bytes)))
        .await
        .unwrap();
    assert_eq!(
        audited_frontier(&store),
        Some(0),
        "a refused gap record leaves the frontier where it was"
    );
    assert_eq!(store.audit_progress().gaps[0], 1);

    sink.refusing.store(false, Ordering::SeqCst);
    assert!(store.deliver_pending(ReplayReason::Retry));
    let gaps = sink.gaps();
    assert_eq!(gaps.len(), 1, "{gaps:?}");
    let params: BTreeMap<_, _> = gaps[0].params.iter().cloned().collect();
    assert_eq!(params["from_index"], "1");
    assert_eq!(params["through_index"], "10");
    assert_eq!(params["reason"], "snapshot_install");
    assert_eq!(gaps[0].outcome, AuditOutcome::Failure);
    assert!(gaps[0]
        .reason
        .as_deref()
        .unwrap()
        .contains("dedupe on the log index"));
    assert_eq!(sink.indexes(), Vec::<u64>::new(), "nothing re-derived");
    assert_eq!(audited_frontier(&store), Some(10), "delivered, so moved");
}

/// #767 review: a sink that refuses the snapshot's gap record and a restart
/// before it recovers must not lose the gap. The frontier did not move, so
/// the restarted replica re-derives the range as a gap from the log - and
/// reads none of the stale branch the snapshot replaced.
#[tokio::test]
async fn a_refused_snapshot_gap_survives_a_restart() {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new("/leader"));
    sim.create_dir_all(Path::new("/n"));
    let env = sim.env(0x70a);
    let mut leader = MetaStorage::open_in(&env, "/leader", cluster()).unwrap();
    let mut entries = workload(10);
    entries[0] = MetaLogEntry {
        term: 2,
        index: 1,
        payload: MetaLogPayload::Membership(MetaMembership {
            voters: vec![MetaNodeId(1), MetaNodeId(2)],
            learners: vec![],
            joint_outgoing: None,
        }),
    };
    for entry in &mut entries {
        entry.term = 2;
    }
    leader.append(&entries).unwrap();
    leader.apply_through(10).unwrap();
    let snapshot = leader.write_snapshot().unwrap();
    let bytes = env.storage.read(&snapshot.path).unwrap();

    // The follower holds a stale branch of term 1 it never applied, reaching
    // past the snapshot, so reopening keeps it and only the log-matching
    // check in `readable_from` stands between it and a replay.
    append(&env, &workload(12));
    let refusing = Sink::refusing();
    let (store, mut machine) = open(&env, &refusing);
    let membership = openraft::Membership::new(
        vec![std::collections::BTreeSet::from([1u64, 2u64])],
        None::<BTreeSet<u64>>,
    );
    let meta = SnapshotMeta {
        last_log_id: Some(LogId::new(CommittedLeaderId::new(2, 0), 9)),
        last_membership: StoredMembership::new(
            Some(LogId::new(CommittedLeaderId::new(2, 0), 0)),
            membership,
        ),
        snapshot_id: snapshot.snapshot_id.clone(),
    };
    machine
        .install_snapshot(&meta, Box::new(Cursor::new(bytes)))
        .await
        .unwrap();
    assert_eq!(audited_frontier(&store), Some(0));
    drop((store, machine));
    sim.crash();
    sim.reboot();

    let sink = Arc::new(Sink::default());
    let (store, _machine) = open(&env, &sink);
    assert!(store.deliver_pending(ReplayReason::Restart));
    let gaps = sink.gaps();
    assert_eq!(gaps.len(), 1, "{gaps:?}");
    let params: BTreeMap<_, _> = gaps[0].params.iter().cloned().collect();
    assert_eq!(params["from_index"], "1");
    assert_eq!(params["through_index"], "10");
    assert_eq!(params["reason"], "snapshot_install");
    assert!(
        sink.indexes().is_empty(),
        "the stale branch was read: {:?}",
        sink.indexes()
    );
    assert_eq!(audited_frontier(&store), Some(10));
}

/// A store written by a release before #706 has no audit frontier: its
/// trail starts at the applied frontier and nothing earlier is replayed.
/// The directory is the real v0.9.0 `vtop-node` output (see the fixture's
/// PROVENANCE), opened on a copy so the fixture stays as it was written.
#[tokio::test]
async fn a_directory_written_by_v0_9_0_starts_its_trail_at_the_applied_frontier() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/v0.9.0-meta/data");
    let copy = std::env::temp_dir().join(format!("vtop-706-v090-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&copy).unwrap();
    let mut before = BTreeMap::new();
    for file in std::fs::read_dir(&fixture).unwrap() {
        let file = file.unwrap();
        let bytes = std::fs::read(file.path()).unwrap();
        std::fs::write(copy.join(file.file_name()), &bytes).unwrap();
        before.insert(file.file_name(), bytes);
    }
    let env = Env::real();
    let cluster = Uuid::parse_str("00000000-0000-0000-0000-0000000c1a55").unwrap();
    let store = MetaRaftStore::open_in(&env, &copy, cluster).unwrap();
    let sink = Arc::new(Sink::default());
    store.set_audit_sink(sink.clone());
    store.with_storage(|storage| {
        assert_eq!(storage.last_applied(), 5);
        assert_eq!(storage.audited_frontier(), Some(5));
        assert_eq!(storage.state().feature_level(), 7);
    });
    assert!(store.deliver_pending(ReplayReason::Restart));
    assert!(sink.accepted().is_empty(), "{:?}", sink.accepted());
    assert!(copy.join("meta.audited").exists());
    drop(store);
    for (name, bytes) in &before {
        assert_eq!(
            std::fs::read(fixture.join(name)).unwrap(),
            *bytes,
            "{name:?}: the fixture itself is never written"
        );
    }
    let _ = std::fs::remove_dir_all(&copy);
}

/// A sink that refused for so long that the log was purged past the
/// frontier: what the log still holds is delivered, the rest named by one
/// gap record.
#[tokio::test]
async fn a_log_purged_past_the_frontier_is_named_by_a_gap_record() {
    use openraft::storage::RaftLogStorage;
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new("/n"));
    let env = sim.env(0x708);
    let entries = workload(9);
    {
        let mut storage = MetaStorage::open_with(
            &env,
            "/n",
            cluster(),
            vtop_meta::MetaStorageConfig {
                log: vtop_meta::MetaLogConfig {
                    max_chunk_bytes: 256,
                },
            },
        )
        .unwrap();
        storage.ensure_raft_applied_frontier().unwrap();
        storage.append(&entries).unwrap();
    }
    let sink = Sink::refusing();
    let store = MetaRaftStore::open_tiny(&env, "/n", cluster()).unwrap();
    store.set_audit_sink(sink.clone());
    let mut machine = MetaRaftStateMachine::new(store.clone()).with_node_id(NODE_ID);
    machine
        .apply(entries.iter().map(raft_entry).collect::<Vec<_>>())
        .await
        .unwrap();
    {
        use openraft::storage::RaftSnapshotBuilder;
        let mut builder = machine.get_snapshot_builder().await;
        builder.build_snapshot().await.unwrap();
    }
    let mut log = vtop_meta::raft::MetaRaftLogStore::new(store.clone());
    log.purge(LogId::new(CommittedLeaderId::new(1, 0), 5))
        .await
        .unwrap();
    let first = store.with_storage(|s| s.log().first_index()).unwrap();
    assert!(first > 1, "the purge dropped whole chunks (first {first})");

    sink.refusing.store(false, Ordering::SeqCst);
    assert!(store.deliver_pending(ReplayReason::Retry));
    let gaps = sink.gaps();
    assert_eq!(gaps.len(), 1, "{gaps:?}");
    let params: BTreeMap<_, _> = gaps[0].params.iter().cloned().collect();
    assert_eq!(params["from_index"], "1");
    assert_eq!(params["through_index"], (first - 1).to_string());
    assert_eq!(params["reason"], "log_purged");
    let expected: Vec<u64> = audited_indexes(&entries)
        .into_iter()
        .filter(|index| *index >= first)
        .collect();
    assert_eq!(sink.indexes(), expected);
    assert_eq!(store.audit_progress().gaps[1], 1);
    assert_eq!(audited_frontier(&store), Some(9));
}

/// A sink that appends each record's log index to a file on the same
/// simulated disk and syncs it, so a crash can land between a record reaching
/// the sink and the audit frontier being saved (review of #767: an in-memory
/// sink never left that window open).
struct DiskSink {
    env: Env,
}

const SINK_FILE: &str = "/n/audit.sink";

impl Auditor for DiskSink {
    fn record(&self, record: &AuditRecord) -> Result<(), String> {
        use std::io::{Seek, SeekFrom, Write};
        let Some(index) = record.log_index else {
            return Ok(());
        };
        let path = Path::new(SINK_FILE);
        let mut file = self
            .env
            .storage
            .open(path, vtop_log::env::OpenMode::CreateAppend)
            .map_err(|e| e.to_string())?;
        file.seek(SeekFrom::End(0)).map_err(|e| e.to_string())?;
        file.write_all(format!("{index}\n").as_bytes())
            .and_then(|()| file.sync_data())
            .map_err(|e| e.to_string())?;
        self.env
            .storage
            .sync_dir(Path::new("/n"))
            .map_err(|e| e.to_string())
    }
}

fn sunk(env: &Env) -> Vec<u64> {
    let bytes = env.storage.read(Path::new(SINK_FILE)).unwrap_or_default();
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter_map(|line| line.parse().ok())
        .collect()
}

/// A crash before every storage operation of three applies - the record
/// writes of the sink among them - then a restart: every audited entry the
/// restarted replica has applied reached the sink at least once across the
/// two lives, nothing at or below the persisted audit frontier reached it
/// again, and the frontier never passes `applied`.
#[tokio::test]
async fn a_crash_at_every_write_of_an_apply_loses_no_applied_record() {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new("/n"));
    let env = sim.env(0x709);
    let entries = workload(9);
    append(&env, &entries);
    let disk = sim.snapshot();
    // Two audited, one node-initiated only, then the rest.
    let batches: [std::ops::Range<usize>; 3] = [0..2, 2..3, 3..9];
    let open_on_disk = |env: &Env| {
        let store = MetaRaftStore::open_in(env, "/n", cluster()).unwrap();
        store.set_audit_sink(Arc::new(DiskSink { env: env.clone() }));
        let machine = MetaRaftStateMachine::new(store.clone()).with_node_id(NODE_ID);
        (store, machine)
    };

    // A dry run counts the operations of the three applies.
    let ops = {
        let (_store, mut machine) = open_on_disk(&env);
        let before = sim.op_count();
        for batch in batches.clone() {
            machine
                .apply(entries[batch].iter().map(raft_entry).collect::<Vec<_>>())
                .await
                .unwrap();
        }
        sim.op_count() - before
    };
    assert!(ops > 10, "{ops} operations");

    for crash_at in 0..=ops {
        sim.restore(&disk);
        let (store, mut machine) = open_on_disk(&env);
        let base = sim.op_count();
        sim.set_fault(FaultPlan::CrashBefore(base + crash_at));
        for batch in batches.clone() {
            if machine
                .apply(entries[batch].iter().map(raft_entry).collect::<Vec<_>>())
                .await
                .is_err()
            {
                break;
            }
        }
        drop((store, machine));
        sim.set_fault(FaultPlan::None);
        sim.crash();
        sim.reboot();

        let first_life = sunk(&env);
        let (store, _machine) = open_on_disk(&env);
        let (applied, persisted) =
            store.with_storage(|s| (s.last_applied(), s.audited_frontier().unwrap()));
        assert!(
            persisted <= applied,
            "crash at op {crash_at}: audit frontier {persisted} passed applied {applied}"
        );
        assert!(store.deliver_pending(ReplayReason::Restart));
        let all = sunk(&env);
        let redelivered = &all[first_life.len()..];
        assert!(
            redelivered.iter().all(|index| *index > persisted),
            "crash at op {crash_at}: {redelivered:?} re-delivered at or below the frontier \
             {persisted}"
        );
        let seen: BTreeSet<u64> = all.iter().copied().collect();
        let owed: BTreeSet<u64> = audited_indexes(&entries)
            .into_iter()
            .filter(|index| *index <= applied)
            .collect();
        assert!(
            owed.is_subset(&seen),
            "crash at op {crash_at}: applied through {applied}, owed {owed:?}, delivered \
             {seen:?}"
        );
    }
}

/// Review of #767: a backlog left by a long sink outage is delivered a
/// bounded chunk at a time. An apply after the retry interval delivers one
/// chunk and lets apply go on; the retry delivers the rest, in order, each
/// record once.
#[tokio::test]
async fn a_backlog_is_delivered_in_bounded_chunks() {
    let (_sim, env) = sim();
    let total = 700_u64;
    let entries: Vec<MetaLogEntry> = (1..=total).map(|index| entry(1, index, true)).collect();
    append(&env, &entries);
    let sink = Sink::refusing();
    let (store, mut machine) = open(&env, &sink);
    machine
        .apply(entries[..600].iter().map(raft_entry).collect::<Vec<_>>())
        .await
        .unwrap();
    assert_eq!(store.audit_progress().lag(), 600);

    sink.refusing.store(false, Ordering::SeqCst);
    // The retry delivers everything, a chunk per pass, letting go of the
    // locks in between.
    assert!(store.deliver_pending(ReplayReason::Retry));
    assert_eq!(sink.indexes(), (1..=600).collect::<Vec<_>>());
    machine
        .apply(entries[600..].iter().map(raft_entry).collect::<Vec<_>>())
        .await
        .unwrap();
    assert_eq!(sink.indexes(), (1..=total).collect::<Vec<_>>());
    assert_eq!(store.audit_progress().lag(), 0);
}

/// Review of #767: a snapshot's refused gap record is replaced, not repeated,
/// by a newer snapshot's gap, which starts at the same frontier and covers
/// it.
#[tokio::test]
async fn a_newer_snapshot_gap_replaces_a_refused_older_one() {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new("/leader"));
    sim.create_dir_all(Path::new("/n"));
    let env = sim.env(0x70c);
    let mut leader = MetaStorage::open_in(&env, "/leader", cluster()).unwrap();
    let mut entries = workload(20);
    entries[0] = MetaLogEntry {
        term: 1,
        index: 1,
        payload: MetaLogPayload::Membership(MetaMembership {
            voters: vec![MetaNodeId(1), MetaNodeId(2)],
            learners: vec![],
            joint_outgoing: None,
        }),
    };
    leader.append(&entries).unwrap();
    leader.apply_through(10).unwrap();
    let first = leader.write_snapshot().unwrap();
    let first_bytes = env.storage.read(&first.path).unwrap();
    leader.apply_through(20).unwrap();
    let second = leader.write_snapshot().unwrap();
    let second_bytes = env.storage.read(&second.path).unwrap();
    let meta = |last: u64, id: &str| SnapshotMeta {
        last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 0), last - 1)),
        last_membership: StoredMembership::new(
            Some(LogId::new(CommittedLeaderId::new(1, 0), 0)),
            openraft::Membership::new(
                vec![std::collections::BTreeSet::from([1u64, 2u64])],
                None::<BTreeSet<u64>>,
            ),
        ),
        snapshot_id: id.to_owned(),
    };

    let sink = Sink::refusing();
    let (store, mut machine) = open(&env, &sink);
    machine
        .install_snapshot(
            &meta(10, &first.snapshot_id),
            Box::new(Cursor::new(first_bytes)),
        )
        .await
        .unwrap();
    sink.refusing.store(false, Ordering::SeqCst);
    machine
        .install_snapshot(
            &meta(20, &second.snapshot_id),
            Box::new(Cursor::new(second_bytes)),
        )
        .await
        .unwrap();
    assert!(store.deliver_pending(ReplayReason::Retry));
    let gaps: Vec<(String, String)> = sink
        .gaps()
        .iter()
        .map(|gap| {
            let params: BTreeMap<_, _> = gap.params.iter().cloned().collect();
            (
                params["from_index"].clone(),
                params["through_index"].clone(),
            )
        })
        .collect();
    assert_eq!(gaps, vec![("1".to_owned(), "20".to_owned())]);
    assert_eq!(audited_frontier(&store), Some(20));
}

/// Review of #767: a snapshot install over a stale branch, with the
/// sink refusing its gap, then new entries applied and a LOCAL snapshot built
/// on top - whose index and term the log agrees with, while the stale branch
/// below the installed snapshot still sits in the log. A restart must still
/// name the installed range as a gap and read nothing of the stale branch.
#[tokio::test]
async fn a_stale_branch_stays_unread_after_a_later_local_snapshot() {
    use openraft::storage::RaftSnapshotBuilder;
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new("/leader"));
    sim.create_dir_all(Path::new("/n"));
    let env = sim.env(0x70d);
    let membership_entry = |term| MetaLogEntry {
        term,
        index: 1,
        payload: MetaLogPayload::Membership(MetaMembership {
            voters: vec![MetaNodeId(1), MetaNodeId(2)],
            learners: vec![],
            joint_outgoing: None,
        }),
    };
    let mut leader = MetaStorage::open_in(&env, "/leader", cluster()).unwrap();
    let mut entries: Vec<MetaLogEntry> = workload(10)
        .into_iter()
        .map(|mut entry| {
            entry.term = 2;
            entry
        })
        .collect();
    entries[0] = membership_entry(2);
    leader.append(&entries).unwrap();
    leader.apply_through(10).unwrap();
    let snapshot = leader.write_snapshot().unwrap();
    let bytes = env.storage.read(&snapshot.path).unwrap();

    // A stale term-1 branch ending exactly at the snapshot's index.
    append(&env, &workload(10));
    let refusing = Sink::refusing();
    let (store, mut machine) = open(&env, &refusing);
    let meta = SnapshotMeta {
        last_log_id: Some(LogId::new(CommittedLeaderId::new(2, 0), 9)),
        last_membership: StoredMembership::new(
            Some(LogId::new(CommittedLeaderId::new(2, 0), 0)),
            openraft::Membership::new(
                vec![std::collections::BTreeSet::from([1u64, 2u64])],
                None::<BTreeSet<u64>>,
            ),
        ),
        snapshot_id: snapshot.snapshot_id.clone(),
    };
    machine
        .install_snapshot(&meta, Box::new(Cursor::new(bytes)))
        .await
        .unwrap();
    drop((store, machine));

    // New committed entries 11..=13 after the install, applied while the
    // sink still refuses, then a snapshot built here at 13.
    let newer: Vec<MetaLogEntry> = (11..=13).map(|index| entry(2, index, true)).collect();
    {
        let mut storage = MetaStorage::open_in(&env, "/n", cluster()).unwrap();
        storage.append(&newer).unwrap();
    }
    let (store, mut machine) = open(&env, &refusing);
    machine
        .apply(newer.iter().map(raft_entry).collect::<Vec<_>>())
        .await
        .unwrap();
    machine
        .get_snapshot_builder()
        .await
        .build_snapshot()
        .await
        .unwrap();
    store.with_storage(|s| {
        assert_eq!(s.snapshots().newest().unwrap().last_index, 13);
        assert_eq!(
            s.log().first_index(),
            Some(1),
            "the stale branch is still there"
        );
    });
    drop((store, machine));
    sim.crash();
    sim.reboot();

    let sink = Arc::new(Sink::default());
    let (store, _machine) = open(&env, &sink);
    assert!(store.deliver_pending(ReplayReason::Restart));
    let gaps = sink.gaps();
    assert_eq!(gaps.len(), 1, "{gaps:?}");
    let params: BTreeMap<_, _> = gaps[0].params.iter().cloned().collect();
    assert_eq!(
        (
            params["from_index"].as_str(),
            params["through_index"].as_str()
        ),
        ("1", "10")
    );
    assert_eq!(params["reason"], "snapshot_install");
    assert_eq!(
        sink.indexes(),
        vec![11, 12, 13],
        "nothing of the stale branch"
    );
    assert_eq!(audited_frontier(&store), Some(13));
}

//! End-to-end recovery: build state through the durable store (log entries
//! including membership and blank entries), snapshot midway, append more,
//! crash at arbitrary points via the sim, reopen, and assert the recovered
//! state machine is byte-identical to a pure in-memory reference instance
//! that applied the same commands — proving storage adds nothing to and
//! loses nothing from the deterministic apply path.

use std::path::Path;
use uuid::Uuid;
use vtop_log::env::Env;
use vtop_log::sim::{FaultPlan, SimStorage, TraceKind};
use vtop_meta::{
    CommandEnvelope, MetaLogConfig, MetaLogEntry, MetaLogPayload, MetaMembership, MetaNodeId,
    MetaStateMachine, MetaStorage, MetaStorageConfig, MetadataCommand, NodeState,
};

const SEED: u64 = 0x5eed_0093;
const ROOT: &str = "/meta";
const NODE: Uuid = Uuid::from_u128(0x10);
const TOPIC: Uuid = Uuid::from_u128(0x20);
const RANGE: Uuid = Uuid::from_u128(0x21);
const SEGMENT: Uuid = Uuid::from_u128(0x30);

fn cluster_id() -> Uuid {
    Uuid::from_u128(0xc1)
}

fn config() -> MetaStorageConfig {
    MetaStorageConfig {
        log: MetaLogConfig {
            max_chunk_bytes: 256,
        },
    }
}

fn envelope(request: u128) -> CommandEnvelope {
    CommandEnvelope {
        request_id: Uuid::from_u128(request),
        issued_at_ms: 1_750_000_000_000,
    }
}

fn membership() -> MetaMembership {
    MetaMembership {
        voters: vec![MetaNodeId(1), MetaNodeId(2), MetaNodeId(3)],
        learners: vec![(MetaNodeId(4), "n4:9200".to_owned())],
    }
}

/// The full ten-entry history: normal commands, one membership change, and
/// one blank, split as 1..=6 before the snapshot and 7..=10 after it.
fn history() -> Vec<MetaLogEntry> {
    let normal = |term, index, command| MetaLogEntry {
        term,
        index,
        payload: MetaLogPayload::Normal(command),
    };
    vec![
        normal(
            1,
            1,
            MetadataCommand::RegisterNode {
                env: envelope(0xe1),
                node_uuid: NODE,
                addr: "10.0.0.1:9200".to_owned(),
                expected_generation: None,
            },
        ),
        normal(
            1,
            2,
            MetadataCommand::CreateTopic {
                env: envelope(0xe2),
                name: "events.v1".to_owned(),
                topic_uuid: TOPIC,
                root_range_uuid: RANGE,
            },
        ),
        normal(
            1,
            3,
            MetadataCommand::GrantRangeLease {
                env: envelope(0xe3),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                holder_node_uuid: NODE,
                expected_range_generation: 0,
            },
        ),
        MetaLogEntry {
            term: 1,
            index: 4,
            payload: MetaLogPayload::Membership(membership()),
        },
        normal(
            1,
            5,
            MetadataCommand::RegisterSealedSegment {
                env: envelope(0xe5),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                base_offset: 0,
                next_offset: 128,
                content_root: [7; 32],
                sealed_by_epoch: 1,
                expected_range_generation: 1,
            },
        ),
        normal(
            1,
            6,
            MetadataCommand::MarkSegmentVerified {
                env: envelope(0xe6),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                content_root: [7; 32],
                expected_generation: 0,
            },
        ),
        MetaLogEntry {
            term: 2,
            index: 7,
            payload: MetaLogPayload::Blank,
        },
        normal(
            2,
            8,
            MetadataCommand::PutKeyRecord {
                env: envelope(0xe8),
                key_uuid: Uuid::from_u128(0x52),
                scheme: 1,
                public_material_digest: [8; 32],
            },
        ),
        normal(
            2,
            9,
            MetadataCommand::SetNodeState {
                env: envelope(0xe9),
                node_uuid: NODE,
                state: NodeState::Draining,
                expected_generation: 0,
            },
        ),
        normal(
            2,
            10,
            MetadataCommand::CreateTopic {
                env: envelope(0xea),
                name: "events.v1".to_owned(),
                topic_uuid: Uuid::from_u128(0x22),
                root_range_uuid: Uuid::from_u128(0x23),
            },
        ),
    ]
}

/// Drive the whole history through the store, snapshotting after entry 6.
/// Stops silently at the first error (the injected crash).
fn run_workload(env: &Env) -> bool {
    let Ok(mut storage) = MetaStorage::open_with(env, ROOT, cluster_id(), config()) else {
        return false;
    };
    let history = history();
    let (before_snapshot, after_snapshot) = history.split_at(6);
    if storage.append(before_snapshot).is_err() || storage.apply_through(6).is_err() {
        return false;
    }
    if storage.write_snapshot().is_err() {
        return false;
    }
    if storage.purge_upto(4).is_err() {
        return false;
    }
    if storage.append(after_snapshot).is_err() || storage.apply_through(10).is_err() {
        return false;
    }
    true
}

/// Build the pure in-memory reference for a given applied frontier.
fn reference_state(applied_through: u64) -> MetaStateMachine {
    let mut machine = MetaStateMachine::new();
    for entry in history() {
        if entry.index > applied_through {
            break;
        }
        if let MetaLogPayload::Normal(command) = &entry.payload {
            machine.apply(entry.index, command);
        }
    }
    machine
}

fn verify_against_reference(sim: &SimStorage, env: &Env, context: &str) {
    sim.reboot();
    let storage = MetaStorage::open_with(env, ROOT, cluster_id(), config())
        .unwrap_or_else(|error| panic!("recovery failed ({context}): {error}"));
    let applied = storage.last_applied();
    assert!(applied <= 10, "impossible frontier {applied} ({context})");
    let reference = reference_state(applied);
    assert_eq!(
        storage.state().encode_snapshot().unwrap(),
        reference.encode_snapshot().unwrap(),
        "recovered state diverges from the reference at frontier {applied} ({context})"
    );
    let expected_membership = if applied >= 4 {
        membership()
    } else {
        MetaMembership::default()
    };
    assert_eq!(storage.membership(), &expected_membership, "{context}");
}

fn clean_trace() -> Vec<vtop_log::sim::TraceEntry> {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new(ROOT));
    assert!(
        run_workload(&sim.env(SEED)),
        "clean run must finish without faults"
    );
    sim.trace()
}

#[test]
fn recovered_state_equals_the_reference_instance_after_a_crash_at_every_operation() {
    let total = clean_trace().len();
    for op in 0..total as u64 {
        let context = format!("crash-before op={op} seed={SEED:#x}");
        let sim = SimStorage::new();
        sim.create_dir_all(Path::new(ROOT));
        let env = sim.env(SEED);
        sim.set_fault(FaultPlan::CrashBefore(op));
        run_workload(&env);
        assert!(sim.has_crashed(), "fault did not trigger ({context})");
        verify_against_reference(&sim, &env, &context);
    }
}

#[test]
fn recovered_state_equals_the_reference_instance_after_torn_writes() {
    let trace = clean_trace();
    for write in trace
        .iter()
        .filter(|entry| entry.kind == TraceKind::HandleWrite)
    {
        // The exhaustive per-byte sweep lives in storage_crash_sweep; here
        // three representative cuts per write keep the state-equality oracle
        // cheap while still covering empty, partial, and complete tears.
        for cut in [0, write.len as usize / 2, write.len as usize] {
            let context = format!(
                "torn-write op={} cut={cut} path={} seed={SEED:#x}",
                write.index,
                write.path.display()
            );
            let sim = SimStorage::new();
            sim.create_dir_all(Path::new(ROOT));
            let env = sim.env(SEED);
            sim.set_fault(FaultPlan::CrashDuringWrite {
                op: write.index,
                byte_cut: cut,
            });
            run_workload(&env);
            assert!(sim.has_crashed(), "fault did not trigger ({context})");
            verify_against_reference(&sim, &env, &context);
        }
    }
}

/// The no-crash path: a clean shutdown, reopen, and continued use must also
/// land exactly on the reference, and the snapshot must cover entry 6 with
/// the membership from entry 4.
#[test]
fn clean_reopen_after_snapshot_and_purge_replays_onto_the_reference_state() {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new(ROOT));
    let env = sim.env(SEED);
    assert!(run_workload(&env));

    let storage = MetaStorage::open_with(&env, ROOT, cluster_id(), config()).unwrap();
    assert_eq!(storage.last_applied(), 10);
    assert_eq!(
        storage.state().encode_snapshot().unwrap(),
        reference_state(10).encode_snapshot().unwrap()
    );
    let newest = storage.snapshots().newest().expect("snapshot must exist");
    assert_eq!((newest.last_index, newest.last_term), (6, 1));
    assert_eq!(newest.membership, membership());
    assert_eq!(storage.membership(), &membership());
    // Purge really dropped whole chunks below the snapshot: the log now
    // starts above entry 1 but still overlaps snapshot coverage.
    let first = storage.log().first_index().expect("log must not be empty");
    assert!(first > 1, "purge removed nothing (first={first})");
    assert!(first <= 7, "purge opened a hole above the snapshot");
}

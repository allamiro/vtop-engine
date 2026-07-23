//! Exhaustive deterministic crash sweeps over the metadata store, in the
//! style of vtop-log's sim_crash_sweep: a scripted workload (vote save,
//! appends across chunk rotations, truncation, more appends, snapshot,
//! purge) is crashed before every storage operation and torn at every byte
//! of every write. After reboot the store must recover a synced prefix,
//! never surface a torn or corrupt entry, never lose a saved vote, never
//! show a half-visible snapshot, and recover deterministically.

use std::path::Path;
use uuid::Uuid;
use vtop_log::env::Env;
use vtop_log::sim::{FaultPlan, SimStorage, TraceKind};
use vtop_meta::{
    CommandEnvelope, HardState, MetaLogConfig, MetaLogEntry, MetaLogPayload, MetaNodeId,
    MetaStorage, MetaStorageConfig, MetadataCommand,
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

/// Tiny chunks so five entries cross several rotations.
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

fn saved_vote() -> HardState {
    HardState {
        term: 3,
        voted_for: Some(MetaNodeId(7)),
        vote_committed: true,
    }
}

fn normal(term: u64, index: u64, command: MetadataCommand) -> MetaLogEntry {
    MetaLogEntry {
        term,
        index,
        payload: MetaLogPayload::Normal(command),
    }
}

/// Entries 1..=5 of the first epoch of writes.
fn first_batch() -> Vec<MetaLogEntry> {
    vec![
        normal(
            1,
            1,
            MetadataCommand::RegisterNode {
                env: envelope(0xa1),
                node_uuid: NODE,
                addr: "10.0.0.1:9200".to_owned(),
                expected_generation: None,
            },
        ),
        normal(
            1,
            2,
            MetadataCommand::CreateTopic {
                env: envelope(0xa2),
                name: "events.v1".to_owned(),
                topic_uuid: TOPIC,
                root_range_uuid: RANGE,
            },
        ),
        normal(
            1,
            3,
            MetadataCommand::GrantRangeLease {
                env: envelope(0xa3),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                holder_node_uuid: NODE,
                expected_range_generation: 0,
            },
        ),
        normal(
            1,
            4,
            MetadataCommand::PutKeyRecord {
                env: envelope(0xa4),
                key_uuid: Uuid::from_u128(0x51),
                scheme: 1,
                public_material_digest: [4; 32],
            },
        ),
        normal(
            1,
            5,
            MetadataCommand::ReleaseRangeLease {
                env: envelope(0xa5),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                expected_fencing_epoch: 1,
            },
        ),
    ]
}

/// Entries 4..=6 written by a newer term after `truncate_since(4)`.
fn second_batch() -> Vec<MetaLogEntry> {
    vec![
        normal(
            2,
            4,
            MetadataCommand::RegisterSealedSegment {
                env: envelope(0xb4),
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
            2,
            5,
            MetadataCommand::MarkSegmentVerified {
                env: envelope(0xb5),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                content_root: [7; 32],
                expected_generation: 0,
            },
        ),
        normal(
            2,
            6,
            MetadataCommand::PutKeyRecord {
                env: envelope(0xb6),
                key_uuid: Uuid::from_u128(0x52),
                scheme: 1,
                public_material_digest: [6; 32],
            },
        ),
    ]
}

fn final_entry() -> MetaLogEntry {
    MetaLogEntry {
        term: 2,
        index: 7,
        payload: MetaLogPayload::Blank,
    }
}

/// Which workload steps returned (were acknowledged) before the crash.
#[derive(Clone, Copy, Debug, Default)]
struct Progress {
    vote: bool,
    appended_first: bool,
    truncated: bool,
    appended_second: bool,
    snapshot: bool,
    purged: bool,
    final_append: bool,
    completed: bool,
}

/// The scripted workload: save vote, append 5 entries across rotations,
/// apply a prefix, truncate the unapplied suffix, append replacements from a
/// newer term, snapshot, purge, and one final append. Stops at the first
/// error (the injected crash).
fn run_workload(env: &Env) -> Progress {
    let mut progress = Progress::default();
    let Ok(mut storage) = MetaStorage::open_with(env, ROOT, cluster_id(), config()) else {
        return progress;
    };
    if storage.save_hard_state(saved_vote()).is_err() {
        return progress;
    }
    progress.vote = true;
    if storage.append(&first_batch()).is_err() {
        return progress;
    }
    progress.appended_first = true;
    if storage.apply_through(3).is_err() {
        return progress;
    }
    if storage.truncate_since(4).is_err() {
        return progress;
    }
    progress.truncated = true;
    if storage.append(&second_batch()).is_err() {
        return progress;
    }
    progress.appended_second = true;
    if storage.apply_through(6).is_err() {
        return progress;
    }
    if storage.write_snapshot().is_err() {
        return progress;
    }
    progress.snapshot = true;
    if storage.purge_upto(6).is_err() {
        return progress;
    }
    progress.purged = true;
    if storage.append(&[final_entry()]).is_err() {
        return progress;
    }
    if storage.apply_through(7).is_err() {
        return progress;
    }
    progress.final_append = true;
    progress.completed = true;
    progress
}

/// The full recovery oracle for one crash-consistent durable state.
fn verify_recovery(sim: &SimStorage, env: &Env, progress: Progress, context: &str) {
    sim.reboot();
    let storage = MetaStorage::open_with(env, ROOT, cluster_id(), config())
        .unwrap_or_else(|error| panic!("recovery failed ({context}): {error}"));

    // A saved vote survives exactly; an unacknowledged save may surface as
    // either the old or the new state, never anything else.
    if progress.vote {
        assert_eq!(storage.hard_state(), &saved_vote(), "{context}");
    } else {
        assert!(
            storage.hard_state() == &HardState::default() || storage.hard_state() == &saved_vote(),
            "unexpected hard state {:?} ({context})",
            storage.hard_state()
        );
    }

    // Every recovered entry must decode; read the full range and pin the
    // contents against what was produced. Indices 1..=3 are immutable;
    // 4..=6 may be first-batch or (once truncation was acknowledged, must
    // be) second-batch entries; 7 is the final blank.
    let entries = match (storage.log().first_index(), storage.log().last_index()) {
        (Some(first), Some(last)) => storage
            .log()
            .read_range(first, last + 1)
            .unwrap_or_else(|error| panic!("recovered log unreadable ({context}): {error}")),
        _ => Vec::new(),
    };
    let first_by_index = first_batch();
    let second_by_index = second_batch();
    for entry in &entries {
        let allowed: Vec<&MetaLogEntry> = match entry.index {
            1..=3 => vec![&first_by_index[entry.index as usize - 1]],
            4 | 5 if !progress.truncated => vec![
                &first_by_index[entry.index as usize - 1],
                &second_by_index[entry.index as usize - 4],
            ],
            4..=6 => vec![&second_by_index[entry.index as usize - 4]],
            7 => vec![],
            other => panic!("impossible recovered index {other} ({context})"),
        };
        if entry.index == 7 {
            assert_eq!(entry, &final_entry(), "{context}");
        } else {
            assert!(
                allowed.contains(&entry),
                "recovered entry {} does not match any produced entry ({context})",
                entry.index
            );
        }
    }

    // Acknowledged frontiers can never regress.
    let last = storage.log().last_index().unwrap_or(0);
    if progress.final_append {
        assert_eq!(last, 7, "{context}");
        assert_eq!(storage.last_applied(), 7, "{context}");
    } else if progress.appended_second {
        assert!(last >= 6, "acked second batch lost ({context})");
    } else if progress.truncated {
        assert!(last >= 3, "acked prefix lost ({context})");
    } else if progress.appended_first {
        assert!(last >= 5, "acked first batch lost ({context})");
    }

    // An acknowledged snapshot is fully visible and covers the applied
    // frontier it was taken at; recovery already re-validated every
    // published snapshot byte-for-byte, so none is ever half-visible.
    if progress.snapshot {
        let newest = storage
            .snapshots()
            .newest()
            .unwrap_or_else(|| panic!("acked snapshot missing ({context})"));
        assert_eq!(newest.last_index, 6, "{context}");
        assert_eq!(newest.last_term, 2, "{context}");
        assert!(storage.last_applied() >= 6, "{context}");
    }

    // Recovery is deterministic: a second recovery from the same durable
    // state reproduces identical state bytes, frontier, and hard state.
    let again = MetaStorage::open_with(env, ROOT, cluster_id(), config())
        .unwrap_or_else(|error| panic!("second recovery failed ({context}): {error}"));
    assert_eq!(
        again.state().encode_snapshot().unwrap(),
        storage.state().encode_snapshot().unwrap(),
        "recovery is not deterministic ({context})"
    );
    assert_eq!(again.last_applied(), storage.last_applied(), "{context}");
    assert_eq!(again.hard_state(), storage.hard_state(), "{context}");
}

fn clean_run() -> Vec<vtop_log::sim::TraceEntry> {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new(ROOT));
    let progress = run_workload(&sim.env(SEED));
    assert!(progress.completed, "clean run must finish without faults");
    sim.trace()
}

#[test]
fn sweep_crash_before_every_storage_operation_recovers_a_synced_prefix() {
    let total = clean_run().len();
    assert!(total > 50, "workload is unexpectedly small ({total} ops)");

    for op in 0..total as u64 {
        let context = format!("crash-before op={op} seed={SEED:#x}");
        let sim = SimStorage::new();
        sim.create_dir_all(Path::new(ROOT));
        let env = sim.env(SEED);
        sim.set_fault(FaultPlan::CrashBefore(op));
        let progress = run_workload(&env);
        assert!(sim.has_crashed(), "fault did not trigger ({context})");
        verify_recovery(&sim, &env, progress, &context);
    }
}

#[test]
fn sweep_torn_write_at_every_byte_of_every_write_recovers_a_synced_prefix() {
    let trace = clean_run();
    let writes: Vec<_> = trace
        .iter()
        .filter(|entry| entry.kind == TraceKind::HandleWrite)
        .collect();
    assert!(!writes.is_empty());

    for write in writes {
        for cut in 0..=write.len as usize {
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
            let progress = run_workload(&env);
            assert!(sim.has_crashed(), "fault did not trigger ({context})");
            verify_recovery(&sim, &env, progress, &context);
        }
    }
}

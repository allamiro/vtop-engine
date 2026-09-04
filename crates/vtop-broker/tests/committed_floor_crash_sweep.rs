//! Deterministic crash sweep over the committed-floor file (#240): crash
//! before every storage operation and during every write at every byte cut,
//! then reopen and read the floor back.
//!
//! The guarantee is deliberately weaker than the journals' sweeps, and the
//! sweep pins exactly that shape: the floor is overwritten in place, so a
//! crash DURING a later save may tear the frame and lose a floor an earlier
//! save had already made durable. That is the documented trade — a torn
//! frame is detected by its checksum and reads as absent, which weakens the
//! truncation guard to the pre-floor behaviour instead of inventing a
//! protection boundary. What must never happen is a floor nobody saved: a
//! floor invented from damage in the too-high direction could refuse
//! legitimate reconciliation and exclude a valid replica from every
//! promotion.

use std::path::Path;
use vtop_broker::committed_floor::CommittedFloorFile;
use vtop_log::env::Env;
use vtop_log::sim::{FaultPlan, SimStorage, TraceKind};

const SEED: u64 = 0x5eed_0240;
const FLOOR_PATH: &str = "/log/committed-floor";
const SAVES: [u64; 2] = [5, 9];

/// Open and save each floor in order, stopping at the first failure.
/// Returns the acknowledged floors — the saves whose fsync completed.
fn run_workload(env: &Env) -> Vec<u64> {
    let mut acked = Vec::new();
    let mut file = CommittedFloorFile::open_in(env, FLOOR_PATH);
    for floor in SAVES {
        if file.save(floor).is_err() {
            return acked;
        }
        acked.push(floor);
    }
    acked
}

fn assert_floor_was_actually_saved(floor: u64, context: &str) {
    assert!(
        floor == 0 || SAVES.contains(&floor),
        "the recovered floor {floor} was never saved — a floor invented from a crash could \
         refuse legitimate reconciliation and exclude a valid replica from promotion ({context})"
    );
}

#[test]
fn a_floor_write_interrupted_at_every_boundary_reads_as_old_value_or_absent_never_garbage() {
    let clean = SimStorage::new();
    clean.create_dir_all(Path::new("/log"));
    let clean_acked = run_workload(&clean.env(SEED));
    assert_eq!(
        clean_acked,
        SAVES.to_vec(),
        "the clean run must ack every save"
    );
    let total = clean.op_count();
    let trace = clean.trace();

    // Crash BEFORE every whole operation. Unsynced bytes vanish entirely
    // here, so the durable floor must be exactly the newest ACKED save (or
    // absent when none was): whole-op crashes never disturb a frame whose
    // fsync completed.
    for op in 0..total {
        let context = format!("crash-before op={op} seed={SEED:#x}");
        let sim = SimStorage::new();
        sim.create_dir_all(Path::new("/log"));
        let env = sim.env(SEED);
        sim.set_fault(FaultPlan::CrashBefore(op));
        let acked = run_workload(&env);
        assert!(sim.has_crashed(), "{context}");
        sim.reboot();

        let recovered = CommittedFloorFile::open_in(&env, FLOOR_PATH);
        assert_eq!(
            recovered.floor(),
            acked.last().copied().unwrap_or(0),
            "an acknowledged floor must survive any whole-op crash ({context})"
        );

        // And the file must remain USABLE protection: the next life can
        // persist a further advance and read it back, whatever the crash
        // left behind.
        let mut file = recovered;
        let next = SAVES[SAVES.len() - 1] + 1;
        file.save(next)
            .unwrap_or_else(|error| panic!("saving after recovery must work ({context}): {error}"));
        assert_eq!(
            CommittedFloorFile::open_in(&env, FLOOR_PATH).floor(),
            next,
            "the post-recovery save must be durable ({context})"
        );
    }

    // Crash DURING every write, at every byte cut: the torn-frame case the
    // checksum exists for. Here an acked floor CAN degrade to absent — the
    // in-place trade — but the recovered value must still be a floor some
    // save attempted, never a fabrication.
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
            sim.create_dir_all(Path::new("/log"));
            let env = sim.env(SEED);
            sim.set_fault(FaultPlan::CrashDuringWrite {
                op: entry.index,
                byte_cut: cut,
            });
            let _ = run_workload(&env);
            assert!(sim.has_crashed(), "{context}");
            sim.reboot();

            let floor = CommittedFloorFile::open_in(&env, FLOOR_PATH).floor();
            assert_floor_was_actually_saved(floor, &context);
        }
    }
}

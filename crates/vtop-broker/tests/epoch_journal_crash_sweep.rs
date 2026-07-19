//! Deterministic crash sweep over the producer epoch journal: crash before
//! every storage operation and during every write at every byte cut. After
//! reopening, each producer's durable epoch must be exactly a prefix of the
//! accepted history (old or new value, never an invented one), acknowledged
//! epochs must survive, and torn entries must be detected as corruption.

use std::path::Path;
use uuid::Uuid;
use vtop_broker::{BrokerError, ProducerEpochJournal};
use vtop_log::env::Env;
use vtop_log::sim::{FaultPlan, SimStorage, TraceKind};

const SEED: u64 = 0x5eed_0140;
const JOURNAL_PATH: &str = "/log/sweep.epochs";
const EPOCH_ENTRY_BYTES: u64 = 16 + 8 + 32;

fn accepts() -> Vec<(Uuid, u64)> {
    vec![
        (Uuid::from_u128(21), 1),
        (Uuid::from_u128(21), 2),
        (Uuid::from_u128(22), 5),
    ]
}

/// Run open + accepts, stopping at the first error. Returns the accepted
/// (acknowledged) prefix.
fn run_workload(env: &Env) -> Vec<(Uuid, u64)> {
    let mut acked = Vec::new();
    let Ok(mut journal) = ProducerEpochJournal::open_in(env, JOURNAL_PATH) else {
        return acked;
    };
    for (producer, epoch) in accepts() {
        if journal.accept(producer, epoch).is_err() {
            return acked;
        }
        acked.push((producer, epoch));
    }
    acked
}

/// Every durable state reachable by our crash model is a prefix of the
/// accepted history (the in-flight entry may be fully applied).
fn allowed_states() -> Vec<Vec<(Uuid, u64)>> {
    let accepts = accepts();
    (0..=accepts.len())
        .map(|length| accepts[..length].to_vec())
        .collect()
}

fn state_matches(journal: &ProducerEpochJournal, state: &[(Uuid, u64)]) -> bool {
    let mut expected = std::collections::HashMap::new();
    for (producer, epoch) in state {
        expected.insert(*producer, *epoch);
    }
    accepts()
        .iter()
        .all(|(producer, _)| journal.current(*producer) == expected.get(producer).copied())
}

fn verify_reopened(journal: &ProducerEpochJournal, acked: &[(Uuid, u64)], context: &str) {
    let matched = allowed_states()
        .iter()
        .position(|state| state_matches(journal, state));
    let Some(prefix) = matched else {
        panic!("durable epochs are not a prefix of accepted history ({context})");
    };
    assert!(
        prefix >= acked.len(),
        "acknowledged epoch lost: durable prefix {prefix} < acked {} ({context})",
        acked.len()
    );
}

#[test]
fn epoch_journal_crash_before_every_operation_preserves_exactly_the_acked_epochs() {
    let clean = SimStorage::new();
    let clean_acked = run_workload(&clean.env(SEED));
    assert_eq!(clean_acked.len(), accepts().len());
    let total = clean.op_count();

    for op in 0..total {
        let context = format!("crash-before op={op} seed={SEED:#x}");
        let sim = SimStorage::new();
        let env = sim.env(SEED);
        sim.set_fault(FaultPlan::CrashBefore(op));
        let acked = run_workload(&env);
        assert!(sim.has_crashed(), "{context}");
        sim.reboot();

        // Nothing volatile survives, so the durable journal holds exactly the
        // acknowledged prefix and reopening always succeeds.
        let mut journal = ProducerEpochJournal::open_in(&env, JOURNAL_PATH)
            .unwrap_or_else(|error| panic!("reopen failed ({context}): {error}"));
        verify_reopened(&journal, &acked, &context);
        assert!(
            state_matches(&journal, &acked),
            "durable epochs differ from the acknowledged prefix ({context})"
        );
        if let Some((producer, epoch)) = acked.last() {
            assert!(
                matches!(
                    journal.accept(*producer, epoch - 1),
                    Err(BrokerError::ProducerFenced { .. })
                ),
                "stale epoch was not fenced after reopen ({context})"
            );
        }
    }
}

#[test]
fn epoch_journal_torn_writes_yield_old_value_new_value_or_detected_corruption() {
    let clean = SimStorage::new();
    run_workload(&clean.env(SEED));
    let trace = clean.trace();

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
            let env = sim.env(SEED);
            sim.set_fault(FaultPlan::CrashDuringWrite {
                op: entry.index,
                byte_cut: cut,
            });
            let acked = run_workload(&env);
            assert!(sim.has_crashed(), "{context}");
            sim.reboot();

            let is_entry_write = entry.len == EPOCH_ENTRY_BYTES;
            match ProducerEpochJournal::open_in(&env, JOURNAL_PATH) {
                Ok(journal) => {
                    if is_entry_write {
                        assert!(
                            cut == 0 || cut == EPOCH_ENTRY_BYTES as usize,
                            "torn entry was silently accepted ({context})"
                        );
                    }
                    verify_reopened(&journal, &acked, &context);
                }
                Err(BrokerError::EpochJournalCorrupt(_)) => {
                    assert!(
                        !is_entry_write || (cut > 0 && cut < EPOCH_ENTRY_BYTES as usize),
                        "intact journal reported corrupt ({context})"
                    );
                    // Detection never destroys acknowledged entries: every
                    // acked entry was synced before the torn write started.
                    if !acked.is_empty() {
                        let durable = sim.snapshot();
                        let bytes = &durable.files[&Path::new(JOURNAL_PATH).to_path_buf()];
                        assert!(
                            bytes.len() as u64 >= 10 + acked.len() as u64 * EPOCH_ENTRY_BYTES,
                            "acked entries missing from durable journal ({context})"
                        );
                    }
                }
                Err(other) => panic!("unexpected reopen error ({context}): {other}"),
            }
        }
    }
}

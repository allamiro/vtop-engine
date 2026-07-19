//! Exhaustive deterministic crash, corruption, and fault-injection sweeps
//! over the simulated storage seam. Twins of the real-filesystem tests plus
//! an acknowledgment oracle: every crash-consistent durable state must
//! classify per the frozen startup decision table, keep every Fsync-acked
//! record readable byte-identically below the commit boundary, expose nothing
//! above the boundary, and accept a full-history retry idempotently.

use std::path::{Path, PathBuf};
use uuid::Uuid;
use vtop_log::env::Env;
use vtop_log::sim::{FaultPlan, SimStorage, TraceEntry, TraceKind};
use vtop_log::{
    ActiveSegment, AppendOutcome, CatalogSegmentState, Durability, LogError, LogRecord,
    QuarantineReason, RangeLineage, SegmentConfig, SegmentDescriptor, SegmentReader,
    StartupCatalog,
};

const SEED: u64 = 0x5eed_0093;
const ROOT: &str = "/log";
const BASE_OFFSET: u64 = 40;

fn descriptor() -> SegmentDescriptor {
    SegmentDescriptor {
        segment_id: Uuid::from_u128(1),
        topic: "events.v1".to_owned(),
        topic_epoch: 7,
        lineage: RangeLineage::root(Uuid::from_u128(100)),
        base_offset: BASE_OFFSET,
    }
}

fn config() -> SegmentConfig {
    SegmentConfig {
        max_record_bytes: 1024,
        max_group_bytes: 4096,
        max_segment_bytes: 16 * 1024,
        max_segment_records: 100,
        index_stride: 2,
    }
}

fn record(sequence: u64, value: &[u8]) -> LogRecord {
    LogRecord {
        producer_id: Uuid::from_u128(200),
        sequence,
        timestamp_millis: 1_700_000_000_000 + sequence as i64,
        key: b"key".to_vec(),
        value: value.to_vec(),
    }
}

fn active_path() -> PathBuf {
    Path::new(ROOT).join("sweep.active")
}

struct RunResult {
    appended: Vec<LogRecord>,
    acked_committed: u64,
    completed: bool,
}

/// Drive create -> appends -> optional seal, stopping at the first error.
/// `acked_committed` is the durable frontier acknowledged to producers.
fn run_workload(env: &Env, steps: &[(LogRecord, Durability)], seal: bool) -> RunResult {
    let mut result = RunResult {
        appended: Vec::new(),
        acked_committed: BASE_OFFSET,
        completed: false,
    };
    let mut segment = match ActiveSegment::create_in(env, active_path(), descriptor(), config()) {
        Ok(segment) => segment,
        Err(_) => return result,
    };
    for (record, durability) in steps {
        match segment.append(record.clone(), *durability) {
            Ok(AppendOutcome::Appended { .. }) => {
                result.appended.push(record.clone());
                if matches!(durability, Durability::Fsync) {
                    result.acked_committed = segment.committed_offset();
                }
            }
            Ok(AppendOutcome::Duplicate { .. }) => {
                panic!("workload never retries, so appends cannot be duplicates")
            }
            Err(_) => return result,
        }
    }
    if seal {
        if segment.seal().is_err() {
            return result;
        }
        result.acked_committed = BASE_OFFSET + steps.len() as u64;
    }
    result.completed = true;
    result
}

fn clean_run(steps: &[(LogRecord, Durability)], seal: bool) -> Vec<TraceEntry> {
    let sim = SimStorage::new();
    let run = run_workload(&sim.env(SEED), steps, seal);
    assert!(run.completed, "clean run must finish without faults");
    sim.trace()
}

fn durable_file_names(sim: &SimStorage) -> Vec<String> {
    sim.snapshot()
        .files
        .keys()
        .map(|path| {
            path.file_name()
                .expect("sim files have names")
                .to_string_lossy()
                .into_owned()
        })
        .collect()
}

fn discover_read_only(sim: &SimStorage, env: &Env, context: &str) -> StartupCatalog {
    let before = sim.snapshot();
    let first = StartupCatalog::discover_in(env, ROOT).unwrap_or_else(|error| {
        panic!("discovery failed ({context}): {error}");
    });
    assert_eq!(sim.snapshot(), before, "discovery wrote bytes ({context})");
    let second = StartupCatalog::discover_in(env, ROOT).unwrap();
    assert_eq!(second, first, "discovery is unstable ({context})");
    first
}

fn mixed_steps() -> Vec<(LogRecord, Durability)> {
    vec![
        (record(0, b"a"), Durability::Fsync),
        (record(1, b"bb"), Durability::Buffered),
        (record(2, b"ccc"), Durability::Fsync),
        (record(3, b"dddd"), Durability::Buffered),
        (record(4, b"eeeee"), Durability::Buffered),
        (record(5, b"ffffff"), Durability::Fsync),
    ]
}

/// Twin of startup_crash_matrix.rs: crash at every operation boundary of a
/// create -> mixed appends -> seal workload and check the durable file set
/// classifies per the frozen decision table.
#[test]
fn sim_startup_catalog_classifies_every_crash_boundary_durable_state() {
    let steps = vec![
        (record(0, b"first"), Durability::Fsync),
        (record(1, b"second"), Durability::Buffered),
        (record(2, b"third"), Durability::Fsync),
    ];
    let total = clean_run(&steps, true).len();

    for op in 0..total as u64 {
        let context = format!("op={op} seed={SEED:#x}");
        let sim = SimStorage::new();
        let env = sim.env(SEED);
        sim.set_fault(FaultPlan::CrashBefore(op));
        let run = run_workload(&env, &steps, true);
        assert!(sim.has_crashed(), "fault did not trigger ({context})");
        sim.reboot();

        let names = durable_file_names(&sim);
        let has = |suffix: &str| names.iter().any(|name| name == &format!("sweep{suffix}"));
        let catalog = discover_read_only(&sim, &env, &context);
        for quarantined in &catalog.quarantined {
            for reason in &quarantined.reasons {
                assert!(
                    matches!(
                        reason,
                        QuarantineReason::InvalidArtifact(_)
                            | QuarantineReason::IncompleteAtomicWrite
                    ),
                    "unexpected quarantine reason {reason:?} ({context})"
                );
            }
        }
        if has(".segment") {
            assert!(
                !has(".active"),
                "conflicting primaries reachable ({context})"
            );
            assert_eq!(catalog.entries.len(), 1, "{context}");
            assert_eq!(
                catalog.entries[0].state,
                CatalogSegmentState::Sealed,
                "{context}"
            );
            assert!(catalog.quarantined.is_empty(), "{context}");
        } else if has(".active") && has(".commit") {
            assert_eq!(catalog.entries.len(), 1, "{context}");
            assert_eq!(
                catalog.entries[0].state,
                CatalogSegmentState::Active,
                "{context}"
            );
            assert!(
                catalog.entries[0].next_offset >= run.acked_committed,
                "acked records lost ({context})"
            );
            assert!(catalog.quarantined.is_empty(), "{context}");
        } else if has(".active") {
            // Active file before initial commit-marker publication: ambiguous,
            // quarantined, and nothing was acknowledged yet.
            assert!(catalog.entries.is_empty(), "{context}");
            assert_eq!(run.acked_committed, BASE_OFFSET, "{context}");
            assert_eq!(catalog.quarantined.len(), 1, "{context}");
        } else {
            assert!(catalog.entries.is_empty(), "{context}");
            assert_eq!(run.acked_committed, BASE_OFFSET, "{context}");
        }
    }
}

fn verify_acknowledgment_oracle(
    sim: &SimStorage,
    env: &Env,
    run: &RunResult,
    steps: &[(LogRecord, Durability)],
    context: &str,
) {
    let catalog = discover_read_only(sim, env, context);
    let acked_any = run.acked_committed > BASE_OFFSET;
    for quarantined in &catalog.quarantined {
        for reason in &quarantined.reasons {
            match reason {
                QuarantineReason::IncompleteAtomicWrite => {}
                // A quarantined primary is reachable only before the first
                // commit-marker publication, when nothing was acknowledged.
                QuarantineReason::InvalidArtifact(_) if !acked_any => {}
                other => panic!("unexpected quarantine reason {other:?} ({context})"),
            }
        }
    }
    if catalog.entries.is_empty() {
        assert!(!acked_any, "acknowledged records disappeared ({context})");
        return;
    }
    assert_eq!(catalog.entries.len(), 1, "{context}");
    let entry = &catalog.entries[0];
    match entry.state {
        CatalogSegmentState::Sealed => {
            let mut reader = SegmentReader::open_in(env, &entry.path)
                .unwrap_or_else(|error| panic!("sealed reader failed ({context}): {error}"));
            assert_eq!(
                reader.manifest().record_count,
                steps.len() as u64,
                "{context}"
            );
            let batch = reader.fetch(BASE_OFFSET, usize::MAX, usize::MAX).unwrap();
            assert_eq!(batch.records.len(), steps.len(), "{context}");
            for (index, fetched) in batch.records.iter().enumerate() {
                assert_eq!(fetched.offset, BASE_OFFSET + index as u64, "{context}");
                assert_eq!(fetched.record, steps[index].0, "{context}");
            }
        }
        CatalogSegmentState::Active => {
            let mut segment = ActiveSegment::recover_in(env, &entry.path)
                .unwrap_or_else(|error| panic!("recovery failed ({context}): {error}"));
            let committed = segment.committed_offset();
            assert!(
                committed >= run.acked_committed,
                "commit boundary regressed below acknowledgments ({context})"
            );
            assert!(
                committed <= BASE_OFFSET + steps.len() as u64,
                "boundary covers records that were never appended ({context})"
            );
            let batch = segment.fetch(BASE_OFFSET, usize::MAX, usize::MAX).unwrap();
            assert_eq!(
                batch.records.len(),
                (committed - BASE_OFFSET) as usize,
                "{context}"
            );
            for (index, fetched) in batch.records.iter().enumerate() {
                assert_eq!(fetched.offset, BASE_OFFSET + index as u64, "{context}");
                assert_eq!(
                    fetched.record, steps[index].0,
                    "visible record differs from what was produced ({context})"
                );
            }
            for (index, (record, _)) in steps.iter().enumerate() {
                let offset = BASE_OFFSET + index as u64;
                let outcome = segment
                    .append(record.clone(), Durability::Fsync)
                    .unwrap_or_else(|error| panic!("history retry failed ({context}): {error}"));
                if offset < committed {
                    assert_eq!(outcome, AppendOutcome::Duplicate { offset }, "{context}");
                } else {
                    assert_eq!(outcome, AppendOutcome::Appended { offset }, "{context}");
                }
            }
            let replayed = segment.fetch(BASE_OFFSET, usize::MAX, usize::MAX).unwrap();
            assert_eq!(replayed.records.len(), steps.len(), "{context}");
        }
    }
}

/// Crash before every operation and during every write at every byte cut;
/// the acknowledgment oracle must hold in every reachable durable state.
#[test]
fn sim_acknowledgment_oracle_holds_across_every_crash_point_and_byte_cut() {
    let steps = mixed_steps();
    let trace = clean_run(&steps, true);

    for op in 0..trace.len() as u64 {
        let context = format!("crash-before op={op} seed={SEED:#x}");
        let sim = SimStorage::new();
        let env = sim.env(SEED);
        sim.set_fault(FaultPlan::CrashBefore(op));
        let run = run_workload(&env, &steps, true);
        assert!(sim.has_crashed(), "{context}");
        sim.reboot();
        verify_acknowledgment_oracle(&sim, &env, &run, &steps, &context);
    }

    for entry in trace
        .iter()
        .filter(|entry| entry.kind == TraceKind::HandleWrite)
    {
        for cut in 0..=entry.len as usize {
            let context = format!(
                "torn-write op={} cut={cut} path={} seed={SEED:#x}",
                entry.index,
                entry.path.display()
            );
            let sim = SimStorage::new();
            let env = sim.env(SEED);
            sim.set_fault(FaultPlan::CrashDuringWrite {
                op: entry.index,
                byte_cut: cut,
            });
            let run = run_workload(&env, &steps, true);
            assert!(sim.has_crashed(), "{context}");
            sim.reboot();
            verify_acknowledgment_oracle(&sim, &env, &run, &steps, &context);
        }
    }
}

/// Twin of the real-FS torn-tail test: tear every record write at every byte,
/// recover, and confirm truncation to the commit boundary plus idempotent
/// re-append of the torn suffix.
#[test]
fn sim_recovery_truncates_every_torn_record_write_and_preserves_idempotency() {
    let steps = vec![
        (record(0, b"one"), Durability::Fsync),
        (record(1, b"two-longer"), Durability::Fsync),
        (record(2, b"three"), Durability::Fsync),
    ];
    let create_ops = {
        let sim = SimStorage::new();
        let env = sim.env(SEED);
        drop(ActiveSegment::create_in(&env, active_path(), descriptor(), config()).unwrap());
        sim.op_count()
    };
    let trace = clean_run(&steps, false);
    let record_writes: Vec<&TraceEntry> = trace
        .iter()
        .filter(|entry| {
            entry.kind == TraceKind::HandleWrite
                && entry.path == active_path()
                && entry.index >= create_ops
        })
        .collect();
    assert_eq!(record_writes.len(), steps.len());

    for (already_committed, entry) in record_writes.iter().enumerate() {
        for cut in 0..=entry.len as usize {
            let context = format!("op={} cut={cut} seed={SEED:#x}", entry.index);
            let sim = SimStorage::new();
            let env = sim.env(SEED);
            sim.set_fault(FaultPlan::CrashDuringWrite {
                op: entry.index,
                byte_cut: cut,
            });
            run_workload(&env, &steps, false);
            assert!(sim.has_crashed(), "{context}");
            sim.reboot();

            let boundary = BASE_OFFSET + already_committed as u64;
            let mut segment = ActiveSegment::recover_in(&env, active_path())
                .unwrap_or_else(|error| panic!("recovery failed ({context}): {error}"));
            assert_eq!(segment.committed_offset(), boundary, "{context}");
            assert_eq!(segment.next_offset(), boundary, "{context}");
            assert_eq!(
                segment.recovery_report().truncated_bytes,
                cut as u64,
                "torn bytes were not truncated ({context})"
            );
            for (index, (record, _)) in steps.iter().enumerate().skip(already_committed) {
                assert_eq!(
                    segment.append(record.clone(), Durability::Fsync).unwrap(),
                    AppendOutcome::Appended {
                        offset: BASE_OFFSET + index as u64
                    },
                    "{context}"
                );
            }
            let batch = segment.fetch(BASE_OFFSET, usize::MAX, usize::MAX).unwrap();
            assert_eq!(batch.records.len(), steps.len(), "{context}");
        }
    }
}

/// Every single-byte flip in the durable artifacts of an active segment must
/// surface as corruption or a commit-boundary mismatch, never be accepted.
#[test]
fn sim_single_byte_corruption_of_active_artifacts_is_always_detected() {
    let steps = vec![
        (record(0, b"guarded"), Durability::Fsync),
        (record(1, b"protected"), Durability::Fsync),
    ];
    let sim = SimStorage::new();
    let env = sim.env(SEED);
    let run = run_workload(&env, &steps, false);
    assert!(run.completed);
    let pristine = sim.snapshot();
    let commit_path = Path::new(ROOT).join("sweep.commit");

    for path in [active_path(), commit_path] {
        let length = pristine.files[&path].len();
        for byte_index in 0..length {
            let context = format!("path={} byte={byte_index}", path.display());
            sim.restore(&pristine);
            sim.corrupt(&path, byte_index, 0xff);

            let catalog = discover_read_only(&sim, &env, &context);
            assert!(
                catalog.entries.is_empty(),
                "corruption accepted ({context})"
            );
            assert_eq!(catalog.quarantined.len(), 1, "{context}");
            assert!(
                catalog.quarantined[0]
                    .reasons
                    .iter()
                    .any(|reason| matches!(reason, QuarantineReason::InvalidArtifact(_))),
                "{context}: {:?}",
                catalog.quarantined[0].reasons
            );
            let error = match ActiveSegment::recover_in(&env, active_path()) {
                Ok(_) => panic!("recovery accepted corruption ({context})"),
                Err(error) => error,
            };
            assert!(
                matches!(
                    error,
                    LogError::Corrupt { .. } | LogError::CommitBoundaryMismatch(_)
                ),
                "{context}: {error}"
            );
        }
    }
}

/// Sealed-artifact flips: segment and manifest corruption is quarantined;
/// index corruption is detected and rebuilt to pristine bytes on open; the
/// commit marker is not consulted after sealing (frozen v1 behavior).
#[test]
fn sim_single_byte_corruption_of_sealed_artifacts_is_never_silently_accepted() {
    let steps = vec![
        (record(0, b"sealed-a"), Durability::Fsync),
        (record(1, b"sealed-b"), Durability::Fsync),
    ];
    let sim = SimStorage::new();
    let env = sim.env(SEED);
    let run = run_workload(&env, &steps, true);
    assert!(run.completed);
    let pristine = sim.snapshot();
    let segment_path = Path::new(ROOT).join("sweep.segment");
    let manifest_path = Path::new(ROOT).join("sweep.manifest.json");
    let index_path = Path::new(ROOT).join("sweep.index");
    let commit_path = Path::new(ROOT).join("sweep.commit");

    for path in [&segment_path, &manifest_path] {
        let length = pristine.files[path].len();
        for byte_index in 0..length {
            let context = format!("path={} byte={byte_index}", path.display());
            sim.restore(&pristine);
            sim.corrupt(path, byte_index, 0xff);
            let catalog = discover_read_only(&sim, &env, &context);
            assert!(
                catalog.entries.is_empty(),
                "corruption accepted ({context})"
            );
            assert_eq!(catalog.quarantined.len(), 1, "{context}");
            assert!(
                SegmentReader::open_in(&env, &segment_path).is_err(),
                "{context}"
            );
        }
    }

    let index_length = pristine.files[&index_path].len();
    for byte_index in 0..index_length {
        let context = format!("index byte={byte_index}");
        sim.restore(&pristine);
        sim.corrupt(&index_path, byte_index, 0xff);
        let mut reader = SegmentReader::open_in(&env, &segment_path)
            .unwrap_or_else(|error| panic!("index must be rebuildable ({context}): {error}"));
        assert_eq!(
            sim.snapshot().files[&index_path],
            pristine.files[&index_path],
            "index was not rebuilt to canonical bytes ({context})"
        );
        let batch = reader.fetch(BASE_OFFSET, usize::MAX, usize::MAX).unwrap();
        assert_eq!(batch.records.len(), steps.len(), "{context}");
    }

    let commit_length = pristine.files[&commit_path].len();
    for byte_index in 0..commit_length {
        sim.restore(&pristine);
        sim.corrupt(&commit_path, byte_index, 0xff);
        let catalog = discover_read_only(&sim, &env, "sealed commit flip");
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.entries[0].state, CatalogSegmentState::Sealed);
    }
}

/// Inject one storage error at every operation of a durable append workload:
/// the error must surface with its kind intact, poisoned writers must refuse
/// further appends, and recovery must always restore a consistent state.
#[test]
fn sim_injected_storage_errors_surface_poison_and_stay_recoverable() {
    let first = record(0, b"first");
    let second = record(1, b"second");
    let steps = vec![
        (first.clone(), Durability::Fsync),
        (second.clone(), Durability::Fsync),
    ];
    let total = clean_run(&steps, false).len();

    for kind in [
        std::io::ErrorKind::PermissionDenied,
        std::io::ErrorKind::WriteZero,
    ] {
        for op in 0..total as u64 {
            let context = format!("op={op} kind={kind:?} seed={SEED:#x}");
            let sim = SimStorage::new();
            let env = sim.env(SEED);
            sim.set_fault(FaultPlan::FailOp { op, kind });

            let assert_injected = |error: &LogError, context: &str| match error {
                LogError::Io { source, .. } => {
                    assert_eq!(source.kind(), kind, "{context}");
                }
                other => panic!("expected injected Io error ({context}), got {other}"),
            };

            let mut segment =
                match ActiveSegment::create_in(&env, active_path(), descriptor(), config()) {
                    Ok(segment) => segment,
                    Err(error) => {
                        assert_injected(&error, &context);
                        discover_read_only(&sim, &env, &context);
                        continue;
                    }
                };
            let mut failed_at = None;
            for (index, (record, durability)) in steps.iter().enumerate() {
                match segment.append(record.clone(), *durability) {
                    Ok(AppendOutcome::Appended { .. }) => {}
                    Ok(outcome) => panic!("unexpected outcome {outcome:?} ({context})"),
                    Err(error) => {
                        assert_injected(&error, &context);
                        failed_at = Some(index);
                        break;
                    }
                }
            }
            let Some(failed_at) = failed_at else {
                assert!(
                    op >= total as u64,
                    "no step surfaced the injection ({context})"
                );
                continue;
            };

            let retried = segment.append(steps[failed_at].0.clone(), Durability::Fsync);
            match retried {
                Ok(AppendOutcome::Appended { offset }) => {
                    assert_eq!(offset, BASE_OFFSET + failed_at as u64, "{context}");
                }
                Err(LogError::WriterPoisoned) => {}
                other => panic!("retry must append or report poisoning ({context}): {other:?}"),
            }
            drop(segment);

            let mut recovered = ActiveSegment::recover_in(&env, active_path())
                .unwrap_or_else(|error| panic!("recovery failed ({context}): {error}"));
            assert!(
                recovered.committed_offset() >= BASE_OFFSET + failed_at as u64,
                "acked records lost ({context})"
            );
            for (index, (record, _)) in steps.iter().enumerate() {
                let offset = BASE_OFFSET + index as u64;
                let outcome = recovered.append(record.clone(), Durability::Fsync).unwrap();
                assert_eq!(outcome.offset(), offset, "{context}");
            }
            let batch = recovered
                .fetch(BASE_OFFSET, usize::MAX, usize::MAX)
                .unwrap();
            assert_eq!(batch.records.len(), steps.len(), "{context}");
        }
    }
}

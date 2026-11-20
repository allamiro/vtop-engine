//! A crash at every storage operation of a commit group (#752).
//!
//! System model: one process, crash-recovery, on the deterministic storage
//! simulator. Safety only: after a crash at ANY operation of a group of
//! three produce requests sharing one barrier, recovery finds a log that is a
//! gap-free prefix of the group ending on a record boundary, and every
//! request that was acknowledged before the crash is in it. Nothing is
//! acknowledged before the group's final barrier, so an acknowledged member
//! means the whole group survived.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use uuid::Uuid;
use vtop_broker::group_commit::GroupCommitConfig;
use vtop_broker::{LocalBroker, ProducerEpochJournal};
use vtop_log::env::Env;
use vtop_log::sim::{FaultPlan, SimStorage, TraceKind};
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
use vtop_protocol::{
    Durability as WireDurability, Message, ProduceRecord, ProduceRequest, RangeIdentity, Role,
    WireFrame,
};

const SEED: u64 = 0x5eed_0752;
const EPOCH: u64 = 3;
const MEMBERS: u128 = 3;

fn range() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

fn active_path() -> PathBuf {
    Path::new("/range").join("range.active")
}

fn value_of(member: u128) -> Vec<u8> {
    format!("member-{member}").into_bytes()
}

/// A broker on `env` whose group commit seals at exactly three requests, so
/// the three always share one group whatever the threads' timing.
fn broker_on(env: &Env) -> Arc<LocalBroker> {
    let range = range();
    let segment = ActiveSegment::create_in(
        env,
        active_path(),
        SegmentDescriptor {
            segment_id: Uuid::from_u128(0xD1),
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
    .expect("the segment is created before any fault is armed");
    let epochs = ProducerEpochJournal::open_in(env, Path::new("/range").join("epochs"))
        .expect("the journal opens before any fault is armed");
    Arc::new(
        LocalBroker::new(segment, epochs, range, EPOCH)
            .unwrap()
            .with_group_commit(GroupCommitConfig {
                max_delay: Duration::from_secs(10),
                max_records: MEMBERS as usize,
                max_bytes: 1024 * 1024,
                max_pending_requests: 32,
            })
            .unwrap(),
    )
}

/// The three requests, released together. Returns which members were
/// acknowledged.
fn run_group(broker: &Arc<LocalBroker>) -> BTreeSet<u128> {
    let barrier = Arc::new(Barrier::new(MEMBERS as usize));
    let handles: Vec<_> = (0..MEMBERS)
        .map(|member| {
            let broker = Arc::clone(broker);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let frame = WireFrame {
                    request_id: member as u64 + 1,
                    stream_id: 1,
                    message: Message::ProduceRequest(ProduceRequest {
                        range: range(),
                        fencing_epoch: EPOCH,
                        producer_id: Uuid::from_u128(0xB0 + member),
                        producer_epoch: 1,
                        first_sequence: 0,
                        durability: WireDurability::LocalFsync,
                        records: vec![ProduceRecord {
                            timestamp_millis: 1,
                            key: b"k".to_vec(),
                            value: value_of(member),
                        }],
                    }),
                };
                let answer = broker.handle(Role::Producer, frame);
                matches!(answer.message, Message::ProduceResponse(_)).then_some(member)
            })
        })
        .collect();
    handles
        .into_iter()
        .filter_map(|handle| handle.join().unwrap())
        .collect()
}

/// What recovery finds: each record's offset and which member wrote it.
fn recovered(env: &Env, context: &str) -> Vec<(u64, u128)> {
    let mut segment = ActiveSegment::recover_in(env, active_path())
        .unwrap_or_else(|error| panic!("recovery failed ({context}): {error}"));
    let batch = segment
        .fetch(0, usize::MAX, usize::MAX)
        .unwrap_or_else(|error| panic!("fetch after recovery failed ({context}): {error}"));
    batch
        .records
        .into_iter()
        .map(|fetched| {
            let member = (0..MEMBERS)
                .find(|member| fetched.record.value == value_of(*member))
                .unwrap_or_else(|| {
                    panic!(
                        "recovery exposed a record no member wrote at offset {} ({context}): \
                         {:?}",
                        fetched.offset, fetched.record.value
                    )
                });
            (fetched.offset, member)
        })
        .collect()
}

fn check(env: &Env, acked: &BTreeSet<u128>, context: &str) {
    let log = recovered(env, context);
    let offsets: Vec<u64> = log.iter().map(|(offset, _)| *offset).collect();
    let expected: Vec<u64> = (0..log.len() as u64).collect();
    assert_eq!(
        offsets, expected,
        "the recovered log must be a gap-free prefix from offset 0 ({context})"
    );
    let members: BTreeSet<u128> = log.iter().map(|(_, member)| *member).collect();
    assert_eq!(
        members.len(),
        log.len(),
        "a member's record appears twice ({context}): {log:?}"
    );
    for member in acked {
        assert!(
            members.contains(member),
            "member {member} was acknowledged and is missing after recovery ({context}): \
             recovered {log:?}"
        );
    }
}

#[test]
fn a_crash_at_any_operation_of_a_group_loses_no_acknowledged_member() {
    // The clean run: where the group's operations start and end.
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new("/range"));
    let env = sim.env(SEED);
    let broker = broker_on(&env);
    let first = sim.op_count();
    let acked = run_group(&broker);
    assert_eq!(
        acked.len(),
        MEMBERS as usize,
        "the clean run acknowledges every member"
    );
    assert_eq!(broker.group_commit().unwrap().metrics().commits_total(), 1);
    let last = sim.op_count();
    let writes: Vec<_> = sim
        .trace()
        .into_iter()
        .filter(|entry| entry.index >= first && entry.kind == TraceKind::HandleWrite)
        .collect();
    assert!(last > first, "the group performed storage operations");
    drop(broker);

    let mut swept = 0;
    for op in first..last {
        let context = format!("crash before op {op} of {first}..{last}, seed {SEED:#x}");
        let sim = SimStorage::new();
        sim.create_dir_all(Path::new("/range"));
        let env = sim.env(SEED);
        let broker = broker_on(&env);
        sim.set_fault(FaultPlan::CrashBefore(op));
        let acked = run_group(&broker);
        assert!(sim.has_crashed(), "the fault did not trigger ({context})");
        drop(broker);
        sim.reboot();
        check(&env, &acked, &context);
        swept += 1;
    }
    for write in &writes {
        for byte_cut in [
            0,
            1,
            write.len as usize / 2,
            (write.len as usize).saturating_sub(1),
        ] {
            let context = format!(
                "crash during write op {} at byte {byte_cut} of {}, seed {SEED:#x}",
                write.index, write.len
            );
            let sim = SimStorage::new();
            sim.create_dir_all(Path::new("/range"));
            let env = sim.env(SEED);
            let broker = broker_on(&env);
            sim.set_fault(FaultPlan::CrashDuringWrite {
                op: write.index,
                byte_cut,
            });
            let acked = run_group(&broker);
            assert!(sim.has_crashed(), "the fault did not trigger ({context})");
            drop(broker);
            sim.reboot();
            check(&env, &acked, &context);
            swept += 1;
        }
    }
    assert!(swept > 0);
}

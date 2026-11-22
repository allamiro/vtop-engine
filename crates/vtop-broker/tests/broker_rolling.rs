//! The broker over a rolling range (#270).
//!
//! `SegmentSet` proved a range rolls and reads as one at the storage layer;
//! these tests pin the same promises at the BROKER's surface, where a client
//! actually lives: a producer at the bound sees an ack rather than
//! `SegmentByteLimit`, offsets stay contiguous across every roll, a fetch
//! crosses sealed/active boundaries byte-exactly, a restart recovers the
//! rolled range through the catalog with the producer continuing mid-stream,
//! and quorum replication neither knows nor cares where each replica's
//! boundaries fell.
//!
//! Configs here are deliberately tiny (512-byte segments) so a handful of
//! records forces several rolls — the roll path is exercised, not described.

use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;
use vtop_broker::replication::{
    ClusterCommittedOffset, InProcessFollower, InProcessReplicaSet, ReplicaSet,
};
use vtop_broker::{LocalBroker, MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::env::Env;
use vtop_log::{KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor, SegmentSet};
use vtop_protocol::{
    Durability as WireDurability, ErrorCode, FetchRequest, Message, ProduceRecord, ProduceRequest,
    RangeIdentity, Role, WireFrame,
};

const PRODUCER: Uuid = Uuid::from_u128(0xB1);
const FENCING_EPOCH: u64 = 7;

fn range_identity() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

fn descriptor(segment_id: u128, range: &RangeIdentity) -> SegmentDescriptor {
    SegmentDescriptor {
        segment_id: Uuid::from_u128(segment_id),
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

/// Small enough that a handful of records fills a segment.
fn small_config() -> SegmentConfig {
    SegmentConfig {
        max_record_bytes: 256,
        max_group_bytes: 512,
        max_segment_bytes: 512,
        max_segment_records: 100,
        index_stride: 2,
    }
}

fn create_set(dir: &TempDir, segment_id: u128, range: &RangeIdentity) -> SegmentSet {
    SegmentSet::create_in(
        &Env::real(),
        dir.path(),
        descriptor(segment_id, range),
        small_config(),
    )
    .unwrap()
}

fn value(sequence: u64) -> Vec<u8> {
    format!("value-{sequence:04}").into_bytes()
}

fn produce_frame(
    range: &RangeIdentity,
    durability: WireDurability,
    first_sequence: u64,
    values: Vec<Vec<u8>>,
    request_id: u64,
) -> WireFrame {
    WireFrame {
        request_id,
        stream_id: 1,
        message: Message::ProduceRequest(ProduceRequest {
            range: range.clone(),
            fencing_epoch: FENCING_EPOCH,
            producer_id: PRODUCER,
            producer_epoch: 1,
            first_sequence,
            durability,
            records: values
                .into_iter()
                .map(|value| ProduceRecord {
                    timestamp_millis: 42,
                    key: b"key".to_vec(),
                    value,
                })
                .collect(),
        }),
    }
}

fn fetch_frame(range: &RangeIdentity, start_offset: u64, request_id: u64) -> WireFrame {
    WireFrame {
        request_id,
        stream_id: 1,
        message: Message::FetchRequest(FetchRequest {
            range: range.clone(),
            fencing_epoch: FENCING_EPOCH,
            start_offset,
            max_bytes: 1 << 20,
            max_records: 1000,
        }),
    }
}

fn sealed_segment_count(dir: &TempDir) -> usize {
    std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "segment"))
        .count()
}

/// Producing past the bound is a boundary, not an error: every produce is
/// acked, offsets are contiguous through every roll, and one fetch returns
/// the whole range byte-exactly across sealed/active boundaries.
#[test]
fn a_produce_past_the_bound_rolls_instead_of_erroring() {
    let dir = tempfile::tempdir().unwrap();
    let range = range_identity();
    let set = create_set(&dir, 0xD1, &range);
    let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
    let broker = LocalBroker::new(set, epochs, range.clone(), FENCING_EPOCH).unwrap();

    for sequence in 0..40 {
        let response = broker.handle(
            Role::Producer,
            produce_frame(
                &range,
                WireDurability::LocalFsync,
                sequence,
                vec![value(sequence)],
                sequence + 1,
            ),
        );
        let Message::ProduceResponse(ack) = response.message else {
            panic!(
                "produce {sequence} must be acked, not refused: {:?}",
                response.message
            );
        };
        assert_eq!(
            ack.outcomes[0].offset, sequence,
            "offsets must stay contiguous across rolls"
        );
        assert_eq!(ack.committed_next_offset, sequence + 1);
    }
    assert!(
        sealed_segment_count(&dir) >= 1,
        "40 records into 512-byte segments must have rolled at least once"
    );

    let response = broker.handle(Role::Consumer, fetch_frame(&range, 0, 100));
    let Message::FetchResponse(batch) = response.message else {
        panic!("expected a fetch response: {:?}", response.message);
    };
    assert_eq!(
        batch.records.len(),
        40,
        "one fetch must return the whole range, not the first segment's worth"
    );
    assert_eq!(batch.committed_high_watermark, 40);
    for (index, record) in batch.records.iter().enumerate() {
        assert_eq!(record.offset, index as u64);
        assert_eq!(
            record.value,
            value(index as u64),
            "records must come back byte-exact across boundaries"
        );
        assert_eq!(record.key, b"key");
    }

    // A fetch STARTING inside a sealed segment crosses into the tail too.
    let response = broker.handle(Role::Consumer, fetch_frame(&range, 2, 101));
    let Message::FetchResponse(batch) = response.message else {
        panic!("expected a fetch response");
    };
    assert_eq!(batch.records.first().unwrap().offset, 2);
    assert_eq!(batch.records.last().unwrap().offset, 39);
}

/// After a restart, the rolled range is recovered THROUGH THE CATALOG and the
/// same producer continues mid-stream — no `FirstSequence`, because the tail's
/// `.producers` sidecar carries the frontier it inherited across the rolls.
#[test]
fn a_restart_after_rolling_recovers_through_the_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let range = range_identity();
    {
        let set = create_set(&dir, 0xD2, &range);
        let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
        let broker = LocalBroker::new(set, epochs, range.clone(), FENCING_EPOCH).unwrap();
        for sequence in 0..40 {
            let response = broker.handle(
                Role::Producer,
                produce_frame(
                    &range,
                    WireDurability::LocalFsync,
                    sequence,
                    vec![value(sequence)],
                    sequence + 1,
                ),
            );
            assert!(matches!(response.message, Message::ProduceResponse(_)));
        }
    }

    // The epochs journal lives beside the segments, exactly as a data node
    // lays its directory out; discovery must ignore what is not its business.
    let reopened = SegmentSet::open_in(&Env::real(), dir.path())
        .unwrap()
        .expect("the rolled range must reopen through the catalog");
    assert!(
        !reopened.sealed().is_empty(),
        "the reopened set must carry its sealed prefix"
    );
    let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
    let broker = LocalBroker::new(reopened, epochs, range.clone(), FENCING_EPOCH).unwrap();

    let response = broker.handle(
        Role::Producer,
        produce_frame(&range, WireDurability::LocalFsync, 40, vec![value(40)], 1),
    );
    let Message::ProduceResponse(ack) = response.message else {
        panic!(
            "the producer's next sequence must be accepted after restart: {:?}",
            response.message
        );
    };
    assert_eq!(ack.outcomes[0].offset, 40);

    let response = broker.handle(Role::Consumer, fetch_frame(&range, 0, 2));
    let Message::FetchResponse(batch) = response.message else {
        panic!("expected a fetch response");
    };
    assert_eq!(batch.records.len(), 41);
}

/// A single group larger than the bound is a configuration error, not a roll
/// trigger: the producer sees a refusal, never an endless roll.
///
/// A group cannot even claim to be "larger than a whole segment" through a
/// valid config — `SegmentConfig` refuses `max_segment_bytes` below
/// `max_group_bytes`, and the group bound is checked BEFORE any bytes are
/// written — so the refusal that reaches a producer is `GroupTooLarge`, and
/// rolling must not be triggered by it: an empty successor would refuse the
/// same group identically.
#[test]
fn a_group_larger_than_the_bound_is_still_refused() {
    let dir = tempfile::tempdir().unwrap();
    let range = range_identity();
    let set = create_set(&dir, 0xD3, &range);
    let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
    let broker = LocalBroker::new(set, epochs, range.clone(), FENCING_EPOCH).unwrap();

    let response = broker.handle(
        Role::Producer,
        produce_frame(&range, WireDurability::LocalFsync, 0, vec![value(0)], 1),
    );
    assert!(matches!(response.message, Message::ProduceResponse(_)));

    // Eight ~100-byte record frames cannot fit the 512-byte group bound.
    let oversize: Vec<Vec<u8>> = (0..8).map(|_| vec![b'x'; 64]).collect();
    let response = broker.handle(
        Role::Producer,
        produce_frame(&range, WireDurability::LocalFsync, 1, oversize, 2),
    );
    let Message::Error(problem) = response.message else {
        panic!(
            "a group larger than the bound must be refused: {:?}",
            response.message
        );
    };
    assert_eq!(problem.code, ErrorCode::Storage);
    assert_eq!(
        sealed_segment_count(&dir),
        0,
        "an oversize group is a refusal, never a roll trigger"
    );

    // The refusal wrote nothing: the same sequence retries with a sane batch
    // and lands at the next contiguous offset.
    let response = broker.handle(
        Role::Producer,
        produce_frame(&range, WireDurability::LocalFsync, 1, vec![value(1)], 3),
    );
    let Message::ProduceResponse(ack) = response.message else {
        panic!("the range must stay usable after an oversize refusal");
    };
    assert_eq!(ack.outcomes[0].offset, 1);
}

/// Quorum produce across rolls: the leader replicates offsets, not files, so
/// each replica rolls at its own bound and the quorum/HWM semantics do not
/// change. Every replica then serves the same records across its own
/// boundaries.
#[test]
fn quorum_produce_rolls_on_leader_and_followers() {
    let range = range_identity();
    let meta = MetaFencingEpoch::new(FENCING_EPOCH);
    let cluster_committed = ClusterCommittedOffset::new(0);

    let leader_dir = tempfile::tempdir().unwrap();
    let leader_set = create_set(&leader_dir, 0xD4, &range);
    let leader_epochs = ProducerEpochJournal::open(leader_dir.path().join("epochs")).unwrap();

    let mut follower_dirs = Vec::new();
    let mut followers = Vec::new();
    for index in 0..2_u128 {
        let dir = tempfile::tempdir().unwrap();
        let set = create_set(&dir, 0xE1 + index, &range);
        let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
        followers.push(Arc::new(
            InProcessFollower::new(
                Uuid::from_u128(0xA2 + index),
                set,
                epochs,
                range.clone(),
                FENCING_EPOCH,
                meta.clone(),
                ClusterCommittedOffset::new(0),
            )
            .unwrap(),
        ));
        follower_dirs.push(dir);
    }
    let replica_set = Arc::new(InProcessReplicaSet::new(followers.clone()));
    let leader = LocalBroker::with_replication(
        leader_set,
        leader_epochs,
        range.clone(),
        FENCING_EPOCH,
        meta,
        Uuid::from_u128(0xA1),
        Some(cluster_committed.clone()),
        Some(replica_set as Arc<dyn ReplicaSet>),
    )
    .unwrap();

    for sequence in 0..40 {
        let response = leader.handle(
            Role::Producer,
            produce_frame(
                &range,
                WireDurability::Quorum,
                sequence,
                vec![value(sequence)],
                sequence + 1,
            ),
        );
        let Message::ProduceResponse(ack) = response.message else {
            panic!(
                "quorum produce {sequence} must be acked across rolls: {:?}",
                response.message
            );
        };
        assert_eq!(ack.outcomes[0].offset, sequence);
        assert_eq!(
            ack.committed_next_offset,
            sequence + 1,
            "the quorum HWM must advance through every roll"
        );
    }
    assert_eq!(cluster_committed.get(), 40);
    assert!(
        sealed_segment_count(&leader_dir) >= 1,
        "the leader must have rolled"
    );
    for dir in &follower_dirs {
        assert!(
            sealed_segment_count(dir) >= 1,
            "each follower must have rolled at its own bound"
        );
    }

    // The leader serves the whole range across its boundaries...
    let response = leader.handle(Role::Consumer, fetch_frame(&range, 0, 100));
    let Message::FetchResponse(batch) = response.message else {
        panic!("expected a fetch response");
    };
    assert_eq!(batch.records.len(), 40);
    assert_eq!(batch.committed_high_watermark, 40);
    for (index, record) in batch.records.iter().enumerate() {
        assert_eq!(record.offset, index as u64);
        assert_eq!(record.value, value(index as u64));
    }

    // ...and so does each follower, across its OWN boundaries, which need
    // not fall where the leader's did.
    for follower in &followers {
        let batch = follower.fetch(0, 1 << 20, 1000).unwrap();
        assert_eq!(batch.records.len(), 40);
        for (index, fetched) in batch.records.iter().enumerate() {
            assert_eq!(fetched.offset, index as u64);
            assert_eq!(fetched.record.value, value(index as u64));
        }
    }
}

/// A disk that fills while the tail rolls, at every storage operation of the
/// roll (#881, finding 1). The cluster soak filled a data node's directory at
/// a roll, and from then on every request to that node panicked — `/metrics`
/// included — until it was restarted, even after the disk was freed.
///
/// The producer must get a storage error, never a panic; offsets, the
/// retained bounds and fetches must keep answering while the disk is full;
/// and once space is freed the same broker, with no restart, must take the
/// retried record at the offset it was refused at and keep rolling.
#[test]
fn a_produce_whose_roll_hits_a_full_disk_is_refused_and_recovers_once_space_is_freed() {
    use vtop_log::sim::{FaultPlan, SimStorage};

    const ROOT: &str = "/range";
    let range = range_identity();
    let build = |sim: &SimStorage, dir: &TempDir| {
        sim.create_dir_all(std::path::Path::new(ROOT));
        let set = SegmentSet::create_in(
            &sim.env(0x5eed_0881),
            ROOT,
            descriptor(0xD9, &range),
            small_config(),
        )
        .unwrap();
        let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
        LocalBroker::new(set, epochs, range.clone(), FENCING_EPOCH).unwrap()
    };
    let produce = |broker: &LocalBroker, sequence: u64, request_id: u64| {
        broker.handle(
            Role::Producer,
            produce_frame(
                &range,
                WireDurability::LocalFsync,
                sequence,
                vec![value(sequence)],
                request_id,
            ),
        )
    };
    let acked_offset = |response: WireFrame, context: &str| match response.message {
        Message::ProduceResponse(ack) => ack.outcomes[0].offset,
        other => panic!("expected an ack ({context}): {other:?}"),
    };

    // Clean run: the first produce whose append rolls, and its storage ops.
    let (rolling_sequence, roll_start, roll_end) = {
        let sim = SimStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let broker = build(&sim, &dir);
        let mut sequence = 0;
        loop {
            let sealed_before = broker.sealed_segment_handles().len();
            let start = sim.op_count();
            acked_offset(produce(&broker, sequence, sequence + 1), "clean run");
            if broker.sealed_segment_handles().len() > sealed_before {
                break (sequence, start, sim.op_count());
            }
            sequence += 1;
        }
    };

    for op in roll_start..roll_end {
        let context = format!("disk full from op={op}");
        let sim = SimStorage::new();
        let dir = tempfile::tempdir().unwrap();
        let broker = build(&sim, &dir);
        for sequence in 0..rolling_sequence {
            acked_offset(produce(&broker, sequence, sequence + 1), &context);
        }

        sim.set_fault(FaultPlan::FailOpsFrom {
            op,
            kind: std::io::ErrorKind::StorageFull,
        });
        let acked = match produce(&broker, rolling_sequence, 1_000).message {
            Message::ProduceResponse(_) => rolling_sequence + 1,
            Message::Error(problem) => {
                assert_eq!(problem.code, ErrorCode::Storage, "{context}");
                rolling_sequence
            }
            other => panic!("unexpected response ({context}): {other:?}"),
        };

        // Still full: everything a status or metrics request reads answers.
        assert_eq!(broker.local_offsets(), (acked, acked), "{context}");
        assert_eq!(broker.retained_bounds().0, 0, "{context}");
        let _ = broker.sealed_segment_handles();
        let _ = broker.handle(Role::Consumer, fetch_frame(&range, 0, 1_001));
        let Message::Error(problem) = produce(&broker, acked, 1_002).message else {
            panic!("a produce to a full disk must be refused ({context})");
        };
        assert_eq!(problem.code, ErrorCode::Storage, "{context}");

        // Space is freed. The same broker takes the retried record where it
        // was refused, and keeps going through another roll.
        sim.set_fault(FaultPlan::None);
        let resumed_to = acked + 12;
        for sequence in acked..resumed_to {
            assert_eq!(
                acked_offset(produce(&broker, sequence, 2_000 + sequence), &context),
                sequence,
                "{context}"
            );
        }
        let Message::FetchResponse(batch) = broker
            .handle(Role::Consumer, fetch_frame(&range, 0, 3_000))
            .message
        else {
            panic!("the range must read once space is freed ({context})");
        };
        assert_eq!(batch.records.len() as u64, resumed_to, "{context}");
        for (index, record) in batch.records.iter().enumerate() {
            assert_eq!(record.offset, index as u64, "{context}");
            assert_eq!(record.value, value(index as u64), "{context}");
        }
    }
}

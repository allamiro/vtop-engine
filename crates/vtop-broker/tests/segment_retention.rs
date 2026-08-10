//! Retention at the broker's surface (#290).
//!
//! The storage layer proved a bounded range reclaims its oldest sealed
//! segments and stays contiguous; these tests pin what a CLIENT and a
//! repairing PEER see: a long-running range holds its size instead of
//! growing until the disk does not, a consumer whose cursor points into the
//! reclaimed prefix gets `OffsetRetained` rather than a silent skip, and a
//! transfer chunk for a reclaimed segment fails closed as `WrongLineage`
//! exactly as it does for any segment that vanished mid-transfer (#278).

use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;
use vtop_broker::replication::{LeaderSegmentTransferHandler, ReplicaPeerHandler};
use vtop_broker::{LocalBroker, ProducerEpochJournal};
use vtop_log::env::Env;
use vtop_log::{
    KeyRange, RangeLineage, RetentionPolicy, SegmentConfig, SegmentDescriptor, SegmentSet,
};
use vtop_protocol::{
    Durability as WireDurability, ErrorCode, FetchRequest, Message, ProduceRecord, ProduceRequest,
    RangeIdentity, Role, WireFrame,
};

const PRODUCER: Uuid = Uuid::from_u128(0xB1);
const PEER: Uuid = Uuid::from_u128(0xF00D);
const FENCING_EPOCH: u64 = 7;

fn range_identity() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

fn small_config() -> SegmentConfig {
    SegmentConfig {
        max_record_bytes: 256,
        max_group_bytes: 512,
        max_segment_bytes: 512,
        max_segment_records: 100,
        index_stride: 2,
    }
}

fn bounded_broker(dir: &TempDir, max_total_bytes: u64) -> LocalBroker {
    let range = range_identity();
    let descriptor = SegmentDescriptor {
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
    };
    let segment =
        SegmentSet::create_in(&Env::real(), dir.path(), descriptor, small_config()).unwrap();
    let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
    let broker = LocalBroker::new(segment, epochs, range, FENCING_EPOCH).unwrap();
    broker.set_retention(Some(RetentionPolicy { max_total_bytes }));
    broker
}

fn produce(broker: &LocalBroker, sequence: u64) {
    let range = range_identity();
    let response = broker.handle(
        Role::Producer,
        WireFrame {
            request_id: sequence + 1,
            stream_id: 1,
            message: Message::ProduceRequest(ProduceRequest {
                range,
                fencing_epoch: FENCING_EPOCH,
                producer_id: PRODUCER,
                producer_epoch: 1,
                first_sequence: sequence,
                durability: WireDurability::LocalFsync,
                records: vec![ProduceRecord {
                    timestamp_millis: 42,
                    key: b"key".to_vec(),
                    value: format!("value-{sequence:04}").into_bytes(),
                }],
            }),
        },
    );
    match response.message {
        Message::ProduceResponse(_) => {}
        other => panic!("produce {sequence} failed: {other:?}"),
    }
}

fn sealed_segment_count(dir: &TempDir) -> usize {
    std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "segment"))
        .count()
}

/// The claim of #290 at the surface that matters: a long-running bounded
/// range HOLDS ITS SIZE. Without retention this workload accumulates one
/// sealed segment per roll forever; with it, the sealed count plateaus while
/// every produce is still acked and the kept suffix still serves.
#[test]
fn a_bounded_range_holds_its_size_while_producing_indefinitely() {
    let dir = TempDir::new().unwrap();
    // Roughly three small segments' worth of frames.
    let broker = bounded_broker(&dir, 1536);

    let mut plateau = 0_usize;
    for sequence in 0..200 {
        produce(&broker, sequence);
        plateau = plateau.max(sealed_segment_count(&dir));
    }

    assert!(
        plateau <= 4,
        "the sealed prefix must plateau under the bound, reached {plateau} segments"
    );
    let handles = broker.sealed_segment_handles();
    let earliest = handles.first().expect("some prefix survives").base_offset;
    assert!(
        earliest > 0,
        "the front must have moved; a range that never reclaims is the bug this closes"
    );
    // The kept suffix still serves, from its new earliest through the tail.
    let response = broker.handle(
        Role::Consumer,
        WireFrame {
            request_id: 999,
            stream_id: 1,
            message: Message::FetchRequest(FetchRequest {
                range: range_identity(),
                fencing_epoch: FENCING_EPOCH,
                start_offset: earliest,
                max_bytes: 1 << 20,
                max_records: 10_000,
            }),
        },
    );
    match response.message {
        Message::FetchResponse(batch) => {
            assert!(!batch.records.is_empty());
            assert_eq!(batch.records.first().unwrap().offset, earliest);
        }
        other => panic!("the retained range must still serve: {other:?}"),
    }
}

/// A consumer at a reclaimed offset is told the records are gone —
/// `OffsetRetained`, naming both offsets in its message — never handed the
/// new front as if nothing were missing, and never a generic storage fault.
#[test]
fn a_fetch_below_the_retained_base_is_refused_as_offset_retained() {
    let dir = TempDir::new().unwrap();
    let broker = bounded_broker(&dir, 1536);
    for sequence in 0..200 {
        produce(&broker, sequence);
    }
    let earliest = broker.sealed_segment_handles().first().unwrap().base_offset;
    assert!(earliest > 0);

    let response = broker.handle(
        Role::Consumer,
        WireFrame {
            request_id: 1000,
            stream_id: 1,
            message: Message::FetchRequest(FetchRequest {
                range: range_identity(),
                fencing_epoch: FENCING_EPOCH,
                start_offset: 0,
                max_bytes: 1 << 20,
                max_records: 10,
            }),
        },
    );
    match response.message {
        Message::Error(error) => {
            assert_eq!(error.code, ErrorCode::OffsetRetained, "{}", error.message);
            assert!(
                error.message.contains("reclaimed by retention"),
                "the error must say what happened: {}",
                error.message
            );
        }
        other => panic!("a reclaimed offset must be a nameable refusal, got {other:?}"),
    }
}

/// A repair reading the prefix while retention reclaims it fails closed, not
/// wrong: the transfer plane re-resolves every chunk by segment identity
/// (#278), so a reclaimed segment stops resolving and the chunk is refused
/// as `WrongLineage` — the receiver abandons the partial copy instead of
/// stitching a segment that no longer exists into a range.
#[test]
fn a_transfer_chunk_for_a_reclaimed_segment_fails_closed() {
    let dir = TempDir::new().unwrap();
    let broker = Arc::new(bounded_broker(&dir, u64::MAX));
    for sequence in 0..60 {
        produce(&broker, sequence);
    }
    let handler = LeaderSegmentTransferHandler::new(Arc::clone(&broker));
    let range = range_identity();
    let listing = handler
        .list_sealed_segments(PEER, &range, FENCING_EPOCH)
        .expect("the prefix lists before retention");
    let front = *listing.first().expect("some sealed prefix");

    // Retention runs between the listing and the chunk — the interleaving a
    // live repair can always hit.
    broker.set_retention(Some(RetentionPolicy {
        max_total_bytes: 1536,
    }));
    for sequence in 60..120 {
        produce(&broker, sequence);
    }
    assert!(
        broker
            .sealed_segment_handles()
            .iter()
            .all(|handle| handle.segment_id != front.segment_id),
        "the front segment must have been reclaimed for this test to bite"
    );

    let refused = handler.fetch_segment_chunk(
        PEER,
        &vtop_protocol::FetchSegmentChunkRequest {
            range,
            fencing_epoch: FENCING_EPOCH,
            segment_id: front.segment_id,
            artifact: vtop_protocol::SegmentArtifact::Segment,
            offset: 0,
            length: 4096,
        },
    );
    match refused {
        Err((code, message)) => {
            assert_eq!(code, ErrorCode::WrongLineage, "{message}");
        }
        Ok(_) => panic!("a chunk of a reclaimed segment must fail closed"),
    }
}

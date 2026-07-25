//! Stage-7 broker surfaces for lineage-aware cursor commit/fetch against an
//! in-process metadata group checkpoint store.

use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;
use vtop_broker::{GroupCheckpointStore, LocalBroker, ProducerEpochJournal};
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
use vtop_meta::{CommandEnvelope, MetadataCommand, MetadataResponse, RangeAssignment};
use vtop_protocol::{
    CommitCursorRequest, ErrorCode, FetchCursorRequest, LineageCursor, Message, RangeIdentity,
    Role, WireFrame,
};

const TOPIC: &str = "events.v1";
const TOPIC_UUID: Uuid = Uuid::from_u128(0x20);
const RANGE_UUID: Uuid = Uuid::from_u128(0x21);
const GROUP_UUID: Uuid = Uuid::from_u128(0x50);
const MEMBER_UUID: Uuid = Uuid::from_u128(0x51);
const NODE_UUID: Uuid = Uuid::from_u128(0x10);
const SEGMENT_UUID: Uuid = Uuid::from_u128(0x30);
const FENCING_EPOCH: u64 = 1;
const SEGMENT_ROOT: [u8; 32] = [7; 32];

fn range() -> RangeIdentity {
    RangeIdentity {
        topic: TOPIC.to_owned(),
        topic_epoch: 1,
        range_id: RANGE_UUID,
        range_generation: 0,
    }
}

fn envelope(n: u128) -> CommandEnvelope {
    CommandEnvelope {
        request_id: Uuid::from_u128(0xcafe_0000 + n),
        issued_at_ms: 0,
    }
}

fn seeded_group_store() -> GroupCheckpointStore {
    let store = GroupCheckpointStore::new();
    assert_eq!(
        store.apply(MetadataCommand::RegisterNode {
            env: envelope(1),
            node_uuid: NODE_UUID,
            addr: "10.0.0.1:9200".to_owned(),
            expected_generation: None,
        }),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        store.apply(MetadataCommand::CreateTopic {
            env: envelope(2),
            name: TOPIC.to_owned(),
            topic_uuid: TOPIC_UUID,
            root_range_uuid: RANGE_UUID,
        }),
        MetadataResponse::TopicCreated {
            topic_uuid: TOPIC_UUID,
            topic_epoch: 1,
            root_range_uuid: RANGE_UUID,
        }
    );
    assert_eq!(
        store.apply(MetadataCommand::GrantRangeLease {
            env: envelope(3),
            topic_uuid: TOPIC_UUID,
            range_uuid: RANGE_UUID,
            holder_node_uuid: NODE_UUID,
            expected_range_generation: 0,
        }),
        MetadataResponse::LeaseGranted { fencing_epoch: 1 }
    );
    assert_eq!(
        store.apply(MetadataCommand::RegisterSealedSegment {
            env: envelope(4),
            topic_uuid: TOPIC_UUID,
            range_uuid: RANGE_UUID,
            segment_uuid: SEGMENT_UUID,
            segment_generation: 0,
            base_offset: 0,
            next_offset: 100,
            content_root: SEGMENT_ROOT,
            sealed_by_epoch: 1,
            expected_range_generation: 1,
        }),
        MetadataResponse::Ack { generation: 2 }
    );
    assert_eq!(
        store.apply(MetadataCommand::CreateConsumerGroup {
            env: envelope(5),
            name: "audit.consumers".to_owned(),
            group_uuid: GROUP_UUID,
        }),
        MetadataResponse::GroupCreated {
            group_uuid: GROUP_UUID,
            generation: 0,
        }
    );
    assert_eq!(
        store.apply(MetadataCommand::JoinConsumerGroup {
            env: envelope(6),
            group_uuid: GROUP_UUID,
            member_uuid: MEMBER_UUID,
            expected_group_generation: 0,
        }),
        MetadataResponse::MemberJoined {
            member_generation: 0,
            group_generation: 1,
        }
    );
    assert_eq!(
        store.apply(MetadataCommand::AssignMemberRanges {
            env: envelope(7),
            group_uuid: GROUP_UUID,
            member_uuid: MEMBER_UUID,
            ranges: vec![RangeAssignment {
                topic_uuid: TOPIC_UUID,
                range_uuid: RANGE_UUID,
            }],
            expected_member_generation: 0,
        }),
        MetadataResponse::Ack { generation: 1 }
    );
    store
}

fn open_broker(store: GroupCheckpointStore) -> (TempDir, Arc<LocalBroker>) {
    let dir = TempDir::new().unwrap();
    let descriptor = SegmentDescriptor {
        segment_id: SEGMENT_UUID,
        topic: TOPIC.to_owned(),
        topic_epoch: 1,
        lineage: RangeLineage {
            range_id: RANGE_UUID,
            generation: 0,
            key_range: KeyRange::full(),
            parents: Vec::new(),
        },
        base_offset: 0,
    };
    let segment = ActiveSegment::create(
        dir.path().join("seg.active"),
        descriptor,
        SegmentConfig::default(),
    )
    .unwrap();
    let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
    let broker = Arc::new(
        LocalBroker::new(segment, epochs, range(), FENCING_EPOCH)
            .unwrap()
            .with_group_checkpoints(store),
    );
    (dir, broker)
}

fn cursor_at(offset: u64, checkpoint_generation: u64) -> LineageCursor {
    LineageCursor {
        group_id: GROUP_UUID,
        topic_id: TOPIC_UUID,
        topic_epoch: 1,
        range_id: RANGE_UUID,
        // The range's lineage generation: still 0 after grant and segment
        // registration, which only bump the metadata CAS generation.
        range_generation: 0,
        segment_id: SEGMENT_UUID,
        segment_generation: 0,
        segment_root: SEGMENT_ROOT,
        record_offset: offset,
        record_index: 0,
        lineage_transition_id: None,
        checkpoint_generation,
    }
}

#[test]
fn commit_and_fetch_lineage_cursor_through_broker() {
    let store = seeded_group_store();
    let (_dir, broker) = open_broker(store);

    let commit = broker.handle(
        Role::Consumer,
        WireFrame {
            request_id: 1,
            stream_id: 1,
            message: Message::CommitCursorRequest(CommitCursorRequest {
                operation_id: Uuid::from_u128(100),
                member_id: MEMBER_UUID,
                cursor: cursor_at(10, 0),
                expected_checkpoint_generation: None,
            }),
        },
    );
    assert!(
        matches!(
            commit.message,
            Message::CommitCursorResponse(ref response) if response.checkpoint_generation == 0
        ),
        "{:?}",
        commit.message
    );

    // A response-loss retry uses a new wire request counter but the same
    // logical operation ID and receives the original durable receipt.
    let retry = broker.handle(
        Role::Consumer,
        WireFrame {
            request_id: 7,
            stream_id: 1,
            message: Message::CommitCursorRequest(CommitCursorRequest {
                operation_id: Uuid::from_u128(100),
                member_id: MEMBER_UUID,
                cursor: cursor_at(10, 0),
                expected_checkpoint_generation: None,
            }),
        },
    );
    assert!(matches!(
        retry.message,
        Message::CommitCursorResponse(ref response) if response.checkpoint_generation == 0
    ));

    let fetched = broker.handle(
        Role::Consumer,
        WireFrame {
            request_id: 2,
            stream_id: 1,
            message: Message::FetchCursorRequest(FetchCursorRequest {
                group_id: GROUP_UUID,
                topic_id: TOPIC_UUID,
                range_id: RANGE_UUID,
            }),
        },
    );
    let Message::FetchCursorResponse(response) = fetched.message else {
        panic!("expected fetch cursor response: {:?}", fetched.message);
    };
    let cursor = response.cursor.expect("checkpoint should exist");
    assert_eq!(cursor.record_offset, 10);
    assert_eq!(cursor.checkpoint_generation, 0);
    assert_eq!(cursor.segment_root, SEGMENT_ROOT);

    let advance = broker.handle(
        Role::Consumer,
        WireFrame {
            request_id: 3,
            stream_id: 1,
            message: Message::CommitCursorRequest(CommitCursorRequest {
                operation_id: Uuid::from_u128(101),
                member_id: MEMBER_UUID,
                cursor: cursor_at(20, 0),
                expected_checkpoint_generation: Some(0),
            }),
        },
    );
    assert!(matches!(
        advance.message,
        Message::CommitCursorResponse(ref response) if response.checkpoint_generation == 1
    ));

    let stale = broker.handle(
        Role::Consumer,
        WireFrame {
            request_id: 4,
            stream_id: 1,
            message: Message::CommitCursorRequest(CommitCursorRequest {
                operation_id: Uuid::from_u128(102),
                member_id: MEMBER_UUID,
                cursor: cursor_at(30, 0),
                expected_checkpoint_generation: Some(0),
            }),
        },
    );
    assert!(
        matches!(
            stale.message,
            Message::Error(ref err) if err.code == ErrorCode::CheckpointConflict
        ),
        "{:?}",
        stale.message
    );

    let backward = broker.handle(
        Role::Consumer,
        WireFrame {
            request_id: 5,
            stream_id: 1,
            message: Message::CommitCursorRequest(CommitCursorRequest {
                operation_id: Uuid::from_u128(103),
                member_id: MEMBER_UUID,
                cursor: cursor_at(5, 0),
                expected_checkpoint_generation: Some(1),
            }),
        },
    );
    assert!(
        matches!(
            backward.message,
            Message::Error(ref err) if err.code == ErrorCode::WrongLineage
        ),
        "{:?}",
        backward.message
    );

    // A cursor carrying the range's CAS generation (2 after grant + segment
    // registration) instead of its lineage generation is a lineage failure,
    // not a checkpoint conflict.
    let mut cas_cursor = cursor_at(40, 0);
    cas_cursor.range_generation = 2;
    let wrong_lineage = broker.handle(
        Role::Consumer,
        WireFrame {
            request_id: 6,
            stream_id: 1,
            message: Message::CommitCursorRequest(CommitCursorRequest {
                operation_id: Uuid::from_u128(104),
                member_id: MEMBER_UUID,
                cursor: cas_cursor,
                expected_checkpoint_generation: Some(1),
            }),
        },
    );
    assert!(
        matches!(
            wrong_lineage.message,
            Message::Error(ref err) if err.code == ErrorCode::WrongLineage
        ),
        "{:?}",
        wrong_lineage.message
    );
}

#[test]
fn cursor_operation_id_rotates_only_after_a_definitive_rejection() {
    let store = seeded_group_store();
    // Temporarily remove the member's assignment so the first operation is
    // definitively rejected and that rejection enters metadata dedup.
    assert_eq!(
        store.apply(MetadataCommand::AssignMemberRanges {
            env: envelope(8),
            group_uuid: GROUP_UUID,
            member_uuid: MEMBER_UUID,
            ranges: Vec::new(),
            expected_member_generation: 1,
        }),
        MetadataResponse::Ack { generation: 2 }
    );
    let shared_store = store.clone();
    let (_dir, broker) = open_broker(store);
    let commit = |request_id, operation_id| {
        broker.handle(
            Role::Consumer,
            WireFrame {
                request_id,
                stream_id: 1,
                message: Message::CommitCursorRequest(CommitCursorRequest {
                    operation_id,
                    member_id: MEMBER_UUID,
                    cursor: cursor_at(10, 0),
                    expected_checkpoint_generation: None,
                }),
            },
        )
    };

    let rejected_once = commit(1, Uuid::from_u128(200));
    assert!(matches!(
        rejected_once.message,
        Message::Error(ref error) if error.code == ErrorCode::WrongLineage
    ));
    assert_eq!(
        shared_store.apply(MetadataCommand::AssignMemberRanges {
            env: envelope(9),
            group_uuid: GROUP_UUID,
            member_uuid: MEMBER_UUID,
            ranges: vec![RangeAssignment {
                topic_uuid: TOPIC_UUID,
                range_uuid: RANGE_UUID,
            }],
            expected_member_generation: 2,
        }),
        MetadataResponse::Ack { generation: 3 }
    );

    // The old ID still means the old rejected operation, even after its
    // prerequisite changes.
    let rejected_retry = commit(2, Uuid::from_u128(200));
    assert!(matches!(
        rejected_retry.message,
        Message::Error(ref error) if error.code == ErrorCode::WrongLineage
    ));
    // A fresh ID starts a new logical operation and can now commit.
    let corrected = commit(3, Uuid::from_u128(201));
    assert!(matches!(
        corrected.message,
        Message::CommitCursorResponse(ref response) if response.checkpoint_generation == 0
    ));
}

#[test]
fn producer_cannot_commit_cursors_and_missing_store_rejects() {
    let dir = TempDir::new().unwrap();
    let descriptor = SegmentDescriptor {
        segment_id: SEGMENT_UUID,
        topic: TOPIC.to_owned(),
        topic_epoch: 1,
        lineage: RangeLineage {
            range_id: RANGE_UUID,
            generation: 0,
            key_range: KeyRange::full(),
            parents: Vec::new(),
        },
        base_offset: 0,
    };
    let segment = ActiveSegment::create(
        dir.path().join("seg.active"),
        descriptor,
        SegmentConfig::default(),
    )
    .unwrap();
    let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
    let broker = LocalBroker::new(segment, epochs, range(), FENCING_EPOCH).unwrap();

    let unauthorized = broker.handle(
        Role::Producer,
        WireFrame {
            request_id: 1,
            stream_id: 1,
            message: Message::CommitCursorRequest(CommitCursorRequest {
                operation_id: Uuid::from_u128(105),
                member_id: MEMBER_UUID,
                cursor: cursor_at(1, 0),
                expected_checkpoint_generation: None,
            }),
        },
    );
    assert!(matches!(
        unauthorized.message,
        Message::Error(ref err) if err.code == ErrorCode::Unauthorized
    ));

    let missing = broker.handle(
        Role::Consumer,
        WireFrame {
            request_id: 2,
            stream_id: 1,
            message: Message::CommitCursorRequest(CommitCursorRequest {
                operation_id: Uuid::from_u128(106),
                member_id: MEMBER_UUID,
                cursor: cursor_at(1, 0),
                expected_checkpoint_generation: None,
            }),
        },
    );
    assert!(matches!(
        missing.message,
        Message::Error(ref err) if err.code == ErrorCode::InvalidRequest
    ));
}

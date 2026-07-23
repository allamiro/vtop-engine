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
const SEGMENT_UUID: Uuid = Uuid::from_u128(0x30);
const FENCING_EPOCH: u64 = 1;

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
        store.apply(MetadataCommand::CreateTopic {
            env: envelope(1),
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
        store.apply(MetadataCommand::CreateConsumerGroup {
            env: envelope(2),
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
            env: envelope(3),
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
            env: envelope(4),
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
        range_generation: 0,
        segment_id: SEGMENT_UUID,
        segment_generation: 0,
        segment_root: [7; 32],
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
    assert_eq!(cursor.segment_root, [7; 32]);

    let advance = broker.handle(
        Role::Consumer,
        WireFrame {
            request_id: 3,
            stream_id: 1,
            message: Message::CommitCursorRequest(CommitCursorRequest {
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

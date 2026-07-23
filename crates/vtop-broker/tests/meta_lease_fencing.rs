//! Stage-6 first slice: metadata lease fencing epochs gate broker produce.
//!
//! Flow: grant a range lease via the metadata state machine → produce OK on
//! the leaseholder broker → grant the range to another holder (newer epoch) →
//! the prior broker is fenced even when producers still present the old epoch.

use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;
use vtop_broker::{LocalBroker, MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
use vtop_meta::{
    CommandEnvelope, MetaKey, MetaStateMachine, MetaValue, MetadataCommand, MetadataResponse,
};
use vtop_protocol::{
    Durability as WireDurability, ErrorCode, ErrorResponse, Message, ProduceRecord, ProduceRequest,
    RangeIdentity, Role, WireFrame,
};

const NODE_A: Uuid = Uuid::from_u128(0x10);
const NODE_B: Uuid = Uuid::from_u128(0x11);
const TOPIC: Uuid = Uuid::from_u128(0x20);
const RANGE: Uuid = Uuid::from_u128(0x21);

struct Requests(u128);

impl Requests {
    fn next(&mut self) -> CommandEnvelope {
        self.0 += 1;
        CommandEnvelope {
            request_id: Uuid::from_u128(0xbeef_0000_0000 + self.0),
            issued_at_ms: 1_750_000_000_000,
        }
    }
}

fn register(requests: &mut Requests, node: Uuid, addr: &str) -> MetadataCommand {
    MetadataCommand::RegisterNode {
        env: requests.next(),
        node_uuid: node,
        addr: addr.to_owned(),
        expected_generation: None,
    }
}

fn create_topic(requests: &mut Requests) -> MetadataCommand {
    MetadataCommand::CreateTopic {
        env: requests.next(),
        name: "events.v1".to_owned(),
        topic_uuid: TOPIC,
        root_range_uuid: RANGE,
    }
}

fn grant(requests: &mut Requests, holder: Uuid, expected_range_generation: u64) -> MetadataCommand {
    MetadataCommand::GrantRangeLease {
        env: requests.next(),
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        holder_node_uuid: holder,
        expected_range_generation,
    }
}

fn release(requests: &mut Requests, expected_fencing_epoch: u64) -> MetadataCommand {
    MetadataCommand::ReleaseRangeLease {
        env: requests.next(),
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        expected_fencing_epoch,
    }
}

fn range_lease_active(machine: &MetaStateMachine) -> bool {
    let Some(MetaValue::Range(range)) = machine.record(&MetaKey::Range {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
    }) else {
        panic!("range record missing");
    };
    range.lease.is_some()
}

fn range_fencing_epoch(machine: &MetaStateMachine) -> u64 {
    let Some(MetaValue::Range(range)) = machine.record(&MetaKey::Range {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
    }) else {
        panic!("range record missing");
    };
    range.fencing_epoch
}

fn open_broker(
    dir: &TempDir,
    held_epoch: u64,
    meta_epoch: MetaFencingEpoch,
) -> (Arc<LocalBroker>, RangeIdentity) {
    let range_id = RANGE;
    let range = RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id,
        range_generation: 0,
    };
    let descriptor = SegmentDescriptor {
        segment_id: Uuid::from_u128(0x30),
        topic: range.topic.clone(),
        topic_epoch: range.topic_epoch,
        lineage: RangeLineage {
            range_id,
            generation: 0,
            key_range: KeyRange::full(),
            parents: Vec::new(),
        },
        base_offset: 0,
    };
    let segment = ActiveSegment::create(
        dir.path().join(format!("seg-{held_epoch}.active")),
        descriptor,
        SegmentConfig::default(),
    )
    .unwrap();
    let epochs =
        ProducerEpochJournal::open(dir.path().join(format!("epochs-{held_epoch}"))).unwrap();
    let broker = Arc::new(
        LocalBroker::with_meta_fencing_epoch(
            segment,
            epochs,
            range.clone(),
            held_epoch,
            meta_epoch,
        )
        .unwrap(),
    );
    (broker, range)
}

fn produce(range: RangeIdentity, fencing_epoch: u64, request_id: u64) -> WireFrame {
    WireFrame {
        request_id,
        stream_id: 1,
        message: Message::ProduceRequest(ProduceRequest {
            range,
            fencing_epoch,
            producer_id: Uuid::from_u128(0x99),
            producer_epoch: 1,
            first_sequence: request_id.saturating_sub(1),
            durability: WireDurability::LocalFsync,
            records: vec![ProduceRecord {
                timestamp_millis: 1,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        }),
    }
}

#[test]
fn grant_produce_steal_fences_prior_broker() {
    let mut requests = Requests(0);
    let mut machine = MetaStateMachine::new();

    assert_eq!(
        machine.apply(1, &register(&mut requests, NODE_A, "10.0.0.1:9200")),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(2, &register(&mut requests, NODE_B, "10.0.0.2:9200")),
        MetadataResponse::Ack { generation: 0 }
    );
    assert!(matches!(
        machine.apply(3, &create_topic(&mut requests)),
        MetadataResponse::TopicCreated { .. }
    ));

    // First grant: NODE_A becomes leaseholder at fencing epoch 1.
    assert_eq!(
        machine.apply(4, &grant(&mut requests, NODE_A, 0)),
        MetadataResponse::LeaseGranted { fencing_epoch: 1 }
    );
    assert_eq!(range_fencing_epoch(&machine), 1);

    let dir = tempfile::tempdir().unwrap();
    let meta_epoch = MetaFencingEpoch::new(range_fencing_epoch(&machine));
    let (broker_a, range) = open_broker(&dir, 1, meta_epoch.clone());

    let ok = broker_a.handle(Role::Producer, produce(range.clone(), 1, 1));
    assert!(
        matches!(ok.message, Message::ProduceResponse(_)),
        "leaseholder must accept produce under its granted epoch"
    );

    // Steal: NODE_B takes the lease; metadata mints epoch 2.
    assert_eq!(
        machine.apply(5, &grant(&mut requests, NODE_B, 1)),
        MetadataResponse::LeaseGranted { fencing_epoch: 2 }
    );
    assert_eq!(range_fencing_epoch(&machine), 2);
    meta_epoch.set(range_fencing_epoch(&machine));

    let fenced = broker_a.handle(Role::Producer, produce(range.clone(), 1, 2));
    assert!(
        matches!(
            fenced.message,
            Message::Error(ErrorResponse {
                code: ErrorCode::Fenced,
                ..
            })
        ),
        "prior leaseholder must be fenced after metadata grants a newer epoch"
    );

    // New leaseholder broker (held=2) produces successfully.
    let (broker_b, _) = open_broker(&dir, 2, meta_epoch);
    let ok_b = broker_b.handle(Role::Producer, produce(range, 2, 1));
    assert!(matches!(ok_b.message, Message::ProduceResponse(_)));
}

#[test]
fn release_clears_lease_and_fences_holder_without_epoch_bump() {
    let mut requests = Requests(0);
    let mut machine = MetaStateMachine::new();
    assert_eq!(
        machine.apply(1, &register(&mut requests, NODE_A, "10.0.0.1:9200")),
        MetadataResponse::Ack { generation: 0 }
    );
    assert!(matches!(
        machine.apply(2, &create_topic(&mut requests)),
        MetadataResponse::TopicCreated { .. }
    ));
    assert_eq!(
        machine.apply(3, &grant(&mut requests, NODE_A, 0)),
        MetadataResponse::LeaseGranted { fencing_epoch: 1 }
    );

    let dir = tempfile::tempdir().unwrap();
    let meta_epoch = MetaFencingEpoch::new(1);
    let (broker, range) = open_broker(&dir, 1, meta_epoch.clone());
    assert!(matches!(
        broker
            .handle(Role::Producer, produce(range.clone(), 1, 1))
            .message,
        Message::ProduceResponse(_)
    ));

    // Release keeps fencing_epoch=1 but clears the live lease.
    assert!(matches!(
        machine.apply(4, &release(&mut requests, 1)),
        MetadataResponse::Ack { .. }
    ));
    assert_eq!(range_fencing_epoch(&machine), 1);
    assert!(!range_lease_active(&machine));
    meta_epoch.clear_lease(1);
    assert!(!meta_epoch.lease_active());
    assert_eq!(meta_epoch.get(), 1);

    let fenced = broker.handle(Role::Producer, produce(range, 1, 2));
    assert!(
        matches!(
            fenced.message,
            Message::Error(ErrorResponse {
                code: ErrorCode::Fenced,
                ..
            })
        ),
        "release must fence the prior holder even when the epoch number is unchanged"
    );
}

#[test]
fn lease_publications_are_monotonic() {
    let view = MetaFencingEpoch::new(2);
    // Stale grant cannot rewind.
    view.set(1);
    assert_eq!(view.get(), 2);
    assert!(view.lease_active());
    // Stale release for epoch 1 cannot clear epoch 2.
    view.clear_lease(1);
    assert!(view.lease_active());
    assert_eq!(view.get(), 2);
    // Matching release clears liveness without rewinding the epoch.
    view.clear_lease(2);
    assert!(!view.lease_active());
    assert_eq!(view.get(), 2);
    // Re-publishing the same released epoch stays inactive.
    view.set(2);
    assert!(!view.lease_active());
    // A newer grant after that release activates.
    view.set(3);
    assert!(view.lease_active());
    assert_eq!(view.get(), 3);
}

#[test]
fn release_before_grant_still_deactivates() {
    // View still at epoch 1; release(2) arrives before set(2).
    let view = MetaFencingEpoch::new(1);
    view.clear_lease(2);
    assert!(view.lease_active()); // epoch-1 holder unaffected
    assert_eq!(view.get(), 1);
    view.set(2);
    assert_eq!(view.get(), 2);
    assert!(
        !view.lease_active(),
        "grant for an already-released epoch must not reactivate"
    );
}

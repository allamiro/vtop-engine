//! The promotion boundary marker (#240), pinned in isolation.
//!
//! Raft §5.4.2 in its actual form: a new leader must not trust a count over
//! records written under earlier epochs — it commits an entry of its OWN
//! epoch and lets the prefix commit implicitly once that entry is
//! quorum-acked. `publish_boundary_marker` is that entry's whole lifecycle:
//! append at the tail, prove on a quorum, and only then publish the tail as
//! the cluster high-water mark. Nothing calls it in production yet — the
//! promotion wiring is its own slice — so these tests are the primitive's
//! only consumer, and they pin the four properties the wiring will lean on:
//! publication follows the quorum, an unacked marker publishes nothing, the
//! marker is invisible to consumers on both fetch surfaces, and a republish
//! is the same marker, not a second one.

use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;
use vtop_broker::replication::{
    ClusterCommittedOffset, InProcessFollower, InProcessReplicaSet, ReplicaSet,
};
use vtop_broker::{
    consumer_visible, BrokerError, LocalBroker, MetaFencingEpoch, ProducerEpochJournal,
    PROMOTION_MARKER_PRODUCER,
};
use vtop_log::{
    ActiveSegment, KeyRange, LogRecord, RangeLineage, SegmentConfigV2, SegmentDescriptor,
    SegmentDescriptorV2,
};
use vtop_protocol::{
    Durability as WireDurability, ErrorResponse, FetchRequest, Message, ProduceRecord,
    ProduceRequest, ProduceResponse, RangeIdentity, Role, WireFrame,
};

const LEADER: Uuid = Uuid::from_u128(0xA1);
const FOLLOWER_1: Uuid = Uuid::from_u128(0xA2);
const FOLLOWER_2: Uuid = Uuid::from_u128(0xA3);
const PRODUCER: Uuid = Uuid::from_u128(0xB1);
const FENCING_EPOCH: u64 = 18;

struct Harness {
    _dirs: Vec<TempDir>,
    range: RangeIdentity,
    meta: MetaFencingEpoch,
    leader: Arc<LocalBroker>,
    followers: Vec<Arc<InProcessFollower>>,
    cluster_committed: ClusterCommittedOffset,
}

fn range_identity() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

fn open_segment(dir: &TempDir, segment_id: u128, range: &RangeIdentity) -> ActiveSegment {
    // V2 on purpose: the marker's identity is the stored producer UUID, and
    // only the v2 frame stores it unmerged — the primitive refuses v1
    // outright, which its own test pins.
    let descriptor = SegmentDescriptorV2 {
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
        segment_generation: 0,
        creation_node_id: LEADER,
        creation_fencing_epoch: FENCING_EPOCH,
    };
    ActiveSegment::create_v2(
        dir.path().join("range.active"),
        descriptor,
        SegmentConfigV2::default(),
    )
    .unwrap()
}

fn harness() -> Harness {
    harness_with_leader_format(true)
}

fn harness_with_leader_format(leader_v2: bool) -> Harness {
    let range = range_identity();
    let meta = MetaFencingEpoch::new(FENCING_EPOCH);
    let cluster_committed = ClusterCommittedOffset::new(0);

    let leader_dir = tempfile::tempdir().unwrap();
    let leader_segment = if leader_v2 {
        open_segment(&leader_dir, 0xD1, &range)
    } else {
        // The one caller that wants v1: the format-refusal pin below.
        ActiveSegment::create(
            leader_dir.path().join("range.active"),
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
            vtop_log::SegmentConfig::default(),
        )
        .unwrap()
    };
    let leader_epochs = ProducerEpochJournal::open(leader_dir.path().join("epochs")).unwrap();

    let mut dirs = vec![leader_dir];
    let mut followers = Vec::new();
    for (index, node_id) in [FOLLOWER_1, FOLLOWER_2].into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let segment = open_segment(&dir, 0xE1 + index as u128, &range);
        let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
        let follower_hwm = ClusterCommittedOffset::new(0);
        followers.push(Arc::new(
            InProcessFollower::new(
                node_id,
                segment,
                epochs,
                range.clone(),
                FENCING_EPOCH,
                meta.clone(),
                follower_hwm,
            )
            .unwrap(),
        ));
        dirs.push(dir);
    }

    let replica_set = Arc::new(InProcessReplicaSet::new(followers.clone()));
    let leader = Arc::new(
        LocalBroker::with_replication(
            leader_segment,
            leader_epochs,
            range.clone(),
            FENCING_EPOCH,
            meta.clone(),
            LEADER,
            Some(cluster_committed.clone()),
            Some(replica_set as Arc<dyn ReplicaSet>),
        )
        .unwrap(),
    );

    Harness {
        _dirs: dirs,
        range,
        meta,
        leader,
        followers,
        cluster_committed,
    }
}

fn produce_ok(broker: &LocalBroker, range: RangeIdentity, sequence: u64) -> ProduceResponse {
    let response = broker.handle(
        Role::Producer,
        WireFrame {
            request_id: sequence + 100,
            stream_id: 1,
            message: Message::ProduceRequest(ProduceRequest {
                range,
                fencing_epoch: FENCING_EPOCH,
                producer_id: PRODUCER,
                producer_epoch: 1,
                first_sequence: sequence,
                durability: WireDurability::Quorum,
                records: vec![ProduceRecord {
                    timestamp_millis: 1_000,
                    key: sequence.to_be_bytes().to_vec(),
                    value: format!("v{sequence}").into_bytes(),
                }],
            }),
        },
    );
    match response.message {
        Message::ProduceResponse(value) => value,
        Message::Error(ErrorResponse { code, message, .. }) => {
            panic!("produce failed: {code:?}: {message}")
        }
        other => panic!("unexpected response: {other:?}"),
    }
}

fn fetch(broker: &LocalBroker, range: RangeIdentity, start: u64) -> vtop_protocol::FetchResponse {
    let response = broker.handle(
        Role::Consumer,
        WireFrame {
            request_id: 900 + start,
            stream_id: 1,
            message: Message::FetchRequest(FetchRequest {
                range,
                fencing_epoch: FENCING_EPOCH,
                start_offset: start,
                max_bytes: 1 << 20,
                max_records: 64,
            }),
        },
    );
    match response.message {
        Message::FetchResponse(value) => value,
        Message::Error(ErrorResponse { code, message, .. }) => {
            panic!("fetch failed: {code:?}: {message}")
        }
        other => panic!("unexpected response: {other:?}"),
    }
}

#[test]
fn the_boundary_publishes_only_when_a_quorum_holds_the_marker() {
    let h = harness();
    let published = h
        .leader
        .publish_boundary_marker(FENCING_EPOCH)
        .expect("a healthy quorum must ack the marker");
    assert_eq!(
        published, 1,
        "the marker sits at offset 0, so the published mark is its end"
    );
    assert_eq!(
        h.cluster_committed.get(),
        1,
        "publication IS the watermark advance; nothing else may have moved it"
    );
    for follower in &h.followers {
        assert_eq!(
            follower.local_committed_offset(),
            1,
            "a quorum ack means the follower durably holds the marker, not merely heard of it"
        );
    }

    // A real produce after publication lands above the marker and is served;
    // the marker itself never is.
    let produced = produce_ok(&h.leader, h.range.clone(), 0);
    assert_eq!(
        produced.outcomes[0].offset, 1,
        "the marker occupies offset 0; the first real record follows it"
    );
    let page = fetch(&h.leader, h.range.clone(), 0);
    assert_eq!(
        page.records.len(),
        1,
        "one real record is visible; the marker is the replication plane's, not the consumer's"
    );
    assert_eq!(page.records[0].offset, 1);
    assert_eq!(
        page.next_offset, 2,
        "the cursor steps over the filtered marker — the gap-tolerant half of the wire \
         contract slice one taught the consumers to honor"
    );
}

#[test]
fn an_unacked_marker_leaves_the_watermark_unpublished() {
    let h = harness();
    h.followers[0].set_online(false);
    h.followers[1].set_online(false);

    let refused = h
        .leader
        .publish_boundary_marker(FENCING_EPOCH)
        .expect_err("no majority holds the marker, so nothing may publish");
    assert!(
        matches!(&refused, BrokerError::BoundaryMarker(message) if message.contains("not quorum-acked")),
        "the refusal must say the quorum failed, not something incidental: {refused}"
    );
    assert_eq!(
        h.cluster_committed.get(),
        0,
        "an unacked marker is locally durable ABOVE the watermark and must stay invisible — \
         publishing here is exactly the counted-majority trust §5.4.2 forbids"
    );
    let page = fetch(&h.leader, h.range.clone(), 0);
    assert!(
        page.records.is_empty(),
        "nothing below the unmoved watermark exists to serve"
    );
    assert_eq!(page.committed_high_watermark, 0);
}

#[test]
fn republishing_the_same_epochs_marker_is_idempotent() {
    let h = harness();
    let first = h.leader.publish_boundary_marker(FENCING_EPOCH).unwrap();
    let second = h
        .leader
        .publish_boundary_marker(FENCING_EPOCH)
        .expect("republication is the retry path and must succeed");
    assert_eq!(
        first, second,
        "one marker per epoch: the retry proves the SAME boundary, it does not mint another"
    );
    let (_, next_offset) = h.leader.local_offsets();
    assert_eq!(
        next_offset, 1,
        "the duplicate path must not have appended a second record"
    );
}

#[test]
fn a_fenced_leader_cannot_publish_a_marker() {
    let h = harness();
    // A newer grant supersedes this leader between adoption and publication.
    h.meta.set(FENCING_EPOCH + 1);
    let refused = h
        .leader
        .publish_boundary_marker(FENCING_EPOCH)
        .expect_err("a fenced leader's marker must not publish");
    assert!(
        matches!(refused, BrokerError::BoundaryMarker(_)),
        "the refusal is the marker's own: {refused}"
    );
    assert_eq!(
        h.cluster_committed.get(),
        0,
        "a fenced leader must leave the watermark exactly where it was"
    );
}

#[test]
fn an_unreplicated_broker_refuses_the_marker_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let range = range_identity();
    let meta = MetaFencingEpoch::new(FENCING_EPOCH);
    let broker = LocalBroker::with_meta_fencing_epoch(
        open_segment(&dir, 0xF1, &range),
        ProducerEpochJournal::open(dir.path().join("epochs")).unwrap(),
        range,
        FENCING_EPOCH,
        meta,
    )
    .unwrap();
    let refused = broker
        .publish_boundary_marker(FENCING_EPOCH)
        .expect_err("a standalone boundary is the node's own log; the hazard is vacuous");
    assert!(
        matches!(&refused, BrokerError::BoundaryMarker(message) if message.contains("quorum to convince")),
        "the refusal must explain itself: {refused}"
    );
}

#[test]
fn a_v1_range_refuses_the_marker_rather_than_writing_one_it_cannot_filter() {
    let h = harness_with_leader_format(false);
    let refused = h
        .leader
        .publish_boundary_marker(FENCING_EPOCH)
        .expect_err("a v1 frame stores the producer identity merged with the epoch");
    assert!(
        matches!(&refused, BrokerError::BoundaryMarker(message) if message.contains("v1")),
        "the refusal must name the format, because an unrecognizable marker would be \
         served to consumers as a mystery record forever: {refused}"
    );
    assert_eq!(
        h.cluster_committed.get(),
        0,
        "a refused marker publishes nothing"
    );
}

#[test]
fn the_marker_is_invisible_on_the_followers_fetch_surface_too() {
    let h = harness();
    h.leader.publish_boundary_marker(FENCING_EPOCH).unwrap();
    produce_ok(&h.leader, h.range.clone(), 0);

    let batch = h.followers[0]
        .fetch(0, 1 << 20, 64)
        .expect("the follower serves below its observed watermark");
    assert_eq!(
        batch.records.len(),
        1,
        "one visibility rule for both surfaces: a marker visible on one side only would \
         read as leader/follower divergence"
    );
    assert!(
        batch
            .records
            .iter()
            .all(|record| consumer_visible(&record.record)),
        "no marker may reach a consumer through the follower path"
    );
    assert_eq!(
        batch.next_offset, 2,
        "the follower's cursor steps over the marker exactly as the leader's does"
    );
}

#[test]
fn the_visibility_predicate_keys_on_the_reserved_identity_alone() {
    let marker = LogRecord {
        producer_id: PROMOTION_MARKER_PRODUCER,
        producer_epoch: 7,
        sequence: 0,
        timestamp_millis: 0,
        attributes: 0,
        key: b"promotion-boundary".to_vec(),
        value: Vec::new(),
    };
    let ordinary = LogRecord {
        producer_id: PRODUCER,
        ..marker.clone()
    };
    assert!(
        !consumer_visible(&marker),
        "the reserved identity IS the marker; nothing else about the record decides"
    );
    assert!(
        consumer_visible(&ordinary),
        "an ordinary producer's record is untouched even with an identical payload"
    );
}

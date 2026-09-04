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

fn open_v1_segment(dir: &TempDir, segment_id: u128, range: &RangeIdentity) -> ActiveSegment {
    // The callers that want v1: the format-refusal pins — the leader's own
    // and the mixed-set follower's.
    ActiveSegment::create(
        dir.path().join("range.active"),
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
        },
        vtop_log::SegmentConfig::default(),
    )
    .unwrap()
}

fn harness() -> Harness {
    harness_config(true, [true, true])
}

fn harness_with_leader_format(leader_v2: bool) -> Harness {
    harness_config(leader_v2, [true, true])
}

fn harness_config(leader_v2: bool, followers_v2: [bool; 2]) -> Harness {
    let range = range_identity();
    let meta = MetaFencingEpoch::new(FENCING_EPOCH);
    let cluster_committed = ClusterCommittedOffset::new(0);

    let leader_dir = tempfile::tempdir().unwrap();
    let leader_segment = if leader_v2 {
        open_segment(&leader_dir, 0xD1, &range)
    } else {
        open_v1_segment(&leader_dir, 0xD1, &range)
    };
    let leader_epochs = ProducerEpochJournal::open(leader_dir.path().join("epochs")).unwrap();

    let mut dirs = vec![leader_dir];
    let mut followers = Vec::new();
    for (index, node_id) in [FOLLOWER_1, FOLLOWER_2].into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let segment = if followers_v2[index] {
            open_segment(&dir, 0xE1 + index as u128, &range)
        } else {
            open_v1_segment(&dir, 0xE1 + index as u128, &range)
        };
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
        matches!(&refused, BrokerError::BoundaryMarkerUnacked { .. }),
        "the refusal must be the TYPED quorum shortfall — it is the one refusal the \
         promotion wiring retries: {refused}"
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

#[test]
fn a_producer_cannot_wear_the_markers_reserved_identity() {
    let h = harness();
    let response = h.leader.handle(
        Role::Producer,
        WireFrame {
            request_id: 100,
            stream_id: 1,
            message: Message::ProduceRequest(ProduceRequest {
                range: h.range.clone(),
                fencing_epoch: FENCING_EPOCH,
                producer_id: PROMOTION_MARKER_PRODUCER,
                producer_epoch: 1,
                first_sequence: 0,
                durability: WireDurability::Quorum,
                records: vec![ProduceRecord {
                    timestamp_millis: 1_000,
                    key: b"forged".to_vec(),
                    value: b"disappears-from-every-fetch".to_vec(),
                }],
            }),
        },
    );
    match response.message {
        Message::Error(ErrorResponse { message, .. }) => assert!(
            message.contains("reserved"),
            "the refusal must say the identity is reserved — a record accepted under it \
             would be silently withheld from every consumer: {message}"
        ),
        other => {
            panic!("a produce under the reserved marker identity must be refused, got: {other:?}")
        }
    }
    let (_, next_offset) = h.leader.local_offsets();
    assert_eq!(next_offset, 0, "the forged record must never have appended");
}

#[test]
fn a_v1_follower_refuses_the_marker_so_the_quorum_fails_honestly() {
    // A v2 leader over two v1 followers: each follower would store the
    // marker under an epoch-merged identity that `consumer_visible` can
    // never recognize, so each refuses — and the leader's quorum count
    // reports the truth instead of a hidden visibility leak.
    let h = harness_config(true, [false, false]);
    let refused = h
        .leader
        .publish_boundary_marker(FENCING_EPOCH)
        .expect_err("no v1 follower may ack a marker it cannot hide");
    assert!(
        matches!(&refused, BrokerError::BoundaryMarkerUnacked { .. }),
        "the failure surfaces as the typed quorum shortfall, because the refusal happened \
         at the followers: {refused}"
    );
    for follower in &h.followers {
        assert_eq!(
            follower.local_committed_offset(),
            0,
            "the refusing follower must not have stored the unrecognizable marker"
        );
    }
    assert_eq!(h.cluster_committed.get(), 0, "nothing published");
}

/// A marker no follower will EVER accept must not stay in the log (#240
/// slice 3 review): with every follower on a v1 frame the quorum can never
/// form, and every later produce's base-offset chain would include the
/// refused marker — followers one offset behind forever, quorum dead.
/// Zero acks after the budget is the retraction's licence: nothing anywhere
/// holds the marker, so removing it restores the pre-marker state exactly,
/// and the produce that follows proves the chain is whole again.
#[test]
fn a_fully_refused_marker_is_retracted_and_produce_stays_whole() {
    let h = harness_config(true, [false, false]);
    let refused = h
        .leader
        .publish_boundary_marker(FENCING_EPOCH)
        .expect_err("no v1 follower may ack the marker");
    let BrokerError::BoundaryMarkerUnacked {
        follower_acks,
        marker_offset,
        ..
    } = refused
    else {
        panic!("the refusal must be the typed quorum shortfall: {refused}");
    };
    assert_eq!(follower_acks, 0, "v1 followers refuse; nobody acks");
    assert!(
        h.leader
            .retract_unacked_boundary_marker(marker_offset)
            .expect("a zero-ack marker at the tail retracts"),
        "the retraction must report that it acted"
    );
    let (_, next_offset) = h.leader.local_offsets();
    assert_eq!(next_offset, 0, "the log is back to its pre-marker state");

    // The whole point: produce works, and the v1 followers ack it — the
    // chain no longer routes through a record they refuse by name.
    let produced = produce_ok(&h.leader, h.range.clone(), 0);
    assert_eq!(
        produced.outcomes[0].offset, 0,
        "the retracted marker's offset is minted to the real record"
    );
    assert_eq!(
        h.cluster_committed.get(),
        1,
        "the produce quorum published — the v1 set is not wedged"
    );
}

/// The retraction truncates THROUGH an unacked suffix (#240 review, round
/// three): a produce failing quorum during the retry budget appends
/// locally above the refused marker, and a tail-equality guard would then
/// leave the poison in place forever. Nothing above the marker can have
/// quorum-acked — every ack chain routes through the marker the evidence
/// says nobody holds — so the whole suffix goes with it. And the epoch
/// journal is re-anchored: the marker sat exactly at the held epoch's
/// recorded start, and losing that entry would attribute every later
/// record to the PRECEDING epoch.
#[test]
fn retraction_clears_the_failed_suffix_and_reanchors_the_epoch() {
    let h = harness_config(true, [false, false]);
    // A journal, as production attaches: the re-anchor assertion below is
    // vacuous without one.
    h.leader.set_fencing_epoch_journal(
        vtop_broker::fencing_epochs::FencingEpochJournal::open(
            h._dirs[0].path().join("fencing-epochs"),
        )
        .unwrap(),
    );
    let refused = h
        .leader
        .publish_boundary_marker(FENCING_EPOCH)
        .expect_err("no v1 follower may ack the marker");
    let BrokerError::BoundaryMarkerUnacked { marker_offset, .. } = refused else {
        panic!("the refusal must be the typed quorum shortfall: {refused}");
    };
    // A produce that fails quorum still appended locally above the marker.
    let response = h.leader.handle(
        Role::Producer,
        WireFrame {
            request_id: 300,
            stream_id: 1,
            message: Message::ProduceRequest(ProduceRequest {
                range: h.range.clone(),
                fencing_epoch: FENCING_EPOCH,
                producer_id: PRODUCER,
                producer_epoch: 1,
                first_sequence: 0,
                durability: WireDurability::Quorum,
                records: vec![ProduceRecord {
                    timestamp_millis: 1_000,
                    key: b"stranded".to_vec(),
                    value: b"failed the quorum".to_vec(),
                }],
            }),
        },
    );
    assert!(
        matches!(response.message, Message::Error(_)),
        "the produce must fail its quorum behind the refused marker"
    );
    let (_, next_offset) = h.leader.local_offsets();
    assert_eq!(next_offset, 2, "marker plus the stranded produce record");

    assert!(
        h.leader
            .retract_unacked_boundary_marker(marker_offset)
            .expect("a zero-ack marker retracts through its unacked suffix"),
        "the retraction must act despite the suffix"
    );
    let (_, next_offset) = h.leader.local_offsets();
    assert_eq!(next_offset, 0, "marker AND stranded suffix are gone");
    assert!(
        h.leader
            .epoch_starts()
            .iter()
            .any(|entry| entry.epoch == FENCING_EPOCH && entry.start_offset == 0),
        "the held epoch is re-anchored where the marker sat, or every later record \
         would be attributed to the preceding epoch: {:?}",
        h.leader.epoch_starts()
    );
    let produced = produce_ok(&h.leader, h.range.clone(), 0);
    assert_eq!(produced.outcomes[0].offset, 0, "the chain is whole again");
}

/// The retraction refuses everything except its exact licence: a published/// The retraction refuses everything except its exact licence: a published
/// marker is committed history, and a tail that moved past the marker means
/// the chain was accepted somewhere after all.
#[test]
fn a_published_marker_is_never_retracted() {
    let h = harness();
    let published = h.leader.publish_boundary_marker(FENCING_EPOCH).unwrap();
    assert_eq!(published, 1);
    assert!(
        !h.leader
            .retract_unacked_boundary_marker(0)
            .expect("the refusal is Ok(false), not an error"),
        "a quorum-acked, published marker is committed history"
    );
    let (_, next_offset) = h.leader.local_offsets();
    assert_eq!(next_offset, 1, "nothing was touched");
}

#[test]
fn one_v2_follower_still_carries_the_quorum_past_a_v1_peer() {
    // Mixed set, majority still possible: leader + the v2 follower are two
    // of three. The v1 peer refuses and simply is not part of the proof.
    let h = harness_config(true, [true, false]);
    let published = h
        .leader
        .publish_boundary_marker(FENCING_EPOCH)
        .expect("the v2 majority proves the boundary without the v1 peer");
    assert_eq!(published, 1);
    assert_eq!(
        h.followers[0].local_committed_offset(),
        1,
        "the v2 follower holds the marker"
    );
    assert_eq!(
        h.followers[1].local_committed_offset(),
        0,
        "the v1 follower refused it and stored nothing"
    );
}

#[test]
fn publication_refuses_a_watermark_this_broker_never_proved() {
    let h = harness();
    // A predecessor's quorum proved five records this broker does not hold.
    h.cluster_committed.advance_to(5);
    let refused = h
        .leader
        .publish_boundary_marker(FENCING_EPOCH)
        .expect_err("a leader below the inherited watermark cannot prove a boundary over it");
    assert!(
        matches!(&refused, BrokerError::BoundaryMarker(message) if message.contains("exceeds")),
        "the refusal must name the inherited-watermark gap: {refused}"
    );
    assert_eq!(
        h.cluster_committed.get(),
        5,
        "the inherited watermark is untouched — refusing to relabel it is the point"
    );
    let (_, next_offset) = h.leader.local_offsets();
    assert_eq!(
        next_offset, 0,
        "the refusal must fire before the marker appends, or the log grows a record \
         whose publication was never legal"
    );
}

#[test]
fn a_starving_byte_budget_still_moves_the_followers_cursor() {
    let h = harness();
    h.leader.publish_boundary_marker(FENCING_EPOCH).unwrap();
    produce_ok(&h.leader, h.range.clone(), 0);

    // One byte covers no record frame. The raw refetch returns exactly the
    // marker, the filter removes it, and the cursor STILL advances — an
    // empty page whose cursor moved is progress, a stuck cursor is not.
    let page = h.followers[0]
        .fetch(0, 1, 64)
        .expect("a starved budget is a small fetch, not an error");
    assert!(
        page.records.is_empty(),
        "the only record under the budget was the marker, and it is not the consumer's"
    );
    assert_eq!(
        page.next_offset, 1,
        "the follower's cursor must step past the marker under any budget, exactly as \
         the leader's wire guard promises"
    );

    let page = h.followers[0]
        .fetch(1, 1, 64)
        .expect("the second page rides the same guard");
    assert_eq!(
        page.records.len(),
        1,
        "the real record is served whole even though the budget never covered it"
    );
    assert_eq!(page.next_offset, 2);
}

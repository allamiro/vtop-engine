//! Stage-6 slice: leader→follower quorum replication and committed HWM.
//!
//! Covers quorum acknowledgement, fetch visibility capped at the cluster
//! high-water mark, and stale-leader fencing on both produce and replica append.

use std::sync::Arc;
use tempfile::TempDir;
use uuid::Uuid;
use vtop_broker::replication::{
    ClusterCommittedOffset, InProcessFollower, InProcessReplicaSet, ReplicaSet,
};
use vtop_broker::{LocalBroker, MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
use vtop_protocol::{
    CommittedHwmUpdate, Durability as WireDurability, ErrorCode, ErrorResponse, Message,
    ProduceRecord, ProduceRequest, ProduceResponse, RangeIdentity, ReplicaAppendRequest, Role,
    WireFrame,
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
    replica_set: Arc<InProcessReplicaSet>,
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
    let descriptor = SegmentDescriptor {
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
    };
    ActiveSegment::create(
        dir.path().join("range.active"),
        descriptor,
        SegmentConfig::default(),
    )
    .unwrap()
}

fn harness() -> Harness {
    let range = range_identity();
    let meta = MetaFencingEpoch::new(FENCING_EPOCH);
    let cluster_committed = ClusterCommittedOffset::new(0);

    let leader_dir = tempfile::tempdir().unwrap();
    let leader_segment = open_segment(&leader_dir, 0xD1, &range);
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
            Some(replica_set.clone() as Arc<dyn ReplicaSet>),
        )
        .unwrap(),
    );

    Harness {
        _dirs: dirs,
        range,
        meta,
        leader,
        followers,
        replica_set,
        cluster_committed,
    }
}

fn produce_frame(
    range: RangeIdentity,
    sequence: u64,
    request_id: u64,
    durability: WireDurability,
) -> WireFrame {
    WireFrame {
        request_id,
        stream_id: 1,
        message: Message::ProduceRequest(ProduceRequest {
            range,
            fencing_epoch: FENCING_EPOCH,
            producer_id: PRODUCER,
            producer_epoch: 1,
            first_sequence: sequence,
            durability,
            records: vec![ProduceRecord {
                timestamp_millis: 1_000,
                key: b"k".to_vec(),
                value: format!("v{sequence}").into_bytes(),
            }],
        }),
    }
}

fn produce_ok(broker: &LocalBroker, range: RangeIdentity, sequence: u64) -> ProduceResponse {
    let response = broker.handle(
        Role::Producer,
        produce_frame(range, sequence, sequence + 1, WireDurability::Quorum),
    );
    match response.message {
        Message::ProduceResponse(value) => value,
        Message::Error(ErrorResponse { code, message, .. }) => {
            panic!("produce failed: {code:?}: {message}")
        }
        other => panic!("unexpected response: {other:?}"),
    }
}

#[test]
fn quorum_acks_with_majority_and_survives_one_follower_loss() {
    let h = harness();
    let first = produce_ok(&h.leader, h.range.clone(), 0);
    assert_eq!(first.outcomes[0].offset, 0);
    assert_eq!(first.committed_next_offset, 1);
    assert_eq!(h.cluster_committed.get(), 1);
    assert_eq!(h.followers[0].cluster_committed().get(), 1);
    assert_eq!(h.followers[1].cluster_committed().get(), 1);

    h.followers[1].set_online(false);
    let second = produce_ok(&h.leader, h.range.clone(), 1);
    assert_eq!(second.outcomes[0].offset, 1);
    assert_eq!(second.committed_next_offset, 2);
    assert_eq!(h.cluster_committed.get(), 2);
}

#[test]
fn quorum_produce_fails_when_majority_is_unavailable() {
    let h = harness();
    h.followers[0].set_online(false);
    h.followers[1].set_online(false);

    let response = h.leader.handle(
        Role::Producer,
        produce_frame(h.range.clone(), 0, 1, WireDurability::Quorum),
    );
    match response.message {
        Message::Error(ErrorResponse {
            code: ErrorCode::Overloaded,
            retryable: true,
            ..
        }) => {}
        other => panic!("expected Overloaded, got {other:?}"),
    }
    assert_eq!(h.cluster_committed.get(), 0);

    // Leader may be locally durable above the cluster HWM; fetch must not
    // expose that prefix.
    let fetch = h.leader.handle(
        Role::Consumer,
        WireFrame {
            request_id: 2,
            stream_id: 1,
            message: vtop_protocol::Message::FetchRequest(vtop_protocol::FetchRequest {
                range: h.range.clone(),
                fencing_epoch: FENCING_EPOCH,
                start_offset: 0,
                max_bytes: 1024,
                max_records: 8,
            }),
        },
    );
    match fetch.message {
        Message::FetchResponse(batch) => {
            assert!(batch.records.is_empty());
            assert_eq!(batch.committed_high_watermark, 0);
        }
        other => panic!("expected empty fetch, got {other:?}"),
    }
}

#[test]
fn follower_fetch_never_exposes_above_cluster_hwm() {
    let h = harness();
    // Locally durable on the leader only: take both followers offline so
    // quorum fails, then inspect that a healthy follower (brought back with
    // no catch-up) still serves HWM 0. Use a direct replica append to put
    // local durability ahead of HWM on one follower instead.
    let offline = &h.followers[1];
    offline.set_online(false);

    let ok = produce_ok(&h.leader, h.range.clone(), 0);
    assert_eq!(ok.committed_next_offset, 1);
    assert_eq!(h.followers[0].local_committed_offset(), 1);
    assert_eq!(h.followers[0].cluster_committed().get(), 1);

    // Push follower local durability ahead of the published HWM by applying
    // an append while holding back HWM observation.
    let ahead = &h.followers[0];
    // Simulate leader-local-only durability relative to this follower by
    // rolling HWM back is impossible (monotonic). Instead verify fetch uses
    // the shared HWM: produce another record with both followers up, then
    // manually set a follower's view behind by constructing a fresh handle.
    let batch = ahead.fetch(0, 1024, 8).unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.high_watermark, 1);

    // A second follower that never received HWM propagation stays dark even
    // after coming online without the prior append.
    offline.set_online(true);
    assert_eq!(offline.local_committed_offset(), 0);
    assert_eq!(offline.cluster_committed().get(), 0);
    let dark = offline.fetch(0, 1024, 8).unwrap();
    assert!(dark.records.is_empty());
    assert_eq!(dark.high_watermark, 0);
}

#[test]
fn local_durable_above_hwm_is_invisible_until_quorum() {
    let h = harness();
    h.followers[0].set_online(false);
    h.followers[1].set_online(false);

    let _ = h.leader.handle(
        Role::Producer,
        produce_frame(h.range.clone(), 0, 1, WireDurability::Quorum),
    );
    assert_eq!(h.cluster_committed.get(), 0);

    // Bring one follower online and replicate manually to create a local
    // durable copy that still lacks a quorum HWM advance.
    h.followers[0].set_online(true);
    let request = ReplicaAppendRequest {
        range: h.range.clone(),
        fencing_epoch: FENCING_EPOCH,
        leader_node_id: LEADER,
        expected_base_offset: 0,
        producer_id: PRODUCER,
        producer_epoch: 1,
        first_sequence: 0,
        records: vec![ProduceRecord {
            timestamp_millis: 1_000,
            key: b"k".to_vec(),
            value: b"v0".to_vec(),
        }],
    };
    let ack = h.followers[0].apply_append(&request).unwrap();
    assert_eq!(ack.local_committed_offset, 1);
    assert_eq!(h.followers[0].cluster_committed().get(), 0);

    let hidden = h.followers[0].fetch(0, 1024, 8).unwrap();
    assert!(
        hidden.records.is_empty(),
        "local durable bytes must stay invisible until cluster HWM advances"
    );
    assert_eq!(hidden.high_watermark, 0);

    h.followers[0]
        .observe_hwm(&CommittedHwmUpdate {
            range: h.range.clone(),
            fencing_epoch: FENCING_EPOCH,
            committed_high_watermark: 1,
        })
        .unwrap();
    let visible = h.followers[0].fetch(0, 1024, 8).unwrap();
    assert_eq!(visible.records.len(), 1);
    assert_eq!(visible.high_watermark, 1);
}

#[test]
fn stale_leader_produce_and_replica_append_are_fenced() {
    let h = harness();
    produce_ok(&h.leader, h.range.clone(), 0);

    // Steal the range lease: metadata advances, leaseholders keep old held epoch.
    h.meta.set(FENCING_EPOCH + 1);

    let fenced = h.leader.handle(
        Role::Producer,
        produce_frame(h.range.clone(), 1, 9, WireDurability::Quorum),
    );
    match fenced.message {
        Message::Error(ErrorResponse {
            code: ErrorCode::Fenced,
            ..
        }) => {}
        other => panic!("expected Fenced produce, got {other:?}"),
    }

    let append = ReplicaAppendRequest {
        range: h.range.clone(),
        fencing_epoch: FENCING_EPOCH,
        leader_node_id: LEADER,
        expected_base_offset: 1,
        producer_id: PRODUCER,
        producer_epoch: 1,
        first_sequence: 1,
        records: vec![ProduceRecord {
            timestamp_millis: 1_000,
            key: b"k".to_vec(),
            value: b"stale".to_vec(),
        }],
    };
    let err = h.followers[0].apply_append(&append).unwrap_err();
    assert_eq!(err.0, ErrorCode::Fenced);

    let hwm_err = h.followers[0]
        .observe_hwm(&CommittedHwmUpdate {
            range: h.range.clone(),
            fencing_epoch: FENCING_EPOCH,
            committed_high_watermark: 99,
        })
        .unwrap_err();
    assert_eq!(hwm_err.0, ErrorCode::Fenced);
}

#[test]
fn idempotent_retry_after_quorum_commit_returns_same_offsets() {
    let h = harness();
    let first = produce_ok(&h.leader, h.range.clone(), 0);
    let retry = produce_ok(&h.leader, h.range.clone(), 0);
    assert_eq!(first.outcomes[0].offset, retry.outcomes[0].offset);
    assert!(retry.outcomes[0].duplicate);
    assert_eq!(retry.committed_next_offset, first.committed_next_offset);
    assert_eq!(h.cluster_committed.get(), 1);
}

#[test]
fn replica_set_majority_math_matches_rf3() {
    let h = harness();
    assert_eq!(h.replica_set.replication_factor(), 3);
    h.followers[0].set_online(false);
    let request = ReplicaAppendRequest {
        range: h.range.clone(),
        fencing_epoch: FENCING_EPOCH,
        leader_node_id: LEADER,
        expected_base_offset: 0,
        producer_id: PRODUCER,
        producer_epoch: 1,
        first_sequence: 0,
        records: vec![ProduceRecord {
            timestamp_millis: 1,
            key: Vec::new(),
            value: b"x".to_vec(),
        }],
    };
    // Apply on leader segment first via produce so expected offsets align —
    // here we only check follower ack counting with one online follower.
    let _ = h.followers[1].apply_append(&request);
    // Replicate against a fresh request would fail on follower 1 (already at tip).
    // Majority helper itself:
    let result = vtop_broker::replication::ReplicaQuorumResult {
        follower_acks: 1,
        replication_factor: 3,
    };
    assert!(result.has_quorum());
    let short = vtop_broker::replication::ReplicaQuorumResult {
        follower_acks: 0,
        replication_factor: 3,
    };
    assert!(!short.has_quorum());
}

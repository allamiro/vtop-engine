//! A restarted leader whose log is SHORTER than its followers' (#599, the case
//! #923 left open).
//!
//! The leader lost a tail across the restart that both followers kept: records
//! it never fsynced reached the followers first, or its own log was cut short.
//! The followers hold offsets the leader does not, and the leader's next write
//! lands at an offset they already hold, with other bytes.
//!
//! The rule, from the replication design: the leader of an epoch holds every
//! record that epoch's quorum acknowledged (it fsyncs before it replicates and
//! a quorum acknowledgement needs its own durability), so when it appends at
//! offset X, nothing at or above X on a follower was acknowledged. That is the
//! conflict rule of an append that finds a different record at its offset: the
//! follower discards its records from X and takes the leader's, as it does for
//! a dispute a fence kept (#611). Two guards stay: a record the follower knows
//! was acknowledged (below its committed high-water mark) is never discarded,
//! and a record the leader sends that the follower already holds, byte for
//! byte, is a retry acknowledged as it always was.
//!
//! Before the fix a follower whose log was longer acknowledged the leader's
//! append without storing it (its tail "was durable through" the batch end),
//! so a quorum was reported for a record the follower did not hold, and the
//! replicas disagreed at that offset from then on.

use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use uuid::Uuid;
use vtop_broker::replication::{
    ClusterCommittedOffset, FlowControlConfig, InProcessFollower, LeaderLineage,
    NetworkFollowerConfig, NetworkedReplicaSet, ReplicaPeerHandler, ReplicaPeerServer, ReplicaSet,
    ReplicaTlsMaterial,
};
use vtop_broker::{LocalBroker, MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::{
    ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentConfigV2, SegmentDescriptor,
    SegmentDescriptorV2,
};
use vtop_protocol::{
    Durability as WireDurability, ErrorCode, ErrorResponse, Message, ProduceRecord, ProduceRequest,
    RangeIdentity, ReplicaAppendRequest, Role, WireFrame,
};

const LEADER: Uuid = Uuid::from_u128(0xA1);
const FOLLOWERS: [Uuid; 2] = [Uuid::from_u128(0xA2), Uuid::from_u128(0xA3)];
const PRODUCER: Uuid = Uuid::from_u128(0xB1);
const EPOCH: u64 = 18;

/// Records every replica holds, and the records only the followers hold: the
/// leader lost that tail across the restart.
const SHARED: u64 = 5;
const FOLLOWER_ONLY: u64 = 4;

fn range_identity() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xC1),
        range_generation: 0,
    }
}

/// A log in the v2 format, or in v1, the default writer: a v1 frame stores
/// the producer identity merged with its epoch, so it is the harder case for
/// re-sending a record.
fn create_segment(
    dir: &TempDir,
    segment_id: u128,
    range: &RangeIdentity,
    v2: bool,
) -> ActiveSegment {
    let lineage = RangeLineage {
        range_id: range.range_id,
        generation: range.range_generation,
        key_range: KeyRange::full(),
        parents: Vec::new(),
    };
    let path = dir.path().join("range.active");
    if v2 {
        ActiveSegment::create_v2(
            path,
            SegmentDescriptorV2 {
                segment_id: Uuid::from_u128(segment_id),
                topic: range.topic.clone(),
                topic_epoch: range.topic_epoch,
                lineage,
                base_offset: 0,
                segment_generation: 0,
                creation_node_id: LEADER,
                creation_fencing_epoch: EPOCH,
            },
            SegmentConfigV2::default(),
        )
        .unwrap()
    } else {
        ActiveSegment::create(
            path,
            SegmentDescriptor {
                segment_id: Uuid::from_u128(segment_id),
                topic: range.topic.clone(),
                topic_epoch: range.topic_epoch,
                lineage,
                base_offset: 0,
            },
            SegmentConfig::default(),
        )
        .unwrap()
    }
}

fn record_value(sequence: u64) -> Vec<u8> {
    format!("v{sequence}").into_bytes()
}

fn produce_request(
    range: &RangeIdentity,
    sequence: u64,
    durability: WireDurability,
) -> ProduceRequest {
    ProduceRequest {
        range: range.clone(),
        fencing_epoch: EPOCH,
        producer_id: PRODUCER,
        producer_epoch: 1,
        first_sequence: sequence,
        durability,
        records: vec![ProduceRecord {
            timestamp_millis: 1_000,
            key: b"k".to_vec(),
            value: record_value(sequence),
        }],
    }
}

fn produce(
    broker: &LocalBroker,
    range: &RangeIdentity,
    sequence: u64,
    durability: WireDurability,
) -> Result<(), ErrorResponse> {
    let frame = WireFrame {
        request_id: sequence + 1,
        stream_id: 1,
        message: Message::ProduceRequest(produce_request(range, sequence, durability)),
    };
    match broker.handle(Role::Producer, frame).message {
        Message::ProduceResponse(_) => Ok(()),
        Message::Error(error) => Err(error),
        other => panic!("unexpected response: {other:?}"),
    }
}

struct Cert {
    cert: CertificateDer<'static>,
    key: PrivatePkcs8KeyDer<'static>,
}

fn cert_for(node: Uuid) -> Cert {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, node.to_string());
    let cert = params.self_signed(&key).unwrap();
    Cert {
        cert: cert.der().clone(),
        key: PrivatePkcs8KeyDer::from(key.serialize_der()),
    }
}

fn material(identity: &Cert, trust: &[&Cert]) -> ReplicaTlsMaterial {
    let mut trust_roots = rustls::RootCertStore::empty();
    for peer in trust {
        trust_roots.add(peer.cert.clone()).unwrap();
    }
    ReplicaTlsMaterial {
        certificate_chain: vec![identity.cert.clone()],
        private_key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            identity.key.secret_pkcs8_der().to_vec(),
        )),
        trust_roots,
    }
}

fn served_values(follower: &InProcessFollower) -> Vec<String> {
    follower
        .fetch(0, usize::MAX, 64)
        .map(|batch| {
            batch
                .records
                .iter()
                .map(|fetched| String::from_utf8_lossy(&fetched.record.value).into_owned())
                .collect()
        })
        .unwrap_or_default()
}

fn await_within(deadline: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let end = std::time::Instant::now() + deadline;
    loop {
        if condition() {
            return true;
        }
        if std::time::Instant::now() >= end {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The post-restart write: other bytes than the lost tail's record at the
/// same offset.
fn new_value() -> Vec<u8> {
    b"after-restart".to_vec()
}

fn wire_record(value: Vec<u8>) -> ProduceRecord {
    ProduceRecord {
        timestamp_millis: 1_000,
        key: b"k".to_vec(),
        value,
    }
}

fn append_request(range: &RangeIdentity, sequence: u64, value: Vec<u8>) -> ReplicaAppendRequest {
    ReplicaAppendRequest {
        range: range.clone(),
        fencing_epoch: EPOCH,
        leader_node_id: LEADER,
        expected_base_offset: sequence,
        producer_id: PRODUCER,
        producer_epoch: 1,
        first_sequence: sequence,
        records: vec![wire_record(value)],
    }
}

fn produce_value(
    broker: &LocalBroker,
    range: &RangeIdentity,
    sequence: u64,
    value: Vec<u8>,
) -> Result<(), ErrorResponse> {
    let mut request = produce_request(range, sequence, WireDurability::Quorum);
    request.records[0].value = value;
    let frame = WireFrame {
        request_id: sequence + 1,
        stream_id: 1,
        message: Message::ProduceRequest(request),
    };
    match broker.handle(Role::Producer, frame).message {
        Message::ProduceResponse(_) => Ok(()),
        Message::Error(error) => Err(error),
        other => panic!("unexpected response: {other:?}"),
    }
}

/// A follower holding SHARED records, then FOLLOWER_ONLY more of the lost
/// tail, with `floor` as the high-water mark it has observed.
fn follower_with_tail(
    node: Uuid,
    dir: &TempDir,
    segment_id: u128,
    range: &RangeIdentity,
    v2: bool,
    meta: &MetaFencingEpoch,
    floor: &ClusterCommittedOffset,
) -> Arc<InProcessFollower> {
    let follower = Arc::new(
        InProcessFollower::new(
            node,
            create_segment(dir, segment_id, range, v2),
            ProducerEpochJournal::open(dir.path().join("epochs")).unwrap(),
            range.clone(),
            EPOCH,
            meta.clone(),
            floor.clone(),
        )
        .unwrap(),
    );
    for sequence in 0..SHARED + FOLLOWER_ONLY {
        follower
            .apply_append(&append_request(range, sequence, record_value(sequence)))
            .unwrap();
    }
    assert_eq!(follower.next_offset(), SHARED + FOLLOWER_ONLY);
    assert_eq!(follower.local_committed_offset(), SHARED + FOLLOWER_ONLY);
    follower
}

/// What the restarted leader's first write is.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FirstWrite {
    /// Other bytes than the lost tail held at that offset.
    NewRecord,
    /// The client's retry of the very record the leader lost: the follower
    /// holds it already.
    RetryOfLostRecord,
}

/// Stage the restart, over `tls` or a plaintext plane: a leader holding
/// SHARED records, two followers holding SHARED + FOLLOWER_ONLY, the
/// followers' high-water mark at `floor`. Returns the outcome of the leader's
/// first quorum write, and the followers; the plane stays up while the third
/// value is held.
fn restarted_leader_behind_both_followers(
    tls: bool,
    v2: bool,
    floor: u64,
    first_write: FirstWrite,
) -> (
    Result<(), ErrorResponse>,
    Vec<Arc<InProcessFollower>>,
    impl Sized,
) {
    let runtime: Runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let range = range_identity();
    let meta = MetaFencingEpoch::new(EPOCH);

    // The leader's log after the restart: the shared prefix, no more.
    let leader_dir = tempfile::tempdir().unwrap();
    {
        let leader = LocalBroker::new(
            create_segment(&leader_dir, 0xD1, &range, v2),
            ProducerEpochJournal::open(leader_dir.path().join("epochs")).unwrap(),
            range.clone(),
            EPOCH,
        )
        .unwrap();
        for sequence in 0..SHARED {
            produce(&leader, &range, sequence, WireDurability::LocalFsync).unwrap();
        }
    }
    let mut follower_dirs = Vec::new();
    let mut followers = Vec::new();
    for (index, node) in FOLLOWERS.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        followers.push(follower_with_tail(
            node,
            &dir,
            0xE1 + index as u128,
            &range,
            v2,
            &meta,
            &ClusterCommittedOffset::new(floor),
        ));
        follower_dirs.push(dir);
    }

    let leader_certs = cert_for(LEADER);
    let follower_certs = [cert_for(FOLLOWERS[0]), cert_for(FOLLOWERS[1])];
    let mut configs = Vec::new();
    let mut servers = Vec::new();
    for (index, node) in FOLLOWERS.into_iter().enumerate() {
        let handler = Arc::clone(&followers[index]) as Arc<dyn ReplicaPeerHandler>;
        let server_material = material(&follower_certs[index], &[&leader_certs]);
        let (addr, handle): (SocketAddr, _) = runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = if tls {
                ReplicaPeerServer::new(server_material, node, handler).unwrap()
            } else {
                ReplicaPeerServer::plaintext(node, handler)
            };
            let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                let _shutdown = shutdown;
                let _ = server.serve(listener, shutdown_rx).await;
            });
            (addr, task.abort_handle())
        });
        servers.push(handle);
        configs.push(NetworkFollowerConfig {
            node_id: node,
            addr,
            host: None,
            server_name: "localhost".to_owned(),
        });
    }
    let flow = FlowControlConfig {
        reconnect_backoff: Duration::from_millis(50),
        ack_timeout: Duration::from_millis(500),
        ..FlowControlConfig::default()
    };
    let replica_set = Arc::new(if tls {
        NetworkedReplicaSet::start_on_handle(
            runtime.handle().clone(),
            configs,
            material(&leader_certs, &[&follower_certs[0], &follower_certs[1]]),
            flow,
        )
        .unwrap()
    } else {
        NetworkedReplicaSet::start_plaintext_on_handle(
            runtime.handle().clone(),
            configs,
            flow,
            None,
        )
        .unwrap()
    });
    let leader = Arc::new(
        LocalBroker::with_replication(
            ActiveSegment::recover(leader_dir.path().join("range.active")).unwrap(),
            ProducerEpochJournal::open(leader_dir.path().join("epochs")).unwrap(),
            range.clone(),
            EPOCH,
            meta.clone(),
            LEADER,
            Some(ClusterCommittedOffset::new(0)),
            Some(replica_set.clone() as Arc<dyn ReplicaSet>),
        )
        .unwrap(),
    );
    assert!(
        replica_set.reconcile_followers_against(
            Arc::downgrade(&leader) as std::sync::Weak<dyn LeaderLineage>
        )
    );

    let value = match first_write {
        FirstWrite::NewRecord => new_value(),
        FirstWrite::RetryOfLostRecord => record_value(SHARED),
    };
    // Retried within a bound, as a client retries `quorum not reached`. An
    // answer that is not a retryable refusal ends it.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let outcome = loop {
        match produce_value(&leader, &range, SHARED, value.clone()) {
            Ok(()) => break Ok(()),
            Err(error)
                if error.retryable
                    && error.code == ErrorCode::Overloaded
                    && std::time::Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => break Err(error),
        }
    };
    (
        outcome,
        followers,
        (runtime, servers, leader, follower_dirs),
    )
}

fn expected_after_new_record() -> Vec<String> {
    (0..SHARED)
        .map(|sequence| String::from_utf8(record_value(sequence)).unwrap())
        .chain(std::iter::once(String::from_utf8(new_value()).unwrap()))
        .collect()
}

/// The followers hold records the restarted leader does not, never
/// acknowledged. Its first write goes through, over them: both followers end
/// holding the leader's log, and the records only they held are gone.
fn a_leader_behind_its_followers_replaces_their_unacknowledged_tail(tls: bool, v2: bool) {
    let (outcome, followers, _plane) =
        restarted_leader_behind_both_followers(tls, v2, 0, FirstWrite::NewRecord);
    assert!(
        outcome.is_ok(),
        "a quorum produce must succeed after the restart; it was refused: {:?} \
         (follower logs end at {} and {})",
        outcome.err().map(|error| error.message),
        followers[0].next_offset(),
        followers[1].next_offset(),
    );
    let expected = expected_after_new_record();
    for (index, follower) in followers.iter().enumerate() {
        // One follower's ack is the quorum; the other takes the leader's
        // append on its own session, so both are awaited.
        assert!(
            await_within(Duration::from_secs(10), || follower.next_offset()
                == SHARED + 1),
            "follower {index} must hold exactly the leader's {} records, not the \
             {} it kept from before the restart: it holds {}",
            SHARED + 1,
            SHARED + FOLLOWER_ONLY,
            follower.next_offset(),
        );
        // The leader's mark reaches the follower with the next frame, so it
        // is awaited, then the served records are the leader's.
        assert!(
            await_within(Duration::from_secs(10), || served_values(follower)
                == expected),
            "follower {index} must serve the leader's records {expected:?}, serves {:?}",
            served_values(follower),
        );
    }
}

#[test]
fn a_v1_leader_behind_its_followers_replaces_their_unacknowledged_tail_on_a_plaintext_plane() {
    a_leader_behind_its_followers_replaces_their_unacknowledged_tail(false, false);
}

#[test]
fn a_v1_leader_behind_its_followers_replaces_their_unacknowledged_tail_on_a_tls_plane() {
    a_leader_behind_its_followers_replaces_their_unacknowledged_tail(true, false);
}

#[test]
fn a_v2_leader_behind_its_followers_replaces_their_unacknowledged_tail_on_a_plaintext_plane() {
    a_leader_behind_its_followers_replaces_their_unacknowledged_tail(false, true);
}

#[test]
fn a_v2_leader_behind_its_followers_replaces_their_unacknowledged_tail_on_a_tls_plane() {
    a_leader_behind_its_followers_replaces_their_unacknowledged_tail(true, true);
}

/// The client retries the record the leader lost, and the followers hold it
/// byte for byte: that is a retry, acknowledged, and nothing is discarded. The
/// followers' tail stays, a prefix-consistent log the leader grows into.
#[test]
fn a_retry_of_the_record_a_follower_already_holds_discards_nothing() {
    for v2 in [false, true] {
        let (outcome, followers, _plane) =
            restarted_leader_behind_both_followers(false, v2, 0, FirstWrite::RetryOfLostRecord);
        assert!(outcome.is_ok(), "v2={v2}: {:?}", outcome.err());
        for follower in &followers {
            assert_eq!(
                follower.next_offset(),
                SHARED + FOLLOWER_ONLY,
                "v2={v2}: a record the leader re-sends byte for byte is not a conflict, and the \
                 records after it are not discarded"
            );
        }
    }
}

/// The followers' high-water mark says the tail was acknowledged (they
/// observed it before the restart), and the leader lost it. Nothing is
/// discarded and the leader's write is not acknowledged: the leader is the one
/// that is missing records, and only a repair of the leader can fix that.
#[test]
fn an_acknowledged_tail_is_never_discarded_and_the_write_over_it_is_refused() {
    for v2 in [false, true] {
        let (outcome, followers, _plane) = restarted_leader_behind_both_followers(
            false,
            v2,
            SHARED + FOLLOWER_ONLY,
            FirstWrite::NewRecord,
        );
        assert!(
            outcome.is_err(),
            "v2={v2}: a write over records a quorum acknowledged must not be acknowledged"
        );
        for follower in &followers {
            assert_eq!(follower.next_offset(), SHARED + FOLLOWER_ONLY, "v2={v2}");
            assert_eq!(
                served_values(follower).len() as u64,
                SHARED + FOLLOWER_ONLY,
                "v2={v2}: every acknowledged record is still served"
            );
        }
    }
}

/// The follower alone, without a network: a leader's append over its tail is a
/// conflict when the bytes differ, below the mark it is refused, and a batch of
/// appends is judged the same way.
#[test]
fn an_append_over_a_followers_tail_is_judged_by_its_bytes_and_its_mark() {
    for v2 in [false, true] {
        let range = range_identity();
        let meta = MetaFencingEpoch::new(EPOCH);

        // Different bytes, unacknowledged: discarded from the offset, then
        // appended.
        let dir = tempfile::tempdir().unwrap();
        let mark = ClusterCommittedOffset::new(0);
        let follower = follower_with_tail(FOLLOWERS[0], &dir, 0xE1, &range, v2, &meta, &mark);
        let response = follower
            .apply_append(&append_request(&range, SHARED, new_value()))
            .unwrap();
        assert_eq!(response.local_committed_offset, SHARED + 1, "v2={v2}");
        assert_eq!(follower.next_offset(), SHARED + 1, "v2={v2}");
        mark.advance_to(SHARED + 1);
        assert_eq!(
            served_values(&follower),
            expected_after_new_record(),
            "v2={v2}"
        );

        // The same, as the second member of a batch whose first member is a
        // retry of a record the follower holds.
        let dir = tempfile::tempdir().unwrap();
        let mark = ClusterCommittedOffset::new(0);
        let follower = follower_with_tail(FOLLOWERS[0], &dir, 0xE2, &range, v2, &meta, &mark);
        let response = follower
            .apply_append_batch(&[
                append_request(&range, SHARED, record_value(SHARED)),
                append_request(&range, SHARED + 1, new_value()),
            ])
            .unwrap();
        assert_eq!(response.local_committed_offset, SHARED + 2, "v2={v2}");
        assert_eq!(follower.next_offset(), SHARED + 2, "v2={v2}");
        mark.advance_to(SHARED + 2);
        let mut expected: Vec<String> = (0..=SHARED)
            .map(|sequence| String::from_utf8(record_value(sequence)).unwrap())
            .collect();
        expected.push(String::from_utf8(new_value()).unwrap());
        assert_eq!(
            served_values(&follower),
            expected,
            "v2={v2}: the retried record and the shared prefix are untouched, and the \
             conflicting one is the leader's"
        );

        // Different bytes below the follower's mark: refused, nothing lost.
        let dir = tempfile::tempdir().unwrap();
        let follower = follower_with_tail(
            FOLLOWERS[0],
            &dir,
            0xE3,
            &range,
            v2,
            &meta,
            &ClusterCommittedOffset::new(SHARED + FOLLOWER_ONLY),
        );
        let refusal = follower
            .apply_append(&append_request(&range, SHARED, new_value()))
            .expect_err("an append over acknowledged records must be refused");
        assert!(
            refusal.1.contains("acknowledged") || refusal.1.contains("high-water mark"),
            "v2={v2}: the refusal says why: {}",
            refusal.1
        );
        assert_eq!(follower.next_offset(), SHARED + FOLLOWER_ONLY, "v2={v2}");

        // A retry of what it holds, below the mark: still just acknowledged.
        let response = follower
            .apply_append(&append_request(&range, SHARED, record_value(SHARED)))
            .unwrap();
        assert_eq!(
            response.local_committed_offset,
            SHARED + FOLLOWER_ONLY,
            "v2={v2}"
        );
    }
}

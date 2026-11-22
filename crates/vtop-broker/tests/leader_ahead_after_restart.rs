//! A restarted leader whose log is longer than every follower's (#599).
//!
//! A whole replica set killed at once and restarted leaves the leader holding
//! records its followers never received: it appended them and fsynced, the
//! replica stream was still carrying them, and the quorum never formed, so no
//! client was told they were stored. The restarted leader's retransmission
//! buffer is empty, so the only copy of those records is its own log.
//!
//! Invariant (liveness after the fault clears): the followers are caught up
//! from the leader's log, a quorum produce succeeds, and every replica ends
//! holding the same bytes. The leader's unacknowledged tail is not discarded:
//! the leader's log is authoritative at its epoch (the same rule a promoted
//! leader follows, #887), and a record a client was never told about may
//! appear.
//!
//! Before the fix this held only for a v2 range on a TLS plane. A plaintext
//! session carries no fence, and catch-up from the log was gated on one; and a
//! v1 range, the default writer, could not re-send its records at all, since
//! a v1 frame stores the producer identity merged with its epoch. Every append
//! to a follower behind the leader then failed its base-offset check and the
//! session was re-established for ever with `follower_connected` at 0.

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

/// Records every replica holds before the crash, and the records only the
/// leader holds.
const SHARED: u64 = 5;
const LEADER_ONLY: u64 = 4;

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

/// Stage the crash and the restart, over `tls` or a plaintext plane.
fn restarted_leader_ahead_of_both_followers(tls: bool, v2: bool) {
    let runtime: Runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let range = range_identity();
    let meta = MetaFencingEpoch::new(EPOCH);

    // BEFORE THE CRASH. Every replica holds SHARED records; the leader alone
    // holds LEADER_ONLY more, fsynced locally and never acknowledged by a
    // quorum.
    let leader_dir = tempfile::tempdir().unwrap();
    {
        let leader = LocalBroker::new(
            create_segment(&leader_dir, 0xD1, &range, v2),
            ProducerEpochJournal::open(leader_dir.path().join("epochs")).unwrap(),
            range.clone(),
            EPOCH,
        )
        .unwrap();
        for sequence in 0..SHARED + LEADER_ONLY {
            produce(&leader, &range, sequence, WireDurability::LocalFsync).unwrap();
        }
    }
    let mut follower_dirs = Vec::new();
    let mut followers = Vec::new();
    for (index, node) in FOLLOWERS.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let follower = Arc::new(
            InProcessFollower::new(
                node,
                create_segment(&dir, 0xE1 + index as u128, &range, v2),
                ProducerEpochJournal::open(dir.path().join("epochs")).unwrap(),
                range.clone(),
                EPOCH,
                meta.clone(),
                ClusterCommittedOffset::new(0),
            )
            .unwrap(),
        );
        for sequence in 0..SHARED {
            follower
                .apply_append(&ReplicaAppendRequest {
                    range: range.clone(),
                    fencing_epoch: EPOCH,
                    leader_node_id: LEADER,
                    expected_base_offset: sequence,
                    producer_id: PRODUCER,
                    producer_epoch: 1,
                    first_sequence: sequence,
                    records: vec![ProduceRecord {
                        timestamp_millis: 1_000,
                        key: b"k".to_vec(),
                        value: record_value(sequence),
                    }],
                })
                .unwrap();
        }
        assert_eq!(follower.local_committed_offset(), SHARED);
        follower_dirs.push(dir);
        followers.push(follower);
    }

    // THE RESTART. The leader recovers its log from disk; its replica set is
    // new and its retransmission buffer is empty.
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
            // Held for the runtime's lifetime: dropping the sender stops the
            // server.
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
    // As `data_node` wires a leader.
    assert!(
        replica_set.reconcile_followers_against(
            Arc::downgrade(&leader) as std::sync::Weak<dyn LeaderLineage>
        )
    );

    // The next client write after the restart. A refusal is retried within a
    // bound, as a client retries `quorum not reached`; the bound is generous
    // against a loaded machine, not against a stuck leader.
    let next = SHARED + LEADER_ONLY;
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let outcome = loop {
        match produce(&leader, &range, next, WireDurability::Quorum) {
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
    let connected: Vec<_> = FOLLOWERS
        .iter()
        .map(|node| replica_set.follower_connected(*node))
        .collect();
    assert!(
        outcome.is_ok(),
        "a quorum produce must succeed after the restart; it was refused: {:?} \
         (followers connected: {connected:?}; follower logs end at {} and {}; leader log holds {})",
        outcome.err().map(|error| error.message),
        followers[0].next_offset(),
        followers[1].next_offset(),
        next,
    );

    // Nothing acknowledged is lost, nothing reordered, and every replica
    // that took part holds the leader's bytes in the leader's order.
    let expected: Vec<String> = (0..=next)
        .map(|sequence| String::from_utf8(record_value(sequence)).unwrap())
        .collect();
    let caught_up = await_within(Duration::from_secs(10), || {
        followers
            .iter()
            .all(|follower| served_values(follower) == expected)
    });
    assert!(
        caught_up,
        "both followers must end holding the leader's {} records: {:?} / {:?}",
        expected.len(),
        served_values(&followers[0]),
        served_values(&followers[1]),
    );
    assert!(await_within(Duration::from_secs(5), || {
        FOLLOWERS
            .iter()
            .all(|node| replica_set.follower_connected(*node) == Some(true))
    }));
    drop(servers);
}

/// The shape #599 found: the default writer (v1) on a static plaintext
/// plane, every node killed at once.
#[test]
fn a_restarted_v1_leader_ahead_of_both_followers_catches_them_up_on_a_plaintext_plane() {
    restarted_leader_ahead_of_both_followers(false, false);
}

/// The same crash on an mTLS plane, whose sessions are fenced. A v1 range's
/// records could not be re-sent, so this plane was stuck too.
#[test]
fn a_restarted_v1_leader_ahead_of_both_followers_catches_them_up_on_a_tls_plane() {
    restarted_leader_ahead_of_both_followers(true, false);
}

/// A v2 range on a plaintext plane: the fence the catch-up waited for is not
/// sent there.
#[test]
fn a_restarted_v2_leader_ahead_of_both_followers_catches_them_up_on_a_plaintext_plane() {
    restarted_leader_ahead_of_both_followers(false, true);
}

/// A v2 range on an mTLS plane, whose fenced sessions were already caught up
/// from the log (#887): kept so the planes and formats cannot diverge again.
#[test]
fn a_restarted_v2_leader_ahead_of_both_followers_catches_them_up_on_a_tls_plane() {
    restarted_leader_ahead_of_both_followers(true, true);
}

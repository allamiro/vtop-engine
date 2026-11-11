//! The replica plane's version handshake across releases (#634).
//!
//! The plane had no negotiation, so the handshake has to work against a peer
//! that does not speak it. These tests pit both directions of that against
//! this build:
//!
//! * a NEW dialer (the leader's replication stream, the one-shot status
//!   client) against a follower that predates the handshake. That follower
//!   is emulated here with the dispatch every release before #634 runs: it
//!   decodes the hello (a kind-1 `ClientHello`) and answers it from its
//!   catch-all arm with `InvalidRequest`, keeping the session open. The arm
//!   is the one `dispatch_replica_frame` still has, pinned by a unit test
//!   beside it;
//! * an OLD dialer - one whose first frame is a request, not a hello -
//!   against this build's server.
//!
//! Each runs before and after the cluster's level is finalized past what an
//! old peer can run: before, the old protocol is spoken; after, the old peer
//! is refused, by name, on both sides.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use uuid::Uuid;
use vtop_broker::replication::version::LEGACY_PEERS_ACCEPTED_THROUGH;
use vtop_broker::replication::{
    ClusterFeatureLevel, FlowControlConfig, NetworkFollowerConfig, NetworkedReplicaSet,
    ReplicaPeerHandler, ReplicaPeerServer, ReplicaSet, ReplicaStatusClient, ReplicaVersionPolicy,
};
use vtop_broker::BrokerError;
use vtop_protocol::{
    read_frame, write_frame, CommittedHwmUpdate, ErrorCode, ErrorResponse, Message, ProduceRecord,
    ProtocolLimits, RangeIdentity, ReplicaAppendRequest, ReplicaAppendResponse, ReplicaHello,
    ReplicaHelloAccept, ReplicaStatusRequest, ReplicaStatusResponse, WireFrame,
    REPLICA_PROTOCOL_HANDSHAKE, REPLICA_PROTOCOL_LEGACY, REPLICA_PROTOCOL_MAX,
};

const LEADER: Uuid = Uuid::from_u128(0x634a);
const FOLLOWER: Uuid = Uuid::from_u128(0x634b);
const LIMITS: ProtocolLimits = ProtocolLimits {
    max_frame_bytes: vtop_protocol::DEFAULT_MAX_FRAME_BYTES,
    max_records: vtop_protocol::DEFAULT_MAX_RECORDS,
};

/// The first level an old peer cannot run.
const FINALIZED_PAST_OLD_PEERS: u32 = LEGACY_PEERS_ACCEPTED_THROUGH + 1;

fn range() -> RangeIdentity {
    RangeIdentity {
        topic: "versions.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0x634c),
        range_generation: 1,
    }
}

fn append(base: u64) -> ReplicaAppendRequest {
    ReplicaAppendRequest {
        range: range(),
        fencing_epoch: 1,
        leader_node_id: LEADER,
        expected_base_offset: base,
        producer_id: Uuid::from_u128(0x634d),
        producer_epoch: 0,
        first_sequence: base,
        records: vec![ProduceRecord {
            timestamp_millis: 1,
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }],
    }
}

/// A follower on a release before the handshake: every frame kind it serves
/// here, answered as that release's dispatcher answers it - including the
/// catch-all `InvalidRequest` a hello falls into.
async fn serve_like_a_release_before_the_handshake(
    listener: TcpListener,
    seen: Arc<Mutex<Vec<&'static str>>>,
) {
    // One disk, whichever session asks.
    let durable = Arc::new(AtomicU64::new(0));
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let seen = Arc::clone(&seen);
        let durable = Arc::clone(&durable);
        tokio::spawn(async move {
            while let Ok(Some(frame)) = read_frame(&mut stream, LIMITS).await {
                let reply = match frame.message {
                    Message::ReplicaAppendBatchRequest(batch) => {
                        seen.lock().unwrap().push("append");
                        for request in &batch.requests {
                            durable.store(
                                request.expected_base_offset + request.records.len() as u64,
                                Ordering::SeqCst,
                            );
                        }
                        Message::ReplicaAppendResponse(ReplicaAppendResponse {
                            local_committed_offset: durable.load(Ordering::SeqCst),
                        })
                    }
                    Message::ReplicaAppendRequest(request) => {
                        seen.lock().unwrap().push("append");
                        durable.store(
                            request.expected_base_offset + request.records.len() as u64,
                            Ordering::SeqCst,
                        );
                        Message::ReplicaAppendResponse(ReplicaAppendResponse {
                            local_committed_offset: durable.load(Ordering::SeqCst),
                        })
                    }
                    Message::ReplicaStatusRequest(_) => {
                        seen.lock().unwrap().push("status");
                        let at = durable.load(Ordering::SeqCst);
                        Message::ReplicaStatusResponse(ReplicaStatusResponse {
                            local_committed_offset: at,
                            next_offset: at,
                        })
                    }
                    Message::CommittedHwmUpdate(_) => continue,
                    Message::ClientHello(_) => {
                        seen.lock().unwrap().push("hello");
                        Message::Error(ErrorResponse {
                            code: ErrorCode::InvalidRequest,
                            retryable: false,
                            message: "unsupported replica peer message".to_owned(),
                        })
                    }
                    _ => Message::Error(ErrorResponse {
                        code: ErrorCode::InvalidRequest,
                        retryable: false,
                        message: "unsupported replica peer message".to_owned(),
                    }),
                };
                let reply = WireFrame {
                    request_id: frame.request_id,
                    stream_id: frame.stream_id,
                    message: reply,
                };
                if write_frame(&mut stream, &reply, LIMITS).await.is_err() {
                    return;
                }
            }
        });
    }
}

/// Just enough of a replica for this build's real server to serve.
struct Replica {
    durable: AtomicU64,
}

impl ReplicaPeerHandler for Replica {
    fn node_id(&self) -> Uuid {
        FOLLOWER
    }
    fn apply_append(
        &self,
        request: &ReplicaAppendRequest,
    ) -> Result<ReplicaAppendResponse, (ErrorCode, String)> {
        self.apply_append_batch(std::slice::from_ref(request))
    }
    fn apply_append_batch(
        &self,
        requests: &[ReplicaAppendRequest],
    ) -> Result<ReplicaAppendResponse, (ErrorCode, String)> {
        for request in requests {
            self.durable.store(
                request.expected_base_offset + request.records.len() as u64,
                Ordering::SeqCst,
            );
        }
        Ok(ReplicaAppendResponse {
            local_committed_offset: self.durable.load(Ordering::SeqCst),
        })
    }
    fn observe_hwm(&self, _: &CommittedHwmUpdate) -> Result<(), (ErrorCode, String)> {
        Ok(())
    }
    fn status(&self, _: &RangeIdentity) -> Result<ReplicaStatusResponse, (ErrorCode, String)> {
        let at = self.durable.load(Ordering::SeqCst);
        Ok(ReplicaStatusResponse {
            local_committed_offset: at,
            next_offset: at,
        })
    }
}

/// This build's replica server, holding dialers to `level`.
async fn serve_this_build(level: ClusterFeatureLevel) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = ReplicaPeerServer::plaintext(
        FOLLOWER,
        Arc::new(Replica {
            durable: AtomicU64::new(0),
        }),
    )
    .with_cluster_level(level);
    let (shutdown, stop) = tokio::sync::oneshot::channel();
    // Held for the life of the test process: dropping the sender would
    // read as a shutdown.
    std::mem::forget(shutdown);
    tokio::spawn(server.serve(listener, stop));
    addr
}

fn leader_of(
    runtime: &Runtime,
    follower: SocketAddr,
    level: ClusterFeatureLevel,
) -> NetworkedReplicaSet {
    NetworkedReplicaSet::start_plaintext_on_handle(
        runtime.handle().clone(),
        vec![NetworkFollowerConfig {
            node_id: FOLLOWER,
            addr: follower,
            host: None,
            server_name: "localhost".to_owned(),
        }],
        FlowControlConfig {
            ack_timeout: Duration::from_secs(5),
            version: ReplicaVersionPolicy::new(LEADER, level),
            ..FlowControlConfig::default()
        },
        None,
    )
    .unwrap()
}

fn until(within: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    done()
}

fn status_client(level: ClusterFeatureLevel) -> ReplicaStatusClient {
    ReplicaStatusClient::plaintext()
        .with_timeout(Duration::from_secs(5))
        .with_version_policy(ReplicaVersionPolicy::new(LEADER, level))
}

/// A new leader meets a follower that predates the handshake: it falls back
/// to the old protocol and replicates, through every level up to the one
/// that lets data nodes report; once the cluster is finalized past what the
/// old follower can run, the leader refuses it and nothing more is sent.
#[test]
fn a_new_leader_speaks_the_old_protocol_to_an_old_follower_until_finalized() {
    let runtime = Runtime::new().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let old_follower = runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve_like_a_release_before_the_handshake(
            listener,
            Arc::clone(&seen),
        ));
        addr
    });

    for known in [0, LEGACY_PEERS_ACCEPTED_THROUGH] {
        let level = ClusterFeatureLevel::unknown();
        level.observe(known);
        let leader = leader_of(&runtime, old_follower, level.clone());
        assert!(
            until(Duration::from_secs(5), || leader
                .follower_connected(FOLLOWER)
                == Some(true)),
            "at level {known} the leader never opened a session with the old follower"
        );
        assert_eq!(
            leader.follower_protocol(FOLLOWER),
            Some(REPLICA_PROTOCOL_LEGACY),
            "at level {known} the leader must speak the old protocol to it"
        );
        let acked = leader.replicate_append_batch(&[append(0)], 1);
        assert_eq!(
            acked.follower_acks, 1,
            "at level {known} an append over the old protocol must be acknowledged"
        );
        let status = runtime
            .block_on(status_client(level).status(old_follower, "localhost", FOLLOWER, &range()))
            .expect("the one-shot status client falls back too");
        assert_eq!(status.local_committed_offset, 1);
    }
    assert!(seen.lock().unwrap().contains(&"hello"));
    assert!(seen.lock().unwrap().contains(&"append"));

    // Finalized past it.
    let level = ClusterFeatureLevel::unknown();
    level.observe(FINALIZED_PAST_OLD_PEERS);
    seen.lock().unwrap().clear();
    let leader = leader_of(&runtime, old_follower, level.clone());
    assert!(
        !until(Duration::from_millis(1_500), || leader
            .follower_connected(FOLLOWER)
            == Some(true)),
        "a leader at level {FINALIZED_PAST_OLD_PEERS} opened a session with a follower that \
         predates the handshake"
    );
    assert_eq!(
        leader.replicate_append_batch(&[append(1)], 2).follower_acks,
        0
    );
    assert!(
        !seen.lock().unwrap().contains(&"append"),
        "nothing past the hello may reach a refused follower: {:?}",
        seen.lock().unwrap()
    );
    let refused = runtime
        .block_on(status_client(level).status(old_follower, "localhost", FOLLOWER, &range()))
        .unwrap_err();
    assert!(
        matches!(&refused, BrokerError::ReplicaVersion(why) if why.contains("predates")),
        "{refused}"
    );
}

/// This build's follower meets a leader that predates the handshake (its
/// first frame is a request): served as before until the level is
/// finalized past it, then refused with a typed `UnsupportedVersion` that
/// says why, and the session closes.
#[test]
fn a_new_follower_serves_an_old_leader_until_finalized() {
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        let level = ClusterFeatureLevel::unknown();
        let follower = serve_this_build(level.clone()).await;
        // An old leader's first frame is its first request.
        let old_leader_asks = |follower| async move {
            let mut stream = TcpStream::connect(follower).await.unwrap();
            write_frame(
                &mut stream,
                &WireFrame {
                    request_id: 1,
                    stream_id: 0,
                    message: Message::ReplicaStatusRequest(ReplicaStatusRequest { range: range() }),
                },
                LIMITS,
            )
            .await
            .unwrap();
            let reply = read_frame(&mut stream, LIMITS).await.unwrap().unwrap();
            (reply.message, stream)
        };

        for known in [0, LEGACY_PEERS_ACCEPTED_THROUGH] {
            level.observe(known);
            let (reply, _session) =
                tokio::time::timeout(Duration::from_secs(5), old_leader_asks(follower))
                    .await
                    .unwrap();
            assert!(
                matches!(reply, Message::ReplicaStatusResponse(_)),
                "at level {known} an old leader must be served: {reply:?}"
            );
        }

        level.observe(FINALIZED_PAST_OLD_PEERS);
        let (reply, mut session) =
            tokio::time::timeout(Duration::from_secs(5), old_leader_asks(follower))
                .await
                .unwrap();
        let after = tokio::time::timeout(Duration::from_secs(5), read_frame(&mut session, LIMITS))
            .await
            .expect("the refused session must close, not hang");
        match reply {
            Message::Error(ErrorResponse {
                code: ErrorCode::UnsupportedVersion,
                message,
                ..
            }) => assert!(message.contains("predates"), "{message}"),
            other => panic!("an old leader at a finalized level must be refused: {other:?}"),
        }
        assert!(
            matches!(after, Ok(None)) || after.is_err(),
            "the refused session must close: {after:?}"
        );
    });
}

/// Two peers that both speak the handshake meet at the lower protocol, and
/// one that implements less than the finalized level is refused - by the
/// serving side when the dialer is behind, by the dialer when the server is.
#[test]
fn peers_that_speak_the_handshake_negotiate_down_and_one_below_the_level_is_refused() {
    let runtime = Runtime::new().unwrap();
    let level = ClusterFeatureLevel::unknown();
    let follower = runtime.block_on(serve_this_build(level.clone()));
    let hello_from = |max_protocol: u16, supported_feature_level: u32| async move {
        let mut stream = TcpStream::connect(follower).await.unwrap();
        let hello = ReplicaHello {
            node_id: LEADER,
            min_protocol: REPLICA_PROTOCOL_HANDSHAKE,
            max_protocol,
            supported_feature_level,
        };
        write_frame(
            &mut stream,
            &WireFrame {
                request_id: 0,
                stream_id: 0,
                message: hello.to_message(),
            },
            LIMITS,
        )
        .await
        .unwrap();
        read_frame(&mut stream, LIMITS)
            .await
            .unwrap()
            .unwrap()
            .message
    };

    // A future release offering more is met at this build's maximum.
    let reply = runtime.block_on(hello_from(REPLICA_PROTOCOL_MAX + 3, 99));
    let accept = ReplicaHelloAccept::from_message(&reply)
        .unwrap_or_else(|| panic!("a newer peer must be accepted: {reply:?}"));
    assert_eq!(accept.selected_protocol, REPLICA_PROTOCOL_MAX);
    assert_eq!(accept.max_protocol, REPLICA_PROTOCOL_MAX);
    assert_eq!(
        accept.supported_feature_level,
        vtop_meta::SUPPORTED_FEATURE_LEVEL
    );

    // This build's own leader negotiates the handshake protocol.
    let leader = leader_of(&runtime, follower, ClusterFeatureLevel::unknown());
    assert!(until(Duration::from_secs(5), || leader
        .follower_protocol(FOLLOWER)
        == Some(REPLICA_PROTOCOL_HANDSHAKE)));
    assert_eq!(
        leader.replicate_append_batch(&[append(0)], 1).follower_acks,
        1
    );

    // Finalized past what a dialer implements: the server refuses it.
    level.observe(FINALIZED_PAST_OLD_PEERS);
    match runtime.block_on(hello_from(
        REPLICA_PROTOCOL_MAX,
        FINALIZED_PAST_OLD_PEERS - 1,
    )) {
        Message::Error(ErrorResponse {
            code: ErrorCode::UnsupportedVersion,
            message,
            ..
        }) => assert!(
            message.contains(&format!("implements only {}", FINALIZED_PAST_OLD_PEERS - 1)),
            "{message}"
        ),
        other => panic!("a dialer below the finalized level must be refused: {other:?}"),
    }
    assert!(ReplicaHelloAccept::from_message(
        &runtime.block_on(hello_from(REPLICA_PROTOCOL_MAX, FINALIZED_PAST_OLD_PEERS))
    )
    .is_some());

    // And a dialer that knows the level refuses a server that implements
    // less: this build's server reports what this build supports.
    let ahead = ClusterFeatureLevel::unknown();
    ahead.observe(vtop_meta::SUPPORTED_FEATURE_LEVEL + 1);
    let behind_server = runtime.block_on(serve_this_build(ClusterFeatureLevel::unknown()));
    let refused = runtime
        .block_on(status_client(ahead).status(behind_server, "localhost", FOLLOWER, &range()))
        .unwrap_err();
    assert!(
        matches!(&refused, BrokerError::ReplicaVersion(why) if why.contains("implements only")),
        "{refused}"
    );
}

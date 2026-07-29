//! Networked pipelined quorum replication (#186).
//!
//! Covers persistent mTLS leader→follower streams, quorum acks with a slow
//! non-quorum replica, reconnect catch-up from the retransmission buffer,
//! and fencing still rejecting stale epochs under group commit.

use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use uuid::Uuid;
use vtop_broker::group_commit::GroupCommitConfig;
use vtop_broker::memory_budget::{MemoryBudgetConfig, MemoryBudgetPool};
use vtop_broker::replication::{
    ClusterCommittedOffset, FlowControlConfig, InProcessFollower, NetworkFollowerConfig,
    NetworkedReplicaSet, ReplicaPeerHandler, ReplicaPeerServer, ReplicaSet, ReplicaTlsMaterial,
};
use vtop_broker::{LocalBroker, MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
use vtop_protocol::{
    CommittedHwmUpdate, Durability as WireDurability, ErrorCode, ErrorResponse, Message,
    ProduceRecord, ProduceRequest, ProduceResponse, RangeIdentity, ReplicaAppendRequest,
    ReplicaAppendResponse, ReplicaStatusResponse, Role, WireFrame,
};

const LEADER: Uuid = Uuid::from_u128(0xA1);
const FOLLOWER_1: Uuid = Uuid::from_u128(0xA2);
const FOLLOWER_2: Uuid = Uuid::from_u128(0xA3);
const PRODUCER: Uuid = Uuid::from_u128(0xB1);
const FENCING_EPOCH: u64 = 18;

struct CertBundle {
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
}

fn cert_for_cn(cn: &str) -> CertBundle {
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    params.distinguished_name = DistinguishedName::new();
    params.distinguished_name.push(DnType::CommonName, cn);
    let cert = params.self_signed(&key).unwrap();
    CertBundle {
        cert: cert.der().clone(),
        key: PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
    }
}

fn clone_private_key(key: &PrivateKeyDer<'static>) -> PrivateKeyDer<'static> {
    match key {
        PrivateKeyDer::Pkcs8(key) => {
            PrivatePkcs8KeyDer::from(key.secret_pkcs8_der().to_vec()).into()
        }
        PrivateKeyDer::Pkcs1(key) => {
            rustls::pki_types::PrivatePkcs1KeyDer::from(key.secret_pkcs1_der().to_vec()).into()
        }
        PrivateKeyDer::Sec1(key) => {
            rustls::pki_types::PrivateSec1KeyDer::from(key.secret_sec1_der().to_vec()).into()
        }
        _ => panic!("unsupported private key type"),
    }
}

fn material(identity: &CertBundle, peer_trust: &[&CertBundle]) -> ReplicaTlsMaterial {
    let mut trust_roots = rustls::RootCertStore::empty();
    for peer in peer_trust {
        trust_roots.add(peer.cert.clone()).unwrap();
    }
    ReplicaTlsMaterial {
        certificate_chain: vec![identity.cert.clone()],
        private_key: clone_private_key(&identity.key),
        trust_roots,
    }
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

struct SlowFollower {
    inner: Arc<InProcessFollower>,
    delay: Duration,
    hold: Arc<AtomicBool>,
}

impl ReplicaPeerHandler for SlowFollower {
    fn node_id(&self) -> Uuid {
        self.inner.node_id()
    }

    fn apply_append(
        &self,
        request: &ReplicaAppendRequest,
    ) -> Result<ReplicaAppendResponse, (ErrorCode, String)> {
        self.block();
        self.inner.apply_append(request)
    }

    fn apply_append_batch(
        &self,
        requests: &[ReplicaAppendRequest],
    ) -> Result<ReplicaAppendResponse, (ErrorCode, String)> {
        self.block();
        self.inner.apply_append_batch(requests)
    }

    fn observe_hwm(&self, update: &CommittedHwmUpdate) -> Result<(), (ErrorCode, String)> {
        self.inner.observe_hwm(update)
    }

    fn status(&self, range: &RangeIdentity) -> Result<ReplicaStatusResponse, (ErrorCode, String)> {
        self.inner.status(range)
    }
}

impl SlowFollower {
    fn block(&self) {
        if self.hold.load(Ordering::SeqCst) {
            // Apply runs on the replica peer server's Tokio worker. A plain
            // thread::sleep would pin that worker and can starve the fast
            // follower on small test runtimes (CI).
            tokio::task::block_in_place(|| std::thread::sleep(self.delay));
        }
    }
}

struct NetworkHarness {
    _dirs: Vec<TempDir>,
    _runtime: Runtime,
    /// Keep peer accept loops alive for the harness lifetime.
    _server_abort: Vec<tokio::task::AbortHandle>,
    range: RangeIdentity,
    meta: MetaFencingEpoch,
    leader: Arc<LocalBroker>,
    followers: Vec<Arc<InProcessFollower>>,
    replica_set: Arc<NetworkedReplicaSet>,
    cluster_committed: ClusterCommittedOffset,
}

fn spawn_follower_server(
    runtime: &Runtime,
    material: ReplicaTlsMaterial,
    node_id: Uuid,
    handler: Arc<dyn ReplicaPeerHandler>,
) -> (SocketAddr, tokio::task::AbortHandle) {
    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = ReplicaPeerServer::new(material, node_id, handler).unwrap();
        let handle = tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        (addr, handle.abort_handle())
    })
}

fn harness_with(
    flow: FlowControlConfig,
    group_commit: Option<GroupCommitConfig>,
    slow_follower2: Option<(Duration, Arc<AtomicBool>)>,
    memory: Option<Arc<MemoryBudgetPool>>,
) -> NetworkHarness {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let range = range_identity();
    let meta = MetaFencingEpoch::new(FENCING_EPOCH);
    let cluster_committed = ClusterCommittedOffset::new(0);

    let leader_cert = cert_for_cn(&LEADER.to_string());
    let f1_cert = cert_for_cn(&FOLLOWER_1.to_string());
    let f2_cert = cert_for_cn(&FOLLOWER_2.to_string());

    let leader_dir = tempfile::tempdir().unwrap();
    let leader_segment = open_segment(&leader_dir, 0xD1, &range);
    let leader_epochs = ProducerEpochJournal::open(leader_dir.path().join("epochs")).unwrap();

    let mut dirs = vec![leader_dir];
    let mut followers = Vec::new();
    let mut server_abort = Vec::new();
    let mut follower_configs = Vec::new();

    for (index, (node_id, cert)) in [(FOLLOWER_1, &f1_cert), (FOLLOWER_2, &f2_cert)]
        .into_iter()
        .enumerate()
    {
        let dir = tempfile::tempdir().unwrap();
        let segment = open_segment(&dir, 0xE1 + index as u128, &range);
        let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
        let follower = Arc::new(
            InProcessFollower::new(
                node_id,
                segment,
                epochs,
                range.clone(),
                FENCING_EPOCH,
                meta.clone(),
                ClusterCommittedOffset::new(0),
            )
            .unwrap(),
        );
        let handler: Arc<dyn ReplicaPeerHandler> = if index == 1 {
            if let Some((delay, hold)) = slow_follower2.clone() {
                Arc::new(SlowFollower {
                    inner: Arc::clone(&follower),
                    delay,
                    hold,
                })
            } else {
                Arc::clone(&follower) as Arc<dyn ReplicaPeerHandler>
            }
        } else {
            Arc::clone(&follower) as Arc<dyn ReplicaPeerHandler>
        };
        let server_material = material(cert, &[&leader_cert]);
        let (addr, abort) = spawn_follower_server(&runtime, server_material, node_id, handler);
        follower_configs.push(NetworkFollowerConfig {
            node_id,
            addr,
            server_name: "localhost".to_owned(),
        });
        server_abort.push(abort);
        followers.push(follower);
        dirs.push(dir);
    }

    let leader_tls = material(&leader_cert, &[&f1_cert, &f2_cert]);
    let replica_set = Arc::new(
        NetworkedReplicaSet::start_on_handle_with_memory(
            runtime.handle().clone(),
            follower_configs,
            leader_tls,
            flow,
            memory,
        )
        .unwrap(),
    );

    // Wait until both follower streams are live before producing.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let ready = [FOLLOWER_1, FOLLOWER_2]
            .iter()
            .all(|node| replica_set.follower_connected(*node) == Some(true));
        if ready {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    let mut leader = LocalBroker::with_replication(
        leader_segment,
        leader_epochs,
        range.clone(),
        FENCING_EPOCH,
        meta.clone(),
        LEADER,
        Some(cluster_committed.clone()),
        Some(replica_set.clone() as Arc<dyn ReplicaSet>),
    )
    .unwrap();
    if let Some(config) = group_commit {
        leader = leader.with_group_commit(config).unwrap();
    }

    NetworkHarness {
        _dirs: dirs,
        _runtime: runtime,
        _server_abort: server_abort,
        range,
        meta,
        leader: Arc::new(leader),
        followers,
        replica_set,
        cluster_committed,
    }
}

fn harness() -> NetworkHarness {
    harness_with(FlowControlConfig::default(), None, None, None)
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
fn networked_quorum_acks_and_propagates_hwm() {
    let h = harness();
    let first = produce_ok(&h.leader, h.range.clone(), 0);
    assert_eq!(first.outcomes[0].offset, 0);
    assert_eq!(first.committed_next_offset, 1);
    assert_eq!(h.cluster_committed.get(), 1);

    // Give HWM propagation a moment on the async streams.
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(h.followers[0].cluster_committed().get(), 1);
    assert_eq!(h.followers[1].cluster_committed().get(), 1);
    assert_eq!(h.replica_set.follower_durable_offset(FOLLOWER_1), Some(1));
    assert_eq!(h.replica_set.follower_durable_offset(FOLLOWER_2), Some(1));
}

#[test]
fn slow_non_quorum_follower_does_not_block_producer() {
    let hold = Arc::new(AtomicBool::new(true));
    let flow = FlowControlConfig {
        // Generous enough for CI TLS/setup jitter on the fast follower, but
        // far below the slow follower's artificial delay.
        ack_timeout: Duration::from_millis(750),
        ..FlowControlConfig::default()
    };
    let h = harness_with(
        flow,
        None,
        Some((Duration::from_secs(5), Arc::clone(&hold))),
        None,
    );

    let started = std::time::Instant::now();
    let first = produce_ok(&h.leader, h.range.clone(), 0);
    let elapsed = started.elapsed();
    assert_eq!(first.committed_next_offset, 1);
    // Quorum is leader + follower1; follower2 is held. Must not wait for the
    // full artificial delay.
    assert!(
        elapsed < Duration::from_secs(3),
        "slow follower blocked produce for {elapsed:?}"
    );
    hold.store(false, Ordering::SeqCst);
}

#[test]
fn quorum_loss_stops_durable_acknowledgements() {
    let h = harness();
    let _ = produce_ok(&h.leader, h.range.clone(), 0);

    h.followers[0].set_online(false);
    h.followers[1].set_online(false);

    let response = h.leader.handle(
        Role::Producer,
        produce_frame(h.range.clone(), 1, 2, WireDurability::Quorum),
    );
    match response.message {
        Message::Error(ErrorResponse {
            code: ErrorCode::Overloaded,
            ..
        }) => {}
        other => panic!("expected Overloaded on quorum loss, got {other:?}"),
    }
    assert_eq!(h.cluster_committed.get(), 1);
}

#[test]
fn reconnect_catch_up_from_retransmission_buffer() {
    let flow = FlowControlConfig {
        reconnect_backoff: Duration::from_millis(20),
        ack_timeout: Duration::from_millis(500),
        ..FlowControlConfig::default()
    };
    let h = harness_with(flow, None, None, None);
    let _ = produce_ok(&h.leader, h.range.clone(), 0);
    assert_eq!(h.cluster_committed.get(), 1);

    assert!(h.replica_set.force_reconnect(FOLLOWER_1));

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline
        && h.replica_set.follower_connected(FOLLOWER_1) != Some(false)
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    while std::time::Instant::now() < deadline
        && h.replica_set.follower_connected(FOLLOWER_1) != Some(true)
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(h.replica_set.follower_connected(FOLLOWER_1), Some(true));
    std::thread::sleep(Duration::from_millis(50));

    let second = produce_ok(&h.leader, h.range.clone(), 1);
    assert_eq!(second.committed_next_offset, 2);

    // Follower1 may ack after the quorum wait returns (reconnect/catch-up).
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline && h.followers[0].local_committed_offset() < 2 {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        h.followers[0].local_committed_offset(),
        2,
        "follower1 should catch up after reconnect"
    );
}

#[test]
fn fencing_still_works_with_networked_group_commit() {
    let h = harness_with(
        FlowControlConfig::default(),
        Some(GroupCommitConfig {
            max_delay: Duration::from_millis(20),
            max_records: 8,
            max_bytes: 64 * 1024,
            max_pending_requests: 8,
        }),
        None,
        None,
    );

    let _ = produce_ok(&h.leader, h.range.clone(), 0);
    h.meta.set(FENCING_EPOCH + 1);

    let response = h.leader.handle(
        Role::Producer,
        produce_frame(h.range.clone(), 1, 2, WireDurability::Quorum),
    );
    match response.message {
        Message::Error(ErrorResponse {
            code: ErrorCode::Fenced,
            ..
        }) => {}
        other => panic!("expected Fenced after lease steal, got {other:?}"),
    }
    assert_eq!(h.cluster_committed.get(), 1);
}

/// Wait for a follower stream to drop and come back after `force_reconnect`.
fn wait_reconnect(replica_set: &NetworkedReplicaSet, node_id: Uuid) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline
        && replica_set.follower_connected(node_id) != Some(false)
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    while std::time::Instant::now() < deadline
        && replica_set.follower_connected(node_id) != Some(true)
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(replica_set.follower_connected(node_id), Some(true));
}

#[test]
fn catch_up_charges_return_to_zero_after_replica_set_shutdown() {
    let pool = MemoryBudgetPool::new(MemoryBudgetConfig::default()).unwrap();
    let h = harness_with(
        FlowControlConfig::default(),
        None,
        None,
        Some(Arc::clone(&pool)),
    );
    let _ = produce_ok(&h.leader, h.range.clone(), 0);

    // Fan-out charges each follower's retransmission buffer; the charges stay
    // until a reconnect drains them.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while pool.metrics().replica_used_bytes() == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        pool.metrics().replica_used_bytes() > 0,
        "retransmission buffers must charge catch-up bytes"
    );

    // Tearing the replica set down shuts the follower drivers down with
    // undrained buffers; their budgets must return the outstanding catch-up
    // charges to the shared pool instead of leaking them.
    drop(h.leader);
    drop(h.replica_set);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while pool.metrics().replica_used_bytes() > 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(pool.metrics().replica_used_bytes(), 0);
    assert_eq!(pool.metrics().process_used_bytes(), 0);
}

#[test]
fn reconnect_catch_up_charges_each_byte_once() {
    let pool = MemoryBudgetPool::new(MemoryBudgetConfig::default()).unwrap();
    let hold = Arc::new(AtomicBool::new(true));
    let flow = FlowControlConfig {
        reconnect_backoff: Duration::from_millis(20),
        ack_timeout: Duration::from_millis(500),
        ..FlowControlConfig::default()
    };
    let h = harness_with(
        flow,
        None,
        Some((Duration::from_secs(5), Arc::clone(&hold))),
        Some(Arc::clone(&pool)),
    );
    let _ = produce_ok(&h.leader, h.range.clone(), 0);

    // Follower1 committed the batch, so reconnecting it drains its covered
    // buffer entry and releases that charge.
    assert!(h.replica_set.force_reconnect(FOLLOWER_1));
    wait_reconnect(&h.replica_set, FOLLOWER_1);

    // What remains charged is follower2's single buffered batch: its apply is
    // held, so it has not committed and the entry is not covered.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while pool.metrics().replica_used_bytes() == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let baseline = pool.metrics().replica_used_bytes();
    assert!(baseline > 0, "follower2 must hold one buffered batch");

    // Reconnect follower2: the un-covered batch is re-queued and re-sent. It
    // must be charged exactly once — the same total as before the reconnect,
    // not once per buffered copy.
    assert!(h.replica_set.force_reconnect(FOLLOWER_2));
    wait_reconnect(&h.replica_set, FOLLOWER_2);
    // Give the driver room to complete the catch-up re-send.
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        pool.metrics().replica_used_bytes(),
        baseline,
        "reconnect catch-up must not charge the same bytes twice"
    );
    hold.store(false, Ordering::SeqCst);
}

//! Shared by the candidate-gateway tests (#751): a small cluster of gateways
//! over bridge slots, and a hand-rolled wire client for the exact bytes a test
//! asserts on. The failover test with a stock client uses the cluster and not
//! the wire client.
//!
//! The cluster models what the metadata plane and the replicas give a node,
//! and nothing more: one lease record (holder and fencing epoch), and one
//! shared log that every leadership appends to. A leadership's bridge appends
//! only while the lease record still names its node at its epoch, and the check
//! and the append happen under one lock, as a replica's fence and append do: an
//! append is acknowledged before a promotion's epoch bump, or refused after it,
//! never both.
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream as StdStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use vtop_kafka::{
    Appended, Bridge, BridgeSlot, Decoder, Encoder, ErrorCode, Fetched, Gateway, GatewayConfig,
    GatewayHandle, HolderView, KafkaBroker, LeaseState, LeaseView, MemoryBridge, RangeHolder,
    RecordBatch, Refusals, Sequenced,
};

pub const TOPIC: &str = "events";

/// The lease record the metadata plane would hold.
#[derive(Debug, Clone, Copy, Default)]
pub struct LeaseRecord {
    /// The node index that holds the range, `None` before the first promotion.
    pub holder: Option<usize>,
    pub epoch: u64,
}

/// The range's state shared by every node: the lease record and the log.
pub struct Shared {
    pub lease: Mutex<LeaseRecord>,
    pub log: Arc<MemoryBridge>,
    /// Records appended under each epoch, for a test to prove a client reached
    /// both leaderships.
    pub appended_by_epoch: Mutex<std::collections::BTreeMap<u64, u64>>,
}

impl Shared {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            lease: Mutex::new(LeaseRecord::default()),
            log: Arc::new(MemoryBridge::with_topics([TOPIC])),
            appended_by_epoch: Mutex::new(Default::default()),
        })
    }

    /// The metadata plane grants the range to `node` at the next epoch. Taking
    /// the lock waits for any append a deposed leader has under way, which is
    /// the fence: those are acknowledged, everything after is refused.
    pub fn grant(&self, node: usize) -> u64 {
        let mut lease = self.lease.lock().unwrap();
        lease.epoch += 1;
        lease.holder = Some(node);
        lease.epoch
    }
}

/// A leadership's bridge: the shared log, behind the fence.
pub struct Leadership {
    pub shared: Arc<Shared>,
    pub node: usize,
    pub epoch: u64,
    /// Calls the gateway made on this bridge, refused or not.
    pub calls: std::sync::atomic::AtomicU64,
}

impl Leadership {
    fn fenced<T>(&self, call: impl FnOnce() -> Result<T, ErrorCode>) -> Result<T, ErrorCode> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let lease = self.shared.lease.lock().unwrap();
        if lease.holder != Some(self.node) || lease.epoch != self.epoch {
            return Err(ErrorCode::NotLeaderOrFollower);
        }
        call()
    }
}

impl Bridge for Leadership {
    fn topics(&self) -> Vec<String> {
        vec![TOPIC.to_owned()]
    }

    fn produce(
        &self,
        topic: &str,
        batches: &[RecordBatch],
        sequenced: Option<Sequenced>,
    ) -> Result<Appended, ErrorCode> {
        self.fenced(|| {
            let before = self.shared.log.bounds(topic)?.1;
            let appended = self.shared.log.produce(topic, batches, sequenced)?;
            let after = self.shared.log.bounds(topic)?.1;
            *self
                .shared
                .appended_by_epoch
                .lock()
                .unwrap()
                .entry(self.epoch)
                .or_default() += u64::try_from(after - before).unwrap_or(0);
            Ok(appended)
        })
    }

    fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode> {
        self.fenced(|| self.shared.log.fetch(topic, offset, max_bytes))
    }

    fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
        self.fenced(|| self.shared.log.bounds(topic))
    }
}

/// One node's view of the lease record. `freeze` stops it learning, which is a
/// node that has been paused or partitioned from the metadata plane: it keeps
/// believing what it last read.
pub struct NodeView {
    pub shared: Arc<Shared>,
    pub node: usize,
    frozen: Mutex<Option<LeaseRecord>>,
}

impl NodeView {
    pub fn freeze(&self) {
        *self.frozen.lock().unwrap() = Some(*self.shared.lease.lock().unwrap());
    }

    fn record(&self) -> LeaseRecord {
        (*self.frozen.lock().unwrap()).unwrap_or_else(|| *self.shared.lease.lock().unwrap())
    }
}

impl LeaseView for NodeView {
    fn lease(&self) -> LeaseState {
        let record = self.record();
        if record.holder == Some(self.node) {
            LeaseState::Held(record.epoch)
        } else {
            LeaseState::Gone
        }
    }
}

impl HolderView for NodeView {
    fn holder(&self) -> Option<RangeHolder> {
        let record = self.record();
        record.holder.map(|holder| RangeHolder {
            broker_id: broker_id(holder),
            epoch: record.epoch,
        })
    }
}

/// The Kafka broker id of node index `node`.
pub fn broker_id(node: usize) -> i32 {
    i32::try_from(node).unwrap() + 1
}

pub struct Node {
    pub index: usize,
    pub addr: SocketAddr,
    pub slot: Arc<BridgeSlot>,
    pub view: Arc<NodeView>,
    pub handle: GatewayHandle,
    pub refusals: Arc<Refusals>,
    /// Dropped to kill the node: every task and socket goes with it, in-flight
    /// requests included, as `kill -9` does. `None` once killed.
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
    /// The bridge a promotion installed, kept as the node's own handle on it.
    pub leadership: Mutex<Option<Arc<Leadership>>>,
}

pub struct Cluster {
    pub shared: Arc<Shared>,
    pub nodes: Vec<Node>,
}

impl Cluster {
    /// `count` nodes, every one a candidate that lists all of them as brokers.
    /// Nothing holds the range until [`Cluster::promote`].
    pub fn start(count: usize) -> Self {
        let shared = Shared::new();
        let listeners: Vec<std::net::TcpListener> = (0..count)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
        let brokers: Vec<KafkaBroker> = addrs
            .iter()
            .enumerate()
            .map(|(index, addr)| KafkaBroker {
                node_id: broker_id(index),
                host: addr.ip().to_string(),
                port: i32::from(addr.port()),
            })
            .collect();
        let nodes = listeners
            .into_iter()
            .enumerate()
            .map(|(index, listener)| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .unwrap();
                let slot = Arc::new(BridgeSlot::new([TOPIC]));
                let view = Arc::new(NodeView {
                    shared: Arc::clone(&shared),
                    node: index,
                    frozen: Mutex::new(None),
                });
                let addr = addrs[index];
                let gateway = Gateway::new(
                    Arc::clone(&slot) as Arc<dyn Bridge>,
                    GatewayConfig {
                        advertised_host: addr.ip().to_string(),
                        advertised_port: i32::from(addr.port()),
                        node_id: broker_id(index),
                        brokers: brokers.clone(),
                        max_fetch_wait: Duration::from_millis(500),
                        fetch_poll_interval: Duration::from_millis(10),
                        max_produce_wait: Duration::from_secs(5),
                        drain_timeout: Duration::from_secs(1),
                        ..GatewayConfig::default()
                    },
                )
                .with_lease(Arc::clone(&view) as Arc<dyn LeaseView>)
                .with_holder(Arc::clone(&view) as Arc<dyn HolderView>);
                let handle = gateway.handle();
                let refusals = gateway.refusals();
                listener.set_nonblocking(true).unwrap();
                let (shutdown, rx) = tokio::sync::watch::channel(false);
                runtime.spawn(async move {
                    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                    let _keep = shutdown;
                    let _ = gateway.serve(listener, rx).await;
                });
                Node {
                    index,
                    addr,
                    slot,
                    view,
                    handle,
                    refusals,
                    runtime: Mutex::new(Some(runtime)),
                    leadership: Mutex::new(None),
                }
            })
            .collect();
        Self { shared, nodes }
    }

    pub fn bootstrap(&self) -> String {
        self.nodes
            .iter()
            .map(|node| node.addr.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    /// The metadata plane grants the range to `node`, and the node, which has
    /// completed its promotion, installs its bridge: the order a node's
    /// promotion runs in.
    pub fn promote(&self, node: usize) -> u64 {
        let epoch = self.shared.grant(node);
        let leadership = Arc::new(Leadership {
            shared: Arc::clone(&self.shared),
            node,
            epoch,
            calls: std::sync::atomic::AtomicU64::new(0),
        });
        let node = &self.nodes[node];
        *node.leadership.lock().unwrap() = Some(Arc::clone(&leadership));
        node.slot.install(leadership as Arc<dyn Bridge>);
        epoch
    }

    /// How many calls the node's gateway has made on its bridge.
    pub fn bridge_calls(&self, node: usize) -> u64 {
        self.nodes[node]
            .leadership
            .lock()
            .unwrap()
            .as_ref()
            .map_or(0, |l| l.calls.load(std::sync::atomic::Ordering::SeqCst))
    }

    /// `kill -9` of a node: its sockets close mid-request and nothing is
    /// drained. The lease record still names it until the lease would expire.
    pub fn kill(&self, node: usize) {
        if let Some(runtime) = self.nodes[node].runtime.lock().unwrap().take() {
            runtime.shutdown_background();
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for node in &self.nodes {
            if let Some(runtime) = node.runtime.lock().unwrap().take() {
                runtime.shutdown_background();
            }
        }
    }
}

// ------------------------------------------------------------------ wire client

/// What a Metadata v8 answer said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataAnswer {
    pub brokers: Vec<(i32, String, i32)>,
    pub controller: i32,
    pub topic_error: i16,
    /// `(error, index, leader, leader_epoch)`.
    pub partitions: Vec<(i16, i32, i32, i32)>,
}

pub fn call(addr: SocketAddr, key: i16, version: i16, body: &[u8]) -> Vec<u8> {
    let mut socket = StdStream::connect(addr).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut e = Encoder::new();
    e.i16(key);
    e.i16(version);
    e.i32(7);
    e.nullable_string(Some("candidate-test"));
    e.raw(body);
    let mut framed = (e.as_slice().len() as i32).to_be_bytes().to_vec();
    framed.extend_from_slice(e.as_slice());
    socket.write_all(&framed).unwrap();
    let mut len = [0_u8; 4];
    socket.read_exact(&mut len).unwrap();
    let mut reply = vec![0_u8; i32::from_be_bytes(len) as usize];
    socket.read_exact(&mut reply).unwrap();
    assert_eq!(i32::from_be_bytes(reply[..4].try_into().unwrap()), 7);
    reply[4..].to_vec()
}

pub fn metadata(addr: SocketAddr, topic: &str) -> MetadataAnswer {
    let mut e = Encoder::new();
    e.array_len(1);
    e.string(topic);
    e.bool(false); // allow_auto_topic_creation
    e.bool(false); // include_cluster_authorized_operations
    e.bool(false); // include_topic_authorized_operations
    let reply = call(addr, 3, 8, e.as_slice());
    let mut d = Decoder::new(&reply);
    d.i32("throttle").unwrap();
    let mut brokers = Vec::new();
    for _ in 0..d.array_len("brokers").unwrap().unwrap() {
        let id = d.i32("node").unwrap();
        let host = d.string("host").unwrap().to_owned();
        let port = d.i32("port").unwrap();
        d.nullable_string("rack").unwrap();
        brokers.push((id, host, port));
    }
    d.nullable_string("cluster").unwrap();
    let controller = d.i32("controller").unwrap();
    assert_eq!(d.array_len("topics").unwrap(), Some(1));
    let topic_error = d.i16("topic error").unwrap();
    d.string("name").unwrap();
    d.bool("internal").unwrap();
    let mut partitions = Vec::new();
    for _ in 0..d.array_len("partitions").unwrap().unwrap() {
        let error = d.i16("error").unwrap();
        let index = d.i32("index").unwrap();
        let leader = d.i32("leader").unwrap();
        let epoch = d.i32("epoch").unwrap();
        for _ in 0..2 {
            for _ in 0..d.array_len("replicas").unwrap().unwrap() {
                d.i32("replica").unwrap();
            }
        }
        d.array_len("offline").unwrap();
        partitions.push((error, index, leader, epoch));
    }
    MetadataAnswer {
        brokers,
        controller,
        topic_error,
        partitions,
    }
}

pub fn records(values: &[&str]) -> Vec<u8> {
    let records: Vec<vtop_kafka::Record> = values
        .iter()
        .enumerate()
        .map(|(i, v)| vtop_kafka::Record {
            offset: i as i64,
            timestamp_millis: 1_700_000_000_000 + i as i64,
            key: None,
            value: Some(v.as_bytes().to_vec()),
            headers: Vec::new(),
        })
        .collect();
    RecordBatch::encode(0, -1, -1, -1, &records)
}

/// Produce v3 of one set to partition 0: `(error, base offset)`.
pub fn produce(addr: SocketAddr, topic: &str, values: &[&str]) -> (i16, i64) {
    let mut e = Encoder::new();
    e.nullable_string(None);
    e.i16(-1);
    e.i32(2_000);
    e.array_len(1);
    e.string(topic);
    e.array_len(1);
    e.i32(0);
    e.nullable_bytes(Some(&records(values)));
    let reply = call(addr, 0, 3, e.as_slice());
    let mut d = Decoder::new(&reply);
    d.array_len("topics").unwrap();
    d.string("topic").unwrap();
    d.array_len("partitions").unwrap();
    d.i32("index").unwrap();
    let error = d.i16("error").unwrap();
    let base = d.i64("base").unwrap();
    (error, base)
}

/// Fetch v9 of partition 0 from `offset`, naming `epoch` as the leader epoch
/// the client last saw: `(error, record bytes)`.
pub fn fetch(addr: SocketAddr, topic: &str, offset: i64, epoch: i32) -> (i16, Vec<u8>) {
    let mut e = Encoder::new();
    e.i32(-1);
    e.i32(0);
    e.i32(1);
    e.i32(1 << 20);
    e.i8(0);
    e.i32(0);
    e.i32(-1);
    e.array_len(1);
    e.string(topic);
    e.array_len(1);
    e.i32(0);
    e.i32(epoch);
    e.i64(offset);
    e.i64(0);
    e.i32(1 << 20);
    e.array_len(0);
    let reply = call(addr, 1, 9, e.as_slice());
    let mut d = Decoder::new(&reply);
    d.i32("throttle").unwrap();
    d.i16("error").unwrap();
    d.i32("session").unwrap();
    d.array_len("topics").unwrap();
    d.string("topic").unwrap();
    d.array_len("partitions").unwrap();
    d.i32("index").unwrap();
    let error = d.i16("error").unwrap();
    d.i64("hwm").unwrap();
    d.i64("lso").unwrap();
    d.i64("log_start").unwrap();
    d.array_len("aborted").unwrap();
    let records = d.nullable_bytes("records").unwrap().unwrap_or(&[]).to_vec();
    (error, records)
}

/// ListOffsets v4 LATEST of partition 0, naming `epoch`: `(error, offset)`.
pub fn list_latest(addr: SocketAddr, topic: &str, epoch: i32) -> (i16, i64) {
    let mut e = Encoder::new();
    e.i32(-1);
    e.i8(0);
    e.array_len(1);
    e.string(topic);
    e.array_len(1);
    e.i32(0);
    e.i32(epoch);
    e.i64(-1);
    let reply = call(addr, 2, 4, e.as_slice());
    let mut d = Decoder::new(&reply);
    d.i32("throttle").unwrap();
    d.array_len("topics").unwrap();
    d.string("topic").unwrap();
    d.array_len("partitions").unwrap();
    d.i32("index").unwrap();
    let error = d.i16("error").unwrap();
    d.i64("timestamp").unwrap();
    let offset = d.i64("offset").unwrap();
    (error, offset)
}

/// FindCoordinator v1 for `group`: `(error, node id, host, port)`.
pub fn find_coordinator(addr: SocketAddr, group: &str) -> (i16, i32, String, i32) {
    let mut e = Encoder::new();
    e.string(group);
    e.i8(0); // key_type: a group
    let reply = call(addr, 10, 1, e.as_slice());
    let mut d = Decoder::new(&reply);
    d.i32("throttle").unwrap();
    let error = d.i16("error").unwrap();
    d.nullable_string("message").unwrap();
    let node = d.i32("node").unwrap();
    let host = d.string("host").unwrap().to_owned();
    let port = d.i32("port").unwrap();
    (error, node, host, port)
}

/// Every record value in `bytes`, in order.
pub fn values(mut bytes: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    while !bytes.is_empty() {
        let len = i32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize + 12;
        let batch = RecordBatch::decode(&bytes[..len]).unwrap();
        out.extend(
            batch
                .records
                .into_iter()
                .map(|r| String::from_utf8(r.value.unwrap()).unwrap()),
        );
        bytes = &bytes[len..];
    }
    out
}

/// Every value in the shared log from offset 0, in offset order.
pub fn log_values(shared: &Shared) -> Vec<String> {
    let mut out = Vec::new();
    let mut offset = 0;
    loop {
        let fetched = shared.log.fetch(TOPIC, offset, 1 << 20).unwrap();
        if fetched.records.is_empty() {
            return out;
        }
        let batch_values = values(&fetched.records);
        offset += i64::try_from(batch_values.len()).unwrap();
        out.extend(batch_values);
    }
}

//! A gateway on a candidate node (#751): one listener, bound once, over a
//! bridge slot that is empty while the node follows. Every test here is about
//! what the gateway says and refuses at the edges of a leadership, with the
//! exact bytes a client reads.

mod common;

use common::*;
use std::sync::Arc;
use std::time::Duration;
use vtop_kafka::{
    Appended, Bridge, BridgeSlot, ErrorCode, Fetched, Gateway, GatewayConfig, MemoryBridge,
    RecordBatch, Refusal, Sequenced, TopicBinding, TopicMap,
};

const NOT_LEADER: i16 = 6;
const LEADER_NOT_AVAILABLE: i16 = 5;
const FENCED_LEADER_EPOCH: i16 = 74;
const UNKNOWN_LEADER_EPOCH: i16 = 75;

fn all_brokers(cluster: &Cluster) -> Vec<(i32, String, i32)> {
    cluster
        .nodes
        .iter()
        .map(|node| {
            (
                broker_id(node.index),
                node.addr.ip().to_string(),
                i32::from(node.addr.port()),
            )
        })
        .collect()
}

#[test]
fn a_follower_lists_every_broker_and_names_the_holder_as_the_leader_at_its_epoch() {
    let cluster = Cluster::start(3);
    let epoch = cluster.promote(0);
    assert_eq!(epoch, 1);

    // Asked of a node that does not hold the range.
    let answer = metadata(cluster.nodes[2].addr, TOPIC);
    assert_eq!(answer.brokers, all_brokers(&cluster));
    assert_eq!(answer.topic_error, 0, "the topic exists whoever leads it");
    assert_eq!(answer.partitions, vec![(0, 0, broker_id(0), 1)]);

    // Asked of the holder: the same answer, which is the point. A client
    // cannot tell the two apart, and does not need to.
    assert_eq!(
        metadata(cluster.nodes[0].addr, TOPIC).partitions,
        answer.partitions
    );

    // The range moves. The old holder has not been told to stop listening,
    // and its own lease says it no longer holds the range: it names the new
    // holder, at the new epoch, from its own address.
    let epoch = cluster.promote(1);
    assert_eq!(epoch, 2);
    for node in [0, 1, 2] {
        assert_eq!(
            metadata(cluster.nodes[node].addr, TOPIC).partitions,
            vec![(0, 0, broker_id(1), 2)],
            "node {node}"
        );
    }
}

#[test]
fn with_no_holder_known_metadata_says_so_by_code_and_nothing_serves() {
    let cluster = Cluster::start(3);
    let answer = metadata(cluster.nodes[1].addr, TOPIC);
    assert_eq!(
        answer.brokers,
        all_brokers(&cluster),
        "a client has somewhere to go"
    );
    assert_eq!(answer.topic_error, 0);
    assert_eq!(
        answer.partitions,
        vec![(LEADER_NOT_AVAILABLE, 0, -1, -1)],
        "no leader, and no epoch a client would compare"
    );
    // Never the empty success a client would read as an empty topic.
    assert_eq!(produce(cluster.nodes[1].addr, TOPIC, &["a"]).0, NOT_LEADER);
    assert_eq!(fetch(cluster.nodes[1].addr, TOPIC, 0, -1).0, NOT_LEADER);
    assert_eq!(list_latest(cluster.nodes[1].addr, TOPIC, -1).0, NOT_LEADER);
    assert_eq!(cluster.nodes[1].refusals.get(Refusal::ProduceNotLeader), 1);
    assert_eq!(cluster.nodes[1].refusals.get(Refusal::FetchNotLeader), 1);
    assert_eq!(
        cluster.nodes[1].refusals.get(Refusal::ListOffsetsNotLeader),
        1
    );
}

/// A consumer follows a failover only if every member tells it where its
/// group's coordinator went (#751). A member that does not hold the range
/// names the holder, from its own address; with no holder known it says the
/// coordinator is not available, which a client asks again about.
#[test]
fn a_follower_names_the_holder_as_the_groups_coordinator() {
    const COORDINATOR_NOT_AVAILABLE: i16 = 15;
    let cluster = Cluster::start(3);
    for node in [0, 1, 2] {
        assert_eq!(
            find_coordinator(cluster.nodes[node].addr, "g").0,
            COORDINATOR_NOT_AVAILABLE,
            "node {node}: nobody holds the range yet"
        );
    }
    let holder_at = |node: usize| {
        (
            0,
            broker_id(node),
            cluster.nodes[node].addr.ip().to_string(),
            i32::from(cluster.nodes[node].addr.port()),
        )
    };
    cluster.promote(0);
    for node in [0, 1, 2] {
        assert_eq!(
            find_coordinator(cluster.nodes[node].addr, "g"),
            holder_at(0),
            "node {node}"
        );
    }
    // The range moves: the old holder, still listening, names the new one.
    cluster.promote(2);
    for node in [0, 1, 2] {
        assert_eq!(
            find_coordinator(cluster.nodes[node].addr, "g"),
            holder_at(2),
            "node {node}"
        );
    }
}

#[test]
fn a_topic_the_slot_was_not_built_for_is_unknown_not_elsewhere() {
    let cluster = Cluster::start(3);
    cluster.promote(0);
    let answer = metadata(cluster.nodes[2].addr, "nope");
    assert_eq!(
        answer.topic_error, 3,
        "UNKNOWN_TOPIC_OR_PARTITION, on every node"
    );
    assert!(answer.partitions.is_empty());
}

#[test]
fn installing_a_bridge_makes_the_same_gateway_serve_at_the_same_address() {
    let cluster = Cluster::start(3);
    let addr = cluster.nodes[0].addr;
    assert_eq!(produce(addr, TOPIC, &["too early"]).0, NOT_LEADER);

    cluster.promote(0);
    let (error, base) = produce(addr, TOPIC, &["first", "second"]);
    assert_eq!((error, base), (0, 0));
    let (error, bytes) = fetch(addr, TOPIC, 0, 1);
    assert_eq!(error, 0);
    assert_eq!(values(&bytes), vec!["first", "second"]);
    assert_eq!(list_latest(addr, TOPIC, 1), (0, 2));
    assert_eq!(
        metadata(addr, TOPIC).partitions,
        vec![(0, 0, broker_id(0), 1)],
        "it names itself, at the epoch it holds"
    );
    assert_eq!(log_values(&cluster.shared), vec!["first", "second"]);
}

#[test]
fn a_deposed_leader_that_has_not_learned_it_is_refused_by_the_receiver() {
    let cluster = Cluster::start(3);
    cluster.promote(0);
    assert_eq!(produce(cluster.nodes[0].addr, TOPIC, &["acked"]).0, 0);

    // The old leader stalls: it keeps believing it holds the range at epoch 1.
    cluster.nodes[0].view.freeze();
    cluster.promote(1);

    // Its gateway still thinks it leads, so the gateway lets the append
    // through, and the log's fence is what stops it. Nothing is appended.
    let (error, _) = produce(cluster.nodes[0].addr, TOPIC, &["from the deposed leader"]);
    assert_eq!(
        error, NOT_LEADER,
        "refused by the fence, not by the gateway's belief"
    );
    assert_eq!(
        produce(cluster.nodes[1].addr, TOPIC, &["from the new leader"]).0,
        0
    );
    assert_eq!(
        log_values(&cluster.shared),
        vec!["acked", "from the new leader"],
        "the deposed leader appended nothing"
    );
}

#[test]
fn a_leader_whose_lease_is_gone_refuses_without_asking_its_bridge() {
    let cluster = Cluster::start(3);
    cluster.promote(0);
    assert_eq!(produce(cluster.nodes[0].addr, TOPIC, &["acked"]).0, 0);
    cluster.promote(1);

    // Node 0's bridge is still installed (nothing has demoted it yet), and its
    // lease view now says Gone. A read from it could miss what the new leader
    // has acknowledged.
    assert!(cluster.nodes[0].slot.is_installed());
    let asked = cluster.bridge_calls(0);
    assert_eq!(
        produce(cluster.nodes[0].addr, TOPIC, &["late"]).0,
        NOT_LEADER
    );
    assert_eq!(fetch(cluster.nodes[0].addr, TOPIC, 0, -1).0, NOT_LEADER);
    assert_eq!(list_latest(cluster.nodes[0].addr, TOPIC, -1).0, NOT_LEADER);
    assert_eq!(
        cluster.bridge_calls(0),
        asked,
        "a deposed leader's gateway asked its bridge instead of refusing"
    );
    assert_eq!(cluster.nodes[0].refusals.get(Refusal::ProduceNotLeader), 1);
    assert_eq!(log_values(&cluster.shared), vec!["acked"]);
}

#[test]
fn a_client_naming_a_leader_epoch_is_held_to_it() {
    let cluster = Cluster::start(3);
    cluster.promote(0);
    cluster.promote(1); // epoch 2
    cluster.promote(2); // epoch 3
    let addr = cluster.nodes[2].addr;

    // The epoch it holds, and no epoch at all, are served.
    assert_eq!(fetch(addr, TOPIC, 0, 3).0, 0);
    assert_eq!(fetch(addr, TOPIC, 0, -1).0, 0);
    // A client behind a failover is told its metadata is stale.
    assert_eq!(fetch(addr, TOPIC, 0, 2).0, FENCED_LEADER_EPOCH);
    assert_eq!(list_latest(addr, TOPIC, 2).0, FENCED_LEADER_EPOCH);
    // A client ahead of this node is told this node is the stale one.
    assert_eq!(fetch(addr, TOPIC, 0, 4).0, UNKNOWN_LEADER_EPOCH);
    assert_eq!(list_latest(addr, TOPIC, 4).0, UNKNOWN_LEADER_EPOCH);
    assert_eq!(list_latest(addr, TOPIC, 3).0, 0);
}

#[test]
fn a_client_that_knows_the_new_epoch_is_not_served_by_a_leader_that_has_not_learned_it() {
    let cluster = Cluster::start(3);
    cluster.promote(0);
    assert_eq!(produce(cluster.nodes[0].addr, TOPIC, &["acked"]).0, 0);
    cluster.nodes[0].view.freeze(); // still believes epoch 1
    cluster.promote(1);

    // The client read epoch 2 from a node that knows it, then reaches the
    // stale leader. Without the epoch, this fetch would be served from a
    // node that may miss acknowledged records.
    let (error, _) = fetch(cluster.nodes[0].addr, TOPIC, 0, 2);
    assert_eq!(error, UNKNOWN_LEADER_EPOCH);
    assert_eq!(
        list_latest(cluster.nodes[0].addr, TOPIC, 2).0,
        UNKNOWN_LEADER_EPOCH
    );
}

// ----------------------------------------------------------------- quiescing

/// A bridge whose produce parks until released, counting what it appended.
struct Gated {
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    appended: std::sync::atomic::AtomicUsize,
}

impl Bridge for Gated {
    fn topics(&self) -> Vec<String> {
        vec![TOPIC.to_owned()]
    }
    fn produce(
        &self,
        _topic: &str,
        _batches: &[RecordBatch],
        _sequenced: Option<Sequenced>,
    ) -> Result<Appended, ErrorCode> {
        self.entered.send(()).unwrap();
        self.release.lock().unwrap().recv().unwrap();
        self.appended
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Appended {
            base_offset: 0,
            log_append_time_ms: -1,
            log_start_offset: 0,
        })
    }
    fn fetch(&self, _: &str, _: i64, _: usize) -> Result<Fetched, ErrorCode> {
        Err(ErrorCode::NotLeaderOrFollower)
    }
    fn bounds(&self, _: &str) -> Result<(i64, i64), ErrorCode> {
        Ok((0, 0))
    }
}

struct GatedNode {
    runtime: Option<tokio::runtime::Runtime>,
    addr: std::net::SocketAddr,
    slot: Arc<BridgeSlot>,
    handle: vtop_kafka::GatewayHandle,
    old: Option<Arc<Gated>>,
    release: std::sync::mpsc::Sender<()>,
    entered: std::sync::mpsc::Receiver<()>,
}

impl GatedNode {
    fn rt(&self) -> &tokio::runtime::Runtime {
        self.runtime.as_ref().unwrap()
    }

    fn old(&self) -> &Arc<Gated> {
        self.old.as_ref().unwrap()
    }
}

/// A test that fails with a call parked in the gate must not hang the suite:
/// the gate is opened and the runtime abandoned, as a killed process is.
impl Drop for GatedNode {
    fn drop(&mut self) {
        let _ = self.release.send(());
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

fn gated_node(ceiling_each: Duration) -> GatedNode {
    let (entered_tx, entered) = std::sync::mpsc::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let old = Arc::new(Gated {
        entered: entered_tx,
        release: std::sync::Mutex::new(release_rx),
        appended: std::sync::atomic::AtomicUsize::new(0),
    });
    let slot = Arc::new(BridgeSlot::new([TOPIC]));
    slot.install(Arc::clone(&old) as Arc<dyn Bridge>);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let gateway = Gateway::new(
        Arc::clone(&slot) as Arc<dyn Bridge>,
        GatewayConfig {
            advertised_port: i32::from(addr.port()),
            max_produce_wait: ceiling_each,
            max_fetch_wait: ceiling_each,
            max_offset_wait: ceiling_each,
            drain_timeout: ceiling_each,
            ..GatewayConfig::default()
        },
    );
    let handle = gateway.handle();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    runtime.spawn(async move {
        let _keep = shutdown;
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        let _ = gateway.serve(listener, rx).await;
    });
    GatedNode {
        runtime: Some(runtime),
        addr,
        slot,
        handle,
        old: Some(old),
        release,
        entered,
    }
}

/// The demotion order, with a produce in flight: the slot is emptied first, the
/// call ends on the bridge it began on, the quiesce returns only after it, and
/// then nothing owns the retired bridge but the caller. Red against a quiesce
/// that returned at once.
#[test]
fn demotion_waits_for_the_call_in_flight_and_then_nothing_owns_the_bridge() {
    let mut node = gated_node(Duration::from_secs(5));
    let witness = Arc::downgrade(node.old());
    let addr = node.addr;

    let client = std::thread::spawn(move || produce(addr, TOPIC, &["in flight"]));
    node.entered.recv().unwrap(); // the append is inside the old bridge

    // Demotion, in the order the node runs it: clear, then quiesce, then drop.
    let retired = node.slot.clear().expect("a bridge was installed");
    let new = Arc::new(MemoryBridge::with_topics([TOPIC]));
    // The next leadership's bridge arrives while the old call is still running.
    node.slot.install(Arc::clone(&new) as Arc<dyn Bridge>);
    assert_eq!(node.handle.in_flight(), 1);

    let handle = node.handle.clone();
    let quiesce = node
        .rt()
        .spawn(async move { handle.quiesce(|_| panic!("well inside the ceiling")).await });
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        !quiesce.is_finished(),
        "the quiesce returned while a call still held the retired bridge"
    );
    assert!(witness.upgrade().is_some());

    node.release.send(()).unwrap();
    let report = node.rt().block_on(quiesce).unwrap();
    assert_eq!(report.in_flight, 1);
    assert!(!report.passed_ceiling);
    // The call finished on the bridge it began on and was acknowledged there.
    assert_eq!(client.join().unwrap(), (0, 0));
    assert_eq!(
        node.old()
            .appended
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(
        new.bounds(TOPIC),
        Ok((0, 0)),
        "a call that began under the old bridge did not append through the new one"
    );

    // The witness the directory's single owner rests on: the gateway and the
    // slot hold no second reference, so dropping what the caller was handed
    // is the last.
    drop(retired);
    node.old.take();
    assert!(
        witness.upgrade().is_none(),
        "something kept the retired bridge alive after demotion"
    );
}

#[test]
fn a_call_past_the_ceiling_is_reported_once_and_still_waited_for() {
    let node = gated_node(Duration::from_millis(50)); // ceilings sum to 200 ms
    let addr = node.addr;
    let client = std::thread::spawn(move || produce(addr, TOPIC, &["slow"]));
    node.entered.recv().unwrap();
    let retired = node.slot.clear();

    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handle = node.handle.clone();
    let on_ceiling = {
        let seen = Arc::clone(&seen);
        move |in_flight: usize| seen.lock().unwrap().push(in_flight)
    };
    let quiesce = node
        .rt()
        .spawn(async move { handle.quiesce(on_ceiling).await });
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(
        *seen.lock().unwrap(),
        vec![1],
        "reported once, with the count still running"
    );
    assert!(!quiesce.is_finished(), "past the ceiling the wait goes on");

    node.release.send(()).unwrap();
    let report = node.rt().block_on(quiesce).unwrap();
    assert!(report.passed_ceiling);
    assert!(report.waited >= Duration::from_millis(200));
    // The client was told it timed out at the produce ceiling; the append ran
    // on and finished, which is what the quiesce waited for.
    let _ = client.join().unwrap();
    assert_eq!(
        node.old()
            .appended
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    drop(retired);
}

#[test]
fn a_call_that_starts_after_the_slot_is_cleared_never_reaches_the_old_bridge() {
    let node = gated_node(Duration::from_secs(5));
    let retired = node.slot.clear();
    let (error, _) = produce(node.addr, TOPIC, &["after demotion"]);
    assert_eq!(error, NOT_LEADER);
    assert!(
        node.entered.try_recv().is_err(),
        "the old bridge was asked after it was cleared"
    );
    assert_eq!(
        node.old()
            .appended
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    drop(retired);
}

/// A node that demotes keeps listening, and every request a client sends is
/// work the gateway counts. The quiesce waits for what was running when the
/// slot was cleared, not for the gateway to go idle: traffic that never
/// touches the retired bridge must not hold a demotion open. Red against a
/// quiesce that waited on the gateway's total (a Codex finding on this PR).
#[test]
fn work_that_arrives_during_a_demotion_does_not_hold_it_open() {
    fn gate() -> (
        Arc<Gated>,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (entered_tx, entered) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let bridge = Arc::new(Gated {
            entered: entered_tx,
            release: std::sync::Mutex::new(release_rx),
            appended: std::sync::atomic::AtomicUsize::new(0),
        });
        (bridge, entered, release)
    }
    let (leader, leader_entered, release_leader) = gate();
    let (unrelated, unrelated_entered, release_unrelated) = gate();
    let slot = Arc::new(BridgeSlot::new([TOPIC]));
    slot.install(Arc::clone(&leader) as Arc<dyn Bridge>);
    // Two Kafka names: the range's, behind the slot, and another backend this
    // node's range has nothing to do with.
    let topics = TopicMap::routed(vec![
        TopicBinding {
            name: TOPIC.to_owned(),
            backend: Arc::clone(&slot) as Arc<dyn Bridge>,
            backend_topic: TOPIC.to_owned(),
        },
        TopicBinding {
            name: "other".to_owned(),
            backend: Arc::clone(&unrelated) as Arc<dyn Bridge>,
            backend_topic: TOPIC.to_owned(),
        },
    ])
    .unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let gateway = Gateway::from_topics(
        topics,
        GatewayConfig {
            advertised_port: i32::from(addr.port()),
            ..GatewayConfig::default()
        },
    );
    let handle = gateway.handle();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (shutdown, rx) = tokio::sync::watch::channel(false);
    runtime.spawn(async move {
        let _keep = shutdown;
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        let _ = gateway.serve(listener, rx).await;
    });
    // A failed assertion below must not hang the suite on a call parked in a
    // gate: the gates are opened and the runtime abandoned on the way out.
    struct Teardown(
        Option<tokio::runtime::Runtime>,
        Vec<std::sync::mpsc::Sender<()>>,
    );
    impl Drop for Teardown {
        fn drop(&mut self) {
            for gate in &self.1 {
                let _ = gate.send(());
            }
            if let Some(runtime) = self.0.take() {
                runtime.shutdown_background();
            }
        }
    }
    let teardown = Teardown(
        Some(runtime),
        vec![release_leader.clone(), release_unrelated.clone()],
    );
    let runtime = teardown.0.as_ref().unwrap();

    let held = std::thread::spawn(move || produce(addr, TOPIC, &["held"]));
    leader_entered.recv().unwrap();
    let retired = slot.clear();
    let quiescing = handle.clone();
    let quiesce = runtime.spawn(async move { quiescing.quiesce(|_| ()).await });
    std::thread::sleep(Duration::from_millis(300)); // the generation is closed
    let arrives_later = std::thread::spawn(move || produce(addr, "other", &["unrelated"]));
    unrelated_entered.recv().unwrap();
    assert!(handle.in_flight() >= 2, "both calls are running");

    release_leader.send(()).unwrap();
    let report = runtime
        .block_on(async { tokio::time::timeout(Duration::from_secs(5), quiesce).await })
        .expect("the quiesce waited for work that arrived after it began")
        .unwrap();
    assert_eq!(
        report.in_flight, 1,
        "it was for the one call that held the bridge"
    );
    assert!(
        handle.in_flight() >= 1,
        "the unrelated call is still running"
    );

    release_unrelated.send(()).unwrap();
    assert_eq!(held.join().unwrap(), (0, 0));
    let _ = arrives_later.join().unwrap();
    drop(retired);
    drop(teardown);
}

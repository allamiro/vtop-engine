//! Three-node deterministic openraft harness over the vtop-meta adapter.
//!
//! Disk: each node owns a [`SimStorage`] + seeded [`Env`].
//! Network: in-process router; partitions drop RPCs as `Unreachable`.
//! Time: paused-clock tokio (`start_paused`) + `enable_tick/elect/heartbeat =
//! false` so elections and commits advance only via explicit triggers.
//! In-process RPCs are `tokio::spawn`'d so the current-thread runtime can
//! interleave leader and follower cores. Network backoff is zero-duration so
//! paused time does not strand replication after Unreachable. Seeds are
//! printed on every assertion failure.

#![allow(clippy::result_large_err)] // openraft RPCError is large by value

use openraft::error::{RPCError, RaftError, Unreachable};
use openraft::network::{Backoff, RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{Config, EmptyNode, Raft, ServerState, SnapshotPolicy};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use uuid::Uuid;
use vtop_log::sim::SimStorage;
use vtop_meta::raft::{
    MetaRaftLogStore, MetaRaftStateMachine, MetaRaftStore, MetaRaftTypeConfig, NodeId,
};
use vtop_meta::{
    CommandEnvelope, MetaKey, MetaValue, MetadataCommand, MetadataResponse, NodeState,
};

const SEED: u64 = 0x5eed_0093;
const CLUSTER: Uuid = Uuid::from_u128(0xc1a5_7e15);

type MemRaft = Raft<MetaRaftTypeConfig>;

// ---------------------------------------------------------------------------
// In-memory network
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Router {
    inner: Arc<Mutex<RouterInner>>,
}

struct RouterInner {
    nodes: BTreeMap<NodeId, MemRaft>,
    /// Directed blocks: (from, to) means `from` cannot send to `to`.
    blocked: HashSet<(NodeId, NodeId)>,
    /// Seeded counter for deterministic delivery sequencing.
    delivery_seq: u64,
    seed: u64,
}

impl Router {
    fn new(seed: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RouterInner {
                nodes: BTreeMap::new(),
                blocked: HashSet::new(),
                delivery_seq: 0,
                seed,
            })),
        }
    }

    fn register(&self, id: NodeId, raft: MemRaft) {
        let mut guard = self.inner.lock().unwrap();
        guard.nodes.insert(id, raft);
    }

    fn unregister(&self, id: NodeId) {
        let mut guard = self.inner.lock().unwrap();
        guard.nodes.remove(&id);
        guard.blocked.retain(|&(a, b)| a != id && b != id);
    }

    fn isolate(&self, id: NodeId) {
        let mut guard = self.inner.lock().unwrap();
        let peers: Vec<NodeId> = guard.nodes.keys().copied().filter(|n| *n != id).collect();
        for peer in peers {
            guard.blocked.insert((id, peer));
            guard.blocked.insert((peer, id));
        }
    }

    fn heal(&self, id: NodeId) {
        let mut guard = self.inner.lock().unwrap();
        guard.blocked.retain(|&(a, b)| a != id && b != id);
    }

    fn raft(&self, id: NodeId) -> Option<MemRaft> {
        self.inner.lock().unwrap().nodes.get(&id).cloned()
    }

    fn note_delivery(&self) -> u64 {
        let mut guard = self.inner.lock().unwrap();
        let seq = guard.delivery_seq;
        guard.delivery_seq = guard.delivery_seq.wrapping_add(1).wrapping_add(guard.seed);
        seq
    }

    fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.inner.lock().unwrap().blocked.contains(&(from, to))
    }
}

#[derive(Clone)]
struct NetworkFactory {
    router: Router,
    source: NodeId,
}

impl RaftNetworkFactory<MetaRaftTypeConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, target: NodeId, _node: &EmptyNode) -> Self::Network {
        NetworkClient {
            router: self.router.clone(),
            source: self.source,
            target,
        }
    }
}

struct NetworkClient {
    router: Router,
    source: NodeId,
    target: NodeId,
}

impl NetworkClient {
    fn unreachable(&self) -> RPCError<NodeId, EmptyNode, RaftError<NodeId>> {
        RPCError::Unreachable(Unreachable::new(&io::Error::new(
            io::ErrorKind::ConnectionRefused,
            format!(
                "partition: {} -> {} (seed={:#x})",
                self.source,
                self.target,
                self.router.inner.lock().unwrap().seed
            ),
        )))
    }

    fn target_raft(&self) -> Result<MemRaft, RPCError<NodeId, EmptyNode, RaftError<NodeId>>> {
        let _ = self.router.note_delivery();
        if self.router.is_blocked(self.source, self.target) {
            return Err(self.unreachable());
        }
        self.router
            .raft(self.target)
            .ok_or_else(|| self.unreachable())
    }
}

impl RaftNetwork<MetaRaftTypeConfig> for NetworkClient {
    /// Zero backoff: under `start_paused`, a non-zero sleep never fires, so a
    /// single `Unreachable` during partition/restart would stall replication
    /// forever. Retries wait on the next RaftCore notify (heartbeat / write).
    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(Duration::from_millis(0)))
    }

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MetaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, EmptyNode, RaftError<NodeId>>> {
        let raft = self.target_raft()?;
        let target = self.target;
        // Spawn so the caller (leader raft core) can yield on current_thread;
        // otherwise in-process RPC deadlocks waiting for the follower core.
        tokio::spawn(async move { raft.append_entries(rpc).await })
            .await
            .map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&io::Error::other(format!(
                    "append join: {e}"
                ))))
            })?
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, EmptyNode, RaftError<NodeId>>> {
        let raft = self.target_raft()?;
        let target = self.target;
        tokio::spawn(async move { raft.vote(rpc).await })
            .await
            .map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&io::Error::other(format!(
                    "vote join: {e}"
                ))))
            })?
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MetaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, EmptyNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        let _ = self.router.note_delivery();
        if self.router.is_blocked(self.source, self.target) {
            return Err(RPCError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!(
                    "partition during snapshot {} -> {}",
                    self.source, self.target
                ),
            ))));
        }
        let raft = self.router.raft(self.target).ok_or_else(|| {
            RPCError::Unreachable(Unreachable::new(&io::Error::new(
                io::ErrorKind::NotFound,
                "target gone",
            )))
        })?;
        let target = self.target;
        tokio::spawn(async move { raft.install_snapshot(rpc).await })
            .await
            .map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&io::Error::other(format!(
                    "snapshot join: {e}"
                ))))
            })?
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(target, e)))
    }
}

// ---------------------------------------------------------------------------
// Cluster fixture
// ---------------------------------------------------------------------------

struct NodeHandle {
    raft: MemRaft,
    store: MetaRaftStore,
    sim: SimStorage,
    root: String,
}

struct Cluster {
    seed: u64,
    router: Router,
    nodes: BTreeMap<NodeId, NodeHandle>,
    next_request: u128,
}

impl Cluster {
    async fn boot(seed: u64) -> Self {
        let router = Router::new(seed);
        let config = Arc::new(
            Config {
                cluster_name: "vtop-meta-raft".into(),
                election_timeout_min: 10_000,
                election_timeout_max: 20_000,
                heartbeat_interval: 1_000,
                // Explicit triggers only — no wall-clock elections.
                enable_tick: false,
                enable_heartbeat: false,
                enable_elect: false,
                snapshot_policy: SnapshotPolicy::Never,
                max_in_snapshot_log_to_keep: 1,
                purge_batch_size: 1,
                ..Default::default()
            }
            .validate()
            .expect("raft config"),
        );

        let mut nodes = BTreeMap::new();
        for id in [1u64, 2, 3] {
            let handle = spawn_node(id, seed, router.clone(), config.clone())
                .await
                .unwrap_or_else(|e| panic!("spawn node {id} seed={seed:#x}: {e}"));
            nodes.insert(id, handle);
        }

        let cluster = Self {
            seed,
            router,
            nodes,
            next_request: 1,
        };
        let members: BTreeSet<NodeId> = [1, 2, 3].into_iter().collect();
        // `initialize` already appends membership at index 0 and starts an
        // election on this node — do not call `trigger().elect()` again or the
        // term advances while a term-1 client write may still be in flight.
        cluster.nodes[&1]
            .raft
            .initialize(members)
            .await
            .unwrap_or_else(|e| panic!("initialize seed={seed:#x}: {e}"));
        cluster.wait_leader().await;
        cluster
    }

    fn leader_id(&self) -> NodeId {
        for (id, node) in &self.nodes {
            let metrics = node.raft.metrics().borrow().clone();
            if metrics.state == ServerState::Leader {
                return *id;
            }
        }
        panic!("no leader seed={:#x}", self.seed);
    }

    fn leader(&self) -> &NodeHandle {
        &self.nodes[&self.leader_id()]
    }

    /// Poll metrics with yields, capped paused-time advances, and heartbeats.
    ///
    /// Advances are capped well below `election_timeout_max` (leader lease) so
    /// the leader does not step down, but are enough to fire openraft's 1ms
    /// chunked-snapshot sleeps under `start_paused`.
    async fn wait_until(&self, label: &str, mut pred: impl FnMut(&Self) -> bool) {
        // Cap paused-time advances well below leader_lease (= election_timeout_max
        // = 20s). openraft's chunked snapshot transport sleeps 1ms between chunks;
        // under start_paused those sleeps never complete without advance().
        let mut advanced_ms = 0u64;
        for step in 0..500_000u32 {
            if pred(self) {
                return;
            }
            tokio::task::yield_now().await;
            if advanced_ms < 5_000 {
                tokio::time::advance(Duration::from_millis(1)).await;
                advanced_ms += 1;
            }
            if step % 1_000 == 999 {
                if let Some((_, node)) = self
                    .nodes
                    .iter()
                    .find(|(_, n)| n.raft.metrics().borrow().state == ServerState::Leader)
                {
                    let _ = node.raft.trigger().heartbeat().await;
                }
            }
        }
        panic!("{label} timed out seed={:#x}", self.seed);
    }

    async fn wait_leader(&self) {
        self.wait_until("leader election", |cluster| {
            cluster
                .nodes
                .values()
                .filter(|n| n.raft.metrics().borrow().state == ServerState::Leader)
                .count()
                == 1
        })
        .await;
        let leaders: Vec<_> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.raft.metrics().borrow().state == ServerState::Leader)
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(
            leaders.len(),
            1,
            "expected exactly one leader, got {leaders:?} seed={:#x}",
            self.seed
        );
        let term = self.nodes[&leaders[0]].raft.metrics().borrow().current_term;
        // Give followers a moment to learn the leader id.
        self.wait_until("followers see leader", |cluster| {
            cluster.nodes.values().all(|n| {
                let m = n.raft.metrics().borrow().clone();
                m.state == ServerState::Leader || m.current_leader == Some(leaders[0])
            })
        })
        .await;
        for (id, node) in &self.nodes {
            let m = node.raft.metrics().borrow().clone();
            if m.state == ServerState::Leader {
                assert_eq!(m.current_term, term);
            } else {
                assert_eq!(
                    m.current_leader,
                    Some(leaders[0]),
                    "node {id} disagrees on leader seed={:#x}",
                    self.seed
                );
            }
        }
    }

    async fn write(&mut self, command: MetadataCommand) -> MetadataResponse {
        let leader = self.leader_id();
        let resp = self.nodes[&leader]
            .raft
            .client_write(command)
            .await
            .unwrap_or_else(|e| panic!("client_write seed={:#x}: {e}", self.seed));
        let _ = self.nodes[&leader].raft.trigger().heartbeat().await;
        let applied = resp.log_id.index;
        self.wait_applied_at_least(applied).await;
        MetadataResponse::decode(&resp.data)
            .unwrap_or_else(|e| panic!("decode response seed={:#x}: {e}", self.seed))
    }

    async fn wait_applied_at_least(&self, index: u64) {
        self.wait_until(&format!("applied>={index}"), |cluster| {
            cluster.nodes.values().all(|n| {
                n.raft
                    .metrics()
                    .borrow()
                    .last_applied
                    .map(|id| id.index >= index)
                    .unwrap_or(false)
            })
        })
        .await;
    }

    fn envelope(&mut self) -> CommandEnvelope {
        let request_id = Uuid::from_u128(self.next_request);
        self.next_request += 1;
        CommandEnvelope {
            request_id,
            issued_at_ms: 1_750_000_000_000,
        }
    }

    fn assert_converged_snapshots(&self) {
        let mut encoded: Option<Vec<u8>> = None;
        for (id, node) in &self.nodes {
            let bytes = node.store.with_storage(|storage| {
                storage
                    .state()
                    .encode_snapshot()
                    .unwrap_or_else(|e| panic!("encode snapshot node {id}: {e}"))
            });
            match &encoded {
                None => encoded = Some(bytes),
                Some(prev) => assert_eq!(
                    prev, &bytes,
                    "node {id} snapshot diverged seed={:#x}",
                    self.seed
                ),
            }
        }
    }

    async fn take_and_shutdown(&mut self, id: NodeId) -> (SimStorage, String) {
        let node = self.nodes.remove(&id).expect("node present");
        self.router.unregister(id);
        let sim = node.sim.clone();
        let root = node.root.clone();
        node.raft
            .shutdown()
            .await
            .unwrap_or_else(|e| panic!("shutdown {id} seed={:#x}: {e}", self.seed));
        (sim, root)
    }

    async fn restart_node(&mut self, id: NodeId, sim: SimStorage, root: String) {
        let config = Arc::new(
            Config {
                cluster_name: "vtop-meta-raft".into(),
                election_timeout_min: 10_000,
                election_timeout_max: 20_000,
                heartbeat_interval: 1_000,
                enable_tick: false,
                enable_heartbeat: false,
                enable_elect: false,
                snapshot_policy: SnapshotPolicy::Never,
                max_in_snapshot_log_to_keep: 1,
                purge_batch_size: 1,
                ..Default::default()
            }
            .validate()
            .expect("raft config"),
        );
        let handle = spawn_node_on(id, self.seed, self.router.clone(), config, sim, root)
            .await
            .unwrap_or_else(|e| panic!("restart {id} seed={:#x}: {e}", self.seed));
        self.nodes.insert(id, handle);
        // Nudge the leader so replication resumes toward the restarted node.
        let leader = self.leader_id();
        let _ = self.nodes[&leader].raft.trigger().heartbeat().await;
    }
}

async fn spawn_node(
    id: NodeId,
    seed: u64,
    router: Router,
    config: Arc<Config>,
) -> Result<NodeHandle, String> {
    let sim = SimStorage::new();
    let root = format!("/meta/{id}");
    sim.create_dir_all(std::path::Path::new(&root));
    spawn_node_on(id, seed, router, config, sim, root).await
}

async fn spawn_node_on(
    id: NodeId,
    seed: u64,
    router: Router,
    config: Arc<Config>,
    sim: SimStorage,
    root: String,
) -> Result<NodeHandle, String> {
    let env = sim.env(seed ^ id);
    let store = MetaRaftStore::open_tiny(&env, &root, CLUSTER).map_err(|e| e.to_string())?;
    let log_store = MetaRaftLogStore::new(store.clone());
    let state_machine = MetaRaftStateMachine::new(store.clone()).with_node_id(id);
    let network = NetworkFactory {
        router: router.clone(),
        source: id,
    };
    let raft = Raft::new(id, config, network, log_store, state_machine)
        .await
        .map_err(|e| e.to_string())?;
    router.register(id, raft.clone());
    Ok(NodeHandle {
        raft,
        store,
        sim,
        root,
    })
}

fn register_cmd(env: CommandEnvelope, node_uuid: Uuid) -> MetadataCommand {
    MetadataCommand::RegisterNode {
        env,
        node_uuid,
        addr: format!("10.0.0.{}:9200", node_uuid.as_u128() & 0xff),
        expected_generation: None,
    }
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn three_node_bootstrap_converges_to_byte_identical_snapshots() {
    let seed = SEED;
    let mut cluster = Cluster::boot(seed).await;
    let leader = cluster.leader_id();
    assert!(
        (1..=3).contains(&leader),
        "leader out of range seed={seed:#x}"
    );

    for i in 0..10u128 {
        let env = cluster.envelope();
        let cmd = register_cmd(env, Uuid::from_u128(0x100 + i));
        let resp = cluster.write(cmd).await;
        assert!(
            matches!(resp, MetadataResponse::Ack { .. }),
            "write {i} seed={seed:#x}: {resp:?}"
        );
    }
    cluster.assert_converged_snapshots();
}

/// #694: every replica writes one applied audit record per audited
/// command, with the same request id and log index, from the state-machine
/// wrapper; a node-initiated command writes none; and a restarted replica
/// replaying its log at open writes nothing again.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn every_replica_writes_one_applied_audit_record_per_audited_command() {
    use vtop_meta::audit::{AuditAction, AuditActor, AuditOutcome, AuditStage, Recording};
    let audit = Recording::installed();
    let seed = SEED ^ 0x694;
    let mut cluster = Cluster::boot(seed).await;
    // Request ids of this test only: the recording auditor is process-wide.
    let envelope = || CommandEnvelope {
        request_id: Uuid::new_v4(),
        issued_at_ms: 1_750_000_000_000,
    };
    let audited = envelope();
    cluster
        .write(register_cmd(audited, Uuid::from_u128(0x694)))
        .await;
    let records = audit.for_request(audited.request_id);
    assert_eq!(
        records.len(),
        3,
        "one applied record per replica: {records:?}"
    );
    let mut replicas: Vec<_> = records
        .iter()
        .map(|r| match r.actor {
            AuditActor::Replica(Some(id)) => id,
            ref other => panic!("an applied record names its replica, not {other:?}"),
        })
        .collect();
    replicas.sort_unstable();
    assert_eq!(replicas, vec![1, 2, 3]);
    let index = records[0]
        .log_index
        .expect("an applied record has its index");
    for record in &records {
        assert_eq!(record.stage, AuditStage::Applied);
        assert_eq!(record.action, AuditAction::ClusterPropose);
        assert_eq!(record.outcome, AuditOutcome::Success);
        assert_eq!(
            record.log_index,
            Some(index),
            "the same index on every replica"
        );
        assert!(record
            .params
            .contains(&("command", "RegisterNode".to_owned())));
    }

    // Rejected by the state machine (a generation that does not match): a
    // failure on every replica, with the reason.
    let rejected = envelope();
    cluster
        .write(MetadataCommand::RegisterNode {
            env: rejected,
            node_uuid: Uuid::from_u128(0x694),
            addr: "10.0.0.94:9200".to_owned(),
            expected_generation: Some(999),
        })
        .await;
    let records = audit.for_request(rejected.request_id);
    assert_eq!(records.len(), 3, "{records:?}");
    assert!(records.iter().all(|r| r.outcome == AuditOutcome::Failure
        && r.reason.as_deref().unwrap_or_default().contains("rejected")));

    // A replica restarted from its disk replays its log at open and writes
    // no record for what it had applied before.
    let follower = [1u64, 2, 3]
        .into_iter()
        .find(|id| *id != cluster.leader_id())
        .unwrap();
    let (sim, root) = cluster.take_and_shutdown(follower).await;
    cluster.restart_node(follower, sim, root).await;
    let after = envelope();
    cluster
        .write(register_cmd(after, Uuid::from_u128(0x695)))
        .await;
    assert_eq!(
        audit.for_request(audited.request_id).len(),
        3,
        "a restart must not write the applied records again"
    );
    assert_eq!(audit.for_request(after.request_id).len(), 3);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn leader_isolation_elects_new_leader_and_heals_without_losing_committed_writes() {
    let seed = SEED ^ 0x11;
    let mut cluster = Cluster::boot(seed).await;

    // Baseline committed prefix.
    for i in 0..3u128 {
        let env = cluster.envelope();
        cluster
            .write(register_cmd(env, Uuid::from_u128(0x200 + i)))
            .await;
    }
    let old_leader = cluster.leader_id();
    let baseline = cluster
        .leader()
        .store
        .with_storage(|s| s.state().encode_snapshot().expect("baseline snapshot"));

    // Partition the leader; elect among the remaining majority.
    cluster.router.isolate(old_leader);
    let candidate = [1u64, 2, 3]
        .into_iter()
        .find(|id| *id != old_leader)
        .unwrap();
    cluster.nodes[&candidate]
        .raft
        .trigger()
        .elect()
        .await
        .unwrap_or_else(|e| panic!("elect after isolation seed={seed:#x}: {e}"));

    // Wait until a *different* node is leader.
    for _ in 0..50 {
        tokio::task::yield_now().await;
        let leaders: Vec<_> = cluster
            .nodes
            .iter()
            .filter(|(id, n)| {
                **id != old_leader && n.raft.metrics().borrow().state == ServerState::Leader
            })
            .map(|(id, _)| *id)
            .collect();
        if leaders.len() == 1 {
            break;
        }
        // Nudge election on the other survivor too.
        let other = [1u64, 2, 3]
            .into_iter()
            .find(|id| *id != old_leader && *id != candidate)
            .unwrap();
        let _ = cluster.nodes[&other].raft.trigger().elect().await;
        tokio::task::yield_now().await;
    }
    let new_leader = cluster
        .nodes
        .iter()
        .find(|(id, n)| {
            **id != old_leader && n.raft.metrics().borrow().state == ServerState::Leader
        })
        .map(|(id, _)| *id)
        .unwrap_or_else(|| panic!("no new leader after isolation seed={seed:#x}"));
    assert_ne!(new_leader, old_leader);

    // New leader commits additional writes (majority of {survivors}).
    for i in 0..3u128 {
        let env = cluster.envelope();
        let cmd = register_cmd(env, Uuid::from_u128(0x300 + i));
        let resp = cluster.nodes[&new_leader]
            .raft
            .client_write(cmd)
            .await
            .unwrap_or_else(|e| panic!("write on new leader seed={seed:#x}: {e}"));
        let _ = cluster.nodes[&new_leader].raft.trigger().heartbeat().await;
        // Only wait on the majority (exclude isolated old leader).
        let need = resp.log_id.index;
        for _ in 0..200_000u32 {
            let ready = cluster.nodes.iter().all(|(id, n)| {
                *id == old_leader
                    || n.raft
                        .metrics()
                        .borrow()
                        .last_applied
                        .map(|id| id.index >= need)
                        .unwrap_or(false)
            });
            if ready {
                break;
            }
            tokio::task::yield_now().await;
            let _ = cluster.nodes[&new_leader].raft.trigger().heartbeat().await;
        }
        let majority_ok = cluster.nodes.iter().all(|(id, n)| {
            *id == old_leader
                || n.raft
                    .metrics()
                    .borrow()
                    .last_applied
                    .map(|id| id.index >= need)
                    .unwrap_or(false)
        });
        assert!(majority_ok, "majority did not apply {need} seed={seed:#x}");
    }

    // Heal; old leader must catch up. Baseline committed writes must remain.
    cluster.router.heal(old_leader);
    let _ = cluster.nodes[&new_leader].raft.trigger().heartbeat().await;
    let target = cluster.nodes[&new_leader]
        .raft
        .metrics()
        .borrow()
        .last_applied
        .map(|id| id.index)
        .unwrap_or(0);
    cluster.wait_applied_at_least(target).await;
    cluster.assert_converged_snapshots();

    // Committed prefix from before the partition is still present.
    let final_snap = cluster
        .leader()
        .store
        .with_storage(|s| s.state().encode_snapshot().expect("final snapshot"));
    assert!(
        final_snap.len() >= baseline.len(),
        "final state shrank after heal seed={seed:#x}"
    );
    for i in 0..3u128 {
        let key = MetaKey::Node {
            node_uuid: Uuid::from_u128(0x200 + i),
        };
        for (id, node) in &cluster.nodes {
            let present = node
                .store
                .with_storage(|s| s.state().record(&key).is_some());
            assert!(
                present,
                "node {id} lost baseline node 0x{:x} seed={seed:#x}",
                0x200 + i
            );
        }
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn follower_restart_recovers_from_sim_disk_and_catches_up() {
    let seed = SEED ^ 0x22;
    let mut cluster = Cluster::boot(seed).await;
    for i in 0..4u128 {
        let env = cluster.envelope();
        cluster
            .write(register_cmd(env, Uuid::from_u128(0x400 + i)))
            .await;
    }

    let follower = [1u64, 2, 3]
        .into_iter()
        .find(|id| *id != cluster.leader_id())
        .unwrap();
    let (sim, root) = cluster.take_and_shutdown(follower).await;

    for i in 0..4u128 {
        let env = cluster.envelope();
        cluster
            .write(register_cmd(env, Uuid::from_u128(0x500 + i)))
            .await;
    }

    cluster.restart_node(follower, sim, root).await;
    let target = cluster
        .leader()
        .raft
        .metrics()
        .borrow()
        .last_applied
        .map(|id| id.index)
        .unwrap_or(0);
    // Leader heartbeat so the restarted follower is driven.
    for _ in 0..20 {
        let _ = cluster.leader().raft.trigger().heartbeat().await;
        tokio::task::yield_now().await;
        let applied = cluster.nodes[&follower]
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map(|id| id.index)
            .unwrap_or(0);
        if applied >= target {
            break;
        }
        tokio::task::yield_now().await;
    }
    cluster.wait_applied_at_least(target).await;
    cluster.assert_converged_snapshots();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn snapshot_install_brings_blank_follower_to_byte_identical_state() {
    let seed = SEED ^ 0x33;
    let mut cluster = Cluster::boot(seed).await;

    // Enough entries that a snapshot is meaningful.
    for i in 0..20u128 {
        let env = cluster.envelope();
        cluster
            .write(register_cmd(env, Uuid::from_u128(0x600 + i)))
            .await;
    }

    let leader = cluster.leader_id();
    cluster.nodes[&leader]
        .raft
        .trigger()
        .snapshot()
        .await
        .unwrap_or_else(|e| panic!("snapshot seed={seed:#x}: {e}"));
    cluster
        .wait_until("leader snapshot", |c| {
            c.nodes[&leader].raft.metrics().borrow().snapshot.is_some()
        })
        .await;

    let snap_index = cluster.nodes[&leader]
        .raft
        .metrics()
        .borrow()
        .snapshot
        .map(|id| id.index)
        .unwrap_or(0);
    cluster.nodes[&leader]
        .raft
        .trigger()
        .purge_log(snap_index)
        .await
        .unwrap_or_else(|e| panic!("purge seed={seed:#x}: {e}"));

    // Replace a follower with a blank disk. openraft's chunked snapshot RPC
    // path sleeps 1ms between chunks (and hard-TTLs the install), which is
    // awkward under start_paused; install the leader snapshot directly through
    // the adapter's RaftStateMachine::install_snapshot via install_full_snapshot.
    let follower = [1u64, 2, 3].into_iter().find(|id| *id != leader).unwrap();
    let _ = cluster.take_and_shutdown(follower).await;
    let blank = SimStorage::new();
    let root = format!("/meta/{follower}");
    blank.create_dir_all(std::path::Path::new(&root));
    cluster.restart_node(follower, blank, root).await;

    let mut leader_sm = MetaRaftStateMachine::new(cluster.nodes[&leader].store.clone());
    let snapshot = leader_sm
        .get_current_snapshot()
        .await
        .unwrap_or_else(|e| panic!("leader get_current_snapshot seed={seed:#x}: {e}"))
        .unwrap_or_else(|| panic!("leader has no snapshot on disk seed={seed:#x}"));
    let mut leader_log = MetaRaftLogStore::new(cluster.nodes[&leader].store.clone());
    let vote = leader_log
        .read_vote()
        .await
        .unwrap_or_else(|e| panic!("leader read_vote seed={seed:#x}: {e}"))
        .unwrap_or_else(|| panic!("leader has no vote seed={seed:#x}"));
    cluster.nodes[&follower]
        .raft
        .install_full_snapshot(vote, snapshot)
        .await
        .unwrap_or_else(|e| panic!("install_full_snapshot seed={seed:#x}: {e}"));

    // One more committed write forces log replication of the post-snapshot
    // suffix; wait_until advances paused time so any residual chunk sleeps fire.
    let env = cluster.envelope();
    cluster
        .write(register_cmd(env, Uuid::from_u128(0x6ff)))
        .await;
    cluster.assert_converged_snapshots();
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn lease_fencing_epochs_are_strictly_monotonic_across_the_cluster() {
    let seed = SEED ^ 0x44;
    let mut cluster = Cluster::boot(seed).await;

    let node_uuid = Uuid::from_u128(0x10);
    let topic_uuid = Uuid::from_u128(0x20);
    let range_uuid = Uuid::from_u128(0x21);

    let env = cluster.envelope();
    let resp = cluster.write(register_cmd(env, node_uuid)).await;
    assert!(matches!(resp, MetadataResponse::Ack { .. }), "{resp:?}");

    // Nodes must be Active to hold leases — RegisterNode already inserts Active.
    let _ = NodeState::Active;

    let env = cluster.envelope();
    let resp = cluster
        .write(MetadataCommand::CreateTopic {
            env,
            name: "events.v1".into(),
            topic_uuid,
            root_range_uuid: range_uuid,
        })
        .await;
    assert!(
        matches!(resp, MetadataResponse::TopicCreated { .. }),
        "CreateTopic seed={seed:#x}: {resp:?}"
    );

    // CreateTopic inserts the root range at generation 0. Grant and Release
    // each bump generation, so every subsequent command must CAS the latest.
    let mut expected_generation = 0u64;
    let mut expected_epoch = 0u64;
    for round in 0..3u64 {
        let env = cluster.envelope();
        let resp = cluster
            .write(MetadataCommand::GrantRangeLease {
                env,
                topic_uuid,
                range_uuid,
                holder_node_uuid: node_uuid,
                expected_range_generation: expected_generation,
            })
            .await;
        match resp {
            MetadataResponse::LeaseGranted { fencing_epoch } => {
                expected_epoch += 1;
                assert_eq!(
                    fencing_epoch, expected_epoch,
                    "grant round {round} seed={seed:#x}"
                );
            }
            other => panic!("grant round {round} seed={seed:#x}: {other:?}"),
        }
        let env = cluster.envelope();
        let resp = cluster
            .write(MetadataCommand::ReleaseRangeLease {
                env,
                topic_uuid,
                range_uuid,
                expected_fencing_epoch: expected_epoch,
            })
            .await;
        match resp {
            MetadataResponse::Ack { generation } => {
                // Grant bumped generation too; release's Ack is authoritative.
                expected_generation = generation;
            }
            other => panic!("release round {round} seed={seed:#x}: {other:?}"),
        }
    }

    // Final grant so a lease is held at the terminal epoch.
    let env = cluster.envelope();
    let resp = cluster
        .write(MetadataCommand::GrantRangeLease {
            env,
            topic_uuid,
            range_uuid,
            holder_node_uuid: node_uuid,
            expected_range_generation: expected_generation,
        })
        .await;
    let MetadataResponse::LeaseGranted { fencing_epoch } = resp else {
        panic!("final grant seed={seed:#x}: {resp:?}");
    };
    assert_eq!(fencing_epoch, expected_epoch + 1);

    let range_key = MetaKey::Range {
        topic_uuid,
        range_uuid,
    };
    for (id, node) in &cluster.nodes {
        let epoch = node
            .store
            .with_storage(|s| match s.state().record(&range_key) {
                Some(MetaValue::Range(range)) => range.fencing_epoch,
                other => panic!("node {id} missing range seed={seed:#x}: {other:?}"),
            });
        assert_eq!(
            epoch,
            expected_epoch + 1,
            "node {id} fencing epoch seed={seed:#x}"
        );
    }
    cluster.assert_converged_snapshots();
}

/// #555: a lease deadline used to be the PROPOSER's wall clock plus the lease
/// length, trusted without limit. A data node with a fast clock then saw every
/// incumbent's lease as expired and stole it, and one holding a lease stamped
/// its own deadline an hour into the future, so after it died nobody could take
/// over until that hour passed. The leader now refuses a lease request whose
/// clock is further from its own than a quarter of the lease, before it is
/// replicated; within the bound the proposer's value is kept (the lease agent
/// compares the recorded deadline with its own clock, so silently moving it
/// into the leader's clock domain would make a skewed holder re-acquire its
/// own fresh lease every round).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn the_leader_refuses_a_skewed_lease_proposer_so_it_cannot_steal_or_poison_a_lease() {
    use std::sync::atomic::{AtomicI64, Ordering};
    use vtop_meta::{Consensus, ConsensusError, MetadataError, OpenraftConsensus};

    const LEADER_NOW: i64 = 1_750_000_100_000;
    const HOUR_MS: i64 = 3_600_000;
    const LEASE_MS: u64 = 15_000; // tolerance: the 1 000 ms floor (a twentieth is 750)

    let seed = SEED ^ 0x555;
    let mut cluster = Cluster::boot(seed).await;
    let (holder, rival) = (Uuid::from_u128(0x10), Uuid::from_u128(0x11));
    let (topic_uuid, range_uuid) = (Uuid::from_u128(0x20), Uuid::from_u128(0x21));
    for node in [holder, rival] {
        let env = cluster.envelope();
        let resp = cluster.write(register_cmd(env, node)).await;
        assert!(matches!(resp, MetadataResponse::Ack { .. }), "{resp:?}");
    }
    let env = cluster.envelope();
    let resp = cluster
        .write(MetadataCommand::CreateTopic {
            env,
            name: "events.v1".into(),
            topic_uuid,
            root_range_uuid: range_uuid,
        })
        .await;
    assert!(
        matches!(resp, MetadataResponse::TopicCreated { .. }),
        "{resp:?}"
    );

    // The consensus façade over the LEADER's raft, with a clock we control and
    // the applied state attached (the dedup check reads it).
    let leader_clock = std::sync::Arc::new(AtomicI64::new(LEADER_NOW));
    let consensus = OpenraftConsensus::new(cluster.leader().raft.clone())
        .with_store(cluster.leader().store.clone());
    {
        let clock = leader_clock.clone();
        consensus.set_leader_clock(move || clock.load(Ordering::SeqCst));
    }
    let read_range = |cluster: &Cluster| {
        let range_key = MetaKey::Range {
            topic_uuid,
            range_uuid,
        };
        cluster
            .leader()
            .store
            .with_storage(|s| match s.state().record(&range_key) {
                Some(MetaValue::Range(range)) => range.clone(),
                other => panic!("missing range seed={seed:#x}: {other:?}"),
            })
    };
    let envelope = |cluster: &mut Cluster, issued_at_ms: i64| CommandEnvelope {
        issued_at_ms,
        ..cluster.envelope()
    };
    let acquire =
        |env, holder_node_uuid, expected_range_generation| MetadataCommand::AcquireRangeLease {
            env,
            topic_uuid,
            range_uuid,
            holder_node_uuid,
            expected_range_generation,
            lease_duration_ms: LEASE_MS,
        };
    let renew = |env, holder_node_uuid, expected_fencing_epoch| MetadataCommand::RenewRangeLease {
        env,
        topic_uuid,
        range_uuid,
        holder_node_uuid,
        expected_fencing_epoch,
        lease_duration_ms: LEASE_MS,
    };

    // 1. A candidate an hour FAST is refused outright: nothing is replicated,
    //    the range keeps no lease. (Before, it won, with a deadline an hour out.)
    let generation = read_range(&cluster).generation;
    let env = envelope(&mut cluster, LEADER_NOW + HOUR_MS);
    match consensus.propose(acquire(env, holder, generation)).await {
        Err(ConsensusError::ClockSkew { skew_ms, bound_ms }) => {
            assert_eq!((skew_ms, bound_ms), (HOUR_MS, 1_000), "seed={seed:#x}");
        }
        other => panic!("a fast-clocked candidate was not refused (seed={seed:#x}): {other:?}"),
    }
    assert!(
        read_range(&cluster).lease.is_none(),
        "nothing was replicated"
    );

    // 2. So is one an hour SLOW — the bound is symmetric.
    let env = envelope(&mut cluster, LEADER_NOW - HOUR_MS);
    assert!(matches!(
        consensus.propose(acquire(env, holder, generation)).await,
        Err(ConsensusError::ClockSkew { skew_ms, .. }) if skew_ms == -HOUR_MS
    ));

    // 3. A candidate whose clock is merely a little off (500 ms) wins, and its
    //    value is KEPT: the deadline is the proposer's time plus the lease, so
    //    the agent that holds it compares like with like.
    let env = envelope(&mut cluster, LEADER_NOW + 500);
    let granted = acquire(env, holder, generation);
    let receipt = consensus.propose(granted.clone()).await.unwrap();
    assert!(
        matches!(receipt.response, MetadataResponse::LeaseGranted { .. }),
        "seed={seed:#x}: {:?}",
        receipt.response
    );
    let _ = cluster.leader().raft.trigger().heartbeat().await;
    cluster
        .wait_applied_at_least(receipt.log_id.index - 1)
        .await; // meta index is 1-based
    let epoch = read_range(&cluster).fencing_epoch;
    let deadline = LEADER_NOW + 500 + LEASE_MS as i64;
    assert_eq!(
        read_range(&cluster).lease.expect("a lease").expires_at_ms,
        Some(deadline),
        "within the bound the proposer's clock is kept (seed={seed:#x})"
    );

    // 4. A retry of a request that already COMMITTED is answered from the dedup
    //    table even when its timestamp has aged past the bound since: a skew
    //    error about a request that succeeded would tell the caller it failed.
    leader_clock.store(LEADER_NOW + HOUR_MS, Ordering::SeqCst);
    let log_before = cluster.leader().raft.metrics().borrow().last_log_index;
    let retried = consensus.propose(granted).await;
    assert!(
        matches!(
            retried,
            Ok(ref r) if matches!(r.response, MetadataResponse::LeaseGranted { fencing_epoch } if fencing_epoch == epoch)
        ),
        "a committed request must be answered from the dedup table (seed={seed:#x}): {retried:?}"
    );
    // The receipt names a position at or after the entry that produced the
    // response - read with it, not from a metrics snapshot that can lag
    // (review of #566).
    let retried_index = retried.as_ref().unwrap().log_id.index;
    assert!(
        retried_index >= receipt.log_id.index,
        "the aged retry's receipt ({retried_index}) predates the original commit ({}) \
         (seed={seed:#x})",
        receipt.log_id.index
    );
    // ...answered, not re-proposed (review of #566): a stale command that
    // reached the log could be applied as new if the table evicted it first.
    assert_eq!(
        cluster.leader().raft.metrics().borrow().last_log_index,
        log_before,
        "the aged retry reached the log (seed={seed:#x})"
    );
    leader_clock.store(LEADER_NOW, Ordering::SeqCst);

    // 5. A FOLLOWER does not judge: its clock may differ from the proposer's
    //    by more than the bound while the leader's does not, and a skew error
    //    is not a redirect. It answers NotLeader, as it always has.
    let leader = cluster.leader_id();
    let follower = *cluster
        .nodes
        .keys()
        .find(|id| **id != leader)
        .expect("a follower");
    let on_follower = OpenraftConsensus::new(cluster.nodes[&follower].raft.clone())
        .with_store(cluster.nodes[&follower].store.clone());
    on_follower.set_leader_clock(|| LEADER_NOW);
    let env = envelope(&mut cluster, LEADER_NOW + HOUR_MS);
    let generation = read_range(&cluster).generation;
    assert!(
        matches!(
            on_follower.propose(acquire(env, rival, generation)).await,
            Err(ConsensusError::NotLeader { .. })
        ),
        "a follower must redirect, not report skew (seed={seed:#x})"
    );

    // 6. A fast-clocked rival is refused before it can try to take the live
    //    lease. (Before: it took the range.)
    let env = envelope(&mut cluster, LEADER_NOW + HOUR_MS);
    assert!(matches!(
        consensus.propose(acquire(env, rival, generation)).await,
        Err(ConsensusError::ClockSkew { .. })
    ));

    // 7. Renewals are held to the same bound, in both directions, and a
    //    refused renewal leaves the deadline alone.
    for skew in [HOUR_MS, -HOUR_MS] {
        let env = envelope(&mut cluster, LEADER_NOW + skew);
        assert!(
            matches!(
                consensus.propose(renew(env, holder, epoch)).await,
                Err(ConsensusError::ClockSkew { .. })
            ),
            "renewal {skew} ms off (seed={seed:#x})"
        );
    }
    assert_eq!(
        read_range(&cluster).lease.expect("a lease").expires_at_ms,
        Some(deadline)
    );

    // 8. Takeover timing follows the replicated deadline. One millisecond
    //    before it a rival with a good clock is turned away because the
    //    incumbent is live; at the deadline it wins.
    leader_clock.store(deadline - 1, Ordering::SeqCst);
    let env = envelope(&mut cluster, deadline - 1);
    let receipt = consensus
        .propose(acquire(env, rival, generation))
        .await
        .unwrap();
    match receipt.response {
        MetadataResponse::Rejected(MetadataError::InvalidTransition(why)) => {
            assert!(why.contains("still live"), "seed={seed:#x}: {why}")
        }
        other => panic!("a live lease was taken (seed={seed:#x}): {other:?}"),
    }
    let _ = cluster.leader().raft.trigger().heartbeat().await;
    cluster
        .wait_applied_at_least(receipt.log_id.index - 1)
        .await;
    let generation = read_range(&cluster).generation;
    leader_clock.store(deadline, Ordering::SeqCst);
    let env = envelope(&mut cluster, deadline);
    let receipt = consensus
        .propose(acquire(env, rival, generation))
        .await
        .unwrap();
    assert!(
        matches!(receipt.response, MetadataResponse::LeaseGranted { fencing_epoch } if fencing_epoch == epoch + 1),
        "at the replicated deadline the lease is up for election (seed={seed:#x}): {:?}",
        receipt.response
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn single_node_client_write_commits() {
    let seed = SEED ^ 0x99;
    let router = Router::new(seed);
    let config = Arc::new(
        Config {
            cluster_name: "vtop-meta-raft-1".into(),
            election_timeout_min: 10_000,
            election_timeout_max: 20_000,
            heartbeat_interval: 1_000,
            enable_tick: false,
            enable_heartbeat: false,
            enable_elect: false,
            snapshot_policy: SnapshotPolicy::Never,
            max_in_snapshot_log_to_keep: 1,
            purge_batch_size: 1,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let handle = spawn_node(1, seed, router.clone(), config).await.unwrap();
    let members: BTreeSet<NodeId> = [1].into_iter().collect();
    // initialize() already elects on a single-node membership.
    handle.raft.initialize(members).await.unwrap();
    for _ in 0..100_000u32 {
        if handle.raft.metrics().borrow().state == ServerState::Leader {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(handle.raft.metrics().borrow().state, ServerState::Leader);
    let cmd = register_cmd(
        CommandEnvelope {
            request_id: Uuid::from_u128(1),
            issued_at_ms: 1,
        },
        Uuid::from_u128(0x42),
    );
    let resp = handle.raft.client_write(cmd).await.expect("write");
    assert!(resp.log_id.index >= 1);
}

/// #554: during a rolling upgrade an upgraded leader must not put a command
/// kind into the log that a voter still on the previous release cannot decode
/// - the old voter would fail the whole AppendEntries batch and append nothing
/// more. A command above the cluster's feature level is refused by the leader
/// before replication; once an operator raises the level (every voter
/// upgraded), the same command reaches the state machine on every node.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_command_newer_than_the_clusters_feature_level_never_reaches_the_log() {
    use vtop_meta::{Consensus, ConsensusError, MetaNodeId, OpenraftConsensus};
    use vtop_meta::{BASELINE_FEATURE_LEVEL, SUPPORTED_FEATURE_LEVEL};

    let seed = SEED ^ 0x554;
    let mut cluster = Cluster::boot(seed).await;
    let consensus = OpenraftConsensus::new(cluster.leader().raft.clone())
        .with_store(cluster.leader().store.clone());
    let split = |env: CommandEnvelope| MetadataCommand::SplitRange {
        env,
        topic_uuid: Uuid::from_u128(0x20),
        parent_range_uuid: Uuid::from_u128(0x21),
        left_range_uuid: Uuid::from_u128(0x22),
        right_range_uuid: Uuid::from_u128(0x23),
        transition_id: Uuid::from_u128(0x24),
        barrier_offset: 0,
        expected_range_generation: 0,
        holder_node_uuid: Uuid::from_u128(0x10),
        fencing_epoch: 1,
    };
    let last_log = |cluster: &Cluster| {
        cluster
            .leader()
            .raft
            .metrics()
            .borrow()
            .last_log_index
            .unwrap_or(0)
    };

    let before = last_log(&cluster);
    let env = cluster.envelope();
    match consensus.propose(split(env)).await {
        Err(ConsensusError::FeatureNotEnabled { needed, enabled }) => {
            assert_eq!((needed, enabled), (4, BASELINE_FEATURE_LEVEL));
        }
        other => panic!("a gated command was not refused (seed={seed:#x}): {other:?}"),
    }
    assert_eq!(
        last_log(&cluster),
        before,
        "the refused command reached the log"
    );

    // A follower does not let it through on the strength of its role (a
    // role read before the write is a race: review) - it sends the caller
    // to the leader, which judges it.
    let leader_id = cluster.leader_id();
    let (follower_id, through_follower) = cluster
        .nodes
        .iter()
        .find(|(id, _)| **id != leader_id)
        .map(|(id, follower)| {
            (
                *id,
                OpenraftConsensus::new(follower.raft.clone()).with_store(follower.store.clone()),
            )
        })
        .unwrap();
    let env = cluster.envelope();
    match through_follower.propose(split(env)).await {
        Err(ConsensusError::NotLeader { leader }) => {
            assert_eq!(
                leader,
                Some(MetaNodeId(leader_id)),
                "follower {follower_id}"
            );
        }
        other => panic!("a follower let a gated command through (seed={seed:#x}): {other:?}"),
    }
    // Nor does a façade with no applied state to judge the level from.
    let storeless = OpenraftConsensus::new(cluster.leader().raft.clone());
    let env = cluster.envelope();
    assert!(
        matches!(
            storeless.propose(split(env)).await,
            Err(ConsensusError::Message(_))
        ),
        "a façade that cannot see the level must refuse (seed={seed:#x})"
    );
    assert_eq!(
        last_log(&cluster),
        before,
        "a refused command reached the log"
    );

    let env = cluster.envelope();
    let raised = consensus
        .propose(MetadataCommand::SetFeatureLevel {
            env,
            feature_level: SUPPORTED_FEATURE_LEVEL,
        })
        .await
        .unwrap();
    assert_eq!(
        raised.response,
        MetadataResponse::Ack {
            generation: u64::from(SUPPORTED_FEATURE_LEVEL)
        }
    );
    let _ = cluster.leader().raft.trigger().heartbeat().await;
    cluster.wait_applied_at_least(raised.log_id.index - 1).await; // meta index is 1-based
    for (id, node) in &cluster.nodes {
        assert_eq!(
            node.store.with_storage(|s| s.state().feature_level()),
            SUPPORTED_FEATURE_LEVEL,
            "node {id} (seed={seed:#x})"
        );
    }

    // Past the gate, the state machine judges it (no such topic here).
    let env = cluster.envelope();
    let judged = consensus.propose(split(env)).await.unwrap();
    assert!(
        matches!(judged.response, MetadataResponse::Rejected(_)),
        "{:?}",
        judged.response
    );
}

/// #577: a promotion report that carries a lineage verdict on each quorum
/// answer is the SAME command kind as v0.5.0's report with a payload v0.6.0
/// cannot decode (a new outcome tag). A cluster below the verdicts' level
/// must not get it into its log, while the verdict-less report the data node
/// falls back to keeps working - promotion must not stall on an old tier.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_lineage_carrying_report_never_reaches_an_old_level_log() {
    use vtop_meta::{
        AnswerLineage, Consensus, ConsensusError, OpenraftConsensus, PromotionOutcome,
        QuorumAnswer, LINEAGE_VERDICTS_FEATURE_LEVEL,
    };

    let seed = SEED ^ 0x577;
    let mut cluster = Cluster::boot(seed).await;
    let consensus = OpenraftConsensus::new(cluster.leader().raft.clone())
        .with_store(cluster.leader().store.clone());
    let report =
        |env: CommandEnvelope, outcome: PromotionOutcome| MetadataCommand::ReportPromotionOutcome {
            env,
            topic_uuid: Uuid::from_u128(0x20),
            range_uuid: Uuid::from_u128(0x21),
            holder_node_uuid: Uuid::from_u128(0x10),
            fencing_epoch: 1,
            outcome,
        };
    let with_verdicts = PromotionOutcome::Established {
        boundary_offset: Some(5),
        sealed_prefix_end: None,
        quorum: vec![
            QuorumAnswer {
                node_uuid: Uuid::from_u128(0x10),
                offset: 5,
                lineage: Some(AnswerLineage::Agreed),
            },
            QuorumAnswer {
                node_uuid: Uuid::from_u128(0x11),
                offset: 5,
                lineage: Some(AnswerLineage::Unknown),
            },
        ],
        votes: 1,
        required: 2,
        attestation: None,
    };
    let last_log = |cluster: &Cluster| {
        cluster
            .leader()
            .raft
            .metrics()
            .borrow()
            .last_log_index
            .unwrap_or(0)
    };

    // The level just below the verdicts': everything else this release adds
    // is enabled, so the refusal is about the payload and nothing else.
    let below = LINEAGE_VERDICTS_FEATURE_LEVEL - 1;
    let env = cluster.envelope();
    let raised = consensus
        .propose(MetadataCommand::SetFeatureLevel {
            env,
            feature_level: below,
        })
        .await
        .unwrap();
    assert_eq!(
        raised.response,
        MetadataResponse::Ack {
            generation: u64::from(below)
        }
    );

    let before = last_log(&cluster);
    let env = cluster.envelope();
    match consensus.propose(report(env, with_verdicts.clone())).await {
        Err(ConsensusError::FeatureNotEnabled { needed, enabled }) => {
            assert_eq!((needed, enabled), (LINEAGE_VERDICTS_FEATURE_LEVEL, below));
        }
        other => panic!(
            "a lineage-carrying report was not refused at level {below} (seed={seed:#x}): \
             {other:?}"
        ),
    }
    assert_eq!(
        last_log(&cluster),
        before,
        "the refused lineage-carrying report reached the log"
    );

    // The legacy form passes the gate (the state machine then judges it: no
    // such range here).
    let env = cluster.envelope();
    let legacy = consensus
        .propose(report(env, with_verdicts.without_lineage()))
        .await
        .unwrap();
    assert!(
        matches!(legacy.response, MetadataResponse::Rejected(_)),
        "{:?}",
        legacy.response
    );

    // Raised, the verdicts pass the gate too.
    let env = cluster.envelope();
    consensus
        .propose(MetadataCommand::SetFeatureLevel {
            env,
            feature_level: LINEAGE_VERDICTS_FEATURE_LEVEL,
        })
        .await
        .unwrap();
    let env = cluster.envelope();
    let judged = consensus.propose(report(env, with_verdicts)).await.unwrap();
    assert!(
        matches!(judged.response, MetadataResponse::Rejected(_)),
        "{:?}",
        judged.response
    );
}

/// #634: a data node's version report is a command kind no release before
/// it can decode, so a tier at the previous release's level - every voter
/// may still be on it - must never get one into its log, while a data node
/// upgraded ahead of the tier keeps asking. Once the level is raised, the
/// report is recorded on every voter, and the cluster-versions read serves
/// it back with the level in force.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_version_report_never_reaches_a_log_below_its_level() {
    use vtop_meta::transport::AdminReadClusterVersionsRequest;
    use vtop_meta::{
        Consensus, ConsensusError, OpenraftConsensus, LINEAGE_VERDICTS_FEATURE_LEVEL,
        NODE_VERSION_REPORTS_FEATURE_LEVEL,
    };

    let seed = SEED ^ 0x634;
    let mut cluster = Cluster::boot(seed).await;
    let consensus = OpenraftConsensus::new(cluster.leader().raft.clone())
        .with_store(cluster.leader().store.clone());
    let data_node = Uuid::from_u128(0x10);
    let report = |env: CommandEnvelope| MetadataCommand::ReportNodeVersion {
        env,
        node_uuid: data_node,
        release: "0.8.0".to_owned(),
        max_replica_protocol: 2,
        supported_feature_level: NODE_VERSION_REPORTS_FEATURE_LEVEL,
    };
    let last_log = |cluster: &Cluster| {
        cluster
            .leader()
            .raft
            .metrics()
            .borrow()
            .last_log_index
            .unwrap_or(0)
    };
    let env = cluster.envelope();
    consensus
        .propose(register_cmd(env, data_node))
        .await
        .unwrap();

    // The highest level a tier of the previous release can be at.
    let previous = LINEAGE_VERDICTS_FEATURE_LEVEL;
    assert!(previous < NODE_VERSION_REPORTS_FEATURE_LEVEL);
    let env = cluster.envelope();
    consensus
        .propose(MetadataCommand::SetFeatureLevel {
            env,
            feature_level: previous,
        })
        .await
        .unwrap();

    let before = last_log(&cluster);
    for _ in 0..3 {
        let env = cluster.envelope();
        match consensus.propose(report(env)).await {
            Err(ConsensusError::FeatureNotEnabled { needed, enabled }) => {
                assert_eq!(
                    (needed, enabled),
                    (NODE_VERSION_REPORTS_FEATURE_LEVEL, previous)
                );
            }
            other => panic!(
                "a version report was not refused at level {previous} (seed={seed:#x}): {other:?}"
            ),
        }
    }
    assert_eq!(
        last_log(&cluster),
        before,
        "a refused version report reached the log an older voter must decode"
    );

    let env = cluster.envelope();
    consensus
        .propose(MetadataCommand::SetFeatureLevel {
            env,
            feature_level: NODE_VERSION_REPORTS_FEATURE_LEVEL,
        })
        .await
        .unwrap();
    let env = cluster.envelope();
    let recorded = consensus.propose(report(env)).await.unwrap();
    assert_eq!(
        recorded.response,
        MetadataResponse::Ack {
            generation: u64::from(NODE_VERSION_REPORTS_FEATURE_LEVEL)
        },
        "the report answers with the level in force"
    );
    let _ = cluster.leader().raft.trigger().heartbeat().await;
    cluster
        .wait_applied_at_least(recorded.log_id.index - 1)
        .await;
    for (id, node) in &cluster.nodes {
        let versions = node.store.with_storage(|s| s.state().cluster_versions());
        let entry = versions
            .nodes
            .iter()
            .find(|entry| entry.node_uuid == data_node)
            .unwrap_or_else(|| panic!("node {id} lost the data node (seed={seed:#x})"));
        assert_eq!(
            entry.report.as_ref().map(|r| r.release.as_str()),
            Some("0.8.0"),
            "node {id} (seed={seed:#x})"
        );
    }

    let read = vtop_meta::transport::AdminHandler::read_cluster_versions(
        &consensus,
        AdminReadClusterVersionsRequest {
            include_nodes: true,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        read.versions.feature_level,
        NODE_VERSION_REPORTS_FEATURE_LEVEL
    );
    assert!(!read.truncated);
    assert!(read
        .versions
        .nodes
        .iter()
        .any(|entry| entry.node_uuid == data_node && entry.report.is_some()));
    let level_only = vtop_meta::transport::AdminHandler::read_cluster_versions(
        &consensus,
        AdminReadClusterVersionsRequest {
            include_nodes: false,
        },
    )
    .await
    .unwrap();
    assert!(level_only.versions.nodes.is_empty());
    assert_eq!(
        level_only.versions.feature_level,
        NODE_VERSION_REPORTS_FEATURE_LEVEL
    );
}

/// #636: below feature level 8 the operator's override cannot enter the log
/// - neither the attestation nor a report that used one - and the
/// lineage-unproven refusal reason is held back with them. An older voter
/// could decode none of the three. At level 8 all three pass the gate.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn the_promotion_override_never_reaches_a_log_below_level_8() {
    use vtop_meta::{
        AnswerLineage, Consensus, ConsensusError, OpenraftConsensus, PromotionAttestation,
        PromotionOutcome, PromotionRefusal, QuorumAnswer, PROMOTE_UNPROVEN_FEATURE_LEVEL,
    };

    let seed = SEED ^ 0x636;
    let mut cluster = Cluster::boot(seed).await;
    let consensus = OpenraftConsensus::new(cluster.leader().raft.clone())
        .with_store(cluster.leader().store.clone());
    let topic_uuid = Uuid::from_u128(0x20);
    let range_uuid = Uuid::from_u128(0x21);
    let attest = |env: CommandEnvelope| MetadataCommand::AttestUnprovenPromotion {
        env,
        topic_uuid,
        range_uuid,
        replica_node_uuid: Uuid::from_u128(0x10),
        operator: "oncall".to_owned(),
    };
    let report =
        |env: CommandEnvelope, outcome: PromotionOutcome| MetadataCommand::ReportPromotionOutcome {
            env,
            topic_uuid,
            range_uuid,
            holder_node_uuid: Uuid::from_u128(0x10),
            fencing_epoch: 1,
            outcome,
        };
    let attested = PromotionOutcome::Established {
        boundary_offset: Some(5),
        sealed_prefix_end: None,
        quorum: vec![
            QuorumAnswer {
                node_uuid: Uuid::from_u128(0x10),
                offset: 5,
                lineage: Some(AnswerLineage::Unknown),
            },
            QuorumAnswer {
                node_uuid: Uuid::from_u128(0x11),
                offset: 5,
                lineage: Some(AnswerLineage::Unknown),
            },
        ],
        votes: 2,
        required: 2,
        attestation: Some(PromotionAttestation {
            attestation_id: Uuid::from_u128(0x636),
            replica_node_uuid: Uuid::from_u128(0x10),
            operator: "oncall".to_owned(),
            attested_at_ms: 0,
        }),
    };
    let unproven_refusal = PromotionOutcome::Refused {
        reason: PromotionRefusal::LineageUnproven,
    };
    let last_log = |cluster: &Cluster| {
        cluster
            .leader()
            .raft
            .metrics()
            .borrow()
            .last_log_index
            .unwrap_or(0)
    };

    // Everything below the override's level is enabled, so each refusal is
    // about the override and nothing else.
    let below = PROMOTE_UNPROVEN_FEATURE_LEVEL - 1;
    let env = cluster.envelope();
    consensus
        .propose(MetadataCommand::SetFeatureLevel {
            env,
            feature_level: below,
        })
        .await
        .unwrap();

    let commands = |cluster: &mut Cluster| {
        vec![
            ("the attestation", attest(cluster.envelope())),
            (
                "an attested report",
                report(cluster.envelope(), attested.clone()),
            ),
            (
                "the lineage-unproven refusal",
                report(cluster.envelope(), unproven_refusal.clone()),
            ),
        ]
    };
    for (what, command) in commands(&mut cluster) {
        let before = last_log(&cluster);
        match consensus.propose(command).await {
            Err(error @ ConsensusError::FeatureNotEnabled { needed, enabled }) => {
                assert_eq!((needed, enabled), (PROMOTE_UNPROVEN_FEATURE_LEVEL, below));
                // The refusal tells the operator what to do.
                assert!(
                    error.to_string().contains("enable-features --level 8"),
                    "{error}"
                );
            }
            other => panic!("{what} was not refused at level {below} (seed={seed:#x}): {other:?}"),
        }
        assert_eq!(last_log(&cluster), before, "{what} reached the log");
    }
    // Below the level the refusal goes as it always did.
    let env = cluster.envelope();
    let legacy = consensus
        .propose(report(env, unproven_refusal.without_unproven_refusal()))
        .await
        .unwrap();
    assert!(
        matches!(legacy.response, MetadataResponse::Rejected(_)),
        "no such range here, so the state machine judges it: {:?}",
        legacy.response
    );

    let env = cluster.envelope();
    consensus
        .propose(MetadataCommand::SetFeatureLevel {
            env,
            feature_level: PROMOTE_UNPROVEN_FEATURE_LEVEL,
        })
        .await
        .unwrap();
    for (what, command) in commands(&mut cluster) {
        let judged = consensus.propose(command).await.unwrap_or_else(|error| {
            panic!("{what} was refused at level 8 (seed={seed:#x}): {error}")
        });
        // No such range here: each is judged by the state machine, which
        // is what passing the gate means.
        assert!(
            matches!(judged.response, MetadataResponse::Rejected(_)),
            "{what}: {:?}",
            judged.response
        );
    }
}

//! Metadata group saturation research harness (#192).
//!
//! Measures when the **single three-node metadata Raft group** approaches
//! saturation and emits quantitative sharding-trigger criteria.
//!
//! **Non-goal:** does **not** implement multi-group / sharded metadata.
//! Epic #93: one correct metadata group first; shard only after measured need.
//!
//! ## How to run
//!
//! ```text
//! cargo test -p vtop-meta --test metadata_saturation_harness --locked
//!
//! VTOP_META_SATURATION_JSON=benchmarks/results/native-meta-saturation/summary.json \
//!   cargo test -p vtop-meta --test metadata_saturation_harness --locked -- --nocapture
//! ```
//!
//! Methodology: `docs/METADATA_SATURATION_RESEARCH.md`.

#![allow(clippy::result_large_err)] // openraft RPCError is large by value

use openraft::error::{RPCError, RaftError, Unreachable};
use openraft::network::{Backoff, RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{Config, EmptyNode, Raft, ServerState, SnapshotPolicy};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;
use vtop_log::sim::SimStorage;
use vtop_meta::placement::{select_replicas, PlacementCandidate};
use vtop_meta::raft::{
    MetaRaftLogStore, MetaRaftStateMachine, MetaRaftStore, MetaRaftTypeConfig, NodeId,
};
use vtop_meta::{
    CommandEnvelope, MetaLogConfig, MetaLogEntry, MetaLogPayload, MetaStorage, MetaStorageConfig,
    MetadataCommand, MetadataResponse, NodeState, RangeAssignment,
};

const SEED: u64 = 0x5eed_0192;
const CLUSTER: Uuid = Uuid::from_u128(0xc192_7e15);
const HARNESS_VERSION: &str = "1";
const ISSUE: &str = "192";
const METHODOLOGY: &str = "docs/METADATA_SATURATION_RESEARCH.md";

/// CI / default smoke counts (fast under `cargo test --workspace --locked`).
const SMOKE: WorkloadScale = WorkloadScale {
    mixed_ops: 48,
    heartbeats: 96,
    cursor_commits: 48,
    topics: 12,
    segments: 12,
    placements: 6,
    storage_topics: 24,
    storage_segments_per_topic: 2,
};

/// Extended lab counts (`--ignored`).
const EXTENDED: WorkloadScale = WorkloadScale {
    mixed_ops: 192,
    heartbeats: 384,
    cursor_commits: 192,
    topics: 48,
    segments: 48,
    placements: 24,
    storage_topics: 96,
    storage_segments_per_topic: 4,
};

// Sharding-trigger thresholds (dedicated soak; CI expected not to trip).
const TRIGGER_P99_COMMIT_MS: f64 = 50.0;
const TRIGGER_THROUGHPUT_SCALE_EFFICIENCY: f64 = 1.25;
const TRIGGER_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;
const TRIGGER_SNAPSHOT_WRITE_MS: f64 = 2000.0;
const TRIGGER_RECOVERY_MS: f64 = 5000.0;

type MemRaft = Raft<MetaRaftTypeConfig>;

#[derive(Clone, Copy, Debug)]
struct WorkloadScale {
    mixed_ops: usize,
    heartbeats: usize,
    cursor_commits: usize,
    topics: usize,
    segments: usize,
    placements: usize,
    storage_topics: usize,
    storage_segments_per_topic: usize,
}

#[derive(Clone, Debug, Serialize)]
struct LatencyMs {
    p50: f64,
    p95: f64,
    p99: f64,
    samples: usize,
}

#[derive(Clone, Debug, Serialize)]
struct EntityInventory {
    topics: u64,
    ranges: u64,
    sealed_segments: u64,
    consumer_groups: u64,
    group_members: u64,
    placements: u64,
}

#[derive(Clone, Debug, Serialize)]
struct ScenarioReport {
    name: String,
    status: &'static str,
    commands: u64,
    elapsed_ms: f64,
    commands_per_sec: f64,
    propose_latency_ms: Option<LatencyMs>,
    commit_latency_ms: Option<LatencyMs>,
    cpu_user_ms: f64,
    cpu_sys_ms: f64,
    inventory: Option<EntityInventory>,
    snapshot_bytes: Option<u64>,
    snapshot_encode_ms: Option<f64>,
    snapshot_write_ms: Option<f64>,
    recovery_ms: Option<f64>,
    notes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
struct TriggerEvaluation {
    id: String,
    tripped: bool,
    observed: f64,
    threshold: f64,
    detail: String,
}

#[derive(Clone, Debug, Serialize)]
struct Recommendation {
    default_path: String,
    pursue_multi_group_sharding: bool,
    tripped_criteria: usize,
    required_criteria: usize,
    gate: String,
    rationale: String,
    lab_limits: Vec<String>,
    evaluations: Vec<TriggerEvaluation>,
}

#[derive(Clone, Debug, Serialize)]
struct HostInfo {
    os: String,
    arch: String,
    unix_cpu_samples: bool,
}

#[derive(Clone, Debug, Serialize)]
struct HarnessReport {
    harness_version: String,
    issue: String,
    seed: u64,
    host: HostInfo,
    methodology: String,
    scale: String,
    caveats: Vec<String>,
    non_goals: Vec<String>,
    related: Vec<String>,
    scenarios: Vec<ScenarioReport>,
    recommendation: Recommendation,
}

struct CpuSample {
    user_ms: f64,
    sys_ms: f64,
}

// ---------------------------------------------------------------------------
// In-memory three-node Raft cluster (subset of raft_three_node patterns)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Router {
    inner: Arc<Mutex<RouterInner>>,
}

struct RouterInner {
    nodes: BTreeMap<NodeId, MemRaft>,
    blocked: HashSet<(NodeId, NodeId)>,
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
        self.inner.lock().unwrap().nodes.insert(id, raft);
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
        self.router.raft(self.target).ok_or_else(|| self.unreachable())
    }
}

impl RaftNetwork<MetaRaftTypeConfig> for NetworkClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MetaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, EmptyNode, RaftError<NodeId>>> {
        let raft = self.target_raft()?;
        let target = self.target;
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
                "partition during snapshot",
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

    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(Duration::from_millis(0)))
    }
}

struct NodeHandle {
    raft: MemRaft,
    store: MetaRaftStore,
}

struct Cluster {
    seed: u64,
    nodes: BTreeMap<NodeId, NodeHandle>,
    next_request: u128,
}

impl Cluster {
    async fn boot(seed: u64) -> Self {
        let router = Router::new(seed);
        let config = Arc::new(
            Config {
                cluster_name: "vtop-meta-saturation".into(),
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

        let mut nodes = BTreeMap::new();
        for id in [1u64, 2, 3] {
            let handle = spawn_node(id, seed, router.clone(), config.clone())
                .await
                .unwrap_or_else(|e| panic!("spawn node {id} seed={seed:#x}: {e}"));
            nodes.insert(id, handle);
        }

        let cluster = Self {
            seed,
            nodes,
            next_request: 1,
        };
        let members: BTreeSet<NodeId> = [1, 2, 3].into_iter().collect();
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
            if node.raft.metrics().borrow().state == ServerState::Leader {
                return *id;
            }
        }
        panic!("no leader seed={:#x}", self.seed);
    }

    async fn wait_until(&self, label: &str, mut pred: impl FnMut(&Self) -> bool) {
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
        let leader = self.leader_id();
        self.wait_until("followers see leader", |cluster| {
            cluster.nodes.values().all(|n| {
                let m = n.raft.metrics().borrow().clone();
                m.state == ServerState::Leader || m.current_leader == Some(leader)
            })
        })
        .await;
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

    /// Propose + wait applied. Returns propose-only and full-commit latencies.
    async fn write_timed(&mut self, command: MetadataCommand) -> (MetadataResponse, f64, f64) {
        let leader = self.leader_id();
        let propose_start = Instant::now();
        let resp = self.nodes[&leader]
            .raft
            .client_write(command)
            .await
            .unwrap_or_else(|e| panic!("client_write seed={:#x}: {e}", self.seed));
        let propose_ms = propose_start.elapsed().as_secs_f64() * 1000.0;
        let _ = self.nodes[&leader].raft.trigger().heartbeat().await;
        let applied = resp.log_id.index;
        let commit_start = Instant::now();
        self.wait_applied_at_least(applied).await;
        let commit_ms = propose_ms + commit_start.elapsed().as_secs_f64() * 1000.0;
        let decoded = MetadataResponse::decode(&resp.data)
            .unwrap_or_else(|e| panic!("decode response seed={:#x}: {e}", self.seed));
        (decoded, propose_ms, commit_ms)
    }

    fn record_count(&self) -> usize {
        let leader = self.leader_id();
        self.nodes[&leader]
            .store
            .with_storage(|storage| storage.state().record_count())
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
    sim.create_dir_all(Path::new(&root));
    let env = sim.env(seed ^ id);
    let store = MetaRaftStore::open_tiny(&env, &root, CLUSTER).map_err(|e| e.to_string())?;
    let log_store = MetaRaftLogStore::new(store.clone());
    let state_machine = MetaRaftStateMachine::new(store.clone());
    let network = NetworkFactory {
        router: router.clone(),
        source: id,
    };
    let raft = Raft::new(id, config, network, log_store, state_machine)
        .await
        .map_err(|e| e.to_string())?;
    router.register(id, raft.clone());
    // Keep SimStorage alive for the process lifetime of this node via the
    // open Env's Arc; MetaRaftStore holds the Env. Dropping the local `sim`
    // is fine because Env retains the storage Arc.
    let _ = sim;
    Ok(NodeHandle { raft, store })
}

// ---------------------------------------------------------------------------
// Metrics helpers
// ---------------------------------------------------------------------------

fn percentile(sorted_ms: &[f64], p: f64) -> f64 {
    if sorted_ms.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_ms.len() as f64 - 1.0) * p).round() as usize;
    sorted_ms[idx.min(sorted_ms.len() - 1)]
}

fn latency_from(mut samples: Vec<f64>) -> LatencyMs {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    LatencyMs {
        p50: percentile(&samples, 0.50),
        p95: percentile(&samples, 0.95),
        p99: percentile(&samples, 0.99),
        samples: samples.len(),
    }
}

#[cfg(unix)]
fn sample_cpu() -> CpuSample {
    // SAFETY: getrusage(RUSAGE_SELF) writes into a local rusage.
    unsafe {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        if libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) != 0 {
            return CpuSample {
                user_ms: 0.0,
                sys_ms: 0.0,
            };
        }
        let usage = usage.assume_init();
        CpuSample {
            user_ms: timeval_ms(usage.ru_utime),
            sys_ms: timeval_ms(usage.ru_stime),
        }
    }
}

#[cfg(not(unix))]
fn sample_cpu() -> CpuSample {
    CpuSample {
        user_ms: 0.0,
        sys_ms: 0.0,
    }
}

#[cfg(unix)]
fn timeval_ms(tv: libc::timeval) -> f64 {
    (tv.tv_sec as f64) * 1000.0 + (tv.tv_usec as f64) / 1000.0
}

fn cpu_delta(before: &CpuSample, after: &CpuSample) -> (f64, f64) {
    (
        (after.user_ms - before.user_ms).max(0.0),
        (after.sys_ms - before.sys_ms).max(0.0),
    )
}

fn empty_scenario(name: &str) -> ScenarioReport {
    ScenarioReport {
        name: name.to_owned(),
        status: "measured",
        commands: 0,
        elapsed_ms: 0.0,
        commands_per_sec: 0.0,
        propose_latency_ms: None,
        commit_latency_ms: None,
        cpu_user_ms: 0.0,
        cpu_sys_ms: 0.0,
        inventory: None,
        snapshot_bytes: None,
        snapshot_encode_ms: None,
        snapshot_write_ms: None,
        recovery_ms: None,
        notes: Vec::new(),
    }
}

fn finish_timed(
    mut report: ScenarioReport,
    commands: u64,
    propose: Vec<f64>,
    commit: Vec<f64>,
    wall: Duration,
    cpu_before: &CpuSample,
    cpu_after: &CpuSample,
) -> ScenarioReport {
    let (user, sys) = cpu_delta(cpu_before, cpu_after);
    let secs = wall.as_secs_f64().max(1e-9);
    report.commands = commands;
    report.elapsed_ms = wall.as_secs_f64() * 1000.0;
    report.commands_per_sec = commands as f64 / secs;
    report.propose_latency_ms = Some(latency_from(propose));
    report.commit_latency_ms = Some(latency_from(commit));
    report.cpu_user_ms = user;
    report.cpu_sys_ms = sys;
    report
}

impl EntityInventory {
    fn bootstrap_baseline() -> Self {
        // RaftFixture::bootstrap creates one topic/range/segment/group/member.
        Self {
            topics: 1,
            ranges: 1,
            sealed_segments: 1,
            consumer_groups: 1,
            group_members: 1,
            placements: 0,
        }
    }

    fn from_storage_scale(scale: &WorkloadScale) -> Self {
        let topics = scale.storage_topics as u64;
        let segs = (scale.storage_topics * scale.storage_segments_per_topic) as u64;
        Self {
            topics,
            ranges: topics,
            sealed_segments: segs,
            consumer_groups: 0,
            group_members: 0,
            placements: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Fixture builders on the Raft cluster
// ---------------------------------------------------------------------------

struct RaftFixture {
    broker_nodes: [Uuid; 3],
    topic_uuid: Uuid,
    range_uuid: Uuid,
    group_uuid: Uuid,
    member_uuid: Uuid,
    segment_root: [u8; 32],
    range_generation: u64,
    fencing_epoch: u64,
    next_segment: u128,
    cursor_generation: Option<u64>,
    /// Next base offset for newly registered segments on the fixture range.
    next_offset: u64,
    /// Monotonic cursor position within the seed sealed segment.
    cursor_offset: u64,
    inventory: EntityInventory,
    /// Per-broker-node placement-attr generation after bootstrap (= 1).
    placement_generations: [u64; 3],
}

impl RaftFixture {
    async fn bootstrap(cluster: &mut Cluster) -> Self {
        let broker_nodes = [
            Uuid::from_u128(0x1920_0001),
            Uuid::from_u128(0x1920_0002),
            Uuid::from_u128(0x1920_0003),
        ];
        for (i, node) in broker_nodes.iter().enumerate() {
            let env = cluster.envelope();
            let resp = cluster
                .write_timed(MetadataCommand::RegisterNode {
                    env,
                    node_uuid: *node,
                    addr: format!("10.0.0.{}:9200", i + 1),
                    expected_generation: None,
                })
                .await
                .0;
            assert!(
                matches!(resp, MetadataResponse::Ack { .. }),
                "register node: {resp:?}"
            );
            let env = cluster.envelope();
            let resp = cluster
                .write_timed(MetadataCommand::SetNodePlacementAttrs {
                    env,
                    node_uuid: *node,
                    failure_domain: format!("rack-{}", (b'a' + i as u8) as char),
                    placement_weight: 100,
                    expected_generation: 0,
                })
                .await
                .0;
            assert!(
                matches!(resp, MetadataResponse::Ack { generation: 1 }),
                "placement attrs: {resp:?}"
            );
            let _ = NodeState::Active;
        }

        let topic_uuid = Uuid::from_u128(0x1921_0001);
        let range_uuid = Uuid::from_u128(0x1921_0002);
        let env = cluster.envelope();
        let resp = cluster
            .write_timed(MetadataCommand::CreateTopic {
                env,
                name: "saturation.events".into(),
                topic_uuid,
                root_range_uuid: range_uuid,
            })
            .await
            .0;
        assert!(
            matches!(resp, MetadataResponse::TopicCreated { .. }),
            "create topic: {resp:?}"
        );

        let env = cluster.envelope();
        let resp = cluster
            .write_timed(MetadataCommand::GrantRangeLease {
                env,
                topic_uuid,
                range_uuid,
                holder_node_uuid: broker_nodes[0],
                expected_range_generation: 0,
            })
            .await
            .0;
        let fencing_epoch = match resp {
            MetadataResponse::LeaseGranted { fencing_epoch } => fencing_epoch,
            other => panic!("grant lease: {other:?}"),
        };

        let group_uuid = Uuid::from_u128(0x1922_0001);
        let member_uuid = Uuid::from_u128(0x1922_0002);
        let env = cluster.envelope();
        let resp = cluster
            .write_timed(MetadataCommand::CreateConsumerGroup {
                env,
                name: "saturation.group".into(),
                group_uuid,
            })
            .await
            .0;
        assert!(
            matches!(resp, MetadataResponse::GroupCreated { .. }),
            "create group: {resp:?}"
        );
        let env = cluster.envelope();
        let resp = cluster
            .write_timed(MetadataCommand::JoinConsumerGroup {
                env,
                group_uuid,
                member_uuid,
                expected_group_generation: 0,
            })
            .await
            .0;
        let member_generation = match resp {
            MetadataResponse::MemberJoined {
                member_generation, ..
            } => member_generation,
            other => panic!("join group: {other:?}"),
        };
        let env = cluster.envelope();
        let resp = cluster
            .write_timed(MetadataCommand::AssignMemberRanges {
                env,
                group_uuid,
                member_uuid,
                ranges: vec![RangeAssignment {
                    topic_uuid,
                    range_uuid,
                }],
                expected_member_generation: member_generation,
            })
            .await
            .0;
        assert!(
            matches!(resp, MetadataResponse::Ack { .. }),
            "assign ranges: {resp:?}"
        );

        // Seed one verified segment with a wide offset window so cursor
        // commits can advance monotonically without wrap/rewind rejects.
        let segment_uuid = Uuid::from_u128(0x1923_0001);
        let segment_root = [0x19; 32];
        let env = cluster.envelope();
        let resp = cluster
            .write_timed(MetadataCommand::RegisterSealedSegment {
                env,
                topic_uuid,
                range_uuid,
                segment_uuid,
                segment_generation: 0,
                base_offset: 0,
                next_offset: 1_000_000,
                content_root: segment_root,
                sealed_by_epoch: fencing_epoch,
                expected_range_generation: 1,
            })
            .await
            .0;
        assert!(
            matches!(resp, MetadataResponse::Ack { .. }),
            "register seed segment: {resp:?}"
        );
        let env = cluster.envelope();
        let resp = cluster
            .write_timed(MetadataCommand::MarkSegmentVerified {
                env,
                topic_uuid,
                range_uuid,
                segment_uuid,
                content_root: segment_root,
                expected_generation: 0,
            })
            .await
            .0;
        assert!(
            matches!(resp, MetadataResponse::Ack { .. }),
            "verify seed segment: {resp:?}"
        );

        Self {
            broker_nodes,
            topic_uuid,
            range_uuid,
            group_uuid,
            member_uuid,
            segment_root,
            range_generation: 2,
            fencing_epoch,
            next_segment: 0x1923_0010,
            cursor_generation: None,
            // Additional sealed segments start after the seed window.
            next_offset: 1_000_000,
            cursor_offset: 0,
            inventory: EntityInventory::bootstrap_baseline(),
            placement_generations: [1, 1, 1],
        }
    }

    async fn heartbeat(&mut self, cluster: &mut Cluster) -> (f64, f64) {
        let env = cluster.envelope();
        let (resp, p, c) = cluster
            .write_timed(MetadataCommand::HeartbeatMember {
                env,
                group_uuid: self.group_uuid,
                member_uuid: self.member_uuid,
            })
            .await;
        assert!(
            matches!(resp, MetadataResponse::Ack { .. }),
            "heartbeat: {resp:?}"
        );
        (p, c)
    }

    async fn cursor_commit(&mut self, cluster: &mut Cluster) -> (f64, f64) {
        let env = cluster.envelope();
        // Pin the seed segment lineage: range.lineage_generation stays 0 for
        // the unsplit root range; verified seed segment generation is 1.
        let offset = self.cursor_offset;
        self.cursor_offset = self.cursor_offset.saturating_add(1);
        let (resp, p, c) = cluster
            .write_timed(MetadataCommand::CommitGroupCursor {
                env,
                group_uuid: self.group_uuid,
                member_uuid: self.member_uuid,
                topic_uuid: self.topic_uuid,
                range_uuid: self.range_uuid,
                topic_epoch: 1,
                range_generation: 0,
                segment_uuid: Uuid::from_u128(0x1923_0001),
                segment_generation: 1,
                segment_root: self.segment_root,
                record_offset: offset,
                record_index: 0,
                lineage_transition_id: None,
                expected_checkpoint_generation: self.cursor_generation,
            })
            .await;
        match resp {
            MetadataResponse::CursorCommitted {
                checkpoint_generation,
            } => {
                self.cursor_generation = Some(checkpoint_generation);
            }
            other => panic!("cursor commit: {other:?}"),
        }
        (p, c)
    }

    async fn register_segment(&mut self, cluster: &mut Cluster) -> (f64, f64) {
        let segment_uuid = Uuid::from_u128(self.next_segment);
        self.next_segment += 1;
        let base = self.next_offset;
        let next = base + 32;
        self.next_offset = next;
        let env = cluster.envelope();
        let (resp, p, c) = cluster
            .write_timed(MetadataCommand::RegisterSealedSegment {
                env,
                topic_uuid: self.topic_uuid,
                range_uuid: self.range_uuid,
                segment_uuid,
                segment_generation: 0,
                base_offset: base,
                next_offset: next,
                content_root: self.segment_root,
                sealed_by_epoch: self.fencing_epoch,
                expected_range_generation: self.range_generation,
            })
            .await;
        match resp {
            MetadataResponse::Ack { generation } => {
                self.range_generation = generation;
            }
            other => panic!("register segment: {other:?}"),
        }
        let env = cluster.envelope();
        let (resp, p2, c2) = cluster
            .write_timed(MetadataCommand::MarkSegmentVerified {
                env,
                topic_uuid: self.topic_uuid,
                range_uuid: self.range_uuid,
                segment_uuid,
                content_root: self.segment_root,
                expected_generation: 0,
            })
            .await;
        assert!(
            matches!(resp, MetadataResponse::Ack { .. }),
            "verify segment: {resp:?}"
        );
        self.inventory.sealed_segments += 1;
        (p + p2, c + c2)
    }
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

async fn scenario_mixed(scale: &WorkloadScale) -> ScenarioReport {
    let mut cluster = Cluster::boot(SEED).await;
    let mut fx = RaftFixture::bootstrap(&mut cluster).await;
    let mut propose = Vec::new();
    let mut commit = Vec::new();
    let cpu_before = sample_cpu();
    let wall = Instant::now();
    let mut commands = 0u64;
    for i in 0..scale.mixed_ops {
        let (p, c) = match i % 3 {
            0 => fx.heartbeat(&mut cluster).await,
            1 => fx.cursor_commit(&mut cluster).await,
            _ => fx.register_segment(&mut cluster).await,
        };
        propose.push(p);
        commit.push(c);
        commands += if i % 3 == 2 { 2 } else { 1 };
    }
    let cpu_after = sample_cpu();
    let mut report = finish_timed(
        empty_scenario("raft_mixed_ops"),
        commands,
        propose,
        commit,
        wall.elapsed(),
        &cpu_before,
        &cpu_after,
    );
    report.inventory = Some(fx.inventory.clone());
    report.notes.push(
        "Mixed heartbeats / cursor commits / segment register+verify on one three-node group."
            .into(),
    );
    report.notes.push(format!(
        "Leader state-machine record_count={}",
        cluster.record_count()
    ));
    report
}

async fn scenario_heartbeats(scale: &WorkloadScale) -> ScenarioReport {
    let mut cluster = Cluster::boot(SEED ^ 0x11).await;
    let mut fx = RaftFixture::bootstrap(&mut cluster).await;
    let mut propose = Vec::new();
    let mut commit = Vec::new();
    let cpu_before = sample_cpu();
    let wall = Instant::now();
    for _ in 0..scale.heartbeats {
        let (p, c) = fx.heartbeat(&mut cluster).await;
        propose.push(p);
        commit.push(c);
    }
    let cpu_after = sample_cpu();
    let mut report = finish_timed(
        empty_scenario("raft_heartbeat_storm"),
        scale.heartbeats as u64,
        propose,
        commit,
        wall.elapsed(),
        &cpu_before,
        &cpu_after,
    );
    report.inventory = Some(fx.inventory.clone());
    report
}

async fn scenario_cursors(scale: &WorkloadScale) -> ScenarioReport {
    let mut cluster = Cluster::boot(SEED ^ 0x22).await;
    let mut fx = RaftFixture::bootstrap(&mut cluster).await;
    let mut propose = Vec::new();
    let mut commit = Vec::new();
    let cpu_before = sample_cpu();
    let wall = Instant::now();
    for _ in 0..scale.cursor_commits {
        let (p, c) = fx.cursor_commit(&mut cluster).await;
        propose.push(p);
        commit.push(c);
    }
    let cpu_after = sample_cpu();
    let mut report = finish_timed(
        empty_scenario("raft_cursor_commits"),
        scale.cursor_commits as u64,
        propose,
        commit,
        wall.elapsed(),
        &cpu_before,
        &cpu_after,
    );
    report.inventory = Some(fx.inventory.clone());
    report
}

async fn scenario_topic_growth(scale: &WorkloadScale) -> ScenarioReport {
    let mut cluster = Cluster::boot(SEED ^ 0x33).await;
    let node = Uuid::from_u128(0x1920_00aa);
    let env = cluster.envelope();
    let _ = cluster
        .write_timed(MetadataCommand::RegisterNode {
            env,
            node_uuid: node,
            addr: "10.0.0.9:9200".into(),
            expected_generation: None,
        })
        .await;
    let mut propose = Vec::new();
    let mut commit = Vec::new();
    let cpu_before = sample_cpu();
    let wall = Instant::now();
    for i in 0..scale.topics {
        let env = cluster.envelope();
        let (resp, p, c) = cluster
            .write_timed(MetadataCommand::CreateTopic {
                env,
                name: format!("sat.topic.{i}"),
                topic_uuid: Uuid::from_u128(0x1930_0000 + i as u128),
                root_range_uuid: Uuid::from_u128(0x1931_0000 + i as u128),
            })
            .await;
        assert!(
            matches!(resp, MetadataResponse::TopicCreated { .. }),
            "create topic {i}: {resp:?}"
        );
        propose.push(p);
        commit.push(c);
    }
    let cpu_after = sample_cpu();
    let mut report = finish_timed(
        empty_scenario("raft_topic_range_growth"),
        scale.topics as u64,
        propose,
        commit,
        wall.elapsed(),
        &cpu_before,
        &cpu_after,
    );
    report.inventory = Some(EntityInventory {
        topics: scale.topics as u64,
        ranges: scale.topics as u64,
        sealed_segments: 0,
        consumer_groups: 0,
        group_members: 0,
        placements: 0,
    });
    report.notes.push(
        "Each CreateTopic also inserts one root range (topics == ranges in this scenario)."
            .into(),
    );
    report
}

async fn scenario_segments(scale: &WorkloadScale) -> ScenarioReport {
    let mut cluster = Cluster::boot(SEED ^ 0x44).await;
    let mut fx = RaftFixture::bootstrap(&mut cluster).await;
    let mut propose = Vec::new();
    let mut commit = Vec::new();
    let cpu_before = sample_cpu();
    let wall = Instant::now();
    let mut commands = 0u64;
    for _ in 0..scale.segments {
        let (p, c) = fx.register_segment(&mut cluster).await;
        propose.push(p);
        commit.push(c);
        commands += 2;
    }
    let cpu_after = sample_cpu();
    let mut report = finish_timed(
        empty_scenario("raft_segment_registration"),
        commands,
        propose,
        commit,
        wall.elapsed(),
        &cpu_before,
        &cpu_after,
    );
    report.inventory = Some(fx.inventory.clone());
    report
        .notes
        .push("Each iteration is RegisterSealedSegment + MarkSegmentVerified.".into());
    report
}

async fn scenario_placement(scale: &WorkloadScale) -> ScenarioReport {
    let mut cluster = Cluster::boot(SEED ^ 0x55).await;
    let mut fx = RaftFixture::bootstrap(&mut cluster).await;
    let candidates: Vec<PlacementCandidate> = fx
        .broker_nodes
        .iter()
        .enumerate()
        .map(|(i, node)| PlacementCandidate {
            node_uuid: *node,
            failure_domain: format!("rack-{}", (b'a' + i as u8) as char),
            weight: 100,
        })
        .collect();

    let mut propose = Vec::new();
    let mut commit = Vec::new();
    let cpu_before = sample_cpu();
    let wall = Instant::now();
    let mut commands = 0u64;

    // Attr refresh + placement commits on freshly registered/verified segments.
    for i in 0..scale.placements {
        let node_idx = i % 3;
        let expected_generation = fx.placement_generations[node_idx];
        let env = cluster.envelope();
        let (resp, p, c) = cluster
            .write_timed(MetadataCommand::SetNodePlacementAttrs {
                env,
                node_uuid: fx.broker_nodes[node_idx],
                failure_domain: format!("rack-{}", (b'a' + node_idx as u8) as char),
                placement_weight: 100 + (i as u32 % 7),
                expected_generation,
            })
            .await;
        match resp {
            MetadataResponse::Ack { generation } => {
                fx.placement_generations[node_idx] = generation;
            }
            other => panic!("placement attrs refresh: {other:?}"),
        }
        propose.push(p);
        commit.push(c);
        commands += 1;

        let (p_seg, c_seg) = fx.register_segment(&mut cluster).await;
        propose.push(p_seg);
        commit.push(c_seg);
        commands += 2;

        let segment_uuid = Uuid::from_u128(fx.next_segment - 1);
        let replicas = select_replicas(segment_uuid, &candidates, 3, true)
            .expect("deterministic placement");
        let env = cluster.envelope();
        let (resp, p, c) = cluster
            .write_timed(MetadataCommand::CommitSegmentPlacement {
                env,
                topic_uuid: fx.topic_uuid,
                range_uuid: fx.range_uuid,
                segment_uuid,
                replication_factor: 3,
                replica_nodes: replicas,
                expected_segment_generation: 1,
                expected_placement_generation: None,
            })
            .await;
        assert!(
            matches!(resp, MetadataResponse::Ack { .. }),
            "commit placement: {resp:?}"
        );
        fx.inventory.placements += 1;
        propose.push(p);
        commit.push(c);
        commands += 1;
    }

    let cpu_after = sample_cpu();
    let mut report = finish_timed(
        empty_scenario("raft_placement_updates"),
        commands,
        propose,
        commit,
        wall.elapsed(),
        &cpu_before,
        &cpu_after,
    );
    report.inventory = Some(fx.inventory.clone());
    report.notes.push(
        "Placement attr CAS + verified segment registration + CommitSegmentPlacement.".into(),
    );
    report
}

fn storage_config() -> MetaStorageConfig {
    MetaStorageConfig {
        log: MetaLogConfig {
            max_chunk_bytes: 64 * 1024,
        },
    }
}

fn storage_envelope(n: u128) -> CommandEnvelope {
    CommandEnvelope {
        request_id: Uuid::from_u128(0x1950_0000 + n),
        issued_at_ms: 1_750_000_000_000,
    }
}

fn build_storage_growth(scale: &WorkloadScale) -> (SimStorage, String, u64) {
    let sim = SimStorage::new();
    let root = "/meta/saturation".to_owned();
    sim.create_dir_all(Path::new(&root));
    let env = sim.env(SEED ^ 0x66);
    let mut storage =
        MetaStorage::open_with(&env, &root, CLUSTER, storage_config()).expect("open storage");

    let mut entries = Vec::new();
    let mut index = 1u64;
    let mut req = 1u128;

    let nodes = [
        Uuid::from_u128(0x1960_0001),
        Uuid::from_u128(0x1960_0002),
        Uuid::from_u128(0x1960_0003),
    ];
    for (i, node) in nodes.iter().enumerate() {
        entries.push(MetaLogEntry {
            term: 1,
            index,
            payload: MetaLogPayload::Normal(MetadataCommand::RegisterNode {
                env: storage_envelope(req),
                node_uuid: *node,
                addr: format!("10.1.0.{}:9200", i + 1),
                expected_generation: None,
            }),
        });
        index += 1;
        req += 1;
        entries.push(MetaLogEntry {
            term: 1,
            index,
            payload: MetaLogPayload::Normal(MetadataCommand::SetNodePlacementAttrs {
                env: storage_envelope(req),
                node_uuid: *node,
                failure_domain: format!("rack-{}", (b'a' + i as u8) as char),
                placement_weight: 100,
                expected_generation: 0,
            }),
        });
        index += 1;
        req += 1;
    }

    for t in 0..scale.storage_topics {
        let topic = Uuid::from_u128(0x1970_0000 + t as u128);
        let range = Uuid::from_u128(0x1971_0000 + t as u128);
        entries.push(MetaLogEntry {
            term: 1,
            index,
            payload: MetaLogPayload::Normal(MetadataCommand::CreateTopic {
                env: storage_envelope(req),
                name: format!("store.topic.{t}"),
                topic_uuid: topic,
                root_range_uuid: range,
            }),
        });
        index += 1;
        req += 1;
        entries.push(MetaLogEntry {
            term: 1,
            index,
            payload: MetaLogPayload::Normal(MetadataCommand::GrantRangeLease {
                env: storage_envelope(req),
                topic_uuid: topic,
                range_uuid: range,
                holder_node_uuid: nodes[0],
                expected_range_generation: 0,
            }),
        });
        index += 1;
        req += 1;
        for (range_gen, s) in (1u64..).zip(0..scale.storage_segments_per_topic) {
            let segment = Uuid::from_u128(0x1980_0000 + (t as u128) * 64 + s as u128);
            let base = (s as u64) * 64;
            entries.push(MetaLogEntry {
                term: 1,
                index,
                payload: MetaLogPayload::Normal(MetadataCommand::RegisterSealedSegment {
                    env: storage_envelope(req),
                    topic_uuid: topic,
                    range_uuid: range,
                    segment_uuid: segment,
                    segment_generation: 0,
                    base_offset: base,
                    next_offset: base + 64,
                    content_root: [0x19; 32],
                    sealed_by_epoch: 1,
                    expected_range_generation: range_gen,
                }),
            });
            index += 1;
            req += 1;
            entries.push(MetaLogEntry {
                term: 1,
                index,
                payload: MetaLogPayload::Normal(MetadataCommand::MarkSegmentVerified {
                    env: storage_envelope(req),
                    topic_uuid: topic,
                    range_uuid: range,
                    segment_uuid: segment,
                    content_root: [0x19; 32],
                    expected_generation: 0,
                }),
            });
            index += 1;
            req += 1;
        }
    }

    storage.append(&entries).expect("append growth log");
    let last = index - 1;
    storage.apply_through(last).expect("apply growth log");
    (sim, root, last)
}

fn scenario_snapshot(scale: &WorkloadScale) -> ScenarioReport {
    let (sim, root, last) = build_storage_growth(scale);
    let env = sim.env(SEED ^ 0x66);
    let mut storage =
        MetaStorage::open_with(&env, &root, CLUSTER, storage_config()).expect("reopen for snapshot");
    assert_eq!(storage.last_applied(), last);

    let encode_start = Instant::now();
    let encoded = storage
        .state()
        .encode_snapshot()
        .expect("encode snapshot");
    let encode_ms = encode_start.elapsed().as_secs_f64() * 1000.0;

    let write_start = Instant::now();
    let meta = storage.write_snapshot().expect("write snapshot");
    let write_ms = write_start.elapsed().as_secs_f64() * 1000.0;

    let mut report = empty_scenario("storage_snapshot_growth");
    report.snapshot_bytes = Some(encoded.len() as u64);
    report.snapshot_encode_ms = Some(encode_ms);
    report.snapshot_write_ms = Some(write_ms);
    report.inventory = Some(EntityInventory::from_storage_scale(scale));
    report.elapsed_ms = encode_ms + write_ms;
    report.notes.push(format!(
        "Snapshot after {last} applied entries; file id={}; record_count={}",
        meta.snapshot_id,
        storage.state().record_count()
    ));
    report.notes.push(
        "SimStorage path — durable byte layout without real disk syscall cost.".into(),
    );
    let _ = sim;
    report
}

fn scenario_recovery(scale: &WorkloadScale) -> ScenarioReport {
    let (sim, root, last) = build_storage_growth(scale);
    let env = sim.env(SEED ^ 0x66);
    {
        let mut storage = MetaStorage::open_with(&env, &root, CLUSTER, storage_config())
            .expect("open before snapshot");
        storage.write_snapshot().expect("snapshot before recovery");
        // Append a short post-snapshot tail so recovery replays something.
        let mut tail = Vec::new();
        let mut index = last + 1;
        for i in 0..8u128 {
            tail.push(MetaLogEntry {
                term: 1,
                index,
                payload: MetaLogPayload::Normal(MetadataCommand::PutKeyRecord {
                    env: storage_envelope(0x1990_0000 + i),
                    key_uuid: Uuid::from_u128(0x1991_0000 + i),
                    scheme: 1,
                    public_material_digest: [0x42; 32],
                }),
            });
            index += 1;
        }
        storage.append(&tail).expect("append tail");
        storage
            .apply_through(index - 1)
            .expect("apply recovery tail");
    }
    sim.reboot();
    let env = sim.env(SEED ^ 0x66);
    let start = Instant::now();
    let storage =
        MetaStorage::open_with(&env, &root, CLUSTER, storage_config()).expect("recovery open");
    let recovery_ms = start.elapsed().as_secs_f64() * 1000.0;
    assert!(storage.last_applied() >= last);

    let mut report = empty_scenario("storage_recovery");
    report.recovery_ms = Some(recovery_ms);
    report.elapsed_ms = recovery_ms;
    // Inventory reflects pre-tail growth; recovery also replays 8 PutKeyRecord entries.
    report.inventory = Some(EntityInventory::from_storage_scale(scale));
    report.notes.push(format!(
        "Recovery = MetaStorage::open_with after reboot (snapshot + log replay); record_count={}",
        storage.state().record_count()
    ));
    report.notes.push(
        "SimStorage path — not a multi-process three-node crash recovery soak.".into(),
    );
    report
}

// ---------------------------------------------------------------------------
// Recommendation gate
// ---------------------------------------------------------------------------

fn evaluate_triggers(scenarios: &[ScenarioReport], scale_name: &str) -> Recommendation {
    let mixed = scenarios
        .iter()
        .find(|s| s.name == "raft_mixed_ops")
        .expect("mixed scenario");
    let snapshot = scenarios
        .iter()
        .find(|s| s.name == "storage_snapshot_growth")
        .expect("snapshot scenario");
    let recovery = scenarios
        .iter()
        .find(|s| s.name == "storage_recovery")
        .expect("recovery scenario");

    let p99 = mixed
        .commit_latency_ms
        .as_ref()
        .map(|l| l.p99)
        .unwrap_or(0.0);
    let snap_bytes = snapshot.snapshot_bytes.unwrap_or(0);
    let snap_ms = snapshot.snapshot_write_ms.unwrap_or(0.0);
    let recovery_ms = recovery.recovery_ms.unwrap_or(0.0);

    // Throughput-plateau proxy: compare mixed cmds/s against heartbeat storm
    // as a cheap same-run relative signal. Full 4× entity scale efficiency is
    // only meaningful across smoke vs extended on one host.
    let hb = scenarios
        .iter()
        .find(|s| s.name == "raft_heartbeat_storm")
        .expect("heartbeat scenario");
    let plateau_ratio = if hb.commands_per_sec > 0.0 {
        mixed.commands_per_sec / hb.commands_per_sec
    } else {
        0.0
    };
    // A "plateau" trip would require scale efficiency < 1.25 when entity load
    // grows 4×. In a single-scale run we only record the relative mixed/hb
    // ratio as an advisory observation (never trips alone).
    let plateau_tripped = false;

    let evaluations = vec![
        TriggerEvaluation {
            id: "p99_commit_ms".into(),
            tripped: p99 > TRIGGER_P99_COMMIT_MS,
            observed: p99,
            threshold: TRIGGER_P99_COMMIT_MS,
            detail: format!(
                "mixed-ops commit p99={p99:.3} ms (threshold {TRIGGER_P99_COMMIT_MS} ms); \
                 requires ≥10 min dedicated soak to count for the gate"
            ),
        },
        TriggerEvaluation {
            id: "throughput_scale_efficiency".into(),
            tripped: plateau_tripped,
            observed: plateau_ratio,
            threshold: TRIGGER_THROUGHPUT_SCALE_EFFICIENCY,
            detail: format!(
                "single-run advisory mixed/hb cmds/s ratio={plateau_ratio:.3}; \
                 gate uses 4× entity scale efficiency < {TRIGGER_THROUGHPUT_SCALE_EFFICIENCY} \
                 across dedicated soaks (smoke vs --ignored on one host)"
            ),
        },
        TriggerEvaluation {
            id: "snapshot_bytes_or_write_ms".into(),
            tripped: snap_bytes > TRIGGER_SNAPSHOT_BYTES || snap_ms > TRIGGER_SNAPSHOT_WRITE_MS,
            observed: snap_bytes as f64,
            threshold: TRIGGER_SNAPSHOT_BYTES as f64,
            detail: format!(
                "snapshot_bytes={snap_bytes} write_ms={snap_ms:.3} \
                 (thresholds {TRIGGER_SNAPSHOT_BYTES} bytes or {TRIGGER_SNAPSHOT_WRITE_MS} ms)"
            ),
        },
        TriggerEvaluation {
            id: "recovery_ms".into(),
            tripped: recovery_ms > TRIGGER_RECOVERY_MS,
            observed: recovery_ms,
            threshold: TRIGGER_RECOVERY_MS,
            detail: format!(
                "recovery_ms={recovery_ms:.3} (threshold {TRIGGER_RECOVERY_MS} ms)"
            ),
        },
    ];

    let tripped = evaluations.iter().filter(|e| e.tripped).count();
    // Gate requires dedicated soak context; CI/smoke never opens sharding.
    let soak_credible = scale_name == "extended";
    let pursue = soak_credible && tripped >= 2;

    Recommendation {
        default_path: "single_three_node_metadata_group".into(),
        pursue_multi_group_sharding: pursue,
        tripped_criteria: tripped,
        required_criteria: 2,
        gate: "Open multi-group design spike only when ≥2 criteria trip on a dedicated \
               three-node lab soak (not CI). Epic #93 defers sharding until measured need."
            .into(),
        rationale: if pursue {
            "Extended lab run tripped ≥2 quantitative criteria — design spike is justified; \
             still do not implement multi-group in issue #192."
                .into()
        } else {
            "Keep the single three-node metadata group. Lab-limited or single-criterion \
             results are insufficient to justify multi-group sharding."
                .into()
        },
        lab_limits: vec![
            "CI / shared runners produce noisy wall-clock and CPU numbers.".into(),
            "Leader CPU is process-wide getrusage, not a dedicated leader process.".into(),
            "p99 is in-process openraft propose/commit, not remote admin/mTLS RTT.".into(),
            "SimStorage snapshot/recovery omit real disk syscall cost.".into(),
            "Multi-hour soaks and production sharding code remain deferred.".into(),
            format!("Current scale profile: {scale_name}"),
        ],
        evaluations,
    }
}

async fn build_report(scale: WorkloadScale, scale_name: &str) -> HarnessReport {
    let mut scenarios = Vec::new();
    scenarios.push(scenario_mixed(&scale).await);
    scenarios.push(scenario_heartbeats(&scale).await);
    scenarios.push(scenario_cursors(&scale).await);
    scenarios.push(scenario_topic_growth(&scale).await);
    scenarios.push(scenario_segments(&scale).await);
    scenarios.push(scenario_placement(&scale).await);
    scenarios.push(scenario_snapshot(&scale));
    scenarios.push(scenario_recovery(&scale));

    let recommendation = evaluate_triggers(&scenarios, scale_name);
    HarnessReport {
        harness_version: HARNESS_VERSION.into(),
        issue: ISSUE.into(),
        seed: SEED,
        host: HostInfo {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            unix_cpu_samples: cfg!(unix),
        },
        methodology: METHODOLOGY.into(),
        scale: scale_name.into(),
        caveats: vec![
            "Research harness only — does not implement multi-group metadata sharding.".into(),
            "Epic #93: one correct three-node metadata group; shard only after measured need.".into(),
            "Wall-clock / CPU numbers are lab-limited and noisy under CI.".into(),
            "Foundation context: meta issues #167 / #171 / #174.".into(),
        ],
        non_goals: vec![
            "No multi-group / sharded metadata Raft implementation in issue #192.".into(),
            "No production capacity claims from CI timings.".into(),
            "No multi-hour dedicated-hardware soak in this PR (harness is the entry point)."
                .into(),
        ],
        related: vec![
            "https://github.com/allamiro/vtop-engine/issues/192".into(),
            "https://github.com/allamiro/vtop-engine/issues/93".into(),
            "https://github.com/allamiro/vtop-engine/issues/167".into(),
            "https://github.com/allamiro/vtop-engine/issues/171".into(),
            "https://github.com/allamiro/vtop-engine/issues/174".into(),
        ],
        scenarios,
        recommendation,
    }
}

fn maybe_write_json(report: &HarnessReport) {
    let Ok(raw) = std::env::var("VTOP_META_SATURATION_JSON") else {
        return;
    };
    let path = PathBuf::from(&raw);
    let path = if path.is_absolute() {
        path
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path)
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create JSON output directory");
    }
    let body = serde_json::to_string_pretty(report).expect("serialize report");
    fs::write(&path, body).expect("write JSON report");
    eprintln!("wrote meta-saturation report to {}", path.display());
}

fn assert_report_sane(report: &HarnessReport) {
    let required = [
        "raft_mixed_ops",
        "raft_heartbeat_storm",
        "raft_cursor_commits",
        "raft_topic_range_growth",
        "raft_segment_registration",
        "raft_placement_updates",
        "storage_snapshot_growth",
        "storage_recovery",
    ];
    for name in required {
        let scenario = report
            .scenarios
            .iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("missing scenario {name}"));
        assert_eq!(scenario.status, "measured", "{name} not measured");
    }

    for scenario in &report.scenarios {
        if scenario.name.starts_with("raft_") {
            assert!(
                scenario.commands > 0,
                "{} recorded zero commands",
                scenario.name
            );
            assert!(
                scenario.commands_per_sec > 0.0,
                "{} commands/s was zero",
                scenario.name
            );
            let p99 = scenario
                .commit_latency_ms
                .as_ref()
                .map(|l| l.p99)
                .unwrap_or(0.0);
            assert!(p99 >= 0.0, "{} p99 negative", scenario.name);
        }
    }

    let snap = report
        .scenarios
        .iter()
        .find(|s| s.name == "storage_snapshot_growth")
        .unwrap();
    assert!(snap.snapshot_bytes.unwrap_or(0) > 0, "snapshot empty");
    assert!(
        snap.snapshot_write_ms.unwrap_or(0.0) >= 0.0,
        "snapshot write ms missing"
    );

    let recovery = report
        .scenarios
        .iter()
        .find(|s| s.name == "storage_recovery")
        .unwrap();
    assert!(
        recovery.recovery_ms.unwrap_or(-1.0) >= 0.0,
        "recovery ms missing"
    );

    assert_eq!(
        report.recommendation.default_path, "single_three_node_metadata_group",
        "default path must remain the single three-node group"
    );
    if report.scale == "smoke" {
        assert!(
            !report.recommendation.pursue_multi_group_sharding,
            "smoke/CI must not open the sharding gate"
        );
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn metadata_saturation_research_smoke() {
    let report = build_report(SMOKE, "smoke").await;
    maybe_write_json(&report);
    assert_report_sane(&report);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore = "extended lab sample; same JSON schema as smoke"]
async fn metadata_saturation_research_extended() {
    let report = build_report(EXTENDED, "extended").await;
    maybe_write_json(&report);
    assert_report_sane(&report);
}

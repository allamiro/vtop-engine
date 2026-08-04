//! Live-cluster metadata node assembly (#215).
//!
//! Everything here existed as library pieces — durable store, TLS Raft
//! network, peer/admin transports — but only tests ever wired them together,
//! always in-process and on simulated disk. This module is the one sanctioned
//! place that assembles a *real* node: real fsync'd disk via
//! [`vtop_log::env::Env::real`], real mTLS TCP between processes, live runtime
//! election timers. `vtop-node` (the live-chaos harness binary) calls this so
//! openraft types stay confined to `crates/vtop-meta/src/raft/`.

use super::consensus::OpenraftConsensus;
use super::log_store::MetaRaftLogStore;
use super::network::{PeerDirectory, RaftPeerHandler, TlsRaftNetworkFactory};
use super::state_machine::MetaRaftStateMachine;
use super::store::MetaRaftStore;
use crate::storage::MetaStorageConfig;
use crate::transport::tls::TlsMaterial;
use openraft::{Config, Raft};
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;
use vtop_log::env::Env;

/// Raft timers for a live node. The runtime implements these with monotonic
/// deadlines; realtime clock skew must not affect elections.
#[derive(Clone, Copy, Debug)]
pub struct MetaNodeTimers {
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
}

impl Default for MetaNodeTimers {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 600,
            heartbeat_interval_ms: 60,
        }
    }
}

/// Handles a live node process needs; openraft stays behind them.
pub struct MetaNode {
    /// Serve this through [`crate::transport::admin::AdminServer`] and use it
    /// for proposals; it also answers init/add-learner/change-membership.
    pub consensus: Arc<OpenraftConsensus>,
    /// Serve this through [`crate::transport::peer::PeerServer`].
    pub peer_handler: Arc<RaftPeerHandler>,
}

/// Open (or recover) the durable store at `data_dir` and start a Raft node
/// that reaches peers through `directory` over mTLS.
///
/// The node starts idle: a fresh cluster is formed once via the admin
/// `init` RPC on exactly one node, after which membership is Raft state.
pub async fn start_meta_node(
    env: &Env,
    data_dir: impl AsRef<Path>,
    cluster_id: Uuid,
    node_id: u64,
    directory: PeerDirectory,
    material: TlsMaterial,
    timers: MetaNodeTimers,
) -> Result<MetaNode, String> {
    let store = MetaRaftStore::open(env, data_dir, cluster_id, MetaStorageConfig::default())
        .map_err(|error| format!("open meta store: {error}"))?;
    let log_store = MetaRaftLogStore::new(store.clone());
    let state_machine = MetaRaftStateMachine::new(store);
    let network = TlsRaftNetworkFactory::new(node_id, directory, material);
    let config = Arc::new(
        Config {
            cluster_name: "vtop-meta-raft".into(),
            election_timeout_min: timers.election_timeout_min_ms,
            election_timeout_max: timers.election_timeout_max_ms,
            heartbeat_interval: timers.heartbeat_interval_ms,
            ..Config::default()
        }
        .validate()
        .map_err(|error| format!("raft config: {error}"))?,
    );
    let raft = Raft::new(node_id, config, network, log_store, state_machine)
        .await
        .map_err(|error| format!("start raft node {node_id}: {error}"))?;
    Ok(MetaNode {
        consensus: Arc::new(OpenraftConsensus::new(raft.clone())),
        peer_handler: Arc::new(RaftPeerHandler::new(raft)),
    })
}

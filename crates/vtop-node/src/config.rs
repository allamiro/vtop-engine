//! YAML configs for live-cluster nodes (#215).
//!
//! Deliberately minimal: this is the chaos-validation harness surface, not a
//! production operator config. Every field maps 1:1 onto an existing library
//! type (`PeerDirectory`, `NetworkFollowerConfig`, `RangeIdentity`, …).

use serde::Deserialize;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use vtop_protocol::RangeIdentity;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsPaths {
    pub ca: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}

/// One metadata Raft peer as seen from this node.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetaPeerConfig {
    pub id: u64,
    /// `host:port` of the peer's Raft listener.
    pub addr: String,
    /// rustls server name the peer's certificate carries as SAN.
    pub server_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetaTimersConfig {
    #[serde(default = "default_election_min")]
    pub election_timeout_min_ms: u64,
    #[serde(default = "default_election_max")]
    pub election_timeout_max_ms: u64,
    #[serde(default = "default_heartbeat")]
    pub heartbeat_interval_ms: u64,
}

fn default_election_min() -> u64 {
    300
}
fn default_election_max() -> u64 {
    600
}
fn default_heartbeat() -> u64 {
    60
}

impl Default for MetaTimersConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: default_election_min(),
            election_timeout_max_ms: default_election_max(),
            heartbeat_interval_ms: default_heartbeat(),
        }
    }
}

/// A metadata Raft node process.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetaNodeConfig {
    /// Decimal Raft node id; must equal the CN of `tls.cert`.
    pub node_id: u64,
    pub cluster_id: Uuid,
    pub data_dir: PathBuf,
    /// Raft peer RPC listener, `host:port`.
    pub peer_listen: String,
    /// Admin endpoint listener (vtopctl meta), `host:port`.
    pub admin_listen: String,
    /// The other members' peer listeners. May include this node; its own
    /// entry is ignored.
    #[serde(default)]
    pub peers: Vec<MetaPeerConfig>,
    pub tls: TlsPaths,
    #[serde(default)]
    pub timers: MetaTimersConfig,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataRole {
    /// Accepts produce/fetch and replicates to `followers`.
    Leader,
    /// Serves the replication protocol for one leader.
    Follower,
    /// Accepts produce/fetch with no replication — used to re-open a killed
    /// node's directory and verify what survived on its local disk.
    Standalone,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeConfig {
    pub topic: String,
    pub topic_epoch: u64,
    pub range_id: Uuid,
    pub range_generation: u64,
}

impl RangeConfig {
    pub fn identity(&self) -> RangeIdentity {
        RangeIdentity {
            topic: self.topic.clone(),
            topic_epoch: self.topic_epoch,
            range_id: self.range_id,
            range_generation: self.range_generation,
        }
    }
}

/// One replication follower as seen from the leader.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowerPeerConfig {
    pub node_uuid: Uuid,
    /// `host:port` of the follower's replica listener.
    pub addr: String,
    pub server_name: String,
}

/// A data-plane node process (one range, mirroring the library harnesses).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataNodeConfig {
    pub role: DataRole,
    /// Broker node UUID; must equal the CN of `replica_tls.cert`.
    pub node_uuid: Uuid,
    pub cluster_id: Uuid,
    pub data_dir: PathBuf,
    pub fencing_epoch: u64,
    pub range: RangeConfig,
    pub segment_id: Uuid,
    /// Follower: replication listener, `host:port`.
    pub replica_listen: Option<String>,
    /// Leader/standalone: produce-fetch listener, `host:port`.
    pub native_listen: Option<String>,
    /// Leader only.
    #[serde(default)]
    pub followers: Vec<FollowerPeerConfig>,
    /// Identity on the replication plane (CN = node_uuid).
    pub replica_tls: TlsPaths,
    /// Leader/standalone: identity + client trust on the produce/fetch plane.
    pub native_tls: Option<TlsPaths>,
    /// Leader/standalone: the one client principal the authorizer accepts.
    pub principal_id: Option<Uuid>,
}

pub fn load<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("read {}: {error}", path.display()))?;
    serde_yaml::from_str(&text).map_err(|error| format!("parse {}: {error}", path.display()))
}

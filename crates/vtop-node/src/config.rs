//! YAML configs for live-cluster nodes (#215).
//!
//! Deliberately minimal: this is the chaos-validation harness surface, not a
//! production operator config. Every field maps 1:1 onto an existing library
//! type (`PeerDirectory`, `NetworkFollowerConfig`, `RangeIdentity`, …).

use serde::Deserialize;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use vtop_meta::transport::AdminAuthorizer;
use vtop_protocol::RangeIdentity;

/// Who may submit which admin commands (#238).
///
/// The YAML shape lives here rather than beside the policy in `vtop-meta`:
/// that crate is the deterministic state machine and keeps serde out of its
/// dependencies so its wire codecs stay hand-rolled and byte-exact.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminAuthorizationConfig {
    /// Common Names permitted to submit cluster-scoped commands — bootstrap,
    /// membership changes, and administrative lease grants naming any holder.
    ///
    /// Empty means no client may change the cluster through this endpoint.
    /// That is a real configuration for a cluster administered out of band,
    /// not an accident to paper over with a fallback — an empty list is
    /// enforced as written.
    #[serde(default)]
    pub operator_common_names: BTreeSet<String>,
}

impl AdminAuthorizationConfig {
    pub fn authorizer(&self) -> AdminAuthorizer {
        AdminAuthorizer::with_operators(self.operator_common_names.iter().cloned())
    }
}

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

/// The node's operational surface (#224).
///
/// Optional: a node with no `observability` block behaves exactly as before,
/// which keeps every existing config file valid. When a `listen` IS given, a
/// bind failure is fatal — the operator asked for this endpoint, and a node
/// that silently came up unscrapeable would pass its own health gate while
/// being invisible to the one thing meant to watch it.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityConfig {
    /// `host:port` for `/metrics`, `/healthz`, and `/readyz`. Bind it to a
    /// private interface: the endpoint is unauthenticated by design (#78).
    pub listen: Option<String>,
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
    /// Who may submit which admin commands (#238).
    ///
    /// `Option` because absence is meaningful and is NOT the same as an empty
    /// policy: absent means "authenticate but do not authorize" — every
    /// CA-signed client may do anything, as before #238 — while
    /// `admin_authorization: {}` means "no operators exist", so nobody may
    /// change the cluster through this endpoint. Enforcing by default would
    /// lock out every deployment on upgrade, including this project's own
    /// chaos harness; a security control that ships broken gets disabled
    /// rather than fixed. The absent case warns at startup so it stays a
    /// deliberate choice instead of an unnoticed default.
    #[serde(default)]
    pub admin_authorization: Option<AdminAuthorizationConfig>,
    /// `Option` so a CO-LOCATED wrapper can tell "absent" from "present but
    /// empty": any per-role block, even `{}`, is a config error there, and
    /// detecting it needs field presence to survive deserialization.
    #[serde(default)]
    pub observability: Option<ObservabilityConfig>,
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
    /// `Option` so a CO-LOCATED wrapper can tell "absent" from "present but
    /// empty": any per-role block, even `{}`, is a config error there, and
    /// detecting it needs field presence to survive deserialization.
    #[serde(default)]
    pub observability: Option<ObservabilityConfig>,
    /// Leader/standalone: drive range leadership from the metadata plane
    /// (#223).
    ///
    /// Optional, and absent by default, so every existing config keeps its
    /// current behaviour: a node with a fixed `fencing_epoch` and no agent.
    /// With it configured the epoch becomes metadata's to decide, and this
    /// process serves only while metadata says it holds the range.
    ///
    /// Valid on FOLLOWERS too, and on a replicated range it is required there
    /// (#239). A follower with this block watches metadata and adopts granted
    /// epochs on its own; a follower without one keeps asserting its static
    /// `fencing_epoch` and will refuse the leader's appends the moment
    /// metadata mints a newer one — fencing that leader out of its own quorum
    /// until someone restarts the follower with a new config.
    ///
    /// The two roles use the block differently. A leader ACQUIRES and RENEWS:
    /// it is competing to hold the range. A follower only OBSERVES: it has no
    /// claim to make, and giving followers an agent would put every replica in
    /// the election. Only `admin_endpoint`, `server_name`, `topic_uuid`,
    /// `tls`, and `poll_interval_ms` are read on a follower.
    ///
    /// Stated limitation on a REPLICATED range. A watching follower starts
    /// fenced and adopts its epoch on the next poll, so it refuses the
    /// leader's appends for up to one `poll_interval_ms` after a grant. Those
    /// refusals put it behind the leader's contiguous sequence, and it rejoins
    /// by way of the leader's resynchronisation (#255) — which can only
    /// retransmit what the leader's buffer still holds. A follower whose gap
    /// exceeds `max_retransmission_bytes`, or that is behind a leader promoted
    /// after the gap opened, cannot be repaired in place and needs its data
    /// directory restored from a peer. Segment transfer removes that
    /// requirement and is tracked on #255; until it lands, keep
    /// `poll_interval_ms` short on replicated ranges so the window stays small.
    pub lease: Option<LeaseConfig>,
}

/// Where to reach the metadata admin endpoint, and how hard to hold the lease.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseConfig {
    /// `host:port` of any metadata node's admin listener.
    pub admin_endpoint: String,
    /// rustls server name the metadata node's certificate carries.
    pub server_name: String,
    /// Topic UUID the range belongs to, as the metadata plane knows it. This
    /// is NOT `range.topic`, which is the wire-level topic name.
    pub topic_uuid: Uuid,
    /// mTLS identity for the admin endpoint. Its CN must be this node's
    /// **`node_uuid`** — the broker's own identity, not a metadata node id.
    ///
    /// This block proposes `AcquireRangeLease`/`RenewRangeLease` naming this
    /// broker as holder, so the credential must identify this broker. Admin
    /// authorization (#238) enforces the match: a node may drive its own
    /// lease and no one else's, and a metadata node's certificate is not a
    /// lease credential at all. This comment previously said "decimal metadata
    /// node id", and the live-chaos harness wired the metadata node's own
    /// certificate here to match — which is exactly the confusion the policy
    /// now rejects.
    pub tls: TlsPaths,
    #[serde(default = "default_lease_duration_ms")]
    pub lease_duration_ms: u64,
    #[serde(default = "default_renew_interval_ms")]
    pub renew_interval_ms: u64,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Every metadata node this range may ask, so a redirect can be followed.
    ///
    /// `admin_endpoint` above is where to ASK FIRST; these are where to go when
    /// that node says it is not the leader. Reads and writes on the metadata
    /// plane must reach the Raft leader, so with one endpoint and no
    /// alternatives a non-leader is a dead end — which is precisely what
    /// happened in Kubernetes, where every pod pointed `admin_endpoint` at its
    /// own co-located metadata node and only the one that happened to
    /// co-locate the leader ever worked (#292).
    ///
    /// Optional, and empty means today's behaviour exactly: one endpoint, no
    /// redirect following. That keeps every single-node and harness config
    /// working untouched, since a single-voter group's only node is always its
    /// leader.
    #[serde(default)]
    pub admin_peers: Vec<LeaseAdminPeer>,
}

/// One metadata node a lease client may be redirected to.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseAdminPeer {
    /// The metadata node id, as Raft knows it. Required, because a redirect
    /// names an id: without it the client can only rotate through peers
    /// hopefully instead of going straight to the leader it was told about.
    pub node_id: u64,
    pub endpoint: String,
    /// rustls server name for this peer's certificate. Empty uses the
    /// `server_name` above, which is correct when a shared SAN is configured
    /// and wrong when certificates are per-pod — so it is per-peer here.
    #[serde(default)]
    pub server_name: String,
}

fn default_lease_duration_ms() -> u64 {
    15_000
}
fn default_renew_interval_ms() -> u64 {
    5_000
}
fn default_poll_interval_ms() -> u64 {
    2_000
}

pub fn load<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("read {}: {error}", path.display()))?;
    serde_yaml::from_str(&text).map_err(|error| format!("parse {}: {error}", path.display()))
}

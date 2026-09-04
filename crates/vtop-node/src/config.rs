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
    /// Starts as neither leader nor follower; the role FOLLOWS THE LEASE
    /// (#284). Every candidate runs the lease agent, campaigns under the
    /// election restriction (#342), serves the replica plane while
    /// following, and serves produce/fetch while holding the range — with
    /// no config change and no restart on failover, which is what a
    /// Kubernetes pod (whose address can never move to another pod)
    /// actually needs. Requires `peers` (the SYMMETRIC replica set,
    /// including this node) and a `lease` block; `followers` must be empty
    /// — the follower list is derived as peers minus self.
    Candidate,
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
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FollowerPeerConfig {
    pub node_uuid: Uuid,
    /// `host:port` of the follower's replica listener.
    pub addr: String,
    pub server_name: String,
}

/// The previous hardcoded roll thresholds, now the defaults.
pub(crate) fn default_max_segment_bytes() -> u64 {
    8 * 1024 * 1024 * 1024
}

pub(crate) fn default_max_segment_records() -> u64 {
    10_000_000
}

pub(crate) fn default_max_group_bytes() -> u64 {
    64 * 1024 * 1024
}

pub(crate) fn default_max_record_bytes() -> u32 {
    16 * 1024 * 1024
}

/// The on-disk segment format a node creates a NEW range in.
///
/// Creation-time only, like the roll thresholds: an existing range keeps
/// the format its tail already has, whatever this says, because the format
/// is a property of the bytes on disk and a config edit cannot change them.
/// The default is v1 so every existing config keeps creating exactly what
/// it did. v2 is the proof-carrying format (#93 stage 4): real producer
/// epochs in every frame, chunk proofs, and — what a replicated range wants
/// it for — the promotion boundary marker (#240), which only a v2 frame can
/// carry under an identity consumers are shielded from. A v1 replicated
/// range keeps the pre-marker publication path.
///
/// Known limit of v2 ranges, stated rather than discovered: the truncation
/// intent marker records a v1 segment identity, so a v2 range that has
/// ROLLED cannot be truncated across segments — a follower whose divergence
/// point lies below its active segment refuses, fail-closed, and comes back
/// through `vtopctl node repair` rather than in place (#429). Divergence
/// inside the active segment reconciles as on v1.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SegmentFormatConfig {
    #[default]
    V1,
    V2,
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
    /// Candidate only: the WHOLE replica set, this node included, identical
    /// on every member — the same shape as the metadata plane's `peers`,
    /// and for the same reason: a symmetric config is one ConfigMap, and a
    /// node's own entry is filtered by `node_uuid` rather than maintained
    /// by hand. The follower list a promotion needs, and the transfer
    /// allowlist a repair needs, are both derived as peers minus self.
    #[serde(default)]
    pub peers: Vec<FollowerPeerConfig>,
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
    /// Bytes a segment may reach before the range rolls to a new one.
    ///
    /// CONFIGURABLE, because segment size is an operational choice and it was
    /// a constant. It decides how much of a range is SEALED — and only sealed
    /// segments transfer, so a deployment that never rolls is one where
    /// `vtopctl node repair` has nothing to move and a lost replica has no
    /// road back. The default is the previous hardcoded 8 GiB, so an existing
    /// config behaves exactly as it did.
    #[serde(default = "default_max_segment_bytes")]
    pub max_segment_bytes: u64,
    /// Records a segment may reach before the range rolls, whichever bound is
    /// reached first.
    #[serde(default = "default_max_segment_records")]
    pub max_segment_records: u64,
    /// Bytes one record group may reach.
    ///
    /// Exposed alongside the segment bound because the two are coupled: the
    /// engine refuses a segment smaller than a group, so lowering the roll
    /// threshold without this simply fails to start. Left at the library
    /// default when unset.
    #[serde(default = "default_max_group_bytes")]
    pub max_group_bytes: u64,
    /// Bytes a single record may reach.
    ///
    /// The third of a coupled set. The engine requires
    /// `max_record_bytes` PLUS FRAME OVERHEAD to fit in `max_group_bytes`,
    /// and `max_group_bytes` to fit in `max_segment_bytes` — so equal values
    /// are refused, and the refusal arrives as a node that will not start with
    /// a message about group sizing, some way from the config that caused it.
    /// Lowering the roll threshold therefore means lowering all three, with
    /// headroom. They were all constants, which made the roll threshold
    /// unreachable in practice.
    #[serde(default = "default_max_record_bytes")]
    pub max_record_bytes: u32,
    /// The segment format a NEW range is created in; see
    /// [`SegmentFormatConfig`]. `v1` (the default) or `v2`.
    #[serde(default)]
    pub segment_format: SegmentFormatConfig,
    /// Node UUIDs allowed to pull this leader's sealed segments, beyond its
    /// own followers.
    ///
    /// A sealed-segment fetch hands over a whole range's bytes, so "any
    /// certificate this cluster trusts" is the wrong granularity for it even
    /// though it is the right one for an append. Followers are allowed
    /// implicitly — they already hold the data. A REPLACEMENT is not: it is
    /// being repaired precisely because it is not yet a follower, so it has to
    /// be named here for the duration of the repair, and removed after.
    ///
    /// Empty by default, which means followers only.
    #[serde(default)]
    pub transfer_peers: Vec<Uuid>,
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
    /// the election. Only `admin_endpoint`, `admin_peers`, `server_name`,
    /// `topic_uuid`, `tls`, and `poll_interval_ms` are read on a follower.
    ///
    /// `admin_peers` is in that list and is not optional in practice on a
    /// replicated range: a follower's watcher must reach the Raft LEADER to read
    /// the lease at all, and under co-location its `admin_endpoint` is its own
    /// node, which usually is not the leader. Omitting the peers is what left
    /// two of three replicas failing closed forever (#292), and it stays wrong
    /// after any leader movement, not just at startup.
    ///
    /// Stated limitation on a REPLICATED range. A watching follower starts
    /// fenced and adopts its epoch on the next poll, so it refuses the
    /// leader's appends for up to one `poll_interval_ms` after a grant. Those
    /// refusals put it behind the leader's contiguous sequence, and it rejoins
    /// by way of the leader's resynchronisation — which can only retransmit
    /// what the leader's buffer still holds. A follower whose gap exceeds
    /// `max_retransmission_bytes`, or that is behind a leader promoted after
    /// the gap opened, cannot catch up in place.
    ///
    /// It is repairable, though, and no longer needs a data directory copied
    /// by hand: `vtopctl node repair --seal-tail` into an EMPTY directory
    /// seals the leader's tail so the transferred prefix reaches the leader's
    /// position, and the replica then catches up from there (#306). Keeping
    /// `poll_interval_ms` short on replicated ranges still narrows the window
    /// in which the gap can open, which is cheaper than repairing it.
    pub lease: Option<LeaseConfig>,
    /// Sealed-prefix retention (#290). Absent = retention disabled, the
    /// previous behaviour: nothing is ever deleted and the range grows until
    /// the disk does not.
    #[serde(default)]
    pub retention: Option<RetentionConfig>,
}

/// What a range keeps on disk before its oldest sealed segments are
/// reclaimed (#290).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionConfig {
    /// Total bytes of encoded record frames (sealed content plus the active
    /// tail) the range may hold. Whole sealed segments are deleted
    /// oldest-first once the bound is exceeded — and only segments wholly
    /// below the acknowledged (cluster-committed) floor are ever eligible,
    /// so unacknowledged data is never reclaimed whatever this says.
    ///
    /// MUST be greater than zero; startup refuses zero rather than guessing
    /// between "reclaim everything" and "disabled". Disabling retention is
    /// spelled by omitting the whole `retention` block.
    pub max_total_bytes: u64,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn data_yaml(extra: &str) -> String {
        format!(
            "role: standalone\n\
             node_uuid: aaaaaaaa-0000-0000-0000-0000000000a1\n\
             cluster_id: aaaaaaaa-0000-0000-0000-0000000000c0\n\
             data_dir: /tmp/vtop-config-test\n\
             fencing_epoch: 1\n\
             range: {{ topic: t, topic_epoch: 1, range_id: aaaaaaaa-0000-0000-0000-0000000000d0, range_generation: 0 }}\n\
             segment_id: aaaaaaaa-0000-0000-0000-0000000000e0\n\
             replica_tls: {{ ca: ca.pem, cert: node.pem, key: node-key.pem }}\n\
             {extra}"
        )
    }

    /// The format knob (#240): absent means v1, exactly what every existing
    /// config created; `v2` is spelled in lowercase like the role, and a
    /// value the binary could not create is refused at parse time rather
    /// than discovered as a range that never starts.
    #[test]
    fn segment_format_defaults_to_v1_and_parses_v2() {
        let default: DataNodeConfig = serde_yaml::from_str(&data_yaml("")).unwrap();
        assert_eq!(default.segment_format, SegmentFormatConfig::V1);
        let v2: DataNodeConfig = serde_yaml::from_str(&data_yaml("segment_format: v2\n")).unwrap();
        assert_eq!(v2.segment_format, SegmentFormatConfig::V2);
        let refused = serde_yaml::from_str::<DataNodeConfig>(&data_yaml("segment_format: v3\n"))
            .expect_err("an unknown format is a config error");
        assert!(refused.to_string().contains("v3"), "{refused}");
    }
}

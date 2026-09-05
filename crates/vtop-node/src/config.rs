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

/// How one plane's listener — and this node's dials on the same plane — is
/// secured (#294). A plane is ONE transport in both directions.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PlaneTransport {
    /// TLS 1.3, mutual. The default, and the only transport a deployment
    /// method selects.
    #[default]
    Tls,
    /// No TLS, loopback only: the listener refuses to bind anywhere else,
    /// because on a plaintext plane whoever reaches the port is whoever
    /// they say they are.
    Plaintext,
    /// No TLS on any interface, acknowledged by name — for a trusted
    /// segment or a sidecar mesh that supplies what this plane gives up.
    PlaintextOnAnyInterface,
}

impl PlaneTransport {
    pub fn is_tls(self) -> bool {
        matches!(self, Self::Tls)
    }
}

/// How this node dials a plane another node serves (#294): the server's
/// exposure is the server's business, so there are only two answers.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ClientTransport {
    #[default]
    Tls,
    Plaintext,
}

impl ClientTransport {
    pub fn is_tls(self) -> bool {
        matches!(self, Self::Tls)
    }
}

/// The TLS paths a `tls` plane needs, or the refusal that names the plane
/// and the knob when they are absent (#294). Asked only on the `tls` arm,
/// so a plaintext plane never needs paths it would not use.
fn tls_paths_for<'a>(
    paths: Option<&'a TlsPaths>,
    plane: &str,
    knob: &str,
    field: &str,
) -> Result<&'a TlsPaths, String> {
    paths.ok_or_else(|| {
        format!(
            "the {plane} plane is `{knob}: tls` (the default) but `{field}` is not set: give it \
             the certificate paths, or set `{knob}: plaintext` to serve it without TLS on \
             loopback"
        )
    })
}

/// Refuse, before anything binds, a `plaintext` plane on a listener that is
/// not a loopback address (#294). The library refuses the same thing at bind
/// time, but a leader's replica-status listener serves from a detached task
/// whose refusal would arrive as a warning after readiness was already
/// announced (review) — so every plane is judged here first, at startup,
/// where a refusal stops the node. A hostname is left to the bind: only a
/// literal address can be judged without resolving it.
pub fn check_plaintext_exposure(
    transport: PlaneTransport,
    listen: &str,
    plane: &str,
    knob: &str,
) -> Result<(), String> {
    if transport != PlaneTransport::Plaintext {
        return Ok(());
    }
    let Ok(address) = listen.parse::<std::net::SocketAddr>() else {
        return Ok(());
    };
    if address.ip().is_loopback() {
        return Ok(());
    }
    Err(format!(
        "`{knob}: plaintext` serves the {plane} plane without TLS on a loopback address only, \
         but its listener is {listen}: bind it to 127.0.0.1, enable TLS, or set `{knob}: \
         plaintext-on-any-interface` if the exposure is genuinely intended"
    ))
}

/// The same judgement on the address a listener actually BOUND (#294,
/// review): a hostname the literal check had to leave alone resolves here,
/// and a listener that serves from a detached task — the leader's
/// replica-status server — is judged before readiness is announced rather
/// than by a warning after it.
pub fn check_plaintext_bound(
    transport: PlaneTransport,
    bound: std::net::SocketAddr,
    plane: &str,
    knob: &str,
) -> Result<(), String> {
    check_plaintext_exposure(transport, &bound.to_string(), plane, knob)
}

/// A role that PROMOTES cannot serve a plaintext replica plane (#294,
/// review): promotion fences the other replicas over that plane, and a
/// plaintext replica endpoint refuses every fence — there is no peer
/// identity to authorize one — so the range could never be taken. That is
/// every candidate, and a leader whose epoch metadata decides (a `lease`)
/// with followers to fence; a static leader, a follower, and a standalone
/// promote nothing. Refused at startup, by name, rather than discovered as
/// an election that never ends or a lease held by a node that never serves.
pub fn refuse_plaintext_promotion(
    role: DataRole,
    leased: bool,
    replicated: bool,
    transport: PlaneTransport,
) -> Result<(), String> {
    if transport.is_tls() {
        return Ok(());
    }
    let promotes = match role {
        DataRole::Candidate => true,
        DataRole::Leader => leased && replicated,
        _ => false,
    };
    if promotes {
        return Err(format!(
            "`role: {role:?}` with {} requires `replica_transport: tls`: promotion fences the \
             other replicas over the replica plane, and a plaintext replica endpoint refuses \
             every fence (it has no peer identity to authorize one), so the range could never \
             be taken. Use a static leader with followers, or TLS, for a plaintext lab",
            if role == DataRole::Candidate {
                "a plaintext replica plane"
            } else {
                "a lease and followers on a plaintext replica plane"
            }
        ));
    }
    Ok(())
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
    /// Transport of the Raft peer plane (#294): `tls` (the default), or
    /// `plaintext` / `plaintext-on-any-interface`. One transport in both
    /// directions — this node's listener and its dials to every peer — so
    /// every member of the group must agree.
    #[serde(default)]
    pub peer_transport: PlaneTransport,
    /// Transport of the admin plane (#294). A plaintext admin plane refuses
    /// an enforcing `admin_authorization`: there is no CN to match.
    #[serde(default)]
    pub admin_transport: PlaneTransport,
    /// Required while any plane is `tls`; may be omitted only when both
    /// planes are plaintext.
    #[serde(default)]
    pub tls: Option<TlsPaths>,
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
    /// Name of the environment variable holding the 32-byte hex key that
    /// signs leadership-transition statements when they are served (#240
    /// item 5). The secret itself is never in this file. Absent means the
    /// statements are served unsigned — stated on the wire as an absent
    /// MAC. Configured but missing at startup is a hard error, never a
    /// silent downgrade to unsigned, matching `manifest_mac_key_env`.
    #[serde(default)]
    pub transition_mac_key_env: Option<String>,
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
    /// Transport of the replica plane (#294): listener and dials alike, so
    /// every member of the range must agree.
    #[serde(default)]
    pub replica_transport: PlaneTransport,
    /// Transport of the native produce/fetch plane (#294). Under plaintext a
    /// session is admitted on the declared principal alone — anything that
    /// can reach the port and knows `principal_id` may produce and consume.
    #[serde(default)]
    pub native_transport: PlaneTransport,
    /// Required while `replica_transport` is `tls`.
    #[serde(default)]
    pub replica_tls: Option<TlsPaths>,
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
    /// How this node dials the metadata admin plane (#294): `tls` (the
    /// default) or `plaintext`, matching the metadata tier's
    /// `admin_transport`.
    #[serde(default)]
    pub transport: ClientTransport,
    /// Required while `transport` is `tls`.
    #[serde(default)]
    pub tls: Option<TlsPaths>,
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
    /// This peer's admin transport (#294), when it differs from
    /// `lease.transport`: a metadata tier migrated one node at a time is
    /// mixed for the duration, and the leader may be on either side of it.
    /// Unset inherits the lease's.
    #[serde(default)]
    pub transport: Option<ClientTransport>,
}

impl LeaseAdminPeer {
    /// The transport this peer is dialed with, given the lease's own.
    pub fn transport_or(&self, default: ClientTransport) -> ClientTransport {
        self.transport.unwrap_or(default)
    }
}

/// The refusal when `lease.transport` is plaintext but a redirect peer is
/// dialed with TLS and `lease.tls` is absent: it names the peers, since the
/// lease's own setting is not what asked for the material.
pub fn lease_tls_missing_for_peers(peers: &[LeaseAdminPeer]) -> String {
    let peers: Vec<String> = peers
        .iter()
        .filter(|peer| peer.transport_or(ClientTransport::Plaintext).is_tls())
        .map(|peer| peer.node_id.to_string())
        .collect();
    format!(
        "`lease.tls` is required: `lease.transport` is plaintext, but admin peer(s) {} are dialed \
         with `transport: tls`; give the certificate paths, or set those peers to plaintext",
        peers.join(", ")
    )
}

/// An enforcing admin policy on a plaintext admin plane can never apply —
/// there is no certificate, so no caller identity to match — and
/// `AdminServer::plaintext` refuses it. Judged HERE too, from the config
/// alone (review): the server is built after Raft has started, and a node
/// that can never expose its admin endpoint must fail before it campaigns.
pub fn refuse_unenforceable_admin_policy(
    transport: PlaneTransport,
    policy_present: bool,
) -> Result<(), String> {
    if matches!(transport, PlaneTransport::Tls) || !policy_present {
        return Ok(());
    }
    let knob = match transport {
        PlaneTransport::Tls => unreachable!("returned above"),
        PlaneTransport::Plaintext => "plaintext",
        PlaneTransport::PlaintextOnAnyInterface => "plaintext-on-any-interface",
    };
    Err(format!(
        "`admin_transport: {knob}` cannot enforce `admin_authorization`: without a client \
         certificate there is no caller identity to match, so the policy would be a fiction. \
         Remove the policy (a plaintext admin plane permits every reachable peer, and says so at \
         startup), or serve the plane with `admin_transport: tls`"
    ))
}

/// Whether dialing the admin plane needs TLS material at all (#294): when the
/// lease's own transport is TLS, or any redirect peer's is.
pub fn lease_needs_tls(transport: ClientTransport, peers: &[LeaseAdminPeer]) -> bool {
    transport.is_tls()
        || peers
            .iter()
            .any(|peer| peer.transport_or(transport).is_tls())
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

impl MetaNodeConfig {
    /// The TLS paths for a `tls` plane, by the plane's name (#294).
    pub fn tls_for(&self, plane: &str) -> Result<&TlsPaths, String> {
        tls_paths_for(
            self.tls.as_ref(),
            plane,
            &format!("{plane}_transport"),
            "tls",
        )
    }
}

impl DataNodeConfig {
    /// The replica plane's TLS paths (#294); asked only when it is a `tls`
    /// plane.
    pub fn replica_tls_paths(&self) -> Result<&TlsPaths, String> {
        tls_paths_for(
            self.replica_tls.as_ref(),
            "replica",
            "replica_transport",
            "replica_tls",
        )
    }

    /// The native plane's TLS paths (#294); `role` names who is asking,
    /// since only a leader or candidate serves it.
    pub fn native_tls_paths(&self, role: &str) -> Result<&TlsPaths, String> {
        self.native_tls.as_ref().ok_or_else(|| {
            format!(
                "{role} requires native_tls while `native_transport: tls` (the default); set \
                 the paths, or `native_transport: plaintext` for a loopback lab"
            )
        })
    }
}

impl LeaseConfig {
    /// TLS material is needed when ANY candidate is dialed with it (review).
    pub fn needs_tls(&self) -> bool {
        lease_needs_tls(self.transport, &self.admin_peers)
    }

    /// The paths for dialing the admin plane under TLS (#294).
    pub fn tls_paths(&self) -> Result<&TlsPaths, String> {
        if self.transport.is_tls() {
            return tls_paths_for(
                self.tls.as_ref(),
                "lease admin",
                "lease.transport",
                "lease.tls",
            );
        }
        // The lease itself is plaintext; a redirect peer is not (review): the
        // refusal names that peer, not a setting the lease already has.
        self.tls
            .as_ref()
            .ok_or_else(|| lease_tls_missing_for_peers(&self.admin_peers))
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;

    /// #294 (review): a redirect peer may be on the other side of a rolling
    /// transport migration, and the lease client keeps TLS material as long
    /// as any peer needs it.
    /// A policy no plaintext plane can enforce is refused from the config
    /// alone (review), before Raft could start.
    #[test]
    fn an_admin_policy_on_a_plaintext_admin_plane_is_refused_by_the_config() {
        use PlaneTransport::*;
        assert!(refuse_unenforceable_admin_policy(Tls, true).is_ok());
        assert!(refuse_unenforceable_admin_policy(Plaintext, false).is_ok());
        let refused = refuse_unenforceable_admin_policy(Plaintext, true).unwrap_err();
        assert!(
            refused.contains("`admin_transport: plaintext`")
                && refused.contains("`admin_transport: tls`"),
            "{refused}"
        );
        assert!(
            refuse_unenforceable_admin_policy(PlaintextOnAnyInterface, true)
                .unwrap_err()
                .contains("plaintext-on-any-interface")
        );
    }

    /// The refusal blames the peer, not the lease (review).
    #[test]
    fn a_missing_lease_tls_is_blamed_on_the_peer_that_needs_it() {
        let peer = |transport| LeaseAdminPeer {
            node_id: 2,
            endpoint: "127.0.0.1:9201".to_owned(),
            server_name: String::new(),
            transport,
        };
        let refusal = lease_tls_missing_for_peers(&[peer(None), peer(Some(ClientTransport::Tls))]);
        assert!(
            refusal.contains("admin peer(s) 2")
                && refusal.contains("`lease.transport` is plaintext"),
            "{refusal}"
        );
    }

    #[test]
    fn a_lease_peer_may_name_its_own_transport_and_material_is_kept_while_any_needs_it() {
        let peer = |transport| LeaseAdminPeer {
            node_id: 2,
            endpoint: "127.0.0.1:9201".to_owned(),
            server_name: String::new(),
            transport,
        };
        assert_eq!(
            peer(None).transport_or(ClientTransport::Plaintext),
            ClientTransport::Plaintext
        );
        assert_eq!(
            peer(Some(ClientTransport::Tls)).transport_or(ClientTransport::Plaintext),
            ClientTransport::Tls
        );
        assert!(!lease_needs_tls(ClientTransport::Plaintext, &[peer(None)]));
        assert!(lease_needs_tls(
            ClientTransport::Plaintext,
            &[peer(Some(ClientTransport::Tls))]
        ));
        assert!(lease_needs_tls(
            ClientTransport::Tls,
            &[peer(Some(ClientTransport::Plaintext))]
        ));
    }

    /// The default is TLS on every plane, spelled or not; the knobs take
    /// kebab-case; a `tls` plane without paths is refused by name.
    #[test]
    fn plane_transports_default_to_tls_and_refuse_a_tls_plane_without_paths() {
        let config: MetaNodeConfig = serde_yaml::from_str(
            r#"
node_id: 1
cluster_id: 11111111-2222-3333-4444-555555555555
data_dir: /tmp/x
peer_listen: 127.0.0.1:9100
admin_listen: 127.0.0.1:9200
tls: { ca: ca.pem, cert: c.pem, key: k.pem }
"#,
        )
        .unwrap();
        assert_eq!(config.peer_transport, PlaneTransport::Tls);
        assert_eq!(config.admin_transport, PlaneTransport::Tls);
        assert!(config.tls_for("peer").is_ok());

        let plaintext: MetaNodeConfig = serde_yaml::from_str(
            r#"
node_id: 1
cluster_id: 11111111-2222-3333-4444-555555555555
data_dir: /tmp/x
peer_listen: 127.0.0.1:9100
admin_listen: 0.0.0.0:9200
peer_transport: plaintext
admin_transport: plaintext-on-any-interface
"#,
        )
        .unwrap();
        assert_eq!(plaintext.peer_transport, PlaneTransport::Plaintext);
        assert_eq!(
            plaintext.admin_transport,
            PlaneTransport::PlaintextOnAnyInterface
        );
        assert!(plaintext.tls.is_none(), "no plane needs paths");

        let half: MetaNodeConfig = serde_yaml::from_str(
            r#"
node_id: 1
cluster_id: 11111111-2222-3333-4444-555555555555
data_dir: /tmp/x
peer_listen: 127.0.0.1:9100
admin_listen: 127.0.0.1:9200
peer_transport: plaintext
"#,
        )
        .unwrap();
        let refusal = half
            .tls_for("admin")
            .expect_err("a tls plane without paths is a configuration error");
        assert!(
            refusal.contains("admin plane") && refusal.contains("admin_transport"),
            "{refusal}"
        );
    }

    /// A plaintext plane off loopback is refused at startup, naming the knob
    /// and the ways out; loopback, TLS, an acknowledged exposure, and a
    /// hostname the bind will judge all pass.
    #[test]
    fn a_plaintext_plane_off_loopback_is_refused_before_anything_binds() {
        use PlaneTransport::*;
        assert!(check_plaintext_exposure(
            Plaintext,
            "127.0.0.1:9300",
            "replica",
            "replica_transport"
        )
        .is_ok());
        assert!(
            check_plaintext_exposure(Plaintext, "[::1]:9300", "replica", "replica_transport")
                .is_ok()
        );
        assert!(
            check_plaintext_exposure(Tls, "0.0.0.0:9300", "replica", "replica_transport").is_ok()
        );
        assert!(check_plaintext_exposure(
            PlaintextOnAnyInterface,
            "0.0.0.0:9300",
            "replica",
            "replica_transport"
        )
        .is_ok());
        assert!(
            check_plaintext_exposure(
                Plaintext,
                "vtop-0.vtop-headless:9300",
                "replica",
                "replica_transport"
            )
            .is_ok(),
            "a name is judged by the bind, which still refuses"
        );
        let refusal =
            check_plaintext_exposure(Plaintext, "0.0.0.0:9300", "replica", "replica_transport")
                .expect_err("an unspecified address is not loopback");
        assert!(
            refusal.contains("replica_transport")
                && refusal.contains("0.0.0.0:9300")
                && refusal.contains("plaintext-on-any-interface"),
            "{refusal}"
        );
    }

    /// A bound address is judged like a literal one, and a candidate on a
    /// plaintext replica plane is refused by name.
    #[test]
    fn a_bound_address_is_judged_and_a_plaintext_candidate_is_refused() {
        let loopback: std::net::SocketAddr = "127.0.0.1:9300".parse().unwrap();
        let exposed: std::net::SocketAddr = "0.0.0.0:9300".parse().unwrap();
        assert!(check_plaintext_bound(
            PlaneTransport::Plaintext,
            loopback,
            "replica",
            "replica_transport"
        )
        .is_ok());
        assert!(check_plaintext_bound(
            PlaneTransport::Plaintext,
            exposed,
            "replica",
            "replica_transport"
        )
        .unwrap_err()
        .contains("0.0.0.0:9300"));
        use PlaneTransport::{Plaintext, PlaintextOnAnyInterface, Tls};
        assert!(refuse_plaintext_promotion(DataRole::Candidate, true, true, Tls).is_ok());
        assert!(refuse_plaintext_promotion(DataRole::Follower, true, true, Plaintext).is_ok());
        assert!(
            refuse_plaintext_promotion(DataRole::Candidate, false, false, Plaintext)
                .unwrap_err()
                .contains("fence")
        );
        assert!(refuse_plaintext_promotion(
            DataRole::Candidate,
            false,
            false,
            PlaintextOnAnyInterface
        )
        .is_err());
        // A leased, replicated leader promotes too (review); a static one or
        // a leased standalone does not.
        assert!(
            refuse_plaintext_promotion(DataRole::Leader, true, true, Plaintext)
                .unwrap_err()
                .contains("lease and followers")
        );
        assert!(refuse_plaintext_promotion(DataRole::Leader, false, true, Plaintext).is_ok());
        assert!(refuse_plaintext_promotion(DataRole::Leader, true, false, Plaintext).is_ok());
    }

    /// The lease client's dial is its own knob, and a data node's two planes
    /// are independent of each other and of it.
    #[test]
    fn data_and_lease_transports_are_independent_knobs() {
        let lease: LeaseConfig = serde_yaml::from_str(
            r#"
admin_endpoint: 127.0.0.1:9200
server_name: meta
topic_uuid: aaaaaaaa-0000-0000-0000-0000000000f1
transport: plaintext
"#,
        )
        .unwrap();
        assert_eq!(lease.transport, ClientTransport::Plaintext);
        let lease_tls: LeaseConfig = serde_yaml::from_str(
            r#"
admin_endpoint: 127.0.0.1:9200
server_name: meta
topic_uuid: aaaaaaaa-0000-0000-0000-0000000000f1
"#,
        )
        .unwrap();
        assert!(lease_tls
            .tls_paths()
            .expect_err("tls by default, and no paths")
            .contains("lease.tls"));

        let data: DataNodeConfig = serde_yaml::from_str(
            r#"
role: follower
node_uuid: aaaaaaaa-0000-0000-0000-0000000000a1
cluster_id: 11111111-2222-3333-4444-555555555555
data_dir: /tmp/x
fencing_epoch: 0
range: { topic: t, topic_epoch: 1, range_id: aaaaaaaa-0000-0000-0000-0000000000c1, range_generation: 0 }
segment_id: aaaaaaaa-0000-0000-0000-0000000000d1
replica_listen: 127.0.0.1:9300
replica_transport: plaintext
native_transport: plaintext-on-any-interface
"#,
        )
        .unwrap();
        assert_eq!(data.replica_transport, PlaneTransport::Plaintext);
        assert_eq!(
            data.native_transport,
            PlaneTransport::PlaintextOnAnyInterface
        );
        assert!(data.replica_tls.is_none() && data.native_tls.is_none());
        assert!(data
            .native_tls_paths("leader")
            .expect_err("asked as if it were a tls plane")
            .contains("native_transport"));
    }
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

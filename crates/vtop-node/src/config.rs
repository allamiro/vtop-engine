//! YAML configs for live-cluster nodes (#215).
//!
//! Deliberately minimal: this is the chaos-validation harness surface, not a
//! production operator config. Every field maps 1:1 onto an existing library
//! type (`PeerDirectory`, `NetworkFollowerConfig`, `RangeIdentity`, …).

use serde::Deserialize;
use std::collections::BTreeSet;
use std::net::{IpAddr, SocketAddr};
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
    /// private interface: the endpoint is unauthenticated (#78) unless `tls`
    /// below makes it mutual.
    pub listen: Option<String>,
    /// TLS for the endpoint (#294 slice 4). Absent, the endpoint is
    /// plaintext — the default, since kubelet probes and most scrapers expect
    /// it that way on a private interface. Present, it serves TLS 1.3 with
    /// `cert` and `key`; `client_ca` set makes it MUTUAL, and then only a
    /// scraper holding a certificate under that CA is served — a kubelet
    /// probe, which presents none, is refused at the handshake.
    #[serde(default)]
    pub tls: Option<ObservabilityTls>,
}

/// The Kafka gateway on a data node (#225).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaGatewayConfig {
    /// `host:port` the Kafka listener binds. Loopback unless `any_interface`.
    pub listen: String,
    /// Kafka has no vtop identity: a listener off loopback admits any peer
    /// that can reach it, and must be asked for by name.
    #[serde(default)]
    pub any_interface: bool,
    /// What Metadata tells clients to connect to. Default: the bound address
    /// — right on loopback, wrong behind a NAT or a Service, where these are
    /// set to what clients dial.
    #[serde(default)]
    pub advertised_host: Option<String>,
    #[serde(default)]
    pub advertised_port: Option<u16>,
    /// The Kafka topic name; the range's wire topic unless set.
    #[serde(default)]
    pub topic: Option<String>,
    /// The broker id Metadata reports; the leader of the partition this node
    /// serves.
    #[serde(default = "default_kafka_node_id")]
    pub node_id: i32,
    /// Which Kafka partition this node's range is (#457 slice 3). Zero, and
    /// the only one, unless `partitions` names a topology.
    #[serde(default)]
    pub partition: i32,
    /// Every partition of the topic and the broker leading it, this node
    /// included. EMPTY is the shape every deployment has today: one
    /// partition, this node's, and Metadata says so.
    ///
    /// A Kafka partition is an independent log, which is what a range is:
    /// each entry names another node's range serving the same Kafka topic
    /// name under its own partition index. The gateway refuses every
    /// partition but its own by name, so a client goes to the broker that
    /// leads it.
    #[serde(default)]
    pub partitions: Vec<KafkaPartitionPeer>,
    /// The longest a fetch waits for data, whatever the client asked.
    #[serde(default = "default_kafka_max_fetch_wait_ms")]
    pub max_fetch_wait_ms: u64,
    /// The producer identity the gateway appends under. Default: a UUID
    /// derived from the principal (v5 in the principal's namespace), never
    /// the principal itself — a native client appending as the principal
    /// shares the producer-epoch journal with whoever else uses that UUID,
    /// and the gateway's minted epoch would fence it (review).
    #[serde(default)]
    pub producer_id: Option<Uuid>,
    /// Per-topic backends (#458). Empty is today's shape: one native topic
    /// (`topic`, or the range's wire name). A non-empty list is the catalog
    /// Metadata advertises, and a name in no row is unknown by name.
    #[serde(default)]
    pub topics: Vec<KafkaTopicRoute>,
}

/// One partition of the Kafka topic and the broker that leads it (#457
/// slice 3).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct KafkaPartitionPeer {
    /// The Kafka partition index this broker leads.
    pub partition: i32,
    /// The broker id it reports as, matching that node's `kafka.node_id`.
    pub node_id: i32,
    /// What clients dial for it: its advertised host and port.
    pub host: String,
    pub port: u16,
    /// The vtop range this partition is, when this node coordinates a group
    /// that commits it (#457 slice 4c). All three together, or none: a
    /// coordinator that does not know the range cannot store a cursor on it.
    #[serde(default)]
    pub topic_uuid: Option<Uuid>,
    #[serde(default)]
    pub range_uuid: Option<Uuid>,
    #[serde(default)]
    pub topic_epoch: Option<u64>,
}

/// One Kafka topic name and the backend that answers it (#458).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct KafkaTopicRoute {
    /// The name a Kafka client produces and consumes.
    pub name: String,
    /// `native`, `kafka`, `dual`, or `shadow`.
    pub backend: String,
    /// Bootstrap brokers for an external cluster (`kafka`, `dual`, `shadow`).
    #[serde(default)]
    pub brokers: Vec<String>,
    /// The topic name on that cluster, if different from `name`.
    #[serde(default)]
    pub remote_topic: Option<String>,
    /// Where dual/shadow reads are served: `native`, `kafka`, or `compare`.
    #[serde(default)]
    pub read: Option<String>,
    /// JSONL path for dual-write / shadow-read receipts. Memory only if unset.
    #[serde(default)]
    pub receipts: Option<String>,
    /// Translate OffsetFetch from the shadow numbering onto native, using
    /// `receipts` (#458). Set when this topic's read side has switched to
    /// native after a dual-write, so a consumer that committed Kafka
    /// offsets resumes without replay or loss. Requires `receipts`.
    #[serde(default)]
    pub cutover: bool,
}

/// The partition topology a gateway may serve (#457 slice 3), or a refusal
/// naming what is wrong with it. An empty list is the single-partition shape
/// and always fine; a list must name every partition from zero once, and must
/// name THIS node's partition as this node's broker id — a topology that
/// points a client at the wrong broker is worse than none, because the client
/// believes it.
impl KafkaGatewayConfig {
    /// Whether Metadata has a host to give clients for this node's partition
    /// (review). A non-empty topology is AUTHORITATIVE — the gateway reads
    /// its own entry there and never looks at `advertised_host` — so a
    /// deployment that binds every interface and names its host in the
    /// topology has said what to advertise, and must not be made to say it
    /// twice under a key that will be ignored. With no topology, the override
    /// is the only answer there is.
    pub fn advertises_a_host(&self, partition: i32) -> bool {
        match self
            .partitions
            .iter()
            .find(|peer| peer.partition == partition)
        {
            Some(mine) => !names_no_host(&mine.host),
            None => self
                .advertised_host
                .as_deref()
                .is_some_and(|host| !names_no_host(host)),
        }
    }
}

pub fn kafka_partitions(kafka: &KafkaGatewayConfig) -> Result<(), String> {
    if kafka.partitions.is_empty() {
        if kafka.partition != 0 {
            return Err(format!(
                "`kafka.partition: {}` names a partition with no `kafka.partitions` topology to \
                 place it in: a gateway that is not partition 0 must say where the others are",
                kafka.partition
            ));
        }
        return Ok(());
    }
    let mut indexes: Vec<i32> = kafka.partitions.iter().map(|p| p.partition).collect();
    indexes.sort_unstable();
    let expected: Vec<i32> = (0..kafka.partitions.len() as i32).collect();
    if indexes != expected {
        return Err(format!(
            "`kafka.partitions` must name every partition from 0 to {} exactly once, got {indexes:?}",
            kafka.partitions.len() - 1
        ));
    }
    // Every peer a client is told to dial is judged the way this node's own
    // advertised endpoint is (review): a blank host, port zero or a negative
    // broker id in Metadata is a client that bootstraps and then connects to
    // nothing, and these values are served verbatim.
    for peer in &kafka.partitions {
        // A wildcard host is a blank one wearing a costume, and a padded
        // host is one that only looks dialable here (review): the topology
        // is copied into Metadata verbatim.
        refuse_undialable_host(
            &peer.host,
            &format!(
                "`kafka.partitions` gives partition {} a host that",
                peer.partition
            ),
        )?;
        if peer.port == 0 {
            return Err(format!(
                "`kafka.partitions` gives partition {} port 0: set it to the port clients dial",
                peer.partition
            ));
        }
        if !(0..=255).contains(&peer.node_id) {
            // The same bound this node's own id has (review): a producer
            // identity encodes the broker id in one byte, so a topology
            // naming 256 points a partition at a broker that cannot start
            // with the configuration that would serve it.
            return Err(format!(
                "`kafka.partitions` gives partition {} the broker id {}: a broker id is 0 to 255, \
                 the range a node can run with",
                peer.partition, peer.node_id
            ));
        }
    }
    // One partition per broker id (review). In Kafka at large a broker leads
    // many partitions, and this rule is not that: it is what THIS gateway can
    // honour. One gateway serves one range, so a topology naming one broker
    // for two partitions advertises a leader that answers
    // `NOT_LEADER_OR_FOLLOWER` for one of them — and the client's metadata
    // refresh points it straight back to the same place. An unroutable
    // partition is worse than a refused config, so it is refused here. When a
    // gateway can serve several ranges, this rule relaxes to "one broker id is
    // one endpoint" and no further.
    for (i, peer) in kafka.partitions.iter().enumerate() {
        if let Some(other) = kafka.partitions[i + 1..]
            .iter()
            .find(|seen| seen.node_id == peer.node_id)
        {
            return Err(format!(
                "`kafka.partitions` gives broker {} both partition {} and partition {}: one \
                 gateway serves one range, so the second would have no server. Give each \
                 partition its own broker",
                peer.node_id, peer.partition, other.partition
            ));
        }
    }
    // And one listener at one endpoint (review). The broker-id rule above
    // catches the same topology written one way; this catches it written the
    // other. Compared by what the host MEANS, not how it is spelled
    // (review): DNS is case-insensitive and one IP address has many
    // spellings, so `broker.example` and `BROKER.EXAMPLE`, or `::1` and
    // `0:0:0:0:0:0:0:1`, are one host each and a byte comparison would let
    // both pairs through as two. Two ids at one `host:port`
    // are two names for the process listening there, which serves one range
    // — so the second partition gets
    // `NOT_LEADER_OR_FOLLOWER` and the client's refresh sends it back to the
    // address it just came from. Distinct ids do not make a second server.
    for (i, peer) in kafka.partitions.iter().enumerate() {
        if let Some(other) = kafka.partitions[i + 1..].iter().find(|seen| {
            seen.port == peer.port && canonical_host(&seen.host) == canonical_host(&peer.host)
        }) {
            return Err(format!(
                "`kafka.partitions` puts partition {} and partition {} both at {}:{}: one                  listener is there and it serves one range, so the second would have no server.                  Give each partition its own endpoint",
                peer.partition, other.partition, peer.host, peer.port
            ));
        }
    }
    for peer in &kafka.partitions {
        match (peer.topic_uuid, peer.range_uuid, peer.topic_epoch) {
            (None, None, None) => {}
            (Some(topic_uuid), Some(range_uuid), Some(_)) => {
                if topic_uuid.is_nil() || range_uuid.is_nil() {
                    return Err(format!(
                        "`kafka.partitions` gives partition {} a nil topic or range uuid: a \
                         coordinator cannot store a cursor on a range that does not exist",
                        peer.partition
                    ));
                }
            }
            _ => {
                return Err(format!(
                    "`kafka.partitions` gives partition {} a partial range identity: set \
                     topic_uuid, range_uuid and topic_epoch together, or none of them",
                    peer.partition
                ));
            }
        }
    }
    for (i, peer) in kafka.partitions.iter().enumerate() {
        let Some(range_uuid) = peer.range_uuid else {
            continue;
        };
        if let Some(other) = kafka.partitions[i + 1..]
            .iter()
            .find(|seen| seen.range_uuid == Some(range_uuid))
        {
            return Err(format!(
                "`kafka.partitions` gives partition {} and partition {} the same range {}: a \
                 Kafka partition is a range of its own",
                peer.partition, other.partition, range_uuid
            ));
        }
    }
    let Some(mine) = kafka
        .partitions
        .iter()
        .find(|p| p.partition == kafka.partition)
    else {
        return Err(format!(
            "`kafka.partitions` does not name this node's partition ({})",
            kafka.partition
        ));
    };
    // One answer to "what do clients dial for this node" (review). The
    // topology is authoritative — Metadata serves it verbatim and the
    // coordinator answers from it — so an `advertised_host` or
    // `advertised_port` that disagrees is a second answer that will never be
    // used. An author who wrote both and meant them believes something about
    // this process that is not true, and silently picking the winner would
    // hide that instead of correcting it.
    if let Some(host) = kafka.advertised_host.as_deref() {
        if canonical_host(host) != canonical_host(&mine.host) {
            return Err(format!(
                "`kafka.advertised_host: {host}` disagrees with what `kafka.partitions` gives \
                 partition {}: `{}`. The topology is what clients are told, so set one or the \
                 other, not both",
                kafka.partition, mine.host
            ));
        }
    }
    if let Some(port) = kafka.advertised_port {
        if port != mine.port {
            return Err(format!(
                "`kafka.advertised_port: {port}` disagrees with what `kafka.partitions` gives \
                 partition {}: {}. The topology is what clients are told, so set one or the \
                 other, not both",
                kafka.partition, mine.port
            ));
        }
    }
    if mine.node_id != kafka.node_id {
        return Err(format!(
            "`kafka.partitions` gives partition {} the broker id {}, but this node's \
             `kafka.node_id` is {}: a client told the wrong broker leads a partition believes it",
            kafka.partition, mine.node_id, kafka.node_id
        ));
    }
    Ok(())
}

/// The topic map a gateway may serve (#458), or a refusal naming what is
/// wrong with it. An empty list is today's single native topic and always
/// fine; a list must name each topic once, with a backend, and an external
/// backend must name the brokers it dials.
pub fn kafka_topics(kafka: &KafkaGatewayConfig) -> Result<(), String> {
    if kafka.topics.is_empty() {
        return Ok(());
    }
    let mut seen = BTreeSet::new();
    let mut native_backed = Vec::new();
    let mut remote_logs = BTreeSet::new();
    for route in &kafka.topics {
        if route.name.is_empty() {
            return Err(
                "`kafka.topics` names a topic with no name: every route needs a Kafka name"
                    .to_owned(),
            );
        }
        if !seen.insert(route.name.clone()) {
            return Err(format!(
                "`kafka.topics` names {:?} twice: one name is one backend",
                route.name
            ));
        }
        match route.backend.as_str() {
            "native" => {
                native_backed.push(route.name.clone());
            }
            "kafka" | "dual" | "shadow" => {
                if matches!(route.backend.as_str(), "dual" | "shadow") {
                    native_backed.push(route.name.clone());
                }
                if route.brokers.is_empty() {
                    return Err(format!(
                        "`kafka.topics` gives {:?} backend {:?} with no brokers: set the \
                         cluster this route dials",
                        route.name, route.backend
                    ));
                }
                for broker in &route.brokers {
                    kafka_bootstrap_broker(broker, &route.name)?;
                }
                let remote_topic = route
                    .remote_topic
                    .clone()
                    .unwrap_or_else(|| route.name.clone());
                let mut brokers = route.brokers.clone();
                brokers.sort();
                brokers.dedup();
                if !remote_logs.insert((brokers, remote_topic.clone())) {
                    return Err(format!(
                        "`kafka.topics` gives {:?} the same remote log as another route \
                         ({remote_topic}): two Kafka names would share one producer-sequence \
                         space on that cluster",
                        route.name
                    ));
                }
            }
            other => {
                return Err(format!(
                    "`kafka.topics` gives {:?} backend {other:?}: use native, kafka, dual or shadow",
                    route.name
                ));
            }
        }
        if let Some(read) = route.read.as_deref() {
            if !matches!(read, "native" | "kafka" | "compare") {
                return Err(format!(
                    "`kafka.topics` gives {:?} read {read:?}: use native, kafka or compare",
                    route.name
                ));
            }
            if route.backend == "native" || route.backend == "kafka" {
                return Err(format!(
                    "`kafka.topics` gives {:?} a read side, but backend {:?} has only one side",
                    route.name, route.backend
                ));
            }
        }
        if route.backend == "native"
            && (route.brokers.iter().any(|b| !b.is_empty()) || route.remote_topic.is_some())
        {
            return Err(format!(
                "`kafka.topics` gives {:?} backend native with brokers or a remote topic: a \
                 native route is this node's range",
                route.name
            ));
        }
        if route.cutover {
            if route.receipts.as_deref().is_none_or(str::is_empty) {
                return Err(format!(
                    "`kafka.topics` gives {:?} cutover with no receipts: OffsetFetch cannot \
                     translate a committed Kafka offset without the dual-write log",
                    route.name
                ));
            }
            match route.backend.as_str() {
                "native" | "dual" | "shadow" => {}
                other => {
                    return Err(format!(
                        "`kafka.topics` gives {:?} cutover on backend {other:?}: cutover is \
                         native after a dual-write, or dual/shadow still serving native reads",
                        route.name
                    ));
                }
            }
            if matches!(route.read.as_deref(), Some("kafka") | Some("compare")) {
                return Err(format!(
                    "`kafka.topics` gives {:?} cutover while read is {}: OffsetFetch would \
                     translate a native committed offset as a shadow offset. Cutover is native \
                     numbering; set read: native",
                    route.name,
                    route.read.as_deref().unwrap()
                ));
            }
            if route.backend == "shadow" && route.read.as_deref() != Some("native") {
                return Err(format!(
                    "`kafka.topics` gives {:?} cutover on backend shadow without read: native: \
                     shadow reads default to compare, which is not native numbering",
                    route.name
                ));
            }
        } else if route.receipts.is_some() && matches!(route.backend.as_str(), "native" | "kafka") {
            return Err(format!(
                "`kafka.topics` gives {:?} receipts on backend {:?}: receipts belong on dual \
                 or shadow, or on native with cutover after the switch",
                route.name, route.backend
            ));
        }
    }
    if native_backed.len() > 1 {
        return Err(format!(
            "`kafka.topics` gives {:?} and {:?} this node's one log: idempotent producers keep a \
             sequence space per Kafka name, and this range has one. Give the second name a kafka \
             backend, or wait until sequences are namespaced",
            native_backed[0], native_backed[1]
        ));
    }
    Ok(())
}

/// What this node tells clients to dial, in one place (review). The
/// topology first: Metadata serves it verbatim, so a gateway whose own
/// endpoint came from anywhere else would answer FindCoordinator with an
/// address Metadata never named — the bound `0.0.0.0` of a wildcard
/// listener, which on the client's own machine is the client. Then the
/// override, then the bound address, which is right on loopback and is why
/// a wildcard bind must say something. `kafka_partitions` refuses a topology
/// that disagrees with the override, so the order here settles nothing that
/// was ever in dispute.
pub fn kafka_advertised_endpoint(kafka: &KafkaGatewayConfig, bound: SocketAddr) -> (String, i32) {
    let mine = kafka
        .partitions
        .iter()
        .find(|peer| peer.partition == kafka.partition);
    let host = mine
        .map(|peer| peer.host.clone())
        .or_else(|| kafka.advertised_host.clone())
        .unwrap_or_else(|| bound.ip().to_string());
    let port = mine
        .map(|peer| i32::from(peer.port))
        .or_else(|| kafka.advertised_port.map(i32::from))
        .unwrap_or_else(|| i32::from(bound.port()));
    (host, port)
}

/// The identity the gateway appends under: the configured one, or one
/// derived from the principal. Never the principal (review): the journal
/// keys producer epochs by UUID, and the gateway mints a wall-clock epoch at
/// every start, so a native client appending as the same UUID with ordinary
/// epochs would be refused as fenced the moment Kafka was enabled.
pub fn kafka_producer_id(principal: Uuid, kafka: &KafkaGatewayConfig) -> Result<Uuid, String> {
    match kafka.producer_id {
        Some(id) if id == principal => Err(format!(
            "`kafka.producer_id` is the node's principal ({principal}): the gateway must append \
             under an identity of its own, or its producer epoch fences every native client \
             appending as the principal. Leave it unset to derive one"
        )),
        Some(id) => Ok(id),
        None => Ok(Uuid::new_v5(&principal, b"vtop-kafka-gateway")),
    }
}

/// `true` for a listen address that names every interface: Metadata must
/// then be told what to advertise, because `0.0.0.0` on a client's own host
/// is the client, not this node.
/// A host that names no particular host: blank, or the unspecified address
/// however it is spelled. Parsed rather than matched against a list of
/// spellings (review), because `::`, `::0`, `0:0:0:0:0:0:0:0` and
/// `0.0.0.0` are the same address and a client can dial none of them.
fn names_no_host(host: &str) -> bool {
    let host = host.trim();
    let bare = host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(host);
    bare.is_empty()
        || bare
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_unspecified())
}

/// One spelling of a host, for deciding whether two entries name the same
/// endpoint (review). An IP literal is parsed and printed back, so `::1`,
/// `0:0:0:0:0:0:0:1` and `[::1]` are one host, and a v4-mapped v6 literal is
/// the v4 address it maps to — the same listener either way. Anything else
/// is a DNS name, which is case-insensitive.
///
/// This is a COMPARISON KEY and never what is served: clients receive the
/// string the operator wrote, which is why `refuse_undialable_host` insists
/// that string be dialable as written.
fn canonical_host(host: &str) -> String {
    let bare = host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(host);
    match bare.parse::<IpAddr>() {
        Ok(IpAddr::V6(v6)) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4).to_string(),
            None => IpAddr::V6(v6).to_string(),
        },
        Ok(ip) => ip.to_string(),
        // A trailing dot is the DNS root label made explicit (review):
        // `broker.example.` is the fully qualified spelling of
        // `broker.example`, and a resolver treats them as one name. Exactly
        // one is stripped — a second dot is not a valid name and stays
        // different, so a typo is not silently folded into a real host.
        Err(_) => bare.strip_suffix('.').unwrap_or(bare).to_ascii_lowercase(),
    }
}

/// A name a resolver could accept, judged by SHAPE rather than by the one
/// defect that was noticed most recently (review). Whitespace, an empty
/// label, a label over 63 bytes, a name over 253, a leading or trailing
/// hyphen, a non-ASCII byte: each of these has the same consequence — the
/// string is copied into Metadata and the client cannot resolve it — so
/// they are refused as one class instead of one at a time.
///
/// Underscores are ALLOWED, though a strict reading of RFC 1123 forbids
/// them: Compose service names and some internal zones use them, they
/// resolve in practice, and refusing them here would reject working
/// deployments to enforce a rule nothing else in the path enforces. A
/// non-ASCII name is refused with the remedy, since a resolver wants the
/// punycode form and only the operator can produce it.
fn refuse_unresolvable_name(host: &str, what: &str) -> Result<(), String> {
    let name = host.strip_suffix('.').unwrap_or(host);
    if name.len() > 253 {
        return Err(format!(
            "{what} is {} bytes long: a domain name is at most 253",
            name.len()
        ));
    }
    if !name.is_ascii() {
        return Err(format!(
            "{what} is `{host}`, which is not ASCII: give the punycode form (`xn--…`), which is \
             what a resolver is asked for"
        ));
    }
    for label in name.split('.') {
        if label.is_empty() {
            return Err(format!(
                "{what} is `{host}`, which has an empty label: `a..b` names nothing"
            ));
        }
        if label.len() > 63 {
            return Err(format!(
                "{what} is `{host}`, whose label `{label}` is {} bytes: a label is at most 63",
                label.len()
            ));
        }
        if label.starts_with('-') || label.ends_with('-') {
            return Err(format!(
                "{what} is `{host}`, whose label `{label}` starts or ends with a hyphen, which a \
                 name may not"
            ));
        }
        if !label
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(format!(
                "{what} is `{host}`, whose label `{label}` has a character a host name may not \
                 carry: letters, digits, hyphen and underscore"
            ));
        }
    }
    Ok(())
}

/// A host that will be COPIED INTO Metadata, judged once (review). The
/// topology and the advertised override are both served verbatim, so the
/// string an operator wrote has to be the string a client can use — not one
/// that happens to validate after trimming and then goes out with its
/// whitespace intact. Refused rather than normalised: an operator who wrote
/// a stray space should see it, not have it quietly repaired here and
/// nowhere else.
fn refuse_undialable_host(host: &str, what: &str) -> Result<(), String> {
    // Anywhere in it, not only around it (review): `broker example` is no
    // more resolvable than ` broker.example `, and both are copied into
    // Metadata exactly as written.
    if host.chars().any(char::is_whitespace) {
        return Err(format!(
            "{what} is `{host}`, which contains whitespace: it is served to clients exactly as \
             written, so write the host they dial"
        ));
    }
    // Brackets belong to a URL authority, where they separate a v6 address
    // from its port (review). Metadata carries host and port in separate
    // fields, so a bracketed host is copied into the address a client builds
    // and resolves to nothing — for either family, which is why both are
    // refused rather than only the v4 spelling.
    if host.starts_with('[') || host.ends_with(']') {
        return Err(format!(
            "{what} is `{host}`: brackets separate an address from a port in a URL, and Metadata \
             carries the port in its own field. Write the address without them"
        ));
    }
    if names_no_host(host) {
        return Err(format!(
            "{what} is `{host}`, which names no host a client can dial: set it to the address \
             clients reach that broker at"
        ));
    }
    // An IP literal has already been judged by parsing it; anything else is
    // a name, and is held to what a name may look like.
    let bare = host
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(host);
    if bare.parse::<IpAddr>().is_err() {
        refuse_unresolvable_name(host, what)?;
    }
    Ok(())
}

/// A bootstrap `host:port` (or `[host]:port` for IPv6) a remote route dials.
/// Syntax is judged here so a typo is a refused config, not a produce that
/// discovers `BROKER_NOT_AVAILABLE`.
fn kafka_bootstrap_broker(broker: &str, topic: &str) -> Result<(), String> {
    let what = format!("`kafka.topics` gives {topic:?} a broker");
    let (host, port) = if let Some(rest) = broker.strip_prefix('[') {
        let Some((host, port)) = rest.split_once("]:") else {
            return Err(format!("{what} {broker:?} that is not [host]:port"));
        };
        if host.parse::<std::net::Ipv6Addr>().is_err() {
            return Err(format!(
                "{what} {broker:?}: [host]:port is for an IPv6 literal, not {host:?}"
            ));
        }
        (host, port)
    } else {
        let Some((host, port)) = broker.rsplit_once(':') else {
            return Err(format!("{what} {broker:?} that is not host:port"));
        };
        if host.is_empty() {
            return Err(format!("{what} {broker:?} that is not host:port"));
        }
        if host.contains(':') {
            return Err(format!(
                "{what} {broker:?} that is not host:port: write an IPv6 address as [host]:port"
            ));
        }
        (host, port)
    };
    let parsed: u16 = port
        .parse()
        .map_err(|_| format!("{what} {broker:?} whose port is not a number in 1..=65535"))?;
    if parsed == 0 {
        return Err(format!(
            "{what} {broker:?} with port 0: set the port clients dial"
        ));
    }
    refuse_undialable_host(host, &format!("{what} {broker:?} whose host"))?;
    Ok(())
}

fn listens_on_every_interface(listen: &str) -> bool {
    listen
        .rsplit_once(':')
        .is_some_and(|(host, _)| names_no_host(host))
}

fn default_kafka_node_id() -> i32 {
    1
}

fn default_kafka_max_fetch_wait_ms() -> u64 {
    5_000
}

/// The gateway's own refusals (#225), judged from the config before a port
/// is bound: only a leader or standalone serves one, and its listener stays
/// on loopback unless the config says otherwise by name.
pub fn refuse_kafka_gateway_misuse(
    role: DataRole,
    kafka: Option<&KafkaGatewayConfig>,
) -> Result<(), String> {
    let Some(kafka) = kafka else {
        return Ok(());
    };
    if !matches!(role, DataRole::Leader | DataRole::Standalone) {
        return Err(format!(
            "`kafka` is configured on a {role:?} node: the gateway is served by a leader or \
             standalone only — it holds one broker, and a candidate's changes with the lease \
             (#225)"
        ));
    }
    // An advertised endpoint clients cannot dial is refused before the bind
    // (review): a blank host or port zero in Metadata is a client that
    // bootstraps and then connects to nothing.
    if let Some(host) = kafka.advertised_host.as_deref() {
        // The same judgement the topology's hosts get: this string is served
        // to clients verbatim too, and leaving it unset is the way to say
        // "advertise the bound address".
        refuse_undialable_host(host, "`kafka.advertised_host`")?;
    }
    if kafka.node_id < 0 {
        return Err(format!(
            "`kafka.node_id: {}` is negative: Kafka reads -1 as \"no leader\", and Metadata names \
             this id as the broker, the controller and every partition's leader. Use 0 or above",
            kafka.node_id
        ));
    }
    // The node id is the low byte of every producer id this gateway mints
    // (#457, review): two leaders of one range whose ids differed by 256
    // would mint the same id in the same microsecond, and two clients would
    // share one sequence space. Refused here, and refused again at the mint
    // should a config reach the gateway some other way.
    if kafka.node_id > 255 {
        return Err(format!(
            "`kafka.node_id: {}` is above 255: the id is the low byte of every idempotent \
             producer id this gateway mints (#457), so ids must be 0..=255. Use a smaller id",
            kafka.node_id
        ));
    }
    if kafka.advertised_port == Some(0) {
        return Err(
            "`kafka.advertised_port: 0` is not a port clients can dial: set the port they \
                    reach, or leave it unset to advertise the bound one"
                .to_owned(),
        );
    }
    if listens_on_every_interface(&kafka.listen) && !kafka.advertises_a_host(kafka.partition) {
        return Err(format!(
            "`kafka.listen: {}` binds every interface and nothing says what to advertise: \
             Metadata would tell clients to connect to the unspecified address, which on their \
             own host is themselves. Set `kafka.advertised_host` to what clients dial, or name \
             this node's host in `kafka.partitions` (review)",
            kafka.listen
        ));
    }
    if !kafka.any_interface {
        let loopback = kafka
            .listen
            .rsplit_once(':')
            .map(|(host, _)| host.trim_matches(['[', ']']))
            .is_some_and(|host| host == "127.0.0.1" || host == "localhost" || host == "::1");
        if !loopback {
            return Err(format!(
                "`kafka.listen: {}` is off loopback: Kafka's protocol carries no vtop identity, \
                 so this listener admits any peer that can reach it. Bind it to 127.0.0.1, or \
                 spell out `kafka.any_interface: true` and put a network policy in front of it",
                kafka.listen
            ));
        }
    }
    Ok(())
}

/// The observability endpoint's TLS material (#294 slice 4).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityTls {
    pub cert: PathBuf,
    pub key: PathBuf,
    #[serde(default)]
    pub client_ca: Option<PathBuf>,
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
/// The one limit v2 ranges used to carry here is closed (#429): the
/// truncation intent marker carries the v2 identity — segment generation,
/// creation node, creation epoch, chunk size — so a rolled v2 range
/// truncates across segments exactly as v1 does, and a follower whose
/// divergence point lies below its active segment reconciles in place.
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
    /// A Kafka wire-protocol listener over this range (#225), served by a
    /// leader or standalone node beside the native plane: one topic (the
    /// range's wire topic unless named), one partition, appending as
    /// `principal_id` — the gateway's single native identity. Refused on a
    /// candidate or follower: the bridge holds one broker, and a candidate's
    /// changes with the lease. The listener speaks Kafka's own protocol,
    /// which carries no vtop identity, so it binds loopback only unless
    /// `any_interface` is spelled out.
    #[serde(default)]
    pub kafka: Option<KafkaGatewayConfig>,
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

    /// #225: the gateway is a leader's or standalone's, and stays on
    /// loopback unless asked off it by name.
    #[test]
    fn a_kafka_gateway_is_refused_on_a_candidate_and_off_loopback_unless_named() {
        let kafka = |listen: &str, any_interface| KafkaGatewayConfig {
            listen: listen.to_owned(),
            any_interface,
            advertised_host: None,
            advertised_port: None,
            topic: None,
            node_id: 1,
            partition: 0,
            partitions: Vec::new(),
            max_fetch_wait_ms: 5_000,
            producer_id: None,
            topics: Vec::new(),
        };
        assert!(refuse_kafka_gateway_misuse(DataRole::Leader, None).is_ok());
        assert!(refuse_kafka_gateway_misuse(
            DataRole::Standalone,
            Some(&kafka("127.0.0.1:9092", false))
        )
        .is_ok());
        assert!(
            refuse_kafka_gateway_misuse(DataRole::Leader, Some(&kafka("[::1]:9092", false)))
                .is_ok()
        );
        let refused =
            refuse_kafka_gateway_misuse(DataRole::Candidate, Some(&kafka("127.0.0.1:9092", false)))
                .unwrap_err();
        assert!(
            refused.contains("Candidate") && refused.contains("lease"),
            "{refused}"
        );
        let refused =
            refuse_kafka_gateway_misuse(DataRole::Standalone, Some(&kafka("0.0.0.0:9092", false)))
                .unwrap_err();
        assert!(refused.contains("advertised_host"), "{refused}");
        let refused =
            refuse_kafka_gateway_misuse(DataRole::Standalone, Some(&kafka("10.0.0.5:9092", false)))
                .unwrap_err();
        assert!(refused.contains("any_interface"), "{refused}");
        // Every interface, named, and told what to advertise: served.
        let mut wildcard = kafka("[::]:9092", true);
        assert!(
            refuse_kafka_gateway_misuse(DataRole::Leader, Some(&wildcard))
                .unwrap_err()
                .contains("advertised_host")
        );
        wildcard.advertised_host = Some("broker.example".to_owned());
        assert!(refuse_kafka_gateway_misuse(DataRole::Leader, Some(&wildcard)).is_ok());
        // An endpoint nobody can dial is refused by name (review).
        let mut blank = kafka("127.0.0.1:9092", false);
        blank.advertised_host = Some(String::new());
        assert!(refuse_kafka_gateway_misuse(DataRole::Leader, Some(&blank))
            .unwrap_err()
            .contains("names no host"));
        // And one that only looks dialable until it is served verbatim.
        let mut padded = kafka("127.0.0.1:9092", false);
        padded.advertised_host = Some(" broker.example ".to_owned());
        assert!(refuse_kafka_gateway_misuse(DataRole::Leader, Some(&padded))
            .unwrap_err()
            .contains("whitespace"));
        let mut negative = kafka("127.0.0.1:9092", false);
        negative.node_id = -1;
        assert!(
            refuse_kafka_gateway_misuse(DataRole::Leader, Some(&negative))
                .unwrap_err()
                .contains("node_id: -1")
        );
        let mut wide = kafka("127.0.0.1:9092", false);
        wide.node_id = 256;
        assert!(refuse_kafka_gateway_misuse(DataRole::Leader, Some(&wide))
            .unwrap_err()
            .contains("above 255"));
        let mut widest = kafka("127.0.0.1:9092", false);
        widest.node_id = 255;
        assert!(refuse_kafka_gateway_misuse(DataRole::Leader, Some(&widest)).is_ok());
        let mut zero = kafka("127.0.0.1:9092", false);
        zero.advertised_port = Some(0);
        assert!(refuse_kafka_gateway_misuse(DataRole::Leader, Some(&zero))
            .unwrap_err()
            .contains("advertised_port: 0"));
    }

    /// The gateway's producer identity is never the principal (review): the
    /// journal keys epochs by UUID, and the gateway's minted epoch would
    /// fence every native client appending as the principal.
    /// A partition topology must place this node correctly or not exist at
    /// all (#457 slice 3): a client told the wrong broker leads a partition
    /// believes it, so the config is judged before the listener binds.
    #[test]
    fn a_partition_topology_must_place_this_node() {
        let base = KafkaGatewayConfig {
            listen: "127.0.0.1:9092".to_owned(),
            any_interface: false,
            advertised_host: None,
            advertised_port: None,
            topic: None,
            node_id: 9,
            partition: 0,
            partitions: Vec::new(),
            max_fetch_wait_ms: 5_000,
            producer_id: None,
            topics: Vec::new(),
        };
        // Each peer at its own endpoint: two brokers are two servers, and a
        // topology that says otherwise is refused below on its own.
        let peer = |partition: i32, node_id: i32| KafkaPartitionPeer {
            partition,
            node_id,
            host: format!("h{node_id}"),
            port: 9092,
            topic_uuid: None,
            range_uuid: None,
            topic_epoch: None,
        };
        // No topology: the shape every deployment has today.
        assert!(kafka_partitions(&base).is_ok());
        // A partition without a topology to place it in.
        let orphan = KafkaGatewayConfig {
            partition: 1,
            ..base.clone()
        };
        assert!(kafka_partitions(&orphan).is_err());
        // A well-formed two-partition topology naming this node.
        let good = KafkaGatewayConfig {
            partition: 1,
            partitions: vec![peer(0, 7), peer(1, 9)],
            ..base.clone()
        };
        assert!(kafka_partitions(&good).is_ok());
        // A gap in the indexes.
        let gap = KafkaGatewayConfig {
            partition: 0,
            partitions: vec![peer(0, 9), peer(2, 7)],
            ..base.clone()
        };
        assert!(kafka_partitions(&gap).is_err());
        // A topology that forgets this node's own partition.
        let absent = KafkaGatewayConfig {
            partition: 1,
            partitions: vec![peer(0, 7)],
            ..base.clone()
        };
        assert!(kafka_partitions(&absent).is_err());
        // A topology that gives this node's partition to another broker.
        let stolen = KafkaGatewayConfig {
            partition: 0,
            partitions: vec![peer(0, 7), peer(1, 9)],
            ..base.clone()
        };
        assert!(kafka_partitions(&stolen).is_err());
        // One broker for two partitions: refused while a gateway serves one
        // range, because the second partition would have no server and a
        // client's metadata refresh would point back at the same place.
        let shared = KafkaGatewayConfig {
            partition: 0,
            partitions: vec![peer(0, 9), peer(1, 9)],
            ..base.clone()
        };
        assert!(kafka_partitions(&shared).is_err());
        // And what the gateway is actually built with: the topology, which
        // is what Metadata already told the client, not the wildcard address
        // the listener bound.
        let wildcard_bound: SocketAddr = "0.0.0.0:9092".parse().unwrap();
        let placed = KafkaGatewayConfig {
            listen: "0.0.0.0:9092".to_owned(),
            any_interface: true,
            partition: 1,
            partitions: vec![peer(0, 7), peer(1, 9)],
            ..base.clone()
        };
        assert_eq!(
            kafka_advertised_endpoint(&placed, wildcard_bound),
            ("h9".to_owned(), 9_092),
            "FindCoordinator must not answer the address the listener bound"
        );
        let overridden = KafkaGatewayConfig {
            advertised_host: Some("edge.example".to_owned()),
            advertised_port: Some(19_092),
            ..base.clone()
        };
        assert_eq!(
            kafka_advertised_endpoint(&overridden, wildcard_bound),
            ("edge.example".to_owned(), 19_092)
        );
        let loopback: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        assert_eq!(
            kafka_advertised_endpoint(&base, loopback),
            ("127.0.0.1".to_owned(), 9_092),
            "with neither, the bound address, which is right on loopback"
        );
        // A host that names no host: what a listen address looks like, and
        // what a client resolves to itself.
        for wildcard_host in [
            "0.0.0.0",
            "::",
            "[::]",
            "::0",
            "0:0:0:0:0:0:0:0",
            "  ",
            " h9 ",
            "h 9",
        ] {
            let unspecified = KafkaGatewayConfig {
                partition: 0,
                partitions: vec![
                    KafkaPartitionPeer {
                        partition: 0,
                        node_id: 9,
                        host: wildcard_host.to_owned(),
                        port: 9092,
                        topic_uuid: None,
                        range_uuid: None,
                        topic_epoch: None,
                    },
                    peer(1, 7),
                ],
                ..base.clone()
            };
            assert!(
                kafka_partitions(&unspecified).is_err(),
                "`{wildcard_host}` is not an address a client can dial"
            );
        }
        // DNS is case-insensitive, so one host spelled two ways is one host,
        // and the second partition would have no server.
        let cased = KafkaGatewayConfig {
            partition: 0,
            partitions: vec![
                KafkaPartitionPeer {
                    partition: 0,
                    node_id: 9,
                    host: "broker.example".to_owned(),
                    port: 9092,
                    topic_uuid: None,
                    range_uuid: None,
                    topic_epoch: None,
                },
                KafkaPartitionPeer {
                    partition: 1,
                    node_id: 7,
                    host: "BROKER.EXAMPLE".to_owned(),
                    port: 9092,
                    topic_uuid: None,
                    range_uuid: None,
                    topic_epoch: None,
                },
            ],
            ..base.clone()
        };
        assert!(kafka_partitions(&cased).is_err());
        // One address, many spellings: still one listener, and still one
        // partition with no server behind it.
        for (a, b) in [
            ("::1", "0:0:0:0:0:0:0:1"),
            ("127.0.0.1", "::ffff:127.0.0.1"),
            ("broker.example", "BROKER.EXAMPLE"),
            ("broker.example", "broker.example."),
            ("broker.example.", "BROKER.EXAMPLE"),
        ] {
            let spelled = KafkaGatewayConfig {
                partition: 0,
                partitions: vec![
                    KafkaPartitionPeer {
                        partition: 0,
                        node_id: 9,
                        host: a.to_owned(),
                        port: 9092,
                        topic_uuid: None,
                        range_uuid: None,
                        topic_epoch: None,
                    },
                    KafkaPartitionPeer {
                        partition: 1,
                        node_id: 7,
                        host: b.to_owned(),
                        port: 9092,
                        topic_uuid: None,
                        range_uuid: None,
                        topic_epoch: None,
                    },
                ],
                ..base.clone()
            };
            assert!(
                kafka_partitions(&spelled).is_err(),
                "`{a}` and `{b}` are one endpoint"
            );
        }
        // Different addresses stay different, however they are written.
        let distinct = KafkaGatewayConfig {
            partition: 0,
            partitions: vec![
                KafkaPartitionPeer {
                    partition: 0,
                    node_id: 9,
                    host: "::1".to_owned(),
                    port: 9092,
                    topic_uuid: None,
                    range_uuid: None,
                    topic_epoch: None,
                },
                KafkaPartitionPeer {
                    partition: 1,
                    node_id: 7,
                    host: "::2".to_owned(),
                    port: 9092,
                    topic_uuid: None,
                    range_uuid: None,
                    topic_epoch: None,
                },
            ],
            ..base.clone()
        };
        assert!(kafka_partitions(&distinct).is_ok());
        // A name is judged by shape, so the whole class goes at once rather
        // than one spelling at a time.
        let long_label = "a".repeat(64);
        let long_name = vec!["b".repeat(60); 5].join(".");
        for bad_name in [
            "-broker.example",
            "broker-.example",
            "broker..example",
            "broker.exa mple",
            "broker.exam*ple",
            "[::1]",
            "[127.0.0.1]",
            "brøker.example",
            long_label.as_str(),
            long_name.as_str(),
        ] {
            let named = KafkaGatewayConfig {
                partition: 0,
                partitions: vec![
                    KafkaPartitionPeer {
                        partition: 0,
                        node_id: 9,
                        host: bad_name.to_owned(),
                        port: 9092,
                        topic_uuid: None,
                        range_uuid: None,
                        topic_epoch: None,
                    },
                    peer(1, 7),
                ],
                ..base.clone()
            };
            assert!(
                kafka_partitions(&named).is_err(),
                "`{bad_name}` is not a name a resolver would take"
            );
        }
        // And the shapes real deployments use are not collateral damage.
        for good_name in [
            "broker.example",
            "broker.example.",
            "vtop-0.vtop-headless.default.svc.cluster.local",
            "vtop_broker",
            "localhost",
            "127.0.0.1",
            "::1",
        ] {
            let named = KafkaGatewayConfig {
                partition: 0,
                partitions: vec![
                    KafkaPartitionPeer {
                        partition: 0,
                        node_id: 9,
                        host: good_name.to_owned(),
                        port: 9092,
                        topic_uuid: None,
                        range_uuid: None,
                        topic_epoch: None,
                    },
                    peer(1, 7),
                ],
                ..base.clone()
            };
            assert!(
                kafka_partitions(&named).is_ok(),
                "`{good_name}` is a host a client dials today"
            );
        }
        // The override is served verbatim too, and gets the same judgement.
        for bad_override in [
            "",
            " ",
            "0.0.0.0",
            "::",
            " edge.example ",
            "edge example",
            "edge\texample",
        ] {
            let padded = KafkaGatewayConfig {
                advertised_host: Some(bad_override.to_owned()),
                ..base.clone()
            };
            assert!(
                refuse_kafka_gateway_misuse(DataRole::Leader, Some(&padded)).is_err(),
                "`{bad_override}` is not a host a client can dial"
            );
        }
        // One answer to what clients dial: an override that disagrees with
        // the topology is a second answer that would never be used.
        let contradicts_host = KafkaGatewayConfig {
            advertised_host: Some("elsewhere".to_owned()),
            partition: 1,
            partitions: vec![peer(0, 7), peer(1, 9)],
            ..base.clone()
        };
        assert!(kafka_partitions(&contradicts_host).is_err());
        let contradicts_port = KafkaGatewayConfig {
            advertised_port: Some(19_092),
            partition: 1,
            partitions: vec![peer(0, 7), peer(1, 9)],
            ..base.clone()
        };
        assert!(kafka_partitions(&contradicts_port).is_err());
        // Saying the same thing twice is not a contradiction.
        let agrees = KafkaGatewayConfig {
            advertised_host: Some("h9".to_owned()),
            advertised_port: Some(9_092),
            partition: 1,
            partitions: vec![peer(0, 7), peer(1, 9)],
            ..base.clone()
        };
        assert!(kafka_partitions(&agrees).is_ok());
        // Two ids at one endpoint: the same topology written the other way,
        // and the same partition with no server. Distinct ids do not make a
        // second listener.
        let one_listener = KafkaGatewayConfig {
            partition: 0,
            partitions: vec![
                KafkaPartitionPeer {
                    partition: 0,
                    node_id: 9,
                    host: "h".to_owned(),
                    port: 9092,
                    topic_uuid: None,
                    range_uuid: None,
                    topic_epoch: None,
                },
                KafkaPartitionPeer {
                    partition: 1,
                    node_id: 7,
                    host: "h".to_owned(),
                    port: 9092,
                    topic_uuid: None,
                    range_uuid: None,
                    topic_epoch: None,
                },
            ],
            ..base.clone()
        };
        let error = kafka_partitions(&one_listener).unwrap_err();
        assert!(
            error.contains("both at h:9092"),
            "the refusal must name the endpoint they share: {error}"
        );
        // The same host on a different port is a different listener, and fine.
        let two_ports = KafkaGatewayConfig {
            partition: 0,
            partitions: vec![
                KafkaPartitionPeer {
                    partition: 0,
                    node_id: 9,
                    host: "h".to_owned(),
                    port: 9092,
                    topic_uuid: None,
                    range_uuid: None,
                    topic_epoch: None,
                },
                KafkaPartitionPeer {
                    partition: 1,
                    node_id: 7,
                    host: "h".to_owned(),
                    port: 9093,
                    topic_uuid: None,
                    range_uuid: None,
                    topic_epoch: None,
                },
            ],
            ..base.clone()
        };
        assert!(kafka_partitions(&two_ports).is_ok());
        // A topology that names this node's host IS what gets advertised, so
        // a wildcard bind needs no second answer under a key never read.
        let wildcard = KafkaGatewayConfig {
            listen: "0.0.0.0:9092".to_owned(),
            any_interface: true,
            partition: 1,
            partitions: vec![peer(0, 7), peer(1, 9)],
            ..base.clone()
        };
        assert!(wildcard.advertises_a_host(wildcard.partition));
        assert!(refuse_kafka_gateway_misuse(DataRole::Leader, Some(&wildcard)).is_ok());
        // With no topology, the override is still the only answer there is.
        let bare = KafkaGatewayConfig {
            listen: "0.0.0.0:9092".to_owned(),
            any_interface: true,
            ..base.clone()
        };
        assert!(!bare.advertises_a_host(bare.partition));
        assert!(refuse_kafka_gateway_misuse(DataRole::Leader, Some(&bare)).is_err());
        // Endpoints a client cannot dial, and ids a node cannot run with.
        for bad in [
            KafkaPartitionPeer {
                partition: 1,
                node_id: 7,
                host: "  ".to_owned(),
                port: 9092,
                topic_uuid: None,
                range_uuid: None,
                topic_epoch: None,
            },
            KafkaPartitionPeer {
                partition: 1,
                node_id: 7,
                host: "h".to_owned(),
                port: 0,
                topic_uuid: None,
                range_uuid: None,
                topic_epoch: None,
            },
            KafkaPartitionPeer {
                partition: 1,
                node_id: 256,
                host: "h".to_owned(),
                port: 9092,
                topic_uuid: None,
                range_uuid: None,
                topic_epoch: None,
            },
            KafkaPartitionPeer {
                partition: 1,
                node_id: -1,
                host: "h".to_owned(),
                port: 9092,
                topic_uuid: None,
                range_uuid: None,
                topic_epoch: None,
            },
        ] {
            let config = KafkaGatewayConfig {
                partition: 0,
                partitions: vec![peer(0, 9), bad],
                ..base.clone()
            };
            assert!(kafka_partitions(&config).is_err());
        }
        // A range identity is all three fields, or none.
        let incomplete = KafkaGatewayConfig {
            partition: 0,
            partitions: vec![peer(0, 9), {
                let mut p = peer(1, 7);
                p.topic_uuid = Some(Uuid::from_u128(1));
                p
            }],
            ..base.clone()
        };
        assert!(kafka_partitions(&incomplete)
            .unwrap_err()
            .contains("partial range identity"));
        let identified = KafkaGatewayConfig {
            partition: 0,
            partitions: vec![
                {
                    let mut p = peer(0, 9);
                    p.topic_uuid = Some(Uuid::from_u128(1));
                    p.range_uuid = Some(Uuid::from_u128(2));
                    p.topic_epoch = Some(1);
                    p
                },
                {
                    let mut p = peer(1, 7);
                    p.topic_uuid = Some(Uuid::from_u128(3));
                    p.range_uuid = Some(Uuid::from_u128(4));
                    p.topic_epoch = Some(1);
                    p
                },
            ],
            ..base.clone()
        };
        assert!(kafka_partitions(&identified).is_ok());
        let duplicated = KafkaGatewayConfig {
            partition: 0,
            partitions: vec![
                {
                    let mut p = peer(0, 9);
                    p.topic_uuid = Some(Uuid::from_u128(1));
                    p.range_uuid = Some(Uuid::from_u128(2));
                    p.topic_epoch = Some(1);
                    p
                },
                {
                    let mut p = peer(1, 7);
                    p.topic_uuid = Some(Uuid::from_u128(3));
                    p.range_uuid = Some(Uuid::from_u128(2));
                    p.topic_epoch = Some(1);
                    p
                },
            ],
            ..base.clone()
        };
        assert!(kafka_partitions(&duplicated)
            .unwrap_err()
            .contains("same range"));
        let nil_uuid = KafkaGatewayConfig {
            partition: 0,
            partitions: vec![
                {
                    let mut p = peer(0, 9);
                    p.topic_uuid = Some(Uuid::nil());
                    p.range_uuid = Some(Uuid::from_u128(2));
                    p.topic_epoch = Some(1);
                    p
                },
                peer(1, 7),
            ],
            ..base.clone()
        };
        assert!(kafka_partitions(&nil_uuid)
            .unwrap_err()
            .contains("nil topic or range"));
    }

    #[test]
    fn a_topic_map_must_name_each_topic_once_with_a_backend() {
        let base = KafkaGatewayConfig {
            listen: "127.0.0.1:9092".to_owned(),
            any_interface: false,
            advertised_host: None,
            advertised_port: None,
            topic: None,
            node_id: 1,
            partition: 0,
            partitions: Vec::new(),
            max_fetch_wait_ms: 5_000,
            producer_id: None,
            topics: Vec::new(),
        };
        assert!(kafka_topics(&base).is_ok());
        let native = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: "events".to_owned(),
                backend: "native".to_owned(),
                ..KafkaTopicRoute::default()
            }],
            ..base.clone()
        };
        assert!(kafka_topics(&native).is_ok());
        let duplicate = KafkaGatewayConfig {
            topics: vec![
                KafkaTopicRoute {
                    name: "events".to_owned(),
                    backend: "native".to_owned(),
                    ..KafkaTopicRoute::default()
                },
                KafkaTopicRoute {
                    name: "events".to_owned(),
                    backend: "kafka".to_owned(),
                    brokers: vec!["127.0.0.1:9092".to_owned()],
                    ..KafkaTopicRoute::default()
                },
            ],
            ..base.clone()
        };
        assert!(kafka_topics(&duplicate).unwrap_err().contains("twice"));
        let blank = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: String::new(),
                backend: "native".to_owned(),
                ..KafkaTopicRoute::default()
            }],
            ..base.clone()
        };
        assert!(kafka_topics(&blank).unwrap_err().contains("no name"));
        let no_brokers = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: "legacy".to_owned(),
                backend: "kafka".to_owned(),
                ..KafkaTopicRoute::default()
            }],
            ..base.clone()
        };
        assert!(kafka_topics(&no_brokers)
            .unwrap_err()
            .contains("no brokers"));
        let unknown = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: "events".to_owned(),
                backend: "s3".to_owned(),
                ..KafkaTopicRoute::default()
            }],
            ..base.clone()
        };
        assert!(kafka_topics(&unknown)
            .unwrap_err()
            .contains("native, kafka, dual or shadow"));
        let native_brokers = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: "events".to_owned(),
                backend: "native".to_owned(),
                brokers: vec!["127.0.0.1:9092".to_owned()],
                ..KafkaTopicRoute::default()
            }],
            ..base.clone()
        };
        assert!(kafka_topics(&native_brokers)
            .unwrap_err()
            .contains("brokers or a remote topic"));
        let native_read = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: "events".to_owned(),
                backend: "native".to_owned(),
                read: Some("compare".to_owned()),
                ..KafkaTopicRoute::default()
            }],
            ..base.clone()
        };
        assert!(kafka_topics(&native_read)
            .unwrap_err()
            .contains("only one side"));
        let dual = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: "events".to_owned(),
                backend: "dual".to_owned(),
                brokers: vec!["127.0.0.1:9092".to_owned()],
                read: Some("compare".to_owned()),
                ..KafkaTopicRoute::default()
            }],
            ..base.clone()
        };
        assert!(kafka_topics(&dual).is_ok());
        let cutover_no_receipts = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: "events".to_owned(),
                backend: "native".to_owned(),
                cutover: true,
                ..KafkaTopicRoute::default()
            }],
            ..base.clone()
        };
        assert!(kafka_topics(&cutover_no_receipts)
            .unwrap_err()
            .contains("no receipts"));
        let cutover_ok = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: "events".to_owned(),
                backend: "native".to_owned(),
                receipts: Some("/tmp/events.jsonl".to_owned()),
                cutover: true,
                ..KafkaTopicRoute::default()
            }],
            ..base.clone()
        };
        assert!(kafka_topics(&cutover_ok).is_ok());
        let cutover_while_shadow = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: "events".to_owned(),
                backend: "dual".to_owned(),
                brokers: vec!["127.0.0.1:9092".to_owned()],
                read: Some("kafka".to_owned()),
                receipts: Some("/tmp/events.jsonl".to_owned()),
                cutover: true,
                ..KafkaTopicRoute::default()
            }],
            ..base.clone()
        };
        assert!(kafka_topics(&cutover_while_shadow)
            .unwrap_err()
            .contains("read is kafka"));
        let cutover_while_compare = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: "events".to_owned(),
                backend: "dual".to_owned(),
                brokers: vec!["127.0.0.1:9092".to_owned()],
                read: Some("compare".to_owned()),
                receipts: Some("/tmp/events.jsonl".to_owned()),
                cutover: true,
                ..KafkaTopicRoute::default()
            }],
            ..base.clone()
        };
        assert!(kafka_topics(&cutover_while_compare)
            .unwrap_err()
            .contains("read is compare"));
        let two_natives = KafkaGatewayConfig {
            topics: vec![
                KafkaTopicRoute {
                    name: "events".to_owned(),
                    backend: "native".to_owned(),
                    ..KafkaTopicRoute::default()
                },
                KafkaTopicRoute {
                    name: "alias".to_owned(),
                    backend: "native".to_owned(),
                    ..KafkaTopicRoute::default()
                },
            ],
            ..base.clone()
        };
        assert!(kafka_topics(&two_natives).unwrap_err().contains("one log"));
        let bad_port = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: "legacy".to_owned(),
                backend: "kafka".to_owned(),
                brokers: vec!["host:bad".to_owned()],
                ..KafkaTopicRoute::default()
            }],
            ..base.clone()
        };
        assert!(kafka_topics(&bad_port)
            .unwrap_err()
            .contains("not a number"));
        let bracketed_dns = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: "legacy".to_owned(),
                backend: "kafka".to_owned(),
                brokers: vec!["[localhost]:9092".to_owned()],
                ..KafkaTopicRoute::default()
            }],
            ..base.clone()
        };
        assert!(kafka_topics(&bracketed_dns)
            .unwrap_err()
            .contains("IPv6 literal"));
        let two_remotes = KafkaGatewayConfig {
            topics: vec![
                KafkaTopicRoute {
                    name: "legacy".to_owned(),
                    backend: "kafka".to_owned(),
                    brokers: vec!["127.0.0.1:9092".to_owned()],
                    remote_topic: Some("shared".to_owned()),
                    ..KafkaTopicRoute::default()
                },
                KafkaTopicRoute {
                    name: "copy".to_owned(),
                    backend: "kafka".to_owned(),
                    brokers: vec!["127.0.0.1:9092".to_owned()],
                    remote_topic: Some("shared".to_owned()),
                    ..KafkaTopicRoute::default()
                },
            ],
            ..base.clone()
        };
        assert!(kafka_topics(&two_remotes)
            .unwrap_err()
            .contains("same remote log"));
        let two_remotes_dup_brokers = KafkaGatewayConfig {
            topics: vec![
                KafkaTopicRoute {
                    name: "legacy".to_owned(),
                    backend: "kafka".to_owned(),
                    brokers: vec!["127.0.0.1:9092".to_owned(), "127.0.0.1:9092".to_owned()],
                    remote_topic: Some("shared".to_owned()),
                    ..KafkaTopicRoute::default()
                },
                KafkaTopicRoute {
                    name: "copy".to_owned(),
                    backend: "kafka".to_owned(),
                    brokers: vec!["127.0.0.1:9092".to_owned()],
                    remote_topic: Some("shared".to_owned()),
                    ..KafkaTopicRoute::default()
                },
            ],
            ..base.clone()
        };
        assert!(kafka_topics(&two_remotes_dup_brokers)
            .unwrap_err()
            .contains("same remote log"));
        let receipts_on_kafka = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: "legacy".to_owned(),
                backend: "kafka".to_owned(),
                brokers: vec!["127.0.0.1:9092".to_owned()],
                receipts: Some("/tmp/events.jsonl".to_owned()),
                ..KafkaTopicRoute::default()
            }],
            ..base.clone()
        };
        assert!(kafka_topics(&receipts_on_kafka)
            .unwrap_err()
            .contains("receipts belong"));
        let blank_broker = KafkaGatewayConfig {
            topics: vec![KafkaTopicRoute {
                name: "legacy".to_owned(),
                backend: "kafka".to_owned(),
                brokers: vec!["noport".to_owned()],
                ..KafkaTopicRoute::default()
            }],
            ..base
        };
        assert!(kafka_topics(&blank_broker)
            .unwrap_err()
            .contains("host:port"));
    }

    #[test]
    fn the_kafka_producer_id_is_derived_from_the_principal_and_never_equal_to_it() {
        let principal = Uuid::from_u128(0x1234);
        let kafka = KafkaGatewayConfig {
            listen: "127.0.0.1:9092".to_owned(),
            any_interface: false,
            advertised_host: None,
            advertised_port: None,
            topic: None,
            node_id: 1,
            partition: 0,
            partitions: Vec::new(),
            max_fetch_wait_ms: 5_000,
            producer_id: None,
            topics: Vec::new(),
        };
        let derived = kafka_producer_id(principal, &kafka).unwrap();
        assert_ne!(derived, principal);
        assert_eq!(
            derived,
            kafka_producer_id(principal, &kafka).unwrap(),
            "stable across restarts"
        );
        assert_ne!(
            derived,
            kafka_producer_id(Uuid::from_u128(0x5678), &kafka).unwrap(),
            "one per principal"
        );
        let own = KafkaGatewayConfig {
            producer_id: Some(Uuid::from_u128(0x9999)),
            ..kafka.clone()
        };
        assert_eq!(
            kafka_producer_id(principal, &own).unwrap(),
            Uuid::from_u128(0x9999)
        );
        let same = KafkaGatewayConfig {
            producer_id: Some(principal),
            ..kafka
        };
        assert!(kafka_producer_id(principal, &same)
            .unwrap_err()
            .contains("principal"));
    }

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

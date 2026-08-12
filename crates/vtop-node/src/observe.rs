//! The node's operational surface: what a scrape sees, and what `/readyz`
//! means for each role (#224).
//!
//! # Pull-time collection, not a background poller
//!
//! Every metric here is a [`prometheus::core::Collector`] that reads live
//! process state during `gather()`. There is no sampling task and no cached
//! snapshot, so a panel can never show a value the node stopped believing
//! minutes ago — the classic failure where an operator stares at a stale
//! "committed offset" while the range is wedged.
//!
//! The cost of that choice is that a collector must never block. The append
//! paths hold their state locks across fsync, and under exactly the conditions
//! an operator needs metrics most — a disk that stopped acknowledging writes —
//! a blocking read would take the scrape endpoint down along with the disk. So
//! offsets are read through the non-blocking `try_local_offsets` accessors and a
//! contended read simply leaves the previous gauge value in place. A gauge that
//! stops advancing is the honest signal; a scrape that hangs is not.
//!
//! # Monotonic sources are exported, not mirrored
//!
//! The broker's own counters are plain monotonic atomics, deliberately free of
//! any Prometheus dependency. Exporting them as gauges would break `rate()`, so
//! they become counter families whose value *is* the source total (see
//! [`CounterFamily`]). The tempting alternative — mirror into an `IntCounter`
//! by `inc_by(source - mirror)` — is racy under concurrent scrapes and drifts
//! permanently upward once it loses that race.
//!
//! # Label cardinality
//!
//! Same rule as the archive engine: labels are closed sets. `role`, `state`,
//! `reason`, and `scope` are enums. `topic`/`range` are bounded by the ranges
//! this process hosts, and `peer`/`follower` by cluster membership — both are
//! deployment-sized, not request-sized. Offsets, request ids, and paths are
//! never labels.

use crate::config::ObservabilityConfig;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntGauge, IntGaugeVec, Opts, Registry};
use std::sync::Arc;
use uuid::Uuid;
use vtop_broker::memory_budget::BudgetRejectReason;
use vtop_broker::replication::{InProcessFollower, NetworkedReplicaSet};
use vtop_broker::server_metrics;
use vtop_broker::{
    LocalBroker, RequestKind, RequestOutcome, ServerMetrics, LATENCY_BUCKETS_MICROS,
};
use vtop_log::RecoveryReport;
use vtop_meta::{OpenraftConsensus, RaftObservation, RaftServerState};
use vtop_observe::{Readiness, ReadinessGate};

/// A live readiness condition evaluated at request time.
///
/// Kept as a probe rather than a background poller for the same reason the
/// metrics are pull-time: a cached readiness bit is a readiness bit that can be
/// wrong for a whole poll interval, and the interval a fenced leader keeps
/// answering "ready" is exactly the window in which someone routes writes at it.
pub type ReadinessProbe = Arc<dyn Fn() -> Readiness + Send + Sync>;

/// A registry plus the readiness level served next to it.
pub struct NodeObservability {
    pub registry: Registry,
    /// Startup gate: flipped once the node's listeners are bound.
    pub gate: ReadinessGate,
    /// Optional live condition ANDed with the gate.
    ///
    /// Behind a shared cell so it can be installed AFTER the endpoint is
    /// serving, and so a process hosting more than one role (#215) can hand
    /// every role a `&NodeObservability` rather than making one of them the
    /// owner. The served source holds the same cell, so a probe installed late
    /// takes effect on the next request rather than the next restart.
    probe: Arc<std::sync::RwLock<Option<ReadinessProbe>>>,
}

/// Serves this node's registry, and its readiness as `gate AND probe`.
struct NodeSource {
    registry: Registry,
    gate: ReadinessGate,
    probe: Arc<std::sync::RwLock<Option<ReadinessProbe>>>,
}

impl vtop_observe::MetricsSource for NodeSource {
    fn encode(&self) -> Result<Vec<u8>, vtop_observe::MetricsError> {
        let mut buf = Vec::new();
        prometheus::Encoder::encode(
            &prometheus::TextEncoder::new(),
            &self.registry.gather(),
            &mut buf,
        )
        .map_err(|error| vtop_observe::MetricsError::Encode(error.to_string()))?;
        Ok(buf)
    }

    fn readiness(&self) -> Readiness {
        // The gate wins when it is closed: a node that has not finished
        // starting has nothing useful to say about its lease.
        let gated = self.gate.get();
        if !gated.is_ready() {
            return gated;
        }
        let probe = match self.probe.read() {
            Ok(guard) => guard.clone(),
            // A poisoned probe cell means some task panicked while installing
            // one. Reporting not-ready is the safe read: the process is in an
            // unknown state.
            Err(_) => return Readiness::not_ready("readiness probe state poisoned"),
        };
        match probe {
            Some(probe) => probe(),
            None => Readiness::Ready,
        }
    }
}

impl NodeObservability {
    /// Build a registry carrying the `vtop_` prefix the dashboards expect, plus
    /// a gate that starts NOT ready. A node that forgets to flip the gate stays
    /// visibly unavailable rather than advertising itself half-initialized.
    pub fn new(role: &str, node_id: &str) -> Result<Self, String> {
        let registry = Registry::new_custom(Some("vtop".into()), None)
            .map_err(|error| format!("build metrics registry: {error}"))?;
        let info = IntGaugeVec::new(
            Opts::new(
                "node_info",
                "Always 1; carries this process's role and node id as labels",
            ),
            &["role", "node_id"],
        )
        .map_err(|error| format!("build node_info: {error}"))?;
        info.with_label_values(&[role, node_id]).set(1);
        registry
            .register(Box::new(info))
            .map_err(|error| format!("register node_info: {error}"))?;
        Ok(Self {
            registry,
            gate: ReadinessGate::starting("node is still starting"),
            probe: Arc::new(std::sync::RwLock::new(None)),
        })
    }

    /// Install a live readiness condition, evaluated on every `/readyz`
    /// request. Takes effect immediately, including on an endpoint that is
    /// already serving.
    pub fn set_readiness_probe(&self, probe: ReadinessProbe) {
        match self.probe.write() {
            Ok(mut guard) => *guard = Some(probe),
            Err(mut poisoned) => {
                **poisoned.get_mut() = Some(probe);
                drop(poisoned);
                self.probe.clear_poison();
            }
        }
    }

    pub fn register(&self, collector: Box<dyn Collector>) -> Result<(), String> {
        self.registry
            .register(collector)
            .map_err(|error| format!("register collector: {error}"))
    }

    /// Bind the endpoint if the config names an address. Returns the bound
    /// address, or `None` when the node was configured without one.
    ///
    /// A configured address that cannot bind is an error, not a warning: see
    /// [`crate::config::ObservabilityConfig`].
    pub async fn serve(
        &self,
        config: &ObservabilityConfig,
    ) -> Result<Option<std::net::SocketAddr>, String> {
        let Some(listen) = config.listen.as_ref() else {
            return Ok(None);
        };
        let addr = vtop_meta::resolve_endpoint(listen).map_err(|error| error.to_string())?;
        let source = Arc::new(NodeSource {
            registry: self.registry.clone(),
            gate: self.gate.clone(),
            probe: Arc::clone(&self.probe),
        });
        let bound = vtop_observe::start(addr, source)
            .await
            .map_err(|error| format!("bind observability endpoint {listen}: {error}"))?;
        Ok(Some(bound))
    }
}

/// A counter family whose exported value *is* the source total.
///
/// The obvious implementation — mirror a monotonic atomic into an `IntCounter`
/// by `inc_by(source - mirror)` — is racy. The endpoint serves up to
/// [`vtop_observe::MAX_CONNECTIONS`] scrapes concurrently on a multi-threaded
/// runtime, so two gathers can read the same stale mirror and each add the full
/// delta, leaving the exported counter permanently above its source. Reading
/// the source and emitting it directly has no shared mutable state to race on,
/// and is also the more honest export: the number served is the number the
/// broker actually holds, not a reconstruction of it.
struct CounterFamily {
    desc: Desc,
    label_names: Vec<String>,
}

impl CounterFamily {
    fn new(name: &str, help: &str, label_names: &[&str]) -> Result<Self, String> {
        let label_names: Vec<String> = label_names.iter().map(|l| (*l).to_string()).collect();
        Ok(Self {
            desc: Desc::new(
                name.to_string(),
                help.to_string(),
                label_names.clone(),
                Default::default(),
            )
            .map_err(|error| format!("build {name}: {error}"))?,
            label_names,
        })
    }

    /// Build the family from `(label values, total)` pairs.
    ///
    /// Label values are positional against the names given at construction.
    fn family<'a>(&self, samples: impl IntoIterator<Item = (Vec<&'a str>, u64)>) -> MetricFamily {
        let mut family = MetricFamily::default();
        family.set_name(self.desc.fq_name.clone());
        family.set_help(self.desc.help.clone());
        family.set_field_type(prometheus::proto::MetricType::COUNTER);
        family.set_metric(
            samples
                .into_iter()
                .map(|(values, total)| {
                    let mut counter = prometheus::proto::Counter::default();
                    // Prometheus counters are f64 on the wire. u64 is exact
                    // there below 2^53, which for a byte counter is nine
                    // petabytes — far past the point where a process restart
                    // has reset it anyway.
                    counter.set_value(total as f64);
                    let mut metric = prometheus::proto::Metric::default();
                    metric.set_label(
                        self.label_names
                            .iter()
                            .zip(values)
                            .map(|(name, value)| {
                                let mut label = prometheus::proto::LabelPair::default();
                                label.set_name(name.clone());
                                label.set_value(value.to_string());
                                label
                            })
                            .collect(),
                    );
                    metric.set_counter(counter);
                    metric
                })
                .collect(),
        );
        family
    }
}

fn gauge(name: &str, help: &str) -> Result<IntGauge, String> {
    IntGauge::with_opts(Opts::new(name, help)).map_err(|error| format!("build {name}: {error}"))
}

fn gauge_vec(name: &str, help: &str, labels: &[&str]) -> Result<IntGaugeVec, String> {
    IntGaugeVec::new(Opts::new(name, help), labels)
        .map_err(|error| format!("build {name}: {error}"))
}

fn descs_of<'a>(members: &[&'a dyn Collector]) -> Vec<&'a Desc> {
    members.iter().flat_map(|m| m.desc()).collect()
}

fn collect_all(members: &[&dyn Collector]) -> Vec<MetricFamily> {
    members.iter().flat_map(|m| m.collect()).collect()
}

// ---------------------------------------------------------------------------
// Metadata Raft
// ---------------------------------------------------------------------------

/// Raft term, role, and progress, from the metadata plane's own
/// [`RaftObservation`] rather than any consensus-library type.
///
/// Answers the questions a metadata incident starts with: who thinks it is
/// leader, how far behind is each follower, and how long since a quorum
/// acknowledged the leader — the last of which is the signal that separates
/// "partitioned leader" from "slow cluster".
pub struct MetaRaftCollector {
    consensus: Arc<OpenraftConsensus>,
    gauges: MetaRaftGauges,
}

impl MetaRaftCollector {
    pub fn new(consensus: Arc<OpenraftConsensus>) -> Result<Self, String> {
        Ok(Self {
            consensus,
            gauges: MetaRaftGauges::new()?,
        })
    }
}

impl Collector for MetaRaftCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.gauges.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        // `observe()` is a lock-free read of the metadata plane's latest
        // published Raft snapshot; it never messages the Raft core.
        self.gauges.publish(&self.consensus.observe());
        self.gauges.collect()
    }
}

/// The gauge set, split from the collector so the observation-to-metric mapping
/// can be asserted without standing up a consensus node.
struct MetaRaftGauges {
    running: IntGauge,
    term: IntGauge,
    state: IntGaugeVec,
    leader_id: IntGauge,
    last_log_index: IntGauge,
    last_applied_index: IntGauge,
    snapshot_index: IntGauge,
    purged_index: IntGauge,
    voters: IntGauge,
    learners: IntGauge,
    millis_since_quorum_ack: IntGauge,
    peer_matched_index: IntGaugeVec,
    peer_lag_entries: IntGaugeVec,
}

impl MetaRaftGauges {
    fn new() -> Result<Self, String> {
        Ok(Self {
            running: gauge(
                "meta_raft_running",
                "1 while the Raft node is running; 0 after a fatal error stopped it",
            )?,
            term: gauge("meta_raft_term", "Current Raft term")?,
            state: gauge_vec(
                "meta_raft_state",
                "1 for the node's current Raft server state, 0 for the others",
                &["state"],
            )?,
            leader_id: gauge(
                "meta_raft_leader_id",
                "Node id this node currently believes is leader; -1 when none is known",
            )?,
            last_log_index: gauge("meta_raft_last_log_index", "Last index in the local log")?,
            last_applied_index: gauge(
                "meta_raft_last_applied_index",
                "Last index applied to the metadata state machine",
            )?,
            snapshot_index: gauge(
                "meta_raft_snapshot_index",
                "Last index included in the newest local snapshot",
            )?,
            purged_index: gauge(
                "meta_raft_purged_index",
                "Last index purged from the local log, inclusive",
            )?,
            voters: gauge(
                "meta_raft_voters",
                "Voting members in the committed membership config",
            )?,
            learners: gauge(
                "meta_raft_learners",
                "Learners in the committed membership config",
            )?,
            millis_since_quorum_ack: gauge(
                "meta_raft_millis_since_quorum_ack",
                "Milliseconds since a quorum last acknowledged this leader; -1 when not leading \
                 or not yet acknowledged. A climbing value on a self-declared leader is the \
                 signature of a partition",
            )?,
            peer_matched_index: gauge_vec(
                "meta_raft_peer_matched_index",
                "Highest log index each peer has acknowledged; leader only",
                &["peer"],
            )?,
            peer_lag_entries: gauge_vec(
                "meta_raft_peer_lag_entries",
                "Entries by which each peer trails this leader's last log index",
                &["peer"],
            )?,
        })
    }

    fn members(&self) -> Vec<&dyn Collector> {
        vec![
            &self.running,
            &self.term,
            &self.state,
            &self.leader_id,
            &self.last_log_index,
            &self.last_applied_index,
            &self.snapshot_index,
            &self.purged_index,
            &self.voters,
            &self.learners,
            &self.millis_since_quorum_ack,
            &self.peer_matched_index,
            &self.peer_lag_entries,
        ]
    }

    /// `-1` is the sentinel for "no value yet" throughout: a Raft node with no
    /// log has no last index, and exporting `0` there would be indistinguishable
    /// from a node whose log genuinely sits at the first VTOP index.
    fn publish(&self, observation: &RaftObservation) {
        self.running.set(i64::from(observation.running));
        self.term.set(observation.current_term as i64);

        for state in RaftServerState::ALL {
            self.state
                .with_label_values(&[state.as_str()])
                .set(i64::from(state == observation.server_state));
        }

        let or_absent = |value: Option<u64>| value.map(|v| v as i64).unwrap_or(-1);
        self.leader_id
            .set(or_absent(observation.current_leader.map(|id| id.0)));
        self.last_log_index
            .set(or_absent(observation.last_log_index));
        self.last_applied_index
            .set(or_absent(observation.last_applied_index));
        self.snapshot_index
            .set(or_absent(observation.snapshot_index));
        self.purged_index.set(or_absent(observation.purged_index));
        self.voters.set(observation.voters as i64);
        self.learners.set(observation.learners as i64);
        self.millis_since_quorum_ack
            .set(or_absent(observation.millis_since_quorum_ack));

        // Per-peer replication exists only on a leader, and the observation
        // reports it empty elsewhere. Reset rather than leave stale series
        // behind when leadership moves: a demoted node still exporting
        // yesterday's follower lag is actively misleading.
        self.peer_matched_index.reset();
        self.peer_lag_entries.reset();
        let leader_index = observation.last_log_index.unwrap_or(0);
        for (peer, matched) in &observation.peer_matched_index {
            let peer = peer.to_string();
            // A peer that has acknowledged nothing reports the same `-1`
            // sentinel as every other absent index here. Reporting `0` would
            // make "this learner has never replied" indistinguishable from
            // "this peer is caught up to the first entry", and the lag would
            // read as the leader's whole log rather than as unknown.
            self.peer_matched_index
                .with_label_values(&[&peer])
                .set(matched.map(|index| index as i64).unwrap_or(-1));
            self.peer_lag_entries
                .with_label_values(&[&peer])
                .set(match matched {
                    Some(index) => leader_index.saturating_sub(*index) as i64,
                    None => -1,
                });
        }
    }
}

impl Collector for MetaRaftGauges {
    fn desc(&self) -> Vec<&Desc> {
        descs_of(&self.members())
    }

    fn collect(&self) -> Vec<MetricFamily> {
        collect_all(&self.members())
    }
}

// ---------------------------------------------------------------------------
// Data plane, leader / standalone
// ---------------------------------------------------------------------------

/// Range progress and replication health for a leader or standalone broker.
pub struct BrokerCollector {
    broker: Arc<LocalBroker>,
    replicas: Option<Arc<NetworkedReplicaSet>>,
    followers: Vec<Uuid>,
    range_labels: [String; 2],

    local_committed: IntGaugeVec,
    next_offset: IntGaugeVec,
    cluster_committed: IntGaugeVec,
    held_fencing_epoch: IntGaugeVec,
    meta_fencing_epoch: IntGaugeVec,
    lease_active: IntGaugeVec,

    follower_durable_offset: IntGaugeVec,
    follower_lag_records: IntGaugeVec,
    follower_connected: IntGaugeVec,

    group_commit_last_batch_records: IntGauge,
    group_commit_last_batch_bytes: IntGauge,
    memory_used_bytes: IntGaugeVec,
    memory_queue_depth: IntGauge,

    group_commits: CounterFamily,
    group_commit_requests: CounterFamily,
    group_commit_records: CounterFamily,
    group_commit_bytes: CounterFamily,
    group_commit_sync_nanos: CounterFamily,
    group_commit_queue_wait_nanos: CounterFamily,
    memory_rejections: CounterFamily,
    backpressure_nanos: CounterFamily,
    backpressure_events: CounterFamily,
}

impl BrokerCollector {
    pub fn new(
        broker: Arc<LocalBroker>,
        replicas: Option<Arc<NetworkedReplicaSet>>,
        followers: Vec<Uuid>,
    ) -> Result<Self, String> {
        let range = broker.range();
        let range_labels = [range.topic.clone(), range.range_id.to_string()];
        Ok(Self {
            broker,
            replicas,
            followers,
            range_labels,
            local_committed: gauge_vec(
                "broker_local_committed_offset",
                "Durable commit boundary of this replica's active segment",
                &["topic", "range"],
            )?,
            next_offset: gauge_vec(
                "broker_next_offset",
                "Next offset the segment will assign, including records not yet durable",
                &["topic", "range"],
            )?,
            cluster_committed: gauge_vec(
                "broker_cluster_committed_offset",
                "Quorum-committed high-water mark; fetch never exposes records above it",
                &["topic", "range"],
            )?,
            held_fencing_epoch: gauge_vec(
                "broker_held_fencing_epoch",
                "Fencing epoch this process was granted as range leaseholder",
                &["topic", "range"],
            )?,
            meta_fencing_epoch: gauge_vec(
                "broker_meta_fencing_epoch",
                "Latest metadata-committed fencing epoch observed for the range",
                &["topic", "range"],
            )?,
            lease_active: gauge_vec(
                "broker_lease_active",
                "1 while metadata still records a live lease for this leaseholder; 0 once fenced",
                &["topic", "range"],
            )?,
            follower_durable_offset: gauge_vec(
                "broker_follower_durable_offset",
                "Highest offset each follower has acknowledged as durable",
                &["follower"],
            )?,
            follower_lag_records: gauge_vec(
                "broker_follower_lag_records",
                "Records by which each follower trails the leader's durable boundary",
                &["follower"],
            )?,
            follower_connected: gauge_vec(
                "broker_follower_connected",
                "1 while the leader holds a live replication stream to the follower",
                &["follower"],
            )?,
            group_commits: CounterFamily::new(
                "broker_group_commits_total",
                "Commit groups sealed and fsynced",
                &[],
            )?,
            group_commit_requests: CounterFamily::new(
                "broker_group_commit_requests_total",
                "Produce requests folded into commit groups",
                &[],
            )?,
            group_commit_records: CounterFamily::new(
                "broker_group_commit_records_total",
                "Records folded into commit groups",
                &[],
            )?,
            group_commit_bytes: CounterFamily::new(
                "broker_group_commit_bytes_total",
                "Record bytes folded into commit groups",
                &[],
            )?,
            group_commit_sync_nanos: CounterFamily::new(
                "broker_group_commit_sync_nanoseconds_total",
                "Nanoseconds spent in group fsync; divide by commits for mean sync cost",
                &[],
            )?,
            group_commit_queue_wait_nanos: CounterFamily::new(
                "broker_group_commit_queue_wait_nanoseconds_total",
                "Nanoseconds requests waited to join a group, summed over groups",
                &[],
            )?,
            group_commit_last_batch_records: gauge(
                "broker_group_commit_last_batch_records",
                "Records in the most recently sealed commit group",
            )?,
            group_commit_last_batch_bytes: gauge(
                "broker_group_commit_last_batch_bytes",
                "Bytes in the most recently sealed commit group",
            )?,
            memory_used_bytes: gauge_vec(
                "broker_memory_used_bytes",
                "Bytes currently charged to each memory-budget ledger",
                &["scope"],
            )?,
            memory_queue_depth: gauge(
                "broker_memory_queue_depth",
                "Admissions currently blocked waiting for budget",
            )?,
            memory_rejections: CounterFamily::new(
                "broker_memory_rejections_total",
                "Admissions refused by a memory budget, by which ledger refused",
                &["reason"],
            )?,
            backpressure_nanos: CounterFamily::new(
                "broker_backpressure_nanoseconds_total",
                "Nanoseconds spent blocked on memory-budget admission",
                &[],
            )?,
            backpressure_events: CounterFamily::new(
                "broker_backpressure_events_total",
                "Admission attempts that had to wait for budget",
                &[],
            )?,
        })
    }

    fn members(&self) -> Vec<&dyn Collector> {
        vec![
            &self.local_committed,
            &self.next_offset,
            &self.cluster_committed,
            &self.held_fencing_epoch,
            &self.meta_fencing_epoch,
            &self.lease_active,
            &self.follower_durable_offset,
            &self.follower_lag_records,
            &self.follower_connected,
            &self.group_commit_last_batch_records,
            &self.group_commit_last_batch_bytes,
            &self.memory_used_bytes,
            &self.memory_queue_depth,
        ]
    }

    fn range(&self) -> [&str; 2] {
        [&self.range_labels[0], &self.range_labels[1]]
    }

    fn refresh(&self) {
        let range = self.range();

        // `None` means the append path holds the lock; leaving the previous
        // value in place is deliberate (see the module docs).
        let mut leader_committed = None;
        if let Some((committed, next)) = self.broker.try_local_offsets() {
            self.local_committed
                .with_label_values(&range)
                .set(committed as i64);
            self.next_offset.with_label_values(&range).set(next as i64);
            leader_committed = Some(committed);
        }

        if let Some(cluster) = self.broker.cluster_committed() {
            self.cluster_committed
                .with_label_values(&range)
                .set(cluster.get() as i64);
        }
        // One non-blocking snapshot for both metadata fields: reading the
        // epoch and the lease bit through separate locks could straddle a
        // grant and report an epoch from before it beside a lease bit from
        // after. `None` means a produce holds the view, and the previous
        // values stand (module docs).
        //
        // The metadata snapshot is read BEFORE the held epoch, deliberately.
        // Promotion writes the held epoch first, then activates the metadata
        // view, and both are monotonic — so a snapshot showing epoch E active
        // proves the held epoch is already at least E, and `1` below is never
        // reported for a broker still mid-promotion (which would claim
        // ownership while the next produce gets refused). The reverse order
        // could do exactly that.
        let meta_snapshot = self.broker.meta_fencing_epoch().try_snapshot();
        let held = self.broker.held_fencing_epoch();
        self.held_fencing_epoch
            .with_label_values(&range)
            .set(held as i64);
        if let Some((meta_epoch, lease_active)) = meta_snapshot {
            self.meta_fencing_epoch
                .with_label_values(&range)
                .set(meta_epoch as i64);
            // Authorization, not merely "some lease exists somewhere". After a
            // steal the metadata lease is live for the NEW holder while this
            // broker is fenced, and a `1` here would tell an operator the
            // opposite of what the broker will do with the next produce.
            self.lease_active
                .with_label_values(&range)
                .set(i64::from(lease_active && meta_epoch == held));
        }

        if let Some(replicas) = self.replicas.as_ref() {
            for node in &self.followers {
                let label = node.to_string();
                let durable = replicas.follower_durable_offset(*node);
                if let Some(durable) = durable {
                    self.follower_durable_offset
                        .with_label_values(&[&label])
                        .set(durable as i64);
                }
                // Lag needs both ends; if the leader's boundary was contended
                // this scrape, skip rather than publish a lag computed against
                // a stale leader offset.
                if let (Some(durable), Some(committed)) = (durable, leader_committed) {
                    self.follower_lag_records
                        .with_label_values(&[&label])
                        .set(committed.saturating_sub(durable) as i64);
                }
                self.follower_connected
                    .with_label_values(&[&label])
                    .set(i64::from(
                        replicas.follower_connected(*node).unwrap_or(false),
                    ));
            }
        }

        if let Some(coordinator) = self.broker.group_commit() {
            let last = coordinator.metrics().last_sample();
            self.group_commit_last_batch_records
                .set(last.records as i64);
            self.group_commit_last_batch_bytes.set(last.bytes as i64);
        }

        let budget = self.broker.memory_budget().metrics();
        for (scope, used) in [
            ("process", budget.process_used_bytes()),
            ("shard", budget.shard_used_bytes()),
            ("fetch_queue", budget.fetch_queue_used_bytes()),
            ("replica", budget.replica_used_bytes()),
        ] {
            self.memory_used_bytes
                .with_label_values(&[scope])
                .set(used as i64);
        }
        self.memory_queue_depth.set(budget.queue_depth() as i64);
    }

    /// Counter families read straight from the broker's monotonic atomics.
    fn counter_families(&self) -> Vec<MetricFamily> {
        let mut families = Vec::new();
        if let Some(coordinator) = self.broker.group_commit() {
            let metrics = coordinator.metrics();
            for (family, total) in [
                (&self.group_commits, metrics.commits_total()),
                (&self.group_commit_requests, metrics.requests_total()),
                (&self.group_commit_records, metrics.records_total()),
                (&self.group_commit_bytes, metrics.bytes_total()),
                (&self.group_commit_sync_nanos, metrics.sync_nanos_total()),
                (
                    &self.group_commit_queue_wait_nanos,
                    metrics.queue_wait_nanos_total(),
                ),
            ] {
                families.push(family.family([(Vec::new(), total)]));
            }
        }

        let budget = self.broker.memory_budget().metrics();
        families.push(
            self.memory_rejections.family(
                BudgetRejectReason::ALL
                    .into_iter()
                    .map(|reason| (vec![reason.as_str()], budget.rejections(reason))),
            ),
        );
        families.push(
            self.backpressure_nanos
                .family([(Vec::new(), budget.backpressure_nanos_total())]),
        );
        families.push(
            self.backpressure_events
                .family([(Vec::new(), budget.backpressure_events())]),
        );
        families
    }

    fn counter_descs(&self) -> Vec<&Desc> {
        vec![
            &self.group_commits.desc,
            &self.group_commit_requests.desc,
            &self.group_commit_records.desc,
            &self.group_commit_bytes.desc,
            &self.group_commit_sync_nanos.desc,
            &self.group_commit_queue_wait_nanos.desc,
            &self.memory_rejections.desc,
            &self.backpressure_nanos.desc,
            &self.backpressure_events.desc,
        ]
    }
}

impl Collector for BrokerCollector {
    fn desc(&self) -> Vec<&Desc> {
        let mut descs = descs_of(&self.members());
        descs.extend(self.counter_descs());
        descs
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.refresh();
        let mut families = collect_all(&self.members());
        families.extend(self.counter_families());
        families
    }
}

// ---------------------------------------------------------------------------
// Data plane, follower
// ---------------------------------------------------------------------------

/// A follower's own view: how far its disk has gone, and whether it is
/// accepting replication at all.
pub struct FollowerCollector {
    follower: Arc<InProcessFollower>,
    range_labels: [String; 2],
    local_committed: IntGaugeVec,
    next_offset: IntGaugeVec,
    cluster_committed: IntGaugeVec,
    meta_fencing_epoch: IntGaugeVec,
    online: IntGaugeVec,
}

impl FollowerCollector {
    pub fn new(follower: Arc<InProcessFollower>) -> Result<Self, String> {
        let range = follower.range();
        let range_labels = [range.topic.clone(), range.range_id.to_string()];
        Ok(Self {
            follower,
            range_labels,
            local_committed: gauge_vec(
                "broker_local_committed_offset",
                "Durable commit boundary of this replica's active segment",
                &["topic", "range"],
            )?,
            next_offset: gauge_vec(
                "broker_next_offset",
                "Next offset the segment will assign, including records not yet durable",
                &["topic", "range"],
            )?,
            cluster_committed: gauge_vec(
                "broker_cluster_committed_offset",
                "Quorum-committed high-water mark this follower has observed",
                &["topic", "range"],
            )?,
            meta_fencing_epoch: gauge_vec(
                "broker_meta_fencing_epoch",
                "Latest metadata-committed fencing epoch observed for the range",
                &["topic", "range"],
            )?,
            online: gauge_vec(
                "broker_follower_online",
                "1 while this follower accepts replication appends",
                &["topic", "range"],
            )?,
        })
    }

    fn members(&self) -> Vec<&dyn Collector> {
        vec![
            &self.local_committed,
            &self.next_offset,
            &self.cluster_committed,
            &self.meta_fencing_epoch,
            &self.online,
        ]
    }

    fn refresh(&self) {
        let range = [self.range_labels[0].as_str(), self.range_labels[1].as_str()];
        if let Some((committed, next)) = self.follower.try_local_offsets() {
            self.local_committed
                .with_label_values(&range)
                .set(committed as i64);
            self.next_offset.with_label_values(&range).set(next as i64);
        }
        self.cluster_committed
            .with_label_values(&range)
            .set(self.follower.cluster_committed().get() as i64);
        // Non-blocking for the same reason as the leader's collector, and for
        // the same failure: `InProcessFollower::apply_append` takes this lock
        // before the append and holds it through `append_group(.., Fsync)`, so
        // a follower whose disk has stalled would park every scrape on a
        // runtime worker instead of serving the last known value.
        if let Some((epoch, _)) = self.follower.meta_fencing_epoch().try_snapshot() {
            self.meta_fencing_epoch
                .with_label_values(&range)
                .set(epoch as i64);
        }
        self.online
            .with_label_values(&range)
            .set(i64::from(self.follower.is_online()));
    }
}

impl Collector for FollowerCollector {
    fn desc(&self) -> Vec<&Desc> {
        descs_of(&self.members())
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.refresh();
        collect_all(&self.members())
    }
}

// ---------------------------------------------------------------------------
// Data plane, candidate (role follows the lease)
// ---------------------------------------------------------------------------

/// What a candidate's collector reads, whichever role currently owns the
/// range (#284).
///
/// NON-BLOCKING throughout, for the reason the module docs give: both role
/// objects hold their state lock across fsync, and a scrape that parks a
/// runtime worker behind a stalled disk takes the observability endpoint
/// down exactly when it is needed most. This is deliberately NOT
/// [`crate::lease_agent::CandidateLocalView`], whose accessors block on
/// purpose — a promotion probe must have the true offset and can afford to
/// wait for it; a scrape must never wait and can afford a stale gauge.
pub trait ReplicaObservation: Send + Sync {
    /// `(committed, next)`, or `None` while the append path holds the lock.
    fn try_local_offsets(&self) -> Option<(u64, u64)>;
    /// The quorum high-water mark, where the role tracks one.
    fn cluster_committed_offset(&self) -> Option<u64>;
    /// `(epoch, lease_active)`, or `None` under contention — one snapshot for
    /// both, so the pair cannot straddle a grant.
    fn try_meta_fencing_epoch(&self) -> Option<(u64, bool)>;
    /// The epoch this role object was granted or adopted.
    fn held_fencing_epoch(&self) -> u64;
    /// Whether this replica currently SERVES the range. The installed role
    /// object is the answer: a broker leads, a follower does not.
    fn is_leading(&self) -> bool;
}

impl ReplicaObservation for LocalBroker {
    fn try_local_offsets(&self) -> Option<(u64, u64)> {
        LocalBroker::try_local_offsets(self)
    }

    fn cluster_committed_offset(&self) -> Option<u64> {
        self.cluster_committed().map(|cluster| cluster.get())
    }

    fn try_meta_fencing_epoch(&self) -> Option<(u64, bool)> {
        self.meta_fencing_epoch().try_snapshot()
    }

    fn held_fencing_epoch(&self) -> u64 {
        LocalBroker::held_fencing_epoch(self)
    }

    fn is_leading(&self) -> bool {
        true
    }
}

impl ReplicaObservation for InProcessFollower {
    fn try_local_offsets(&self) -> Option<(u64, u64)> {
        InProcessFollower::try_local_offsets(self)
    }

    fn cluster_committed_offset(&self) -> Option<u64> {
        Some(self.cluster_committed().get())
    }

    fn try_meta_fencing_epoch(&self) -> Option<(u64, bool)> {
        self.meta_fencing_epoch().try_snapshot()
    }

    fn held_fencing_epoch(&self) -> u64 {
        InProcessFollower::held_fencing_epoch(self)
    }

    fn is_leading(&self) -> bool {
        false
    }
}

/// A candidate's range progress, read through whichever role holds it now.
///
/// ONE collector for the life of the process, which is what makes candidate
/// mode observable at all. Registering `BrokerCollector` on promotion and
/// `FollowerCollector` on demotion cannot work: the two export the same
/// descriptors, the registry refuses duplicates, and there is no unregister
/// — so the first transition would either fail or leave the process
/// exporting a role it no longer has. This collector is registered once and
/// reads through the switching view, so the metric names stay stable across
/// every transition and a dashboard does not have to know which pod
/// currently leads.
///
/// The names match the leader's and the follower's deliberately: a panel
/// built for a statically-rendered range keeps working against a candidate
/// deployment, which is the whole point of retiring the rendered role.
pub struct CandidateCollector {
    view: Arc<dyn ReplicaObservation>,
    range_labels: [String; 2],
    local_committed: IntGaugeVec,
    next_offset: IntGaugeVec,
    cluster_committed: IntGaugeVec,
    held_fencing_epoch: IntGaugeVec,
    meta_fencing_epoch: IntGaugeVec,
    lease_active: IntGaugeVec,
    leading: IntGaugeVec,
}

impl CandidateCollector {
    pub fn new(
        view: Arc<dyn ReplicaObservation>,
        range: &vtop_protocol::RangeIdentity,
    ) -> Result<Self, String> {
        Ok(Self {
            view,
            range_labels: [range.topic.clone(), range.range_id.to_string()],
            local_committed: gauge_vec(
                "broker_local_committed_offset",
                "Durable commit boundary of this replica's active segment",
                &["topic", "range"],
            )?,
            next_offset: gauge_vec(
                "broker_next_offset",
                "Next offset the segment will assign, including records not yet durable",
                &["topic", "range"],
            )?,
            cluster_committed: gauge_vec(
                "broker_cluster_committed_offset",
                "Quorum-committed high-water mark this replica has observed",
                &["topic", "range"],
            )?,
            held_fencing_epoch: gauge_vec(
                "broker_held_fencing_epoch",
                "Fencing epoch this replica was granted or has adopted",
                &["topic", "range"],
            )?,
            meta_fencing_epoch: gauge_vec(
                "broker_meta_fencing_epoch",
                "Latest metadata-committed fencing epoch observed for the range",
                &["topic", "range"],
            )?,
            lease_active: gauge_vec(
                "broker_lease_active",
                "1 while THIS replica is the authorized leaseholder; 0 while it \
                 follows another holder or is fenced",
                &["topic", "range"],
            )?,
            leading: gauge_vec(
                "broker_candidate_leading",
                "1 while this candidate serves the range; 0 while it follows another holder",
                &["topic", "range"],
            )?,
        })
    }

    fn members(&self) -> Vec<&dyn Collector> {
        vec![
            &self.local_committed,
            &self.next_offset,
            &self.cluster_committed,
            &self.held_fencing_epoch,
            &self.meta_fencing_epoch,
            &self.lease_active,
            &self.leading,
        ]
    }

    fn refresh(&self) {
        let range = [self.range_labels[0].as_str(), self.range_labels[1].as_str()];
        // A contended read leaves the previous value standing (module docs).
        // Mid-transition the switching view holds no role at all and answers
        // the same way — the gauges pause for the length of a transition
        // rather than reporting a zero the replica never went back to.
        if let Some((committed, next)) = self.view.try_local_offsets() {
            self.local_committed
                .with_label_values(&range)
                .set(committed as i64);
            self.next_offset.with_label_values(&range).set(next as i64);
        }
        if let Some(cluster) = self.view.cluster_committed_offset() {
            self.cluster_committed
                .with_label_values(&range)
                .set(cluster as i64);
        }
        // The metadata snapshot BEFORE the held epoch, matching the leader's
        // collector for the reason recorded there: both writes are monotonic
        // and promotion orders them, so this order can never report ownership
        // for a replica still mid-promotion.
        let snapshot = self.view.try_meta_fencing_epoch();
        let held = self.view.held_fencing_epoch();
        let leading = self.view.is_leading();
        self.held_fencing_epoch
            .with_label_values(&range)
            .set(held as i64);
        if let Some((epoch, active)) = snapshot {
            self.meta_fencing_epoch
                .with_label_values(&range)
                .set(epoch as i64);
            // AUTHORIZATION, exactly as the leader's collector defines it —
            // and with one more term, because a candidate can be neither
            // fenced nor the holder. A FOLLOWING candidate adopts the
            // holder's granted epoch and activates its view at it (that is
            // how it accepts replication), so `active && epoch == held` is
            // true on all three replicas at once: without the role term this
            // gauge would report three leaseholders for one range and tell
            // an operator the opposite of what two of them will do with a
            // produce (review).
            self.lease_active
                .with_label_values(&range)
                .set(i64::from(active && epoch == held && leading));
        }
        self.leading
            .with_label_values(&range)
            .set(i64::from(leading));
    }
}

impl Collector for CandidateCollector {
    fn desc(&self) -> Vec<&Desc> {
        descs_of(&self.members())
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.refresh();
        collect_all(&self.members())
    }
}

// ---------------------------------------------------------------------------
// Native server request path
// ---------------------------------------------------------------------------

/// Request rates, latency quantiles, and session counts from the native
/// server's own atomics.
///
/// The latency families are built as protobuf by hand rather than mirrored into
/// a `prometheus::Histogram`: the broker records bucket counts in atomics (it
/// owns no Prometheus types by design), and there is no API to push existing
/// counts into a `Histogram`. Emitting the family directly is the honest
/// translation; re-observing every sample at scrape time would be a fabrication
/// and would also lose the sum.
pub struct ServerCollector {
    metrics: Arc<ServerMetrics>,
    sessions_active: IntGaugeVec,
    requests: CounterFamily,
    sessions_accepted: CounterFamily,
    sessions_refused: CounterFamily,
    produced_records: CounterFamily,
    produced_bytes: CounterFamily,
    fetched_records: CounterFamily,
    fetched_bytes: CounterFamily,
    latency_desc: Desc,
}

/// Name of the request-latency family, before the registry's `vtop_` prefix.
const LATENCY_METRIC: &str = "broker_request_duration_seconds";

impl ServerCollector {
    pub fn new(metrics: Arc<ServerMetrics>) -> Result<Self, String> {
        Ok(Self {
            metrics,
            sessions_active: gauge_vec(
                "broker_sessions_active",
                "Authenticated sessions currently open, by role",
                &["role"],
            )?,
            requests: CounterFamily::new(
                "broker_requests_total",
                "Requests the broker answered, by request kind and whether it was served or \
                 refused",
                &["kind", "outcome"],
            )?,
            sessions_accepted: CounterFamily::new(
                "broker_sessions_accepted_total",
                "Sessions that completed authorization and negotiation, by role",
                &["role"],
            )?,
            sessions_refused: CounterFamily::new(
                "broker_sessions_refused_total",
                "Connections that never became a session, by why they were turned away",
                &["reason"],
            )?,
            produced_records: CounterFamily::new(
                "broker_produced_records_total",
                "Records accepted by successful produce requests",
                &[],
            )?,
            produced_bytes: CounterFamily::new(
                "broker_produced_bytes_total",
                "Key and value bytes accepted by successful produce requests",
                &[],
            )?,
            fetched_records: CounterFamily::new(
                "broker_fetched_records_total",
                "Records returned by successful fetch requests",
                &[],
            )?,
            fetched_bytes: CounterFamily::new(
                "broker_fetched_bytes_total",
                "Key and value bytes returned by successful fetch requests",
                &[],
            )?,
            latency_desc: Desc::new(
                LATENCY_METRIC.to_string(),
                "Time the broker held a request, measured around its own work and not the \
                 socket write, so a slow consumer's backpressure cannot be mistaken for a slow \
                 log"
                .to_string(),
                vec!["kind".to_string()],
                Default::default(),
            )
            .map_err(|error| format!("build {LATENCY_METRIC}: {error}"))?,
        })
    }

    fn members(&self) -> Vec<&dyn Collector> {
        vec![&self.sessions_active]
    }

    fn refresh(&self) {
        for role in server_metrics::ROLES {
            self.sessions_active
                .with_label_values(&[server_metrics::role_label(role)])
                .set(self.metrics.sessions_active(role) as i64);
        }
    }

    /// Counter families read straight from the server's monotonic atomics.
    fn counter_families(&self) -> Vec<MetricFamily> {
        vec![
            self.requests
                .family(RequestKind::ALL.into_iter().flat_map(|kind| {
                    RequestOutcome::ALL.into_iter().map(move |outcome| {
                        (
                            vec![kind.as_str(), outcome.as_str()],
                            self.metrics.requests_total(kind, outcome),
                        )
                    })
                })),
            self.sessions_accepted
                .family(server_metrics::ROLES.into_iter().map(|role| {
                    (
                        vec![server_metrics::role_label(role)],
                        self.metrics.sessions_accepted(role),
                    )
                })),
            self.sessions_refused.family([
                (
                    vec!["capacity"],
                    self.metrics.sessions_refused_at_capacity_total(),
                ),
                (
                    vec!["unauthorized"],
                    self.metrics.sessions_refused_unauthorized_total(),
                ),
                (
                    vec!["handshake"],
                    self.metrics.sessions_refused_handshake_total(),
                ),
                // A candidate that is not currently leading (#284): the
                // remedy is "produce to the leader", which is the opposite
                // of capacity's "scale or shed".
                (
                    vec!["no_broker"],
                    self.metrics.sessions_refused_no_broker_total(),
                ),
            ]),
            self.produced_records
                .family([(Vec::new(), self.metrics.records_produced_total())]),
            self.produced_bytes
                .family([(Vec::new(), self.metrics.bytes_produced_total())]),
            self.fetched_records
                .family([(Vec::new(), self.metrics.records_fetched_total())]),
            self.fetched_bytes
                .family([(Vec::new(), self.metrics.bytes_fetched_total())]),
        ]
    }

    /// Emit one histogram family carrying every request kind as a series.
    fn latency_family(&self) -> MetricFamily {
        let mut family = MetricFamily::default();
        family.set_name(LATENCY_METRIC.to_string());
        family.set_help(self.latency_desc.help.clone());
        family.set_field_type(prometheus::proto::MetricType::HISTOGRAM);

        let mut series = Vec::with_capacity(RequestKind::ALL.len());
        for kind in RequestKind::ALL {
            let snapshot = self.metrics.latency(kind);
            let mut histogram = prometheus::proto::Histogram::default();
            histogram.set_sample_count(snapshot.count);
            histogram.set_sample_sum(snapshot.total_seconds);
            histogram.set_bucket(
                LATENCY_BUCKETS_MICROS
                    .iter()
                    .zip(snapshot.cumulative_counts.iter())
                    .map(|(bound_micros, cumulative)| {
                        let mut bucket = prometheus::proto::Bucket::default();
                        // Prometheus bounds are seconds; the broker's ladder is
                        // in microseconds because that is the resolution the
                        // data path is measured at.
                        bucket.set_upper_bound(*bound_micros as f64 / 1e6);
                        bucket.set_cumulative_count(*cumulative);
                        bucket
                    })
                    .collect(),
            );

            let mut label = prometheus::proto::LabelPair::default();
            label.set_name("kind".to_string());
            label.set_value(kind.as_str().to_string());
            let mut metric = prometheus::proto::Metric::default();
            metric.set_label(vec![label]);
            metric.set_histogram(histogram);
            series.push(metric);
        }
        family.set_metric(series);
        family
    }
}

impl Collector for ServerCollector {
    fn desc(&self) -> Vec<&Desc> {
        let mut descs = descs_of(&self.members());
        descs.extend([
            &self.requests.desc,
            &self.sessions_accepted.desc,
            &self.sessions_refused.desc,
            &self.produced_records.desc,
            &self.produced_bytes.desc,
            &self.fetched_records.desc,
            &self.fetched_bytes.desc,
            &self.latency_desc,
        ]);
        descs
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.refresh();
        let mut families = collect_all(&self.members());
        families.extend(self.counter_families());
        families.push(self.latency_family());
        families
    }
}

// ---------------------------------------------------------------------------
// Integrity
// ---------------------------------------------------------------------------

/// What recovery had to throw away when this segment was re-opened.
///
/// The differentiator neither Kafka nor Northguard exposes: an operator can
/// alert on *verification* health, not just throughput. A non-zero
/// `truncated_bytes` after a crash is expected and benign — it is the torn tail
/// beyond the durable boundary — but a node that truncates on every restart is
/// telling you its fsync story is wrong.
pub struct SegmentRecoveryCollector {
    truncated_bytes: IntGauge,
    recovered_bytes: IntGauge,
    recovered_records: IntGauge,
    recovered: IntGauge,
}

impl SegmentRecoveryCollector {
    /// `report` is `None` when the segment was created fresh rather than
    /// recovered, which is reported as `broker_segment_recovered=0` instead of
    /// silently exporting zeros that look like a clean recovery.
    pub fn new(report: Option<&RecoveryReport>) -> Result<Self, String> {
        let collector = Self {
            truncated_bytes: gauge(
                "broker_segment_recovery_truncated_bytes",
                "Bytes discarded past the durable boundary when this segment was re-opened",
            )?,
            recovered_bytes: gauge(
                "broker_segment_recovery_recovered_bytes",
                "Bytes accepted as durable when this segment was re-opened",
            )?,
            recovered_records: gauge(
                "broker_segment_recovery_recovered_records",
                "Records accepted as durable when this segment was re-opened",
            )?,
            recovered: gauge(
                "broker_segment_recovered",
                "1 when this process re-opened an existing segment, 0 when it created a fresh one",
            )?,
        };
        match report {
            Some(report) => {
                collector.recovered.set(1);
                collector.truncated_bytes.set(report.truncated_bytes as i64);
                collector.recovered_bytes.set(report.recovered_bytes as i64);
                collector.recovered_records.set(report.records as i64);
            }
            None => collector.recovered.set(0),
        }
        Ok(collector)
    }

    fn members(&self) -> Vec<&dyn Collector> {
        vec![
            &self.truncated_bytes,
            &self.recovered_bytes,
            &self.recovered_records,
            &self.recovered,
        ]
    }
}

impl Collector for SegmentRecoveryCollector {
    fn desc(&self) -> Vec<&Desc> {
        descs_of(&self.members())
    }

    fn collect(&self) -> Vec<MetricFamily> {
        collect_all(&self.members())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{Encoder, TextEncoder};

    fn scrape(registry: &Registry) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn node_info_carries_the_role_and_prefixes_with_vtop() {
        let observability = NodeObservability::new("meta", "7").unwrap();
        let text = scrape(&observability.registry);
        assert!(
            text.contains(r#"vtop_node_info{node_id="7",role="meta"} 1"#),
            "{text}"
        );
    }

    /// A candidate exports range progress under the SAME metric names a
    /// statically-rendered leader or follower does, through whichever role
    /// currently holds the range.
    ///
    /// This pins a real regression: candidate mode registered no role
    /// collector at all, so `vtop_broker_local_committed_offset` — the metric
    /// the k8s smoke, the chaos harness and every dashboard read — simply did
    /// not exist on a candidate pod. A replica nobody can measure is one
    /// nobody can operate, and the gap was invisible until a live cluster
    /// asked the question.
    #[test]
    fn a_candidate_exports_range_progress_through_whichever_role_holds_it() {
        struct Role {
            offsets: Option<(u64, u64)>,
            leading: bool,
        }
        impl ReplicaObservation for Role {
            fn try_local_offsets(&self) -> Option<(u64, u64)> {
                self.offsets
            }
            fn cluster_committed_offset(&self) -> Option<u64> {
                Some(40)
            }
            fn try_meta_fencing_epoch(&self) -> Option<(u64, bool)> {
                Some((3, true))
            }
            fn held_fencing_epoch(&self) -> u64 {
                3
            }
            fn is_leading(&self) -> bool {
                self.leading
            }
        }

        let range = test_range();
        // The node's OWN registry, not a bare one: the `vtop_` prefix is
        // applied by the registry, and a test that scrapes an unprefixed
        // registry would assert a name no operator ever sees.
        let registry = Registry::new_custom(Some("vtop".into()), None).unwrap();
        let leading = Arc::new(Role {
            offsets: Some((41, 42)),
            leading: true,
        });
        let collector =
            CandidateCollector::new(Arc::clone(&leading) as Arc<dyn ReplicaObservation>, &range)
                .unwrap();
        registry.register(Box::new(collector)).unwrap();
        let text = scrape(&registry);
        assert!(
            text.contains(&format!(
                r#"vtop_broker_local_committed_offset{{range="{}",topic="{}"}} 41"#,
                range.range_id, range.topic
            )),
            "a leading candidate must export the durable boundary under the \
             shared name, so a dashboard built for a rendered leader keeps \
             working: {text}"
        );
        assert!(
            text.contains("vtop_broker_candidate_leading"),
            "the one question candidate mode adds — who serves the range — \
             must be answerable from metrics: {text}"
        );
        assert!(
            text.lines()
                .any(|line| line.starts_with("vtop_broker_candidate_leading")
                    && line.ends_with(" 1")),
            "the installed role is the answer, and this one leads: {text}"
        );

        // A FOLLOWING candidate reports the same names with leading=0, and a
        // contended (or mid-transition) read leaves the last value standing
        // rather than reporting a zero the replica never returned to.
        let following = Registry::new_custom(Some("vtop".into()), None).unwrap();
        let follower = Arc::new(Role {
            offsets: None,
            leading: false,
        });
        following
            .register(Box::new(
                CandidateCollector::new(
                    Arc::clone(&follower) as Arc<dyn ReplicaObservation>,
                    &range,
                )
                .unwrap(),
            ))
            .unwrap();
        let text = scrape(&following);
        assert!(
            text.lines()
                .any(|line| line.starts_with("vtop_broker_candidate_leading")
                    && line.ends_with(" 0")),
            "a following candidate must say so: {text}"
        );
        assert!(
            !text.contains("vtop_broker_local_committed_offset"),
            "an offset that could not be read must be ABSENT, never zero: a \
             zero here reads as an empty replica and would send an operator \
             hunting a replication failure that did not happen: {text}"
        );
        // A following candidate adopts the HOLDER's epoch and activates its
        // view at it — that is how it accepts replication — so every term of
        // the leader's own authorization test is satisfied on all three
        // replicas at once. Only the role separates them, and without it this
        // gauge would report three leaseholders for one range.
        assert!(
            text.lines()
                .any(|line| line.starts_with("vtop_broker_lease_active{") && line.ends_with(" 0")),
            "a following candidate must NOT report itself the authorized \
             leaseholder, however live the lease it observes: {text}"
        );
    }

    #[test]
    fn a_fresh_node_is_not_ready_until_it_says_so() {
        let observability = NodeObservability::new("data", "abc").unwrap();
        assert!(!observability.gate.is_ready());
        observability.gate.mark_ready();
        assert!(observability.gate.is_ready());
    }

    /// Repeated collection must be idempotent. A delta-mirroring exporter
    /// would pass a single scrape and then drift on every one after it, which
    /// is exactly the bug this design removes.
    #[test]
    fn a_counter_family_exports_the_source_total_however_often_it_is_read() {
        let family = CounterFamily::new("probe_total", "probe", &["reason"]).unwrap();
        for _ in 0..3 {
            let emitted = family.family([(vec!["disk"], 9_u64)]);
            assert_eq!(emitted.get_metric().len(), 1);
            assert_eq!(emitted.get_metric()[0].get_counter().get_value(), 9.0);
            assert_eq!(emitted.get_metric()[0].get_label()[0].value(), "disk");
        }
    }

    #[test]
    fn a_fresh_segment_is_distinguishable_from_a_recovered_one() {
        let registry = Registry::new_custom(Some("vtop".into()), None).unwrap();
        registry
            .register(Box::new(SegmentRecoveryCollector::new(None).unwrap()))
            .unwrap();
        let text = scrape(&registry);
        assert!(text.contains("vtop_broker_segment_recovered 0"), "{text}");
    }

    #[test]
    fn a_recovery_report_is_exported_verbatim() {
        let registry = Registry::new_custom(Some("vtop".into()), None).unwrap();
        let report = RecoveryReport {
            records: 12,
            recovered_bytes: 3400,
            truncated_bytes: 17,
            next_offset: 12,
        };
        registry
            .register(Box::new(
                SegmentRecoveryCollector::new(Some(&report)).unwrap(),
            ))
            .unwrap();
        let text = scrape(&registry);
        assert!(text.contains("vtop_broker_segment_recovered 1"), "{text}");
        assert!(
            text.contains("vtop_broker_segment_recovery_truncated_bytes 17"),
            "a torn tail must be visible, not rounded away: {text}"
        );
    }

    fn observation() -> RaftObservation {
        RaftObservation {
            node_id: vtop_meta::MetaNodeId(2),
            running: true,
            current_term: 5,
            server_state: RaftServerState::Leader,
            current_leader: Some(vtop_meta::MetaNodeId(2)),
            last_log_index: Some(40),
            last_applied_index: Some(39),
            snapshot_index: Some(20),
            purged_index: None,
            voters: 3,
            learners: 1,
            millis_since_quorum_ack: Some(12),
            peer_matched_index: [
                (vtop_meta::MetaNodeId(1), Some(38)),
                (vtop_meta::MetaNodeId(3), Some(40)),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn meta_registry(collector: MetaRaftGauges) -> Registry {
        let registry = Registry::new_custom(Some("vtop".into()), None).unwrap();
        registry.register(Box::new(collector)).unwrap();
        registry
    }

    /// The state label is one-hot, so `sum by (state)` across a cluster counts
    /// leaders directly — the query an operator writes first.
    #[test]
    fn exactly_one_raft_state_series_is_hot() {
        let collector = MetaRaftGauges::new().unwrap();
        collector.publish(&observation());
        let text = scrape(&meta_registry(collector));
        assert!(
            text.contains(r#"vtop_meta_raft_state{state="leader"} 1"#),
            "{text}"
        );
        for cold in ["learner", "follower", "candidate", "shutdown"] {
            assert!(
                text.contains(&format!(r#"vtop_meta_raft_state{{state="{cold}"}} 0"#)),
                "state {cold} must be explicitly 0, not absent: {text}"
            );
        }
    }

    /// Follower lag is the number an operator acts on; it must be derived, not
    /// left for a dashboard expression to reconstruct.
    #[test]
    fn peer_lag_is_measured_against_the_leaders_last_index() {
        let collector = MetaRaftGauges::new().unwrap();
        collector.publish(&observation());
        let text = scrape(&meta_registry(collector));
        assert!(
            text.contains(r#"vtop_meta_raft_peer_lag_entries{peer="1"} 2"#),
            "{text}"
        );
        assert!(
            text.contains(r#"vtop_meta_raft_peer_lag_entries{peer="3"} 0"#),
            "{text}"
        );
    }

    /// A node that stops leading must stop publishing follower progress, or a
    /// dashboard keeps showing the lag it saw at the moment of the failover.
    #[test]
    fn losing_leadership_clears_stale_peer_series() {
        let collector = MetaRaftGauges::new().unwrap();
        collector.publish(&observation());
        let demoted = RaftObservation {
            server_state: RaftServerState::Follower,
            current_leader: Some(vtop_meta::MetaNodeId(3)),
            millis_since_quorum_ack: None,
            peer_matched_index: Default::default(),
            ..observation()
        };
        collector.publish(&demoted);
        let text = scrape(&meta_registry(collector));
        assert!(
            !text.contains("vtop_meta_raft_peer_lag_entries"),
            "a demoted node must publish no peer lag at all: {text}"
        );
        assert!(
            text.contains("vtop_meta_raft_millis_since_quorum_ack -1"),
            "not-leading must read as absent (-1), never as a healthy 0: {text}"
        );
    }

    /// An absent index must not be reported as 0; index 0 is a real value in
    /// the neighbouring series and the two would be indistinguishable.
    #[test]
    fn an_absent_index_is_reported_as_absent() {
        let collector = MetaRaftGauges::new().unwrap();
        collector.publish(&observation());
        let text = scrape(&meta_registry(collector));
        assert!(text.contains("vtop_meta_raft_purged_index -1"), "{text}");
    }

    /// A learner that has acknowledged nothing is not a peer sitting at the
    /// first index. Reporting 0 would show a brand-new replica as making real
    /// progress, and its lag as the leader's whole log.
    #[test]
    fn a_peer_that_has_never_acknowledged_is_absent_not_zero() {
        let collector = MetaRaftGauges::new().unwrap();
        let mut observation = observation();
        observation
            .peer_matched_index
            .insert(vtop_meta::MetaNodeId(4), None);
        collector.publish(&observation);
        let text = scrape(&meta_registry(collector));
        assert!(
            text.contains(r#"vtop_meta_raft_peer_matched_index{peer="4"} -1"#),
            "{text}"
        );
        assert!(
            text.contains(r#"vtop_meta_raft_peer_lag_entries{peer="4"} -1"#),
            "unknown lag must read as unknown, not as the whole log: {text}"
        );
    }

    fn server_registry(metrics: Arc<ServerMetrics>) -> Registry {
        let registry = Registry::new_custom(Some("vtop".into()), None).unwrap();
        registry
            .register(Box::new(ServerCollector::new(metrics).unwrap()))
            .unwrap();
        registry
    }

    /// The hand-built histogram family has to satisfy the same shape a
    /// `prometheus::Histogram` would, or `histogram_quantile()` returns nothing
    /// and every p99 panel is empty while looking perfectly healthy.
    #[test]
    fn the_latency_family_is_a_well_formed_prometheus_histogram() {
        let metrics = Arc::new(ServerMetrics::new());
        metrics.request_completed(
            RequestKind::Produce,
            RequestOutcome::Ok,
            std::time::Duration::from_micros(400),
        );
        let text = scrape(&server_registry(metrics));

        assert!(
            text.contains("# TYPE vtop_broker_request_duration_seconds histogram"),
            "{text}"
        );
        assert!(
            text.contains(
                r#"vtop_broker_request_duration_seconds_bucket{kind="produce",le="0.0005"} 1"#
            ),
            "a 400us observation belongs to the 500us bucket, in seconds: {text}"
        );
        assert!(
            text.contains(r#"vtop_broker_request_duration_seconds_count{kind="produce"} 1"#),
            "{text}"
        );
        assert!(
            text.contains(r#"vtop_broker_request_duration_seconds_sum{kind="produce"}"#),
            "without _sum an average is unanswerable: {text}"
        );
    }

    /// A refusal is the system working; it must not land in the success series.
    #[test]
    fn served_and_refused_requests_are_counted_apart() {
        let metrics = Arc::new(ServerMetrics::new());
        metrics.request_completed(
            RequestKind::Fetch,
            RequestOutcome::Ok,
            std::time::Duration::from_micros(30),
        );
        metrics.request_completed(
            RequestKind::Produce,
            RequestOutcome::Error,
            std::time::Duration::from_micros(30),
        );
        let text = scrape(&server_registry(metrics));
        assert!(
            text.contains(r#"vtop_broker_requests_total{kind="fetch",outcome="ok"} 1"#),
            "{text}"
        );
        assert!(
            text.contains(r#"vtop_broker_requests_total{kind="produce",outcome="error"} 1"#),
            "{text}"
        );
    }

    #[test]
    fn session_counts_are_exported_per_role() {
        let metrics = Arc::new(ServerMetrics::new());
        metrics.session_opened(vtop_protocol::Role::Producer);
        metrics.session_opened(vtop_protocol::Role::Consumer);
        metrics.session_closed(vtop_protocol::Role::Consumer);
        metrics.session_refused_at_capacity();
        let text = scrape(&server_registry(metrics));
        assert!(
            text.contains(r#"vtop_broker_sessions_active{role="producer"} 1"#),
            "{text}"
        );
        assert!(
            text.contains(r#"vtop_broker_sessions_active{role="consumer"} 0"#),
            "a closed session must read as zero, not vanish: {text}"
        );
        assert!(
            text.contains(r#"vtop_broker_sessions_refused_total{reason="capacity"} 1"#),
            "{text}"
        );
    }

    fn test_range() -> vtop_protocol::RangeIdentity {
        vtop_protocol::RangeIdentity {
            topic: "telemetry".into(),
            topic_epoch: 1,
            range_id: Uuid::from_u128(7),
            // A root range: generation 0 is the only lineage with no parents.
            range_generation: 0,
        }
    }

    fn open_broker(dir: &std::path::Path) -> LocalBroker {
        let range = test_range();
        let descriptor = vtop_log::SegmentDescriptor {
            segment_id: Uuid::from_u128(9),
            topic: range.topic.clone(),
            topic_epoch: range.topic_epoch,
            lineage: vtop_log::RangeLineage {
                range_id: range.range_id,
                generation: range.range_generation,
                key_range: vtop_log::KeyRange::full(),
                parents: Vec::new(),
            },
            base_offset: 0,
        };
        let segment = vtop_log::ActiveSegment::create(
            dir.join("range.active"),
            descriptor,
            vtop_log::SegmentConfig::default(),
        )
        .unwrap();
        let epochs = vtop_broker::ProducerEpochJournal::open(dir.join("epochs")).unwrap();
        LocalBroker::new(segment, epochs, range, 4).unwrap()
    }

    /// The whole point of the leader metrics: an operator can see the range's
    /// durable boundary and its fencing state without reading a log file.
    #[test]
    fn a_leader_exports_its_range_state() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Arc::new(open_broker(dir.path()));
        let registry = Registry::new_custom(Some("vtop".into()), None).unwrap();
        registry
            .register(Box::new(
                BrokerCollector::new(Arc::clone(&broker), None, Vec::new()).unwrap(),
            ))
            .unwrap();

        let text = scrape(&registry);
        assert!(
            text.contains(r#"vtop_broker_held_fencing_epoch{range="00000000-0000-0000-0000-000000000007",topic="telemetry"} 4"#),
            "{text}"
        );
        assert!(
            text.contains(r#"vtop_broker_lease_active{range="00000000-0000-0000-0000-000000000007",topic="telemetry"} 1"#),
            "a live leaseholder must read as leased: {text}"
        );
    }

    /// The one that matters most: after a steal, metadata's lease is live for
    /// the NEW holder. Exporting "a lease is active" would tell an operator
    /// this broker can take writes at the exact moment it started refusing
    /// them.
    #[test]
    fn a_stolen_lease_is_not_reported_as_this_brokers_lease() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Arc::new(open_broker(dir.path()));
        let registry = Registry::new_custom(Some("vtop".into()), None).unwrap();
        registry
            .register(Box::new(
                BrokerCollector::new(Arc::clone(&broker), None, Vec::new()).unwrap(),
            ))
            .unwrap();

        // A newer grant: the lease is active, but for someone else.
        broker.meta_fencing_epoch().set(9);
        let text = scrape(&registry);
        assert!(
            text.contains(r#"vtop_broker_lease_active{range="00000000-0000-0000-0000-000000000007",topic="telemetry"} 0"#),
            "a fenced holder must not advertise the winner's lease as its own: {text}"
        );
        assert!(
            text.contains(r#"vtop_broker_meta_fencing_epoch{range="00000000-0000-0000-0000-000000000007",topic="telemetry"} 9"#),
            "the epoch that fenced it must still be visible: {text}"
        );
    }

    /// The collector must not park a runtime worker behind a produce that is
    /// mid-fsync — the module's central contract.
    #[test]
    fn a_contended_lease_view_leaves_the_previous_values_standing() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Arc::new(open_broker(dir.path()));
        let registry = Registry::new_custom(Some("vtop".into()), None).unwrap();
        registry
            .register(Box::new(
                BrokerCollector::new(Arc::clone(&broker), None, Vec::new()).unwrap(),
            ))
            .unwrap();
        // Prime the gauges, then hold the view exactly as an append does.
        let _primed = scrape(&registry);
        let held = broker.meta_fencing_epoch().hold_for_test();

        let text = scrape(&registry);
        drop(held);
        assert!(
            text.contains(r#"vtop_broker_lease_active{range="00000000-0000-0000-0000-000000000007",topic="telemetry"} 1"#),
            "a contended scrape must serve the last known value, not block: {text}"
        );
    }

    /// Fencing must be visible the instant metadata revokes the lease — this is
    /// the signal that separates "leader is down" from "leader is no longer
    /// allowed to be leader".
    #[test]
    fn revoking_the_lease_shows_up_on_the_very_next_scrape() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Arc::new(open_broker(dir.path()));
        let registry = Registry::new_custom(Some("vtop".into()), None).unwrap();
        registry
            .register(Box::new(
                BrokerCollector::new(Arc::clone(&broker), None, Vec::new()).unwrap(),
            ))
            .unwrap();

        broker.meta_fencing_epoch().clear_lease(4);
        let text = scrape(&registry);
        assert!(
            text.contains(r#"vtop_broker_lease_active{range="00000000-0000-0000-0000-000000000007",topic="telemetry"} 0"#),
            "a fenced leaseholder must stop reading as leased: {text}"
        );
    }

    /// Cardinality guard, mirroring the archive engine's contract test: an
    /// unbounded label would grow the TSDB without limit.
    #[test]
    fn every_node_label_is_from_a_bounded_set() {
        const BOUNDED: [&str; 8] = [
            "role", "node_id", "state", "reason", "scope", "topic", "range", "follower",
        ];
        let observability = NodeObservability::new("data", "abc").unwrap();
        let report = RecoveryReport {
            records: 1,
            recovered_bytes: 1,
            truncated_bytes: 0,
            next_offset: 1,
        };
        observability
            .register(Box::new(
                SegmentRecoveryCollector::new(Some(&report)).unwrap(),
            ))
            .unwrap();
        let text = scrape(&observability.registry);
        let mut seen = std::collections::BTreeSet::new();
        for line in text.lines().filter(|l| !l.starts_with('#')) {
            if let (Some(a), Some(b)) = (line.find('{'), line.find('}')) {
                for pair in line[a + 1..b].split(',') {
                    if let Some((key, _)) = pair.split_once('=') {
                        seen.insert(key.trim().to_string());
                    }
                }
            }
        }
        let unexpected: Vec<_> = seen
            .iter()
            .filter(|k| !BOUNDED.contains(&k.as_str()) && !k.is_empty())
            .collect();
        assert!(
            unexpected.is_empty(),
            "labels {unexpected:?} are not in the bounded set {BOUNDED:?}"
        );
    }
}

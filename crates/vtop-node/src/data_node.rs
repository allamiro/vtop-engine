//! Run one live data-plane node for a single range.
//!
//! Mirrors the library harnesses exactly — `InProcessFollower` behind
//! `ReplicaPeerServer` for follower duty, `NetworkedReplicaSet` +
//! `LocalBroker` + `NativeServer` for leader duty — but as a real OS process
//! on a real disk, so chaos scripts can kill, freeze, and starve it.
//!
//! Honest scope note: there is no data-plane leader election. Killing the
//! leader validates durability and recovery (acked records survive a restart
//! of the same directory), not failover.

use crate::config::{DataNodeConfig, DataRole};
use crate::observe::{
    BrokerCollector, FollowerCollector, NodeObservability, SegmentRecoveryCollector,
};
use crate::tls;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use uuid::Uuid;
use vtop_broker::replication::{
    ClusterCommittedOffset, FlowControlConfig, InProcessFollower, NetworkFollowerConfig,
    NetworkedReplicaSet, ReplicaPeerHandler, ReplicaPeerServer, ReplicaSet,
};
use vtop_broker::{
    LocalBroker, MetaFencingEpoch, NativeServer, ProducerEpochJournal, ServerConfig,
    SessionAuthorizer,
};
use vtop_log::{
    ActiveSegment, KeyRange, RangeLineage, RecoveryReport, SegmentConfig, SegmentDescriptor,
};
use vtop_protocol::{RangeIdentity, Role};

pub const MAX_FRAME_BYTES: u32 = 32 * 1024 * 1024;
pub const MAX_RECORDS: u32 = 65_536;
pub const WINDOW_BYTES: u64 = 32 * 1024 * 1024;

fn segment_path(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join("range.active")
}

/// Recover a quiesced active segment, discard any tail beyond its durable
/// commit boundary, and publish the sealed bundle consumed by `vtopctl
/// segment verify`.
pub fn seal_active(path: &Path) -> Result<(), String> {
    let segment = ActiveSegment::recover(path).map_err(|error| error.to_string())?;
    let committed = segment.committed_offset();
    segment.seal().map_err(|error| error.to_string())?;
    println!(
        "segment_sealed path={} committed_offset={committed}",
        path.with_extension("segment").display()
    );
    Ok(())
}

/// Create the segment on first start, recover it on every restart. The
/// recover path is exactly what the kill/disk chaos scenarios exercise.
fn open_segment(
    data_dir: &Path,
    segment_id: Uuid,
    range: &RangeIdentity,
) -> Result<(ActiveSegment, Option<RecoveryReport>), String> {
    let path = segment_path(data_dir);
    if path.exists() {
        let segment = ActiveSegment::recover(&path).map_err(|error| error.to_string())?;
        let report = segment.recovery_report().clone();
        println!("segment_recovered report={report:?}");
        return Ok((segment, Some(report)));
    }
    let descriptor = SegmentDescriptor {
        segment_id,
        topic: range.topic.clone(),
        topic_epoch: range.topic_epoch,
        lineage: RangeLineage {
            range_id: range.range_id,
            generation: range.range_generation,
            key_range: KeyRange::full(),
            parents: Vec::new(),
        },
        base_offset: 0,
    };
    let config = SegmentConfig {
        max_segment_bytes: 8 * 1024 * 1024 * 1024,
        max_segment_records: 10_000_000,
        ..SegmentConfig::default()
    };
    let segment =
        ActiveSegment::create(&path, descriptor, config).map_err(|error| error.to_string())?;
    Ok((segment, None))
}

/// mTLS already authenticated the chain against the harness CA; the
/// authorizer additionally pins the one configured principal.
struct PrincipalAuthorizer {
    principal: Uuid,
}

impl SessionAuthorizer for PrincipalAuthorizer {
    fn authorize(&self, _peer_chain_der: &[Vec<u8>], principal_id: Uuid, role: Role) -> bool {
        principal_id == self.principal && matches!(role, Role::Producer | Role::Consumer)
    }
}

pub async fn run(config: DataNodeConfig) -> Result<(), String> {
    std::fs::create_dir_all(&config.data_dir)
        .map_err(|error| format!("create {}: {error}", config.data_dir.display()))?;
    let range = config.range.identity();
    let (segment, recovery) = open_segment(&config.data_dir, config.segment_id, &range)?;
    let epochs = ProducerEpochJournal::open(config.data_dir.join("epochs"))
        .map_err(|error| error.to_string())?;
    let meta = MetaFencingEpoch::new(config.fencing_epoch);

    let observability = NodeObservability::new(
        match config.role {
            DataRole::Follower => "data-follower",
            DataRole::Leader => "data-leader",
            DataRole::Standalone => "data-standalone",
        },
        &config.node_uuid.to_string(),
    )?;
    observability.register(Box::new(SegmentRecoveryCollector::new(recovery.as_ref())?))?;

    match config.role {
        DataRole::Follower => {
            run_follower(config, range, segment, epochs, meta, observability).await
        }
        DataRole::Leader => {
            run_leader(config, range, segment, epochs, meta, true, observability).await
        }
        DataRole::Standalone => {
            run_leader(config, range, segment, epochs, meta, false, observability).await
        }
    }
}

async fn run_follower(
    config: DataNodeConfig,
    range: RangeIdentity,
    segment: ActiveSegment,
    epochs: ProducerEpochJournal,
    meta: MetaFencingEpoch,
    observability: NodeObservability,
) -> Result<(), String> {
    let listen = config
        .replica_listen
        .as_ref()
        .ok_or("follower requires replica_listen")?;
    let follower = Arc::new(
        InProcessFollower::new(
            config.node_uuid,
            segment,
            epochs,
            range,
            config.fencing_epoch,
            meta,
            ClusterCommittedOffset::new(0),
        )
        .map_err(|error| error.to_string())?,
    );
    observability.register(Box::new(FollowerCollector::new(Arc::clone(&follower))?))?;
    let server = ReplicaPeerServer::new(
        tls::replica_material(&config.replica_tls)?,
        config.node_uuid,
        follower as Arc<dyn ReplicaPeerHandler>,
    )
    .map_err(|error| error.to_string())?;
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|error| format!("bind {listen}: {error}"))?;
    let metrics_addr = observability.serve(&config.observability).await?;
    observability.gate.mark_ready();
    println!(
        "data_node_ready role=follower node={} replica={listen}{}",
        config.node_uuid,
        metrics_addr
            .map(|addr| format!(" metrics={addr}"))
            .unwrap_or_default()
    );
    use std::io::Write;
    std::io::stdout().flush().ok();
    server
        .serve(listener)
        .await
        .map_err(|error| format!("replica server exited: {error}"))
}

async fn run_leader(
    config: DataNodeConfig,
    range: RangeIdentity,
    segment: ActiveSegment,
    epochs: ProducerEpochJournal,
    meta: MetaFencingEpoch,
    replicated: bool,
    observability: NodeObservability,
) -> Result<(), String> {
    let listen = config
        .native_listen
        .as_ref()
        .ok_or("leader/standalone requires native_listen")?;
    let native_tls = config
        .native_tls
        .as_ref()
        .ok_or("leader/standalone requires native_tls")?;
    let principal = config
        .principal_id
        .ok_or("leader/standalone requires principal_id")?;

    // Kept beside the broker: the collector needs the concrete replica set for
    // per-follower lag, which the `dyn ReplicaSet` the broker holds does not
    // expose.
    let mut observed_replicas = None;
    let mut follower_ids = Vec::new();

    let broker = if replicated {
        let follower_configs = config
            .followers
            .iter()
            .map(|follower| {
                Ok(NetworkFollowerConfig {
                    node_id: follower.node_uuid,
                    addr: vtop_meta::resolve_endpoint(&follower.addr)
                        .map_err(|error| error.to_string())?,
                    server_name: follower.server_name.clone(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        if follower_configs.is_empty() {
            return Err("leader requires at least one follower".into());
        }
        follower_ids = follower_configs
            .iter()
            .map(|f| f.node_id)
            .collect::<Vec<Uuid>>();
        let replica_set = Arc::new(
            NetworkedReplicaSet::start_on_handle_with_memory(
                tokio::runtime::Handle::current(),
                follower_configs,
                tls::replica_material(&config.replica_tls)?,
                FlowControlConfig::default(),
                None,
            )
            .map_err(|error| error.to_string())?,
        );
        // Wait for follower streams so the first quorum produce does not race
        // the dials; proceed anyway after the deadline (a scenario may start
        // a leader against a deliberately dead follower).
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if follower_ids
                .iter()
                .all(|node| replica_set.follower_connected(*node) == Some(true))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        observed_replicas = Some(Arc::clone(&replica_set));
        LocalBroker::with_replication(
            segment,
            epochs,
            range,
            config.fencing_epoch,
            meta,
            config.node_uuid,
            Some(ClusterCommittedOffset::new(0)),
            Some(replica_set as Arc<dyn ReplicaSet>),
        )
        .map_err(|error| error.to_string())?
    } else {
        LocalBroker::new(segment, epochs, range, config.fencing_epoch)
            .map_err(|error| error.to_string())?
    };

    let broker = Arc::new(broker);
    observability.register(Box::new(BrokerCollector::new(
        Arc::clone(&broker),
        observed_replicas,
        follower_ids,
    )?))?;
    // A leaseholder that metadata has fenced must stop advertising itself as
    // ready even though its listener is still bound and its process healthy.
    // That distinction is the whole point of separating /healthz from /readyz:
    // the node is alive and must stay up to be inspected, but it must not be
    // sent produce traffic it is now obliged to refuse.
    //
    // The predicate is the broker's own write-authorization rule, not merely
    // "a lease exists": after a steal the metadata lease is live for the NEW
    // holder, and reporting ready there would keep traffic pointed at the one
    // process guaranteed to reject it.
    //
    // Honest scope note: no production path publishes committed metadata
    // grants into this view yet — `MetaFencingEpoch::set`/`clear_lease` are
    // driven by a Raft applied-state watcher that is still follow-up work (see
    // the `MetaFencingEpoch` docs). Until that lands, this probe reports ready
    // for the configured epoch and the fenced branch fires only under test. It
    // is wired now so readiness becomes correct the moment the watcher does,
    // rather than being remembered afterwards.
    let lease = broker.meta_fencing_epoch().clone();
    let held = broker.held_fencing_epoch();
    let observability = observability.with_readiness_probe(Arc::new(move || {
        // Non-blocking: a produce mid-fsync holds this view, and a readiness
        // probe must never park a runtime worker behind a stalling disk.
        // Treating contention as ready is the right default — the broker is
        // demonstrably working, which is the opposite of fenced.
        match lease.try_snapshot() {
            None => vtop_observe::Readiness::Ready,
            Some((epoch, true)) if epoch == held => vtop_observe::Readiness::Ready,
            Some((epoch, true)) => vtop_observe::Readiness::not_ready(format!(
                "metadata lease moved to epoch {epoch}; this leaseholder is fenced at {held}"
            )),
            Some((epoch, false)) => vtop_observe::Readiness::not_ready(format!(
                "metadata lease released; range is fenced at epoch {epoch}"
            )),
        }
    }));

    let server = NativeServer::new(
        Arc::clone(&broker),
        tls::server_material(native_tls)?,
        Arc::new(PrincipalAuthorizer { principal }),
        ServerConfig {
            cluster_id: config.cluster_id,
            node_id: config.node_uuid,
            segment_format: vtop_broker::SegmentFormat::V1,
            max_frame_bytes: MAX_FRAME_BYTES,
            max_records_per_frame: MAX_RECORDS,
            window_bytes: WINDOW_BYTES,
            max_sessions: 8,
            max_inflight_requests: 8,
            handshake_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(300),
        },
    )
    .map_err(|error| error.to_string())?;

    let listener = TcpListener::bind(listen)
        .await
        .map_err(|error| format!("bind {listen}: {error}"))?;
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    let metrics_addr = observability.serve(&config.observability).await?;
    observability.gate.mark_ready();
    println!(
        "data_node_ready role={} node={} native={listen}{}",
        if replicated { "leader" } else { "standalone" },
        config.node_uuid,
        metrics_addr
            .map(|addr| format!(" metrics={addr}"))
            .unwrap_or_default()
    );
    use std::io::Write;
    std::io::stdout().flush().ok();
    let result = server.serve(listener, shutdown_rx).await;
    drop(shutdown);
    result.map_err(|error| format!("native server exited: {error}"))
}

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
    ServerCollector,
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

/// Serves the replica-status RPC on a leader, and refuses everything else
/// (#224).
///
/// A leader is a replica of its own range, so `vtopctl node status` must be
/// able to ask it where the range's commit boundary actually is — without it,
/// follower lag can only be measured against the furthest-ahead follower, which
/// is a strictly weaker answer than measuring against the leader.
///
/// It is emphatically not a follower, though. Accepting an append here would
/// let another process replicate into a range this one still leads, so every
/// write path refuses rather than being left unimplemented and drifting into
/// working by accident.
struct LeaderStatusReplica {
    broker: Arc<LocalBroker>,
    node_id: Uuid,
}

impl ReplicaPeerHandler for LeaderStatusReplica {
    fn node_id(&self) -> Uuid {
        self.node_id
    }

    fn apply_append(
        &self,
        _request: &vtop_protocol::ReplicaAppendRequest,
    ) -> Result<vtop_protocol::ReplicaAppendResponse, (vtop_protocol::ErrorCode, String)> {
        Err(self.refuse_write())
    }

    fn apply_append_batch(
        &self,
        _requests: &[vtop_protocol::ReplicaAppendRequest],
    ) -> Result<vtop_protocol::ReplicaAppendResponse, (vtop_protocol::ErrorCode, String)> {
        Err(self.refuse_write())
    }

    fn observe_hwm(
        &self,
        _update: &vtop_protocol::CommittedHwmUpdate,
    ) -> Result<(), (vtop_protocol::ErrorCode, String)> {
        Err(self.refuse_write())
    }

    fn status(
        &self,
        range: &RangeIdentity,
    ) -> Result<vtop_protocol::ReplicaStatusResponse, (vtop_protocol::ErrorCode, String)> {
        if range != self.broker.range() {
            return Err((
                vtop_protocol::ErrorCode::WrongRange,
                "replica status range identity does not match this leader".to_owned(),
            ));
        }
        let (local_committed_offset, next_offset) = self.broker.local_offsets();
        Ok(vtop_protocol::ReplicaStatusResponse {
            local_committed_offset,
            next_offset,
        })
    }
}

impl LeaderStatusReplica {
    fn refuse_write(&self) -> (vtop_protocol::ErrorCode, String) {
        (
            vtop_protocol::ErrorCode::Fenced,
            "this node leads the range and does not accept replication appends".to_owned(),
        )
    }
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

/// Run a data node that owns its own observability endpoint.
pub async fn run(mut config: DataNodeConfig) -> Result<(), String> {
    let observability = NodeObservability::new(
        match config.role {
            DataRole::Follower => "data-follower",
            DataRole::Leader => "data-leader",
            DataRole::Standalone => "data-standalone",
        },
        &config.node_uuid.to_string(),
    )?;
    let endpoint = config.observability.take().unwrap_or_default();
    let metrics_addr = observability.serve(&endpoint).await?;
    serve(config, &observability, metrics_addr).await
}

/// Run a data node against a caller-owned observability surface (#215).
pub async fn serve(
    config: DataNodeConfig,
    observability: &NodeObservability,
    metrics_addr: Option<std::net::SocketAddr>,
) -> Result<(), String> {
    std::fs::create_dir_all(&config.data_dir)
        .map_err(|error| format!("create {}: {error}", config.data_dir.display()))?;
    let range = config.range.identity();
    let (segment, recovery) = open_segment(&config.data_dir, config.segment_id, &range)?;
    let epochs = ProducerEpochJournal::open(config.data_dir.join("epochs"))
        .map_err(|error| error.to_string())?;
    // With a lease configured, authority to serve comes from the metadata
    // plane, not from static configuration — so the view starts INACTIVE and
    // the broker fails closed until the agent's first successful acquisition
    // or renewal publishes a grant. Without one, the configured epoch is
    // simply asserted, which is the pre-#223 behaviour every existing config
    // still gets.
    let meta = if config.lease.is_some() {
        MetaFencingEpoch::new_inactive(config.fencing_epoch)
    } else {
        MetaFencingEpoch::new(config.fencing_epoch)
    };

    observability.register(Box::new(SegmentRecoveryCollector::new(recovery.as_ref())?))?;

    match config.role {
        DataRole::Follower => {
            run_follower(
                config,
                range,
                segment,
                epochs,
                meta,
                observability,
                metrics_addr,
            )
            .await
        }
        DataRole::Leader => {
            run_leader(
                config,
                range,
                segment,
                epochs,
                meta,
                true,
                observability,
                metrics_addr,
            )
            .await
        }
        DataRole::Standalone => {
            run_leader(
                config,
                range,
                segment,
                epochs,
                meta,
                false,
                observability,
                metrics_addr,
            )
            .await
        }
    }
}

async fn run_follower(
    config: DataNodeConfig,
    range: RangeIdentity,
    segment: ActiveSegment,
    epochs: ProducerEpochJournal,
    meta: MetaFencingEpoch,
    observability: &NodeObservability,
    metrics_addr: Option<std::net::SocketAddr>,
) -> Result<(), String> {
    let listen = config
        .replica_listen
        .as_ref()
        .ok_or("follower requires replica_listen")?;
    // Captured before the range moves into the follower; the watcher needs the
    // same range id the follower was constructed for, not a second reading of
    // the config.
    let watched_range_id = range.range_id;
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
    // Epoch history on real disk (#240): which fencing epoch wrote each
    // stretch of this replica's log. Promotion cannot compare two replicas'
    // offsets without it — a bare offset says where a replica is, not whose
    // writes put it there.
    follower.set_fencing_epoch_journal(
        vtop_broker::fencing_epochs::FencingEpochJournal::open(
            config.data_dir.join("fencing-epochs"),
        )
        .map_err(|error| error.to_string())?,
    );
    observability.register(Box::new(FollowerCollector::new(Arc::clone(&follower))?))?;

    // With a lease configured, this follower learns its epoch from metadata
    // instead of from its config file (#239). Without one it keeps asserting
    // the configured epoch, which is what every pre-#239 config still gets.
    if let Some(lease) = config.lease.as_ref() {
        // Readiness becomes a conjunction: the replica listener is bound AND
        // the watcher has read metadata once. A follower that does not yet
        // know its epoch refuses every append, so reporting ready before that
        // first read advertises a replica that cannot participate — which is
        // how a freshly started follower sat at offset 0 while its leader
        // moved on without it.
        observability.gate.require_marks(2);
        let watcher = crate::lease_watcher::LeaseWatcher::new(
            vtop_meta::AdminClient::new(
                tls::meta_material(&lease.tls)?,
                vtop_meta::resolve_endpoint(&lease.admin_endpoint)
                    .map_err(|error| error.to_string())?,
                lease.server_name.clone(),
            )
            .map_err(|error| error.to_string())?,
            lease.topic_uuid,
            watched_range_id,
            crate::lease_watcher::LeaseWatcherConfig {
                poll_interval: Duration::from_millis(lease.poll_interval_ms),
                // The lease duration bounds how long a read may take before it
                // is worth abandoning: a read outstanding longer than the lease
                // itself can only return an epoch that has already turned over.
                request_timeout: Duration::from_millis(lease.lease_duration_ms),
            },
            Arc::new(crate::lease_watcher::FollowerLeasePublisher::new(
                Arc::clone(&follower),
            )),
            Some(observability.gate.clone()),
        )?;
        tokio::spawn(watcher.run());
    }

    let server = ReplicaPeerServer::new(
        tls::replica_material(&config.replica_tls)?,
        config.node_uuid,
        follower as Arc<dyn ReplicaPeerHandler>,
    )
    .map_err(|error| error.to_string())?;
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|error| format!("bind {listen}: {error}"))?;
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

/// Argument list mirrors what `serve` has already unpacked; grouping it into a
/// struct would move the same list one indirection from its only caller.
#[allow(clippy::too_many_arguments)]
async fn run_leader(
    config: DataNodeConfig,
    range: RangeIdentity,
    segment: ActiveSegment,
    epochs: ProducerEpochJournal,
    meta: MetaFencingEpoch,
    replicated: bool,
    observability: &NodeObservability,
    metrics_addr: Option<std::net::SocketAddr>,
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
        // The shared handle, not `LocalBroker::new` (which builds its own
        // always-active view): a lease-configured standalone leader must also
        // start fenced until the agent publishes a grant.
        LocalBroker::with_meta_fencing_epoch(segment, epochs, range, config.fencing_epoch, meta)
            .map_err(|error| error.to_string())?
    };

    let broker = Arc::new(broker);
    // Same epoch history a follower keeps (#240). A leader needs its own: the
    // range's history is the union of what each replica recorded, and a leader
    // that could not say which epoch wrote its tail is exactly the replica a
    // future promotion cannot reconcile against.
    broker.set_fencing_epoch_journal(
        vtop_broker::fencing_epochs::FencingEpochJournal::open(
            config.data_dir.join("fencing-epochs"),
        )
        .map_err(|error| error.to_string())?,
    );
    // Verified promotion probes each follower's DISK over the replication
    // plane rather than reading this leader's own replication counters, which
    // on a fresh promotion have never been advanced and would report every
    // follower at offset zero. `None` for a standalone range: there is no
    // replica set to establish a boundary against.
    let promotion_probe: Option<Arc<dyn crate::lease_agent::QuorumProbe>> = if replicated {
        let endpoints = config
            .followers
            .iter()
            .map(|follower| {
                Ok(crate::lease_agent::FollowerEndpoint {
                    node_uuid: follower.node_uuid,
                    addr: vtop_meta::resolve_endpoint(&follower.addr)
                        .map_err(|error| error.to_string())?,
                    server_name: follower.server_name.clone(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Some(Arc::new(crate::lease_agent::ReplicaPlaneProbe::new(
            Arc::clone(&broker),
            config.node_uuid,
            vtop_broker::replication::ReplicaStatusClient::new(tls::replica_material(
                &config.replica_tls,
            )?)
            .map_err(|error| error.to_string())?,
            endpoints,
            broker.range().clone(),
        )))
    } else {
        None
    };
    // Range leadership from the metadata plane (#223). Without this the
    // configured `fencing_epoch` is simply asserted and never revisited, which
    // is the pre-#223 behaviour every existing config still gets.
    if let Some(lease) = config.lease.as_ref() {
        // Followers learn granted epochs on their own now (#239), so a
        // replicated range no longer needs them restarted on every grant —
        // but only if they are actually configured to watch. A follower with
        // no `lease` block still asserts its static epoch and will fence this
        // leader out of its own quorum the moment metadata mints a new one.
        //
        // That is not something this process can check: it cannot see its
        // followers' configs, only reach their replica ports. So the warning
        // is now about the configuration the operator controls rather than a
        // limitation of the code, and it names the fix.
        if replicated {
            tracing::info!(
                range = %config.range.range_id,
                "lease-driven leadership on a replicated range requires every follower to \
                 carry a `lease` block of its own; a follower without one asserts its static \
                 fencing_epoch and will refuse appends at a newly granted epoch"
            );
        }
        let agent = crate::lease_agent::LeaseAgent::new(
            vtop_meta::AdminClient::new(
                tls::meta_material(&lease.tls)?,
                vtop_meta::resolve_endpoint(&lease.admin_endpoint)
                    .map_err(|error| error.to_string())?,
                lease.server_name.clone(),
            )
            .map_err(|error| error.to_string())?,
            crate::lease_agent::LeaseAgentConfig {
                lease_duration: Duration::from_millis(lease.lease_duration_ms),
                renew_interval: Duration::from_millis(lease.renew_interval_ms),
                poll_interval: Duration::from_millis(lease.poll_interval_ms),
            },
            config.node_uuid,
            lease.topic_uuid,
            config.range.range_id,
            Arc::new(crate::lease_agent::BrokerLeasePublisher::new(Arc::clone(
                &broker,
            ))),
            promotion_probe,
        )?;
        tokio::spawn(agent.run());
    }
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
    // Scope note: with a lease configured, the agent spawned above drives
    // this view from committed metadata — grants on acquisition and renewal,
    // releases on loss. Without one, nothing publishes into it and the probe
    // reports ready for the configured epoch, the pre-#223 behaviour.
    let lease = broker.meta_fencing_epoch().clone();
    let probe_broker = Arc::clone(&broker);
    // Last decided verdict, served under lock contention. Fail-closed start
    // for a lease-driven broker: until the agent's first grant is OBSERVED,
    // contention must not read as ready — an unpromoted broker being hammered
    // with (refused) requests is exactly when the lock is busiest. Without a
    // lease the configured epoch is authoritative and the initial verdict is
    // ready, the pre-#223 behaviour.
    let last_ready = Arc::new(std::sync::atomic::AtomicBool::new(config.lease.is_none()));
    observability.set_readiness_probe(Arc::new(move || {
        // Non-blocking: a produce mid-fsync holds this view, and a readiness
        // probe must never park a runtime worker behind a stalling disk.
        // Contention serves the LAST DECIDED verdict rather than guessing in
        // either direction: a broker mid-append is working, and a broker that
        // was fenced a scrape ago is still fenced.
        match lease.try_snapshot() {
            None => {
                if last_ready.load(std::sync::atomic::Ordering::Relaxed) {
                    vtop_observe::Readiness::Ready
                } else {
                    vtop_observe::Readiness::not_ready(
                        "lease view contended; last decided state was fenced".to_owned(),
                    )
                }
            }
            Some((epoch, live)) => {
                // Read the held epoch inside the probe: the lease agent can
                // promote this broker onto a new epoch at any time, and a
                // value captured at startup would report a freshly re-elected
                // leader as fenced forever — draining the one process that is
                // actually authorized to serve.
                let held = probe_broker.held_fencing_epoch();
                let ready = live && epoch == held;
                last_ready.store(ready, std::sync::atomic::Ordering::Relaxed);
                if ready {
                    vtop_observe::Readiness::Ready
                } else if live {
                    vtop_observe::Readiness::not_ready(format!(
                        "metadata lease moved to epoch {epoch}; this leaseholder is fenced at {held}"
                    ))
                } else {
                    vtop_observe::Readiness::not_ready(format!(
                        "metadata lease released; range is fenced at epoch {epoch}"
                    ))
                }
            }
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
    // Taken before `serve`, which consumes the server.
    observability.register(Box::new(ServerCollector::new(Arc::clone(
        server.metrics(),
    ))?))?;

    // A leader that names a `replica_listen` also answers the replica-status
    // RPC, so `vtopctl node status` can measure follower lag against the
    // leader's own boundary rather than against the furthest-ahead follower.
    // Status only: see LeaderStatusReplica.
    let status_addr = match config.replica_listen.as_ref() {
        Some(status_listen) => {
            let status_server = ReplicaPeerServer::new(
                tls::replica_material(&config.replica_tls)?,
                config.node_uuid,
                Arc::new(LeaderStatusReplica {
                    broker: Arc::clone(&broker),
                    node_id: config.node_uuid,
                }) as Arc<dyn ReplicaPeerHandler>,
            )
            .map_err(|error| error.to_string())?;
            let status_listener = TcpListener::bind(status_listen)
                .await
                .map_err(|error| format!("bind {status_listen}: {error}"))?;
            tokio::spawn(async move {
                if let Err(error) = status_server.serve(status_listener).await {
                    tracing::warn!(%error, "leader replica-status server exited");
                }
            });
            Some(status_listen.clone())
        }
        None => None,
    };

    let listener = TcpListener::bind(listen)
        .await
        .map_err(|error| format!("bind {listen}: {error}"))?;
    let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
    observability.gate.mark_ready();
    println!(
        "data_node_ready role={} node={} native={listen}{}{}",
        if replicated { "leader" } else { "standalone" },
        config.node_uuid,
        status_addr
            .map(|addr| format!(" replica_status={addr}"))
            .unwrap_or_default(),
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

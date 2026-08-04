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
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
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
) -> Result<ActiveSegment, String> {
    let path = segment_path(data_dir);
    if path.exists() {
        let segment = ActiveSegment::recover(&path).map_err(|error| error.to_string())?;
        println!("segment_recovered report={:?}", segment.recovery_report());
        return Ok(segment);
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
    ActiveSegment::create(&path, descriptor, config).map_err(|error| error.to_string())
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
    let segment = open_segment(&config.data_dir, config.segment_id, &range)?;
    let epochs = ProducerEpochJournal::open(config.data_dir.join("epochs"))
        .map_err(|error| error.to_string())?;
    let meta = MetaFencingEpoch::new(config.fencing_epoch);

    match config.role {
        DataRole::Follower => run_follower(config, range, segment, epochs, meta).await,
        DataRole::Leader => run_leader(config, range, segment, epochs, meta, true).await,
        DataRole::Standalone => run_leader(config, range, segment, epochs, meta, false).await,
    }
}

async fn run_follower(
    config: DataNodeConfig,
    range: RangeIdentity,
    segment: ActiveSegment,
    epochs: ProducerEpochJournal,
    meta: MetaFencingEpoch,
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
    let server = ReplicaPeerServer::new(
        tls::replica_material(&config.replica_tls)?,
        config.node_uuid,
        follower as Arc<dyn ReplicaPeerHandler>,
    )
    .map_err(|error| error.to_string())?;
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|error| format!("bind {listen}: {error}"))?;
    println!(
        "data_node_ready role=follower node={} replica={listen}",
        config.node_uuid
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
        let follower_ids: Vec<Uuid> = follower_configs.iter().map(|f| f.node_id).collect();
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

    let server = NativeServer::new(
        Arc::new(broker),
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
    println!(
        "data_node_ready role={} node={} native={listen}",
        if replicated { "leader" } else { "standalone" },
        config.node_uuid
    );
    use std::io::Write;
    std::io::stdout().flush().ok();
    let result = server.serve(listener, shutdown_rx).await;
    drop(shutdown);
    result.map_err(|error| format!("native server exited: {error}"))
}

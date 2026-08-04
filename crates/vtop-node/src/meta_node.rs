//! Run one live metadata Raft node: durable store on real disk, Raft peer
//! RPCs and the admin endpoint over mTLS TCP.

use crate::config::MetaNodeConfig;
use crate::observe::{MetaRaftCollector, NodeObservability};
use crate::tls;
use std::sync::Arc;
use tokio::net::TcpListener;
use vtop_log::env::Env;
use vtop_meta::{
    resolve_endpoint, start_meta_node, AdminHandler, AdminServer, MetaNodeId, MetaNodeTimers,
    PeerDirectory, PeerEndpoint, PeerRpcHandler, PeerServer,
};

/// Run a metadata node that owns its own observability endpoint.
pub async fn run(mut config: MetaNodeConfig) -> Result<(), String> {
    let observability = NodeObservability::new("meta", &config.node_id.to_string())?;
    let endpoint = config.observability.take().unwrap_or_default();
    let metrics_addr = observability.serve(&endpoint).await?;
    serve(config, &observability, metrics_addr).await
}

/// Run a metadata node against an observability surface someone else owns.
///
/// Split out so a co-located process (#215) can expose ONE endpoint covering
/// both of its roles. An operator scraping a host should find one target, not
/// have to know which roles happen to share it.
pub async fn serve(
    config: MetaNodeConfig,
    observability: &NodeObservability,
    metrics_addr: Option<std::net::SocketAddr>,
) -> Result<(), String> {
    std::fs::create_dir_all(&config.data_dir)
        .map_err(|error| format!("create {}: {error}", config.data_dir.display()))?;

    let directory = PeerDirectory::new();
    for peer in &config.peers {
        if peer.id == config.node_id {
            continue;
        }
        directory.insert(
            peer.id,
            PeerEndpoint {
                addr: resolve_endpoint(&peer.addr).map_err(|error| error.to_string())?,
                server_name: peer.server_name.clone(),
            },
        );
    }

    let env = Env::real();
    let node = start_meta_node(
        &env,
        &config.data_dir,
        config.cluster_id,
        config.node_id,
        directory,
        tls::meta_material(&config.tls)?,
        MetaNodeTimers {
            election_timeout_min_ms: config.timers.election_timeout_min_ms,
            election_timeout_max_ms: config.timers.election_timeout_max_ms,
            heartbeat_interval_ms: config.timers.heartbeat_interval_ms,
        },
    )
    .await?;

    let peer_listener = TcpListener::bind(&config.peer_listen)
        .await
        .map_err(|error| format!("bind {}: {error}", config.peer_listen))?;
    let admin_listener = TcpListener::bind(&config.admin_listen)
        .await
        .map_err(|error| format!("bind {}: {error}", config.admin_listen))?;

    let peer_server = PeerServer::new(
        tls::meta_material(&config.tls)?,
        MetaNodeId(config.node_id),
        Arc::clone(&node.peer_handler) as Arc<dyn PeerRpcHandler>,
    )
    .map_err(|error| error.to_string())?;
    let admin_server = AdminServer::new(
        tls::meta_material(&config.tls)?,
        Arc::clone(&node.consensus) as Arc<dyn AdminHandler>,
    )
    .map_err(|error| error.to_string())?;

    // The operational surface comes up before the ready marker so a scraper or
    // health gate can observe the node from the same instant a script can.
    observability.register(Box::new(MetaRaftCollector::new(Arc::clone(
        &node.consensus,
    ))?))?;

    // Readiness for a metadata node is "both listeners are bound", NOT "the
    // cluster has a leader". A fresh cluster has no leader until the admin
    // `init` RPC lands, and that RPC arrives over the very endpoint being
    // gated — requiring leadership here would deadlock bringup. Leadership is
    // published as `vtop_meta_raft_state{state="leader"}` instead, where it is
    // alertable without being a startup precondition.
    observability.gate.mark_ready();

    // Scripts wait for this marker before issuing admin RPCs.
    println!(
        "meta_node_ready id={} peer={} admin={}{}",
        config.node_id,
        config.peer_listen,
        config.admin_listen,
        metrics_addr
            .map(|addr| format!(" metrics={addr}"))
            .unwrap_or_default()
    );
    use std::io::Write;
    std::io::stdout().flush().ok();

    tokio::select! {
        result = peer_server.serve(peer_listener) => {
            Err(format!("peer server exited: {result:?}"))
        }
        result = admin_server.serve(admin_listener) => {
            Err(format!("admin server exited: {result:?}"))
        }
    }
}

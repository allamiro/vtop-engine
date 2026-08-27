//! Run one live metadata Raft node: durable store on real disk, Raft peer
//! RPCs and the admin endpoint over mTLS TCP.

use crate::config::MetaNodeConfig;
use crate::observe::{MetaRaftCollector, NodeObservability};
use crate::tls;
use std::sync::Arc;
use tokio::net::TcpListener;
use vtop_log::env::Env;
use vtop_meta::transport::AdminAuthorizer;
use vtop_meta::{
    start_meta_node, AdminHandler, AdminServer, MetaNodeId, MetaNodeTimers, PeerDirectory,
    PeerRpcHandler, PeerServer,
};

/// Run a metadata node that owns its own observability endpoint.
pub async fn run(
    mut config: MetaNodeConfig,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    let observability = NodeObservability::new("meta", &config.node_id.to_string())?;
    let endpoint = config.observability.take().unwrap_or_default();
    let metrics_addr = observability.serve(&endpoint).await?;
    serve(config, &observability, metrics_addr, shutdown).await
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
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    std::fs::create_dir_all(&config.data_dir)
        .map_err(|error| format!("create {}: {error}", config.data_dir.display()))?;

    // PEERS GO IN BY NAME, NOT BY THE ADDRESS THE NAME HAPPENS TO HOLD NOW
    // (#367). This used to resolve each peer once, here, and store the
    // resulting `SocketAddr` for the life of the process. Two things follow
    // from that, and both were observed in CI.
    //
    // A member whose pod is replaced comes back at a new address, and every
    // surviving member keeps dialling the old one — forever. The break is
    // ONE-WAY, which is what made it so hard to see: the returning member
    // resolved its neighbours at its own start, so it can reach them, and
    // they cannot reach it. It is never replicated to, never learns the
    // current term, and campaigns for the rest of its life — burning a term
    // per election timeout, which the consensus layer's own notes explain is
    // not free.
    //
    // And a member whose neighbours do not exist yet could not start at all:
    // resolution failure was fatal, so in a group that boots together
    // (podManagementPolicy: Parallel) whichever member won the race crash-
    // looped until the others had addresses. A peer that is not there yet is
    // not a configuration error.
    let directory = PeerDirectory::new();
    for peer in &config.peers {
        if peer.id == config.node_id {
            continue;
        }
        directory
            .insert_by_name(peer.id, peer.addr.clone(), peer.server_name.clone())
            .await;
        // ANNOUNCED, so a typo and a startup race are distinguishable. Both
        // present as an unreachable member; only one of them will ever fix
        // itself, and an operator reading a node that came up "fine" has
        // nothing to go on otherwise (review).
        if directory.get(peer.id).is_none() {
            eprintln!(
                "metadata peer {} at {:?} does not resolve yet; it will be looked up \
                 again on every failed exchange and stays unreachable until it answers",
                peer.id, peer.addr
            );
        }
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
    // An absent policy keeps the pre-#238 behaviour — any CA-signed client may
    // do anything — so it warns rather than passing silently. The warning names
    // the concrete exposure, not just the missing key: an operator who reads
    // "unauthorized" without knowing what it permits has no reason to act.
    let authorizer = match &config.admin_authorization {
        Some(policy) => policy.authorizer(),
        None => {
            eprintln!(
                "warning: admin endpoint {} is authenticated but NOT authorized: any client \
                 holding a certificate signed by this CA — including every data node — may \
                 change cluster membership and grant range leases. Set `admin_authorization` \
                 to restrict cluster-scoped commands to named operator CNs.",
                config.admin_listen
            );
            AdminAuthorizer::permissive()
        }
    };
    let admin_server = AdminServer::with_authorization(
        tls::meta_material(&config.tls)?,
        Arc::clone(&node.consensus) as Arc<dyn AdminHandler>,
        authorizer,
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
        () = wait_for_shutdown(shutdown) => {
            // An orderly stop (#280). Raft state is durable on every apply,
            // so there is nothing to flush: dropping the listeners is the
            // whole drain, and the survivors elect around the departure.
            println!("meta_node_stopping");
            Ok(())
        }
    }
}

/// Resolve only when the process-wide shutdown flag flips (#280); a dropped
/// sender parks forever rather than reading as an implicit shutdown.
async fn wait_for_shutdown(mut shutdown: tokio::sync::watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

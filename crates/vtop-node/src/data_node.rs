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
    ClusterCommittedOffset, FlowControlConfig, InProcessFollower, LeaderSegmentTransferHandler,
    NetworkFollowerConfig, NetworkedReplicaSet, ReplicaPeerHandler, ReplicaPeerServer, ReplicaSet,
};
use vtop_broker::{
    LocalBroker, MetaFencingEpoch, NativeServer, ProducerEpochJournal, ServerConfig,
    SessionAuthorizer,
};
use vtop_log::env::Env;
use vtop_log::{
    ActiveSegment, KeyRange, RangeLineage, RecoveryReport, SegmentConfig, SegmentDescriptor,
    SegmentSet,
};
use vtop_protocol::{RangeIdentity, Role};

pub const MAX_FRAME_BYTES: u32 = 32 * 1024 * 1024;
pub const MAX_RECORDS: u32 = 65_536;
pub const WINDOW_BYTES: u64 = 32 * 1024 * 1024;

/// Recover a quiesced active segment, discard any tail beyond its durable
/// commit boundary, and publish the sealed bundle consumed by `vtopctl
/// segment verify`.
///
/// On a rolled range this is handed the TAIL's path — its predecessors are
/// already sealed and verify as-is. Recovery seeds the tail's inherited
/// producer frontier from its `.producers` sidecar, so sealing a rolled
/// tail needs nothing beyond what sealing the first segment ever did.
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

/// Build the lease client with every metadata node it may be redirected to.
///
/// `admin_endpoint` is asked first and the peers are where a redirect leads.
/// Both matter: a client with only the first cannot follow a redirect at all,
/// and a client that ignored the first would stop preferring the local
/// endpoint under co-location, where it is genuinely the cheapest hop.
fn lease_admin_client(
    lease: &crate::config::LeaseConfig,
) -> Result<vtop_meta::AdminClient, String> {
    let mut candidates = vec![vtop_meta::AdminCandidate {
        // The configured endpoint's node id is not stated, and is not needed:
        // it is tried first regardless. A redirect naming it will match a peer
        // entry if one is configured for the same node.
        node_id: None,
        endpoint: vtop_meta::resolve_endpoint(&lease.admin_endpoint)
            .map_err(|error| error.to_string())?,
        server_name: lease.server_name.clone(),
        // TLS remains the only mode this path builds. Slice 1 of #294 makes the
        // admin transport CAPABLE of plaintext; wiring a node's lease client to
        // choose it belongs with the config surface, not here.
        plaintext: false,
    }];
    for peer in &lease.admin_peers {
        candidates.push(vtop_meta::AdminCandidate {
            node_id: Some(vtop_meta::MetaNodeId(peer.node_id)),
            endpoint: vtop_meta::resolve_endpoint(&peer.endpoint)
                .map_err(|error| error.to_string())?,
            server_name: if peer.server_name.is_empty() {
                lease.server_name.clone()
            } else {
                peer.server_name.clone()
            },
            plaintext: false,
        });
    }
    vtop_meta::AdminClient::with_candidates(tls::meta_material(&lease.tls)?, candidates)
        .map_err(|error| error.to_string())
}

/// Open the range through the startup catalog on every restart; create its
/// first segment on first start.
///
/// Opening through the catalog (#270) is what picks up a rolled range —
/// sealed segments plus the tail, read as one — instead of a single
/// hardcoded filename. It is also where a quarantined bundle refuses
/// startup with its reason named: a node must never silently serve
/// whichever subset of an ambiguous directory still looks healthy.
///
/// When a range rolls to a new segment. Passed rather than read from a
/// constant so a deployment can choose it: only SEALED segments transfer, so a
/// node that never rolls is one where `vtopctl node repair` has nothing to move
/// and a lost replica has no road back.
#[derive(Clone, Copy, Debug)]
pub struct SegmentRoll {
    pub max_bytes: u64,
    pub max_records: u64,
    /// Carried with the roll bounds because the engine refuses a segment
    /// smaller than a group: setting one without the other does not start.
    pub max_group_bytes: u64,
    pub max_record_bytes: u32,
}

/// A pre-#270 directory holding only the legacy single `range.active`
/// opens the same way, as a set of one — discovery keys segments by their
/// contents, not their stems — so no migration step exists to get wrong.
/// New ranges take offset-based stems (`range-<base>.active`), the naming
/// rolling itself produces.
fn open_range(
    data_dir: &Path,
    segment_id: Uuid,
    range: &RangeIdentity,
    roll: SegmentRoll,
) -> Result<(SegmentSet, Option<RecoveryReport>), String> {
    let env = Env::real();
    if let Some(set) = SegmentSet::open_in(&env, data_dir).map_err(|error| error.to_string())? {
        // The tail is the only segment recovery had judgement calls to make
        // about; sealed segments either validate or quarantine.
        let report = set.active().recovery_report().clone();
        println!("segment_recovered report={report:?}");
        // An existing range runs the limits in its tail's HEADER; the YAML
        // only ever applies at creation. Said out loud when they disagree,
        // with the remedy — otherwise an operator who edited the config
        // watches nothing change and has no line telling them why (#314).
        let (record, group, bytes, records) = match set.active().config_v2() {
            Some(config) => (
                config.max_record_bytes,
                config.max_group_bytes,
                config.max_segment_bytes,
                config.max_segment_records,
            ),
            None => {
                let config = set.active().config();
                (
                    config.max_record_bytes,
                    config.max_group_bytes,
                    config.max_segment_bytes,
                    config.max_segment_records,
                )
            }
        };
        if (record, group, bytes, records)
            != (
                roll.max_record_bytes,
                roll.max_group_bytes,
                roll.max_bytes,
                roll.max_records,
            )
        {
            println!(
                "roll_thresholds_differ configured=({},{},{},{}) header=({record},{group},{bytes},{records}) \
                 note=\"an existing range runs its header's limits; change them with vtopctl node \
                 reconfigure-range\"",
                roll.max_record_bytes, roll.max_group_bytes, roll.max_bytes, roll.max_records,
            );
        }
        return Ok((set, Some(report)));
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
        max_segment_bytes: roll.max_bytes,
        max_segment_records: roll.max_records,
        max_group_bytes: roll.max_group_bytes,
        max_record_bytes: roll.max_record_bytes,
        ..SegmentConfig::default()
    };
    let set = SegmentSet::create_in(&env, data_dir, descriptor, config)
        .map_err(|error| error.to_string())?;
    Ok((set, None))
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
    /// SEALED-SEGMENT TRANSFER, delegated (#270/#301).
    ///
    /// Nothing installed this handler, so every leader answered "this peer
    /// does not serve sealed-segment transfer" and `vtopctl node repair` could
    /// not work against any real cluster. The transfer plane, its client, its
    /// CLI and its tests all existed; the one line that put the server on a
    /// running node did not, and the tests wired it up themselves so nothing
    /// noticed.
    ///
    /// Delegated rather than substituted: this handler also answers
    /// `epoch_history`, which the transfer handler does not, and a leader must
    /// keep doing both.
    transfer: LeaderSegmentTransferHandler,
    /// Who may pull sealed segments from this leader.
    ///
    /// Its own followers, plus whatever `transfer_peers` names. A replacement
    /// replica is in the second set and not the first: it is being repaired
    /// because it is not a follower yet.
    transfer_allowed: std::collections::BTreeSet<Uuid>,
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

    /// A leader is a replica of its own range, so it must be able to vouch for
    /// its own epoch history too — a promotion that could only read followers
    /// would be reconciling against a strict subset of the range's lineage.
    fn epoch_history(
        &self,
        range: &RangeIdentity,
    ) -> Result<Vec<vtop_protocol::ReplicaEpochStart>, (vtop_protocol::ErrorCode, String)> {
        if range != self.broker.range() {
            return Err((
                vtop_protocol::ErrorCode::WrongRange,
                "epoch history range identity does not match this leader".to_owned(),
            ));
        }
        Ok(self
            .broker
            .epoch_starts()
            .into_iter()
            .map(|entry| vtop_protocol::ReplicaEpochStart {
                epoch: entry.epoch,
                start_offset: entry.start_offset,
            })
            .collect())
    }

    // INSIDE THE TRAIT IMPL, deliberately noted: these first landed in an
    // inherent `impl` block, where they compiled cleanly, shadowed nothing,
    // and left the trait's refusing defaults in place — so the leader went on
    // answering "this peer does not serve sealed-segment transfer" while the
    // code that was supposed to fix it sat right there.
    fn list_sealed_segments(
        &self,
        peer: Uuid,
        range: &vtop_protocol::RangeIdentity,
        fencing_epoch: u64,
    ) -> Result<Vec<vtop_protocol::SealedSegmentEntry>, (vtop_protocol::ErrorCode, String)> {
        self.authorize_transfer(peer)?;
        self.transfer
            .list_sealed_segments(peer, range, fencing_epoch)
    }

    fn fetch_segment_chunk(
        &self,
        peer: Uuid,
        request: &vtop_protocol::FetchSegmentChunkRequest,
    ) -> Result<vtop_protocol::FetchSegmentChunkResponse, (vtop_protocol::ErrorCode, String)> {
        // CHECKED ON EVERY CHUNK, not only on the listing. A listing and a
        // fetch are separate requests on a connection that may be reused, and
        // authorizing only the cheap one would leave the expensive one — the
        // one that actually moves bytes — open.
        self.authorize_transfer(peer)?;
        self.transfer.fetch_segment_chunk(peer, request)
    }

    fn seal_tail(
        &self,
        peer: Uuid,
        range: &vtop_protocol::RangeIdentity,
        fencing_epoch: u64,
    ) -> Result<vtop_protocol::SealTailResponse, (vtop_protocol::ErrorCode, String)> {
        // The transfer allowlist gates the seal too: sealing exists FOR the
        // transfer, and a peer that may not pull the bytes has no business
        // reshaping the leader's segments to prepare for a pull it will be
        // refused.
        self.authorize_transfer(peer)?;
        self.transfer.seal_tail(peer, range, fencing_epoch)
    }
}

impl LeaderStatusReplica {
    /// Refuse a sealed-segment transfer to a peer that is neither a follower
    /// nor a named repair destination.
    fn authorize_transfer(&self, peer: Uuid) -> Result<(), (vtop_protocol::ErrorCode, String)> {
        if self.transfer_allowed.contains(&peer) {
            return Ok(());
        }
        Err((
            vtop_protocol::ErrorCode::InvalidRequest,
            format!(
                "{peer} is not authorized to pull sealed segments from this leader. A transfer \
                 hands over a whole range's bytes, so it is limited to this leader's followers \
                 and to the node UUIDs named in `transfer_peers`. Add the replacement replica \
                 there for the duration of its repair."
            ),
        ))
    }

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
pub async fn run(
    mut config: DataNodeConfig,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    let observability = NodeObservability::new(
        match config.role {
            DataRole::Follower => "data-follower",
            DataRole::Leader => "data-leader",
            DataRole::Standalone => "data-standalone",
            // Set ONCE, like every identity label; the live role verdict is
            // carried by the lease gauges, not by a mutating label.
            DataRole::Candidate => "data-candidate",
        },
        &config.node_uuid.to_string(),
    )?;
    let endpoint = config.observability.take().unwrap_or_default();
    let metrics_addr = observability.serve(&endpoint).await?;
    serve(config, &observability, metrics_addr, shutdown).await
}

/// Adapt the process-wide shutdown flag to the oneshot a server takes (#280).
/// A dropped sender never fires: only a real signal drains the listeners.
fn oneshot_on_shutdown(
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::sync::oneshot::Receiver<()> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        while !*shutdown.borrow() {
            if shutdown.changed().await.is_err() {
                return;
            }
        }
        let _ = sender.send(());
    });
    receiver
}

/// Run a data node against a caller-owned observability surface (#215).
pub async fn serve(
    config: DataNodeConfig,
    observability: &NodeObservability,
    metrics_addr: Option<std::net::SocketAddr>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    std::fs::create_dir_all(&config.data_dir)
        .map_err(|error| format!("create {}: {error}", config.data_dir.display()))?;
    let range = config.range.identity();
    let (set, recovery) = open_range(
        &config.data_dir,
        config.segment_id,
        &range,
        SegmentRoll {
            max_bytes: config.max_segment_bytes,
            max_records: config.max_segment_records,
            max_group_bytes: config.max_group_bytes,
            max_record_bytes: config.max_record_bytes,
        },
    )?;
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

    if let Some(retention) = &config.retention {
        // Zero is ambiguous between "reclaim everything eligible" (the
        // storage API's reading) and "disabled" (the broker's atomic
        // sentinel). Refusing it at startup keeps an operator who typed 0
        // from silently getting the unbounded growth this config exists to
        // prevent (#290).
        if retention.max_total_bytes == 0 {
            return Err(
                "retention.max_total_bytes must be greater than zero; omit the retention block                  to disable retention"
                    .to_owned(),
            );
        }
    }

    match config.role {
        DataRole::Follower => {
            run_follower(
                config,
                range,
                set,
                epochs,
                meta,
                observability,
                metrics_addr,
                shutdown,
            )
            .await
        }
        DataRole::Leader => {
            run_leader(
                config,
                range,
                set,
                epochs,
                meta,
                true,
                observability,
                metrics_addr,
                shutdown,
            )
            .await
        }
        DataRole::Candidate => {
            run_candidate(
                config,
                range,
                set,
                epochs,
                meta,
                observability,
                metrics_addr,
                shutdown,
            )
            .await
        }
        DataRole::Standalone => {
            run_leader(
                config,
                range,
                set,
                epochs,
                meta,
                false,
                observability,
                metrics_addr,
                shutdown,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_follower(
    config: DataNodeConfig,
    range: RangeIdentity,
    set: SegmentSet,
    epochs: ProducerEpochJournal,
    meta: MetaFencingEpoch,
    observability: &NodeObservability,
    metrics_addr: Option<std::net::SocketAddr>,
    shutdown: tokio::sync::watch::Receiver<bool>,
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
            set,
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
    if let Some(retention) = &config.retention {
        // Followers reclaim by their own policy exactly as they roll at
        // their own bound: the leader replicates offsets, not files (#290).
        follower.set_retention(Some(vtop_log::RetentionPolicy {
            max_total_bytes: retention.max_total_bytes,
        }));
    }
    observability.register(Box::new(FollowerCollector::new(Arc::clone(&follower))?))?;

    // With a lease configured, this follower learns its epoch from metadata
    // instead of from its config file (#239). Without one it keeps asserting
    // the configured epoch, which is what every pre-#239 config still gets.
    let mut watcher_task = None;
    if let Some(lease) = config.lease.as_ref() {
        // Readiness becomes a conjunction: the replica listener is bound AND
        // the watcher has read metadata once. A follower that does not yet
        // know its epoch refuses every append, so reporting ready before that
        // first read advertises a replica that cannot participate — which is
        // how a freshly started follower sat at offset 0 while its leader
        // moved on without it.
        observability.gate.require_marks(2);
        let watcher = crate::lease_watcher::LeaseWatcher::new(
            lease_admin_client(lease)?,
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
        watcher_task = Some(tokio::spawn(watcher.run(shutdown.clone())));
    }

    let server = ReplicaPeerServer::new(
        tls::replica_material(&config.replica_tls)?,
        config.node_uuid,
        Arc::clone(&follower) as Arc<dyn ReplicaPeerHandler>,
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
        .serve(listener, oneshot_on_shutdown(shutdown.clone()))
        .await
        .map_err(|error| format!("replica server exited: {error}"))?;
    // Orderly stop (#280): the listener is closed and in-flight connections
    // drained; stop the watcher, then write the final commit boundary so the
    // next open finds no torn tail to truncate.
    println!("data_node_stopping role=follower node={}", config.node_uuid);
    if let Some(task) = watcher_task {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), task).await;
    }
    match follower.quiesce() {
        Ok(committed) => println!(
            "data_node_stopped role=follower node={} committed={committed}",
            config.node_uuid
        ),
        Err(error) => eprintln!("final commit failed; recovery will handle it: {error}"),
    }
    Ok(())
}

/// Argument list mirrors what `serve` has already unpacked; grouping it into a
/// struct would move the same list one indirection from its only caller.
#[allow(clippy::too_many_arguments)]
async fn run_leader(
    config: DataNodeConfig,
    range: RangeIdentity,
    set: SegmentSet,
    epochs: ProducerEpochJournal,
    meta: MetaFencingEpoch,
    replicated: bool,
    observability: &NodeObservability,
    metrics_addr: Option<std::net::SocketAddr>,
    shutdown: tokio::sync::watch::Receiver<bool>,
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
            set,
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
        LocalBroker::with_meta_fencing_epoch(set, epochs, range, config.fencing_epoch, meta)
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
    if let Some(retention) = &config.retention {
        broker.set_retention(Some(vtop_log::RetentionPolicy {
            max_total_bytes: retention.max_total_bytes,
        }));
    }
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
            Arc::clone(&broker) as Arc<dyn crate::lease_agent::CandidateLocalView>,
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
    // The agent's exit trigger is NOT the process signal: releasing the
    // lease while the native server still executes an admitted produce would
    // let metadata authorize a successor at a higher epoch under a broker
    // still acking at the old one. run_leader fires this only after the
    // server has drained (#280).
    let (release_lease, release_lease_rx) = tokio::sync::watch::channel(false);
    let mut agent_task = None;
    let mut agent_drain = std::time::Duration::from_secs(5);
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
            lease_admin_client(lease)?,
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
        agent_task = Some(tokio::spawn(agent.run(release_lease_rx)));
        // The drain wait must survive an in-flight admin round trip, whose
        // own budget is the lease duration; past one full lease the release
        // is moot anyway — the lease has lapsed on its own.
        agent_drain = agent_drain.max(Duration::from_millis(lease.lease_duration_ms));
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
                    transfer: LeaderSegmentTransferHandler::new(Arc::clone(&broker)),
                    transfer_allowed: config
                        .followers
                        .iter()
                        .map(|follower| follower.node_uuid)
                        .chain(config.transfer_peers.iter().copied())
                        .collect(),
                }) as Arc<dyn ReplicaPeerHandler>,
            )
            .map_err(|error| error.to_string())?;
            let status_listener = TcpListener::bind(status_listen)
                .await
                .map_err(|error| format!("bind {status_listen}: {error}"))?;
            let status_shutdown = oneshot_on_shutdown(shutdown.clone());
            tokio::spawn(async move {
                if let Err(error) = status_server.serve(status_listener, status_shutdown).await {
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
    let shutdown_rx = oneshot_on_shutdown(shutdown.clone());
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
    server
        .serve(listener, shutdown_rx)
        .await
        .map_err(|error| format!("native server exited: {error}"))?;
    // Orderly stop (#280). The native listener is closed and sessions are
    // drained; the lease agent — racing on the same signal — releases the
    // range so failover need not wait out the lease deadline, and the final
    // commit boundary spares the next open a torn-tail truncation.
    println!(
        "data_node_stopping role={} node={}",
        if replicated { "leader" } else { "standalone" },
        config.node_uuid
    );
    // Stop admitting and drain FIRST (the serve above has returned), only
    // then hand the range back — see the channel's comment for why the order
    // is load-bearing.
    let _ = release_lease.send(true);
    if let Some(task) = agent_task {
        let _ = tokio::time::timeout(agent_drain, task).await;
    }
    match broker.quiesce() {
        Ok(committed) => println!(
            "data_node_stopped role={} node={} committed={committed}",
            if replicated { "leader" } else { "standalone" },
            config.node_uuid
        ),
        Err(error) => eprintln!("final commit failed; recovery will handle it: {error}"),
    }
    Ok(())
}

/// The candidate supervisor (#284): both planes bind ONCE, and the role
/// behind them follows the lease.
///
/// The agent runs for the life of the process; its verdicts arrive on a
/// watch channel and this loop restructures the node around them — build a
/// leader on `Lead`, rebuild a follower on `Follow` — in #280 order, with
/// the transition window served by refusing placeholders. The two state
/// machines that own storage (`InProcessFollower`, `LocalBroker`) take the
/// `SegmentSet` by value, so every transition quiesces, drops, and re-opens
/// the range from disk: the directory is the handoff, exactly as it is
/// between processes, minus the processes.
#[allow(clippy::too_many_arguments)]
async fn run_candidate(
    config: DataNodeConfig,
    range: RangeIdentity,
    set: SegmentSet,
    epochs: ProducerEpochJournal,
    meta: MetaFencingEpoch,
    observability: &NodeObservability,
    metrics_addr: Option<std::net::SocketAddr>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), String> {
    let lease = config.lease.as_ref().ok_or(
        "candidate requires a lease block: the role follows the lease, and a node that \
                cannot observe the lease has no role to follow",
    )?;
    if !config.followers.is_empty() {
        return Err(
            "candidate takes `peers` (the whole symmetric replica set); `followers` is \
                    derived as peers minus self and must not be set"
                .into(),
        );
    }
    if !config
        .peers
        .iter()
        .any(|peer| peer.node_uuid == config.node_uuid)
    {
        return Err(
            "candidate `peers` must include this node: the list is the SYMMETRIC replica \
                    set, identical on every member, and a file that omits its own node is a file \
                    that was edited per node — the exact practice candidate mode retires"
                .into(),
        );
    }
    let peers: Vec<crate::config::FollowerPeerConfig> = config
        .peers
        .iter()
        .filter(|peer| peer.node_uuid != config.node_uuid)
        .cloned()
        .collect();
    if peers.is_empty() {
        return Err(
            "candidate requires at least one peer besides this node; a single-node range \
                    is `standalone`"
                .into(),
        );
    }
    let native_listen = config
        .native_listen
        .as_ref()
        .ok_or("candidate requires native_listen: any member may become the leader")?;
    let native_tls = config
        .native_tls
        .as_ref()
        .ok_or("candidate requires native_tls")?;
    let principal = config
        .principal_id
        .ok_or("candidate requires principal_id")?;
    let replica_listen = config
        .replica_listen
        .as_ref()
        .ok_or("candidate requires replica_listen: any member may become a follower")?;
    let roll = SegmentRoll {
        max_bytes: config.max_segment_bytes,
        max_records: config.max_segment_records,
        max_group_bytes: config.max_group_bytes,
        max_record_bytes: config.max_record_bytes,
    };

    // --- both planes, bound once --------------------------------------------
    let switching = Arc::new(SwitchingReplicaHandler::new(config.node_uuid));
    let replica_server = ReplicaPeerServer::new(
        tls::replica_material(&config.replica_tls)?,
        config.node_uuid,
        Arc::clone(&switching) as Arc<dyn ReplicaPeerHandler>,
    )
    .map_err(|error| error.to_string())?;
    let replica_listener = TcpListener::bind(replica_listen)
        .await
        .map_err(|error| format!("bind {replica_listen}: {error}"))?;
    let replica_shutdown = oneshot_on_shutdown(shutdown.clone());
    tokio::spawn(async move {
        if let Err(error) = replica_server
            .serve(replica_listener, replica_shutdown)
            .await
        {
            tracing::warn!(%error, "candidate replica server exited");
        }
    });

    let slot = Arc::new(vtop_broker::BrokerSlot::empty());
    let native_server = vtop_broker::NativeServer::over_slot(
        Arc::clone(&slot),
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
    observability.register(Box::new(ServerCollector::new(Arc::clone(
        native_server.metrics(),
    ))?))?;
    let native_listener = TcpListener::bind(native_listen)
        .await
        .map_err(|error| format!("bind {native_listen}: {error}"))?;
    let native_shutdown = oneshot_on_shutdown(shutdown.clone());
    tokio::spawn(async move {
        if let Err(error) = native_server.serve(native_listener, native_shutdown).await {
            tracing::warn!(%error, "candidate native server exited");
        }
    });
    // NO role collector in this first slice, deliberately: BrokerCollector
    // and FollowerCollector export the same metric names, the registry
    // refuses duplicate descriptors, and there is no unregister path — a
    // role-agnostic replica collector is the follow-up. The server and
    // lease gauges above are live regardless.

    // --- the agent, for the life of the process -----------------------------
    let endpoints = peers
        .iter()
        .map(|peer| {
            Ok(crate::lease_agent::FollowerEndpoint {
                node_uuid: peer.node_uuid,
                addr: vtop_meta::resolve_endpoint(&peer.addr).map_err(|error| error.to_string())?,
                server_name: peer.server_name.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let view = Arc::new(SwitchingLocalView::empty());
    let probe = crate::lease_agent::ReplicaPlaneProbe::new(
        Arc::clone(&view) as Arc<dyn crate::lease_agent::CandidateLocalView>,
        config.node_uuid,
        vtop_broker::replication::ReplicaStatusClient::new(tls::replica_material(
            &config.replica_tls,
        )?)
        .map_err(|error| error.to_string())?,
        endpoints,
        range.clone(),
    );
    let (verdict_tx, mut verdict_rx) = tokio::sync::watch::channel(RoleVerdict::Undecided);
    let publisher = Arc::new(CandidateLeasePublisher {
        target: std::sync::RwLock::new(None),
        verdicts: verdict_tx,
    });
    let (release_lease, release_lease_rx) = tokio::sync::watch::channel(false);
    let agent = crate::lease_agent::LeaseAgent::new(
        lease_admin_client(lease)?,
        crate::lease_agent::LeaseAgentConfig {
            lease_duration: Duration::from_millis(lease.lease_duration_ms),
            renew_interval: Duration::from_millis(lease.renew_interval_ms),
            poll_interval: Duration::from_millis(lease.poll_interval_ms),
        },
        config.node_uuid,
        lease.topic_uuid,
        config.range.range_id,
        Arc::clone(&publisher) as Arc<dyn crate::lease_agent::LeasePublisher>,
        Some(Arc::new(probe) as Arc<dyn crate::lease_agent::QuorumProbe>),
    )?;
    let agent_drain = std::time::Duration::from_secs(5)
        .max(std::time::Duration::from_millis(lease.lease_duration_ms));
    let mut agent_task = tokio::spawn(agent.run(release_lease_rx));

    // --- readiness ----------------------------------------------------------
    // 0 = following, 1 = leading, 2 = transitioning. A follower is ready the
    // moment its planes are bound — appends are epoch-gated regardless — and
    // a leader is ready only with a live lease view, same as run_leader.
    let role_flag = Arc::new(std::sync::atomic::AtomicU8::new(0));
    let probe_meta = meta.clone();
    let probe_slot = Arc::clone(&slot);
    let probe_flag = Arc::clone(&role_flag);
    observability.set_readiness_probe(Arc::new(move || {
        match probe_flag.load(std::sync::atomic::Ordering::Relaxed) {
            1 => match (probe_meta.try_snapshot(), probe_slot.current()) {
                (Some((epoch, live)), Some(broker)) => {
                    if live && epoch == broker.held_fencing_epoch() {
                        vtop_observe::Readiness::Ready
                    } else {
                        vtop_observe::Readiness::not_ready(
                            "leading, but the lease view is not live at the held epoch".to_owned(),
                        )
                    }
                }
                _ => vtop_observe::Readiness::not_ready(
                    "leading, but the lease view is contended or the broker is absent".to_owned(),
                ),
            },
            2 => vtop_observe::Readiness::not_ready("role transition in progress".to_owned()),
            _ => vtop_observe::Readiness::Ready,
        }
    }));
    observability.gate.mark_ready();
    println!(
        "data_node_ready role=candidate node={} native={native_listen} replica={replica_listen}{}",
        config.node_uuid,
        metrics_addr
            .map(|addr| format!(" metrics={addr}"))
            .unwrap_or_default()
    );
    use std::io::Write;
    std::io::stdout().flush().ok();

    // --- phases -------------------------------------------------------------
    let build_follower =
        |set: SegmentSet, epochs: ProducerEpochJournal| -> Result<Arc<InProcessFollower>, String> {
            let follower = Arc::new(
                InProcessFollower::new(
                    config.node_uuid,
                    set,
                    epochs,
                    range.clone(),
                    config.fencing_epoch,
                    meta.clone(),
                    ClusterCommittedOffset::new(0),
                )
                .map_err(|error| error.to_string())?,
            );
            follower.set_fencing_epoch_journal(
                vtop_broker::fencing_epochs::FencingEpochJournal::open(
                    config.data_dir.join("fencing-epochs"),
                )
                .map_err(|error| error.to_string())?,
            );
            if let Some(retention) = &config.retention {
                follower.set_retention(Some(vtop_log::RetentionPolicy {
                    max_total_bytes: retention.max_total_bytes,
                }));
            }
            Ok(follower)
        };

    let install_follower = |follower: &Arc<InProcessFollower>| {
        switching.install(Arc::clone(follower) as Arc<dyn ReplicaPeerHandler>);
        view.install(Arc::clone(follower) as Arc<dyn crate::lease_agent::CandidateLocalView>);
        publisher.set_target(Some(
            Arc::new(crate::lease_watcher::FollowerLeasePublisher::new(
                Arc::clone(follower),
            )) as Arc<dyn crate::lease_agent::LeasePublisher>,
        ));
        role_flag.store(0, std::sync::atomic::Ordering::Relaxed);
    };

    let initial = build_follower(set, epochs)?;
    install_follower(&initial);
    let mut phase = Phase::Following(initial);

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            changed = verdict_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let verdict = *verdict_rx.borrow();
                match (&phase, verdict) {
                    (Phase::Following(_), RoleVerdict::Lead { fencing_epoch, committed_offset }) => {
                        role_flag.store(2, std::sync::atomic::Ordering::Relaxed);
                        switching.transitioning();
                        publisher.set_target(None);
                        let Phase::Following(follower) =
                            std::mem::replace(&mut phase, Phase::Transitioning)
                        else {
                            unreachable!("matched Following above");
                        };
                        if let Err(error) = follower.quiesce() {
                            eprintln!("pre-promotion commit failed; recovery will handle it: {error}");
                        }
                        // EVERY Arc dropped before the directory reopens: the
                        // handler and view already point elsewhere, and this
                        // binding was the last.
                        drop(follower);
                        match build_leader_phase(
                            &config, &range, &peers, roll, &meta, &slot, &switching, &view,
                            &publisher, fencing_epoch, committed_offset,
                        )
                        .await
                        {
                            Ok(next) => {
                                role_flag.store(1, std::sync::atomic::Ordering::Relaxed);
                                println!(
                                    "data_node_role_changed role=leader node={} epoch={fencing_epoch}",
                                    config.node_uuid
                                );
                                std::io::stdout().flush().ok();
                                phase = next;
                            }
                            Err(error) => {
                                // The lease is held but the leader could not be
                                // built. Fail closed as a follower again: the
                                // unrenewed lease lapses and another candidate
                                // wins.
                                eprintln!("promotion failed to build the leader: {error}");
                                let (set, _) = open_range(
                                    &config.data_dir,
                                    config.segment_id,
                                    &range,
                                    roll,
                                )?;
                                let epochs =
                                    ProducerEpochJournal::open(config.data_dir.join("epochs"))
                                        .map_err(|error| error.to_string())?;
                                let follower = build_follower(set, epochs)?;
                                install_follower(&follower);
                                phase = Phase::Following(follower);
                            }
                        }
                    }
                    (Phase::Leading { publisher: leader_publisher, .. },
                     RoleVerdict::Lead { fencing_epoch, committed_offset }) => {
                        // Re-promotion at a new epoch (a re-grant after a
                        // suspension) completes against the standing leader.
                        crate::lease_agent::LeasePublisher::promote(
                            leader_publisher.as_ref(),
                            fencing_epoch,
                            committed_offset,
                        );
                    }
                    (Phase::Leading { .. }, RoleVerdict::Follow) => {
                        role_flag.store(2, std::sync::atomic::Ordering::Relaxed);
                        // #280 order: no new sessions, no new appends, then
                        // the durable boundary, then the rebuild. The demote
                        // that produced this verdict already forwarded to the
                        // broker, so in-flight requests are refusing.
                        slot.clear();
                        switching.transitioning();
                        publisher.set_target(None);
                        let Phase::Leading { broker, .. } =
                            std::mem::replace(&mut phase, Phase::Transitioning)
                        else {
                            unreachable!("matched Leading above");
                        };
                        if let Err(error) = broker.quiesce() {
                            eprintln!("post-demotion commit failed; recovery will handle it: {error}");
                        }
                        drop(broker);
                        let (set, _) =
                            open_range(&config.data_dir, config.segment_id, &range, roll)?;
                        let epochs = ProducerEpochJournal::open(config.data_dir.join("epochs"))
                            .map_err(|error| error.to_string())?;
                        let follower = build_follower(set, epochs)?;
                        install_follower(&follower);
                        println!(
                            "data_node_role_changed role=follower node={}",
                            config.node_uuid
                        );
                        std::io::stdout().flush().ok();
                        phase = Phase::Following(follower);
                    }
                    _ => {}
                }
            }
        }
    }

    // --- drain (#280) -------------------------------------------------------
    println!(
        "data_node_stopping role=candidate node={}",
        config.node_uuid
    );
    let _ = release_lease.send(true);
    let _ = tokio::time::timeout(agent_drain, &mut agent_task).await;
    match phase {
        Phase::Following(follower) => match follower.quiesce() {
            Ok(committed) => println!(
                "data_node_stopped role=candidate node={} committed={committed}",
                config.node_uuid
            ),
            Err(error) => eprintln!("final commit failed; recovery will handle it: {error}"),
        },
        Phase::Leading { broker, .. } => match broker.quiesce() {
            Ok(committed) => println!(
                "data_node_stopped role=candidate node={} committed={committed}",
                config.node_uuid
            ),
            Err(error) => eprintln!("final commit failed; recovery will handle it: {error}"),
        },
        Phase::Transitioning => {}
    }
    Ok(())
}

/// A candidate's current shape (#284).
enum Phase {
    Following(Arc<InProcessFollower>),
    Leading {
        broker: Arc<LocalBroker>,
        publisher: Arc<crate::lease_agent::BrokerLeasePublisher>,
        /// Held so the follower drivers live exactly as long as the role.
        _replicas: Arc<NetworkedReplicaSet>,
    },
    Transitioning,
}

/// Build the leader half of a candidate: replica set from peers minus self,
/// broker over the reopened range, transfer surface, and the COMPLETED
/// promotion — the boundary the agent proved is published only here, once
/// the broker it authorizes actually exists.
#[allow(clippy::too_many_arguments)]
async fn build_leader_phase(
    config: &DataNodeConfig,
    range: &RangeIdentity,
    peers: &[crate::config::FollowerPeerConfig],
    roll: SegmentRoll,
    meta: &MetaFencingEpoch,
    slot: &Arc<vtop_broker::BrokerSlot>,
    switching: &Arc<SwitchingReplicaHandler>,
    view: &Arc<SwitchingLocalView>,
    publisher: &Arc<CandidateLeasePublisher>,
    fencing_epoch: u64,
    committed_offset: Option<u64>,
) -> Result<Phase, String> {
    let (set, _recovery) = open_range(&config.data_dir, config.segment_id, range, roll)?;
    let epochs = ProducerEpochJournal::open(config.data_dir.join("epochs"))
        .map_err(|error| error.to_string())?;
    let follower_configs = peers
        .iter()
        .map(|peer| {
            Ok(NetworkFollowerConfig {
                node_id: peer.node_uuid,
                addr: vtop_meta::resolve_endpoint(&peer.addr).map_err(|error| error.to_string())?,
                server_name: peer.server_name.clone(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let follower_ids: Vec<Uuid> = follower_configs.iter().map(|f| f.node_id).collect();
    let replicas = Arc::new(
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
    // the dials — the same courtesy run_leader extends, and equally bounded.
    // This runs in the SUPERVISOR, not the agent loop, so renewals continue
    // underneath it.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if follower_ids
            .iter()
            .all(|node| replicas.follower_connected(*node) == Some(true))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let broker = Arc::new(
        LocalBroker::with_replication(
            set,
            epochs,
            range.clone(),
            config.fencing_epoch,
            meta.clone(),
            config.node_uuid,
            Some(ClusterCommittedOffset::new(0)),
            Some(Arc::clone(&replicas) as Arc<dyn ReplicaSet>),
        )
        .map_err(|error| error.to_string())?,
    );
    broker.set_fencing_epoch_journal(
        vtop_broker::fencing_epochs::FencingEpochJournal::open(
            config.data_dir.join("fencing-epochs"),
        )
        .map_err(|error| error.to_string())?,
    );
    if let Some(retention) = &config.retention {
        broker.set_retention(Some(vtop_log::RetentionPolicy {
            max_total_bytes: retention.max_total_bytes,
        }));
    }
    // The transfer surface a leader owes the repair plane, gated on the
    // symmetric allowlist: peers minus self, plus any named repair
    // destinations.
    switching.install(Arc::new(LeaderStatusReplica {
        broker: Arc::clone(&broker),
        node_id: config.node_uuid,
        transfer: LeaderSegmentTransferHandler::new(Arc::clone(&broker)),
        transfer_allowed: peers
            .iter()
            .map(|peer| peer.node_uuid)
            .chain(config.transfer_peers.iter().copied())
            .collect(),
    }) as Arc<dyn ReplicaPeerHandler>);
    view.install(Arc::clone(&broker) as Arc<dyn crate::lease_agent::CandidateLocalView>);
    // COMPLETE the promotion: the verdict recorded the epoch and the proven
    // boundary; only now does a broker exist for them to authorize. Install
    // the real publisher first so a demotion racing this instant forwards
    // to the broker rather than into the void.
    let broker_publisher = Arc::new(crate::lease_agent::BrokerLeasePublisher::new(Arc::clone(
        &broker,
    )));
    publisher.set_target(Some(
        Arc::clone(&broker_publisher) as Arc<dyn crate::lease_agent::LeasePublisher>
    ));
    crate::lease_agent::LeasePublisher::promote(
        broker_publisher.as_ref(),
        fencing_epoch,
        committed_offset,
    );
    slot.install(Arc::clone(&broker));
    Ok(Phase::Leading {
        broker,
        publisher: broker_publisher,
        _replicas: replicas,
    })
}

// ---------------------------------------------------------------------------
// Candidate mode (#284): the role follows the lease.
// ---------------------------------------------------------------------------

/// The replica-plane handler behind a candidate's listener, which outlives
/// any one role (#284). The socket binds once; the delegate swaps at role
/// transitions — `InProcessFollower` while following, `LeaderStatusReplica`
/// while leading, and the trait's refusing defaults mid-transition — so no
/// address ever moves, which is the property a Kubernetes pod needs and the
/// live-chaos harness's port-takeover choreography exists to work around.
struct SwitchingReplicaHandler {
    node_uuid: Uuid,
    delegate: std::sync::RwLock<Arc<dyn ReplicaPeerHandler>>,
}

/// The delegate installed mid-transition: every method inherits the trait's
/// refusing defaults, so a request that races a role change is refused
/// rather than served by a half-built role.
struct TransitioningHandler {
    node_uuid: Uuid,
}

impl ReplicaPeerHandler for TransitioningHandler {
    fn node_id(&self) -> Uuid {
        self.node_uuid
    }

    fn apply_append(
        &self,
        _request: &vtop_protocol::ReplicaAppendRequest,
    ) -> Result<vtop_protocol::ReplicaAppendResponse, (vtop_protocol::ErrorCode, String)> {
        Err((
            vtop_protocol::ErrorCode::InvalidRequest,
            "this candidate is mid role-transition; retry".to_owned(),
        ))
    }

    fn apply_append_batch(
        &self,
        _requests: &[vtop_protocol::ReplicaAppendRequest],
    ) -> Result<vtop_protocol::ReplicaAppendResponse, (vtop_protocol::ErrorCode, String)> {
        Err((
            vtop_protocol::ErrorCode::InvalidRequest,
            "this candidate is mid role-transition; retry".to_owned(),
        ))
    }

    fn observe_hwm(
        &self,
        _update: &vtop_protocol::CommittedHwmUpdate,
    ) -> Result<(), (vtop_protocol::ErrorCode, String)> {
        Err((
            vtop_protocol::ErrorCode::InvalidRequest,
            "this candidate is mid role-transition; retry".to_owned(),
        ))
    }

    fn status(
        &self,
        _range: &vtop_protocol::RangeIdentity,
    ) -> Result<vtop_protocol::ReplicaStatusResponse, (vtop_protocol::ErrorCode, String)> {
        Err((
            vtop_protocol::ErrorCode::InvalidRequest,
            "this candidate is mid role-transition; retry".to_owned(),
        ))
    }
}

impl SwitchingReplicaHandler {
    fn new(node_uuid: Uuid) -> Self {
        Self {
            node_uuid,
            delegate: std::sync::RwLock::new(Arc::new(TransitioningHandler { node_uuid })),
        }
    }

    fn install(&self, delegate: Arc<dyn ReplicaPeerHandler>) {
        *self
            .delegate
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = delegate;
    }

    fn transitioning(&self) {
        self.install(Arc::new(TransitioningHandler {
            node_uuid: self.node_uuid,
        }));
    }

    fn current(&self) -> Arc<dyn ReplicaPeerHandler> {
        Arc::clone(
            &self
                .delegate
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

impl ReplicaPeerHandler for SwitchingReplicaHandler {
    fn node_id(&self) -> Uuid {
        self.node_uuid
    }

    fn apply_append(
        &self,
        request: &vtop_protocol::ReplicaAppendRequest,
    ) -> Result<vtop_protocol::ReplicaAppendResponse, (vtop_protocol::ErrorCode, String)> {
        self.current().apply_append(request)
    }

    fn apply_append_batch(
        &self,
        requests: &[vtop_protocol::ReplicaAppendRequest],
    ) -> Result<vtop_protocol::ReplicaAppendResponse, (vtop_protocol::ErrorCode, String)> {
        self.current().apply_append_batch(requests)
    }

    fn observe_hwm(
        &self,
        update: &vtop_protocol::CommittedHwmUpdate,
    ) -> Result<(), (vtop_protocol::ErrorCode, String)> {
        self.current().observe_hwm(update)
    }

    fn status(
        &self,
        range: &vtop_protocol::RangeIdentity,
    ) -> Result<vtop_protocol::ReplicaStatusResponse, (vtop_protocol::ErrorCode, String)> {
        self.current().status(range)
    }

    fn epoch_history(
        &self,
        range: &vtop_protocol::RangeIdentity,
    ) -> Result<Vec<vtop_protocol::ReplicaEpochStart>, (vtop_protocol::ErrorCode, String)> {
        self.current().epoch_history(range)
    }

    fn fence(
        &self,
        range: &vtop_protocol::RangeIdentity,
        fencing_epoch: u64,
        leader_epoch_starts: &[vtop_broker::fencing_epochs::EpochStart],
    ) -> Result<vtop_protocol::ReplicaFenceResponse, (vtop_protocol::ErrorCode, String)> {
        self.current()
            .fence(range, fencing_epoch, leader_epoch_starts)
    }

    fn list_sealed_segments(
        &self,
        peer: Uuid,
        range: &vtop_protocol::RangeIdentity,
        fencing_epoch: u64,
    ) -> Result<Vec<vtop_protocol::SealedSegmentEntry>, (vtop_protocol::ErrorCode, String)> {
        self.current()
            .list_sealed_segments(peer, range, fencing_epoch)
    }

    fn fetch_segment_chunk(
        &self,
        peer: Uuid,
        request: &vtop_protocol::FetchSegmentChunkRequest,
    ) -> Result<vtop_protocol::FetchSegmentChunkResponse, (vtop_protocol::ErrorCode, String)> {
        self.current().fetch_segment_chunk(peer, request)
    }

    fn seal_tail(
        &self,
        peer: Uuid,
        range: &vtop_protocol::RangeIdentity,
        fencing_epoch: u64,
    ) -> Result<vtop_protocol::SealTailResponse, (vtop_protocol::ErrorCode, String)> {
        self.current().seal_tail(peer, range, fencing_epoch)
    }
}

/// The candidate's own view for the promotion probe, switching with the
/// role (#284).
struct SwitchingLocalView {
    delegate: std::sync::RwLock<Option<Arc<dyn crate::lease_agent::CandidateLocalView>>>,
}

impl SwitchingLocalView {
    fn empty() -> Self {
        Self {
            delegate: std::sync::RwLock::new(None),
        }
    }

    fn install(&self, view: Arc<dyn crate::lease_agent::CandidateLocalView>) {
        *self
            .delegate
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(view);
    }
}

impl crate::lease_agent::CandidateLocalView for SwitchingLocalView {
    fn local_committed_offset(&self) -> u64 {
        self.delegate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map_or(0, |view| view.local_committed_offset())
    }

    fn epoch_starts(&self) -> Vec<vtop_broker::fencing_epochs::EpochStart> {
        self.delegate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map_or_else(Vec::new, |view| view.epoch_starts())
    }
}

/// What the lease agent decided, as the supervisor consumes it (#284).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RoleVerdict {
    /// Startup: no verdict yet; the candidate begins as a follower.
    Undecided,
    /// The agent holds the range; the supervisor must build (or keep) the
    /// leader and then complete the promotion by publishing this boundary.
    Lead {
        fencing_epoch: u64,
        committed_offset: Option<u64>,
    },
    /// The agent does not hold the range.
    Follow,
}

/// The candidate's lease publisher: a VERDICT RECORDER, not a role toggle
/// (#284). Promotion cannot take effect here — the leader it promotes does
/// not exist until the supervisor builds it — so `promote` only records,
/// and the supervisor completes it by calling the real
/// [`crate::lease_agent::BrokerLeasePublisher`] once the broker stands.
/// Demotion and suspension CANNOT wait for the supervisor: fail-closed has
/// no build step, so they forward to the current role object immediately
/// and record second.
struct CandidateLeasePublisher {
    target: std::sync::RwLock<Option<Arc<dyn crate::lease_agent::LeasePublisher>>>,
    verdicts: tokio::sync::watch::Sender<RoleVerdict>,
}

impl CandidateLeasePublisher {
    fn set_target(&self, target: Option<Arc<dyn crate::lease_agent::LeasePublisher>>) {
        *self
            .target
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = target;
    }

    fn forward(&self, act: impl Fn(&dyn crate::lease_agent::LeasePublisher)) {
        if let Some(target) = self
            .target
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
        {
            act(target.as_ref());
        }
    }
}

impl crate::lease_agent::LeasePublisher for CandidateLeasePublisher {
    fn promote(&self, fencing_epoch: u64, committed_offset: Option<u64>) {
        let _ = self.verdicts.send(RoleVerdict::Lead {
            fencing_epoch,
            committed_offset,
        });
    }

    fn demote(&self, fencing_epoch: u64) {
        self.forward(|target| target.demote(fencing_epoch));
        let _ = self.verdicts.send(RoleVerdict::Follow);
    }

    fn suspend(&self, fencing_epoch: u64) {
        self.forward(|target| target.suspend(fencing_epoch));
        // Suspension is not a role change: the epoch is still this node's
        // live grant and the agent will retry. The broker is already
        // refusing (the forward above); rebuilding as a follower here would
        // turn every transient quorum miss into a full teardown.
    }
}

#[cfg(test)]
mod tests {
    /// The production roll thresholds, so these tests exercise the same
    /// rolling behaviour a real node has rather than a size chosen to make
    /// them pass.
    fn test_roll() -> SegmentRoll {
        SegmentRoll {
            max_bytes: crate::config::default_max_segment_bytes(),
            max_records: crate::config::default_max_segment_records(),
            max_group_bytes: crate::config::default_max_group_bytes(),
            max_record_bytes: crate::config::default_max_record_bytes(),
        }
    }

    use super::*;
    use vtop_log::{Durability, LogRecord};

    fn test_range() -> RangeIdentity {
        RangeIdentity {
            topic: "events.v1".to_owned(),
            topic_epoch: 1,
            range_id: Uuid::from_u128(0xC1),
            range_generation: 0,
        }
    }

    fn record(sequence: u64) -> LogRecord {
        LogRecord {
            producer_id: Uuid::from_u128(0xB1),
            producer_epoch: 0,
            sequence,
            timestamp_millis: 1_700_000_000_000,
            attributes: 0,
            key: b"key".to_vec(),
            value: format!("value-{sequence}").into_bytes(),
        }
    }

    /// A fresh directory creates the range's first segment under the
    /// offset-based stem — the naming rolling itself produces — so a new
    /// node's layout and a rolled node's layout follow one contract.
    #[test]
    fn a_fresh_directory_creates_the_first_segment_under_the_offset_stem() {
        let dir = tempfile::tempdir().unwrap();
        let range = test_range();
        let (set, recovery) =
            open_range(dir.path(), Uuid::from_u128(0xD1), &range, test_roll()).unwrap();
        assert!(recovery.is_none(), "nothing existed to recover");
        assert_eq!(set.next_offset(), 0);
        assert!(
            dir.path()
                .join("range-00000000000000000000.active")
                .exists(),
            "the first segment must take the stem its base offset names"
        );
    }

    /// A data directory from before the broker opened through the catalog —
    /// one legacy `range.active`, nothing else — opens as a set of one with
    /// its records intact. This is the compatibility contract: no migration
    /// step, because discovery keys segments by their contents, not their
    /// stems.
    #[test]
    fn a_legacy_single_file_directory_opens_as_a_set_of_one() {
        let dir = tempfile::tempdir().unwrap();
        let range = test_range();
        let descriptor = SegmentDescriptor {
            segment_id: Uuid::from_u128(0xD2),
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
        {
            let mut segment = ActiveSegment::create(
                dir.path().join("range.active"),
                descriptor,
                SegmentConfig::default(),
            )
            .unwrap();
            for sequence in 0..3 {
                segment
                    .append_group(&[record(sequence)], Durability::Fsync)
                    .unwrap();
            }
        }

        let (set, recovery) =
            open_range(dir.path(), Uuid::from_u128(0xD3), &range, test_roll()).unwrap();
        assert!(recovery.is_some(), "the legacy segment was recovered");
        assert_eq!(set.next_offset(), 3);
        assert!(set.sealed().is_empty(), "a legacy layout is a set of one");
    }

    /// The offline seal used by `vtopctl segment verify` and the chaos
    /// harness works on a rolled range: it seals the TAIL, and the segments
    /// the range sealed while running are already the artifacts verification
    /// consumes. Recovery of the tail seeds its inherited producer frontier
    /// from the `.producers` sidecar, which is what makes a mid-range tail
    /// recoverable in isolation at all.
    #[test]
    fn seal_active_seals_the_tail_of_a_rolled_range() {
        let dir = tempfile::tempdir().unwrap();
        let range = test_range();
        let descriptor = SegmentDescriptor {
            segment_id: Uuid::from_u128(0xD5),
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
        let sealed_while_running = {
            let mut set = SegmentSet::create_in(
                &Env::real(),
                dir.path(),
                descriptor,
                SegmentConfig {
                    // Small enough that 40 records roll several times.
                    max_record_bytes: 256,
                    max_group_bytes: 512,
                    max_segment_bytes: 512,
                    max_segment_records: 100,
                    index_stride: 2,
                },
            )
            .unwrap();
            for sequence in 0..40 {
                set.append_group_minting(&[record(sequence)], Durability::Fsync)
                    .unwrap();
            }
            assert!(!set.sealed().is_empty(), "the range must have rolled");
            set.sealed().len()
        };

        let tails: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "active"))
            .collect();
        assert_eq!(tails.len(), 1, "a quiesced range has exactly one tail");

        seal_active(&tails[0]).unwrap();
        assert!(
            tails[0].with_extension("segment").exists(),
            "sealing must publish the tail as a sealed segment"
        );
        let sealed_now = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "segment"))
            .count();
        assert_eq!(
            sealed_now,
            sealed_while_running + 1,
            "the segments sealed while running are untouched; only the tail was added"
        );
    }

    /// A quarantined bundle refuses startup with the reason in the error —
    /// never a node silently serving whichever subset still looks healthy.
    #[test]
    fn a_quarantined_bundle_refuses_startup_naming_the_reason() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("stray.active"), b"not a segment").unwrap();

        let Err(problem) = open_range(
            dir.path(),
            Uuid::from_u128(0xD4),
            &test_range(),
            test_roll(),
        ) else {
            panic!("a quarantined bundle must refuse startup");
        };
        assert!(
            problem.contains("InvalidArtifact") && problem.contains("stray.active"),
            "the refusal must name the reason and the path: {problem}"
        );
    }
}

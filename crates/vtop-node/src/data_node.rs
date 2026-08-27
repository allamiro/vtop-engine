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
        // moved on without it. ADDED to the marks already owed, not declared
        // as a total: under colocated::run the gate is shared with the meta
        // role's own mark, and a stated total of two would let any two of
        // the three components open /readyz (review).
        observability.gate.add_required_marks(1);
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
    {
        // Distinct members, verified (review): a duplicated peer would count
        // toward the quorum twice, letting one reachable node satisfy a
        // majority that exists to require different disks.
        let mut seen = std::collections::BTreeSet::new();
        for peer in &config.peers {
            if !seen.insert(peer.node_uuid) {
                return Err(format!(
                    "candidate `peers` lists {} more than once; a quorum over duplicated \
                     members is a quorum in name only",
                    peer.node_uuid
                ));
            }
        }
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
    // HELD, not detached (review): a listener that dies while this node
    // holds the lease leaves a ready leader with a dead endpoint. The
    // supervisor selects on these handles and treats an early exit as
    // fatal — fail-stop, so the lease lapses and a healthy candidate wins.
    let mut replica_task = tokio::spawn(async move {
        replica_server
            .serve(replica_listener, replica_shutdown)
            .await
            .map_err(|error| format!("candidate replica server exited: {error}"))
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
    let mut native_task = tokio::spawn(async move {
        native_server
            .serve(native_listener, native_shutdown)
            .await
            .map_err(|error| format!("candidate native server exited: {error}"))
    });

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
    // The candidate's ONE role collector, registered here for the life of the
    // process and reading through the switching view. Registering a
    // role-specific collector per transition cannot work — the leader's and
    // follower's collectors export the same descriptors, the registry refuses
    // duplicates, and there is no unregister — so without this a candidate
    // exported no range progress at all: `vtop_broker_local_committed_offset`
    // simply did not exist on a candidate pod, which is how the k8s smoke
    // caught it (a replica nobody can measure is one nobody can operate).
    observability.register(Box::new(crate::observe::CandidateCollector::new(
        Arc::clone(&view) as Arc<dyn crate::observe::ReplicaObservation>,
        &range,
    )?))?;
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
        finished_through: std::sync::atomic::AtomicU64::new(0),
        recorded_through: std::sync::atomic::AtomicU64::new(0),
    });
    let (release_lease, release_lease_rx) = tokio::sync::watch::channel(false);
    // Set when a granted epoch turns out to be unservable (#367); the agent
    // releases it and sits out the next rounds rather than renewing it.
    let stand_down = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Before anything here may want the range (#367). A candidate that cannot
    // write its directory cannot serve what it would win and cannot record
    // that it could not — the loop, with the one mechanism that breaks it
    // removed. Scoped to the candidate path deliberately: a statically
    // configured leader has no election to re-enter.
    refuse_an_unwritable_data_dir(&config.data_dir)?;
    let stand_aside_marker = StandAsideMarker::new(&config.data_dir);
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
    // One MORE mark before /readyz goes green, same shape as the static
    // follower: the binds' own mark plus the agent's first completed
    // metadata exchange (review: a candidate that has never reached
    // metadata reports a follower that refuses every append). Added, not
    // declared as a total, so a colocated meta role's mark stays counted.
    observability.gate.add_required_marks(1);
    let mut agent = agent
        .with_ready_gate(observability.gate.clone())
        .with_stand_down(Arc::clone(&stand_down));
    // A previous incarnation of this process may have been granted an epoch it
    // could not serve (#367). The marker outlives the process precisely because
    // the hold-off cannot.
    let stand_aside = stand_aside_marker.take();
    if !matches!(stand_aside, StandAside::NeverFailed) {
        let rounds = crate::lease_agent::stand_aside_rounds_for(
            Duration::from_millis(lease.lease_duration_ms),
            Duration::from_millis(lease.poll_interval_ms),
        );
        match &stand_aside {
            StandAside::Failed(failed_epoch) => eprintln!(
                "a previous run of this node was granted epoch {failed_epoch} and could \
                 not serve it; standing aside for {rounds} poll round(s) so another \
                 candidate gets an uncontested turn"
            ),
            StandAside::Unreadable(why) => eprintln!(
                "a previous run of this node left a stand-aside marker that could not be \
                 read ({why}); standing aside for {rounds} poll round(s) anyway, because \
                 an unreadable marker is a marker"
            ),
            StandAside::NeverFailed => unreachable!("excluded above"),
        }
        agent = agent.standing_aside(rounds);
    }
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
    // Same contract as run_leader's probe: non-blocking, and contention
    // serves the LAST DECIDED verdict rather than guessing — a produce
    // mid-fsync holds the lease view for its whole critical section, and a
    // probe landing then must not drain a healthy leader (review).
    let probe_last = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let probe_last_probe = Arc::clone(&probe_last);
    observability.set_readiness_probe(Arc::new(move || {
        match probe_flag.load(std::sync::atomic::Ordering::Relaxed) {
            1 => match probe_slot.current() {
                Some(broker) => match probe_meta.try_snapshot() {
                    Some((epoch, live)) => {
                        let ready = live && epoch == broker.held_fencing_epoch();
                        probe_last_probe.store(ready, std::sync::atomic::Ordering::Relaxed);
                        if ready {
                            vtop_observe::Readiness::Ready
                        } else {
                            vtop_observe::Readiness::not_ready(
                                "leading, but the lease view is not live at the held epoch"
                                    .to_owned(),
                            )
                        }
                    }
                    None => {
                        if probe_last_probe.load(std::sync::atomic::Ordering::Relaxed) {
                            vtop_observe::Readiness::Ready
                        } else {
                            vtop_observe::Readiness::not_ready(
                                "lease view contended; last decided state was fenced".to_owned(),
                            )
                        }
                    }
                },
                None => {
                    probe_last_probe.store(false, std::sync::atomic::Ordering::Relaxed);
                    vtop_observe::Readiness::not_ready(
                        "leading, but the broker is absent".to_owned(),
                    )
                }
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
        view.install(Arc::clone(follower) as Arc<dyn CandidateSurface>);
        publisher.set_target(Some(Arc::new(FollowerObservationAdapter {
            inner: crate::lease_watcher::FollowerLeasePublisher::new(Arc::clone(follower)),
        })
            as Arc<dyn crate::lease_agent::LeasePublisher>));
        role_flag.store(0, std::sync::atomic::Ordering::Relaxed);
    };

    let initial = build_follower(set, epochs)?;
    install_follower(&initial);
    let mut phase = Phase::Following(initial);

    let mut native_task_done = false;
    // A fatal plane failure, carried out of the loop instead of returned from
    // inside it.
    //
    // Returning early skipped the drain below — which is where the lease is
    // RELEASED — so a node that fail-stopped kept the range until its deadline
    // and the survivors could not take it for a full lease duration. Worse, a
    // restart inside that window sees itself as the holder and tries to serve
    // again, which is #367's loop reached by a different door. Every exit now
    // leaves through the same drain, so handing the range back is not
    // something a new failure path can forget to do.
    let mut fatal: Option<String> = None;
    'supervisor: loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            // A plane dying under a live candidate is fail-stop (review): a
            // ready leader with a dead endpoint pins metadata to a leader
            // nobody can reach, and readiness reads the slot, not the task.
            // But the SAME watch that breaks this loop also stops both
            // servers, and select does not order its arms — a listener
            // finishing its orderly shutdown first must read as the
            // shutdown it is, not as a dead plane, or the node would exit
            // without draining and let its lease expire instead of
            // releasing it (review, round three).
            result = &mut native_task => {
                if *shutdown.borrow() {
                    if let Ok(Err(error)) = &result {
                        eprintln!("native server error during shutdown: {error}");
                    }
                    native_task_done = true;
                    break;
                }
                // Marked done: the drain must not poll a JoinHandle this arm
                // has already driven to completion.
                native_task_done = true;
                fatal = Some(match result {
                    Ok(Ok(())) => "candidate native server exited early".to_owned(),
                    Ok(Err(error)) => error,
                    Err(join) => format!("candidate native server task failed: {join}"),
                });
                break 'supervisor;
            }
            result = &mut replica_task => {
                if *shutdown.borrow() {
                    if let Ok(Err(error)) = &result {
                        eprintln!("replica server error during shutdown: {error}");
                    }
                    break;
                }
                fatal = Some(match result {
                    Ok(Ok(())) => "candidate replica server exited early".to_owned(),
                    Ok(Err(error)) => error,
                    Err(join) => format!("candidate replica server task failed: {join}"),
                });
                break 'supervisor;
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
                        // The cached probe verdict belongs to the role that
                        // decided it (review): a fresh promotion must start
                        // fail-closed, not inherit the prior leader's green.
                        probe_last.store(false, std::sync::atomic::Ordering::Relaxed);
                        view.clear();
                        publisher.set_target(None);
                        let Phase::Following(follower) =
                            std::mem::replace(&mut phase, Phase::Transitioning)
                        else {
                            unreachable!("matched Following above");
                        };
                        // THE DRAIN, disguised as a suspension (review): a
                        // replication call that took the old delegate before
                        // the handler switched still holds its own Arc, and
                        // dropping our binding does not end it. But every
                        // such call holds the shared meta view's lock from
                        // fence check through write, so this suspend —
                        // which must take that same lock — cannot return
                        // until the last in-flight call has finished, and
                        // every call after it refuses under the guard. Only
                        // then is reopening the directory safe; the retained
                        // object outlives the swap as a refusal machine, not
                        // a writer. The promotion's own set() reactivates
                        // the view at the granted epoch.
                        follower
                            .meta_fencing_epoch()
                            .suspend(follower.meta_fencing_epoch().get());
                        if let Err(error) = follower.quiesce() {
                            eprintln!("pre-promotion commit failed; recovery will handle it: {error}");
                        }
                        // EVERY Arc dropped before the directory reopens: the
                        // handler and view already point elsewhere, and this
                        // binding was the last.
                        drop(follower);
                        // STAND DOWN on a failed build (#367), where this
                        // used to fail-stop. The property worth keeping is
                        // that a node never renews a lease over a slot it
                        // cannot serve. Exiting achieved that and then made
                        // things worse: its stated reasoning — "exiting lets
                        // the lease lapse and a healthy candidate win" — is
                        // false under an orchestrator, which restarts the pod
                        // well inside the lease duration. The fresh process
                        // campaigns at once, wins again because nothing marks
                        // it as the node that just failed, and fails
                        // identically. Five grants in ninety seconds, all to
                        // the same replica, none ever served, while the two
                        // healthy ones starve.
                        //
                        // So the node hands the epoch back and sits out the
                        // next rounds instead, and stays up as a follower —
                        // which also leaves something diagnosable behind
                        // rather than a restart count.
                        // The build can wait up to ten seconds on follower
                        // streams, and the supervisor's select arms are
                        // unreachable while it does — so the build is RACED
                        // against the very things those arms watch (review,
                        // round four). A listener dying mid-build is the
                        // same fail-stop as ever; shutdown mid-build
                        // abandons a leader nothing has published yet and
                        // runs the ordinary drain.
                        let build = build_leader_phase(&config, &range, &peers, roll, &meta);
                        tokio::pin!(build);
                        let built = loop {
                            tokio::select! {
                                built = &mut build => {
                                    // Both can be ready; select does not
                                    // order its arms. A build completing
                                    // during shutdown is still a shutdown
                                    // (review, round five): abandon the
                                    // unpublished leader — and abandon a
                                    // failed build the same way, because an
                                    // orderly stop must not exit as a
                                    // build failure.
                                    if *shutdown.borrow() {
                                        break 'supervisor;
                                    }
                                    match built {
                                        Ok(built) => break built,
                                        Err(error) => {
                                            // Hand the epoch back and sit out
                                            // (#367), then rebuild as a
                                            // follower below.
                                            eprintln!(
                                                "leader build failed at epoch \
                                                 {fencing_epoch}; standing down and \
                                                 letting another candidate take the \
                                                 range: {error}"
                                            );
                                            stand_down.store(
                                                true,
                                                std::sync::atomic::Ordering::SeqCst,
                                            );
                                            // AND on disk, because this
                                            // process may not live long enough
                                            // to serve the hold-off it just
                                            // armed (#367).
                                            stand_aside_marker.record(fencing_epoch);
                                            publisher.set_target(None);
                                            let (set, _) = open_range(
                                                &config.data_dir,
                                                config.segment_id,
                                                &range,
                                                roll,
                                            )?;
                                            let epochs = ProducerEpochJournal::open(
                                                config.data_dir.join("epochs"),
                                            )
                                            .map_err(|error| error.to_string())?;
                                            let follower = build_follower(set, epochs)?;
                                            install_follower(&follower);
                                            phase = Phase::Following(follower);
                                            println!(
                                                "data_node_role_changed role=follower \
                                                 node={} note=leader-build-failed",
                                                config.node_uuid
                                            );
                                            std::io::stdout().flush().ok();
                                            continue 'supervisor;
                                        }
                                    }
                                }
                                changed = shutdown.changed() => {
                                    if changed.is_err() || *shutdown.borrow() {
                                        break 'supervisor;
                                    }
                                }
                                result = &mut native_task => {
                                    if *shutdown.borrow() {
                                        if let Ok(Err(error)) = &result {
                                            eprintln!(
                                                "native server error during shutdown: {error}"
                                            );
                                        }
                                        native_task_done = true;
                                        break 'supervisor;
                                    }
                                    native_task_done = true;
                                    fatal = Some(match result {
                                        Ok(Ok(())) => {
                                            "candidate native server exited early".to_owned()
                                        }
                                        Ok(Err(error)) => error,
                                        Err(join) => format!(
                                            "candidate native server task failed: {join}"
                                        ),
                                    });
                                    break 'supervisor;
                                }
                                result = &mut replica_task => {
                                    if *shutdown.borrow() {
                                        if let Ok(Err(error)) = &result {
                                            eprintln!(
                                                "replica server error during shutdown: {error}"
                                            );
                                        }
                                        break 'supervisor;
                                    }
                                    fatal = Some(match result {
                                        Ok(Ok(())) => {
                                            "candidate replica server exited early".to_owned()
                                        }
                                        Ok(Err(error)) => error,
                                        Err(join) => format!(
                                            "candidate replica server task failed: {join}"
                                        ),
                                    });
                                    break 'supervisor;
                                }
                            }
                        };
                        // ATOMIC COMPLETION (review P0, round two): a lease
                        // lost during the build — the follower-stream wait
                        // can take seconds — must win over the build, and a
                        // recheck separate from publication only narrowed
                        // that race. `complete_promotion` closes it: the
                        // publish and the target install happen under the
                        // same lock the demote path holds, so a demotion
                        // either landed first (the ceiling refuses this
                        // completion — nothing was published) or lands
                        // after (it reaches the broker publisher installed
                        // here and fences the live broker).
                        let broker_publisher =
                            Arc::new(crate::lease_agent::BrokerLeasePublisher::new(
                                Arc::clone(&built.broker),
                            ));
                        if !publisher.complete_promotion(
                            Arc::new(LeadingDemoteAdapter {
                                broker: Arc::clone(&built.broker),
                                inner: Arc::clone(&broker_publisher),
                            })
                                as Arc<dyn crate::lease_agent::LeasePublisher>,
                            fencing_epoch,
                            committed_offset,
                        ) {
                            if let Err(error) = built.broker.quiesce() {
                                eprintln!(
                                    "post-abandoned-build commit failed; recovery will handle \
                                     it: {error}"
                                );
                            }
                            drop(built);
                            let (set, _) =
                                open_range(&config.data_dir, config.segment_id, &range, roll)?;
                            let epochs =
                                ProducerEpochJournal::open(config.data_dir.join("epochs"))
                                    .map_err(|error| error.to_string())?;
                            let follower = build_follower(set, epochs)?;
                            install_follower(&follower);
                            phase = Phase::Following(follower);
                            println!(
                                "data_node_role_changed role=follower node={} \
                                 note=lease-moved-during-promotion",
                                config.node_uuid
                            );
                            std::io::stdout().flush().ok();
                            continue;
                        }
                        // The promotion is live and every later demotion
                        // reaches the broker publisher; only now does the
                        // broker become observable and reachable. A demote
                        // between the completion and these installs fences
                        // the broker before any session could reach it.
                        switching.install(Arc::clone(&built.status));
                        view.install(Arc::clone(&built.broker) as Arc<dyn CandidateSurface>);
                        slot.install(Arc::clone(&built.broker));
                        role_flag.store(1, std::sync::atomic::Ordering::Relaxed);
                        println!(
                            "data_node_role_changed role=leader node={} epoch={fencing_epoch}",
                            config.node_uuid
                        );
                        std::io::stdout().flush().ok();
                        phase = Phase::Leading {
                            broker: built.broker,
                            publisher: broker_publisher,
                            _replicas: built.replicas,
                        };
                    }
                    (Phase::Leading { broker, publisher: leader_publisher, .. },
                     RoleVerdict::Lead { fencing_epoch, committed_offset }) => {
                        // Re-promotion at a new epoch (a re-grant after a
                        // suspension) completes against the standing leader —
                        // through the SAME atomic handoff as a fresh build
                        // (review): a direct promote here bypassed the
                        // demotion lock and ceilings, so a lease lost after
                        // this verdict was read could be fenced by its
                        // demotion and then reactivated by this stale
                        // promotion until the queued Follow was processed.
                        // A refusal publishes nothing; the queued verdict
                        // decides what happens next.
                        publisher.complete_promotion(
                            Arc::new(LeadingDemoteAdapter {
                                broker: Arc::clone(broker),
                                inner: Arc::clone(leader_publisher),
                            })
                                as Arc<dyn crate::lease_agent::LeasePublisher>,
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
                        // The cached probe verdict belongs to the role that
                        // decided it (review): a fresh promotion must start
                        // fail-closed, not inherit the prior leader's green.
                        probe_last.store(false, std::sync::atomic::Ordering::Relaxed);
                        view.clear();
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

    // A FATAL EXIT STOPS THE SURVIVING PLANE FIRST (review).
    //
    // Only one listener fails; the other is still serving, and nobody has told
    // it to stop — the shutdown watch belongs to the caller and this is not a
    // shutdown. The drain below awaits the native listener within
    // `agent_drain`, which is at least a full lease duration, so a replica
    // failure would have waited that entire window before releasing: exactly
    // the delay this commit exists to remove, reintroduced on the other path.
    //
    // Aborted rather than asked politely, and the trade is deliberate. A
    // fail-stop has already decided this process cannot serve the range, so
    // the choice is between dropping in-flight sessions now and holding the
    // range for fifteen seconds while they finish. In-flight work is
    // crash-equivalent either way — a produce acks only after fsync, so
    // anything an abort interrupts was never acknowledged — while a range
    // nobody can serve is a range nobody else can take.
    if fatal.is_some() {
        // A FATAL EXIT IS ALSO A FAILURE TO SERVE (#367). This process was
        // granted an epoch and its plane died under it; the restart that
        // follows must not walk straight back into the same grant. The
        // in-process hold-off cannot help — there is no next round of this
        // process — so the marker is the only thing that carries the fact
        // across.
        //
        // THE EPOCH COMES FROM THE VERDICT WHEN THERE IS NO LEADER TO ASK
        // (review). Keying this on `Phase::Leading` alone missed the window
        // the marker most needs to cover: a listener that dies while the
        // promotion is still being built leaves `phase` at `Transitioning`,
        // and the agent has ALREADY accepted the grant — so the exit that
        // follows is precisely an unservable epoch, and it was the one case
        // that recorded nothing. The last verdict is the agent's own answer
        // to "what was I granted", which is the question being asked.
        let failed_epoch = match &phase {
            Phase::Leading { broker, .. } => Some(broker.held_fencing_epoch()),
            _ => match *verdict_rx.borrow() {
                RoleVerdict::Lead { fencing_epoch, .. } => Some(fencing_epoch),
                _ => None,
            },
        };
        if let Some(fencing_epoch) = failed_epoch {
            stand_aside_marker.record(fencing_epoch);
        }
        native_task.abort();
        replica_task.abort();
        // Aborted handles must not be awaited again below.
        native_task_done = true;
    }

    // --- drain (#280) -------------------------------------------------------
    // The leader's ordering, in-process: stop admission, drain the native
    // sessions, and only THEN release — a release racing an admitted
    // produce would let metadata authorize a successor under a broker
    // still acking at the old epoch (review; run_leader's own invariant).
    println!(
        "data_node_stopping role=candidate node={}",
        config.node_uuid
    );
    if matches!(phase, Phase::Leading { .. }) {
        slot.clear();
    }
    // Both server tasks were signalled by the shutdown watch already; await
    // the native drain (the accept loop joins its sessions) within the same
    // budget the agent gets. A drain that runs out the budget is SAID, not
    // swallowed: aborted session futures cannot cancel a request already
    // inside `spawn_blocking`, so a timeout here means such a request may
    // still be running (review) — and the quiesce below is what bounds it.
    if !native_task_done
        && tokio::time::timeout(agent_drain, &mut native_task)
            .await
            .is_err()
    {
        eprintln!(
            "native drain ran out its budget; an admitted request may still \
             be in flight — quiescing before release to serialize with it"
        );
    }
    // QUIESCE BEFORE RELEASE when leading (review, round two): `quiesce`
    // takes the same state lock every admitted append holds through its
    // critical section, so it cannot return until in-flight blocking work
    // has committed — and its commit is then durable before the release
    // lets metadata authorize a successor. A straggler that reaches the
    // lock after this has no client left to ack (its session future is
    // gone) and cannot reach quorum once the successor fences the
    // followers: the same exposure as a SIGKILL, which the protocol
    // already tolerates.
    let committed = match &phase {
        Phase::Following(follower) => Some(follower.quiesce()),
        Phase::Leading { broker, .. } => Some(broker.quiesce()),
        Phase::Transitioning => None,
    };
    let _ = release_lease.send(true);
    let _ = tokio::time::timeout(agent_drain, &mut agent_task).await;
    match committed {
        Some(Ok(committed)) => println!(
            "data_node_stopped role=candidate node={} committed={committed}",
            config.node_uuid
        ),
        Some(Err(error)) => {
            eprintln!("final commit failed; recovery will handle it: {error}")
        }
        None => {}
    }
    // THE FAILURE IS REPORTED LAST, after the range has been handed back. The
    // node still exits — a dead plane is not something it can serve through —
    // but it exits having released the lease rather than holding it to its
    // deadline, so a survivor can take the range now instead of in fifteen
    // seconds, and a fast restart cannot find itself still the holder.
    if let Some(error) = fatal {
        return Err(error);
    }
    Ok(())
}

/// The marker a candidate leaves behind when it was granted an epoch it could
/// not serve (#367).
///
/// The hold-off that stops a failed candidate from immediately re-winning the
/// range lives in the lease agent, which lives in the process. When the
/// process ends — a fail-stop, a panic, an orchestrator restart — that memory
/// goes with it, the fresh agent campaigns at once, wins because nothing marks
/// it as the replica that just failed, and fails identically. Five grants in
/// ninety seconds, none of them served.
///
/// So the fact outlives the process, in the one place that also does: the
/// range's own data directory. It is a fact about THIS replica and THIS range,
/// which is exactly what a per-range directory is.
///
/// ONE-SHOT BY CONSTRUCTION. Reading it removes it, so a node stands aside for
/// one hold-off and then competes normally. A marker that persisted would be
/// worse than the loop it prevents: a single-candidate range would never come
/// back, and the failure this guards against is usually transient (a peer's
/// DNS that had not resolved yet, a port still held by the dying process).
struct StandAsideMarker {
    path: std::path::PathBuf,
}

impl StandAsideMarker {
    fn new(data_dir: &std::path::Path) -> Self {
        Self {
            path: data_dir.join(".stand-aside"),
        }
    }

    /// Record that this node could not serve `fencing_epoch`.
    ///
    /// Best effort, deliberately: a node that cannot write here is a node
    /// whose data directory is already unusable, which is the same condition
    /// that stopped it serving. Failing the shutdown over the marker would
    /// trade a recoverable loop for an unrecoverable stop.
    fn record(&self, fencing_epoch: u64) {
        if let Err(error) = std::fs::write(&self.path, fencing_epoch.to_string()) {
            eprintln!(
                "could not record the stand-aside marker at {}; a restart may \
                 re-acquire the range it just failed to serve: {error}",
                self.path.display()
            );
        }
    }

    /// Consume the marker, if one is there.
    ///
    /// ABSENT AND UNREADABLE ARE DIFFERENT ANSWERS (review). The first
    /// version of this collapsed them with `.ok()?`, so an I/O error or a
    /// truncated write read as "this node never failed" — the marker's whole
    /// purpose discarded by the one class of fault most likely to have
    /// written it badly. Only `NotFound` now means no marker; anything else
    /// stands the node aside, because the safe reading of a marker we cannot
    /// read is that it said something.
    fn take(&self) -> StandAside {
        let contents = match std::fs::read_to_string(&self.path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return StandAside::NeverFailed
            }
            Err(error) => {
                // Removed on this path too: an unreadable marker that
                // survived its own reading would sit this node out forever.
                let _ = std::fs::remove_file(&self.path);
                return StandAside::Unreadable(error.to_string());
            }
        };
        // Removed BEFORE the value is used: a marker that survives its own
        // reading would stand this node aside on every restart forever.
        let _ = std::fs::remove_file(&self.path);
        match contents.trim().parse() {
            Ok(fencing_epoch) => StandAside::Failed(fencing_epoch),
            Err(_) => StandAside::Unreadable(format!(
                "the marker held {contents:?}, which is not an epoch"
            )),
        }
    }
}

/// What the stand-aside marker had to say (#367).
enum StandAside {
    /// No marker. This node has no reason to sit out.
    NeverFailed,
    /// A marker naming the epoch a previous incarnation could not serve.
    Failed(u64),
    /// A marker was there and could not be read, or did not parse. Treated as
    /// a failure rather than an absence — see `take`.
    Unreadable(String),
}

/// Prove the data directory is writable before this node may campaign (#367).
///
/// This is the fail-closed half the marker cannot provide for itself
/// (review). `record` is best effort on purpose: it runs on the way out of a
/// process that has already decided it cannot serve, and failing a shutdown
/// over a note trades a recoverable loop for an unrecoverable stop. But the
/// hazard behind that trade is real — a node whose directory filled up or
/// remounted read-only cannot write the marker, restarts with nothing to read,
/// campaigns at once, and fails again. So the condition is refused where it
/// can be refused: at startup, before the agent is allowed to want the range
/// at all.
///
/// Opening the range does not catch it. A directory that became unwritable
/// after the last clean stop still OPENS — reads succeed, and the first write
/// is a produce that has already been acknowledged as this node's job.
///
/// The probe is a fixed name rather than a unique one so a process killed
/// between the write and the unlink leaves one empty dotfile that the next
/// start overwrites, instead of accumulating litter the range sweep does not
/// recognise.
fn refuse_an_unwritable_data_dir(data_dir: &std::path::Path) -> Result<(), String> {
    let probe = data_dir.join(".writable-probe");
    std::fs::write(&probe, b"").map_err(|error| {
        format!(
            "{} is not writable ({error}); refusing to campaign for a range this node              could not serve and could not even record that it could not serve",
            data_dir.display()
        )
    })?;
    let _ = std::fs::remove_file(&probe);
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

/// A leader BUILT but not yet serving: nothing in it is installed,
/// promoted, or reachable until the supervisor rechecks the verdict — a
/// lease lost during this build must win over the build (review P0).
struct BuiltLeader {
    broker: Arc<LocalBroker>,
    status: Arc<dyn ReplicaPeerHandler>,
    replicas: Arc<NetworkedReplicaSet>,
}

/// Build the leader half of a candidate: replica set from peers minus
/// self, broker over the reopened range, transfer surface. PURE
/// CONSTRUCTION — the promotion completes in the supervisor, after it has
/// confirmed the verdict that ordered this build still stands.
async fn build_leader_phase(
    config: &DataNodeConfig,
    range: &RangeIdentity,
    peers: &[crate::config::FollowerPeerConfig],
    roll: SegmentRoll,
    meta: &MetaFencingEpoch,
) -> Result<BuiltLeader, String> {
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
    // destinations. BUILT here, INSTALLED by the supervisor — nothing this
    // function does may take effect before the verdict recheck.
    let status = Arc::new(LeaderStatusReplica {
        broker: Arc::clone(&broker),
        node_id: config.node_uuid,
        transfer: LeaderSegmentTransferHandler::new(Arc::clone(&broker)),
        transfer_allowed: peers
            .iter()
            .map(|peer| peer.node_uuid)
            .chain(config.transfer_peers.iter().copied())
            .collect(),
    }) as Arc<dyn ReplicaPeerHandler>;
    Ok(BuiltLeader {
        broker,
        status,
        replicas,
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
/// The two surfaces a candidate's range is read through, in one object.
///
/// The probe needs the TRUE offset and blocks for it; the metrics scrape
/// must never block and takes a stale gauge instead. One installed role
/// answers both, so the switching view carries both traits rather than
/// keeping two containers that could disagree about which role is current
/// mid-transition.
trait CandidateSurface:
    crate::lease_agent::CandidateLocalView + crate::observe::ReplicaObservation
{
}

impl<T> CandidateSurface for T where
    T: crate::lease_agent::CandidateLocalView + crate::observe::ReplicaObservation
{
}

struct SwitchingLocalView {
    delegate: std::sync::RwLock<Option<Arc<dyn CandidateSurface>>>,
}

impl SwitchingLocalView {
    fn empty() -> Self {
        Self {
            delegate: std::sync::RwLock::new(None),
        }
    }

    fn install(&self, view: Arc<dyn CandidateSurface>) {
        *self
            .delegate
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(view);
    }

    /// Release the current delegate. Called BEFORE a transition drops its
    /// role object (review): this view's Arc would otherwise keep the old
    /// storage owner alive across the reopen, and the handoff contract is
    /// that the directory has exactly one owner at a time.
    fn clear(&self) {
        *self
            .delegate
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
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

impl crate::observe::ReplicaObservation for SwitchingLocalView {
    fn try_local_offsets(&self) -> Option<(u64, u64)> {
        self.delegate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .and_then(|view| view.try_local_offsets())
    }

    fn cluster_committed_offset(&self) -> Option<u64> {
        self.delegate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .and_then(|view| view.cluster_committed_offset())
    }

    fn try_meta_fencing_epoch(&self) -> Option<(u64, bool)> {
        self.delegate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .and_then(|view| view.try_meta_fencing_epoch())
    }

    fn held_fencing_epoch(&self) -> u64 {
        self.delegate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map_or(0, |view| view.held_fencing_epoch())
    }

    fn is_leading(&self) -> bool {
        self.delegate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_some_and(|view| view.is_leading())
    }

    /// ONE acquisition for the whole reading, which is the point of it
    /// (review). Asked question by question, `clear()` could land between "is
    /// a role installed?" and the answers, and the scrape would publish this
    /// view's empty answers — held epoch 0, leading false — as though a role
    /// had given them, rewinding a monotonic gauge and leaving the departed
    /// leader's `lease_active` of 1 standing beside it. Holding the read lock
    /// across all three makes that interleaving unrepresentable.
    ///
    /// Safe to hold it across them because every call under this guard is an
    /// atomic load or a `try_` read: the guard cannot park a transition behind
    /// a scrape, which is the direction that would actually hurt.
    fn role_reading(&self) -> Option<crate::observe::RoleReading> {
        let delegate = self
            .delegate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let view = delegate.as_ref()?;
        Some(crate::observe::RoleReading {
            meta: view.try_meta_fencing_epoch(),
            held: view.held_fencing_epoch(),
            leading: view.is_leading(),
        })
    }
}

/// The watcher's semantics for an agent-driven follower (#284).
///
/// A LeaseWatcher translates an OBSERVED grant into `promote(epoch, None)` —
/// "serve this epoch" — because that is what a granted epoch means to a
/// follower. The lease agent speaks the holder's dialect: a rival's grant
/// arrives as `demote(rival_epoch)`. For a FOLLOWING candidate every demote
/// is exactly that observation (a non-holder has no renewal to lose), so
/// this adapter translates it back — without it, a following candidate
/// clears its lease view on every poll and refuses every append from the
/// leader it is supposed to follow, which is how scenario 14's first quorum
/// produce found three healthy replicas and zero acks.
struct FollowerObservationAdapter {
    inner: crate::lease_watcher::FollowerLeasePublisher,
}

impl crate::lease_agent::LeasePublisher for FollowerObservationAdapter {
    fn promote(&self, fencing_epoch: u64, committed_offset: Option<u64>) {
        self.inner.promote(fencing_epoch, committed_offset);
    }

    fn demote(&self, fencing_epoch: u64) {
        // A rival holds the range at this epoch: to a follower, that IS the
        // grant to serve under.
        self.inner.promote(fencing_epoch, None);
    }

    fn suspend(&self, fencing_epoch: u64) {
        self.inner.suspend(fencing_epoch);
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

/// The LEADING candidate's demotion dialect (#284): fence what THIS node
/// held, not the epoch the rival was granted. run_leader's static broker
/// clears through the rival's epoch, and that is correct THERE — the
/// process never follows, so poisoning reactivation at the rival's epoch
/// costs nothing. A candidate becomes the rival's follower NEXT:
/// `clear_lease` records its epoch in the shared view's `released_through`,
/// and recording the RIVAL's would leave the successor's own grant
/// unactivatable — a deposed but otherwise healthy candidate refusing
/// every append for the successor's entire epoch (review). Clearing the
/// broker's own held epoch fences it just as surely, and leaves the
/// successor's epoch free to activate the rebuilt follower.
struct LeadingDemoteAdapter {
    broker: Arc<LocalBroker>,
    inner: Arc<crate::lease_agent::BrokerLeasePublisher>,
}

impl crate::lease_agent::LeasePublisher for LeadingDemoteAdapter {
    fn promote(&self, fencing_epoch: u64, committed_offset: Option<u64>) {
        crate::lease_agent::LeasePublisher::promote(
            self.inner.as_ref(),
            fencing_epoch,
            committed_offset,
        );
    }

    fn demote(&self, _rival_epoch: u64) {
        crate::lease_agent::LeasePublisher::demote(
            self.inner.as_ref(),
            self.broker.held_fencing_epoch(),
        );
    }

    fn suspend(&self, fencing_epoch: u64) {
        crate::lease_agent::LeasePublisher::suspend(self.inner.as_ref(), fencing_epoch);
    }
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
    /// The highest epoch ever demoted through this publisher. A recorded
    /// promotion is completed only while its epoch is above this ceiling,
    /// and the check happens under the SAME lock the demotion path holds —
    /// so a demote racing a completion either lands first and refuses it,
    /// or lands second and reaches the just-installed broker publisher.
    /// There is no third interleaving (review: a check separate from the
    /// publication only narrowed the window; this closes it).
    finished_through: std::sync::atomic::AtomicU64,
    /// The highest epoch ever RECORDED as a promotion. A completion below
    /// this watermark is superseded — the agent has since verified a newer
    /// grant — and installing it would publish a boundary metadata has
    /// moved past (review: reachable via suspend-then-regrant, which never
    /// raises the finished ceiling). The refusal is safe because the newer
    /// verdict is already queued: the supervisor's next iteration builds
    /// for the grant that superseded this one.
    recorded_through: std::sync::atomic::AtomicU64,
}

impl CandidateLeasePublisher {
    fn set_target(&self, target: Option<Arc<dyn crate::lease_agent::LeasePublisher>>) {
        *self
            .target
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = target;
    }

    /// Complete a recorded promotion: publish the boundary through the
    /// real broker publisher and make it the demotion target, atomically
    /// against the demote path. Returns false — publishing NOTHING — if a
    /// demotion at or above this epoch already happened, which is exactly
    /// the lease-moved-during-build case; the caller must stand the node
    /// back up as a follower.
    ///
    /// Observed rival epochs (recorded while following) cannot refuse a
    /// legitimate completion: metadata mints epochs monotonically, so a
    /// grant issued to this node is strictly above every epoch it ever
    /// watched a rival hold.
    fn complete_promotion(
        &self,
        target: Arc<dyn crate::lease_agent::LeasePublisher>,
        fencing_epoch: u64,
        committed_offset: Option<u64>,
    ) -> bool {
        let mut guard = self
            .target
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self
            .finished_through
            .load(std::sync::atomic::Ordering::SeqCst)
            >= fencing_epoch
            || self
                .recorded_through
                .load(std::sync::atomic::Ordering::SeqCst)
                > fencing_epoch
        {
            return false;
        }
        target.promote(fencing_epoch, committed_offset);
        *guard = Some(target);
        true
    }
}

impl crate::lease_agent::LeasePublisher for CandidateLeasePublisher {
    fn promote(&self, fencing_epoch: u64, committed_offset: Option<u64>) {
        // Under the SAME lock `complete_promotion` holds (review): recorded
        // outside it, a newer grant's watermark could land between an older
        // completion's check and its install, and the stale build would
        // publish anyway. Inside it, the two linearize — the completion
        // either sees this record and refuses, or finishes first and the
        // queued verdict re-promotes the standing leader at this epoch.
        {
            let _guard = self
                .target
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.recorded_through
                .fetch_max(fencing_epoch, std::sync::atomic::Ordering::SeqCst);
        }
        let _ = self.verdicts.send(RoleVerdict::Lead {
            fencing_epoch,
            committed_offset,
        });
    }

    fn demote(&self, fencing_epoch: u64) {
        // The WRITE lock, not a read: demotion must be exclusive with
        // `complete_promotion`, and the ceiling must rise inside the same
        // critical section that forwards — otherwise a completion could
        // slip between the two and serve at an epoch already finished.
        {
            let guard = self
                .target
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.finished_through
                .fetch_max(fencing_epoch, std::sync::atomic::Ordering::SeqCst);
            if let Some(target) = guard.as_ref() {
                target.demote(fencing_epoch);
            }
        }
        let _ = self.verdicts.send(RoleVerdict::Follow);
    }

    fn suspend(&self, fencing_epoch: u64) {
        if let Some(target) = self
            .target
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
        {
            target.suspend(fencing_epoch);
        }
        // Suspension is not a role change and does not raise the finished
        // ceiling: the epoch is still this node's live grant and the agent
        // will retry. The broker is already refusing (the forward above);
        // rebuilding as a follower here would turn every transient quorum
        // miss into a full teardown. A suspend that races a completion is
        // benign in either order: landing after suspends the live broker,
        // landing before is healed by the agent's next successful renewal
        // re-promoting the standing leader.
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

    /// A scrape racing a role transition never sees half a role.
    ///
    /// The switching view answers every authorization question under one lock
    /// acquisition, so a reading either comes wholly from an installed role or
    /// is `None`. Asked question by question it could straddle a `clear()`, and
    /// the collector would publish this view's empty answers — held epoch 0,
    /// leading false — as though a role had given them (review).
    ///
    /// The race is real but rare, so this hammers it: 5000 transitions against
    /// 5000 readings. A regression that split the reading back into separate
    /// lock acquisitions fails here intermittently rather than never.
    #[test]
    fn a_scrape_racing_a_transition_never_sees_half_a_role() {
        use crate::observe::ReplicaObservation as _;

        struct Leading;
        impl crate::lease_agent::CandidateLocalView for Leading {
            fn local_committed_offset(&self) -> u64 {
                12
            }
            fn epoch_starts(&self) -> Vec<vtop_broker::fencing_epochs::EpochStart> {
                Vec::new()
            }
        }
        impl crate::observe::ReplicaObservation for Leading {
            fn try_local_offsets(&self) -> Option<(u64, u64)> {
                Some((12, 13))
            }
            fn cluster_committed_offset(&self) -> Option<u64> {
                Some(12)
            }
            fn try_meta_fencing_epoch(&self) -> Option<(u64, bool)> {
                Some((5, true))
            }
            fn held_fencing_epoch(&self) -> u64 {
                5
            }
            fn is_leading(&self) -> bool {
                true
            }
        }

        let view = Arc::new(SwitchingLocalView::empty());
        view.install(Arc::new(Leading));
        let churn = {
            let view = Arc::clone(&view);
            std::thread::spawn(move || {
                for _ in 0..5_000 {
                    view.clear();
                    view.install(Arc::new(Leading));
                }
            })
        };
        for _ in 0..5_000 {
            // `None` is the honest mid-transition answer and is allowed. What
            // must never happen is a reading that MIXES the two: an installed
            // role's presence with the empty view's values.
            if let Some(reading) = view.role_reading() {
                assert_eq!(
                    reading.held, 5,
                    "a reading taken from an installed role must carry that \
                     role's epoch, never the empty view's zero"
                );
                assert!(
                    reading.leading,
                    "the installed role leads; only the empty view answers false"
                );
                assert_eq!(reading.meta, Some((5, true)));
            }
        }
        churn.join().unwrap();
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

    /// The candidate publisher records a promotion rather than acting on
    /// it — the leader it would promote does not exist until the supervisor
    /// builds one — while demotion forwards immediately, because
    /// fail-closed has no build step to wait for.
    #[test]
    fn candidate_publisher_records_promotions_and_forwards_demotions() {
        use crate::lease_agent::LeasePublisher;
        #[derive(Default)]
        struct Target {
            demoted: std::sync::Mutex<Vec<u64>>,
            promoted: std::sync::Mutex<Vec<u64>>,
        }
        impl LeasePublisher for Target {
            fn promote(&self, fencing_epoch: u64, _committed_offset: Option<u64>) {
                self.promoted.lock().unwrap().push(fencing_epoch);
            }
            fn demote(&self, fencing_epoch: u64) {
                self.demoted.lock().unwrap().push(fencing_epoch);
            }
            fn suspend(&self, _fencing_epoch: u64) {}
        }
        let (verdict_tx, verdict_rx) = tokio::sync::watch::channel(RoleVerdict::Undecided);
        let publisher = CandidateLeasePublisher {
            target: std::sync::RwLock::new(None),
            verdicts: verdict_tx,
            finished_through: std::sync::atomic::AtomicU64::new(0),
            recorded_through: std::sync::atomic::AtomicU64::new(0),
        };
        let target = std::sync::Arc::new(Target::default());
        publisher.set_target(Some(
            std::sync::Arc::clone(&target) as std::sync::Arc<dyn LeasePublisher>
        ));

        publisher.promote(7, Some(41));
        assert_eq!(
            *verdict_rx.borrow(),
            RoleVerdict::Lead {
                fencing_epoch: 7,
                committed_offset: Some(41)
            },
            "a promotion is recorded for the supervisor to complete"
        );
        assert!(
            target.promoted.lock().unwrap().is_empty(),
            "a promotion must NOT reach the role object from the publisher: the broker it \
             authorizes does not exist yet"
        );

        publisher.demote(7);
        assert_eq!(
            *target.demoted.lock().unwrap(),
            vec![7],
            "a demotion forwards immediately — fail-closed cannot wait for the supervisor"
        );
        assert_eq!(*verdict_rx.borrow(), RoleVerdict::Follow);

        // The lease-moved-during-build race, replayed in miniature: the
        // demotion at epoch 7 already happened, so completing the recorded
        // promotion at 7 must publish NOTHING — the broker built for that
        // grant would serve at an epoch metadata moved past.
        let stale = std::sync::Arc::new(Target::default());
        assert!(
            !publisher.complete_promotion(
                std::sync::Arc::clone(&stale) as std::sync::Arc<dyn LeasePublisher>,
                7,
                Some(41)
            ),
            "a completion at or below the demoted ceiling must be refused"
        );
        assert!(
            stale.promoted.lock().unwrap().is_empty(),
            "a refused completion publishes nothing"
        );

        // A fresh grant sits strictly above every finished epoch, so its
        // completion publishes the boundary and becomes the demote target.
        let fresh = std::sync::Arc::new(Target::default());
        assert!(
            publisher.complete_promotion(
                std::sync::Arc::clone(&fresh) as std::sync::Arc<dyn LeasePublisher>,
                8,
                Some(41)
            ),
            "a grant above the ceiling completes"
        );
        assert_eq!(
            *fresh.promoted.lock().unwrap(),
            vec![8],
            "completion publishes through the real publisher"
        );
        publisher.demote(8);
        assert_eq!(
            *fresh.demoted.lock().unwrap(),
            vec![8],
            "a demotion after completion reaches the broker it authorized"
        );

        // Suspend-then-regrant never raises the finished ceiling, so the
        // recorded watermark must refuse a build for a superseded grant:
        // epoch 10 was recorded while the build for 9 was still in flight.
        publisher.promote(9, Some(41));
        publisher.promote(10, Some(41));
        let superseded = std::sync::Arc::new(Target::default());
        assert!(
            !publisher.complete_promotion(
                std::sync::Arc::clone(&superseded) as std::sync::Arc<dyn LeasePublisher>,
                9,
                Some(41)
            ),
            "a completion below the recorded watermark is superseded and must be refused"
        );
        assert!(
            superseded.promoted.lock().unwrap().is_empty(),
            "a superseded completion publishes nothing"
        );
        let latest = std::sync::Arc::new(Target::default());
        assert!(
            publisher.complete_promotion(
                std::sync::Arc::clone(&latest) as std::sync::Arc<dyn LeasePublisher>,
                10,
                Some(41)
            ),
            "the latest recorded grant completes"
        );
    }

    /// The stand-aside marker outlives the process, and is consumed by the
    /// first restart that reads it (#367).
    ///
    /// Both halves matter and they pull against each other. It must SURVIVE,
    /// because the in-process hold-off dies with the process and a restarted
    /// candidate would otherwise campaign immediately, win because nothing
    /// marks it as the replica that just failed, and fail the same way. It
    /// must be CONSUMED, because a marker that persisted would leave a
    /// single-candidate range permanently unled — and the failures it guards
    /// against are usually transient.
    #[test]
    fn a_stand_aside_marker_survives_the_process_and_only_the_first_reader() {
        let dir = tempfile::tempdir().unwrap();
        let marker = StandAsideMarker::new(dir.path());

        assert!(
            matches!(marker.take(), StandAside::NeverFailed),
            "a node that never failed must not stand aside"
        );

        marker.record(7);
        // A DIFFERENT handle over the same directory: this is the restarted
        // process, which shares nothing with the one that wrote it except the
        // data dir.
        let after_restart = StandAsideMarker::new(dir.path());
        assert!(
            matches!(after_restart.take(), StandAside::Failed(7)),
            "the epoch this node could not serve must survive the process that \
             could not serve it"
        );
        assert!(
            matches!(
                StandAsideMarker::new(dir.path()).take(),
                StandAside::NeverFailed
            ),
            "and exactly one restart may claim it: a marker that outlived its own \
             reading would stand this node aside on every restart forever, which \
             leaves a single-candidate range with no leader at all"
        );
    }

    /// A marker that cannot be read is a marker, not an absence (#367).
    ///
    /// The first version collapsed both into `None` with `.ok()?`, which
    /// meant the one class of fault most likely to have written a marker
    /// badly — a directory that ran out of room mid-write — was also the
    /// class that made the marker disappear. The node would then campaign
    /// immediately for the epoch it had just failed to serve, which is the
    /// loop this file exists to break.
    #[test]
    fn an_unreadable_stand_aside_marker_still_stands_the_node_aside() {
        let dir = tempfile::tempdir().unwrap();
        let marker = StandAsideMarker::new(dir.path());

        // A half-written marker: the bytes are there, the epoch is not.
        std::fs::write(dir.path().join(".stand-aside"), b"").unwrap();
        assert!(
            matches!(marker.take(), StandAside::Unreadable(_)),
            "a marker holding no epoch must sit the node out, not read as a node \
             that never failed"
        );
        assert!(
            matches!(marker.take(), StandAside::NeverFailed),
            "and it must still be consumed, or an unparseable marker sits the node \
             out forever"
        );

        // Unreadable rather than unparseable: a directory where the file
        // should be is the cheapest deterministic read error there is.
        std::fs::create_dir(dir.path().join(".stand-aside")).unwrap();
        assert!(
            matches!(marker.take(), StandAside::Unreadable(_)),
            "an I/O error reading the marker must not be reported as no marker"
        );
    }

    /// A candidate that cannot write its data directory must not campaign
    /// (#367).
    ///
    /// This is the fail-closed half `record` cannot provide: it runs on the
    /// way out and must stay best effort, so the condition that would have
    /// stopped it writing is refused at the only point where refusing costs
    /// nothing — before the node is allowed to want the range.
    #[test]
    fn a_candidate_refuses_to_campaign_from_a_directory_it_cannot_write() {
        let dir = tempfile::tempdir().unwrap();
        refuse_an_unwritable_data_dir(dir.path())
            .expect("a writable directory is the ordinary case and must pass");
        assert!(
            !dir.path().join(".writable-probe").exists(),
            "the probe must not survive itself; a data directory is not a place to \
             leave litter"
        );

        // Unwritable expressed as a missing parent rather than a mode change:
        // the same refusal, and it holds when the suite runs as root.
        let error = refuse_an_unwritable_data_dir(&dir.path().join("no-such-dir"))
            .expect_err("a directory that cannot be written must be refused");
        assert!(
            error.contains("refusing to campaign"),
            "the refusal must say what it is refusing and why, not just fail: {error}"
        );
    }

    /// A deposed leading candidate must remain able to FOLLOW its successor.
    /// The leading demotion dialect fences the epoch this node held, never
    /// the rival's: `clear_lease` records its argument in the shared view's
    /// `released_through`, and recording the rival's epoch would leave the
    /// successor's own grant unactivatable — a healthy replica refusing
    /// every append for the successor's entire epoch.
    #[test]
    fn a_deposed_leader_fences_its_own_epoch_so_the_successors_grant_activates() {
        use crate::lease_agent::LeasePublisher;
        let dir = tempfile::tempdir().unwrap();
        let range = test_range();
        let (set, _) = open_range(dir.path(), Uuid::from_u128(0xD9), &range, test_roll()).unwrap();
        let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
        let meta = MetaFencingEpoch::new_inactive(0);
        let broker = Arc::new(
            LocalBroker::with_meta_fencing_epoch(set, epochs, range, 0, meta.clone()).unwrap(),
        );
        let adapter = LeadingDemoteAdapter {
            broker: Arc::clone(&broker),
            inner: Arc::new(crate::lease_agent::BrokerLeasePublisher::new(Arc::clone(
                &broker,
            ))),
        };
        adapter.promote(1, None);
        assert_eq!(
            meta.try_snapshot(),
            Some((1, true)),
            "the grant at epoch 1 activates the shared view"
        );
        adapter.demote(2);
        assert_eq!(
            meta.try_snapshot(),
            Some((1, false)),
            "the deposed broker is fenced — its own epoch is no longer live"
        );
        meta.set(2);
        assert_eq!(
            meta.try_snapshot(),
            Some((2, true)),
            "the successor's grant must still activate the shared view: fencing the \
             rival's epoch instead of our own would pin this replica out of the range \
             for the successor's entire reign"
        );
    }

    /// The switching handler serves whatever is installed and refuses
    /// mid-transition, so a request racing a role change is refused rather
    /// than served by a half-built role.
    #[test]
    fn switching_handler_refuses_mid_transition() {
        let node = Uuid::from_u128(9);
        let handler = SwitchingReplicaHandler::new(node);
        let range = vtop_protocol::RangeIdentity {
            topic: "t".into(),
            topic_epoch: 1,
            range_id: Uuid::from_u128(1),
            range_generation: 0,
        };
        let (code, message) = handler.status(&range).unwrap_err();
        assert_eq!(code, vtop_protocol::ErrorCode::InvalidRequest);
        assert!(
            message.contains("transition"),
            "the refusal must say WHY, so a peer retries instead of diagnosing: {message}"
        );
        assert_eq!(handler.node_id(), node);
    }
}

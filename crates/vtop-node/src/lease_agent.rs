//! The loop that makes range leadership live (#223).
//!
//! The metadata plane already knew how to grant, expire, and renew a range
//! lease, and the broker already refused any request whose fencing epoch did
//! not match its own. What was missing was the thing in between: a process that
//! actually asks for the lease, keeps it, notices when it has lost it, and
//! tells its broker. Until this existed, `MetaFencingEpoch` was a value nobody
//! ever published to — which is why #224's readiness probe shipped with a
//! caveat saying its fenced branch could not fire.
//!
//! # What the agent guarantees, and what it does not
//!
//! It guarantees that this process's broker only believes it holds epoch `E`
//! while metadata says so. It does **not** guarantee that exactly one process
//! is trying at a time — that is Raft's job, and acquisition is a single
//! linearizable proposal, so exactly one candidate can win a given round.
//!
//! Safety never rests on the timers here. Acquisition always mints
//! `fencing_epoch + 1`, so a slow, skewed, or paused agent that acquires late
//! still fences whoever held the range before it. The intervals below are a
//! liveness tuning knob: too long and failover is slow, too short and the
//! metadata group carries needless proposals. Neither setting can produce two
//! brokers that both believe they may write.
//!
//! # Why renewal failure is not retried in place
//!
//! When a renewal is refused the agent does not try again with a fresh epoch —
//! it publishes the loss to the broker first and only then re-enters the
//! acquisition path. A broker that keeps serving during that window is a broker
//! serving under an epoch metadata has already given away.
//!
//! # The local deadline
//!
//! An unreachable metadata plane is not proof the lease is gone — but past the
//! deadline of the last lease metadata actually confirmed, it is no longer
//! proof of anything, and a rival may already have been granted the range. The
//! agent therefore tracks that deadline locally and demotes the broker once it
//! passes without a successful renewal, instead of relying on a future read to
//! deliver the news. The tracked value is always at or before the deadline
//! metadata recorded (it is taken from the read view, or stamped before a
//! proposal is sent), so the local demotion always precedes the earliest
//! instant a rival could legally acquire. Administrative grants have no
//! deadline and are exempt: an operator's pin outlives any partition.

use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;
use vtop_broker::LocalBroker;
use vtop_meta::command::CommandEnvelope;
use vtop_meta::{AdminClient, MetadataCommand, MetadataResponse};

/// Where the agent publishes what metadata decided.
///
/// A trait rather than a direct `LocalBroker` handle so the transitions that
/// matter — promotion, and the demotion a partitioned node discovers — can be
/// asserted without standing up a broker, a segment, and a disk.
pub trait LeasePublisher: Send + Sync {
    /// This node now holds the range at `fencing_epoch`, with `committed_offset`
    /// the boundary a quorum proved (`None` for a standalone range, which has
    /// no quorum to prove anything).
    fn promote(&self, fencing_epoch: u64, committed_offset: Option<u64>);
    /// This node no longer holds the range at `fencing_epoch` — metadata has
    /// moved on (a rival grant, a refused renewal, a released lease). The
    /// epoch is finished for this process.
    fn demote(&self, fencing_epoch: u64);
    /// This node must stop serving at `fencing_epoch` NOW, but the epoch is
    /// still its own live grant and a retry may reactivate it — a promotion
    /// whose quorum probe transiently failed. Distinct from [`Self::demote`]:
    /// demotion records the epoch as finished, and a later promotion at the
    /// same epoch would be a permanent no-op.
    fn suspend(&self, fencing_epoch: u64);
}

/// Asks the replica set where its disks actually are.
///
/// Behind a trait because the real implementation makes N concurrent RPCs, and
/// the agent's decisions must be assertable without a network.
#[async_trait::async_trait]
pub trait QuorumProbe: Send + Sync {
    /// Fence every configured replica at `fencing_epoch` and report what each
    /// held at that instant, including the leader's own view.
    ///
    /// Takes the epoch because probing without fencing reads a moving target:
    /// the deposed leader may still be appending to a follower while the new
    /// leader measures it. A replica that could not be fenced must report
    /// `None` — absent from the quorum — rather than its last known offset.
    async fn probe(&self, fencing_epoch: u64) -> Vec<crate::promotion::ReplicaProbe>;
    /// The CONFIGURED replica-set size, not the number that answered.
    fn replication_factor(&self) -> usize;
}

/// Probes over the replication plane, one RPC per follower.
///
/// Deliberately NOT `NetworkedReplicaSet::follower_durable_offset`. That
/// accessor reads a counter this leader's own replication stream advances, and
/// it returns `None` only when a node id is absent from the configured set — a
/// config mismatch, never an unreachable peer. On a freshly promoted leader
/// that stream has never run, so every follower would report `Some(0)`: a
/// disconnected replica would be counted as holding nothing, the quorum floor
/// would collapse to zero, and the refusal path could never fire. It would make
/// verified promotion a no-op precisely on the failover it exists for.
///
/// `ReplicaStatusClient` asks the follower's disk instead, and a peer that does
/// not answer is genuinely absent.
/// A candidate's own local view, as the promotion probe reads it (#284):
/// the committed offset it votes with, and the epoch lineage it sends with
/// every fence. A leader answers from its broker and a follower from its
/// replica state; a candidate answers through whichever it currently is —
/// which is exactly why this is a trait and not `Arc<LocalBroker>`.
pub trait CandidateLocalView: Send + Sync {
    fn local_committed_offset(&self) -> u64;
    fn epoch_starts(&self) -> Vec<vtop_broker::fencing_epochs::EpochStart>;
}

impl CandidateLocalView for LocalBroker {
    fn local_committed_offset(&self) -> u64 {
        // The BLOCKING accessor, deliberately — see the probe body.
        self.local_offsets().0
    }

    fn epoch_starts(&self) -> Vec<vtop_broker::fencing_epochs::EpochStart> {
        self.epoch_starts()
    }
}

impl CandidateLocalView for vtop_broker::replication::InProcessFollower {
    fn local_committed_offset(&self) -> u64 {
        self.local_committed_offset()
    }

    fn epoch_starts(&self) -> Vec<vtop_broker::fencing_epochs::EpochStart> {
        self.epoch_starts()
    }
}

pub struct ReplicaPlaneProbe {
    view: Arc<dyn CandidateLocalView>,
    node_uuid: Uuid,
    client: vtop_broker::replication::ReplicaStatusClient,
    followers: Vec<FollowerEndpoint>,
    range: vtop_protocol::RangeIdentity,
    /// The most recent address each named follower actually resolved to.
    ///
    /// The fallback when a lookup fails, and it must not be the address this
    /// process started with (#367): a follower that moved and was found stays
    /// found, so the next resolver hiccup does not send the probe back to
    /// where the follower used to be and report a healthy replica absent.
    last_known: std::sync::Mutex<std::collections::HashMap<Uuid, std::net::SocketAddr>>,
}

/// How long a probe will wait for one follower's name.
///
/// Well inside the fence deadline it precedes, because a probe that waited out
/// a stalled resolver would spend its round budget before asking a single
/// replica anything.
const PROBE_RESOLVE_TIMEOUT: Duration = Duration::from_millis(500);

/// One follower, as the leader dials it.
#[derive(Clone, Debug)]
pub struct FollowerEndpoint {
    pub node_uuid: Uuid,
    /// Where the follower was, last time its name was looked up.
    pub addr: std::net::SocketAddr,
    /// The `host:port` it was configured under, when it was a name (#367).
    ///
    /// A probe is what decides whether a candidate may promote, so a stale
    /// address here does not merely lose a connection — it withholds a vote,
    /// and a range with one absent replica out of three cannot establish its
    /// boundary at all. Resolved per probe rather than cached: a probe runs
    /// once per grant, not per heartbeat, so the lookup is free at this rate
    /// and the answer is never older than the question.
    ///
    /// `None` for a follower given as a literal address.
    pub host: Option<String>,
    pub server_name: String,
}

impl ReplicaPlaneProbe {
    pub fn new(
        view: Arc<dyn CandidateLocalView>,
        node_uuid: Uuid,
        client: vtop_broker::replication::ReplicaStatusClient,
        followers: Vec<FollowerEndpoint>,
        range: vtop_protocol::RangeIdentity,
    ) -> Self {
        Self {
            view,
            node_uuid,
            client,
            followers,
            range,
            last_known: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl QuorumProbe for ReplicaPlaneProbe {
    async fn probe(&self, fencing_epoch: u64) -> Vec<crate::promotion::ReplicaProbe> {
        // The leader's own disk, read with the BLOCKING accessor. Promotion is
        // a request handler, not a scrape: it is allowed to queue behind an
        // append. The non-blocking variant would have the leader abstain from
        // its own quorum under momentary lock contention, which in a 2-replica
        // range turns a lock hold into a refused promotion.
        //
        // No fence RPC to itself. This process holds the epoch it is promoting
        // at — that is what made it the candidate — so nothing else can be
        // writing here under an older one. Dialling its own port to be told so
        // would add a failure mode without adding a fact.
        let local_committed = self.view.local_committed_offset();
        let mut probes = vec![crate::promotion::ReplicaProbe {
            node_id: self.node_uuid,
            local_committed_offset: Some(local_committed),
        }];
        // This candidate's own lineage, sent with every fence so each replica
        // can reconcile against it while it is stopped. Empty when this node
        // cannot vouch for its own history, in which case no replica truncates
        // anything — the honest outcome, since there is nothing to reconcile
        // against.
        let leader_epoch_starts: Vec<vtop_protocol::ReplicaEpochStart> = self
            .view
            .epoch_starts()
            .into_iter()
            .map(|entry| vtop_protocol::ReplicaEpochStart {
                epoch: entry.epoch,
                start_offset: entry.start_offset,
            })
            .collect();
        let leader_epoch_starts = &leader_epoch_starts;
        // Concurrent: a follower that has stopped answering must not add its
        // full deadline to every other follower's wait.
        let answers = futures::future::join_all(self.followers.iter().map(|follower| async move {
            // The name, if we have one, outranks the address we last resolved
            // it to (#367). Inside the concurrent block so one follower whose
            // name is slow to answer does not delay the others.
            // THE FALLBACK IS THE LAST ADDRESS THAT WORKED, not the one this
            // process started with (review). A follower that moved and was
            // found is found again on the next probe; falling back to the
            // startup address would send the next transient resolver failure
            // straight back to where the follower used to be, report a healthy
            // replica absent, and lose the promotion quorum on it.
            //
            // Bounded by the same deadline the fence itself gets: a probe that
            // waited out a stalled resolver would spend the round budget
            // before asking a single replica anything.
            let addr = match &follower.host {
                Some(host) => {
                    match vtop_broker::replication::address_now(host, PROBE_RESOLVE_TIMEOUT).await {
                        // REMEMBERED ON RESOLUTION, and this reverses an
                        // earlier revision that waited for a fence to prove it
                        // (review, twice, in both directions — so here is the
                        // argument that settles it).
                        //
                        // The two candidates for the fallback are "the last
                        // address that answered a fence" and "the last address
                        // the NAME gave us". They differ exactly when a
                        // follower has moved and is not serving yet, and there
                        // the second is right: the name is authoritative about
                        // WHERE a peer is, while a fence only reports whether
                        // it is READY. Preferring the proven address means
                        // preferring one that is definitively dead — a
                        // replaced pod's old address never comes back — over
                        // one that is merely early, and a peer that is early
                        // becomes correct on its own.
                        //
                        // The startup address survives only as the last
                        // resort, for a peer no lookup has ever answered for.
                        Some(addr) => {
                            self.last_known
                                .lock()
                                .expect("resolved peers")
                                .insert(follower.node_uuid, addr);
                            addr
                        }
                        None => self
                            .last_known
                            .lock()
                            .expect("resolved peers")
                            .get(&follower.node_uuid)
                            .copied()
                            .unwrap_or(follower.addr),
                    }
                }
                None => follower.addr,
            };
            let fenced = self
                .client
                .fence(
                    addr,
                    &follower.server_name,
                    follower.node_uuid,
                    &self.range,
                    fencing_epoch,
                    leader_epoch_starts,
                )
                .await;
            crate::promotion::ReplicaProbe {
                node_id: follower.node_uuid,
                local_committed_offset: match fenced {
                    Ok(response) => {
                        if response.truncated_records > 0 {
                            tracing::warn!(
                                follower = %follower.node_uuid,
                                fencing_epoch,
                                records = response.truncated_records,
                                "replica discarded records written under a leadership this \
                                 candidate does not share"
                            );
                        }
                        Some(response.local_committed_offset)
                    }
                    Err(error) => {
                        // ABSENT, not "last known offset". A replica that could
                        // not be fenced may still be taking the deposed
                        // leader's appends, so its offset is a measurement of
                        // something still moving. Counting it is exactly the
                        // bug fencing exists to close, and a replica that has
                        // simply not yet seen the grant refuses here too —
                        // correctly, since it is not fenced until it has.
                        tracing::debug!(
                            follower = %follower.node_uuid,
                            fencing_epoch,
                            %error,
                            "replica could not be fenced; excluded from the promotion quorum"
                        );
                        None
                    }
                },
            }
        }))
        .await;
        probes.extend(answers);
        probes
    }

    fn replication_factor(&self) -> usize {
        // The leader plus its configured followers.
        self.followers.len() + 1
    }
}

/// Publishes into a live broker.
///
/// On a replicated v2 range the probed boundary is NOT published here (#240,
/// Raft §5.4.2): a boundary established by counting other replicas' pasts is
/// a CANDIDATE value, and the only thing that may raise the published
/// high-water mark is an entry of this epoch that a quorum holds. So
/// `promote` adopts the epoch and stashes the candidate; the driver
/// ([`Self::drive_boundary_marker`]) appends the epoch's marker, proves it on
/// a quorum, and publishes the marker's END — which covers the candidate,
/// because the election restriction (§5.4.1, #342) guarantees the winner
/// holds everything the boundary names. Until then the watermark stays at
/// the durable floor the broker opened with (#402), so consumers see
/// nothing this epoch has not proven and a produce that lands first commits
/// the marker prefix implicitly, which is Raft's own rule.
///
/// The consequence is deliberate: on a range where a majority cannot hold
/// the new leader's tail, promotion establishes, appends, and then refuses
/// to publish until repair closes the gap. Failover on such a range is
/// slower to serve; the alternative was publishing a boundary nothing
/// proved. A v1-format range keeps the pre-marker publication path — its
/// frame cannot carry a marker consumers could be shielded from — and a
/// standalone range has no quorum to convince and no other past to trust,
/// so neither is gated.
pub struct BrokerLeasePublisher {
    broker: Arc<LocalBroker>,
    /// The epoch whose boundary marker has yet to be quorum-acked, if any.
    /// Cleared by publication, or by a demotion or suspension — a fenced
    /// leader's marker refuses anyway, and the driver must not spin on it.
    pending_marker: std::sync::Mutex<Option<u64>>,
    /// Woken on every change to `pending_marker`, so the driver waits
    /// rather than polls.
    marker_changed: tokio::sync::Notify,
}

impl BrokerLeasePublisher {
    pub fn new(broker: Arc<LocalBroker>) -> Self {
        Self {
            broker,
            pending_marker: std::sync::Mutex::new(None),
            marker_changed: tokio::sync::Notify::new(),
        }
    }

    /// Whether promotion on this broker publishes through the marker: only a
    /// replicated v2 range can append one a quorum proves and consumers
    /// never see. Public so a role that knows the answer is "never" can
    /// skip spawning a driver that would only ever park.
    pub fn gates_on_marker(&self) -> bool {
        self.broker.cluster_committed().is_some()
            && self.broker.segment_format() == vtop_broker::SegmentFormat::V2
    }

    /// The epoch whose boundary is established but not yet published.
    pub fn pending_marker_epoch(&self) -> Option<u64> {
        *self
            .pending_marker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Pend `epoch`'s marker unless a newer epoch is already pending
    /// (review): promotion rounds can arrive reordered, and a stale one
    /// must not replace the marker the current epoch still owes.
    fn pend_marker(&self, epoch: u64) {
        let mut pending = self
            .pending_marker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending.is_some_and(|current| current > epoch) {
            return;
        }
        *pending = Some(epoch);
        // The gate is raised UNDER the same lock that pends the marker, so
        // no fetch can slip between the promotion and the refusal.
        self.broker.set_boundary_pending(true);
        drop(pending);
        self.marker_changed.notify_one();
    }

    /// Retire the pending marker only if it is `epoch`'s (review): a delayed
    /// demotion or suspension naming an older epoch says nothing about the
    /// newer epoch's marker, whose lease the broker still holds.
    fn retire_marker(&self, epoch: u64) {
        let mut pending = self
            .pending_marker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *pending == Some(epoch) {
            *pending = None;
            self.broker.set_boundary_pending(false);
            drop(pending);
            self.marker_changed.notify_one();
        }
    }

    /// One attempt at the pending marker: `Ok(Some(mark))` published it,
    /// `Ok(None)` found nothing pending, `Err` names why it stays pending.
    /// Synchronous and blocking (an fsync plus a replication round trip), so
    /// the driver runs it on the blocking pool.
    pub fn try_publish_pending_boundary(&self) -> Result<Option<u64>, String> {
        let Some(epoch) = self.pending_marker_epoch() else {
            return Ok(None);
        };
        match self.broker.publish_boundary_marker(epoch) {
            Ok(mark) => {
                // Cleared only if this is still the epoch that was pending: a
                // re-grant between the attempt and here is a newer marker to
                // publish, not a job done.
                let mut pending = self
                    .pending_marker
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if *pending == Some(epoch) {
                    *pending = None;
                    // Proven: the mark is published, and fetch may serve it.
                    self.broker.set_boundary_pending(false);
                }
                Ok(Some(mark))
            }
            Err(error) => Err(error.to_string()),
        }
    }

    /// Publish every pending boundary marker until told to stop: retried
    /// with a bounded backoff while a quorum cannot hold the marker, woken
    /// the moment a new promotion or a demotion changes what is pending.
    /// The retry is the whole point — the refusal is the #340 wall, and the
    /// boundary must publish the instant repair closes the gap, without a
    /// restart and without anybody re-promoting.
    ///
    /// A closed shutdown watch is not a shutdown: the arm stops being polled
    /// and the driver keeps serving whoever still owns it, the same rule the
    /// supervisor's adapters follow.
    pub async fn drive_boundary_marker(
        self: Arc<Self>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut attempts: u32 = 0;
        let mut watch_closed = false;
        loop {
            if *shutdown.borrow() {
                return;
            }
            let Some(epoch) = self.pending_marker_epoch() else {
                attempts = 0;
                tokio::select! {
                    _ = self.marker_changed.notified() => {}
                    changed = shutdown.changed(), if !watch_closed => {
                        if changed.is_err() {
                            watch_closed = true;
                        } else if *shutdown.borrow() {
                            return;
                        }
                    }
                }
                continue;
            };
            let publisher = Arc::clone(&self);
            let outcome =
                tokio::task::spawn_blocking(move || publisher.try_publish_pending_boundary()).await;
            match outcome {
                Ok(Ok(Some(committed))) => {
                    attempts = 0;
                    tracing::info!(
                        range = %self.broker.range().range_id,
                        fencing_epoch = epoch,
                        committed_high_watermark = committed,
                        "boundary marker quorum-acked; the high-water mark is published"
                    );
                    // The harness-readable line, beside the structured one:
                    // scenarios assert this exact event.
                    println!(
                        "boundary_marker_published epoch={epoch} committed={committed} range={}",
                        self.broker.range().range_id
                    );
                    use std::io::Write;
                    std::io::stdout().flush().ok();
                }
                Ok(Ok(None)) => {}
                Ok(Err(reason)) => {
                    attempts = attempts.saturating_add(1);
                    // The first refusal and every twentieth after it: the
                    // condition is expected to persist for as long as repair
                    // takes, and a log line per attempt would bury the
                    // reason under its own repetitions.
                    if attempts == 1 || attempts.is_multiple_of(20) {
                        tracing::warn!(
                            range = %self.broker.range().range_id,
                            fencing_epoch = epoch,
                            attempts,
                            %reason,
                            "boundary marker not yet published; the high-water mark stays \
                             at the durable floor until a majority holds this epoch's tail"
                        );
                    }
                    let backoff = std::time::Duration::from_millis(
                        250_u64.saturating_mul(1_u64 << attempts.min(3)),
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = self.marker_changed.notified() => {}
                        changed = shutdown.changed(), if !watch_closed => {
                            if changed.is_err() {
                                watch_closed = true;
                            } else if *shutdown.borrow() {
                                return;
                            }
                        }
                    }
                }
                Err(join) => {
                    tracing::error!(%join, "boundary marker attempt panicked; retrying");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
}

impl LeasePublisher for BrokerLeasePublisher {
    fn promote(&self, fencing_epoch: u64, committed_offset: Option<u64>) {
        let gated = self.gates_on_marker();
        if !gated {
            // The pre-marker publication path, kept ONLY where no marker can
            // exist: a v1 range cannot shield consumers from one, and a
            // standalone range (no cluster cell) has nothing to prove and
            // nothing to publish here either.
            if let (Some(offset), Some(cluster)) =
                (committed_offset, self.broker.cluster_committed())
            {
                cluster.advance_to(offset);
            }
        }
        // Both values must end up equal or the broker refuses every request.
        // The order is not a safety question — produce checks equality, so any
        // window between the two writes fails closed — but both must happen.
        self.broker.adopt_fencing_epoch(fencing_epoch);
        self.broker.meta_fencing_epoch().set(fencing_epoch);
        if gated {
            // AFTER the epoch is adopted, so the driver's first attempt finds
            // a broker that holds the epoch rather than one that refuses it
            // for a reason that will clear a microsecond later. And only if
            // the adoption stands (review): adoption is monotonic, so a
            // stale round names an epoch the broker has moved past, and the
            // marker it would pend is one the newer epoch's would supersede
            // anyway — pending it would only displace the marker still owed.
            if self.broker.held_fencing_epoch() != fencing_epoch {
                tracing::debug!(
                    range = %self.broker.range().range_id,
                    fencing_epoch,
                    held = self.broker.held_fencing_epoch(),
                    "stale promotion round; the held epoch's marker stays pending"
                );
                return;
            }
            tracing::info!(
                range = %self.broker.range().range_id,
                fencing_epoch,
                candidate_boundary = ?committed_offset,
                "promotion established a candidate boundary; publishing through this \
                 epoch's marker rather than by trusting the count (#240, §5.4.2)"
            );
            self.pend_marker(fencing_epoch);
        }
    }

    fn demote(&self, fencing_epoch: u64) {
        // Only the metadata view is cleared. The held epoch stays where it is:
        // it records what this process was last granted, and rewinding it
        // would let a later stale grant look current.
        self.retire_marker(fencing_epoch);
        self.broker.meta_fencing_epoch().clear_lease(fencing_epoch);
    }

    fn suspend(&self, fencing_epoch: u64) {
        // NOT `clear_lease`: that records the epoch in `released_through`,
        // after which a successful re-promotion at the same epoch could never
        // reactivate the view — the broker would stay fenced under its own
        // live lease until an external epoch change.
        // The pending marker goes too: a suspended lease refuses the marker,
        // and the re-promotion that lifts the suspension pends it again — a
        // republish at the same epoch rides the duplicate path.
        self.retire_marker(fencing_epoch);
        self.broker.meta_fencing_epoch().suspend(fencing_epoch);
    }
}

/// The decision to keep holding an epoch through a quorum miss, and the
/// budget state that follows it (#375).
///
/// A pure function on purpose. The policy is the part worth testing — that the
/// hold is bounded, that the bound belongs to an epoch rather than to the
/// process, and that it is measured in the unit it is described in — and the
/// surrounding method needs a live admin client, which is the same reason
/// [`Promoter`] is split out from [`LeaseAgent`].
struct QuorumMissBudget {
    /// Whether to renew, holding this epoch still for another round.
    hold: bool,
    /// The state to carry into the next round.
    state: Option<QuorumMissHold>,
}

/// How much of an epoch's hold is left.
///
/// LATCHED, not recomputed from a timestamp every round (review). Comparing
/// wall clock against a stored deadline means a clock that steps backwards —
/// NTP correction, a VM restored from a snapshot — reopens a window that had
/// already closed, and the hold restarts. Once an epoch's window is spent it
/// stays spent, because "we already decided this" is a fact and not a
/// measurement.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum QuorumMissHold {
    /// Holding `epoch` until `until_ms`.
    Until { epoch: u64, until_ms: i64 },
    /// `epoch`'s window is closed and does not reopen.
    Spent { epoch: u64 },
}

impl QuorumMissHold {
    fn epoch(self) -> u64 {
        match self {
            Self::Until { epoch, .. } | Self::Spent { epoch } => epoch,
        }
    }
}

/// WALL CLOCK, NOT ROUND COUNT (review). The bound is described as two lease
/// lifetimes and was counted in poll rounds, which are not the same thing: a
/// probe against blackholed replicas spends the fence deadline before the poll
/// interval is applied, so N rounds can be several times N poll intervals.
///
/// `hold_for` is ONE lease lifetime, not two, and the difference is the
/// promise (review). The last renewal inside the window extends the metadata
/// lease by a further full lifetime, so a window of two would hold the epoch
/// for nearly three. One in, one trailing, two total — which is what a
/// survivor is waiting on.
///
/// A range that recovered and later missed again gets a FRESH window rather
/// than inheriting a spent one, which is why the epoch travels with the state:
/// the two situations are indistinguishable from the deadline alone.
fn quorum_miss_budget(
    fencing_epoch: u64,
    now_ms: i64,
    carried: Option<QuorumMissHold>,
    hold_for: Duration,
) -> QuorumMissBudget {
    let carried = carried.filter(|held| held.epoch() == fencing_epoch);
    match carried {
        // Already decided, and not revisited: see `QuorumMissHold`.
        Some(QuorumMissHold::Spent { epoch }) => QuorumMissBudget {
            hold: false,
            state: Some(QuorumMissHold::Spent { epoch }),
        },
        Some(QuorumMissHold::Until { epoch, until_ms }) if now_ms < until_ms => QuorumMissBudget {
            hold: true,
            state: Some(QuorumMissHold::Until { epoch, until_ms }),
        },
        // The window just closed. Latch it.
        Some(QuorumMissHold::Until { epoch, .. }) => QuorumMissBudget {
            hold: false,
            state: Some(QuorumMissHold::Spent { epoch }),
        },
        // First miss on this epoch, or a different epoch entirely.
        None => QuorumMissBudget {
            hold: true,
            state: Some(QuorumMissHold::Until {
                epoch: fencing_epoch,
                until_ms: now_ms
                    .saturating_add_unsigned(hold_for.as_millis().min(i64::MAX as u128) as u64),
            }),
        },
    }
}

/// What a promotion attempt decided, as distinct from whether it succeeded.
///
/// `publish_held` returned a bare bool and every `false` was treated alike:
/// stop renewing, let the lease lapse. For an eligibility refusal that is the
/// point — the lapse is how the range reaches the replica the refusal named.
/// For a quorum MISS it is backwards, and #375 is what it costs.
///
/// A replica refuses a fence precisely when the epoch is above the grant it
/// has observed, and lapsing is how the epoch goes UP: acquisition mints
/// `fencing_epoch + 1` where renewal mints nothing. So the answer to "my
/// followers are behind me" was to get further ahead of them, every lease
/// lifetime, without bound.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Promoted {
    /// Servable at this epoch.
    Yes,
    /// Refused on an eligibility verdict: a peer is the better holder, and
    /// letting the lease lapse is the mechanism that hands it over.
    Refused,
    /// Refused because too few replicas answered. No rival can serve this
    /// range either — the missing answers are the ones a rival would need —
    /// so the epoch is worth holding still while they catch up.
    QuorumMissed,
}

/// Owns everything about believing this node leads: verifying the boundary,
/// publishing the epoch, and giving it up.
///
/// Separate from [`LeaseAgent`] so the transitions can be tested without an
/// admin client — the interesting behaviour here is not "can we reach
/// metadata", it is what happens when a quorum will not answer.
struct Promoter {
    /// Set by the quorum-miss arm; read and cleared by the caller (#375).
    quorum_miss: bool,
    publisher: Arc<dyn LeasePublisher>,
    probe: Option<Arc<dyn QuorumProbe>>,
    /// The epoch whose boundary has already been verified.
    ///
    /// Verification happens once per epoch TRANSITION, not once per renewal.
    /// Re-probing a quorum every few seconds for a leader that has not changed
    /// is pure cost.
    verified_epoch: Option<u64>,
    /// This node, so `establish` can check the candidate's own probe covers
    /// the boundary it is about to publish.
    node_uuid: Uuid,
    range_uuid: Uuid,
    /// Set when a refusal was an ELIGIBILITY verdict — the quorum answered
    /// and this node's log is the problem — rather than a transient quorum
    /// miss. The agent turns it into a campaign hold-off: a refused
    /// candidate that keeps winning the acquisition race starves the very
    /// replica its own refusal named, because a suspended non-leader
    /// receives no replication with which to become eligible.
    stand_aside: bool,
}

impl Promoter {
    /// Make this node servable at `fencing_epoch`, verifying the committed
    /// boundary first if this epoch has not been verified yet.
    async fn ensure(&mut self, fencing_epoch: u64) -> bool {
        if self.verified_epoch == Some(fencing_epoch) {
            return true;
        }
        let committed_offset = match self.probe.as_ref() {
            // A standalone range has no quorum to establish against and
            // promotes on its own durable boundary; requiring one it cannot
            // form would make single-replica deployments unleadable.
            None => None,
            Some(probe) => {
                let probes = probe.probe(fencing_epoch).await;
                match crate::promotion::establish(
                    &probes,
                    probe.replication_factor(),
                    self.node_uuid,
                ) {
                    crate::promotion::Promotion::Established {
                        committed_offset,
                        answered,
                    } => {
                        tracing::info!(
                            range = %self.range_uuid,
                            fencing_epoch,
                            committed_offset,
                            replicas = answered.len(),
                            "verified promotion: committed boundary established by quorum"
                        );
                        Some(committed_offset)
                    }
                    crate::promotion::Promotion::QuorumUnavailable { answered, required } => {
                        tracing::warn!(
                            range = %self.range_uuid,
                            fencing_epoch,
                            answered,
                            required,
                            "refusing promotion: too few replicas could confirm the \
                             committed boundary"
                        );
                        // Publish the refusal, not merely a local state flip.
                        // Flipping state alone would leave the broker's
                        // metadata view live while the agent stops renewing:
                        // the lease lapses, a rival takes it, and the `Wait`
                        // branch that normally demotes is guarded on `Held` —
                        // so nothing would ever clear it and this node would
                        // keep passing /readyz as a deposed leader.
                        //
                        // SUSPEND, though, not demote: a quorum miss is
                        // retryable, and demotion marks the epoch released —
                        // making the successful re-probe a promotion the view
                        // permanently refuses, wedging the range under its own
                        // live lease.
                        self.quorum_miss = true;
                        self.suspended(fencing_epoch);
                        return false;
                    }
                    crate::promotion::Promotion::LeaderBehind {
                        committed_offset,
                        leader_committed_offset,
                    } => {
                        tracing::warn!(
                            range = %self.range_uuid,
                            fencing_epoch,
                            committed_offset,
                            leader_committed_offset,
                            "refusing promotion: this node's log does not reach the \
                             quorum-proven boundary; letting the lease lapse so a \
                             caught-up replica can win the range"
                        );
                        // Also retryable in principle: probes are a snapshot,
                        // and a stale follower answer can transiently place
                        // the boundary above this node's own disk.
                        self.stand_aside = true;
                        self.suspended(fencing_epoch);
                        return false;
                    }
                    crate::promotion::Promotion::CandidateBehindVoters {
                        candidate_offset,
                        votes,
                        required,
                        most_complete,
                    } => {
                        tracing::warn!(
                            range = %self.range_uuid,
                            fencing_epoch,
                            candidate_offset,
                            votes,
                            required,
                            most_complete_node = %most_complete.0,
                            most_complete_offset = most_complete.1,
                            "refusing promotion: fewer than a majority of the fenced \
                             replicas are at or below this node's offset (Raft §5.4.1); \
                             letting the lease lapse so the more complete replica can \
                             win the range"
                        );
                        // Retryable for the same reason as LeaderBehind: the
                        // right fix is a different candidate, and suspending
                        // leaves the epoch grantable to it.
                        self.stand_aside = true;
                        self.suspended(fencing_epoch);
                        return false;
                    }
                }
            }
        };
        self.publisher.promote(fencing_epoch, committed_offset);
        self.verified_epoch = Some(fencing_epoch);
        tracing::info!(range = %self.range_uuid, fencing_epoch, "range lease held");
        true
    }

    fn lost(&mut self, fencing_epoch: u64) {
        self.publisher.demote(fencing_epoch);
        self.verified_epoch = None;
    }

    /// Stop serving at `fencing_epoch` without finishing the epoch: the lease
    /// is still ours and the refusal that forced this may clear on retry.
    /// `verified_epoch` resets so the next round re-probes rather than
    /// trusting the verification that just failed.
    fn suspended(&mut self, fencing_epoch: u64) {
        self.publisher.suspend(fencing_epoch);
        self.verified_epoch = None;
    }

    /// Whether the last refusal was a quorum miss, as opposed to an
    /// eligibility verdict or no refusal at all.
    ///
    /// The two need opposite responses and the caller could not tell them
    /// apart (#375). An eligibility refusal names a better holder, so lapsing
    /// is the mechanism that hands the range over. A quorum miss names
    /// nobody — the answers a rival would need are the same ones that did not
    /// arrive — and lapsing is what mints the next epoch, putting it out of
    /// reach of the very follower that was catching up to this one.
    fn take_quorum_miss(&mut self) -> bool {
        std::mem::take(&mut self.quorum_miss)
    }

    /// Whether the last refusal was an eligibility verdict; reading clears
    /// it, because one verdict funds one hold-off.
    fn take_stand_aside(&mut self) -> bool {
        std::mem::take(&mut self.stand_aside)
    }
}

/// How the agent paces itself.
#[derive(Clone, Copy, Debug)]
pub struct LeaseAgentConfig {
    /// Lease length requested on every acquire and renew.
    pub lease_duration: Duration,
    /// How often a holder renews. Must be comfortably shorter than
    /// `lease_duration`, or a single lost round trip costs the range.
    pub renew_interval: Duration,
    /// How often a non-holder re-checks whether the lease has lapsed.
    pub poll_interval: Duration,
}

impl Default for LeaseAgentConfig {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(15),
            // A third of the lease: two consecutive renewals can fail — a
            // metadata leader election, say — and the range still does not
            // change hands. At half, one hiccup is a failover.
            renew_interval: Duration::from_secs(5),
            poll_interval: Duration::from_secs(2),
        }
    }
}

impl LeaseAgentConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.lease_duration.is_zero() {
            return Err("lease_duration must be greater than zero".into());
        }
        if self.renew_interval >= self.lease_duration {
            return Err(format!(
                "renew_interval ({:?}) must be shorter than lease_duration ({:?}), or the \
                 lease expires before its holder renews it",
                self.renew_interval, self.lease_duration
            ));
        }
        if self.poll_interval.is_zero() {
            return Err("poll_interval must be greater than zero".into());
        }
        Ok(())
    }
}

/// What the agent believes about this node's hold on the range.
///
/// Deliberately not exposed: the broker's fencing view is the authoritative
/// signal — it is what produce checks and what `/readyz` reports — and a second
/// public source of truth would only invite the two to diverge. This exists so
/// the agent can tell a transition from a repeat and log accordingly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseState {
    /// This node holds the range at the given epoch.
    Held { fencing_epoch: u64 },
    /// Someone else holds it, or nobody does.
    NotHeld,
}

/// One round of the agent's decision, separated from the I/O so it can be
/// tested without a metadata cluster.
///
/// The agent is a state machine over (what metadata says, what we believed).
/// Keeping that decision pure is what makes the interesting transitions —
/// losing a lease to a rival, discovering a stale epoch — testable at all;
/// they are exactly the cases that are hardest to stage against a live cluster.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseDecision {
    /// Metadata agrees we hold it and the lease is still live; renew before
    /// the deadline.
    Renew { fencing_epoch: u64 },
    /// Metadata pinned this node administratively (no deadline). There is
    /// nothing to renew — the state machine refuses renewing a deadline-less
    /// lease — so publish it as held and check back later.
    HeldAdministratively { fencing_epoch: u64 },
    /// Nobody holds a live lease (including our own lapsed one); try to take
    /// it. A lapsed lease cannot be renewed back to life — the only way back
    /// is acquisition, which mints a fresh epoch.
    Acquire { expected_range_generation: u64 },
    /// Someone else holds a live lease at this epoch; wait.
    Wait { fencing_epoch: u64 },
    /// The range does not exist. Waiting is right: a range is created by an
    /// operator, and spinning on acquisition would bury that in noise.
    RangeMissing,
}

/// Decide what to do from a lease view and this node's identity.
///
/// `now_ms` is the caller's clock and is used only to judge liveness of
/// someone else's lease. Being wrong here costs a needless acquisition attempt
/// that the metadata group will adjudicate — never correctness, because the
/// epoch mint fences regardless.
pub fn decide(
    view: &vtop_meta::AdminReadRangeLeaseResponse,
    this_node: Uuid,
    now_ms: i64,
) -> LeaseDecision {
    if !view.found {
        return LeaseDecision::RangeMissing;
    }
    match view.lease.as_ref() {
        None => LeaseDecision::Acquire {
            expected_range_generation: view.range_generation,
        },
        Some(lease) if lease.holder_node_uuid == this_node => {
            match lease.expires_at_ms {
                // An operator pinned this node permanently. Renewing would be
                // refused by the state machine — and demoting on that refusal
                // would oscillate a holder metadata says owns the range.
                None => LeaseDecision::HeldAdministratively {
                    fencing_epoch: lease.fencing_epoch,
                },
                // Ours and live: renew it. Note this is keyed on the epoch
                // metadata reports, not the one we remember, so an agent whose
                // broker has drifted re-synchronises rather than renewing a
                // fiction.
                Some(deadline) if now_ms < deadline => LeaseDecision::Renew {
                    fencing_epoch: lease.fencing_epoch,
                },
                // Ours but lapsed. Returning Renew here would loop forever:
                // renew → refused (expiry is final) → demote → read → still
                // nominally ours → renew again. Re-enter acquisition instead,
                // which mints a fresh epoch.
                Some(_) => LeaseDecision::Acquire {
                    expected_range_generation: view.range_generation,
                },
            }
        }
        Some(lease) => {
            let live = match lease.expires_at_ms {
                // An administrative grant never lapses. Waiting forever is
                // correct: an operator pinned this holder, and an election
                // must not quietly undo that.
                None => true,
                Some(deadline) => now_ms < deadline,
            };
            if live {
                LeaseDecision::Wait {
                    fencing_epoch: lease.fencing_epoch,
                }
            } else {
                LeaseDecision::Acquire {
                    expected_range_generation: view.range_generation,
                }
            }
        }
    }
}

/// Drives one range's lease against the metadata group.
pub struct LeaseAgent {
    admin: AdminClient,
    config: LeaseAgentConfig,
    node_uuid: Uuid,
    topic_uuid: Uuid,
    range_uuid: Uuid,
    promoter: Promoter,
    state: LeaseState,
    /// Marked once the first metadata exchange COMPLETES, whatever it said
    /// (granted, refused, rival held — each tells this node where it
    /// stands). Same doctrine as the watcher's gate: a candidate that has
    /// never reached metadata does not know the current epoch, its follower
    /// refuses every append, and reporting ready before that first read is
    /// the same false green the static follower already refuses to show.
    ready: Option<vtop_observe::ReadinessGate>,
    /// Set by the SUPERVISOR when it holds a granted epoch it cannot serve
    /// (#367): the leader build failed after the grant, so the lease must go
    /// back and this node must not immediately campaign for it again.
    ///
    /// The eligibility stand-aside in `publish_held` cannot cover this. That
    /// one fires BEFORE a grant, on a refusal the promoter made; this fires
    /// AFTER one, on a failure only the supervisor can see.
    stand_down: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// A persistent handle on the same gate, kept so a range that goes
    /// missing AFTER the gate opened can revoke readiness — the consumable
    /// `ready` above only governs the first open (review).
    gate: Option<vtop_observe::ReadinessGate>,
    /// True while readiness is being withheld because of a missing range,
    /// so recovery re-marks the gate exactly once.
    gate_degraded: bool,
    /// True when the last completed exchange said the RANGE ITSELF is gone.
    /// That answer must not open the readiness gate: a candidate configured
    /// with a deleted or unknown range is a configuration fault, and the
    /// watcher keeps readiness closed for the same case (review).
    range_missing: bool,
    /// The rival epoch most recently fenced into the local view by the Wait
    /// arm. Kept so that LOSING SIGHT of metadata while following suspends
    /// that epoch — the watcher's exact doctrine: without it, a following
    /// candidate would keep accepting replication at a stale epoch for as
    /// long as metadata stays unreachable, on nothing but the memory of a
    /// read that may already be superseded (review).
    observed_rival: Option<u64>,
    /// Rounds of the run loop during which this node will NOT campaign for
    /// the lease, set when a promotion was refused on eligibility grounds
    /// (LeaderBehind or the §5.4.1 vote check). A refused candidate that
    /// keeps winning the acquisition race starves the replica its own
    /// refusal named — it holds the lease it cannot serve, lets it lapse,
    /// and wins again — while receiving no replication with which to become
    /// eligible. Standing aside for a bounded window gives the eligible
    /// replica uncontested acquisitions; BOUNDED, not until-another-holder,
    /// because if the eligible replica is down someone must keep probing,
    /// and the refusal repeating is the honest unavailability signal.
    campaign_hold_off_rounds: u32,
    /// How much of the current epoch's quorum-miss hold is left (#375).
    ///
    /// Per epoch, not per process: cleared on a successful promotion, and a
    /// different epoch opens a fresh window rather than inheriting a spent one.
    quorum_miss_hold: Option<QuorumMissHold>,
    /// Local upper bound on how long the current hold may be trusted without
    /// hearing from metadata, in the same wall-clock the envelope carries.
    ///
    /// Always at or before the deadline metadata recorded: it is taken from
    /// the lease view, or computed from a timestamp captured BEFORE the
    /// renewal/acquisition proposal — the state machine mints its deadline
    /// later than that. `None` while not held, or held administratively
    /// (which cannot lapse).
    held_until_ms: Option<i64>,
}

/// Whether a holder that cannot reach the metadata plane must stop serving.
///
/// Without this, a broker partitioned from metadata after promotion would
/// keep accepting writes forever on its process-local epoch while a rival
/// legitimately acquires the range after the recorded deadline. The epoch
/// still guarantees no torn history — this is about not accepting writes we
/// already know a rival may be authorized to supersede.
/// How many poll rounds a stand-aside sits out, given the two durations that
/// decide it.
///
/// A free function so the arithmetic is testable without an admin client —
/// and it is worth testing, because the LENGTH is the whole mechanism. Too
/// short and the node that just stood aside wins the next race anyway, which
/// is the re-acquisition loop of #367 with extra steps; the hold-off has to
/// outlast the lease so somebody else gets an uncontested acquisition.
fn stand_aside_rounds_for(lease: Duration, poll: Duration) -> u32 {
    let lease_ms = lease.as_millis().max(1);
    let poll_ms = poll.as_millis().max(1);
    (lease_ms.saturating_mul(2).div_ceil(poll_ms)).min(u128::from(u32::MAX)) as u32
}

fn must_demote_locally(state: LeaseState, held_until_ms: Option<i64>, now_ms: i64) -> bool {
    matches!(state, LeaseState::Held { .. })
        && held_until_ms.is_some_and(|deadline| now_ms >= deadline)
}

impl LeaseAgent {
    pub fn new(
        admin: AdminClient,
        config: LeaseAgentConfig,
        node_uuid: Uuid,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        publisher: Arc<dyn LeasePublisher>,
        probe: Option<Arc<dyn QuorumProbe>>,
    ) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            admin,
            config,
            node_uuid,
            topic_uuid,
            range_uuid,
            promoter: Promoter {
                quorum_miss: false,
                publisher,
                probe,
                verified_epoch: None,
                node_uuid,
                range_uuid,
                stand_aside: false,
            },
            state: LeaseState::NotHeld,
            ready: None,
            stand_down: None,
            gate: None,
            gate_degraded: false,
            range_missing: false,
            observed_rival: None,
            held_until_ms: None,
            campaign_hold_off_rounds: 0,
            quorum_miss_hold: None,
        })
    }

    /// Share the flag a supervisor sets when it cannot serve an epoch it was
    /// granted (#367). See [`LeaseAgent::stand_down`].
    pub fn with_stand_down(mut self, flag: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.stand_down = Some(flag);
        self
    }

    /// Open `gate` on the first completed metadata exchange (see the
    /// `ready` field for why a candidate is not ready before that).
    pub fn with_ready_gate(mut self, gate: vtop_observe::ReadinessGate) -> Self {
        self.ready = Some(gate.clone());
        self.gate = Some(gate);
        self
    }

    /// Run until the process stops or `release` fires (#280).
    ///
    /// Losing a lease is a state transition, not a reason to exit — the node
    /// stays up so it can be inspected and can win the range back. Shutdown
    /// is different: a departing holder RELEASES the range instead of letting
    /// the lease lapse, so failover starts as soon as the holder leaves
    /// rather than after a metadata deadline the departed leader can no
    /// longer make use of.
    ///
    /// `release` is NOT the raw process signal — the node fires it only after
    /// the native server has stopped admitting and drained. Releasing any
    /// earlier would let metadata authorize a successor at a higher epoch
    /// while this broker still executes an admitted produce under the old
    /// one, and a record acked in that window could sit above the boundary
    /// the successor's promotion proves — acknowledged, then outside the
    /// range everyone agrees on. The order is: stop admitting, drain, THEN
    /// hand the range back.
    pub async fn run(mut self, mut release: tokio::sync::watch::Receiver<bool>) {
        // Said out loud at start of life: an agent that never logs is
        // indistinguishable from an agent that never ran, and the difference
        // once cost a debugging session (#284).
        tracing::info!(
            range = %self.range_uuid,
            node = %self.node_uuid,
            "lease agent running"
        );
        let mut release_closed = false;
        loop {
            if *release.borrow() {
                break;
            }
            // GIVE THE EPOCH BACK BEFORE ANYTHING ELSE (#367). The supervisor
            // sets this when a leader build failed after the grant: the node
            // holds an epoch it demonstrably cannot serve, so renewing it
            // strands the range on a replica that will never answer.
            //
            // Releasing alone is not enough, and that is the whole bug. The
            // previous behaviour — exit the process — assumed the lease would
            // lapse and a healthy candidate would win, but an orchestrator
            // restarts the pod well inside the lease duration and the fresh
            // process campaigns at once, wins again because nothing marks it
            // as the node that just failed, and fails the same way. The epoch
            // climbs, the survivors starve. So the release is paired with the
            // same hold-off an eligibility refusal takes: hand it back AND sit
            // out, long enough for somebody else to win uncontested.
            if self
                .stand_down
                .as_ref()
                .is_some_and(|flag| flag.swap(false, std::sync::atomic::Ordering::SeqCst))
            {
                if let LeaseState::Held { fencing_epoch } = self.state {
                    tracing::warn!(
                        range = %self.range_uuid,
                        fencing_epoch,
                        "standing down: this node was granted an epoch it could not serve, \
                         so the lease goes back and this node sits out the next rounds"
                    );
                    // Raced against the release signal exactly as the round
                    // is (review): this release can spend two bounded
                    // deadlines, and a shutdown arriving inside it used to
                    // wait them out before the loop head could break —
                    // leaving the FINAL release less drain budget than its
                    // own two calls need. Abandoning loses nothing: the
                    // exit-path release re-reconciles against metadata and
                    // proposes with whatever epoch it finds.
                    if release_closed {
                        self.release().await;
                    } else {
                        tokio::select! {
                            () = self.release() => {}
                            changed = release.changed() => {
                                if changed.is_err() {
                                    release_closed = true;
                                }
                                // Only an actual `true` abandons — the loop
                                // head breaks on it and the exit release
                                // takes over. A closed watch or a spurious
                                // wake finishes the hand-back here.
                                if !*release.borrow() {
                                    self.release().await;
                                }
                            }
                        }
                    }
                    self.publish_lost(fencing_epoch);
                }
                self.state = LeaseState::NotHeld;
                self.held_until_ms = None;
                self.campaign_hold_off_rounds = self.stand_aside_rounds();
                // Re-checked AFTER the block (review, round six): the
                // abandon arm above CONSUMES the watch notification, so the
                // round's own race below can never wake for it — falling
                // through would spend a full round plus its delay before
                // the loop head looked again, exactly the starvation the
                // race exists to prevent. The loss is published and the
                // state reset either way; the exit release reconciles the
                // epoch from metadata.
                if *release.borrow() {
                    break;
                }
            }
            // The round races the release (#408): an agent already inside
            // `step()` used to finish it — up to one bounded deadline reading
            // metadata and another renewing — before it ever looked at the
            // flag again, and with `renew_interval` near the lease duration a
            // valid configuration could run the drain budget dry before the
            // ReleaseRangeLease proposal was even submitted. A holder that is
            // leaving has no use for the abandoned round's answer; the loop
            // head re-reads the flag, so the release path starts immediately
            // and a spurious wake just re-races. A watch that CLOSED without
            // publishing `true` is "no release will ever be requested" — the
            // oneshot adapter's rule — not a reason to abandon rounds.
            //
            // One window stays open KNOWINGLY (review): dropping the step
            // future does not cancel a proposal the admin server already
            // decoded, so an abandoned AcquireRangeLease can commit AFTER
            // the shutdown reconciliation read observed no lease. That
            // shape is crash-equivalent — an acquisition always mints an
            // EXPIRING lease (only the operator's grant path mints the
            // non-expiring kind), so it lapses on its deadline exactly as a
            // SIGKILL'd holder's would — and closing it would mean awaiting
            // the in-flight proposal at shutdown, the very delay this race
            // exists to remove.
            let stepped = if release_closed {
                self.step().await
            } else {
                tokio::select! {
                    stepped = self.step() => stepped,
                    changed = release.changed() => {
                        // The VALUE decides before the closure does
                        // (review): a sender that published `true` and was
                        // then dropped is a release whose channel closed —
                        // the loop head breaks on it — not a closure to
                        // keep stepping through.
                        if *release.borrow() {
                            continue;
                        }
                        if changed.is_err() {
                            release_closed = true;
                            self.step().await
                        } else {
                            continue;
                        }
                    }
                }
            };
            let delay = match stepped {
                Ok(delay) => {
                    if self.range_missing {
                        // Definitive, not transient: metadata answered and
                        // said the range does not exist. Readiness is
                        // withheld before the first open and REVOKED after
                        // it — a node serving a deleted range is a
                        // configuration fault however long it has been up
                        // (review).
                        if let Some(gate) = &self.gate {
                            gate.mark_not_ready(
                                "the configured range is missing from metadata".to_owned(),
                            );
                        }
                        self.gate_degraded = true;
                    } else {
                        if let Some(gate) = self.ready.take() {
                            gate.mark_ready();
                        } else if self.gate_degraded {
                            if let Some(gate) = &self.gate {
                                gate.mark_ready();
                            }
                        }
                        self.gate_degraded = false;
                    }
                    delay
                }
                Err(error) => {
                    // A metadata group mid-election refuses reads. Retrying is
                    // right; what would be wrong is treating an unreachable
                    // metadata plane as proof that we still hold the lease.
                    // The broker keeps serving only until the deadline of the
                    // last lease metadata actually confirmed — past that, a
                    // rival may already have been granted the range, so the
                    // broker is demoted locally rather than trusting a future
                    // read to deliver the news.
                    tracing::warn!(
                        %error,
                        range = %self.range_uuid,
                        "lease agent round failed; retrying"
                    );
                    if must_demote_locally(self.state, self.held_until_ms, now_ms()) {
                        if let LeaseState::Held { fencing_epoch } = self.state {
                            tracing::warn!(
                                range = %self.range_uuid,
                                fencing_epoch,
                                "metadata unreachable past the lease deadline; demoting locally"
                            );
                            self.publish_lost(fencing_epoch);
                        }
                    } else if !matches!(self.state, LeaseState::Held { .. }) {
                        // Following, and blind: the watcher's doctrine applies
                        // here too. Losing sight of metadata is not the lease
                        // ending, but it is the end of knowing the observed
                        // epoch is still current — so the view is suspended,
                        // reactivatable the moment a read lands, rather than
                        // left accepting replication on the memory of a read
                        // a turnover may already have superseded (review).
                        // Suspension is idempotent; repeating it every
                        // failing round is free.
                        if let Some(epoch) = self.observed_rival {
                            self.promoter.publisher.suspend(epoch);
                        }
                    }
                    self.config.poll_interval
                }
            };
            if release_closed {
                tokio::time::sleep(delay).await;
                continue;
            }
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                changed = release.changed() => {
                    // A closed watch completes instantly forever; without the
                    // flag this select would never sleep again and the agent
                    // would hot-poll metadata (#408).
                    if changed.is_err() {
                        release_closed = true;
                    }
                }
            }
        }
        self.release().await;
    }

    /// Hand the range back on the way out (#280) — the operational half of an
    /// orderly stop. Best-effort by design: a refusal means the lease was
    /// already lost or taken, and blocking exit on metadata would trade a
    /// prompt failover for a hung shutdown, the exact inversion of the goal.
    async fn release(&mut self) {
        // What THIS PROCESS recorded is not what metadata decided (review,
        // twice over): the abandoned round can die after metadata committed
        // an acquisition and before `self.state` caught up (NotHeld with a
        // lease on record), and it can equally die during a re-grant's
        // publish while `self.state` still names the PREVIOUS epoch — a
        // release proposed at a stale epoch is refused, and an
        // administrative lease has no expiry to mop up the refusal. One
        // bounded read reconciles both shapes: whatever epoch metadata
        // attributes to this node is the epoch the release names. The read
        // failing falls back to what the agent recorded — best effort, as
        // this whole path is — and a lease metadata attributes to someone
        // else is not this node's to release. Loss is published only for a
        // hold that was actually published.
        let recorded = match self.state {
            LeaseState::Held { fencing_epoch } => Some(fencing_epoch),
            _ => None,
        };
        let metadata_view = self
            .bounded(
                "read range lease at shutdown",
                self.admin
                    .read_range_lease(self.topic_uuid, self.range_uuid),
            )
            .await
            .ok()
            .map(|view| {
                view.lease
                    .filter(|lease| lease.holder_node_uuid == self.node_uuid)
                    .map(|lease| lease.fencing_epoch)
            });
        let fencing_epoch = match (recorded, metadata_view) {
            // Metadata answered: its attribution wins, stale or fresh.
            (recorded, Some(Some(meta_epoch))) => {
                if recorded.is_none() {
                    tracing::warn!(
                        range = %self.range_uuid,
                        fencing_epoch = meta_epoch,
                        "metadata says this node holds a lease this agent never \
                         recorded (a round abandoned mid-acquisition); releasing it \
                         before exit"
                    );
                } else if recorded != Some(meta_epoch) {
                    tracing::warn!(
                        range = %self.range_uuid,
                        recorded = recorded.unwrap_or_default(),
                        fencing_epoch = meta_epoch,
                        "metadata attributes this node's lease to a newer epoch than \
                         the agent recorded (a round abandoned mid-publish); the \
                         release names metadata's epoch"
                    );
                }
                meta_epoch
            }
            // Metadata answered and this node holds nothing: no proposal to
            // make, but a hold that was published is still published as lost.
            (recorded, Some(None)) => {
                if let Some(epoch) = recorded {
                    self.publish_lost(epoch);
                }
                return;
            }
            // The read failed: propose with what the agent recorded, the
            // pre-reconciliation behavior — best effort either way.
            (Some(recorded), None) => recorded,
            (None, None) => return,
        };
        let was_published = recorded.is_some();
        let proposal = self
            .bounded(
                "propose release",
                self.admin.propose(MetadataCommand::ReleaseRangeLease {
                    env: envelope(),
                    topic_uuid: self.topic_uuid,
                    range_uuid: self.range_uuid,
                    expected_fencing_epoch: fencing_epoch,
                }),
            )
            .await;
        match proposal {
            Ok(response) => match response.response {
                MetadataResponse::Ack { .. } => tracing::info!(
                    range = %self.range_uuid,
                    fencing_epoch,
                    "range lease released for shutdown; failover need not wait out the deadline"
                ),
                other => tracing::warn!(
                    range = %self.range_uuid,
                    fencing_epoch,
                    ?other,
                    "lease release refused; the lease will lapse on its deadline"
                ),
            },
            Err(error) => tracing::warn!(
                %error,
                range = %self.range_uuid,
                fencing_epoch,
                "lease release failed; the lease will lapse on its deadline"
            ),
        }
        if was_published {
            self.publish_lost(fencing_epoch);
        }
    }

    /// Every admin round trip is bounded by this. A blackholed endpoint (as
    /// opposed to a refused connection) would otherwise leave the await
    /// pending indefinitely — and the local-deadline demotion only runs
    /// between rounds, so an unbounded await is a partitioned holder that
    /// stays promoted past the instant a rival may legally own the range.
    /// The renew interval is the natural bound: an answer that arrives later
    /// than the next renewal was due is an answer the pacing cannot use.
    fn rpc_deadline(&self) -> Duration {
        self.config.renew_interval
    }

    async fn bounded<T, E: std::fmt::Display>(
        &self,
        what: &str,
        call: impl std::future::Future<Output = Result<T, E>>,
    ) -> Result<T, String> {
        match tokio::time::timeout(self.rpc_deadline(), call).await {
            Ok(result) => result.map_err(|error| format!("{what}: {error}")),
            Err(_) => Err(format!(
                "{what}: no answer within {:?}",
                self.rpc_deadline()
            )),
        }
    }

    /// One round. Returns how long to wait before the next.
    async fn step(&mut self) -> Result<Duration, String> {
        // Captured BEFORE any proposal: the state machine mints its deadline
        // later than this, so a local deadline derived from it is always at
        // or before the one metadata records.
        let round_started_ms = now_ms();
        self.range_missing = false;
        let view = self
            .bounded(
                "read range lease",
                self.admin
                    .read_range_lease(self.topic_uuid, self.range_uuid),
            )
            .await?;
        let decision = decide(&view, self.node_uuid, now_ms());
        match decision {
            LeaseDecision::Renew { fencing_epoch } => {
                // Trust exactly what metadata reported until the renewal
                // lands; a renewal that errors out must not slide the local
                // deadline forward on the strength of a read alone.
                let confirmed_until = view.lease.as_ref().and_then(|lease| lease.expires_at_ms);
                match self.publish_held(fencing_epoch, confirmed_until).await {
                    Promoted::Yes => self.clear_quorum_miss_hold(),
                    // AN ELIGIBILITY REFUSAL LAPSES ON PURPOSE. It named a
                    // better holder, and the lapse is how the range reaches it.
                    Promoted::Refused => return Ok(self.config.poll_interval),
                    Promoted::QuorumMissed => {
                        self.hold_through_quorum_miss(fencing_epoch, round_started_ms)
                            .await?;
                        return Ok(self.config.poll_interval);
                    }
                }
                match self.renew(fencing_epoch).await? {
                    true => {
                        let renewed_until = round_started_ms
                            .saturating_add_unsigned(duration_ms(self.config.lease_duration)?);
                        self.held_until_ms = Some(
                            self.held_until_ms
                                .map_or(renewed_until, |current| current.max(renewed_until)),
                        );
                        Ok(self.config.renew_interval)
                    }
                    false => {
                        // Refused: metadata has moved on. Publish the loss
                        // BEFORE trying to win it back, so there is no window
                        // in which the broker serves under an epoch metadata
                        // has already given away.
                        self.publish_lost(fencing_epoch);
                        Ok(self.config.poll_interval)
                    }
                }
            }
            LeaseDecision::HeldAdministratively { fencing_epoch } => {
                // No deadline to track: an administrative lease cannot lapse,
                // so a metadata partition never forces a local demotion.
                if self.publish_held(fencing_epoch, None).await != Promoted::Yes {
                    return Ok(self.config.poll_interval);
                }
                Ok(self.config.renew_interval)
            }
            LeaseDecision::Acquire {
                expected_range_generation,
            } => {
                // Metadata no longer guarantees us the range — no lease, an
                // expired rival, or our own lapsed one. If the broker was
                // serving, stop it BEFORE campaigning: a rival may win the
                // acquisition race the moment we enter it.
                if let LeaseState::Held { fencing_epoch } = self.state {
                    self.publish_lost(fencing_epoch);
                }
                // Standing aside after an eligibility refusal: campaigning
                // now would only take the lease away from the replica the
                // refusal named, hold it unserved, and lapse it again.
                if self.campaign_hold_off_rounds > 0 {
                    self.campaign_hold_off_rounds -= 1;
                    tracing::debug!(
                        range = %self.range_uuid,
                        rounds_remaining = self.campaign_hold_off_rounds,
                        "standing aside from the lease race after an eligibility refusal"
                    );
                    return Ok(self.config.poll_interval);
                }
                match self.acquire(expected_range_generation).await? {
                    Some(fencing_epoch) => {
                        let granted_until = Some(
                            round_started_ms
                                .saturating_add_unsigned(duration_ms(self.config.lease_duration)?),
                        );
                        match self.publish_held(fencing_epoch, granted_until).await {
                            Promoted::Yes => {
                                self.clear_quorum_miss_hold();
                                Ok(self.config.renew_interval)
                            }
                            // A FRESH EPOCH CAN MISS ON ITS FIRST PROBE, and
                            // falling through here let the next poll arrive
                            // after the grant expired — minting another epoch
                            // and bypassing the bound (review).
                            Promoted::QuorumMissed => {
                                self.hold_through_quorum_miss(fencing_epoch, round_started_ms)
                                    .await?;
                                Ok(self.config.poll_interval)
                            }
                            // An eligibility verdict named a better holder, so
                            // metadata's deadline hands it on; serving now
                            // would be the guess.
                            Promoted::Refused => Ok(self.config.poll_interval),
                        }
                    }
                    // Lost the race, or our CAS token was stale. Either way the
                    // next read tells us the truth; guessing would not.
                    None => Ok(self.config.poll_interval),
                }
            }
            LeaseDecision::Wait { fencing_epoch } => {
                if let LeaseState::Held {
                    fencing_epoch: ours,
                } = self.state
                {
                    // We thought we held it and metadata disagrees. This is
                    // the transition that matters most: it is how a node that
                    // was partitioned learns it has been replaced.
                    self.publish_lost(ours);
                }
                // Fence the local view up to the rival's epoch even when we
                // never believed we held the range: at startup the broker may
                // still carry a configured epoch metadata has already
                // superseded, and it must not keep serving on it. Demotion is
                // monotonic and idempotent, so repeating it every poll is
                // free.
                self.promoter.lost(fencing_epoch);
                self.observed_rival = Some(fencing_epoch);
                // A rival holding the range is the stand-aside's purpose
                // ACHIEVED: the replica this node's refusal made way for (or
                // any other eligible one) has the lease. Clearing the
                // hold-off here means a much later failure of that holder is
                // answered by an immediate campaign, not by serving out the
                // residue of a wait that already did its job (review round
                // four).
                self.campaign_hold_off_rounds = 0;
                Ok(self.config.poll_interval)
            }
            LeaseDecision::RangeMissing => {
                self.range_missing = true;
                if let LeaseState::Held { fencing_epoch } = self.state {
                    // A range deleted out from under its holder is an operator
                    // action; the broker must not keep serving it.
                    self.publish_lost(fencing_epoch);
                } else if let Some(epoch) = self.observed_rival {
                    // A FOLLOWING view fails closed on the same answer:
                    // without this, the last observed rival epoch stays
                    // active and the node keeps accepting authenticated
                    // replication for a range metadata has deleted (review).
                    // Suspend, not demote — through the candidate's
                    // observation dialect a demotion MEANS "rival grant,
                    // serve this epoch", while suspension is the fail-closed
                    // verb in both dialects, and it leaves the view
                    // reactivatable should the range be recreated and a new
                    // grant observed. Idempotent, so repeating it every
                    // round the range stays missing is free.
                    self.promoter.publisher.suspend(epoch);
                }
                Ok(self.config.poll_interval)
            }
        }
    }

    async fn acquire(&self, expected_range_generation: u64) -> Result<Option<u64>, String> {
        let response = self
            .bounded(
                "propose acquire",
                self.admin.propose(MetadataCommand::AcquireRangeLease {
                    env: envelope(),
                    topic_uuid: self.topic_uuid,
                    range_uuid: self.range_uuid,
                    holder_node_uuid: self.node_uuid,
                    expected_range_generation,
                    lease_duration_ms: duration_ms(self.config.lease_duration)?,
                }),
            )
            .await?;
        Ok(match response.response {
            MetadataResponse::LeaseGranted { fencing_epoch } => Some(fencing_epoch),
            // A rejection here is ordinary: a rival won, or the generation
            // moved. It is not an error to log loudly.
            _ => None,
        })
    }

    async fn renew(&self, fencing_epoch: u64) -> Result<bool, String> {
        let response = self
            .bounded(
                "propose renew",
                self.admin.propose(MetadataCommand::RenewRangeLease {
                    env: envelope(),
                    topic_uuid: self.topic_uuid,
                    range_uuid: self.range_uuid,
                    holder_node_uuid: self.node_uuid,
                    expected_fencing_epoch: fencing_epoch,
                    lease_duration_ms: duration_ms(self.config.lease_duration)?,
                }),
            )
            .await?;
        Ok(matches!(
            response.response,
            MetadataResponse::LeaseGranted { .. }
        ))
    }

    async fn publish_held(&mut self, fencing_epoch: u64, held_until_ms: Option<i64>) -> Promoted {
        if !self.promoter.ensure(fencing_epoch).await {
            let quorum_miss = self.promoter.take_quorum_miss();
            if self.promoter.take_stand_aside() {
                // Two lease lifetimes of poll rounds: enough for the replica
                // the refusal named to see the lapse and win at least one
                // uncontested acquisition, however the two agents' polls
                // interleave.
                self.campaign_hold_off_rounds = self.stand_aside_rounds();
            }
            self.state = LeaseState::NotHeld;
            self.held_until_ms = None;
            return if quorum_miss {
                Promoted::QuorumMissed
            } else {
                Promoted::Refused
            };
        }
        self.state = LeaseState::Held { fencing_epoch };
        self.held_until_ms = held_until_ms;
        Promoted::Yes
    }

    /// Hold a quorum-missed epoch still for as long as the budget allows.
    ///
    /// SHARED BY BOTH ARMS THAT CAN SEE A QUORUM MISS (review). The renew arm
    /// is the common one, but a freshly acquired epoch can miss on its very
    /// first probe, and leaving that arm to fall through meant the next poll
    /// could arrive after the grant expired — minting another epoch and
    /// bypassing the bound this exists to impose. One path, so there is one
    /// behaviour.
    async fn hold_through_quorum_miss(
        &mut self,
        fencing_epoch: u64,
        now_ms: i64,
    ) -> Result<(), String> {
        // ONE lease lifetime, because the last renewal inside it adds another.
        let budget = quorum_miss_budget(
            fencing_epoch,
            now_ms,
            self.quorum_miss_hold,
            self.config.lease_duration,
        );
        self.quorum_miss_hold = budget.state;
        if budget.hold {
            // Renewing is not serving: `suspended` cleared `lease_active` on
            // the broker's view and only a successful re-probe restores it.
            // All this does is stop the epoch moving, which is the one thing a
            // replica that is behind needs in order to stop being behind.
            let _ = self.renew(fencing_epoch).await?;
        }
        Ok(())
    }

    fn clear_quorum_miss_hold(&mut self) {
        self.quorum_miss_hold = None;
    }

    /// How many poll rounds an eligibility refusal sits out: two lease
    /// lifetimes, expressed in this agent's own polling cadence.
    fn stand_aside_rounds(&self) -> u32 {
        stand_aside_rounds_for(self.config.lease_duration, self.config.poll_interval)
    }

    fn publish_lost(&mut self, fencing_epoch: u64) {
        tracing::warn!(
            range = %self.range_uuid,
            fencing_epoch,
            "range lease lost; the broker will refuse writes under this epoch"
        );
        self.promoter.lost(fencing_epoch);
        self.state = LeaseState::NotHeld;
        self.held_until_ms = None;
    }
}

fn envelope() -> CommandEnvelope {
    CommandEnvelope {
        request_id: Uuid::new_v4(),
        issued_at_ms: now_ms(),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| i64::try_from(since.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn duration_ms(duration: Duration) -> Result<u64, String> {
    u64::try_from(duration.as_millis())
        .map_err(|_| format!("lease duration {duration:?} does not fit the wire format"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use vtop_meta::{AdminLeaseView, AdminReadRangeLeaseResponse};

    const US: Uuid = Uuid::from_u128(1);
    const THEM: Uuid = Uuid::from_u128(2);

    fn view(lease: Option<AdminLeaseView>) -> AdminReadRangeLeaseResponse {
        AdminReadRangeLeaseResponse {
            found: true,
            range_generation: 7,
            fencing_epoch: lease.map(|l| l.fencing_epoch).unwrap_or(0),
            lease,
            read_at_applied_index: 42,
        }
    }

    fn held_by(holder: Uuid, expires_at_ms: Option<i64>) -> Option<AdminLeaseView> {
        Some(AdminLeaseView {
            holder_node_uuid: holder,
            fencing_epoch: 4,
            expires_at_ms,
        })
    }

    #[test]
    fn an_unleased_range_is_acquired() {
        assert_eq!(
            decide(&view(None), US, 1_000),
            LeaseDecision::Acquire {
                expected_range_generation: 7
            }
        );
    }

    #[test]
    fn our_own_lease_is_renewed_at_the_epoch_metadata_reports() {
        // Keyed on metadata's epoch, not one we remember, so an agent whose
        // broker drifted re-synchronises instead of renewing a fiction.
        assert_eq!(
            decide(&view(held_by(US, Some(9_000))), US, 1_000),
            LeaseDecision::Renew { fencing_epoch: 4 }
        );
    }

    #[test]
    fn a_live_rival_lease_is_waited_out() {
        assert_eq!(
            decide(&view(held_by(THEM, Some(9_000))), US, 1_000),
            LeaseDecision::Wait { fencing_epoch: 4 }
        );
    }

    /// Our own administrative grant is held, not renewed: the state machine
    /// refuses renewing a deadline-less lease, and demoting on that refusal
    /// every round would oscillate a holder metadata says owns the range.
    #[test]
    fn our_own_administrative_lease_is_held_without_renewing() {
        assert_eq!(
            decide(&view(held_by(US, None)), US, i64::MAX),
            LeaseDecision::HeldAdministratively { fencing_epoch: 4 }
        );
    }

    /// Our own lapsed lease is re-acquired, never renewed: expiry is final in
    /// the state machine, so returning Renew would loop forever — renew,
    /// refused, demote, read, still nominally ours, renew — and the range
    /// would stay unservable.
    #[test]
    fn our_own_lapsed_lease_is_reacquired_not_renewed() {
        assert_eq!(
            decide(&view(held_by(US, Some(9_000))), US, 9_000),
            LeaseDecision::Acquire {
                expected_range_generation: 7
            }
        );
    }

    /// The liveness property the whole issue exists for: once the incumbent
    /// stops renewing, a rival takes over.
    #[test]
    fn an_expired_rival_lease_is_acquired() {
        assert_eq!(
            decide(&view(held_by(THEM, Some(9_000))), US, 9_001),
            LeaseDecision::Acquire {
                expected_range_generation: 7
            }
        );
    }

    /// An administrative grant is an operator's explicit choice and must never
    /// be taken by an election, however long the agent waits.
    #[test]
    fn an_administrative_rival_lease_is_never_taken() {
        assert_eq!(
            decide(&view(held_by(THEM, None)), US, i64::MAX),
            LeaseDecision::Wait { fencing_epoch: 4 }
        );
    }

    /// The fail-stop half of the lease contract: a holder that cannot reach
    /// metadata must stop serving once the last confirmed deadline passes,
    /// because past it a rival may legally hold the range.
    #[test]
    fn a_partitioned_holder_demotes_itself_once_its_deadline_passes() {
        let held = LeaseState::Held { fencing_epoch: 4 };
        assert!(must_demote_locally(held, Some(9_000), 9_000));
        assert!(
            !must_demote_locally(held, Some(9_000), 8_999),
            "before the deadline the lease is still metadata's word"
        );
        assert!(
            !must_demote_locally(held, None, i64::MAX),
            "an administrative hold has no deadline to outlive"
        );
        assert!(
            !must_demote_locally(LeaseState::NotHeld, Some(9_000), i64::MAX),
            "a non-holder has nothing to demote"
        );
    }

    /// A range nobody created is not a range to campaign for; spinning on
    /// acquisition would bury the real problem in noise.
    #[test]
    fn a_missing_range_is_not_campaigned_for() {
        let mut missing = view(None);
        missing.found = false;
        assert_eq!(decide(&missing, US, 1_000), LeaseDecision::RangeMissing);
    }

    /// Promotion must leave the broker's two epoch views EQUAL. Produce
    /// requires that equality, so a promotion that moved only one of them
    /// would wedge the range while looking like a successful failover.
    #[test]
    fn promotion_leaves_the_broker_able_to_serve_the_new_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Arc::new(test_broker(dir.path(), 4));
        let publisher = BrokerLeasePublisher::new(Arc::clone(&broker));

        publisher.promote(5, None);
        assert_eq!(broker.held_fencing_epoch(), 5);
        let (epoch, live) = broker.meta_fencing_epoch().try_snapshot().unwrap();
        assert_eq!(epoch, 5);
        assert!(live, "a promoted broker must hold a live lease");
    }

    /// A standalone broker has no replica set to establish against and must
    /// still promote on its own durable boundary; requiring a quorum it cannot
    /// form would make single-replica deployments unleadable.
    #[test]
    fn a_standalone_broker_promotes_without_a_replica_set() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Arc::new(test_broker(dir.path(), 4));
        let publisher = BrokerLeasePublisher::new(Arc::clone(&broker));
        publisher.promote(5, None);
    }

    /// A stale promotion arriving late must not rewind the broker onto an
    /// epoch metadata has already superseded.
    #[test]
    fn a_stale_promotion_cannot_rewind_the_held_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Arc::new(test_broker(dir.path(), 4));
        let publisher = BrokerLeasePublisher::new(Arc::clone(&broker));

        publisher.promote(7, None);
        publisher.promote(5, None);
        assert_eq!(
            broker.held_fencing_epoch(),
            7,
            "adoption is monotonic; a reordered round must not undo a newer grant"
        );
    }

    /// Demotion is what a partitioned node discovers. The broker must stop
    /// believing it may write, without rewinding what it was last granted.
    #[test]
    fn demotion_stops_the_broker_serving_but_keeps_the_granted_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Arc::new(test_broker(dir.path(), 4));
        let publisher = BrokerLeasePublisher::new(Arc::clone(&broker));

        publisher.promote(5, None);
        publisher.demote(5);
        let (_, live) = broker.meta_fencing_epoch().try_snapshot().unwrap();
        assert!(!live, "a demoted broker must not hold a live lease");
        assert_eq!(
            broker.held_fencing_epoch(),
            5,
            "the granted epoch records history; rewinding it would let a later \
             stale grant look current"
        );
    }

    /// A replicated v2 range publishes NOTHING at promotion (#240, §5.4.2):
    /// the probed boundary is a candidate, the cell stays where it was, and
    /// the epoch's marker is what pends. Driving the marker once against a
    /// healthy quorum publishes the marker's end — which covers the
    /// candidate — and clears the pending epoch.
    #[test]
    fn a_replicated_v2_promotion_publishes_only_through_its_marker() {
        let h = replicated_v2_harness();
        let publisher = BrokerLeasePublisher::new(Arc::clone(&h.leader));

        publisher.promote(FENCING_EPOCH, Some(0));
        assert_eq!(h.leader.held_fencing_epoch(), FENCING_EPOCH);
        assert_eq!(
            h.cluster_committed.get(),
            0,
            "the candidate boundary must not raise the cell: that is the counted-majority \
             trust the marker exists to replace"
        );
        assert_eq!(publisher.pending_marker_epoch(), Some(FENCING_EPOCH));

        let published = publisher
            .try_publish_pending_boundary()
            .expect("a healthy quorum acks the marker");
        assert_eq!(
            published,
            Some(1),
            "the marker sits at offset 0, so the published mark is its end"
        );
        assert_eq!(h.cluster_committed.get(), 1);
        assert_eq!(
            publisher.pending_marker_epoch(),
            None,
            "publication is what retires the pending epoch"
        );
        assert_eq!(
            publisher.try_publish_pending_boundary(),
            Ok(None),
            "nothing pending means nothing to do — not a second marker"
        );
    }

    /// The #340 wall, in miniature: while no majority can hold the marker
    /// the boundary stays unpublished and PENDING, and the moment the
    /// quorum returns the same pending epoch publishes — no restart, no
    /// re-promotion, and no second marker.
    #[test]
    fn a_pending_marker_publishes_the_moment_a_quorum_returns() {
        let h = replicated_v2_harness();
        let publisher = BrokerLeasePublisher::new(Arc::clone(&h.leader));
        h.followers[0].set_online(false);
        h.followers[1].set_online(false);

        publisher.promote(FENCING_EPOCH, Some(0));
        let refused = publisher
            .try_publish_pending_boundary()
            .expect_err("no majority holds the marker, so nothing may publish");
        assert!(
            refused.contains("not quorum-acked"),
            "the refusal must name the quorum, not something incidental: {refused}"
        );
        assert_eq!(
            h.cluster_committed.get(),
            0,
            "unpublished, as §5.4.2 requires"
        );
        assert_eq!(
            publisher.pending_marker_epoch(),
            Some(FENCING_EPOCH),
            "a refusal keeps the epoch pending for the next attempt"
        );

        h.followers[0].set_online(true);
        assert_eq!(
            publisher.try_publish_pending_boundary(),
            Ok(Some(1)),
            "one follower back is a majority of three; the retry publishes"
        );
        let (_, next_offset) = h.leader.local_offsets();
        assert_eq!(
            next_offset, 1,
            "the retry rode the duplicate path: one marker, not one per attempt"
        );
    }

    /// The driver itself, end to end on a runtime: it publishes what pends,
    /// keeps retrying through a refusal, and picks up a re-grant at a newer
    /// epoch without being poked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_driver_publishes_pending_boundaries_until_stopped() {
        let h = replicated_v2_harness();
        let publisher = Arc::new(BrokerLeasePublisher::new(Arc::clone(&h.leader)));
        let (stop, stop_rx) = tokio::sync::watch::channel(false);
        let driver = tokio::spawn(Arc::clone(&publisher).drive_boundary_marker(stop_rx));

        // Refused first: the driver must not give up.
        h.followers[0].set_online(false);
        h.followers[1].set_online(false);
        publisher.promote(FENCING_EPOCH, Some(0));
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        assert_eq!(
            h.cluster_committed.get(),
            0,
            "nothing publishes without a quorum"
        );
        assert_eq!(publisher.pending_marker_epoch(), Some(FENCING_EPOCH));

        h.followers[0].set_online(true);
        h.followers[1].set_online(true);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while publisher.pending_marker_epoch().is_some() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(
            h.cluster_committed.get(),
            1,
            "the retry published once quorum returned"
        );

        // A re-grant at a newer epoch pends a new marker; the driver wakes.
        // The followers adopt the epoch first, as their lease watchers do in
        // production — a follower still at the old epoch refuses the
        // marker, which is fencing working, not the driver failing.
        h.meta.set(FENCING_EPOCH + 1);
        for follower in &h.followers {
            follower.adopt_fencing_epoch(FENCING_EPOCH + 1);
        }
        publisher.promote(FENCING_EPOCH + 1, Some(1));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while publisher.pending_marker_epoch().is_some() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(
            h.cluster_committed.get(),
            2,
            "the newer epoch's marker is a second record and publishes its own end"
        );

        let _ = stop.send(true);
        tokio::time::timeout(std::time::Duration::from_secs(5), driver)
            .await
            .expect("the driver exits on shutdown")
            .unwrap();
    }

    /// A demotion or suspension retires the pending marker: a fenced or
    /// suspended leader's marker refuses anyway, and the driver must not
    /// spin against it. The re-promotion that lifts a suspension pends the
    /// epoch again.
    #[test]
    fn demotion_and_suspension_retire_the_pending_marker() {
        let h = replicated_v2_harness();
        let publisher = BrokerLeasePublisher::new(Arc::clone(&h.leader));

        publisher.promote(FENCING_EPOCH, Some(0));
        assert_eq!(publisher.pending_marker_epoch(), Some(FENCING_EPOCH));
        publisher.suspend(FENCING_EPOCH);
        assert_eq!(publisher.pending_marker_epoch(), None);
        publisher.promote(FENCING_EPOCH, Some(0));
        assert_eq!(
            publisher.pending_marker_epoch(),
            Some(FENCING_EPOCH),
            "lifting the suspension pends the same epoch again"
        );
        publisher.demote(FENCING_EPOCH);
        assert_eq!(publisher.pending_marker_epoch(), None);
        assert_eq!(h.cluster_committed.get(), 0, "nothing ever published");
    }

    /// While the marker is pending, fetch refuses — retryably — rather
    /// than serve the durable floor, which may sit below a mark the
    /// previous leader already published (review): a consumer must never
    /// watch the mark move backwards across a failover. Produce is still
    /// admitted, and publication reopens fetch.
    #[test]
    fn fetch_refuses_while_the_boundary_is_pending_and_serves_once_proven() {
        let h = replicated_v2_harness();
        let publisher = BrokerLeasePublisher::new(Arc::clone(&h.leader));
        let fetch = |start: u64| {
            h.leader.handle(
                vtop_protocol::Role::Consumer,
                vtop_protocol::WireFrame {
                    request_id: 900 + start,
                    stream_id: 1,
                    message: vtop_protocol::Message::FetchRequest(vtop_protocol::FetchRequest {
                        range: h.leader.range().clone(),
                        fencing_epoch: FENCING_EPOCH,
                        start_offset: start,
                        max_bytes: 1 << 20,
                        max_records: 64,
                    }),
                },
            )
        };
        assert!(!h.leader.boundary_pending());
        publisher.promote(FENCING_EPOCH, Some(0));
        assert!(h.leader.boundary_pending(), "the promotion raised the gate");
        match fetch(0).message {
            vtop_protocol::Message::Error(error) => assert_eq!(
                error.code,
                vtop_protocol::ErrorCode::Overloaded,
                "the refusal is the retryable kind, not a fence"
            ),
            other => panic!("fetch served before the boundary was proven: {other:?}"),
        }
        publisher
            .try_publish_pending_boundary()
            .expect("a healthy quorum acks the marker");
        assert!(!h.leader.boundary_pending(), "publication lowered the gate");
        assert!(
            matches!(fetch(0).message, vtop_protocol::Message::FetchResponse(_)),
            "fetch serves once the mark is proven"
        );

        // A demotion that retires the pending marker lowers the gate too: a
        // fenced broker refuses for its own reason, not this one.
        h.meta.set(FENCING_EPOCH + 1);
        for follower in &h.followers {
            follower.adopt_fencing_epoch(FENCING_EPOCH + 1);
        }
        publisher.promote(FENCING_EPOCH + 1, Some(1));
        assert!(h.leader.boundary_pending());
        publisher.demote(FENCING_EPOCH + 1);
        assert!(!h.leader.boundary_pending());
    }

    /// Rounds can arrive reordered (review): a stale promotion must not
    /// displace the marker the current epoch still owes, and a delayed
    /// demotion or suspension naming an older epoch must not retire it.
    #[test]
    fn a_stale_round_neither_displaces_nor_retires_the_current_markers() {
        let h = replicated_v2_harness();
        let publisher = BrokerLeasePublisher::new(Arc::clone(&h.leader));
        h.meta.set(FENCING_EPOCH + 1);
        publisher.promote(FENCING_EPOCH + 1, Some(0));
        assert_eq!(publisher.pending_marker_epoch(), Some(FENCING_EPOCH + 1));

        // A reordered promotion at the older epoch: adoption is monotonic,
        // so the broker stays at the newer epoch and so does the marker.
        publisher.promote(FENCING_EPOCH, Some(0));
        assert_eq!(h.leader.held_fencing_epoch(), FENCING_EPOCH + 1);
        assert_eq!(
            publisher.pending_marker_epoch(),
            Some(FENCING_EPOCH + 1),
            "a stale round must not displace the current epoch's marker"
        );

        // A delayed suspension and demotion naming the older epoch: neither
        // says anything about the newer epoch's marker.
        publisher.suspend(FENCING_EPOCH);
        assert_eq!(publisher.pending_marker_epoch(), Some(FENCING_EPOCH + 1));
        publisher.demote(FENCING_EPOCH);
        assert_eq!(
            publisher.pending_marker_epoch(),
            Some(FENCING_EPOCH + 1),
            "an older epoch's retirement leaves the newer marker pending"
        );
        // Naming the right epoch retires it.
        publisher.demote(FENCING_EPOCH + 1);
        assert_eq!(publisher.pending_marker_epoch(), None);
    }

    /// A v1-format replicated range keeps the direct publication path: its
    /// frame cannot carry a marker consumers could be shielded from, so the
    /// probed boundary is published as before and nothing pends.
    #[test]
    fn a_v1_range_keeps_the_pre_marker_publication_path() {
        let h = replicated_harness(false);
        let publisher = BrokerLeasePublisher::new(Arc::clone(&h.leader));
        publisher.promote(FENCING_EPOCH, Some(3));
        assert_eq!(
            h.cluster_committed.get(),
            3,
            "a v1 range publishes the probed boundary directly, as it always did"
        );
        assert_eq!(publisher.pending_marker_epoch(), None);
        assert_eq!(publisher.try_publish_pending_boundary(), Ok(None));
    }

    const FENCING_EPOCH: u64 = 18;

    struct ReplicatedHarness {
        _dirs: Vec<tempfile::TempDir>,
        meta: vtop_broker::MetaFencingEpoch,
        leader: Arc<LocalBroker>,
        followers: Vec<Arc<vtop_broker::replication::InProcessFollower>>,
        cluster_committed: vtop_broker::replication::ClusterCommittedOffset,
    }

    fn replicated_v2_harness() -> ReplicatedHarness {
        replicated_harness(true)
    }

    /// The boundary-marker harness's shape (vtop-broker/tests): one leader,
    /// two in-process followers, a shared metadata view, starting fenced at
    /// an epoch below the one the tests promote to.
    fn replicated_harness(v2: bool) -> ReplicatedHarness {
        use vtop_broker::replication::{
            ClusterCommittedOffset, InProcessFollower, InProcessReplicaSet, ReplicaSet,
        };
        let range = vtop_protocol::RangeIdentity {
            topic: "events.v1".to_owned(),
            topic_epoch: 1,
            range_id: Uuid::from_u128(0xC1),
            range_generation: 0,
        };
        let lineage = vtop_log::RangeLineage {
            range_id: range.range_id,
            generation: 0,
            key_range: vtop_log::KeyRange::full(),
            parents: Vec::new(),
        };
        let segment = |dir: &tempfile::TempDir, id: u128, node: Uuid| {
            if v2 {
                vtop_log::ActiveSegment::create_v2(
                    dir.path().join("range.active"),
                    vtop_log::SegmentDescriptorV2 {
                        segment_id: Uuid::from_u128(id),
                        topic: range.topic.clone(),
                        topic_epoch: range.topic_epoch,
                        lineage: lineage.clone(),
                        base_offset: 0,
                        segment_generation: 0,
                        creation_node_id: node,
                        creation_fencing_epoch: FENCING_EPOCH,
                    },
                    vtop_log::SegmentConfigV2::default(),
                )
                .unwrap()
            } else {
                vtop_log::ActiveSegment::create(
                    dir.path().join("range.active"),
                    vtop_log::SegmentDescriptor {
                        segment_id: Uuid::from_u128(id),
                        topic: range.topic.clone(),
                        topic_epoch: range.topic_epoch,
                        lineage: lineage.clone(),
                        base_offset: 0,
                    },
                    vtop_log::SegmentConfig::default(),
                )
                .unwrap()
            }
        };
        let leader_id = Uuid::from_u128(0xA1);
        // Fenced below the promoted epoch until `promote` adopts it, exactly
        // as a freshly granted leader is.
        let meta = vtop_broker::MetaFencingEpoch::new(FENCING_EPOCH - 1);
        let cluster_committed = ClusterCommittedOffset::new(0);
        let leader_dir = tempfile::tempdir().unwrap();
        let leader_segment = segment(&leader_dir, 0xD1, leader_id);
        let leader_epochs =
            vtop_broker::ProducerEpochJournal::open(leader_dir.path().join("epochs")).unwrap();
        let mut dirs = vec![leader_dir];
        let mut followers = Vec::new();
        for (index, node_id) in [Uuid::from_u128(0xA2), Uuid::from_u128(0xA3)]
            .into_iter()
            .enumerate()
        {
            let dir = tempfile::tempdir().unwrap();
            let follower_segment = segment(&dir, 0xE1 + index as u128, node_id);
            let epochs =
                vtop_broker::ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
            followers.push(Arc::new(
                InProcessFollower::new(
                    node_id,
                    follower_segment,
                    epochs,
                    range.clone(),
                    FENCING_EPOCH,
                    meta.clone(),
                    ClusterCommittedOffset::new(0),
                )
                .unwrap(),
            ));
            dirs.push(dir);
        }
        let replica_set = Arc::new(InProcessReplicaSet::new(followers.clone()));
        let leader = Arc::new(
            LocalBroker::with_replication(
                leader_segment,
                leader_epochs,
                range,
                FENCING_EPOCH - 1,
                meta.clone(),
                leader_id,
                Some(cluster_committed.clone()),
                Some(replica_set as Arc<dyn ReplicaSet>),
            )
            .unwrap(),
        );
        ReplicatedHarness {
            _dirs: dirs,
            meta,
            leader,
            followers,
            cluster_committed,
        }
    }

    fn test_broker(dir: &std::path::Path, epoch: u64) -> LocalBroker {
        let range = vtop_protocol::RangeIdentity {
            topic: "telemetry".into(),
            topic_epoch: 1,
            range_id: Uuid::from_u128(7),
            range_generation: 0,
        };
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
        LocalBroker::new(segment, epochs, range, epoch).unwrap()
    }

    fn promoter(
        publisher: Arc<dyn LeasePublisher>,
        probe: Option<Arc<dyn QuorumProbe>>,
    ) -> Promoter {
        Promoter {
            quorum_miss: false,
            publisher,
            probe,
            verified_epoch: None,
            stand_aside: false,
            // Matches `at(1, ..)`: the tests' candidate is node 1.
            node_uuid: Uuid::from_u128(1),
            range_uuid: Uuid::from_u128(21),
        }
    }

    #[derive(Default)]
    struct Recorder {
        promoted: std::sync::Mutex<Vec<(u64, Option<u64>)>>,
        demoted: std::sync::Mutex<Vec<u64>>,
        suspended: std::sync::Mutex<Vec<u64>>,
    }

    impl LeasePublisher for Recorder {
        fn promote(&self, fencing_epoch: u64, committed_offset: Option<u64>) {
            self.promoted
                .lock()
                .unwrap()
                .push((fencing_epoch, committed_offset));
        }
        fn demote(&self, fencing_epoch: u64) {
            self.demoted.lock().unwrap().push(fencing_epoch);
        }
        fn suspend(&self, fencing_epoch: u64) {
            self.suspended.lock().unwrap().push(fencing_epoch);
        }
    }

    struct FixedProbe {
        probes: std::sync::Mutex<Vec<crate::promotion::ReplicaProbe>>,
        factor: usize,
        calls: std::sync::atomic::AtomicUsize,
        /// Every epoch the probe was asked to fence at, in order. A probe taken
        /// at the wrong epoch would fence nothing that matters, so the value
        /// reaching this trait is worth asserting rather than assuming.
        fenced_at: std::sync::Mutex<Vec<u64>>,
    }

    #[async_trait::async_trait]
    impl QuorumProbe for FixedProbe {
        async fn probe(&self, fencing_epoch: u64) -> Vec<crate::promotion::ReplicaProbe> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.fenced_at.lock().unwrap().push(fencing_epoch);
            self.probes.lock().unwrap().clone()
        }
        fn replication_factor(&self) -> usize {
            self.factor
        }
    }

    fn fixed(probes: Vec<crate::promotion::ReplicaProbe>, factor: usize) -> Arc<FixedProbe> {
        Arc::new(FixedProbe {
            probes: std::sync::Mutex::new(probes),
            factor,
            calls: std::sync::atomic::AtomicUsize::new(0),
            fenced_at: std::sync::Mutex::new(Vec::new()),
        })
    }

    fn at(node: u128, offset: Option<u64>) -> crate::promotion::ReplicaProbe {
        crate::promotion::ReplicaProbe {
            node_id: Uuid::from_u128(node),
            local_committed_offset: offset,
        }
    }

    /// A refused promotion must PUBLISH the refusal, not merely flip local
    /// state — but as a suspension, not a demotion.
    ///
    /// Two bugs pinned here, one from each direction. Flipping local state
    /// alone left the broker's metadata view live while the agent stopped
    /// renewing — a deposed leader that keeps passing `/readyz`. And demoting
    /// (a release) poisoned the epoch: `clear_lease` records it in
    /// `released_through`, so when the quorum recovered a beat later, the
    /// successful re-promotion at the SAME epoch could never reactivate the
    /// view, wedging the range under its own live lease.
    #[tokio::test]
    async fn a_refused_promotion_suspends_rather_than_stranding_or_poisoning() {
        let recorder = Arc::new(Recorder::default());
        let mut promoter = promoter(
            Arc::clone(&recorder) as Arc<dyn LeasePublisher>,
            Some(fixed(vec![at(1, Some(10)), at(2, None), at(3, None)], 3)),
        );

        assert!(!promoter.ensure(5).await, "one of three cannot promote");
        assert!(
            recorder.promoted.lock().unwrap().is_empty(),
            "nothing may be published when the quorum refused"
        );
        assert_eq!(
            *recorder.suspended.lock().unwrap(),
            vec![5],
            "the refusal must be published, or the broker keeps advertising a \
             lease it cannot currently serve"
        );
        assert!(
            recorder.demoted.lock().unwrap().is_empty(),
            "a transient quorum miss must not release the epoch — release is \
             permanent, and the coming retry must be able to reactivate it"
        );
    }

    /// A stand-aside must outlast the lease it just gave back (#367).
    ///
    /// This is the length that makes standing aside mean anything. A node
    /// hands the epoch back because it could not serve it; if the hold-off
    /// expired inside the lease duration it would campaign again before any
    /// survivor had an uncontested shot, win — nothing marks it as the node
    /// that just failed — and fail identically. That is the loop this fix
    /// exists to break, and a hold-off measured in rounds rather than time is
    /// only correct if the arithmetic converts between them properly.
    #[test]
    fn a_stand_aside_outlasts_two_lease_lifetimes() {
        let lease = Duration::from_millis(15_000);
        let poll = Duration::from_millis(2_000);
        let rounds = stand_aside_rounds_for(lease, poll);
        assert_eq!(rounds, 15, "30s of hold-off at a 2s cadence");
        assert!(
            u128::from(rounds) * poll.as_millis() >= lease.as_millis() * 2,
            "the hold-off must cover two lease lifetimes in WALL CLOCK, not just \
             in round count: {rounds} rounds x {poll:?} against {lease:?}"
        );

        // A cadence that does not divide the window rounds UP, never down: a
        // hold-off one round short is a hold-off that ends inside the lease.
        assert_eq!(
            stand_aside_rounds_for(lease, Duration::from_millis(4_000)),
            8
        );

        // Degenerate durations must not produce a zero-round hold-off, which
        // would be no hold-off at all.
        assert!(stand_aside_rounds_for(Duration::ZERO, Duration::ZERO) >= 1);
        assert!(stand_aside_rounds_for(lease, Duration::from_secs(3_600)) >= 1);
    }

    /// An eligibility refusal — the quorum answered and this node's log is
    /// the problem — requests a stand-aside, so the agent stops winning
    /// acquisition races away from the replica the refusal named. A quorum
    /// MISS does not: standing aside there would delay recovery for a
    /// verdict nobody reached.
    #[tokio::test]
    async fn an_eligibility_refusal_requests_a_stand_aside_and_a_quorum_miss_does_not() {
        let recorder = Arc::new(Recorder::default());
        // Node 1 answers 100 and holds the floor, but node 2 is ahead at
        // 101: one vote of a required two — the §5.4.1 refusal.
        let mut behind = promoter(
            Arc::clone(&recorder) as Arc<dyn LeasePublisher>,
            Some(fixed(vec![at(1, Some(100)), at(2, Some(101))], 3)),
        );
        assert!(!behind.ensure(7).await);
        assert!(
            behind.take_stand_aside(),
            "an eligibility verdict must request a stand-aside, or the refused candidate              keeps winning the race away from the replica it named"
        );
        assert!(
            !behind.take_stand_aside(),
            "one verdict funds one hold-off; reading clears it"
        );

        let mut miss = promoter(
            Arc::clone(&recorder) as Arc<dyn LeasePublisher>,
            Some(fixed(vec![at(1, Some(10)), at(2, None), at(3, None)], 3)),
        );
        assert!(!miss.ensure(8).await);
        assert!(
            !miss.take_stand_aside(),
            "a quorum miss is not an eligibility verdict; standing aside would delay              recovery for nothing"
        );
    }

    /// A quorum miss and an eligibility refusal are told apart, because they
    /// need opposite answers (#375).
    ///
    /// The asymmetry in `stand_aside` was already pinned by the test above;
    /// this pins the other half of it. `publish_held` returned a bare bool, so
    /// the caller treated every refusal alike — stop renewing, let the lease
    /// lapse — and for a quorum miss that lapse is what mints the next epoch
    /// and puts it out of reach of the follower catching up to this one.
    #[tokio::test]
    async fn a_quorum_miss_is_reported_as_one_and_an_eligibility_refusal_is_not() {
        let recorder = Arc::new(Recorder::default());

        let mut miss = promoter(
            Arc::clone(&recorder) as Arc<dyn LeasePublisher>,
            Some(fixed(vec![at(1, Some(10)), at(2, None), at(3, None)], 3)),
        );
        assert!(!miss.ensure(8).await);
        assert!(
            miss.take_quorum_miss(),
            "too few answers must be reported as a quorum miss, or the caller cannot \
             tell it from a verdict that named a better holder"
        );
        assert!(
            !miss.take_quorum_miss(),
            "and reading clears it: one refusal funds one decision"
        );

        // Node 2 is ahead, so this is the §5.4.1 refusal, not a miss.
        let mut behind = promoter(
            Arc::clone(&recorder) as Arc<dyn LeasePublisher>,
            Some(fixed(vec![at(1, Some(100)), at(2, Some(101))], 3)),
        );
        assert!(!behind.ensure(9).await);
        assert!(
            !behind.take_quorum_miss(),
            "an eligibility verdict is not a quorum miss; holding the epoch for it would \
             keep the range away from the replica the refusal named"
        );
    }

    /// Holding an epoch through a quorum miss is bounded, latched, and per
    /// epoch (#375).
    ///
    /// Four properties, and they pull against each other. It must HOLD,
    /// because lapsing mints `fencing_epoch + 1` and a replica refuses a fence
    /// for an epoch it has not observed — so the loop sustains itself. It must
    /// be BOUNDED, because a range that is genuinely unservable has to reach a
    /// survivor eventually. The bound must be measured in the unit it is
    /// stated in — counted in poll rounds it was several times longer than
    /// promised, because a probe against blackholed replicas spends the fence
    /// deadline before the poll interval is applied. And it must LATCH, or a
    /// clock that steps backwards reopens a window that had closed.
    #[test]
    fn a_quorum_miss_hold_is_bounded_latched_and_per_epoch() {
        let hold_for = Duration::from_secs(15);

        let opened = quorum_miss_budget(7, 1_000, None, hold_for);
        assert!(opened.hold, "the first miss on an epoch must hold it");
        assert_eq!(
            opened.state,
            Some(QuorumMissHold::Until {
                epoch: 7,
                until_ms: 16_000
            }),
            "the deadline is the hold measured from now, not a round count"
        );

        let inside = quorum_miss_budget(7, 15_999, opened.state, hold_for);
        assert!(inside.hold, "a millisecond before the deadline still holds");
        assert_eq!(
            inside.state, opened.state,
            "and the deadline does not slide forward on each round, or the hold would \
             never end while rounds kept happening"
        );

        let spent = quorum_miss_budget(7, 16_000, opened.state, hold_for);
        assert!(
            !spent.hold,
            "the budget must run out, or an unservable range never reaches a survivor"
        );
        assert_eq!(
            spent.state,
            Some(QuorumMissHold::Spent { epoch: 7 }),
            "and closing it must be LATCHED rather than recomputed"
        );

        // The clock steps backwards — NTP, or a restored snapshot. A spent
        // window must not reopen, which a bare timestamp comparison would do.
        assert!(
            !quorum_miss_budget(7, 1_000, spent.state, hold_for).hold,
            "a clock that moves backwards must not reopen a window that closed; the \
             decision is a fact, not a measurement to repeat"
        );

        // A single round that itself outlasts the hold exhausts it, which is
        // the whole reason this is wall clock and not a count of rounds.
        let one = quorum_miss_budget(9, 0, None, hold_for);
        assert!(
            !quorum_miss_budget(9, 15_001, one.state, hold_for).hold,
            "one slow round can spend the entire window"
        );

        // A NEW epoch is a new window, even carrying a spent one.
        let fresh = quorum_miss_budget(8, 16_000, spent.state, hold_for);
        assert!(
            fresh.hold,
            "a range that recovered and missed again must get a fresh window; inheriting \
             a spent one would make the second outage unrecoverable for no reason"
        );
        assert_eq!(
            fresh.state,
            Some(QuorumMissHold::Until {
                epoch: 8,
                until_ms: 31_000
            }),
            "measured from the new miss, and following the new epoch"
        );
    }

    /// The recovery half of the transient-refusal story, end to end against a
    /// real fencing view: quorum miss, then quorum back, and the broker must
    /// actually serve again at the SAME epoch.
    #[tokio::test]
    async fn a_transient_quorum_miss_does_not_wedge_the_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let broker = Arc::new(test_broker(dir.path(), 0));
        broker.meta_fencing_epoch().suspend(0);
        let publisher: Arc<dyn LeasePublisher> =
            Arc::new(BrokerLeasePublisher::new(Arc::clone(&broker)));
        let probe = fixed(vec![at(1, Some(10)), at(2, None), at(3, None)], 3);
        let mut promoter = promoter(
            Arc::clone(&publisher),
            Some(Arc::clone(&probe) as Arc<dyn QuorumProbe>),
        );

        assert!(!promoter.ensure(5).await);
        let (_, live) = broker.meta_fencing_epoch().try_snapshot().unwrap();
        assert!(!live, "the refusal must fence the broker");

        // The followers come back before the lease lapses.
        *probe.probes.lock().unwrap() = vec![at(1, Some(10)), at(2, Some(10)), at(3, Some(10))];
        assert!(
            promoter.ensure(5).await,
            "the recovered quorum must verify the same epoch"
        );
        let (epoch, live) = broker.meta_fencing_epoch().try_snapshot().unwrap();
        assert_eq!(epoch, 5);
        assert!(
            live,
            "the same live grant must reactivate the broker; anything else wedges \
             the range under its own lease until an external epoch change"
        );
    }

    /// A candidate whose own log does not reach the quorum-proven boundary
    /// must refuse AND publish that refusal: publishing the boundary instead
    /// would let the produce fast path acknowledge writes into offsets
    /// occupied by committed records this node never held. The refusal is a
    /// SUSPENSION, not a demotion — probes are a snapshot, and a stale
    /// follower answer can transiently place the boundary above this node's
    /// disk; releasing the epoch for that would wedge the range under its own
    /// live lease.
    #[tokio::test]
    async fn a_leader_behind_the_boundary_refuses_and_suspends() {
        let recorder = Arc::new(Recorder::default());
        let mut promoter = promoter(
            Arc::clone(&recorder) as Arc<dyn LeasePublisher>,
            Some(fixed(
                vec![at(1, Some(50)), at(2, Some(90)), at(3, Some(90))],
                3,
            )),
        );
        assert!(!promoter.ensure(5).await);
        assert!(
            recorder.promoted.lock().unwrap().is_empty(),
            "a boundary beyond this node's log must never be published"
        );
        assert_eq!(*recorder.suspended.lock().unwrap(), vec![5]);
        assert!(
            recorder.demoted.lock().unwrap().is_empty(),
            "a possibly-transient refusal must not release the epoch"
        );
    }

    /// The REAL probe, not a double: a follower it cannot fence must come back
    /// absent, so promotion cannot count it.
    ///
    /// The tests above use a `QuorumProbe` double, which means the mapping from
    /// "the fence RPC failed" to "this replica is absent" — the safety property
    /// this whole change exists for — is not exercised by any of them. A future
    /// edit turning a fence failure back into a counted offset would leave them
    /// all green.
    ///
    /// The follower here is an address with nothing listening, which is the
    /// cheapest genuine failure: it exercises the same arm as an older peer, a
    /// refused grant, or a crashed replica. Real TLS material is built because
    /// the client needs it to exist, not because the handshake gets that far.
    #[tokio::test]
    async fn the_real_probe_reports_an_unfenceable_follower_as_absent() {
        let dir = tempfile::tempdir().unwrap();
        let leader_uuid = Uuid::from_u128(1);
        let follower_uuid = Uuid::from_u128(2);
        let broker = Arc::new(test_broker(dir.path(), 5));

        let certified = rcgen::generate_simple_self_signed(vec!["replica".to_owned()]).unwrap();
        let cert = rustls::pki_types::CertificateDer::from(certified.cert.der().to_vec());
        let mut trust_roots = rustls::RootCertStore::empty();
        trust_roots.add(cert.clone()).unwrap();
        let client = vtop_broker::replication::ReplicaStatusClient::new(
            vtop_broker::replication::ReplicaTlsMaterial {
                certificate_chain: vec![cert],
                private_key: rustls::pki_types::PrivatePkcs8KeyDer::from(
                    certified.signing_key.serialize_der(),
                )
                .into(),
                trust_roots,
            },
        )
        .unwrap()
        .with_timeout(std::time::Duration::from_millis(200));

        // Port 1 on loopback: reserved, and nothing this test could race with.
        let unreachable = "127.0.0.1:1".parse().unwrap();
        let probe = ReplicaPlaneProbe::new(
            Arc::clone(&broker) as Arc<dyn CandidateLocalView>,
            leader_uuid,
            client,
            vec![FollowerEndpoint {
                node_uuid: follower_uuid,
                addr: unreachable,
                // A literal address, so the probe uses it verbatim: this test
                // is about an unreachable follower, not about resolution.
                host: None,
                server_name: "replica".to_owned(),
            }],
            broker.range().clone(),
        );

        let probes = probe.probe(6).await;
        assert_eq!(probes.len(), 2, "the leader plus its one follower");
        let follower = probes
            .iter()
            .find(|entry| entry.node_id == follower_uuid)
            .expect("the follower must still appear, as absent");
        assert_eq!(
            follower.local_committed_offset, None,
            "a replica that could not be fenced must be absent from the quorum, \
             not counted at whatever offset it last reported"
        );
        // And the leader still counts itself: it holds the epoch by construction.
        let leader = probes
            .iter()
            .find(|entry| entry.node_id == leader_uuid)
            .expect("the leader probes its own disk");
        assert!(leader.local_committed_offset.is_some());
    }

    /// The probe must fence at the epoch being promoted, not some other one.
    ///
    /// A fence taken at the wrong epoch stops nothing that matters: the deposed
    /// leader writes under the epoch BELOW the one being granted, so fencing at
    /// anything else leaves it free to keep appending while the new leader
    /// measures. The value is easy to thread wrongly and impossible to notice
    /// from the outside, so it is asserted rather than assumed.
    #[tokio::test]
    async fn the_probe_fences_at_the_epoch_being_promoted() {
        let recorder = Arc::new(Recorder::default());
        let probe = fixed(vec![at(1, Some(10)), at(2, Some(10))], 2);
        let mut promoter = promoter(
            Arc::clone(&recorder) as Arc<dyn LeasePublisher>,
            Some(Arc::clone(&probe) as Arc<dyn QuorumProbe>),
        );

        assert!(promoter.ensure(19).await);
        assert!(promoter.ensure(20).await);

        assert_eq!(
            *probe.fenced_at.lock().unwrap(),
            vec![19, 20],
            "each promotion must fence at its own epoch"
        );
    }

    /// A replica that could not be fenced does not count toward the quorum.
    ///
    /// This is the safety property the fence exists for. Its offset is a
    /// measurement of something that may still be moving — the deposed leader
    /// can be appending to it right now — so counting it would let promotion
    /// establish a boundary on a moving target, which is what the probe was
    /// doing before.
    #[tokio::test]
    async fn a_replica_that_could_not_be_fenced_is_absent_from_the_quorum() {
        let recorder = Arc::new(Recorder::default());
        // Three replicas, majority 2. The leader is fenced by construction; one
        // follower refused the fence and reports absent.
        let mut promoter = promoter(
            Arc::clone(&recorder) as Arc<dyn LeasePublisher>,
            Some(fixed(vec![at(1, Some(90)), at(2, None), at(3, None)], 3)),
        );

        assert!(
            !promoter.ensure(19).await,
            "one fenced replica out of three cannot establish a boundary, \
             however high the unfenced ones claim to be"
        );
    }

    /// Verification is once per epoch TRANSITION. Re-probing on every renewal
    /// is pure cost for a leader that has not changed — and it is what made the
    /// promotion log line fire every few seconds for the life of the process.
    #[tokio::test]
    async fn a_held_epoch_is_verified_once_not_on_every_renewal() {
        let recorder = Arc::new(Recorder::default());
        let probe = fixed(vec![at(1, Some(10)), at(2, Some(10))], 2);
        let mut promoter = promoter(
            Arc::clone(&recorder) as Arc<dyn LeasePublisher>,
            Some(Arc::clone(&probe) as Arc<dyn QuorumProbe>),
        );

        for _ in 0..5 {
            assert!(promoter.ensure(7).await);
        }
        assert_eq!(
            probe.calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "one epoch, one verification"
        );

        // A NEW epoch is a different claim and must be verified again.
        assert!(promoter.ensure(8).await);
        assert_eq!(probe.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    /// The established boundary must reach the broker, or verification was
    /// theatre: the whole point is that fetch stops hiding acknowledged
    /// records after a failover.
    #[tokio::test]
    async fn the_established_boundary_is_published_with_the_epoch() {
        let recorder = Arc::new(Recorder::default());
        let mut promoter = promoter(
            Arc::clone(&recorder) as Arc<dyn LeasePublisher>,
            Some(fixed(
                vec![at(1, Some(90)), at(2, Some(90)), at(3, Some(50))],
                3,
            )),
        );
        assert!(promoter.ensure(5).await);
        assert_eq!(*recorder.promoted.lock().unwrap(), vec![(5, Some(90))]);
    }

    /// A standalone range has no quorum to establish against and must still
    /// promote; requiring one it cannot form would make single-replica
    /// deployments unleadable.
    #[tokio::test]
    async fn a_standalone_range_promotes_without_a_probe() {
        let recorder = Arc::new(Recorder::default());
        let mut promoter = promoter(Arc::clone(&recorder) as Arc<dyn LeasePublisher>, None);
        assert!(promoter.ensure(5).await);
        assert_eq!(*recorder.promoted.lock().unwrap(), vec![(5, None)]);
    }

    #[test]
    fn a_renew_interval_at_or_past_the_lease_is_rejected() {
        let config = LeaseAgentConfig {
            lease_duration: Duration::from_secs(10),
            renew_interval: Duration::from_secs(10),
            poll_interval: Duration::from_secs(1),
        };
        let error = config.validate().unwrap_err();
        assert!(
            error.contains("must be shorter than"),
            "a renew interval that never beats the deadline loses the range every \
             time: {error}"
        );
    }

    #[test]
    fn the_default_pacing_leaves_room_for_a_failed_renewal() {
        let config = LeaseAgentConfig::default();
        config.validate().unwrap();
        assert!(
            config.renew_interval * 2 < config.lease_duration,
            "two consecutive renewals must be able to fail without losing the range"
        );
    }
}

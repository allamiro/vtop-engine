//! Applied audit records, delivered at least once (#706).
//!
//! Every replica writes one applied record per audited entry it applies
//! (#694). Before #706 the record was handed to the sink after
//! `meta.applied` was saved and nothing re-derived it: a crash between the
//! two, a sink that refused it, or a snapshot install left the replica
//! without it, silently.
//!
//! Each replica now keeps an audit frontier, `meta.audited`: every record it
//! owes at or below the frontier has been accepted by its sink. The
//! frontier is saved only after delivery, so a crash re-delivers rather than
//! loses. What lies above it is delivered again from the log, marked
//! `replayed`:
//!
//! - at the next apply, when the frontier is behind the batch;
//! - every [`crate::audit::AUDIT_RETRY_INTERVAL`] by the node's retry task;
//! - when the node starts, before Raft does.
//!
//! What the log no longer holds cannot be re-derived on this replica: one
//! `audit.gap` record names the range instead (a snapshot install, or a log
//! purged past a frontier that fell behind while its sink was failing).
//!
//! # Where it runs (#706 decision 10)
//!
//! Here, in the raft adapter, not in `vtop-node`: the adapter owns the log,
//! the applied frontier and the apply batches, and the replay at start must
//! run before Raft serves - which only `start_meta_node` sees. The node
//! process only installs the sink. `vtop-meta` takes no metrics dependency:
//! [`AuditProgress`] counts, and the node exports the counts.
//!
//! # Locks
//!
//! The delivery lock is taken BEFORE the store lock, everywhere, and held
//! across the sink call: a delivery and the retry task must not interleave
//! (two deliverers would write one record twice and race the frontier). The
//! store lock is never held across the sink call, which may fsync.

use crate::audit::{AuditRecord, Auditor, GapReason};
use crate::command::{MetadataCommand, MetadataResponse};
use crate::raft::store::MetaRaftStore;
use crate::storage::log::{MetaLogPayload, MetaMembership};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// Save the frontier after a batch that delivered no record only once it
/// lags the applied frontier by this many entries. Openraft keeps 1000
/// entries below a snapshot, so a frontier within 512 of `applied` is always
/// inside the retained log, and a restart can re-derive what it covers.
pub(crate) const AUDIT_SAVE_LAG: u64 = 512;

/// The most log entries one catch-up reads and delivers before it lets go of
/// the locks (review of #767): a sink that recovers after a long outage must
/// not hold apply for the whole backlog, and an apply must not re-read it.
pub(crate) const AUDIT_CATCH_UP_CHUNK: u64 = 256;

/// What one bounded catch-up step left.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CatchUp {
    /// The frontier reached the applied frontier.
    Done,
    /// More to deliver: call again (after letting go of the locks).
    More,
    /// The sink refused, or the log could not be read: try later.
    Stopped,
}

/// Why records were delivered again. Closed: the `reason` label of
/// `vtop_audit_replayed_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayReason {
    /// At node start, from the log.
    Restart,
    /// After a sink refused a record: at the next apply, or by the retry task.
    Retry,
    /// From the attributed tail a snapshot brought (feature level 9).
    Snapshot,
}

impl ReplayReason {
    pub fn as_str(self) -> &'static str {
        match self {
            ReplayReason::Restart => "restart",
            ReplayReason::Retry => "retry",
            ReplayReason::Snapshot => "snapshot",
        }
    }
}

/// What a node exports about its audit delivery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AuditProgressSnapshot {
    /// Records delivered again, by [`ReplayReason`]: restart, retry, snapshot.
    pub replayed: [u64; 3],
    /// `audit.gap` records written, by [`GapReason`]: snapshot install, log
    /// purged, tail evicted.
    pub gaps: [u64; 3],
    /// The frontier in memory: every record at or below it was accepted.
    pub delivered_index: u64,
    /// The applied frontier, as of the last delivery.
    pub applied_index: u64,
}

impl AuditProgressSnapshot {
    /// Applied entries whose records are not yet delivered: above 0 for long
    /// means the sink is failing.
    pub fn lag(&self) -> u64 {
        self.applied_index.saturating_sub(self.delivered_index)
    }
}

/// Counters a node exports; see [`AuditProgressSnapshot`].
#[derive(Debug, Default)]
pub struct AuditProgress {
    replayed: [AtomicU64; 3],
    gaps: [AtomicU64; 3],
    delivered: AtomicU64,
    applied: AtomicU64,
}

impl AuditProgress {
    pub fn snapshot(&self) -> AuditProgressSnapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        AuditProgressSnapshot {
            replayed: [
                load(&self.replayed[0]),
                load(&self.replayed[1]),
                load(&self.replayed[2]),
            ],
            gaps: [
                load(&self.gaps[0]),
                load(&self.gaps[1]),
                load(&self.gaps[2]),
            ],
            delivered_index: load(&self.delivered),
            applied_index: load(&self.applied),
        }
    }

    fn count_replayed(&self, reason: ReplayReason, count: u64) {
        let slot = match reason {
            ReplayReason::Restart => 0,
            ReplayReason::Retry => 1,
            ReplayReason::Snapshot => 2,
        };
        self.replayed[slot].fetch_add(count, Ordering::Relaxed);
    }

    fn count_gap(&self, reason: GapReason) {
        let slot = match reason {
            GapReason::SnapshotInstall => 0,
            GapReason::LogPurged => 1,
            GapReason::TailEvicted => 2,
        };
        self.gaps[slot].fetch_add(1, Ordering::Relaxed);
    }

    /// The delivered frontier, readable without the delivery lock (a
    /// snapshot build saves it under the store lock alone).
    pub(crate) fn delivered(&self) -> u64 {
        self.delivered.load(Ordering::Relaxed)
    }
}

/// The delivery cursor; behind [`MetaRaftStore`]'s delivery lock.
pub(crate) struct DeliveryState {
    /// This replica, named in its records.
    pub(crate) node_id: Option<u64>,
    /// The sink; `None` is the process's installed auditor.
    pub(crate) sink: Option<Arc<dyn Auditor>>,
    /// Every record this replica owes at or below this index was accepted.
    pub(crate) delivered: u64,
    /// A snapshot's gap record a sink refused, and the snapshot index it
    /// covers: delivered first next time, and only then does the frontier
    /// move to that index. In memory only on purpose: the frontier was not
    /// moved, so a restart re-derives the same gap from the log (#767
    /// review).
    pending_gap: Option<(AuditRecord, u64)>,
    /// When the sink last refused. An apply does not try again before
    /// [`crate::audit::AUDIT_RETRY_INTERVAL`] has passed, so a sink that
    /// stays down costs each apply nothing (review of #767).
    refused_at: Option<std::time::Instant>,
}

impl DeliveryState {
    pub(crate) fn new(delivered: u64) -> Self {
        Self {
            node_id: None,
            sink: None,
            delivered,
            pending_gap: None,
            refused_at: None,
        }
    }

    fn send(&self, record: &AuditRecord) -> Result<(), String> {
        match &self.sink {
            Some(sink) => sink.record(record),
            None => crate::audit::deliver(record),
        }
    }
}

/// The applied record of `command` at `index`/`term` on `replica`, or `None`
/// when the command is not audited. `response` is what the state machine
/// answered; `None` when it is no longer held (a replay whose request the
/// dedup table has evicted).
pub(crate) fn applied_command_record(
    replica: Option<u64>,
    index: u64,
    term: u64,
    command: &MetadataCommand,
    response: Option<&MetadataResponse>,
) -> Option<AuditRecord> {
    let audit = crate::audit::command_audit(command)?;
    let mut record = AuditRecord::for_command(
        crate::audit::AuditStage::Applied,
        crate::audit::AuditActor::Replica(replica),
        command,
        audit,
        crate::audit::AuditOutcome::Success,
    );
    record.log_index = Some(index);
    record.log_term = Some(term);
    record.issued_at_ms = Some(command.envelope().issued_at_ms);
    match response {
        Some(MetadataResponse::Rejected(error)) => {
            record.outcome = crate::audit::AuditOutcome::Failure;
            record.reason = Some(format!(
                "rejected by the state machine at index {index}: {error}"
            ));
        }
        Some(_) => {}
        None => {
            record.outcome = crate::audit::AuditOutcome::Failure;
            record.reason = Some(format!(
                "outcome unknown on re-delivery: the state machine's answer for index {index} is \
                 no longer held here; the other replicas' records and the edge record say"
            ));
        }
    }
    Some(record)
}

/// The applied record of a membership entry at `index`/`term`.
pub(crate) fn applied_membership_record(
    replica: Option<u64>,
    index: u64,
    term: u64,
    membership: &MetaMembership,
) -> AuditRecord {
    let voters = membership
        .voters
        .iter()
        .map(|id| id.0.to_string())
        .collect::<Vec<_>>()
        .join(",");
    AuditRecord {
        stage: crate::audit::AuditStage::Applied,
        action: crate::audit::AuditAction::ClusterMembership,
        actor: crate::audit::AuditActor::Replica(replica),
        target_kind: "cluster",
        target_id: None,
        params: vec![("voters", voters)],
        outcome: crate::audit::AuditOutcome::Success,
        reason: None,
        request_id: None,
        log_index: Some(index),
        log_term: Some(term),
        issued_at_ms: None,
        replayed: false,
    }
}

impl MetaRaftStore {
    pub(crate) fn lock_delivery(&self) -> MutexGuard<'_, DeliveryState> {
        self.delivery
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Deliver records through this sink instead of the process's auditor.
    /// For tests that need a sink of their own, or one that fails.
    pub fn set_audit_sink(&self, sink: Arc<dyn Auditor>) {
        self.lock_delivery().sink = Some(sink);
    }

    /// Name this replica in its records.
    pub(crate) fn set_audit_node_id(&self, node_id: u64) {
        self.lock_delivery().node_id = Some(node_id);
    }

    /// What this replica's audit delivery has done, for its metrics.
    pub fn audit_progress(&self) -> AuditProgressSnapshot {
        self.progress.snapshot()
    }

    /// The live records of an apply batch `first..=through`, just applied.
    /// Delivered in order unless the frontier is behind the batch. Then one
    /// bounded step of the backlog is delivered from the log instead - and
    /// not even that while the sink refused within the retry interval: the
    /// retry task and the next applies catch up, a chunk at a time.
    pub(crate) fn deliver_applied(
        &self,
        delivery: &mut DeliveryState,
        first: u64,
        through: u64,
        records: Vec<AuditRecord>,
    ) {
        self.progress.applied.store(through, Ordering::Relaxed);
        if delivery.pending_gap.is_some() || delivery.delivered.saturating_add(1) < first {
            if delivery
                .refused_at
                .is_some_and(|at| at.elapsed() < crate::audit::AUDIT_RETRY_INTERVAL)
            {
                return;
            }
            self.catch_up(delivery, ReplayReason::Retry);
            return;
        }
        let delivered_any = !records.is_empty();
        for record in records {
            let Some(index) = record.log_index else {
                continue;
            };
            if index <= delivery.delivered {
                continue;
            }
            if let Err(error) = delivery.send(&record) {
                delivery.delivered = delivery.delivered.max(index - 1);
                self.sink_failed(delivery, index, &error);
                self.save_frontier(delivery, true);
                return;
            }
            delivery.delivered = index;
        }
        delivery.delivered = delivery.delivered.max(through);
        self.save_frontier(delivery, delivered_any);
    }

    /// Deliver everything above the frontier: at start, and from the retry
    /// task. A chunk at a time, letting go of the locks between chunks so
    /// apply goes on. Returns whether the frontier reached the applied
    /// frontier.
    pub fn deliver_pending(&self, reason: ReplayReason) -> bool {
        loop {
            let mut delivery = self.lock_delivery();
            match self.catch_up(&mut delivery, reason) {
                CatchUp::Done => return true,
                CatchUp::Stopped => return false,
                CatchUp::More => {}
            }
        }
    }

    /// A snapshot install moved this replica to `snapshot_index` without
    /// applying the entries below it one by one: their records cannot be
    /// re-derived here. One gap record says so, and once the sink accepts
    /// it the frontier moves to the snapshot.
    ///
    /// If the sink refuses it, the frontier stays where it was (#767
    /// review): moving it would lose the gap at the next restart, since the
    /// pending record is in memory only. The record is delivered first next
    /// time, which then moves the frontier. A restart before that re-derives
    /// the same range as a gap from the log: `readable_from` never reads
    /// below a snapshot the log does not agree with, so the stale branch the
    /// snapshot replaced is never read either way.
    pub(crate) fn snapshot_installed(&self, delivery: &mut DeliveryState, snapshot_index: u64) {
        if snapshot_index <= delivery.delivered {
            return;
        }
        // A newer snapshot's gap starts at the same frontier and covers an
        // older one still owed (review of #767: it was sent twice).
        delivery.pending_gap = None;
        // The boundary of what this replica never applied one by one, kept
        // durably before anything else (Codex review of #767): a refused gap
        // lives in memory only, and a later local snapshot can make a stale
        // branch below it look like the committed log.
        if let Err(error) = self.lock().storage.save_audited_floor(snapshot_index) {
            crate::transport::warn_rate_limited(
                "audit-floor-save",
                &format!(
                    "could not save the audit install floor at {snapshot_index} ({error}); a \
                     restart may re-derive the range from the log's newest snapshot instead"
                ),
            );
        }
        let from = delivery.delivered + 1;
        let gap = AuditRecord::gap(
            delivery.node_id,
            from,
            snapshot_index,
            GapReason::SnapshotInstall,
        );
        eprintln!(
            "warning: meta node {} installed a snapshot covering log indexes {from}..={snapshot_index}: \
             their applied audit records cannot be re-derived on this node (record audit.gap). \
             Other nodes' audit files hold them: copy them and dedupe on log index",
            describe_node(delivery.node_id)
        );
        self.progress.count_gap(GapReason::SnapshotInstall);
        match delivery.send(&gap) {
            Ok(()) => delivery.delivered = snapshot_index,
            Err(error) => {
                self.sink_failed(delivery, from, &error);
                delivery.pending_gap = Some((gap, snapshot_index));
            }
        }
        self.save_frontier(delivery, true);
    }

    /// Deliver up to [`AUDIT_CATCH_UP_CHUNK`] entries above the frontier
    /// from the log, in order, marked `replayed`. A range the log no longer
    /// holds gets one gap record.
    fn catch_up(&self, delivery: &mut DeliveryState, reason: ReplayReason) -> CatchUp {
        if let Some((gap, through)) = delivery.pending_gap.take() {
            if let Err(error) = delivery.send(&gap) {
                let from = delivery.delivered + 1;
                self.sink_failed(delivery, from, &error);
                delivery.pending_gap = Some((gap, through));
                return CatchUp::Stopped;
            }
            delivery.delivered = delivery.delivered.max(through);
        }
        let (applied, chunk_end, gap, records) = {
            let guard = self.lock();
            let storage = &guard.storage;
            let applied = storage.last_applied();
            if delivery.delivered >= applied {
                drop(guard);
                // A refused gap delivered just now moved the frontier.
                self.save_frontier(delivery, true);
                return CatchUp::Done;
            }
            let from = delivery.delivered + 1;
            let (readable, cut_by) = readable_from(storage, from, applied);
            let gap = (readable > from).then(|| (from, (readable - 1).min(applied), cut_by));
            let chunk_end = applied.min(readable.saturating_add(AUDIT_CATCH_UP_CHUNK - 1));
            let mut records = Vec::new();
            if readable <= chunk_end {
                match storage.log().read_range(readable, chunk_end + 1) {
                    Ok(entries) => {
                        for entry in entries {
                            let record = match &entry.payload {
                                MetaLogPayload::Normal(command) => applied_command_record(
                                    delivery.node_id,
                                    entry.index,
                                    entry.term,
                                    command,
                                    storage
                                        .state()
                                        .applied_response(command.envelope().request_id),
                                ),
                                MetaLogPayload::Membership(membership) => {
                                    Some(applied_membership_record(
                                        delivery.node_id,
                                        entry.index,
                                        entry.term,
                                        membership,
                                    ))
                                }
                                MetaLogPayload::Blank => None,
                            };
                            if let Some(mut record) = record {
                                record.replayed = true;
                                records.push(record);
                            }
                        }
                    }
                    Err(error) => {
                        crate::transport::warn_rate_limited(
                            "audit-replay-read",
                            &format!(
                                "could not read log indexes {readable}..={chunk_end} to deliver \
                                 their applied audit records ({error}); the node keeps serving \
                                 and tries again within {}s",
                                crate::audit::AUDIT_RETRY_INTERVAL.as_secs()
                            ),
                        );
                        return CatchUp::Stopped;
                    }
                }
            }
            (
                applied,
                chunk_end.max(readable.saturating_sub(1)),
                gap,
                records,
            )
        };
        self.progress.applied.store(applied, Ordering::Relaxed);
        if let Some((from, through, cut_by)) = gap {
            let record = AuditRecord::gap(delivery.node_id, from, through, cut_by);
            if let Err(error) = delivery.send(&record) {
                self.sink_failed(delivery, from, &error);
                return CatchUp::Stopped;
            }
            eprintln!(
                "warning: meta node {} could not re-derive the applied audit records for log \
                 indexes {from}..={through} (record audit.gap, {}). Other nodes' audit files \
                 hold them: copy them and dedupe on log index",
                describe_node(delivery.node_id),
                cut_by.as_str(),
            );
            self.progress.count_gap(cut_by);
            delivery.delivered = through;
        }
        let mut replayed = 0_u64;
        let (mut first, mut last) = (None, None);
        for record in records {
            let index = record.log_index.expect("built with an index");
            if index <= delivery.delivered {
                continue;
            }
            if let Err(error) = delivery.send(&record) {
                delivery.delivered = delivery.delivered.max(index - 1);
                self.count_and_report_replay(delivery, reason, replayed, first, last);
                self.sink_failed(delivery, index, &error);
                self.save_frontier(delivery, true);
                return CatchUp::Stopped;
            }
            replayed += 1;
            first.get_or_insert(index);
            last = Some(index);
            delivery.delivered = index;
        }
        delivery.delivered = delivery.delivered.max(chunk_end);
        delivery.refused_at = None;
        self.count_and_report_replay(delivery, reason, replayed, first, last);
        self.save_frontier(delivery, true);
        if delivery.delivered >= applied {
            CatchUp::Done
        } else {
            CatchUp::More
        }
    }

    fn count_and_report_replay(
        &self,
        delivery: &DeliveryState,
        reason: ReplayReason,
        count: u64,
        first: Option<u64>,
        last: Option<u64>,
    ) {
        if count == 0 {
            return;
        }
        self.progress.count_replayed(reason, count);
        let (first, last) = (first.unwrap_or(0), last.unwrap_or(0));
        let why = match reason {
            ReplayReason::Restart => "that a crash or a stop had left unwritten",
            ReplayReason::Retry => "that its audit sink had refused",
            ReplayReason::Snapshot => "from the attributed tail of an installed snapshot",
        };
        eprintln!(
            "meta node {} delivered {count} applied audit records for log indexes \
             {first}..={last} {why} (marked replayed). Readers dedupe on engine and log index",
            describe_node(delivery.node_id)
        );
    }

    fn sink_failed(&self, delivery: &mut DeliveryState, index: u64, error: &str) {
        delivery.refused_at = Some(std::time::Instant::now());
        let applied = self.progress.applied.load(Ordering::Relaxed);
        crate::transport::warn_rate_limited(
            "audit-sink-refused",
            &format!(
                "the audit sink refused the applied record for log index {index} ({error}). The \
                 node keeps serving and delivers it again at the next apply, within {}s, and at \
                 restart (delivered through {}, applied {applied}). Fix the audit sink: disk \
                 space and permissions of the file VTOP_AUDIT_LOG names",
                crate::audit::AUDIT_RETRY_INTERVAL.as_secs(),
                delivery.delivered,
            ),
        );
    }

    /// Publish the frontier, and persist it when `force` or when it lags the
    /// applied frontier by [`AUDIT_SAVE_LAG`] or more.
    fn save_frontier(&self, delivery: &DeliveryState, force: bool) {
        let mut guard = self.lock();
        let applied = guard.storage.last_applied();
        self.progress.applied.store(applied, Ordering::Relaxed);
        self.progress
            .delivered
            .store(delivery.delivered, Ordering::Relaxed);
        let persisted = guard.storage.audited_frontier().unwrap_or(0);
        if delivery.delivered <= persisted {
            return;
        }
        if !force && applied.saturating_sub(persisted) < AUDIT_SAVE_LAG {
            return;
        }
        let target = delivery.delivered.min(applied);
        if let Err(error) = guard.storage.save_audited(target) {
            crate::transport::warn_rate_limited(
                "audit-frontier-save",
                &format!(
                    "could not save the audit frontier at {target} ({error}); records above the \
                     last saved one are delivered again at restart, marked replayed"
                ),
            );
        }
    }
}

/// The first index at or above `from` whose entry this replica can read as
/// the committed one, and what cut the range below it. Below the log's
/// first entry nothing can be read (`LogPurged`). Below the newest snapshot
/// only when the log agrees with it at the snapshot's index (Raft's log
/// matching: the same index and term mean the same prefix); otherwise the
/// log below it was replaced by an installed snapshot and may be a stale
/// branch this replica never applied (`SnapshotInstall`).
fn readable_from(
    storage: &crate::storage::MetaStorage,
    from: u64,
    applied: u64,
) -> (u64, GapReason) {
    let newest = storage.snapshots().newest();
    let installed_over = |cut_at: u64| {
        newest
            .as_ref()
            .is_some_and(|snapshot| snapshot.last_index >= cut_at.saturating_sub(1))
    };
    let Some(first_in_log) = storage.log().first_index() else {
        let reason = if installed_over(applied + 1) {
            GapReason::SnapshotInstall
        } else {
            GapReason::LogPurged
        };
        return (applied + 1, reason);
    };
    let mut readable = from.max(first_in_log);
    let mut reason = GapReason::LogPurged;
    // Never at or below what a snapshot install brought this replica past.
    let floor = storage.audited_floor();
    if readable <= floor {
        readable = floor + 1;
        reason = GapReason::SnapshotInstall;
    }
    if let Some(snapshot) = &newest {
        if readable <= snapshot.last_index {
            let agrees = storage
                .log()
                .read_range(snapshot.last_index, snapshot.last_index + 1)
                .ok()
                .and_then(|entries| entries.into_iter().next())
                .is_some_and(|entry| entry.term == snapshot.last_term);
            if !agrees {
                readable = snapshot.last_index + 1;
                reason = GapReason::SnapshotInstall;
            }
        }
    }
    (readable, reason)
}

fn describe_node(node_id: Option<u64>) -> String {
    node_id.map_or_else(|| "?".to_owned(), |id| id.to_string())
}

/// The shared delivery state a store is built with.
pub(crate) fn new_delivery(delivered: u64) -> (Arc<Mutex<DeliveryState>>, Arc<AuditProgress>) {
    let progress = AuditProgress::default();
    progress.delivered.store(delivered, Ordering::Relaxed);
    (
        Arc::new(Mutex::new(DeliveryState::new(delivered))),
        Arc::new(progress),
    )
}

//! Native VTOP broker with a bounded TLS transport.
//!
//! Single-node produce acknowledgements use the local `Fsync` durability
//! boundary. When configured with a [`replication::ReplicaSet`], `Quorum`
//! produce waits for a majority of locally durable replica acks, advances the
//! [`replication::ClusterCommittedOffset`], and propagates that high-water mark
//! so fetch never exposes records above the quorum-committed point.
//!
//! When [`group_commit::GroupCommitConfig`] is attached, concurrent producer
//! sessions share one append + local durability / quorum barrier per sealed
//! commit group instead of fsyncing and replicating per request.
//!
//! [`memory_budget::MemoryBudgetPool`] enforces explicit byte ceilings for
//! produce, fetch-response queues, and (with networked replicas) follower
//! inflight / catch-up buffers. Overload fails closed with retryable
//! `Overloaded` — never by silently dropping accepted records.
//!
//! Range leadership is gated by a metadata-issued lease view
//! ([`MetaFencingEpoch`]): the broker holds the epoch it was granted and, on
//! every produce/fetch, locks that shared view while validating and mutating
//! storage. Observers publish grants and releases into the same handle; a
//! newer grant or a release fences the prior leaseholder before the next
//! durable append can complete.
//!
//! Stage-7 consumer checkpoints use an optional shared [`GroupCheckpointStore`]
//! backed by the deterministic metadata state machine. Commit/fetch cursor
//! requests validate lineage against that store; membership join/leave/assign
//! remain metadata commands (Raft-proposed in later wiring).

pub mod fencing_epochs;
pub mod group_commit;
pub mod memory_budget;
pub mod replication;
pub mod server_metrics;

use crate::group_commit::{GroupCommitConfig, GroupCommitCoordinator, QueuedProduce};
use crate::memory_budget::{reject_message, BudgetReservation, ConnectionBudget};
use crate::replication::{ClusterCommittedOffset, ReplicaSet};

pub use crate::group_commit::{FlushReason, GroupCommitMetrics, GroupCommitSample};
pub use crate::memory_budget::{
    BudgetRejectReason, MemoryBudgetConfig, MemoryBudgetMetrics, MemoryBudgetPool, OverloadAction,
};
pub use crate::server_metrics::{
    LatencySnapshot, RequestKind, RequestOutcome, ServerMetrics, LATENCY_BUCKETS_MICROS,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Semaphore};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;
use uuid::Uuid;
use vtop_log::env::{Env, OpenMode, Storage, StorageFile};
use vtop_log::{AppendOutcome, Durability, LogRecord, SegmentSet};
use vtop_meta::{
    CommandEnvelope, MetaKey, MetaStateMachine, MetaValue, MetadataCommand, MetadataError,
    MetadataResponse,
};
use vtop_protocol::{
    encode_frame, read_frame, write_frame, ClientHello, CommitCursorResponse, CommittedHwmUpdate,
    Durability as WireDurability, ErrorCode, ErrorResponse, FetchCursorResponse, FetchResponse,
    FetchedRecord, LineageCursor, Message, ProduceOutcome, ProduceResponse, ProtocolLimits,
    RangeIdentity, ReplicaAppendRequest, Role, ServerHello, WireFrame, ABSOLUTE_MAX_FRAME_BYTES,
    ABSOLUTE_MAX_RECORDS, DEFAULT_MAX_FRAME_BYTES, DEFAULT_MAX_RECORDS, PROTOCOL_MAJOR,
    PROTOCOL_MINOR,
};

const EPOCH_MAGIC: &[u8; 8] = b"VTOPEPC1";
const EPOCH_VERSION: u16 = 1;
const EPOCH_HEADER_BYTES: u64 = 10;
const EPOCH_ENTRY_BYTES: u64 = 16 + 8 + 32;
const EPOCH_DOMAIN: &[u8] = b"vtop-producer-epoch-v1\0";
const MAX_EPOCH_JOURNAL_BYTES: u64 = 64 * 1024 * 1024;
const MAX_WINDOW_BYTES: u64 = vtop_protocol::MAX_WINDOW_BYTES as u64;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("invalid broker configuration: {0}")]
    InvalidConfig(String),
    #[error("I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("producer epoch journal is corrupt: {0}")]
    EpochJournalCorrupt(String),
    #[error("producer {producer_id} epoch {actual} is fenced by durable epoch {current}")]
    ProducerFenced {
        producer_id: Uuid,
        current: u64,
        actual: u64,
    },
    #[error(
        "refusing to truncate to offset {requested}: records below the cluster high-water mark {high_watermark} were acknowledged to producers"
    )]
    TruncationBelowAcknowledged { requested: u64, high_watermark: u64 },
    #[error(
        "refusing to truncate an unreplicated broker: every durable record here is acknowledged"
    )]
    TruncationWithoutReplication,
    #[error("TLS configuration error: {0}")]
    Tls(#[from] rustls::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] vtop_protocol::ProtocolError),
    #[error("server task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("{0} timed out")]
    Timeout(&'static str),
    #[error("segment storage error: {0}")]
    Segment(#[from] vtop_log::LogError),
}

pub type BrokerResult<T> = Result<T, BrokerError>;

pub struct ProducerEpochJournal {
    path: PathBuf,
    file: Box<dyn StorageFile>,
    current: HashMap<Uuid, u64>,
    poisoned: bool,
}

impl ProducerEpochJournal {
    pub fn open(path: impl AsRef<Path>) -> BrokerResult<Self> {
        Self::open_in(&Env::real(), path)
    }

    pub fn open_in(env: &Env, path: impl AsRef<Path>) -> BrokerResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = env
            .storage
            .open(&path, OpenMode::CreateAppend)
            .map_err(|source| io_error(&path, source))?;
        let mut len = file.len().map_err(|source| io_error(&path, source))?;
        if len > MAX_EPOCH_JOURNAL_BYTES {
            return Err(BrokerError::EpochJournalCorrupt(format!(
                "journal is {len} bytes; maximum is {MAX_EPOCH_JOURNAL_BYTES}"
            )));
        }
        if len == 0 {
            file.write_all(EPOCH_MAGIC)
                .and_then(|()| file.write_all(&EPOCH_VERSION.to_be_bytes()))
                .and_then(|()| file.sync_data())
                .map_err(|source| io_error(&path, source))?;
            sync_parent(env.storage.as_ref(), &path)?;
            len = EPOCH_HEADER_BYTES;
        }
        if len < EPOCH_HEADER_BYTES {
            return Err(BrokerError::EpochJournalCorrupt(
                "truncated journal header".to_owned(),
            ));
        }
        file.seek(SeekFrom::Start(0))
            .map_err(|source| io_error(&path, source))?;
        let mut header = [0_u8; EPOCH_HEADER_BYTES as usize];
        file.read_exact(&mut header)
            .map_err(|source| io_error(&path, source))?;
        if &header[..8] != EPOCH_MAGIC {
            return Err(BrokerError::EpochJournalCorrupt(
                "journal magic mismatch".to_owned(),
            ));
        }
        let version = u16::from_be_bytes(header[8..].try_into().expect("two bytes"));
        if version != EPOCH_VERSION {
            return Err(BrokerError::EpochJournalCorrupt(format!(
                "unsupported journal version {version}"
            )));
        }

        let payload_len = len - EPOCH_HEADER_BYTES;
        if !payload_len.is_multiple_of(EPOCH_ENTRY_BYTES) {
            return Err(BrokerError::EpochJournalCorrupt(format!(
                "journal has a torn epoch entry at byte {}",
                EPOCH_HEADER_BYTES + payload_len - (payload_len % EPOCH_ENTRY_BYTES)
            )));
        }
        let mut current = HashMap::new();
        let mut entry = [0_u8; EPOCH_ENTRY_BYTES as usize];
        let entries = payload_len / EPOCH_ENTRY_BYTES;
        for index in 0..entries {
            file.read_exact(&mut entry)
                .map_err(|source| io_error(&path, source))?;
            let producer_id = Uuid::from_slice(&entry[..16]).map_err(|error| {
                BrokerError::EpochJournalCorrupt(format!("entry {index} UUID: {error}"))
            })?;
            let epoch = u64::from_be_bytes(entry[16..24].try_into().expect("eight bytes"));
            let expected = epoch_checksum(producer_id, epoch);
            if expected.as_bytes() != &entry[24..] {
                return Err(BrokerError::EpochJournalCorrupt(format!(
                    "entry {index} checksum mismatch"
                )));
            }
            let previous = current.insert(producer_id, epoch);
            if previous.is_some_and(|value| epoch <= value) {
                return Err(BrokerError::EpochJournalCorrupt(format!(
                    "entry {index} does not advance producer {producer_id}"
                )));
            }
        }
        file.seek(SeekFrom::End(0))
            .map_err(|source| io_error(&path, source))?;
        Ok(Self {
            path,
            file,
            current,
            poisoned: false,
        })
    }

    pub fn current(&self, producer_id: Uuid) -> Option<u64> {
        self.current.get(&producer_id).copied()
    }

    /// Fence older sessions before any record for a newer epoch can be acked.
    pub fn accept(&mut self, producer_id: Uuid, epoch: u64) -> BrokerResult<()> {
        if self.poisoned {
            return Err(BrokerError::EpochJournalCorrupt(
                "journal writer is poisoned after an uncertain append; reopen and validate it"
                    .to_owned(),
            ));
        }
        match self.current(producer_id) {
            Some(current) if epoch < current => {
                return Err(BrokerError::ProducerFenced {
                    producer_id,
                    current,
                    actual: epoch,
                });
            }
            Some(current) if epoch == current => return Ok(()),
            _ => {}
        }
        let next_len = self
            .file
            .len()
            .map_err(|source| io_error(&self.path, source))?
            .saturating_add(EPOCH_ENTRY_BYTES);
        if next_len > MAX_EPOCH_JOURNAL_BYTES {
            return Err(BrokerError::InvalidConfig(
                "producer epoch journal reached its explicit size ceiling".to_owned(),
            ));
        }
        let mut encoded = Vec::with_capacity(EPOCH_ENTRY_BYTES as usize);
        encoded.extend_from_slice(producer_id.as_bytes());
        encoded.extend_from_slice(&epoch.to_be_bytes());
        encoded.extend_from_slice(epoch_checksum(producer_id, epoch).as_bytes());
        if let Err(source) = self
            .file
            .write_all(&encoded)
            .and_then(|()| self.file.sync_data())
        {
            self.poisoned = true;
            return Err(io_error(&self.path, source));
        }
        self.current.insert(producer_id, epoch);
        Ok(())
    }
}

fn epoch_checksum(producer_id: Uuid, epoch: u64) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(EPOCH_DOMAIN);
    hasher.update(producer_id.as_bytes());
    hasher.update(&epoch.to_be_bytes());
    hasher.finalize()
}

pub(crate) fn storage_producer_id(producer_id: Uuid, epoch: u64) -> Uuid {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"vtop-segment-v1-producer-epoch-namespace\0");
    hasher.update(producer_id.as_bytes());
    hasher.update(&epoch.to_be_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Uuid::from_bytes(bytes)
}

/// On-disk segment format the broker writes to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SegmentFormat {
    /// v1 frames cannot carry producer epochs, so each epoch is folded into
    /// a derived storage producer id.
    #[default]
    V1,
    /// v2 frames persist the producer epoch, so records land with their real
    /// producer identity and the log itself enforces epoch fencing.
    V2,
}

/// One sealed segment of a leader's range, snapshotted for transfer (#270).
///
/// Carries the primary path rather than open handles: sealed files are
/// immutable, so a path taken under the state lock can be read after it
/// drops, and the transfer plane's sibling-artifact naming stays owned by
/// `vtop_log::sealed_artifact_path`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealedSegmentHandle {
    pub segment_id: Uuid,
    pub base_offset: u64,
    pub next_offset: u64,
    pub segment_path: std::path::PathBuf,
}

struct BrokerState {
    /// The range as a SEQUENCE of segments (#270), not one file: appends land
    /// on the tail and roll it at its configured bound, fetches cross
    /// sealed/active boundaries. Held as a set here — rather than the node
    /// swapping segments under the broker — because rolling happens INSIDE
    /// the append critical section, under the same lock as the fencing check
    /// and the fsync.
    segment: SegmentSet,
    producer_epochs: ProducerEpochJournal,
}

/// Shared metadata lease view for a range.
///
/// Observers publish grants ([`MetaFencingEpoch::set`]) and releases
/// ([`MetaFencingEpoch::clear_lease`]). Brokers lock this view for the
/// entire produce/fetch critical section so a concurrent revocation cannot
/// race past the fencing check into a durable append.
///
/// This handle is process-local: a Raft applied-state watcher (follow-up)
/// must drive [`MetaFencingEpoch::set`] / [`MetaFencingEpoch::clear_lease`]
/// from committed metadata on every node. Publication itself is monotonic so
/// out-of-order observer callbacks cannot rewind a fenced view.
#[derive(Clone, Debug)]
pub struct MetaFencingEpoch {
    state: Arc<Mutex<MetaLeaseState>>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct MetaLeaseState {
    /// Latest metadata fencing epoch for the range (never rewound on release).
    pub(crate) fencing_epoch: u64,
    /// Whether a live lease currently exists. Release clears this without
    /// changing `fencing_epoch`.
    pub(crate) lease_active: bool,
    /// Highest epoch for which a release has been observed. Lets a release
    /// that arrives before its matching grant still win when the grant is
    /// applied later.
    released_through: u64,
}

impl MetaFencingEpoch {
    /// Start with an active lease at `epoch` (fixed single-node / test default).
    pub fn new(epoch: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(MetaLeaseState {
                fencing_epoch: epoch,
                lease_active: true,
                released_through: 0,
            })),
        }
    }

    /// Start with `epoch` as the monotonic floor but NO active lease.
    ///
    /// For processes whose authority arrives later — a lease agent's first
    /// successful acquisition — rather than from static configuration. Until
    /// a grant is published the broker fails closed: it refuses produce and
    /// reports itself fenced. The first [`Self::set`] at `epoch` or above
    /// activates the view. (`epoch` is only a floor; a grant below it is
    /// stale by definition and stays ignored.)
    pub fn new_inactive(epoch: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(MetaLeaseState {
                fencing_epoch: epoch,
                lease_active: false,
                released_through: 0,
            })),
        }
    }

    /// Latest metadata fencing epoch (whether or not a lease is live).
    pub fn get(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .fencing_epoch
    }

    /// Whether metadata currently records an active lease for the range.
    pub fn lease_active(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .lease_active
    }

    /// Non-blocking `(fencing_epoch, lease_active)`, for observation only
    /// (#224).
    ///
    /// `None` while a produce or fetch holds the lease view. That is not a
    /// rare window: the broker locks this for the *entire* critical section,
    /// fsync included, so a blocking read from a scrape handler would park a
    /// runtime worker behind a stalling disk — under precisely the failure an
    /// operator is scraping to diagnose. Concurrent scrapes would then occupy
    /// every worker and take the endpoint down with the disk.
    ///
    /// Both fields come from one lock acquisition so the caller can never
    /// observe an epoch from before a grant beside a lease bit from after it.
    pub fn try_snapshot(&self) -> Option<(u64, bool)> {
        let state = match self.state.try_lock() {
            Ok(state) => state,
            // A poisoned lock still holds a readable view, and the last known
            // fencing state of a broker whose append path panicked is more
            // useful than nothing.
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        Some((state.fencing_epoch, state.lease_active))
    }

    /// Publish a metadata grant (including a steal that mints a newer epoch).
    ///
    /// Stale grants with a lower epoch than the view already knows are ignored
    /// so concurrent/retried observer callbacks cannot rewind fencing. A grant
    /// whose epoch was already released (possibly out of order) stays inactive.
    pub fn set(&self, epoch: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if epoch < state.fencing_epoch {
            return;
        }
        state.fencing_epoch = epoch;
        state.lease_active = epoch > state.released_through;
    }

    /// Locally suspend serving at `expected_epoch` WITHOUT recording a
    /// release.
    ///
    /// For a holder that cannot currently serve its own live lease — a
    /// verified promotion whose quorum probe transiently failed, say. The
    /// broker must stop accepting writes NOW, but the epoch is still this
    /// process's grant, and a successful retry must be able to reactivate it
    /// with [`Self::set`]. Using [`Self::clear_lease`] here would advance
    /// `released_through` past the epoch, turning that future reactivation
    /// into a permanent no-op: the broker would sit fenced under its own live
    /// lease until an external epoch change.
    ///
    /// A no-op for any other epoch: a stale suspension must not deactivate a
    /// newer grant.
    pub fn suspend(&self, expected_epoch: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.fencing_epoch == expected_epoch {
            state.lease_active = false;
        }
    }

    /// Publish a metadata release for `expected_epoch`.
    ///
    /// The fencing epoch is retained when releasing the current epoch, but no
    /// leaseholder remains authorized. Releases always advance
    /// `released_through`, so a release that arrives before its grant still
    /// deactivates that epoch when the grant shows up. A release newer than
    /// the current view also advances/deactivates immediately: observing
    /// `clear_lease(n)` proves grant `n` already committed and fenced every
    /// older holder. A release for an older epoch than the current view does
    /// not deactivate a newer live lease.
    pub fn clear_lease(&self, expected_epoch: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if expected_epoch > state.released_through {
            state.released_through = expected_epoch;
        }
        if state.fencing_epoch == expected_epoch {
            state.lease_active = false;
        } else if expected_epoch > state.fencing_epoch {
            state.fencing_epoch = expected_epoch;
            state.lease_active = false;
        }
    }

    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, MetaLeaseState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Hold the lease view exactly as an in-flight append does, for tests that
    /// need to prove an observation path does not block on it.
    ///
    /// The guard's type is deliberately opaque: callers may only hold and drop
    /// it, never read or mutate the state through it. Exposed because the
    /// non-blocking contract of [`Self::try_snapshot`] is worth testing from
    /// the crates that depend on it, not only from inside this one.
    #[doc(hidden)]
    pub fn hold_for_test(&self) -> impl Sized + '_ {
        self.lock()
    }
}

impl Default for MetaFencingEpoch {
    fn default() -> Self {
        Self::new(0)
    }
}

/// Shared metadata state machine used for durable consumer-group checkpoints.
///
/// In this slice the broker applies group/cursor commands in-process. A later
/// stage wires the same command types through Raft propose on the metadata
/// group; the broker surface stays the lineage-aware commit/fetch path.
#[derive(Clone, Default)]
pub struct GroupCheckpointStore {
    state: Arc<Mutex<MetaStateMachine>>,
    /// Monotonic apply index for in-process command application.
    apply_index: Arc<Mutex<u64>>,
}

impl GroupCheckpointStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_state(state: MetaStateMachine) -> Self {
        Self {
            state: Arc::new(Mutex::new(state)),
            apply_index: Arc::new(Mutex::new(0)),
        }
    }

    pub fn apply(&self, command: MetadataCommand) -> MetadataResponse {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut apply_index = self
            .apply_index
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *apply_index = apply_index.saturating_add(1);
        state.apply(*apply_index, &command)
    }

    pub fn with_state<R>(&self, f: impl FnOnce(&MetaStateMachine) -> R) -> R {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        f(&state)
    }
}

pub struct LocalBroker {
    range: RangeIdentity,
    /// Epoch this process was granted as leaseholder.
    ///
    /// Atomic because leadership can be re-granted while the broker is alive
    /// (#223): a lease agent that wins a new epoch must be able to tell the
    /// broker, or the broker would fence itself against its own promotion —
    /// metadata would report epoch N+1 while this field still said N, and
    /// every produce would be refused.
    held_fencing_epoch: AtomicU64,
    /// Which epoch wrote each stretch of this replica's log (#240).
    ///
    /// Optional because every existing constructor and in-process test builds
    /// a broker without one, and a range that never changes leadership has no
    /// history to record. A live node wires it; when it is absent, promotion
    /// simply has no epoch evidence to reconcile against and must say so
    /// rather than pretend a bare offset is comparable.
    fencing_epoch_journal: Mutex<Option<crate::fencing_epochs::FencingEpochJournal>>,
    /// Set if a start could not be recorded. From that point the vector has a
    /// hole, so it is no longer safe to reconcile against.
    fencing_epoch_history_broken: AtomicBool,
    /// Latest metadata-committed fencing epoch for the range.
    meta_fencing_epoch: MetaFencingEpoch,
    segment_format: SegmentFormat,
    /// Quorum-committed high-water mark. When set, fetch never exposes above it
    /// and `Durability::Quorum` produce waits for it to cover the append.
    cluster_committed: Option<ClusterCommittedOffset>,
    /// Sealed-prefix retention bound in bytes; 0 = retention disabled (#290).
    /// Atomic so operators can set it after construction without a lock, and
    /// the produce path can read it inside the state-lock critical section.
    retention_max_total_bytes: std::sync::atomic::AtomicU64,
    /// Optional follower fan-out for quorum produce.
    replicas: Option<Arc<dyn ReplicaSet>>,
    /// Leader identity embedded in replica append requests.
    node_id: Uuid,
    /// Optional durable consumer-group checkpoint store.
    group_checkpoints: Option<GroupCheckpointStore>,
    /// Optional adaptive cross-session group-commit coordinator.
    group_commit: Option<Arc<GroupCommitCoordinator>>,
    /// Shared memory budgets for produce / fetch / replication admission.
    memory: Arc<MemoryBudgetPool>,
    state: Mutex<BrokerState>,
}

impl LocalBroker {
    /// Construct a broker whose held lease epoch and metadata view start equal.
    ///
    /// Prefer [`Self::with_meta_fencing_epoch`] when the broker should observe
    /// live metadata grants (so a newer grant fences this instance).
    ///
    /// `segment` is anything that converts into a [`SegmentSet`]: a
    /// catalog-opened set from a node, or a bare `ActiveSegment` from a
    /// harness, which becomes a set of one. The conversion is where the two
    /// meet — a broker always serves a set, and a single segment is just the
    /// set every range starts as.
    pub fn new(
        segment: impl Into<SegmentSet>,
        producer_epochs: ProducerEpochJournal,
        range: RangeIdentity,
        fencing_epoch: u64,
    ) -> BrokerResult<Self> {
        Self::with_meta_fencing_epoch(
            segment,
            producer_epochs,
            range,
            fencing_epoch,
            MetaFencingEpoch::new(fencing_epoch),
        )
    }

    /// Construct a broker bound to a live metadata fencing-epoch handle.
    ///
    /// `held_fencing_epoch` is the grant this process accepted. Produce and
    /// fetch succeed only while that value still equals
    /// [`MetaFencingEpoch::get`] and the request carries the same epoch.
    pub fn with_meta_fencing_epoch(
        segment: impl Into<SegmentSet>,
        producer_epochs: ProducerEpochJournal,
        range: RangeIdentity,
        held_fencing_epoch: u64,
        meta_fencing_epoch: MetaFencingEpoch,
    ) -> BrokerResult<Self> {
        Self::with_replication(
            segment,
            producer_epochs,
            range,
            held_fencing_epoch,
            meta_fencing_epoch,
            Uuid::nil(),
            None,
            None,
        )
    }

    /// Construct a leaseholder that can acknowledge `Durability::Quorum`.
    ///
    /// `cluster_committed` is the shared quorum high-water mark observed by
    /// fetch. `replicas` fans appends out to followers after the leader's local
    /// `Fsync`.
    #[allow(clippy::too_many_arguments)]
    pub fn with_replication(
        segment: impl Into<SegmentSet>,
        producer_epochs: ProducerEpochJournal,
        range: RangeIdentity,
        held_fencing_epoch: u64,
        meta_fencing_epoch: MetaFencingEpoch,
        node_id: Uuid,
        cluster_committed: Option<ClusterCommittedOffset>,
        replicas: Option<Arc<dyn ReplicaSet>>,
    ) -> BrokerResult<Self> {
        let segment: SegmentSet = segment.into();
        // The format is derived from the tail itself so the broker's
        // produce-time behavior can never disagree with the on-disk frames.
        // The tail speaks for the set: it is the only segment appends land
        // on, and rolling preserves format, so a set never mixes formats
        // forward from here.
        let segment_format = if segment.active().format_version() == vtop_log::FORMAT_VERSION_V2 {
            SegmentFormat::V2
        } else {
            SegmentFormat::V1
        };
        let identity_matches = match segment_format {
            SegmentFormat::V1 => {
                let descriptor = segment.active().descriptor();
                descriptor.topic == range.topic
                    && descriptor.topic_epoch == range.topic_epoch
                    && descriptor.lineage.range_id == range.range_id
                    && descriptor.lineage.generation == range.range_generation
            }
            SegmentFormat::V2 => {
                let descriptor = segment
                    .active()
                    .descriptor_v2()
                    .expect("v2 format was detected from this segment");
                descriptor.topic == range.topic
                    && descriptor.topic_epoch == range.topic_epoch
                    && descriptor.lineage.range_id == range.range_id
                    && descriptor.lineage.generation == range.range_generation
            }
        };
        if !identity_matches {
            return Err(BrokerError::InvalidConfig(
                "broker range identity does not match active segment".to_owned(),
            ));
        }
        if replicas.is_some() && cluster_committed.is_none() {
            return Err(BrokerError::InvalidConfig(
                "replica set requires a cluster committed high-water mark".to_owned(),
            ));
        }
        Ok(Self {
            range,
            held_fencing_epoch: AtomicU64::new(held_fencing_epoch),
            fencing_epoch_journal: Mutex::new(None),
            fencing_epoch_history_broken: AtomicBool::new(false),
            meta_fencing_epoch,
            segment_format,
            retention_max_total_bytes: std::sync::atomic::AtomicU64::new(0),
            cluster_committed,
            replicas,
            node_id,
            group_checkpoints: None,
            group_commit: None,
            memory: MemoryBudgetPool::new(MemoryBudgetConfig::default())
                .expect("default memory budget config is valid"),
            state: Mutex::new(BrokerState {
                segment,
                producer_epochs,
            }),
        })
    }

    /// Attach a durable consumer-group checkpoint store for CommitCursor /
    /// FetchCursor handling.
    pub fn with_group_checkpoints(mut self, store: GroupCheckpointStore) -> Self {
        self.group_checkpoints = Some(store);
        self
    }

    /// Enable adaptive cross-session group commit for produce durability.
    pub fn with_group_commit(mut self, config: GroupCommitConfig) -> BrokerResult<Self> {
        self.group_commit = Some(Arc::new(
            GroupCommitCoordinator::new(config).map_err(BrokerError::InvalidConfig)?,
        ));
        Ok(self)
    }

    /// Replace the broker memory-budget pool (produce / fetch / replica ceilings).
    pub fn with_memory_budget(mut self, pool: Arc<MemoryBudgetPool>) -> Self {
        self.memory = pool;
        self
    }

    /// Shared group checkpoint store, when configured.
    pub fn group_checkpoints(&self) -> Option<&GroupCheckpointStore> {
        self.group_checkpoints.as_ref()
    }

    /// Group-commit coordinator, when configured.
    pub fn group_commit(&self) -> Option<&Arc<GroupCommitCoordinator>> {
        self.group_commit.as_ref()
    }

    /// Shared memory-budget pool.
    pub fn memory_budget(&self) -> &Arc<MemoryBudgetPool> {
        &self.memory
    }

    /// The segment format this broker writes, derived from its segment.
    pub fn segment_format(&self) -> SegmentFormat {
        self.segment_format
    }

    /// Fencing epoch this broker was granted as range leaseholder.
    pub fn held_fencing_epoch(&self) -> u64 {
        self.held_fencing_epoch.load(Ordering::SeqCst)
    }

    /// Adopt an epoch this process has been granted (#223).
    ///
    /// Monotonic: a stale or reordered promotion cannot rewind the broker to an
    /// epoch metadata has already superseded. Returns whether the value moved.
    ///
    /// The caller must publish the same epoch to [`Self::meta_fencing_epoch`].
    /// Either order is safe — produce requires the two to be equal, so any
    /// window between them refuses rather than admits — but leaving them
    /// permanently unequal would silently wedge the range.
    pub fn adopt_fencing_epoch(&self, epoch: u64) -> bool {
        // The start is made durable BEFORE the epoch becomes visible.
        //
        // `fetch_max` is what admits produce under the new epoch, so recording
        // after it leaves a window in which a record is written under an epoch
        // whose start offset is not yet known. The journal would then name a
        // start ABOVE the first record that epoch actually wrote, and a later
        // divergence comparison would attribute that record to the previous
        // epoch — misreading the very boundary this vector exists to fix.
        //
        // Reading the offset first is equally deliberate: it must be the tail
        // as of before the epoch is servable, not after.
        if epoch > self.held_fencing_epoch.load(Ordering::SeqCst) {
            let (_, next_offset) = self.local_offsets();
            self.record_epoch_start(epoch, next_offset);
        }
        self.held_fencing_epoch.fetch_max(epoch, Ordering::SeqCst) < epoch
    }

    /// Bound this range's disk in bytes; `None` disables retention (#290).
    ///
    /// Takes effect on the next produce that appends. The bound covers
    /// encoded record frames (sealed content plus the active tail), and only
    /// segments wholly below the acknowledged floor are ever reclaimed. A
    /// `Some` policy with a zero bound is treated as disabled at this layer;
    /// node configuration rejects zero outright, so the only way to disable
    /// retention is to not configure it.
    pub fn set_retention(&self, policy: Option<vtop_log::RetentionPolicy>) {
        self.retention_max_total_bytes.store(
            policy.map(|policy| policy.max_total_bytes).unwrap_or(0),
            Ordering::SeqCst,
        );
    }

    /// Run one retention pass under the state lock the caller already holds.
    ///
    /// A failed pass is REPORTED, never returned: the append it follows was
    /// already acknowledged, and an interrupted retention is finished by the
    /// next open — refusing the produce over reclamation trouble would turn
    /// a disk-space policy into an availability incident.
    fn run_retention(&self, segment: &mut SegmentSet) {
        let max_total_bytes = self.retention_max_total_bytes.load(Ordering::SeqCst);
        if max_total_bytes == 0 {
            return;
        }
        let local = segment.committed_offset();
        let floor = self
            .cluster_committed
            .as_ref()
            .map(|committed| committed.get().min(local))
            .unwrap_or(local);
        if let Err(problem) = segment.retain(&vtop_log::RetentionPolicy { max_total_bytes }, floor)
        {
            eprintln!(
                "retention on {} failed and will be retried after the next append: {problem}",
                self.range.topic
            );
        }
    }

    /// Install the durable epoch→offset vector for this replica (#240).
    ///
    /// Seeds the currently held epoch when the vector is empty AND the log is
    /// empty. Without that, a replica configured with a static epoch never
    /// calls `adopt_fencing_epoch`, so it would report "unknown" for its
    /// entire history and be unreconcilable forever.
    ///
    /// The log-empty condition is the part that matters, and it is narrower
    /// than it looks tempting to make it: for a replica that ALREADY holds
    /// records, this process cannot know where its held epoch began — the
    /// records may have been written under an older one. Claiming it started
    /// at the current tail would be a fabricated boundary, and a later
    /// truncation computed from it could discard acknowledged records.
    /// Reporting "unknown" there is the honest answer, and the API already
    /// handles it.
    ///
    /// Also completes an interrupted truncation. See
    /// [`Self::attach_epoch_journal_to_log`].
    pub fn set_fencing_epoch_journal(
        &self,
        mut journal: crate::fencing_epochs::FencingEpochJournal,
    ) {
        let (_, next_offset) = self.local_offsets();
        if !Self::attach_epoch_journal_to_log(&mut journal, next_offset) {
            self.fencing_epoch_history_broken
                .store(true, Ordering::SeqCst);
        }
        if journal.latest().is_none() {
            let epoch = self.held_fencing_epoch.load(Ordering::SeqCst);
            // Epoch 0 is the "no grant yet" sentinel a lease-driven replica
            // starts at, not an epoch that ever wrote a record. Seeding it
            // would put a fabricated entry at the head of the vector claiming
            // epoch 0 owns records it never wrote.
            if next_offset == 0 && epoch > 0 && journal.record(epoch, 0).is_err() {
                self.fencing_epoch_history_broken
                    .store(true, Ordering::SeqCst);
            }
        }
        *self
            .fencing_epoch_journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(journal);
    }

    /// Reconcile a freshly opened epoch vector against the log it describes,
    /// completing a truncation that a crash interrupted (#240).
    ///
    /// [`Self::truncate_to`] shortens the log before the vector. A crash
    /// between the two leaves entries whose start offset sits beyond the log's
    /// tail — a claim to own records past the end of the file, which cannot be
    /// true of any log and so is safe to drop on sight. Doing it here, where
    /// the vector first meets the log, means the window closes on the next open
    /// rather than waiting for a promotion to trip over it.
    ///
    /// Strictly beyond, not at: an entry AT the tail is the normal state of a
    /// replica granted an epoch it has not yet written under, which
    /// [`Self::adopt_fencing_epoch`] records deliberately.
    ///
    /// Returns false if the repair could not be made durable, in which case the
    /// vector still overstates the log and must not be offered to a peer.
    fn attach_epoch_journal_to_log(
        journal: &mut crate::fencing_epochs::FencingEpochJournal,
        next_offset: u64,
    ) -> bool {
        let overstates = journal
            .latest()
            .is_some_and(|entry| entry.start_offset > next_offset);
        if !overstates {
            return true;
        }
        journal.truncate_to(next_offset.saturating_add(1)).is_ok()
    }

    /// This replica's epoch history.
    ///
    /// Empty means "no history you may reconcile against" — either none was
    /// configured, or a start failed to record and the vector now has a hole.
    /// Both are the same decision for a caller: an offset from this replica is
    /// a bare number and cannot be compared by epoch. Callers must treat empty
    /// as "unknown", never as "no leadership changes".
    pub fn epoch_starts(&self) -> Vec<crate::fencing_epochs::EpochStart> {
        // Flag read UNDER the lock. Checked before it, a record that fails
        // while this call is waiting would let a now-broken history be
        // returned as authoritative — the partial vector this API exists to
        // never hand out.
        let guard = self
            .fencing_epoch_journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.fencing_epoch_history_broken.load(Ordering::SeqCst) {
            return Vec::new();
        }
        guard
            .as_ref()
            .map(|journal| journal.entries().to_vec())
            .unwrap_or_default()
    }

    /// Discard every record at or above `offset` and drop the epoch entries
    /// that described them (#240).
    ///
    /// This is the repair for a replica that diverged from the range's current
    /// leadership: it wrote records under a leader that lost the range before
    /// those records reached a quorum, and it cannot follow the new leader
    /// until they are gone. Without it such a replica is stranded permanently —
    /// it refuses every append and no amount of retrying fixes it.
    ///
    /// # The bound this enforces
    ///
    /// `offset` may never fall below the cluster high-water mark. Records below
    /// it were acknowledged to producers, and discarding them is data loss, not
    /// repair. This is the check the segment layer cannot make — it has no idea
    /// what a quorum agreed to — and it is the reason the primitive is not
    /// exposed directly.
    ///
    /// A broker with no replication at all refuses outright. Every durable
    /// record on such a node is acknowledged by definition, so there is no
    /// divergence to repair and any truncation would be pure loss.
    ///
    /// # Order
    ///
    /// The log is truncated before the epoch vector, and the crash window
    /// between them is why. Log-first leaves entries claiming epochs that begin
    /// past the tail — impossible on their face, so [`Self::attach_epoch_journal_to_log`]
    /// detects and drops them on the next open. Vector-first would leave
    /// surviving records silently re-attributed to the preceding epoch, which
    /// is undetectable and is precisely the misattribution the vector exists to
    /// prevent. Neither order is atomic; only one fails in a way we can see.
    pub fn truncate_to(&self, offset: u64) -> BrokerResult<vtop_log::TruncateOutcome> {
        let Some(cluster_committed) = self.cluster_committed.as_ref() else {
            return Err(BrokerError::TruncationWithoutReplication);
        };
        let high_watermark = cluster_committed.get();
        if offset < high_watermark {
            return Err(BrokerError::TruncationBelowAcknowledged {
                requested: offset,
                high_watermark,
            });
        }

        let outcome = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state
                .segment
                .truncate_to(offset)
                .map_err(|source| BrokerError::InvalidConfig(source.to_string()))?
        };

        let mut guard = self
            .fencing_epoch_journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(journal) = guard.as_mut() {
            if journal.truncate_to(offset).is_err() {
                // The records are gone and the vector still describes them. It
                // now overstates this replica's history, so it must stop being
                // offered as one: `epoch_starts` reports "unknown", which every
                // caller already handles, rather than a vector a peer could
                // reconcile against and truncate itself on the strength of.
                self.fencing_epoch_history_broken
                    .store(true, Ordering::SeqCst);
            }
        }
        Ok(outcome)
    }

    fn record_epoch_start(&self, epoch: u64, start_offset: u64) {
        let mut guard = self
            .fencing_epoch_journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(journal) = guard.as_mut() {
            if journal.record_adoption(epoch, start_offset).is_err() {
                // Do not fail the append path over it, but do not carry on
                // pretending the vector is complete either. From here the
                // history has a hole, and a promotion reconciling against it
                // could compute a truncation target that discards
                // acknowledged records. Marking it broken makes
                // `epoch_starts` report "unknown", which callers must already
                // handle — a structurally unusable answer rather than a log
                // line someone has to notice.
                self.fencing_epoch_history_broken
                    .store(true, Ordering::SeqCst);
            }
        }
    }

    /// Shared metadata fencing-epoch handle observed by this broker.
    pub fn meta_fencing_epoch(&self) -> &MetaFencingEpoch {
        &self.meta_fencing_epoch
    }

    /// Quorum-committed high-water mark when this broker is configured for
    /// cluster durability; `None` for single-node LocalFsync-only brokers.
    pub fn cluster_committed(&self) -> Option<&ClusterCommittedOffset> {
        self.cluster_committed.as_ref()
    }

    /// The range this broker leads.
    pub fn range(&self) -> &RangeIdentity {
        &self.range
    }

    /// This broker's node identity, as embedded in replica append requests.
    pub fn node_id(&self) -> Uuid {
        self.node_id
    }

    /// Snapshot of the sealed prefix for transfer serving (#270).
    ///
    /// Taken under the state lock so the listing describes one instant, then
    /// served WITHOUT it: sealed files are immutable, so the paths stay valid
    /// after the lock drops, and a transfer reading gigabytes must not park
    /// the append path behind its disk reads. The active tail is deliberately
    /// not represented — bytes read from a file still being appended to can
    /// be superseded by a truncation before the transfer finishes.
    pub fn sealed_segment_handles(&self) -> Vec<SealedSegmentHandle> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .segment
            .sealed()
            .iter()
            .map(|reader| SealedSegmentHandle {
                segment_id: reader.segment_id(),
                base_offset: reader.base_offset(),
                next_offset: reader.next_offset(),
                segment_path: reader.path().to_path_buf(),
            })
            .collect()
    }

    /// The active tail's segment id, so a transfer refusal can NAME what was
    /// asked for instead of reporting a tail request as merely unknown.
    pub fn active_segment_id(&self) -> Uuid {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let active = state.segment.active();
        match active.descriptor_v2() {
            Some(descriptor) => descriptor.segment_id,
            None => active.descriptor().segment_id,
        }
    }

    /// `(local_committed_offset, next_offset)`, waiting for the append path if
    /// it holds the state lock.
    ///
    /// For request handlers, which are already allowed to queue behind an
    /// append. Metrics collection must use [`Self::try_local_offsets`] instead:
    /// a scrape that blocks takes the observability endpoint down with a
    /// stalling disk.
    pub fn local_offsets(&self) -> (u64, u64) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (
            state.segment.committed_offset(),
            state.segment.next_offset(),
        )
    }

    /// Durably commit the tail's boundary, for an orderly shutdown (#280).
    ///
    /// Every acknowledged append was already fsynced, so this loses nothing
    /// if skipped — it writes the commit-boundary sidecar one final time so
    /// the next open's recovery finds a boundary that matches the file
    /// exactly and has no torn tail to truncate. The tail is NOT sealed: a
    /// tail sealed without a successor is a directory `open_in` refuses.
    pub fn quiesce(&self) -> BrokerResult<u64> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.segment.commit().map_err(BrokerError::from)
    }

    /// Seal the active tail so the sealed prefix reaches this leader's
    /// actual position (#306).
    ///
    /// Only sealed segments transfer, so before this a repair stopped at
    /// wherever the range last happened to roll — up to a whole segment
    /// bound behind the leader. Sealing on demand is an ordinary roll taken
    /// deliberately: the tail seals, a successor opens at its end, and the
    /// freshly sealed segment is transferable the moment this returns.
    ///
    /// FENCED IN HERE, under the produce path's own lock discipline —
    /// metadata lease view first, then broker state, both held through the
    /// roll — so a grant or release cannot land between the fencing check
    /// and the seal. A check performed outside these locks would leave
    /// exactly that window, and sealing is a write a deposed leader must
    /// not perform (review: the one-snapshot check alone was not enough).
    ///
    /// RETENTION DOES NOT RUN HERE, deliberately. A bytes-bound retention
    /// pass could reclaim the very segment this call just sealed — the
    /// committed floor covers it by construction — and the RPC would then
    /// report a `sealed_end` the transfer listing cannot reach from within
    /// this very call. What this does NOT promise is persistence: retention
    /// runs after every successful append, so a produce landing between
    /// this seal and the repair's listing can still reclaim the segment
    /// under a bound smaller than the sealed tail. That window is the same
    /// one every listed segment already lives in until its chunks are
    /// fetched, and both ends of it fail HONESTLY — a shorter listing is a
    /// measured, reported gap (exit 1), and a segment reclaimed mid-fetch
    /// is a clean, resumable refusal. A cross-RPC pin was considered and
    /// rejected: the leader cannot know when a repairer is done, and a pin
    /// that outlives a crashed repairer is a retention bound that silently
    /// stopped being one.
    ///
    /// Returns `(sealed_end, records_sealed)`. An EMPTY tail over a sealed
    /// prefix is an idempotent no-op — the prefix already reaches the
    /// leader's position, records_sealed is zero, and a retrying repair can
    /// tell that from progress. An empty tail with NO sealed prefix is
    /// refused: a never-written range has nothing a transfer could carry,
    /// and minting a degenerate sealed segment to say so would cost a file
    /// and a lie.
    pub fn seal_tail(
        &self,
        range: &RangeIdentity,
        fencing_epoch: u64,
    ) -> Result<(u64, u64), (ErrorCode, String)> {
        // Lock order: metadata lease view, then broker state — identical to
        // the produce path, which documents why: held together through the
        // write, a concurrent grant/release cannot revoke between the
        // fencing check and the mutation.
        let meta = self.meta_fencing_epoch.lock();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.check_range(&meta, range, fencing_epoch)
            .map_err(|(code, message)| (code, message.to_owned()))?;
        let tail_base = state.segment.active().base_offset();
        let tail_next = state.segment.next_offset();
        if tail_next == tail_base {
            if state.segment.sealed().is_empty() {
                return Err((
                    ErrorCode::InvalidRequest,
                    "this range has never held a record; there is nothing to seal and nothing \
                     a transfer could carry — produce to the range before repairing from it"
                        .to_owned(),
                ));
            }
            return Ok((tail_next, 0));
        }
        let records_sealed = tail_next - tail_base;
        state
            .segment
            .roll_minting()
            .map_err(|problem| (ErrorCode::Storage, problem.to_string()))?;
        Ok((tail_next, records_sealed))
    }

    /// Non-blocking `(local_committed_offset, next_offset)`, for observation
    /// only (#224).
    ///
    /// Returns `None` while the append path holds the state lock. Metrics must
    /// never park a runtime worker behind an in-progress fsync: under exactly
    /// the conditions where an operator needs the endpoint most — a disk that
    /// has stopped acknowledging writes — a blocking read would take the scrape
    /// endpoint down along with the disk. A gauge that stops advancing is the
    /// honest signal there; a scrape that hangs is not.
    pub fn try_local_offsets(&self) -> Option<(u64, u64)> {
        let state = match self.state.try_lock() {
            Ok(state) => state,
            // A poisoned lock still yields a readable segment view, and
            // reporting the last durable boundary of a broker whose append path
            // panicked is more useful than reporting nothing.
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        Some((
            state.segment.committed_offset(),
            state.segment.next_offset(),
        ))
    }

    pub fn handle(&self, role: Role, frame: WireFrame) -> WireFrame {
        self.handle_with_connection(role, frame, None)
    }

    /// Handle a frame while charging an optional per-session connection budget.
    pub fn handle_with_connection(
        &self,
        role: Role,
        frame: WireFrame,
        conn: Option<&ConnectionBudget>,
    ) -> WireFrame {
        let WireFrame {
            request_id,
            stream_id,
            message,
        } = frame;
        match message {
            Message::ProduceRequest(request) => {
                self.handle_produce(role, request_id, stream_id, request, conn)
            }
            Message::FetchRequest(request) => {
                if role != Role::Consumer {
                    return error(
                        request_id,
                        stream_id,
                        ErrorCode::Unauthorized,
                        "session role cannot fetch",
                    );
                }
                if request.max_bytes == 0 || request.max_records == 0 {
                    return error(
                        request_id,
                        stream_id,
                        ErrorCode::InvalidRequest,
                        "fetch limits must be non-zero",
                    );
                }
                let meta = self.meta_fencing_epoch.lock();
                if let Err((code, message)) =
                    self.check_range(&meta, &request.range, request.fencing_epoch)
                {
                    return error(request_id, stream_id, code, message);
                }
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let fetch_hwm = self
                    .cluster_committed
                    .as_ref()
                    .map(|hwm| hwm.get().min(state.segment.committed_offset()))
                    .unwrap_or_else(|| state.segment.committed_offset());
                let mut fetched = state.segment.fetch_through(
                    request.start_offset,
                    request.max_bytes as usize,
                    request.max_records as usize,
                    fetch_hwm,
                );
                if let Ok(batch) = &fetched {
                    // The byte budget excluded even the first committed
                    // record. Refetch exactly that record so the consumer
                    // always makes progress; the session layer enforces the
                    // negotiated wire-frame cap on the actual response.
                    if batch.records.is_empty()
                        && batch.next_offset == request.start_offset
                        && batch.next_offset < batch.high_watermark
                    {
                        fetched = state.segment.fetch_through(
                            request.start_offset,
                            usize::MAX,
                            1,
                            fetch_hwm,
                        );
                    }
                }
                match fetched {
                    Ok(batch) => WireFrame {
                        request_id,
                        stream_id,
                        message: Message::FetchResponse(FetchResponse {
                            records: batch
                                .records
                                .into_iter()
                                .map(|record| FetchedRecord {
                                    offset: record.offset,
                                    timestamp_millis: record.record.timestamp_millis,
                                    key: record.record.key,
                                    value: record.record.value,
                                })
                                .collect(),
                            next_offset: batch.next_offset,
                            committed_high_watermark: batch.high_watermark,
                        }),
                    },
                    Err(problem) => {
                        // A reclaimed offset is a consumer decision, not a
                        // storage fault; give it the code that says so (#290).
                        let code = match &problem {
                            vtop_log::LogError::OffsetBelowRange { .. } => {
                                ErrorCode::OffsetRetained
                            }
                            _ => ErrorCode::Storage,
                        };
                        error(request_id, stream_id, code, &problem.to_string())
                    }
                }
            }
            Message::CommitCursorRequest(request) => {
                if role != Role::Consumer {
                    return error(
                        request_id,
                        stream_id,
                        ErrorCode::Unauthorized,
                        "session role cannot commit cursors",
                    );
                }
                let Some(store) = self.group_checkpoints.as_ref() else {
                    return error(
                        request_id,
                        stream_id,
                        ErrorCode::InvalidRequest,
                        "broker has no group checkpoint store configured",
                    );
                };
                if request.operation_id.is_nil() {
                    return error(
                        request_id,
                        stream_id,
                        ErrorCode::InvalidRequest,
                        "cursor commit requires a non-nil operation ID",
                    );
                }
                let command = MetadataCommand::CommitGroupCursor {
                    env: CommandEnvelope {
                        // Retry an ambiguous commit with this same ID. After
                        // a definitive rejection, the client may rotate it
                        // once prerequisites have changed.
                        request_id: request.operation_id,
                        issued_at_ms: 0,
                    },
                    group_uuid: request.cursor.group_id,
                    member_uuid: request.member_id,
                    topic_uuid: request.cursor.topic_id,
                    range_uuid: request.cursor.range_id,
                    topic_epoch: request.cursor.topic_epoch,
                    range_generation: request.cursor.range_generation,
                    segment_uuid: request.cursor.segment_id,
                    segment_generation: request.cursor.segment_generation,
                    segment_root: request.cursor.segment_root,
                    record_offset: request.cursor.record_offset,
                    record_index: request.cursor.record_index,
                    lineage_transition_id: request.cursor.lineage_transition_id,
                    expected_checkpoint_generation: request.expected_checkpoint_generation,
                };
                match store.apply(command) {
                    MetadataResponse::CursorCommitted {
                        checkpoint_generation,
                    } => WireFrame {
                        request_id,
                        stream_id,
                        message: Message::CommitCursorResponse(CommitCursorResponse {
                            checkpoint_generation,
                        }),
                    },
                    MetadataResponse::Rejected(error_kind) => {
                        let (code, message) = map_metadata_error(&error_kind);
                        error(request_id, stream_id, code, &message)
                    }
                    other => error(
                        request_id,
                        stream_id,
                        ErrorCode::Storage,
                        &format!("unexpected metadata response for cursor commit: {other:?}"),
                    ),
                }
            }
            Message::FetchCursorRequest(request) => {
                if role != Role::Consumer {
                    return error(
                        request_id,
                        stream_id,
                        ErrorCode::Unauthorized,
                        "session role cannot fetch cursors",
                    );
                }
                let Some(store) = self.group_checkpoints.as_ref() else {
                    return error(
                        request_id,
                        stream_id,
                        ErrorCode::InvalidRequest,
                        "broker has no group checkpoint store configured",
                    );
                };
                let cursor = store.with_state(|state| {
                    let key = MetaKey::GroupCursor {
                        group_uuid: request.group_id,
                        topic_uuid: request.topic_id,
                        range_uuid: request.range_id,
                    };
                    match state.record(&key) {
                        Some(MetaValue::GroupCursor(record)) => Some(LineageCursor {
                            group_id: request.group_id,
                            topic_id: request.topic_id,
                            topic_epoch: record.topic_epoch,
                            range_id: request.range_id,
                            range_generation: record.range_generation,
                            segment_id: record.segment_uuid,
                            segment_generation: record.segment_generation,
                            segment_root: record.segment_root,
                            record_offset: record.record_offset,
                            record_index: record.record_index,
                            lineage_transition_id: record.lineage_transition_id,
                            checkpoint_generation: record.checkpoint_generation,
                        }),
                        _ => None,
                    }
                });
                WireFrame {
                    request_id,
                    stream_id,
                    message: Message::FetchCursorResponse(FetchCursorResponse { cursor }),
                }
            }
            _ => error(
                request_id,
                stream_id,
                ErrorCode::InvalidRequest,
                "expected produce, fetch, or cursor request",
            ),
        }
    }

    fn handle_produce(
        &self,
        role: Role,
        request_id: u64,
        stream_id: u64,
        request: vtop_protocol::ProduceRequest,
        conn: Option<&ConnectionBudget>,
    ) -> WireFrame {
        if role != Role::Producer {
            return error(
                request_id,
                stream_id,
                ErrorCode::Unauthorized,
                "session role cannot produce",
            );
        }
        if let Some(frame) = self.validate_produce_admission(request_id, stream_id, &request) {
            return frame;
        }
        let queued = QueuedProduce::new(request_id, stream_id, request);
        let reservation = match self.memory.try_reserve_produce(queued.payload_bytes, conn) {
            Ok(reservation) => reservation,
            Err(reason) => {
                return overloaded_budget(request_id, stream_id, reason);
            }
        };
        if let Some(coordinator) = &self.group_commit {
            let frame =
                coordinator.enqueue_and_wait(queued, |batch| self.flush_produce_group(batch));
            drop(reservation);
            return frame;
        }
        let frame = self
            .flush_produce_group(std::slice::from_ref(&queued))
            .into_iter()
            .next()
            .expect("single produce flush returns one frame");
        drop(reservation);
        frame
    }

    /// Returns `Some(error_frame)` when the request must be rejected before
    /// joining a commit group.
    fn validate_produce_admission(
        &self,
        request_id: u64,
        stream_id: u64,
        request: &vtop_protocol::ProduceRequest,
    ) -> Option<WireFrame> {
        if request.records.is_empty() {
            return Some(error(
                request_id,
                stream_id,
                ErrorCode::InvalidRequest,
                "produce request has no records",
            ));
        }
        for record in &request.records {
            if let Err(reason) = self
                .memory
                .check_record_size(record.key.len(), record.value.len())
            {
                // Oversized records are rejected before expensive allocation;
                // not retryable — the client must shrink the record.
                return Some(error(
                    request_id,
                    stream_id,
                    ErrorCode::InvalidRequest,
                    reject_message(reason),
                ));
            }
        }
        if request.durability != WireDurability::LocalFsync
            && request.durability != WireDurability::Quorum
        {
            return Some(error(
                request_id,
                stream_id,
                ErrorCode::InvalidRequest,
                "broker acknowledges only LocalFsync or Quorum produce requests",
            ));
        }
        if request.durability == WireDurability::Quorum
            && (self.replicas.is_none() || self.cluster_committed.is_none())
        {
            return Some(error(
                request_id,
                stream_id,
                ErrorCode::InvalidRequest,
                "Quorum durability requires a configured replica set",
            ));
        }
        // Fail closed rather than silently upgrade: a LocalFsync append on a
        // replicated range would land on the leader only, opening a follower
        // gap the client believes is durable, and replicating it anyway would
        // change the acknowledged durability contract under the client.
        if request.durability == WireDurability::LocalFsync && self.replicas.is_some() {
            return Some(error(
                request_id,
                stream_id,
                ErrorCode::InvalidRequest,
                "brokers with a configured replica set accept only Quorum durability produce requests",
            ));
        }
        None
    }

    /// Append every admitted member under one local durability barrier, then
    /// (for Quorum) one replica fan-out / HWM advance covering the group.
    fn flush_produce_group(&self, batch: &[QueuedProduce]) -> Vec<WireFrame> {
        if batch.is_empty() {
            return Vec::new();
        }
        let durability = batch[0].request.durability;
        // Lock order: metadata lease view, then broker state. Hold both only
        // through the local durable append so a concurrent grant/release cannot
        // revoke between the fencing check and fsync. Quorum fan-out runs after
        // these locks are released.
        let prepared = {
            let meta = self.meta_fencing_epoch.lock();
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let tip_before = state.segment.next_offset();
            let mut records = Vec::new();
            let mut member_record_counts = Vec::with_capacity(batch.len());
            let mut early: Vec<Option<WireFrame>> = Vec::with_capacity(batch.len());
            for item in batch {
                if let Err((code, message)) =
                    self.check_range(&meta, &item.request.range, item.request.fencing_epoch)
                {
                    early.push(Some(error(item.request_id, item.stream_id, code, message)));
                    member_record_counts.push(0);
                    continue;
                }
                if let Err(problem) = state
                    .producer_epochs
                    .accept(item.request.producer_id, item.request.producer_epoch)
                {
                    let frame = match problem {
                        BrokerError::ProducerFenced { .. } => error(
                            item.request_id,
                            item.stream_id,
                            ErrorCode::Fenced,
                            &problem.to_string(),
                        ),
                        _ => error(
                            item.request_id,
                            item.stream_id,
                            ErrorCode::Storage,
                            &problem.to_string(),
                        ),
                    };
                    early.push(Some(frame));
                    member_record_counts.push(0);
                    continue;
                }
                let (stored_id, stored_epoch) = match self.segment_format {
                    SegmentFormat::V1 => (
                        storage_producer_id(item.request.producer_id, item.request.producer_epoch),
                        0,
                    ),
                    SegmentFormat::V2 => (item.request.producer_id, item.request.producer_epoch),
                };
                let mut member_records = Vec::with_capacity(item.request.records.len());
                let mut overflow = false;
                for (index, record) in item.request.records.iter().enumerate() {
                    let Some(sequence) = item.request.first_sequence.checked_add(index as u64)
                    else {
                        overflow = true;
                        break;
                    };
                    member_records.push(LogRecord {
                        producer_id: stored_id,
                        producer_epoch: stored_epoch,
                        sequence,
                        timestamp_millis: record.timestamp_millis,
                        attributes: 0,
                        key: record.key.clone(),
                        value: record.value.clone(),
                    });
                }
                if overflow {
                    early.push(Some(error(
                        item.request_id,
                        item.stream_id,
                        ErrorCode::InvalidRequest,
                        "producer sequence range overflows u64",
                    )));
                    member_record_counts.push(0);
                    continue;
                }
                member_record_counts.push(member_records.len());
                records.append(&mut member_records);
                early.push(None);
            }

            if records.is_empty() {
                return early
                    .into_iter()
                    .map(|frame| {
                        frame.expect(
                            "rejected member must carry an error frame when nothing appends",
                        )
                    })
                    .collect();
            }

            // `append_group_minting` rolls the tail at its configured bound
            // and retries the whole group in the successor, so a producer at
            // the bound sees an ack, not `SegmentByteLimit` — that error now
            // reaches a client only for a group larger than a whole segment,
            // which is a configuration problem no roll can fix.
            match state
                .segment
                .append_group_minting(&records, Durability::Fsync)
            {
                Ok(outcomes) => {
                    // Reclaim AFTER the append that may have rolled, under
                    // the same lock, so the sealed prefix never outlives the
                    // policy by more than one group (#290).
                    self.run_retention(&mut state.segment);
                    let leader_committed = state.segment.committed_offset();
                    let mut wire_by_member = Vec::with_capacity(batch.len());
                    let mut cursor = 0usize;
                    for (count, prior) in member_record_counts.into_iter().zip(early) {
                        if let Some(frame) = prior {
                            wire_by_member.push(Err(frame));
                            continue;
                        }
                        let member_outcomes: Vec<ProduceOutcome> = outcomes[cursor..cursor + count]
                            .iter()
                            .map(|outcome| ProduceOutcome {
                                offset: outcome.offset(),
                                duplicate: matches!(outcome, AppendOutcome::Duplicate { .. }),
                            })
                            .collect();
                        cursor += count;
                        wire_by_member.push(Ok(member_outcomes));
                    }
                    (tip_before, leader_committed, wire_by_member)
                }
                Err(problem) => {
                    let code = match &problem {
                        vtop_log::LogError::FirstSequence { .. }
                        | vtop_log::LogError::SequenceGap { .. }
                        | vtop_log::LogError::SequenceConflict { .. }
                        | vtop_log::LogError::SequenceBelowWindow { .. } => {
                            ErrorCode::SequenceConflict
                        }
                        vtop_log::LogError::ProducerFenced { .. } => ErrorCode::Fenced,
                        _ => ErrorCode::Storage,
                    };
                    let message = problem.to_string();
                    return batch
                        .iter()
                        .zip(early)
                        .map(|(item, prior)| {
                            prior.unwrap_or_else(|| {
                                error(item.request_id, item.stream_id, code, &message)
                            })
                        })
                        .collect();
                }
            }
        };

        let (tip_before, leader_committed, wire_by_member) = prepared;
        if durability == WireDurability::LocalFsync {
            return batch
                .iter()
                .zip(wire_by_member)
                .map(|(item, member)| match member {
                    Err(frame) => frame,
                    Ok(outcomes) => WireFrame {
                        request_id: item.request_id,
                        stream_id: item.stream_id,
                        message: Message::ProduceResponse(ProduceResponse {
                            outcomes,
                            committed_next_offset: leader_committed,
                        }),
                    },
                })
                .collect();
        }

        let cluster = self
            .cluster_committed
            .as_ref()
            .expect("Quorum path checked cluster_committed");
        if cluster.get() >= leader_committed {
            let committed = cluster.get();
            return batch
                .iter()
                .zip(wire_by_member)
                .map(|(item, member)| match member {
                    Err(frame) => frame,
                    Ok(outcomes) => WireFrame {
                        request_id: item.request_id,
                        stream_id: item.stream_id,
                        message: Message::ProduceResponse(ProduceResponse {
                            outcomes,
                            committed_next_offset: committed,
                        }),
                    },
                })
                .collect();
        }

        let replicas = self
            .replicas
            .as_ref()
            .expect("Quorum path checked replicas");
        let mut replica_requests = Vec::new();
        let mut offset_cursor = tip_before;
        for (item, member) in batch.iter().zip(wire_by_member.iter()) {
            let Ok(outcomes) = member else {
                continue;
            };
            if outcomes.is_empty() {
                continue;
            }
            // New appends replicate from the pre-append tip. An
            // all-duplicate retry after a prior quorum failure uses
            // the lowest assigned offset so lagging followers can
            // catch up from the original base.
            let member_end = outcomes
                .iter()
                .map(|outcome| outcome.offset.saturating_add(1))
                .max()
                .unwrap_or(offset_cursor);
            let expected_base_offset = if offset_cursor < member_end {
                offset_cursor
            } else {
                outcomes
                    .iter()
                    .map(|outcome| outcome.offset)
                    .min()
                    .unwrap_or(offset_cursor)
            };
            replica_requests.push(ReplicaAppendRequest {
                range: item.request.range.clone(),
                fencing_epoch: item.request.fencing_epoch,
                leader_node_id: self.node_id,
                expected_base_offset,
                producer_id: item.request.producer_id,
                producer_epoch: item.request.producer_epoch,
                first_sequence: item.request.first_sequence,
                records: item.request.records.clone(),
            });
            offset_cursor = member_end.max(offset_cursor);
        }

        let quorum = replicas.replicate_append_batch(&replica_requests, leader_committed);
        if !quorum.has_quorum() {
            let message = format!(
                "quorum not reached: {} follower ack(s), need majority of {}",
                quorum.follower_acks, quorum.replication_factor
            );
            return batch
                .iter()
                .zip(wire_by_member)
                .map(|(item, member)| match member {
                    Err(frame) => frame,
                    Ok(_) => error(
                        item.request_id,
                        item.stream_id,
                        ErrorCode::Overloaded,
                        &message,
                    ),
                })
                .collect();
        }

        // Re-validate the lease before publishing cluster commit.
        let hwm_epoch = batch[0].request.fencing_epoch;
        let hwm_range = batch[0].request.range.clone();
        let committed = {
            let meta = self.meta_fencing_epoch.lock();
            for item in batch {
                if let Err((code, message)) =
                    self.check_range(&meta, &item.request.range, item.request.fencing_epoch)
                {
                    return batch
                        .iter()
                        .zip(wire_by_member)
                        .map(|(member_item, member)| match member {
                            Err(frame) => frame,
                            Ok(_) => {
                                error(member_item.request_id, member_item.stream_id, code, message)
                            }
                        })
                        .collect();
                }
            }
            cluster.advance_to(leader_committed)
        };
        replicas.propagate_committed_hwm(&CommittedHwmUpdate {
            range: hwm_range,
            fencing_epoch: hwm_epoch,
            committed_high_watermark: committed,
        });
        batch
            .iter()
            .zip(wire_by_member)
            .map(|(item, member)| match member {
                Err(frame) => frame,
                Ok(outcomes) => WireFrame {
                    request_id: item.request_id,
                    stream_id: item.stream_id,
                    message: Message::ProduceResponse(ProduceResponse {
                        outcomes,
                        committed_next_offset: committed,
                    }),
                },
            })
            .collect()
    }

    fn check_range(
        &self,
        meta: &MetaLeaseState,
        range: &RangeIdentity,
        fencing_epoch: u64,
    ) -> Result<(), (ErrorCode, &'static str)> {
        if range != &self.range {
            return Err((
                ErrorCode::WrongRange,
                "request range identity does not match this broker",
            ));
        }
        if fencing_epoch != self.held_fencing_epoch() {
            return Err((
                ErrorCode::Fenced,
                "request fencing epoch does not match this broker's lease",
            ));
        }
        // Release clears lease_active without bumping the epoch; a steal
        // advances fencing_epoch. Either case fences this leaseholder.
        if !meta.lease_active || meta.fencing_epoch != self.held_fencing_epoch() {
            return Err((
                ErrorCode::Fenced,
                "broker lease is inactive or fenced by a newer metadata grant",
            ));
        }
        Ok(())
    }
}

fn map_metadata_error(error: &MetadataError) -> (ErrorCode, String) {
    match error {
        MetadataError::GenerationMismatch { .. } => {
            (ErrorCode::CheckpointConflict, error.to_string())
        }
        MetadataError::EpochMismatch { .. } | MetadataError::LineageMismatch { .. } => {
            (ErrorCode::WrongLineage, error.to_string())
        }
        MetadataError::AlreadyExists | MetadataError::NotFound => {
            (ErrorCode::InvalidRequest, error.to_string())
        }
        MetadataError::InvalidTransition(detail) => (ErrorCode::WrongLineage, detail.clone()),
        MetadataError::Limit(detail) => (ErrorCode::InvalidRequest, detail.clone()),
    }
}

fn error(request_id: u64, stream_id: u64, code: ErrorCode, message: &str) -> WireFrame {
    let mut end = message.len().min(vtop_protocol::MAX_ERROR_BYTES);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    let message = message[..end].to_owned();
    WireFrame {
        request_id,
        stream_id,
        message: Message::Error(ErrorResponse {
            code,
            retryable: matches!(code, ErrorCode::Overloaded | ErrorCode::Storage),
            message,
        }),
    }
}

fn overloaded_budget(request_id: u64, stream_id: u64, reason: BudgetRejectReason) -> WireFrame {
    let _ = reason.default_action(); // document action catalog; PR1 uses RejectRetryable
    debug_assert_eq!(reason.default_action(), OverloadAction::RejectRetryable);
    error(
        request_id,
        stream_id,
        ErrorCode::Overloaded,
        reject_message(reason),
    )
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub cluster_id: Uuid,
    pub node_id: Uuid,
    /// Segment format the server expects its broker to write. V2 is an
    /// explicit opt-in; the default keeps every existing deployment on v1.
    pub segment_format: SegmentFormat,
    pub max_frame_bytes: u32,
    pub max_records_per_frame: u32,
    pub window_bytes: u64,
    pub max_sessions: usize,
    pub max_inflight_requests: usize,
    pub handshake_timeout: Duration,
    pub idle_timeout: Duration,
}

impl ServerConfig {
    pub fn validate(&self) -> BrokerResult<()> {
        if self.max_frame_bytes < 1024 || self.max_frame_bytes > ABSOLUTE_MAX_FRAME_BYTES {
            return Err(BrokerError::InvalidConfig(format!(
                "max_frame_bytes must be in 1024..={ABSOLUTE_MAX_FRAME_BYTES}"
            )));
        }
        if self.window_bytes == 0 || self.window_bytes > MAX_WINDOW_BYTES {
            return Err(BrokerError::InvalidConfig(format!(
                "window_bytes must be in 1..={MAX_WINDOW_BYTES}"
            )));
        }
        if self.max_records_per_frame == 0 || self.max_records_per_frame > ABSOLUTE_MAX_RECORDS {
            return Err(BrokerError::InvalidConfig(format!(
                "max_records_per_frame must be in 1..={ABSOLUTE_MAX_RECORDS}"
            )));
        }
        if self.max_sessions == 0 || self.max_inflight_requests == 0 {
            return Err(BrokerError::InvalidConfig(
                "session and in-flight request limits must be non-zero".to_owned(),
            ));
        }
        if self.handshake_timeout.is_zero() || self.idle_timeout.is_zero() {
            return Err(BrokerError::InvalidConfig(
                "timeouts must be non-zero".to_owned(),
            ));
        }
        Ok(())
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            cluster_id: Uuid::nil(),
            node_id: Uuid::nil(),
            segment_format: SegmentFormat::V1,
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_records_per_frame: DEFAULT_MAX_RECORDS,
            window_bytes: u64::from(DEFAULT_MAX_FRAME_BYTES),
            max_sessions: 1024,
            max_inflight_requests: 128,
            handshake_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(30),
        }
    }
}

pub struct ServerTlsMaterial {
    pub certificate_chain: Vec<CertificateDer<'static>>,
    pub private_key: PrivateKeyDer<'static>,
    pub client_roots: rustls::RootCertStore,
}

/// Maps an authenticated TLS certificate chain and declared principal to the
/// narrow role allowed on a session. The server has no permissive fallback:
/// callers must supply an authorization policy explicitly.
pub trait SessionAuthorizer: Send + Sync + 'static {
    fn authorize(&self, peer_chain_der: &[Vec<u8>], principal_id: Uuid, role: Role) -> bool;
}

/// The broker behind a listener that OUTLIVES any one broker (#284).
///
/// A candidate's native listener binds once and never moves, but the
/// [`LocalBroker`] behind it is rebuilt at each role transition. The slot
/// is read at session ACCEPT: sessions in flight keep the broker they
/// started with — which fails closed the moment it is fenced — and new
/// sessions see the current one. An EMPTY slot refuses the socket
/// outright: a candidate that is not leading has no broker, and spending
/// a TLS handshake to say so would be politeness at the price of load.
#[derive(Default)]
pub struct BrokerSlot {
    inner: std::sync::RwLock<Option<Arc<LocalBroker>>>,
}

impl BrokerSlot {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn holding(broker: Arc<LocalBroker>) -> Self {
        Self {
            inner: std::sync::RwLock::new(Some(broker)),
        }
    }

    /// Install the broker a promotion built. The caller is responsible for
    /// the #280 ordering — the previous broker must have been drained and
    /// quiesced before its successor is installed.
    pub fn install(&self, broker: Arc<LocalBroker>) {
        *self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(broker);
    }

    /// Empty the slot; new sessions are refused until the next install.
    pub fn clear(&self) {
        *self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    pub fn current(&self) -> Option<Arc<LocalBroker>> {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

pub struct NativeServer {
    broker: Arc<BrokerSlot>,
    authorizer: Arc<dyn SessionAuthorizer>,
    acceptor: TlsAcceptor,
    config: ServerConfig,
    sessions: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    metrics: Arc<ServerMetrics>,
}

impl NativeServer {
    /// Build an mTLS server restricted to TLS 1.3.
    pub fn new(
        broker: Arc<LocalBroker>,
        tls: ServerTlsMaterial,
        authorizer: Arc<dyn SessionAuthorizer>,
        config: ServerConfig,
    ) -> BrokerResult<Self> {
        // The configured format is a declaration of intent; refusing a
        // mismatch here keeps a v1 deployment from silently serving a v2
        // segment (or the reverse) after a bad rollout.
        if config.segment_format != broker.segment_format() {
            return Err(BrokerError::InvalidConfig(format!(
                "configured segment format {:?} does not match the broker's active segment ({:?})",
                config.segment_format,
                broker.segment_format()
            )));
        }
        Self::over_slot(
            Arc::new(BrokerSlot::holding(broker)),
            tls,
            authorizer,
            config,
        )
    }

    /// Build the server over a [`BrokerSlot`], for a candidate whose broker
    /// comes and goes with the lease (#284). The format declaration is
    /// checked per ACCEPT rather than at construction — the slot may be
    /// empty now and hold a broker later — and a mismatched install refuses
    /// sessions loudly instead of serving the wrong format.
    pub fn over_slot(
        broker: Arc<BrokerSlot>,
        tls: ServerTlsMaterial,
        authorizer: Arc<dyn SessionAuthorizer>,
        config: ServerConfig,
    ) -> BrokerResult<Self> {
        config.validate()?;
        // Pin the provider: workspace feature unification can enable more
        // than one rustls backend, and process-level auto-detection then
        // aborts instead of choosing.
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(tls.client_roots),
            Arc::clone(&provider),
        )
        .build()
        .map_err(|error| {
            BrokerError::InvalidConfig(format!("client certificate roots: {error}"))
        })?;
        let tls_config = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_client_cert_verifier(verifier)
            .with_single_cert(tls.certificate_chain, tls.private_key)?;
        Ok(Self {
            broker,
            authorizer,
            acceptor: TlsAcceptor::from(Arc::new(tls_config)),
            sessions: Arc::new(Semaphore::new(config.max_sessions)),
            requests: Arc::new(Semaphore::new(config.max_inflight_requests)),
            metrics: Arc::new(ServerMetrics::new()),
            config,
        })
    }

    /// This server's request-path counters (#224).
    ///
    /// Always present rather than opt-in: the recording cost is one relaxed
    /// atomic add per request, and an optional path would mean an embedded
    /// broker and a node disagreeing about what they can tell you. Take the
    /// handle before [`Self::serve`], which consumes the server.
    pub fn metrics(&self) -> &Arc<ServerMetrics> {
        &self.metrics
    }

    pub async fn serve(
        self,
        listener: TcpListener,
        mut shutdown: oneshot::Receiver<()>,
    ) -> BrokerResult<()> {
        let mut sessions = JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                completed = sessions.join_next(), if !sessions.is_empty() => {
                    if let Some(result) = completed { result?; }
                }
                accepted = listener.accept() => {
                    let (socket, peer) = match accepted {
                        Ok(value) => value,
                        Err(source) => return Err(BrokerError::Io { path: PathBuf::from("tcp-listener"), source }),
                    };
                    let Ok(permit) = Arc::clone(&self.sessions).try_acquire_owned() else {
                        // Explicit overload: refuse the socket rather than queue
                        // unbounded accept work. No request was accepted.
                        self.metrics.session_refused_at_capacity();
                        drop(socket);
                        continue;
                    };
                    // Read AT ACCEPT: sessions keep the broker they started
                    // with (it fails closed once fenced); new sessions see
                    // the slot's current holder. Empty — a candidate not
                    // currently leading — refuses the socket (#284).
                    let Some(broker) = self.broker.current() else {
                        self.metrics.session_refused_no_broker();
                        drop(socket);
                        continue;
                    };
                    if self.config.segment_format != broker.segment_format() {
                        // A mismatched install: refuse loudly rather than
                        // serve a format the deployment did not declare.
                        self.metrics.session_refused_no_broker();
                        eprintln!(
                            "refusing session: installed broker serves {:?} but this listener \
                             declared {:?}",
                            broker.segment_format(),
                            self.config.segment_format
                        );
                        drop(socket);
                        continue;
                    }
                    let context = SessionContext {
                        acceptor: self.acceptor.clone(),
                        broker,
                        authorizer: Arc::clone(&self.authorizer),
                        requests: Arc::clone(&self.requests),
                        config: self.config.clone(),
                        metrics: Arc::clone(&self.metrics),
                    };
                    sessions.spawn(async move {
                        let _permit = permit;
                        let _ = serve_connection(socket, peer, context).await;
                    });
                }
            }
        }
        sessions.abort_all();
        while let Some(result) = sessions.join_next().await {
            if let Err(problem) = result {
                if !problem.is_cancelled() {
                    return Err(problem.into());
                }
            }
        }
        Ok(())
    }
}

async fn write_session_frame(
    stream: &mut tokio_rustls::server::TlsStream<TcpStream>,
    frame: &WireFrame,
    limits: ProtocolLimits,
    write_timeout: Duration,
) -> BrokerResult<()> {
    timeout(write_timeout, write_frame(stream, frame, limits))
        .await
        .map_err(|_| BrokerError::Timeout("protocol response write"))??;
    Ok(())
}

/// Closes the active-session gauge on every exit path from the session loop.
struct SessionGuard {
    metrics: Arc<ServerMetrics>,
    role: Role,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.metrics.session_closed(self.role);
    }
}

/// Records one request's outcome exactly once, on whichever path it leaves by.
///
/// The session loop answers a request from a dozen places: the broker's own
/// reply, but also capacity refusals, budget rejections, authorization
/// failures, and the encode/window checks that can turn a successful broker
/// response into an error frame after the fact. Accounting written at the
/// happy path alone under-counts refusals precisely when refusals are the
/// story, and over-counts successes for a fetch the client never received.
///
/// So the default on drop is `Error`, and a caller must explicitly claim
/// success. A future `continue` added to this loop is then counted correctly
/// without anyone remembering to; the failure mode of forgetting is an
/// over-reported error rate, which is investigated, rather than an
/// under-reported one, which is not.
struct RequestRecord<'a> {
    metrics: &'a ServerMetrics,
    kind: RequestKind,
    started: std::time::Instant,
    recorded: bool,
}

impl<'a> RequestRecord<'a> {
    fn new(metrics: &'a ServerMetrics, kind: RequestKind) -> Self {
        Self {
            metrics,
            kind,
            started: std::time::Instant::now(),
            recorded: false,
        }
    }

    fn complete(&mut self, outcome: RequestOutcome) {
        if self.recorded {
            return;
        }
        self.recorded = true;
        // Measured around the broker's own work, not the socket write: this is
        // the number that answers "is the log slow", and folding a slow
        // consumer's TCP backpressure into it would hide that answer.
        self.metrics
            .request_completed(self.kind, outcome, self.started.elapsed());
    }
}

impl Drop for RequestRecord<'_> {
    fn drop(&mut self) {
        self.complete(RequestOutcome::Error);
    }
}

/// Everything one session needs from the server that spawned it.
///
/// Bundled rather than passed as eight positional arguments: they are all
/// clones of server-wide state with the same lifetime, and a long positional
/// list is where a future edit swaps two `Arc`s of compatible type without the
/// compiler noticing.
struct SessionContext {
    acceptor: TlsAcceptor,
    broker: Arc<LocalBroker>,
    authorizer: Arc<dyn SessionAuthorizer>,
    requests: Arc<Semaphore>,
    config: ServerConfig,
    metrics: Arc<ServerMetrics>,
}

async fn serve_connection(
    socket: TcpStream,
    _peer: SocketAddr,
    context: SessionContext,
) -> BrokerResult<()> {
    let SessionContext {
        acceptor,
        broker,
        authorizer,
        requests,
        config,
        metrics,
    } = context;
    let handshake = timeout(config.handshake_timeout, acceptor.accept(socket)).await;
    let mut stream = match handshake {
        Err(_) => {
            metrics.session_refused_handshake();
            return Err(BrokerError::Timeout("TLS handshake"));
        }
        Ok(Err(source)) => {
            metrics.session_refused_handshake();
            return Err(BrokerError::Io {
                path: PathBuf::from("tls-session"),
                source,
            });
        }
        Ok(Ok(stream)) => stream,
    };
    let peer_chain_der = stream
        .get_ref()
        .1
        .peer_certificates()
        .unwrap_or_default()
        .iter()
        .map(|certificate| certificate.as_ref().to_vec())
        .collect::<Vec<_>>();
    let initial_limits = ProtocolLimits {
        max_frame_bytes: config.max_frame_bytes,
        max_records: config.max_records_per_frame,
    };
    let frame = match timeout(
        config.handshake_timeout,
        read_frame(&mut stream, initial_limits),
    )
    .await
    {
        Err(_) => {
            metrics.session_refused_handshake();
            return Err(BrokerError::Timeout("protocol handshake"));
        }
        Ok(Err(problem)) => {
            metrics.session_refused_handshake();
            return Err(problem.into());
        }
        Ok(Ok(frame)) => frame,
    };
    let Some(WireFrame {
        request_id: 0,
        stream_id: 0,
        message: Message::ClientHello(hello),
    }) = frame
    else {
        metrics.session_refused_handshake();
        return Ok(());
    };
    if !authorizer.authorize(&peer_chain_der, hello.principal_id, hello.role) {
        metrics.session_refused_unauthorized();
        write_session_frame(
            &mut stream,
            &error(
                0,
                0,
                ErrorCode::Unauthorized,
                "certificate is not authorized for the requested principal and role",
            ),
            initial_limits,
            config.idle_timeout,
        )
        .await?;
        return Ok(());
    }
    let (role, negotiated_limits, negotiated_window) = match negotiate(&hello, &config) {
        Ok(value) => value,
        Err((code, message)) => {
            metrics.session_refused_handshake();
            write_session_frame(
                &mut stream,
                &error(0, 0, code, message),
                initial_limits,
                config.idle_timeout,
            )
            .await?;
            return Ok(());
        }
    };
    let first_nonce = Uuid::new_v4();
    let second_nonce = Uuid::new_v4();
    let mut session_nonce = [0_u8; 32];
    session_nonce[..16].copy_from_slice(first_nonce.as_bytes());
    session_nonce[16..].copy_from_slice(second_nonce.as_bytes());
    let ack = WireFrame {
        request_id: 0,
        stream_id: 0,
        message: Message::ServerHello(ServerHello {
            cluster_id: config.cluster_id,
            node_id: config.node_id,
            selected_major: PROTOCOL_MAJOR,
            selected_minor: PROTOCOL_MINOR,
            max_frame_bytes: negotiated_limits.max_frame_bytes,
            max_records: negotiated_limits.max_records,
            // This first implementation processes one request at a time per
            // connection and bounds concurrency across sessions globally.
            max_inflight_requests: 1,
            initial_window_bytes: negotiated_window,
            session_nonce,
        }),
    };
    write_session_frame(&mut stream, &ack, negotiated_limits, config.idle_timeout).await?;
    metrics.session_opened(role);
    // The session loop below leaves through a dozen `return` sites, several of
    // them error paths. A guard closes the gauge on all of them; a manual
    // decrement at each `return` is exactly the bookkeeping that goes stale on
    // the next edit and leaves the active-session count climbing forever.
    let _session = SessionGuard {
        metrics: Arc::clone(&metrics),
        role,
    };

    let mut last_request_id = 0_u64;
    let mut send_credit = negotiated_window;
    let principal_id = hello.principal_id;
    let conn_budget = match role {
        Role::Consumer => broker.memory_budget().open_consumer_connection(),
        Role::Producer | Role::Peer | Role::Administrator => {
            broker.memory_budget().open_producer_connection()
        }
    };
    // When true, stop admitting new work frames until WindowUpdate restores
    // send credit (OverloadAction::PauseReads for slow consumers).
    let mut pause_reads = false;
    loop {
        if pause_reads {
            // Only accept WindowUpdate / Ping while paused; other frames are
            // rejected retryably so accepted produce/fetch work is never dropped.
            let frame = match timeout(
                config.idle_timeout,
                read_frame(&mut stream, negotiated_limits),
            )
            .await
            {
                Err(_) => return Ok(()),
                Ok(Err(problem)) => return Err(problem.into()),
                Ok(Ok(None)) => return Ok(()),
                Ok(Ok(Some(frame))) => frame,
            };
            let request_id = frame.request_id;
            let stream_id = frame.stream_id;
            // Defaults to Error on drop; each arm below claims success only
            // when the client actually got the answer it asked for.
            let mut record = RequestRecord::new(&metrics, RequestKind::of(&frame.message));
            match frame.message {
                Message::WindowUpdate(update) => {
                    if update.additional_bytes == 0 {
                        let response = error(
                            request_id,
                            0,
                            ErrorCode::InvalidRequest,
                            "window update must add at least one byte",
                        );
                        write_session_frame(
                            &mut stream,
                            &response,
                            negotiated_limits,
                            config.idle_timeout,
                        )
                        .await?;
                    } else {
                        send_credit = send_credit
                            .saturating_add(update.additional_bytes)
                            .min(config.window_bytes);
                        if send_credit > 0 {
                            pause_reads = false;
                        }
                        record.complete(RequestOutcome::Ok);
                    }
                    continue;
                }
                Message::Ping => {
                    write_session_frame(
                        &mut stream,
                        &WireFrame {
                            request_id,
                            stream_id,
                            message: Message::Pong,
                        },
                        negotiated_limits,
                        config.idle_timeout,
                    )
                    .await?;
                    record.complete(RequestOutcome::Ok);
                    continue;
                }
                _ => {
                    let response =
                        overloaded_budget(request_id, stream_id, BudgetRejectReason::ConsumerConn);
                    write_session_frame(
                        &mut stream,
                        &response,
                        negotiated_limits,
                        config.idle_timeout,
                    )
                    .await?;
                    continue;
                }
            }
        }
        let frame = match timeout(
            config.idle_timeout,
            read_frame(&mut stream, negotiated_limits),
        )
        .await
        {
            Err(_) => return Ok(()),
            Ok(Err(problem)) => return Err(problem.into()),
            Ok(Ok(None)) => return Ok(()),
            Ok(Ok(Some(frame))) => frame,
        };
        let request_id = frame.request_id;
        // Created here, before ANY of the refusal paths below, so a capacity
        // refusal, an authorization failure, or a budget rejection is counted
        // as the refused request it is. This is the accounting that used to
        // sit only on the happy path and went flat exactly when the broker was
        // busy refusing work.
        let mut record = RequestRecord::new(&metrics, RequestKind::of(&frame.message));
        if request_id == 0 || request_id <= last_request_id {
            let response = error(
                request_id,
                frame.stream_id,
                ErrorCode::InvalidRequest,
                "request IDs must be non-zero and strictly increasing per session",
            );
            write_session_frame(
                &mut stream,
                &response,
                negotiated_limits,
                config.idle_timeout,
            )
            .await?;
            continue;
        }
        last_request_id = request_id;
        if matches!(
            &frame.message,
            Message::ProduceRequest(request) if request.producer_id != principal_id
        ) {
            let response = error(
                request_id,
                frame.stream_id,
                ErrorCode::Unauthorized,
                "producer ID must equal the authenticated session principal ID",
            );
            write_session_frame(
                &mut stream,
                &response,
                negotiated_limits,
                config.idle_timeout,
            )
            .await?;
            continue;
        }
        // Mirror of the producer rule above: a consumer-group member identity
        // is only trusted when it is the TLS-authenticated principal, so one
        // authenticated consumer cannot move another member's cursor.
        if matches!(
            &frame.message,
            Message::CommitCursorRequest(request) if request.member_id != principal_id
        ) {
            let response = error(
                request_id,
                frame.stream_id,
                ErrorCode::Unauthorized,
                "member ID must equal the authenticated session principal ID",
            );
            write_session_frame(
                &mut stream,
                &response,
                negotiated_limits,
                config.idle_timeout,
            )
            .await?;
            continue;
        }
        // The fetch budget reservation is an RAII guard scoped to the rest of
        // this loop iteration: it must stay alive until this request's
        // response bytes have been written to the session (every exit path
        // included). Dropping it earlier releases the fetch-response-queue
        // budget while the bytes still sit in the write path, so
        // slow-consumer pileups would go unaccounted exactly when they
        // matter.
        let (frame, _fetch_reservation): (WireFrame, Option<BudgetReservation>) = match frame {
            WireFrame {
                message: Message::WindowUpdate(update),
                ..
            } => {
                if update.additional_bytes == 0 {
                    let response = error(
                        request_id,
                        0,
                        ErrorCode::InvalidRequest,
                        "window update must add at least one byte",
                    );
                    write_session_frame(
                        &mut stream,
                        &response,
                        negotiated_limits,
                        config.idle_timeout,
                    )
                    .await?;
                } else {
                    send_credit = send_credit
                        .saturating_add(update.additional_bytes)
                        .min(config.window_bytes);
                    if send_credit > 0 {
                        pause_reads = false;
                    }
                    record.complete(RequestOutcome::Ok);
                }
                continue;
            }
            WireFrame {
                request_id,
                stream_id,
                message: Message::Ping,
            } => {
                write_session_frame(
                    &mut stream,
                    &WireFrame {
                        request_id,
                        stream_id,
                        message: Message::Pong,
                    },
                    negotiated_limits,
                    config.idle_timeout,
                )
                .await?;
                record.complete(RequestOutcome::Ok);
                continue;
            }
            WireFrame {
                request_id,
                stream_id,
                message: Message::FetchRequest(mut request),
            } => {
                if send_credit == 0 {
                    let response = error(
                        request_id,
                        stream_id,
                        ErrorCode::Overloaded,
                        "session byte window is exhausted; send WindowUpdate",
                    );
                    write_session_frame(
                        &mut stream,
                        &response,
                        negotiated_limits,
                        config.idle_timeout,
                    )
                    .await?;
                    // PauseReads: stop admitting fetch work until credit returns.
                    pause_reads = true;
                    broker
                        .memory_budget()
                        .record_rejection(BudgetRejectReason::ConsumerConn);
                    continue;
                }
                // Budget the log fetch in log-encoded bytes, which bound the
                // wire bytes from above (the storage frame carries more fixed
                // overhead per record than the wire frame); 128 covers the
                // response's fixed fields. A first record excluded by this
                // budget is still served alone by the progress refetch in
                // `LocalBroker::handle`.
                let response_budget = negotiated_limits
                    .max_frame_bytes
                    .saturating_sub(vtop_protocol::HEADER_LEN as u32 + 128)
                    .max(1);
                request.max_bytes = request
                    .max_bytes
                    .min(u32::try_from(send_credit).unwrap_or(u32::MAX))
                    .min(response_budget);
                let reservation = match broker
                    .memory_budget()
                    .try_reserve_fetch(u64::from(request.max_bytes), &conn_budget)
                {
                    Ok(reservation) => reservation,
                    Err(reason) => {
                        let response = overloaded_budget(request_id, stream_id, reason);
                        write_session_frame(
                            &mut stream,
                            &response,
                            negotiated_limits,
                            config.idle_timeout,
                        )
                        .await?;
                        if matches!(
                            reason,
                            BudgetRejectReason::ConsumerConn | BudgetRejectReason::FetchQueue
                        ) {
                            pause_reads = true;
                        }
                        continue;
                    }
                };
                (
                    WireFrame {
                        request_id,
                        stream_id,
                        message: Message::FetchRequest(request),
                    },
                    Some(reservation),
                )
            }
            value => (value, None),
        };
        let Ok(permit) = Arc::clone(&requests).try_acquire_owned() else {
            let response = error(
                request_id,
                frame.stream_id,
                ErrorCode::Overloaded,
                "broker request capacity is exhausted",
            );
            write_session_frame(
                &mut stream,
                &response,
                negotiated_limits,
                config.idle_timeout,
            )
            .await?;
            continue;
        };
        // Size the request before it moves into the blocking task. Produce
        // volume is taken from the request rather than the response because
        // the response carries only offsets, and it is recorded only once the
        // FINAL frame is known, so neither a refused append nor a fetch the
        // client never received is counted as accepted throughput.
        let produced = match &frame.message {
            Message::ProduceRequest(request) => Some((
                request.records.len() as u64,
                request
                    .records
                    .iter()
                    .map(|record| (record.key.len() + record.value.len()) as u64)
                    .sum::<u64>(),
            )),
            _ => None,
        };

        let broker = Arc::clone(&broker);
        let session_budget = conn_budget.clone();
        let response = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            broker.handle_with_connection(role, frame, Some(&session_budget))
        })
        .await?;
        // The broker's verdict is not yet final: the encode-limit and
        // send-credit checks below can still turn this into an error frame, so
        // nothing is claimed until the bytes the client will actually receive
        // are known.
        let broker_outcome = RequestOutcome::of(&response.message);
        let fetched = match &response.message {
            Message::FetchResponse(fetched) => Some((
                fetched.records.len() as u64,
                fetched
                    .records
                    .iter()
                    .map(|record| (record.key.len() + record.value.len()) as u64)
                    .sum::<u64>(),
            )),
            _ => None,
        };
        let is_fetch_response = fetched.is_some();
        let encoded = match encode_frame(&response, negotiated_limits) {
            Ok(encoded) => encoded,
            // Only a single-record progress fetch can exceed the negotiated
            // frame: the client must reconnect with a larger frame limit, so
            // answer with a terminal error instead of dropping the session.
            Err(vtop_protocol::ProtocolError::Limit(_)) if is_fetch_response => {
                let response = error(
                    request_id,
                    response.stream_id,
                    ErrorCode::InvalidRequest,
                    "the next record exceeds the negotiated frame limit; reconnect with a larger max_frame_bytes",
                );
                write_session_frame(
                    &mut stream,
                    &response,
                    negotiated_limits,
                    config.idle_timeout,
                )
                .await?;
                continue;
            }
            Err(problem) => return Err(problem.into()),
        };
        if is_fetch_response {
            let response_bytes = encoded.len() as u64;
            if response_bytes > send_credit {
                let response = error(
                    request_id,
                    response.stream_id,
                    ErrorCode::Overloaded,
                    "session byte window is exhausted; send WindowUpdate",
                );
                write_session_frame(
                    &mut stream,
                    &response,
                    negotiated_limits,
                    config.idle_timeout,
                )
                .await?;
                pause_reads = true;
                continue;
            }
            send_credit -= response_bytes;
        }
        write_session_bytes(&mut stream, &encoded, config.idle_timeout).await?;
        // The client has the bytes: this is the first point at which the
        // outcome and the volume are true.
        record.complete(broker_outcome);
        if broker_outcome == RequestOutcome::Ok {
            if let Some((records, bytes)) = produced {
                metrics.produced(records, bytes);
            }
            if let Some((records, bytes)) = fetched {
                metrics.fetched(records, bytes);
            }
        }
    }
}

async fn write_session_bytes(
    stream: &mut tokio_rustls::server::TlsStream<TcpStream>,
    encoded: &[u8],
    write_timeout: Duration,
) -> BrokerResult<()> {
    use tokio::io::AsyncWriteExt;
    timeout(write_timeout, async {
        stream.write_all(encoded).await?;
        stream.flush().await
    })
    .await
    .map_err(|_| BrokerError::Timeout("protocol response write"))?
    .map_err(|source| BrokerError::Io {
        path: PathBuf::from("tls-session"),
        source,
    })?;
    Ok(())
}

fn negotiate(
    hello: &ClientHello,
    config: &ServerConfig,
) -> Result<(Role, ProtocolLimits, u64), (ErrorCode, &'static str)> {
    if hello.cluster_id != config.cluster_id {
        return Err((ErrorCode::WrongCluster, "cluster identity mismatch"));
    }
    if hello.minimum_major > PROTOCOL_MAJOR || hello.maximum_major < PROTOCOL_MAJOR {
        return Err((
            ErrorCode::UnsupportedVersion,
            "no common protocol major version",
        ));
    }
    if hello.requested_max_frame_bytes < 1024
        || hello.requested_max_frame_bytes > ABSOLUTE_MAX_FRAME_BYTES
    {
        return Err((ErrorCode::InvalidRequest, "invalid client frame limit"));
    }
    if hello.requested_max_inflight_requests == 0 {
        return Err((
            ErrorCode::InvalidRequest,
            "invalid client in-flight request limit",
        ));
    }
    if hello.requested_max_records == 0 || hello.requested_max_records > ABSOLUTE_MAX_RECORDS {
        return Err((ErrorCode::InvalidRequest, "invalid client record limit"));
    }
    if hello.initial_window_bytes == 0 || hello.initial_window_bytes > MAX_WINDOW_BYTES {
        return Err((ErrorCode::InvalidRequest, "invalid client receive window"));
    }
    Ok((
        hello.role,
        ProtocolLimits {
            max_frame_bytes: hello.requested_max_frame_bytes.min(config.max_frame_bytes),
            max_records: hello
                .requested_max_records
                .min(config.max_records_per_frame),
        },
        hello.initial_window_bytes.min(config.window_bytes),
    ))
}

fn io_error(path: &Path, source: std::io::Error) -> BrokerError {
    BrokerError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(unix)]
fn sync_parent(storage: &dyn Storage, path: &Path) -> BrokerResult<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    storage
        .sync_dir(parent)
        .map_err(|source| io_error(parent, source))
}

#[cfg(not(unix))]
fn sync_parent(_storage: &dyn Storage, _path: &Path) -> BrokerResult<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{generate_simple_self_signed, CertifiedKey};
    use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
    use std::fs::OpenOptions;
    use tempfile::TempDir;
    use tokio_rustls::TlsConnector;
    use vtop_log::{
        ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentConfigV2, SegmentDescriptor,
        SegmentDescriptorV2,
    };
    use vtop_protocol::{
        ClientHello, CommitCursorRequest, FetchRequest, ProduceRecord, ProduceRequest,
    };

    /// The whole point of the non-blocking accessor: a scrape must give up
    /// rather than queue behind a produce that is mid-fsync.
    #[test]
    fn a_lease_snapshot_yields_rather_than_waiting_for_the_append_path() {
        let lease = MetaFencingEpoch::new(7);
        assert_eq!(lease.try_snapshot(), Some((7, true)));

        let held = lease.lock();
        assert_eq!(
            lease.try_snapshot(),
            None,
            "a contended view must report contention, not block the caller"
        );
        drop(held);

        lease.clear_lease(7);
        assert_eq!(
            lease.try_snapshot(),
            Some((7, false)),
            "the epoch is retained on release; only the lease bit drops"
        );
    }

    /// A lease-driven broker must not serve on its configured epoch before
    /// the agent's first successful acquisition: authority comes from
    /// metadata, and at startup metadata has said nothing yet.
    #[test]
    fn an_inactive_view_stays_fenced_until_the_first_grant() {
        let lease = MetaFencingEpoch::new_inactive(3);
        assert_eq!(
            lease.try_snapshot(),
            Some((3, false)),
            "no grant has been observed; the broker must fail closed"
        );

        // A grant below the configured floor is stale by definition.
        lease.set(2);
        assert_eq!(lease.try_snapshot(), Some((3, false)));

        // The restart case: metadata still records this node's lease at the
        // configured epoch, and the agent's first renewal republishes it.
        lease.set(3);
        assert_eq!(
            lease.try_snapshot(),
            Some((3, true)),
            "republishing the configured epoch must activate the view"
        );
    }

    /// The distinction between suspending and releasing, pinned: a holder
    /// that could not verify its promotion must stop serving, but its own
    /// live grant must still be able to reactivate the view on retry. A
    /// release here would wedge the range under its own lease.
    #[test]
    fn a_suspended_epoch_reactivates_on_the_same_grant_where_a_release_would_not() {
        let lease = MetaFencingEpoch::new_inactive(0);
        lease.set(5);
        assert_eq!(lease.try_snapshot(), Some((5, true)));

        lease.suspend(5);
        assert_eq!(
            lease.try_snapshot(),
            Some((5, false)),
            "a suspended holder must not serve"
        );
        lease.set(5);
        assert_eq!(
            lease.try_snapshot(),
            Some((5, true)),
            "the same live grant must reactivate a suspended view"
        );

        // A stale suspension must not deactivate a newer grant.
        lease.set(6);
        lease.suspend(5);
        assert_eq!(lease.try_snapshot(), Some((6, true)));

        // Contrast with release: the epoch is finished for good.
        lease.clear_lease(6);
        lease.set(6);
        assert_eq!(
            lease.try_snapshot(),
            Some((6, false)),
            "a released epoch stays finished; only a newer grant serves again"
        );
    }

    struct TestAuthorizer {
        leaf_der: Vec<u8>,
        principal_id: Uuid,
    }

    impl SessionAuthorizer for TestAuthorizer {
        fn authorize(&self, peer_chain_der: &[Vec<u8>], principal_id: Uuid, role: Role) -> bool {
            peer_chain_der.first() == Some(&self.leaf_der)
                && principal_id == self.principal_id
                && matches!(role, Role::Producer | Role::Consumer)
        }
    }

    fn fixture() -> (TempDir, Arc<LocalBroker>, RangeIdentity) {
        let dir = tempfile::tempdir().unwrap();
        let range_id = Uuid::from_u128(10);
        let range = RangeIdentity {
            topic: "native".to_owned(),
            topic_epoch: 1,
            range_id,
            range_generation: 0,
        };
        let descriptor = SegmentDescriptor {
            segment_id: Uuid::from_u128(11),
            topic: range.topic.clone(),
            topic_epoch: range.topic_epoch,
            lineage: RangeLineage {
                range_id,
                generation: 0,
                key_range: KeyRange::full(),
                parents: Vec::new(),
            },
            base_offset: 0,
        };
        let segment = ActiveSegment::create(
            dir.path().join("native.active"),
            descriptor,
            SegmentConfig::default(),
        )
        .unwrap();
        let epochs = ProducerEpochJournal::open(dir.path().join("native.epochs")).unwrap();
        let broker = Arc::new(LocalBroker::new(segment, epochs, range.clone(), 7).unwrap());
        (dir, broker, range)
    }

    /// A v2-mode broker over a fresh proof-carrying segment. Sealing is not
    /// broker API, so broker tests never seal this segment.
    fn fixture_v2(dir: &TempDir, journal_name: &str) -> (Arc<LocalBroker>, RangeIdentity) {
        let range_id = Uuid::from_u128(50);
        let range = RangeIdentity {
            topic: "native-v2".to_owned(),
            topic_epoch: 1,
            range_id,
            range_generation: 0,
        };
        let descriptor = SegmentDescriptorV2 {
            segment_id: Uuid::from_u128(51),
            topic: range.topic.clone(),
            topic_epoch: range.topic_epoch,
            lineage: RangeLineage::root(range_id),
            base_offset: 0,
            segment_generation: 0,
            creation_node_id: Uuid::from_u128(52),
            creation_fencing_epoch: 7,
        };
        let path = dir.path().join("native-v2.active");
        let segment = if path.exists() {
            ActiveSegment::recover(&path).unwrap()
        } else {
            ActiveSegment::create_v2(&path, descriptor, SegmentConfigV2::default()).unwrap()
        };
        let epochs = ProducerEpochJournal::open(dir.path().join(journal_name)).unwrap();
        let broker = Arc::new(LocalBroker::new(segment, epochs, range.clone(), 7).unwrap());
        assert_eq!(broker.segment_format(), SegmentFormat::V2);
        (broker, range)
    }

    fn produce(
        range: RangeIdentity,
        producer_id: Uuid,
        epoch: u64,
        sequence: u64,
        request_id: u64,
    ) -> WireFrame {
        WireFrame {
            request_id,
            stream_id: 1,
            message: Message::ProduceRequest(ProduceRequest {
                range,
                fencing_epoch: 7,
                producer_id,
                producer_epoch: epoch,
                first_sequence: sequence,
                durability: WireDurability::LocalFsync,
                records: vec![ProduceRecord {
                    timestamp_millis: 42,
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                }],
            }),
        }
    }

    #[test]
    fn durable_ack_fetch_and_epoch_fencing() {
        let (_dir, broker, range) = fixture();
        let producer = Uuid::from_u128(12);
        let first = broker.handle(Role::Producer, produce(range.clone(), producer, 1, 0, 1));
        let Message::ProduceResponse(first) = first.message else {
            panic!("expected ack")
        };
        assert_eq!(first.committed_next_offset, 1);
        let duplicate = broker.handle(Role::Producer, produce(range.clone(), producer, 1, 0, 2));
        let Message::ProduceResponse(duplicate) = duplicate.message else {
            panic!("expected duplicate ack")
        };
        assert!(duplicate.outcomes[0].duplicate);

        let newer = broker.handle(Role::Producer, produce(range.clone(), producer, 2, 0, 3));
        assert!(matches!(newer.message, Message::ProduceResponse(_)));
        let gap = broker.handle(Role::Producer, produce(range.clone(), producer, 2, 2, 4));
        assert!(matches!(
            gap.message,
            Message::Error(ErrorResponse {
                code: ErrorCode::SequenceConflict,
                ..
            })
        ));
        let stale = broker.handle(Role::Producer, produce(range.clone(), producer, 1, 1, 5));
        assert!(matches!(
            stale.message,
            Message::Error(ErrorResponse {
                code: ErrorCode::Fenced,
                ..
            })
        ));

        let fetched = broker.handle(
            Role::Consumer,
            WireFrame {
                request_id: 6,
                stream_id: 1,
                message: Message::FetchRequest(FetchRequest {
                    range,
                    fencing_epoch: 7,
                    start_offset: 0,
                    max_bytes: 4096,
                    max_records: 10,
                }),
            },
        );
        let Message::FetchResponse(fetched) = fetched.message else {
            panic!("expected fetch response")
        };
        assert_eq!(fetched.records.len(), 2);
        assert_eq!(fetched.committed_high_watermark, 2);
    }

    #[test]
    fn metadata_epoch_bump_fences_prior_leaseholder() {
        let dir = tempfile::tempdir().unwrap();
        let range_id = Uuid::from_u128(42);
        let range = RangeIdentity {
            topic: "native".to_owned(),
            topic_epoch: 1,
            range_id,
            range_generation: 0,
        };
        let descriptor = SegmentDescriptor {
            segment_id: Uuid::from_u128(7),
            topic: range.topic.clone(),
            topic_epoch: range.topic_epoch,
            lineage: RangeLineage {
                range_id,
                generation: 0,
                key_range: KeyRange::full(),
                parents: Vec::new(),
            },
            base_offset: 0,
        };
        let segment = ActiveSegment::create(
            dir.path().join("native.active"),
            descriptor,
            SegmentConfig::default(),
        )
        .unwrap();
        let epochs = ProducerEpochJournal::open(dir.path().join("native.epochs")).unwrap();
        let meta_epoch = MetaFencingEpoch::new(1);
        let broker = LocalBroker::with_meta_fencing_epoch(
            segment,
            epochs,
            range.clone(),
            1,
            meta_epoch.clone(),
        )
        .unwrap();

        let producer = Uuid::from_u128(12);
        let ok = broker.handle(
            Role::Producer,
            produce_at(range.clone(), 1, producer, 1, 0, 1),
        );
        assert!(matches!(ok.message, Message::ProduceResponse(_)));

        // Release keeps the epoch number but clears lease liveness.
        meta_epoch.clear_lease(1);
        let released = broker.handle(
            Role::Producer,
            produce_at(range.clone(), 1, producer, 1, 1, 2),
        );
        assert!(matches!(
            released.message,
            Message::Error(ErrorResponse {
                code: ErrorCode::Fenced,
                ..
            })
        ));

        // Re-grant at a newer epoch, then steal again via set(3).
        meta_epoch.set(2);
        let still_old = broker.handle(Role::Producer, produce_at(range, 1, producer, 1, 2, 3));
        assert!(matches!(
            still_old.message,
            Message::Error(ErrorResponse {
                code: ErrorCode::Fenced,
                ..
            })
        ));
    }

    fn produce_at(
        range: RangeIdentity,
        fencing_epoch: u64,
        producer_id: Uuid,
        epoch: u64,
        sequence: u64,
        request_id: u64,
    ) -> WireFrame {
        WireFrame {
            request_id,
            stream_id: 1,
            message: Message::ProduceRequest(ProduceRequest {
                range,
                fencing_epoch,
                producer_id,
                producer_epoch: epoch,
                first_sequence: sequence,
                durability: WireDurability::LocalFsync,
                records: vec![ProduceRecord {
                    timestamp_millis: 42,
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                }],
            }),
        }
    }

    #[test]
    fn fetch_returns_the_first_record_even_when_the_byte_budget_excludes_it() {
        let (_dir, broker, range) = fixture();
        let producer = Uuid::from_u128(40);
        let ack = broker.handle(Role::Producer, produce(range.clone(), producer, 1, 0, 1));
        assert!(matches!(ack.message, Message::ProduceResponse(_)));
        let fetched = broker.handle(
            Role::Consumer,
            WireFrame {
                request_id: 2,
                stream_id: 1,
                message: Message::FetchRequest(FetchRequest {
                    range,
                    fencing_epoch: 7,
                    start_offset: 0,
                    max_bytes: 1,
                    max_records: 10,
                }),
            },
        );
        let Message::FetchResponse(fetched) = fetched.message else {
            panic!("expected fetch response")
        };
        assert_eq!(fetched.records.len(), 1);
        assert_eq!(fetched.next_offset, 1);
        assert_eq!(fetched.committed_high_watermark, 1);
    }

    #[test]
    fn v2_mode_broker_persists_real_producer_epochs_and_fences_stale_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let producer = Uuid::from_u128(53);
        {
            let (broker, range) = fixture_v2(&dir, "native-v2.epochs");
            let first = broker.handle(Role::Producer, produce(range.clone(), producer, 1, 0, 1));
            let Message::ProduceResponse(first) = first.message else {
                panic!("expected ack for the epoch-1 produce")
            };
            assert_eq!(first.committed_next_offset, 1);

            // An epoch bump restarts the per-(producer, epoch) sequence at 0.
            let bumped = broker.handle(Role::Producer, produce(range.clone(), producer, 2, 0, 2));
            let Message::ProduceResponse(bumped) = bumped.message else {
                panic!("expected ack for the epoch-2 produce")
            };
            assert_eq!(bumped.committed_next_offset, 2);

            let fetched = broker.handle(
                Role::Consumer,
                WireFrame {
                    request_id: 3,
                    stream_id: 1,
                    message: Message::FetchRequest(FetchRequest {
                        range: range.clone(),
                        fencing_epoch: 7,
                        start_offset: 0,
                        max_bytes: 4096,
                        max_records: 10,
                    }),
                },
            );
            let Message::FetchResponse(fetched) = fetched.message else {
                panic!("expected fetch response")
            };
            assert_eq!(fetched.records.len(), 2);

            // The durable journal has seen epoch 2, so the older session is
            // fenced before its records ever reach the segment.
            let stale = broker.handle(Role::Producer, produce(range.clone(), producer, 1, 1, 4));
            assert!(matches!(
                stale.message,
                Message::Error(ErrorResponse {
                    code: ErrorCode::Fenced,
                    ..
                })
            ));
        }

        // Reopen the segment directly: v2 frames must carry the producer's
        // real id and epoch instead of a derived storage id with epoch 0.
        let mut recovered = ActiveSegment::recover(dir.path().join("native-v2.active")).unwrap();
        let batch = recovered.fetch(0, usize::MAX, 16).unwrap();
        let stored: Vec<(Uuid, u64, u64)> = batch
            .records
            .iter()
            .map(|entry| {
                (
                    entry.record.producer_id,
                    entry.record.producer_epoch,
                    entry.record.sequence,
                )
            })
            .collect();
        assert_eq!(stored, vec![(producer, 1, 0), (producer, 2, 0)]);
    }

    #[test]
    fn v2_mode_segment_fences_stale_epochs_the_journal_never_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let producer = Uuid::from_u128(54);
        {
            let (broker, range) = fixture_v2(&dir, "first.epochs");
            let newest = broker.handle(Role::Producer, produce(range, producer, 3, 0, 1));
            assert!(matches!(newest.message, Message::ProduceResponse(_)));
        }

        // A fresh journal has no memory of epoch 3, so only the recovered
        // segment's own fencing (LogError::ProducerFenced) can reject the
        // stale session; the broker must surface it as Fenced.
        let (broker, range) = fixture_v2(&dir, "second.epochs");
        let stale = broker.handle(Role::Producer, produce(range, producer, 2, 0, 1));
        assert!(matches!(
            stale.message,
            Message::Error(ErrorResponse {
                code: ErrorCode::Fenced,
                ..
            })
        ));
    }

    #[test]
    fn native_server_rejects_a_config_whose_format_disagrees_with_the_broker() {
        let dir = tempfile::tempdir().unwrap();
        let (broker, _range) = fixture_v2(&dir, "native-v2.epochs");
        let identity = generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let mut client_roots = rustls::RootCertStore::empty();
        client_roots.add(identity.cert.der().clone()).unwrap();
        let result = NativeServer::new(
            broker,
            ServerTlsMaterial {
                certificate_chain: vec![identity.cert.der().clone()],
                private_key: private_key(&identity),
                client_roots,
            },
            Arc::new(TestAuthorizer {
                leaf_der: identity.cert.der().as_ref().to_vec(),
                principal_id: Uuid::from_u128(55),
            }),
            // Default config declares V1, but the broker writes V2 frames.
            ServerConfig::default(),
        );
        let Err(BrokerError::InvalidConfig(message)) = result else {
            panic!("expected the format mismatch to be rejected")
        };
        assert!(message.contains("segment format"));
    }

    #[test]
    fn producer_epoch_survives_clean_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs");
        let producer = Uuid::from_u128(21);
        {
            let mut journal = ProducerEpochJournal::open(&path).unwrap();
            journal.accept(producer, 9).unwrap();
        }
        let mut reopened = ProducerEpochJournal::open(&path).unwrap();
        assert_eq!(reopened.current(producer), Some(9));
        assert!(matches!(
            reopened.accept(producer, 8),
            Err(BrokerError::ProducerFenced { .. })
        ));
    }

    #[test]
    fn producer_epoch_journal_rejects_partial_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs");
        let producer = Uuid::from_u128(21);
        {
            let mut journal = ProducerEpochJournal::open(&path).unwrap();
            journal.accept(producer, 9).unwrap();
        }
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"torn")
            .unwrap();
        assert!(matches!(
            ProducerEpochJournal::open(&path),
            Err(BrokerError::EpochJournalCorrupt(_))
        ));
    }

    #[tokio::test]
    async fn mtls_session_acks_durable_produce_and_fetches_committed_data() {
        let (_dir, broker, range) = fixture();
        let cluster_id = Uuid::from_u128(30);
        let principal_id = Uuid::from_u128(32);
        let server_identity = generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let client_identity = generate_simple_self_signed(vec!["vtop-client".to_owned()]).unwrap();

        let mut client_roots = rustls::RootCertStore::empty();
        client_roots
            .add(client_identity.cert.der().clone())
            .unwrap();
        let server = NativeServer::new(
            broker,
            ServerTlsMaterial {
                certificate_chain: vec![server_identity.cert.der().clone()],
                private_key: private_key(&server_identity),
                client_roots,
            },
            Arc::new(TestAuthorizer {
                leaf_der: client_identity.cert.der().as_ref().to_vec(),
                principal_id,
            }),
            ServerConfig {
                cluster_id,
                node_id: Uuid::from_u128(31),
                segment_format: SegmentFormat::V1,
                max_frame_bytes: 16 * 1024,
                max_records_per_frame: 32,
                window_bytes: 16 * 1024,
                max_sessions: 4,
                max_inflight_requests: 2,
                handshake_timeout: Duration::from_secs(2),
                idle_timeout: Duration::from_secs(2),
            },
        )
        .unwrap();
        let mut server_roots = rustls::RootCertStore::empty();
        server_roots
            .add(server_identity.cert.der().clone())
            .unwrap();
        let client_tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(server_roots)
        .with_client_auth_cert(
            vec![client_identity.cert.der().clone()],
            private_key(&client_identity),
        )
        .unwrap();
        let connector = TlsConnector::from(Arc::new(client_tls));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_task = tokio::spawn(server.serve(listener, shutdown_rx));

        let limits = ProtocolLimits {
            max_frame_bytes: 16 * 1024,
            max_records: 32,
        };
        let (rejected, response) = open_and_hello(
            &connector,
            address,
            cluster_id,
            Uuid::from_u128(999),
            Role::Producer,
            limits,
        )
        .await;
        assert!(matches!(
            response.message,
            Message::Error(ErrorResponse {
                code: ErrorCode::Unauthorized,
                ..
            })
        ));
        drop(rejected);

        let mut producer = connect(
            &connector,
            address,
            cluster_id,
            principal_id,
            Role::Producer,
            limits,
        )
        .await;
        write_frame(
            &mut producer,
            &produce(range.clone(), Uuid::from_u128(999), 9, 0, 1),
            limits,
        )
        .await
        .unwrap();
        let rejected = read_frame(&mut producer, limits).await.unwrap().unwrap();
        assert!(matches!(
            rejected.message,
            Message::Error(ErrorResponse {
                code: ErrorCode::Unauthorized,
                ..
            })
        ));
        write_frame(
            &mut producer,
            &produce(range.clone(), principal_id, 1, 0, 2),
            limits,
        )
        .await
        .unwrap();
        let produced = read_frame(&mut producer, limits).await.unwrap().unwrap();
        let Message::ProduceResponse(produced) = produced.message else {
            panic!("expected produce response")
        };
        assert_eq!(produced.committed_next_offset, 1);
        drop(producer);

        let mut consumer = connect(
            &connector,
            address,
            cluster_id,
            principal_id,
            Role::Consumer,
            limits,
        )
        .await;
        write_frame(
            &mut consumer,
            &WireFrame {
                request_id: 1,
                stream_id: 1,
                message: Message::FetchRequest(FetchRequest {
                    range,
                    fencing_epoch: 7,
                    start_offset: 0,
                    max_bytes: 4096,
                    max_records: 10,
                }),
            },
            limits,
        )
        .await
        .unwrap();
        let fetched = read_frame(&mut consumer, limits).await.unwrap().unwrap();
        let Message::FetchResponse(fetched) = fetched.message else {
            panic!("expected fetch response")
        };
        assert_eq!(fetched.records.len(), 1);
        assert_eq!(fetched.committed_high_watermark, 1);
        drop(consumer);

        shutdown_tx.send(()).unwrap();
        server_task.await.unwrap().unwrap();
    }

    /// Refusals must be counted, not skipped.
    ///
    /// The accounting originally sat only on the path where the broker
    /// answered, so `requests_total{outcome="error"}` stayed flat exactly when
    /// the session layer was busy turning work away — an error-rate panel that
    /// reads healthiest under the conditions that should light it up.
    #[tokio::test]
    async fn session_layer_refusals_are_counted_and_never_credited_as_throughput() {
        let (_dir, broker, range) = fixture();
        let cluster_id = Uuid::from_u128(60);
        let principal_id = Uuid::from_u128(62);
        let server_identity = generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let client_identity = generate_simple_self_signed(vec!["vtop-client".to_owned()]).unwrap();

        let mut client_roots = rustls::RootCertStore::empty();
        client_roots
            .add(client_identity.cert.der().clone())
            .unwrap();
        let server = NativeServer::new(
            broker,
            ServerTlsMaterial {
                certificate_chain: vec![server_identity.cert.der().clone()],
                private_key: private_key(&server_identity),
                client_roots,
            },
            Arc::new(TestAuthorizer {
                leaf_der: client_identity.cert.der().as_ref().to_vec(),
                principal_id,
            }),
            ServerConfig {
                cluster_id,
                node_id: Uuid::from_u128(61),
                segment_format: SegmentFormat::V1,
                max_frame_bytes: 16 * 1024,
                max_records_per_frame: 32,
                window_bytes: 16 * 1024,
                max_sessions: 4,
                max_inflight_requests: 2,
                handshake_timeout: Duration::from_secs(2),
                idle_timeout: Duration::from_secs(2),
            },
        )
        .unwrap();
        // Taken before `serve`, which consumes the server.
        let metrics = Arc::clone(server.metrics());

        let mut server_roots = rustls::RootCertStore::empty();
        server_roots
            .add(server_identity.cert.der().clone())
            .unwrap();
        let client_tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(server_roots)
        .with_client_auth_cert(
            vec![client_identity.cert.der().clone()],
            private_key(&client_identity),
        )
        .unwrap();
        let connector = TlsConnector::from(Arc::new(client_tls));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_task = tokio::spawn(server.serve(listener, shutdown_rx));

        let limits = ProtocolLimits {
            max_frame_bytes: 16 * 1024,
            max_records: 32,
        };
        let mut producer = connect(
            &connector,
            address,
            cluster_id,
            principal_id,
            Role::Producer,
            limits,
        )
        .await;

        // Refused by the session layer before the broker ever sees it: the
        // producer id does not match the authenticated principal.
        write_frame(
            &mut producer,
            &produce(range.clone(), Uuid::from_u128(999), 9, 0, 1),
            limits,
        )
        .await
        .unwrap();
        let rejected = read_frame(&mut producer, limits).await.unwrap().unwrap();
        assert!(matches!(
            rejected.message,
            Message::Error(ErrorResponse {
                code: ErrorCode::Unauthorized,
                ..
            })
        ));

        write_frame(
            &mut producer,
            &produce(range.clone(), principal_id, 1, 0, 2),
            limits,
        )
        .await
        .unwrap();
        let produced = read_frame(&mut producer, limits).await.unwrap().unwrap();
        assert!(matches!(produced.message, Message::ProduceResponse(_)));
        drop(producer);

        shutdown_tx.send(()).unwrap();
        server_task.await.unwrap().unwrap();

        assert_eq!(
            metrics.requests_total(RequestKind::Produce, RequestOutcome::Error),
            1,
            "a session-layer refusal must be counted as a refused request"
        );
        assert_eq!(
            metrics.requests_total(RequestKind::Produce, RequestOutcome::Ok),
            1
        );
        // Both requests carried one record; only the accepted one is
        // throughput.
        assert_eq!(
            metrics.records_produced_total(),
            1,
            "a refused append must never be credited as accepted volume"
        );
    }

    #[tokio::test]
    async fn mtls_session_binds_cursor_member_id_to_the_authenticated_principal() {
        let (_dir, broker, _range) = fixture();
        let cluster_id = Uuid::from_u128(60);
        let principal_id = Uuid::from_u128(61);
        let server_identity = generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let client_identity = generate_simple_self_signed(vec!["vtop-client".to_owned()]).unwrap();

        let mut client_roots = rustls::RootCertStore::empty();
        client_roots
            .add(client_identity.cert.der().clone())
            .unwrap();
        let server = NativeServer::new(
            broker,
            ServerTlsMaterial {
                certificate_chain: vec![server_identity.cert.der().clone()],
                private_key: private_key(&server_identity),
                client_roots,
            },
            Arc::new(TestAuthorizer {
                leaf_der: client_identity.cert.der().as_ref().to_vec(),
                principal_id,
            }),
            ServerConfig {
                cluster_id,
                node_id: Uuid::from_u128(62),
                segment_format: SegmentFormat::V1,
                max_frame_bytes: 16 * 1024,
                max_records_per_frame: 32,
                window_bytes: 16 * 1024,
                max_sessions: 4,
                max_inflight_requests: 2,
                handshake_timeout: Duration::from_secs(2),
                idle_timeout: Duration::from_secs(2),
            },
        )
        .unwrap();
        let mut server_roots = rustls::RootCertStore::empty();
        server_roots
            .add(server_identity.cert.der().clone())
            .unwrap();
        let client_tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(server_roots)
        .with_client_auth_cert(
            vec![client_identity.cert.der().clone()],
            private_key(&client_identity),
        )
        .unwrap();
        let connector = TlsConnector::from(Arc::new(client_tls));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_task = tokio::spawn(server.serve(listener, shutdown_rx));

        let limits = ProtocolLimits {
            max_frame_bytes: 16 * 1024,
            max_records: 32,
        };
        let mut consumer = connect(
            &connector,
            address,
            cluster_id,
            principal_id,
            Role::Consumer,
            limits,
        )
        .await;
        let commit_as = |member_id: Uuid, request_id: u64| WireFrame {
            request_id,
            stream_id: 1,
            message: Message::CommitCursorRequest(CommitCursorRequest {
                operation_id: Uuid::from_u128(79),
                member_id,
                cursor: LineageCursor {
                    group_id: Uuid::from_u128(70),
                    topic_id: Uuid::from_u128(71),
                    topic_epoch: 1,
                    range_id: Uuid::from_u128(72),
                    range_generation: 0,
                    segment_id: Uuid::from_u128(73),
                    segment_generation: 0,
                    segment_root: [3; 32],
                    record_offset: 1,
                    record_index: 0,
                    lineage_transition_id: None,
                    checkpoint_generation: 0,
                },
                expected_checkpoint_generation: None,
            }),
        };

        // A member id that is not the authenticated principal is refused at
        // the session boundary, before the broker touches metadata.
        write_frame(&mut consumer, &commit_as(Uuid::from_u128(999), 1), limits)
            .await
            .unwrap();
        let rejected = read_frame(&mut consumer, limits).await.unwrap().unwrap();
        assert!(
            matches!(
                rejected.message,
                Message::Error(ErrorResponse {
                    code: ErrorCode::Unauthorized,
                    ..
                })
            ),
            "{:?}",
            rejected.message
        );

        // The principal's own member id passes the identity gate; this broker
        // has no checkpoint store, so the request reaches the handler and is
        // answered with InvalidRequest rather than Unauthorized.
        write_frame(&mut consumer, &commit_as(principal_id, 2), limits)
            .await
            .unwrap();
        let handled = read_frame(&mut consumer, limits).await.unwrap().unwrap();
        assert!(
            matches!(
                handled.message,
                Message::Error(ErrorResponse {
                    code: ErrorCode::InvalidRequest,
                    ..
                })
            ),
            "{:?}",
            handled.message
        );
        drop(consumer);

        shutdown_tx.send(()).unwrap();
        server_task.await.unwrap().unwrap();
    }

    fn private_key(identity: &CertifiedKey<rcgen::KeyPair>) -> PrivateKeyDer<'static> {
        PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der()).into()
    }

    async fn connect(
        connector: &TlsConnector,
        address: SocketAddr,
        cluster_id: Uuid,
        principal_id: Uuid,
        role: Role,
        limits: ProtocolLimits,
    ) -> tokio_rustls::client::TlsStream<TcpStream> {
        let (stream, hello) =
            open_and_hello(connector, address, cluster_id, principal_id, role, limits).await;
        assert!(matches!(hello.message, Message::ServerHello(_)));
        stream
    }

    async fn open_and_hello(
        connector: &TlsConnector,
        address: SocketAddr,
        cluster_id: Uuid,
        principal_id: Uuid,
        role: Role,
        limits: ProtocolLimits,
    ) -> (tokio_rustls::client::TlsStream<TcpStream>, WireFrame) {
        let socket = TcpStream::connect(address).await.unwrap();
        let mut stream = connector
            .connect(ServerName::try_from("localhost").unwrap(), socket)
            .await
            .unwrap();
        write_frame(
            &mut stream,
            &WireFrame {
                request_id: 0,
                stream_id: 0,
                message: Message::ClientHello(ClientHello {
                    cluster_id,
                    principal_id,
                    role,
                    minimum_major: PROTOCOL_MAJOR,
                    maximum_major: PROTOCOL_MAJOR,
                    requested_max_frame_bytes: limits.max_frame_bytes,
                    requested_max_records: limits.max_records,
                    requested_max_inflight_requests: 1,
                    initial_window_bytes: u64::from(limits.max_frame_bytes),
                    session_nonce: [7; 32],
                }),
            },
            limits,
        )
        .await
        .unwrap();
        let hello = read_frame(&mut stream, limits).await.unwrap().unwrap();
        (stream, hello)
    }
}

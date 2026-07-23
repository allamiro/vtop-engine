//! Single-node native VTOP broker with a bounded TLS transport.
//!
//! This slice deliberately has no Kafka, database, object-store, or consensus
//! dependency. Produce acknowledgements use the local `Fsync` durability
//! boundary, fetches stop at the committed high-water mark, and producer epochs
//! are fenced by a separate durable append-only journal.
//!
//! Range leadership is gated by a metadata-issued lease view
//! ([`MetaFencingEpoch`]): the broker holds the epoch it was granted and, on
//! every produce/fetch, locks that shared view while validating and mutating
//! storage. Observers publish grants and releases into the same handle; a
//! newer grant or a release fences the prior leaseholder before the next
//! durable append can complete.

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
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
use vtop_log::{ActiveSegment, AppendOutcome, Durability, LogRecord};
use vtop_protocol::{
    encode_frame, read_frame, write_frame, ClientHello, Durability as WireDurability, ErrorCode,
    ErrorResponse, FetchResponse, FetchedRecord, Message, ProduceOutcome, ProduceResponse,
    ProtocolLimits, RangeIdentity, Role, ServerHello, WireFrame, ABSOLUTE_MAX_FRAME_BYTES,
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
    #[error("TLS configuration error: {0}")]
    Tls(#[from] rustls::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] vtop_protocol::ProtocolError),
    #[error("server task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("{0} timed out")]
    Timeout(&'static str),
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

fn storage_producer_id(producer_id: Uuid, epoch: u64) -> Uuid {
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

struct BrokerState {
    segment: ActiveSegment,
    producer_epochs: ProducerEpochJournal,
}

/// Shared metadata lease view for a range.
///
/// Observers publish grants ([`MetaFencingEpoch::set`]) and releases
/// ([`MetaFencingEpoch::clear_lease`]). Brokers lock this view for the
/// entire produce/fetch critical section so a concurrent revocation cannot
/// race past the fencing check into a durable append.
#[derive(Clone, Debug)]
pub struct MetaFencingEpoch {
    state: Arc<Mutex<MetaLeaseState>>,
}

#[derive(Clone, Copy, Debug)]
struct MetaLeaseState {
    /// Latest metadata fencing epoch for the range (never rewound on release).
    fencing_epoch: u64,
    /// Whether a live lease currently exists. Release clears this without
    /// changing `fencing_epoch`.
    lease_active: bool,
}

impl MetaFencingEpoch {
    /// Start with an active lease at `epoch` (fixed single-node / test default).
    pub fn new(epoch: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(MetaLeaseState {
                fencing_epoch: epoch,
                lease_active: true,
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

    /// Publish a metadata grant (including a steal that mints a newer epoch).
    pub fn set(&self, epoch: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.fencing_epoch = epoch;
        state.lease_active = true;
    }

    /// Publish a metadata release: the fencing epoch is retained, but no
    /// leaseholder remains authorized.
    pub fn clear_lease(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.lease_active = false;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MetaLeaseState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Default for MetaFencingEpoch {
    fn default() -> Self {
        Self::new(0)
    }
}

pub struct LocalBroker {
    range: RangeIdentity,
    /// Epoch this broker was granted as range leaseholder.
    held_fencing_epoch: u64,
    /// Latest metadata-committed fencing epoch for the range.
    meta_fencing_epoch: MetaFencingEpoch,
    segment_format: SegmentFormat,
    state: Mutex<BrokerState>,
}

impl LocalBroker {
    /// Construct a broker whose held lease epoch and metadata view start equal.
    ///
    /// Prefer [`Self::with_meta_fencing_epoch`] when the broker should observe
    /// live metadata grants (so a newer grant fences this instance).
    pub fn new(
        segment: ActiveSegment,
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
        segment: ActiveSegment,
        producer_epochs: ProducerEpochJournal,
        range: RangeIdentity,
        held_fencing_epoch: u64,
        meta_fencing_epoch: MetaFencingEpoch,
    ) -> BrokerResult<Self> {
        // The format is derived from the segment itself so the broker's
        // produce-time behavior can never disagree with the on-disk frames.
        let segment_format = if segment.format_version() == vtop_log::FORMAT_VERSION_V2 {
            SegmentFormat::V2
        } else {
            SegmentFormat::V1
        };
        let identity_matches = match segment_format {
            SegmentFormat::V1 => {
                let descriptor = segment.descriptor();
                descriptor.topic == range.topic
                    && descriptor.topic_epoch == range.topic_epoch
                    && descriptor.lineage.range_id == range.range_id
                    && descriptor.lineage.generation == range.range_generation
            }
            SegmentFormat::V2 => {
                let descriptor = segment
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
        Ok(Self {
            range,
            held_fencing_epoch,
            meta_fencing_epoch,
            segment_format,
            state: Mutex::new(BrokerState {
                segment,
                producer_epochs,
            }),
        })
    }

    /// The segment format this broker writes, derived from its segment.
    pub fn segment_format(&self) -> SegmentFormat {
        self.segment_format
    }

    /// Fencing epoch this broker was granted as range leaseholder.
    pub fn held_fencing_epoch(&self) -> u64 {
        self.held_fencing_epoch
    }

    /// Shared metadata fencing-epoch handle observed by this broker.
    pub fn meta_fencing_epoch(&self) -> &MetaFencingEpoch {
        &self.meta_fencing_epoch
    }

    pub fn handle(&self, role: Role, frame: WireFrame) -> WireFrame {
        let WireFrame {
            request_id,
            stream_id,
            message,
        } = frame;
        match message {
            Message::ProduceRequest(request) => {
                if role != Role::Producer {
                    return error(
                        request_id,
                        stream_id,
                        ErrorCode::Unauthorized,
                        "session role cannot produce",
                    );
                }
                if request.records.is_empty() {
                    return error(
                        request_id,
                        stream_id,
                        ErrorCode::InvalidRequest,
                        "produce request has no records",
                    );
                }
                if request.durability != WireDurability::LocalFsync {
                    return error(
                        request_id,
                        stream_id,
                        ErrorCode::InvalidRequest,
                        "the local broker acknowledges only LocalFsync produce requests",
                    );
                }
                // Lock order: metadata lease view, then broker state. Holding
                // the lease lock across the durable append ensures a concurrent
                // grant/release cannot revoke after the fencing check but
                // before fsync.
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
                if let Err(problem) = state
                    .producer_epochs
                    .accept(request.producer_id, request.producer_epoch)
                {
                    return match problem {
                        BrokerError::ProducerFenced { .. } => error(
                            request_id,
                            stream_id,
                            ErrorCode::Fenced,
                            &problem.to_string(),
                        ),
                        _ => error(
                            request_id,
                            stream_id,
                            ErrorCode::Storage,
                            &problem.to_string(),
                        ),
                    };
                }
                // The v1 storage schema cannot carry the epoch, so it is
                // folded into a derived producer id; schema v2 persists the
                // epoch in every frame (attributes stay reserved as zero).
                let (stored_id, stored_epoch) = match self.segment_format {
                    SegmentFormat::V1 => (
                        storage_producer_id(request.producer_id, request.producer_epoch),
                        0,
                    ),
                    SegmentFormat::V2 => (request.producer_id, request.producer_epoch),
                };
                let records = match request
                    .records
                    .into_iter()
                    .enumerate()
                    .map(|(index, record)| {
                        let sequence = request.first_sequence.checked_add(index as u64).ok_or(());
                        sequence.map(|sequence| LogRecord {
                            producer_id: stored_id,
                            producer_epoch: stored_epoch,
                            sequence,
                            timestamp_millis: record.timestamp_millis,
                            attributes: 0,
                            key: record.key,
                            value: record.value,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(records) => records,
                    Err(()) => {
                        return error(
                            request_id,
                            stream_id,
                            ErrorCode::InvalidRequest,
                            "producer sequence range overflows u64",
                        )
                    }
                };
                match state.segment.append_group(&records, Durability::Fsync) {
                    Ok(outcomes) => WireFrame {
                        request_id,
                        stream_id,
                        message: Message::ProduceResponse(ProduceResponse {
                            outcomes: outcomes
                                .into_iter()
                                .map(|outcome| ProduceOutcome {
                                    offset: outcome.offset(),
                                    duplicate: matches!(outcome, AppendOutcome::Duplicate { .. }),
                                })
                                .collect(),
                            committed_next_offset: state.segment.committed_offset(),
                        }),
                    },
                    Err(problem) => error(
                        request_id,
                        stream_id,
                        match problem {
                            vtop_log::LogError::FirstSequence { .. }
                            | vtop_log::LogError::SequenceGap { .. }
                            | vtop_log::LogError::SequenceConflict { .. }
                            | vtop_log::LogError::SequenceBelowWindow { .. } => {
                                ErrorCode::SequenceConflict
                            }
                            // In v2 mode the segment sees real epochs and can
                            // fence a session the journal has not heard from.
                            vtop_log::LogError::ProducerFenced { .. } => ErrorCode::Fenced,
                            _ => ErrorCode::Storage,
                        },
                        &problem.to_string(),
                    ),
                }
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
                let mut fetched = state.segment.fetch(
                    request.start_offset,
                    request.max_bytes as usize,
                    request.max_records as usize,
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
                        fetched = state.segment.fetch(request.start_offset, usize::MAX, 1);
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
                    Err(problem) => error(
                        request_id,
                        stream_id,
                        ErrorCode::Storage,
                        &problem.to_string(),
                    ),
                }
            }
            _ => error(
                request_id,
                stream_id,
                ErrorCode::InvalidRequest,
                "expected produce or fetch request",
            ),
        }
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
        if fencing_epoch != self.held_fencing_epoch {
            return Err((
                ErrorCode::Fenced,
                "request fencing epoch does not match this broker's lease",
            ));
        }
        // Release clears lease_active without bumping the epoch; a steal
        // advances fencing_epoch. Either case fences this leaseholder.
        if !meta.lease_active || meta.fencing_epoch != self.held_fencing_epoch {
            return Err((
                ErrorCode::Fenced,
                "broker lease is inactive or fenced by a newer metadata grant",
            ));
        }
        Ok(())
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

pub struct NativeServer {
    broker: Arc<LocalBroker>,
    authorizer: Arc<dyn SessionAuthorizer>,
    acceptor: TlsAcceptor,
    config: ServerConfig,
    sessions: Arc<Semaphore>,
    requests: Arc<Semaphore>,
}

impl NativeServer {
    /// Build an mTLS server restricted to TLS 1.3.
    pub fn new(
        broker: Arc<LocalBroker>,
        tls: ServerTlsMaterial,
        authorizer: Arc<dyn SessionAuthorizer>,
        config: ServerConfig,
    ) -> BrokerResult<Self> {
        config.validate()?;
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
            config,
        })
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
                        drop(socket);
                        continue;
                    };
                    let acceptor = self.acceptor.clone();
                    let broker = Arc::clone(&self.broker);
                    let authorizer = Arc::clone(&self.authorizer);
                    let requests = Arc::clone(&self.requests);
                    let config = self.config.clone();
                    sessions.spawn(async move {
                        let _permit = permit;
                        let _ = serve_connection(socket, peer, acceptor, broker, authorizer, requests, config).await;
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

async fn serve_connection(
    socket: TcpStream,
    _peer: SocketAddr,
    acceptor: TlsAcceptor,
    broker: Arc<LocalBroker>,
    authorizer: Arc<dyn SessionAuthorizer>,
    requests: Arc<Semaphore>,
    config: ServerConfig,
) -> BrokerResult<()> {
    let mut stream = timeout(config.handshake_timeout, acceptor.accept(socket))
        .await
        .map_err(|_| BrokerError::Timeout("TLS handshake"))?
        .map_err(|source| BrokerError::Io {
            path: PathBuf::from("tls-session"),
            source,
        })?;
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
    let frame = timeout(
        config.handshake_timeout,
        read_frame(&mut stream, initial_limits),
    )
    .await
    .map_err(|_| BrokerError::Timeout("protocol handshake"))??;
    let Some(WireFrame {
        request_id: 0,
        stream_id: 0,
        message: Message::ClientHello(hello),
    }) = frame
    else {
        return Ok(());
    };
    if !authorizer.authorize(&peer_chain_der, hello.principal_id, hello.role) {
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

    let mut last_request_id = 0_u64;
    let mut send_credit = negotiated_window;
    let principal_id = hello.principal_id;
    loop {
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
        let frame = match frame {
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
                WireFrame {
                    request_id,
                    stream_id,
                    message: Message::FetchRequest(request),
                }
            }
            value => value,
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
        let broker = Arc::clone(&broker);
        let response = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            broker.handle(role, frame)
        })
        .await?;
        let is_fetch_response = matches!(response.message, Message::FetchResponse(_));
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
                continue;
            }
            send_credit -= response_bytes;
        }
        write_session_bytes(&mut stream, &encoded, config.idle_timeout).await?;
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
        KeyRange, RangeLineage, SegmentConfig, SegmentConfigV2, SegmentDescriptor,
        SegmentDescriptorV2,
    };
    use vtop_protocol::{ClientHello, FetchRequest, ProduceRecord, ProduceRequest};

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
        meta_epoch.clear_lease();
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

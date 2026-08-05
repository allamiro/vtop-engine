//! Persistent mTLS leader→follower replication streams (#186).
//!
//! [`NetworkedReplicaSet`] implements [`ReplicaSet`] over pipelined VTOP wire
//! frames. Per-follower outstanding-batch / byte windows bound memory and keep
//! a slow non-quorum replica from stalling producer acks indefinitely.
//! Reconnect probes follower status and retransmits from a bounded buffer when
//! the gap is still covered; sealed-segment transfer remains deferred.

use super::{InProcessFollower, ReplicaQuorumResult, ReplicaSet};
use crate::memory_budget::{BudgetRejectReason, FollowerBudget, MemoryBudgetPool};
use crate::BrokerResult;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::server::TlsStream as ServerTlsStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use uuid::Uuid;
use vtop_protocol::{
    read_frame, write_frame, CommittedHwmUpdate, ErrorCode, ErrorResponse, Message, ProtocolLimits,
    RangeIdentity, ReplicaAppendBatchRequest, ReplicaAppendRequest, ReplicaAppendResponse,
    ReplicaStatusRequest, ReplicaStatusResponse, WireFrame, DEFAULT_MAX_FRAME_BYTES,
    DEFAULT_MAX_RECORDS,
};

const REPLICA_LIMITS: ProtocolLimits = ProtocolLimits {
    max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
    max_records: DEFAULT_MAX_RECORDS,
};

/// mTLS material for a replica peer (leader client or follower server).
pub struct ReplicaTlsMaterial {
    pub certificate_chain: Vec<CertificateDer<'static>>,
    pub private_key: PrivateKeyDer<'static>,
    pub trust_roots: RootCertStore,
}

/// Per-follower flow-control and timeout knobs.
#[derive(Clone, Debug)]
pub struct FlowControlConfig {
    pub max_inflight_batches: usize,
    pub max_inflight_bytes: usize,
    pub ack_timeout: Duration,
    pub connect_timeout: Duration,
    pub reconnect_backoff: Duration,
    /// Bound of recently sent batches retained for reconnect catch-up.
    pub max_retransmission_bytes: usize,
}

impl Default for FlowControlConfig {
    fn default() -> Self {
        Self {
            max_inflight_batches: 16,
            max_inflight_bytes: 4 * 1024 * 1024,
            ack_timeout: Duration::from_secs(2),
            connect_timeout: Duration::from_secs(2),
            reconnect_backoff: Duration::from_millis(50),
            max_retransmission_bytes: 8 * 1024 * 1024,
        }
    }
}

/// Dial target for one follower replica.
#[derive(Clone, Debug)]
pub struct NetworkFollowerConfig {
    pub node_id: Uuid,
    pub addr: SocketAddr,
    /// Expected server certificate name (SNI / rustls ServerName).
    pub server_name: String,
}

/// Apply surface hosted by a follower peer server.
pub trait ReplicaPeerHandler: Send + Sync {
    fn node_id(&self) -> Uuid;
    fn apply_append(
        &self,
        request: &ReplicaAppendRequest,
    ) -> Result<ReplicaAppendResponse, (ErrorCode, String)>;
    fn apply_append_batch(
        &self,
        requests: &[ReplicaAppendRequest],
    ) -> Result<ReplicaAppendResponse, (ErrorCode, String)>;
    fn observe_hwm(&self, update: &CommittedHwmUpdate) -> Result<(), (ErrorCode, String)>;
    fn status(
        &self,
        range: &vtop_protocol::RangeIdentity,
    ) -> Result<ReplicaStatusResponse, (ErrorCode, String)>;

    /// Which fencing epoch wrote each stretch of this replica's log (#240).
    ///
    /// Defaults to empty, which means UNKNOWN rather than "no leadership
    /// changes" — the same answer a replica gives when its vector is absent or
    /// broken, and the same decision it licenses: do not reconcile this
    /// replica's offsets by epoch. Defaulted so a handler that does not track
    /// history is honest by construction instead of failing to compile.
    fn epoch_history(
        &self,
        _range: &vtop_protocol::RangeIdentity,
    ) -> Result<Vec<vtop_protocol::ReplicaEpochStart>, (ErrorCode, String)> {
        Ok(Vec::new())
    }

    /// Fence this replica and report what it holds at that instant (#240).
    ///
    /// Unlike [`Self::epoch_history`], this defaults to REFUSING rather than to
    /// a benign empty value, and the asymmetry is deliberate. An unknown
    /// history is a fact a caller can safely act on — it means "do not
    /// reconcile me by epoch". An unknown fence is not: the whole point is that
    /// a fenced replica has stopped moving, and a handler that silently
    /// reported success without fencing anything would have its offset counted
    /// toward a promotion boundary while a deposed leader kept writing to it.
    /// That is the bug this message exists to close, so the default must be the
    /// answer that cannot be mistaken for success.
    fn fence(
        &self,
        _range: &vtop_protocol::RangeIdentity,
        _fencing_epoch: u64,
        _leader_epoch_starts: &[crate::fencing_epochs::EpochStart],
    ) -> Result<vtop_protocol::ReplicaFenceResponse, (ErrorCode, String)> {
        Err((
            ErrorCode::InvalidRequest,
            "this replica cannot be fenced; its offset must not be counted toward a promotion \
             boundary"
                .to_owned(),
        ))
    }
}

impl ReplicaPeerHandler for InProcessFollower {
    fn node_id(&self) -> Uuid {
        InProcessFollower::node_id(self)
    }

    fn apply_append(
        &self,
        request: &ReplicaAppendRequest,
    ) -> Result<ReplicaAppendResponse, (ErrorCode, String)> {
        InProcessFollower::apply_append(self, request)
    }

    fn apply_append_batch(
        &self,
        requests: &[ReplicaAppendRequest],
    ) -> Result<ReplicaAppendResponse, (ErrorCode, String)> {
        InProcessFollower::apply_append_batch(self, requests)
    }

    fn observe_hwm(&self, update: &CommittedHwmUpdate) -> Result<(), (ErrorCode, String)> {
        InProcessFollower::observe_hwm(self, update)
    }

    fn status(
        &self,
        range: &vtop_protocol::RangeIdentity,
    ) -> Result<ReplicaStatusResponse, (ErrorCode, String)> {
        if range != self.range() {
            return Err((
                ErrorCode::WrongRange,
                "replica status range identity does not match this follower".to_owned(),
            ));
        }
        Ok(ReplicaStatusResponse {
            local_committed_offset: self.local_committed_offset(),
            next_offset: self.next_offset(),
        })
    }

    fn epoch_history(
        &self,
        range: &vtop_protocol::RangeIdentity,
    ) -> Result<Vec<vtop_protocol::ReplicaEpochStart>, (ErrorCode, String)> {
        if range != self.range() {
            return Err((
                ErrorCode::WrongRange,
                "epoch history range identity does not match this follower".to_owned(),
            ));
        }
        Ok(InProcessFollower::epoch_starts(self)
            .into_iter()
            .map(|entry| vtop_protocol::ReplicaEpochStart {
                epoch: entry.epoch,
                start_offset: entry.start_offset,
            })
            .collect())
    }

    fn fence(
        &self,
        range: &vtop_protocol::RangeIdentity,
        fencing_epoch: u64,
        leader_epoch_starts: &[crate::fencing_epochs::EpochStart],
    ) -> Result<vtop_protocol::ReplicaFenceResponse, (ErrorCode, String)> {
        if range != self.range() {
            return Err((
                ErrorCode::WrongRange,
                "fence range identity does not match this follower".to_owned(),
            ));
        }
        let outcome = InProcessFollower::fence(self, fencing_epoch, leader_epoch_starts)?;
        Ok(vtop_protocol::ReplicaFenceResponse {
            fencing_epoch: outcome.fencing_epoch,
            local_committed_offset: outcome.local_committed_offset,
            next_offset: outcome.next_offset,
            epoch_starts: outcome
                .epoch_starts
                .into_iter()
                .map(|entry| vtop_protocol::ReplicaEpochStart {
                    epoch: entry.epoch,
                    start_offset: entry.start_offset,
                })
                .collect(),
            truncated_records: outcome.truncated_records,
        })
    }
}

/// Accepts persistent mTLS replication streams and dispatches wire messages.
pub struct ReplicaPeerServer {
    acceptor: TlsAcceptor,
    local_id: Uuid,
    handler: Arc<dyn ReplicaPeerHandler>,
}

impl ReplicaPeerServer {
    pub fn new(
        material: ReplicaTlsMaterial,
        local_id: Uuid,
        handler: Arc<dyn ReplicaPeerHandler>,
    ) -> BrokerResult<Self> {
        Ok(Self {
            acceptor: build_server_acceptor(material)?,
            local_id,
            handler,
        })
    }

    pub async fn serve(self, listener: TcpListener) -> BrokerResult<()> {
        loop {
            let (tcp, _peer) =
                listener
                    .accept()
                    .await
                    .map_err(|source| crate::BrokerError::Io {
                        path: std::path::PathBuf::from("replica-peer-listener"),
                        source,
                    })?;
            let acceptor = self.acceptor.clone();
            let handler = Arc::clone(&self.handler);
            let local_id = self.local_id;
            tokio::spawn(async move {
                let _ = serve_follower_connection(acceptor, tcp, handler, local_id).await;
            });
        }
    }
}

async fn serve_follower_connection(
    acceptor: TlsAcceptor,
    tcp: TcpStream,
    handler: Arc<dyn ReplicaPeerHandler>,
    local_id: Uuid,
) -> BrokerResult<()> {
    let mut stream = acceptor
        .accept(tcp)
        .await
        .map_err(|source| crate::BrokerError::Io {
            path: std::path::PathBuf::from("replica-peer-accept"),
            source,
        })?;
    let peer_id = peer_uuid_from_server_stream(&stream)?;
    // Authenticate the leader cert; local_id is the follower's own identity.
    let _ = (peer_id, local_id);
    loop {
        let frame = match read_frame(&mut stream, REPLICA_LIMITS).await {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(problem) => return Err(problem.into()),
        };
        let response = dispatch_replica_frame(handler.as_ref(), frame);
        if let Some(response) = response {
            write_frame(&mut stream, &response, REPLICA_LIMITS)
                .await
                .map_err(crate::BrokerError::from)?;
        }
    }
}

fn dispatch_replica_frame(handler: &dyn ReplicaPeerHandler, frame: WireFrame) -> Option<WireFrame> {
    let WireFrame {
        request_id,
        stream_id,
        message,
    } = frame;
    let message = match message {
        Message::ReplicaAppendRequest(request) => match handler.apply_append(&request) {
            Ok(response) => Message::ReplicaAppendResponse(response),
            Err((code, message)) => error_message(code, message),
        },
        Message::ReplicaAppendBatchRequest(batch) => {
            match handler.apply_append_batch(&batch.requests) {
                Ok(response) => Message::ReplicaAppendResponse(response),
                Err((code, message)) => error_message(code, message),
            }
        }
        Message::CommittedHwmUpdate(update) => {
            let _ = handler.observe_hwm(&update);
            return None;
        }
        Message::ReplicaEpochHistoryRequest(request) => {
            match handler.epoch_history(&request.range) {
                Ok(epoch_starts) => Message::ReplicaEpochHistoryResponse(
                    vtop_protocol::ReplicaEpochHistoryResponse { epoch_starts },
                ),
                Err((code, message)) => error_message(code, message),
            }
        }
        Message::ReplicaStatusRequest(request) => match handler.status(&request.range) {
            Ok(response) => Message::ReplicaStatusResponse(response),
            Err((code, message)) => error_message(code, message),
        },
        Message::ReplicaFenceRequest(request) => {
            let leader_epoch_starts: Vec<crate::fencing_epochs::EpochStart> = request
                .leader_epoch_starts
                .iter()
                .map(|entry| crate::fencing_epochs::EpochStart {
                    epoch: entry.epoch,
                    start_offset: entry.start_offset,
                })
                .collect();
            match handler.fence(&request.range, request.fencing_epoch, &leader_epoch_starts) {
                Ok(response) => Message::ReplicaFenceResponse(response),
                Err((code, message)) => error_message(code, message),
            }
        }
        _ => error_message(
            ErrorCode::InvalidRequest,
            "unsupported replica peer message".to_owned(),
        ),
    };
    Some(WireFrame {
        request_id,
        stream_id,
        message,
    })
}

fn error_message(code: ErrorCode, message: String) -> Message {
    Message::Error(ErrorResponse {
        code,
        retryable: matches!(code, ErrorCode::Overloaded | ErrorCode::Storage),
        message,
    })
}

/// Networked RF=N replica set (leader is external).
pub struct NetworkedReplicaSet {
    runtime: RuntimeBinding,
    followers: Vec<Arc<FollowerChannel>>,
    flow: FlowControlConfig,
    _shutdown: Vec<mpsc::Sender<()>>,
}

enum RuntimeBinding {
    Owned(Runtime),
    External(Handle),
}

impl RuntimeBinding {
    fn handle(&self) -> Handle {
        match self {
            Self::Owned(runtime) => runtime.handle().clone(),
            Self::External(handle) => handle.clone(),
        }
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        match self {
            Self::Owned(runtime) => runtime.block_on(future),
            Self::External(handle) => handle.block_on(future),
        }
    }
}

impl NetworkedReplicaSet {
    /// Build channels and spawn driver tasks on a dedicated multi-thread runtime.
    pub fn start(
        followers: Vec<NetworkFollowerConfig>,
        leader_tls: ReplicaTlsMaterial,
        flow: FlowControlConfig,
    ) -> BrokerResult<Self> {
        Self::start_with_memory(followers, leader_tls, flow, None)
    }

    /// Like [`Self::start`], sharing a broker-wide [`MemoryBudgetPool`].
    pub fn start_with_memory(
        followers: Vec<NetworkFollowerConfig>,
        leader_tls: ReplicaTlsMaterial,
        flow: FlowControlConfig,
        memory: Option<Arc<MemoryBudgetPool>>,
    ) -> BrokerResult<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("vtop-replica")
            .build()
            .map_err(|source| crate::BrokerError::Io {
                path: std::path::PathBuf::from("replica-runtime"),
                source,
            })?;
        Self::start_on(
            RuntimeBinding::Owned(runtime),
            followers,
            leader_tls,
            flow,
            memory,
        )
    }

    /// Build channels on an existing Tokio handle (tests / embedded runtimes).
    pub fn start_on_handle(
        handle: Handle,
        followers: Vec<NetworkFollowerConfig>,
        leader_tls: ReplicaTlsMaterial,
        flow: FlowControlConfig,
    ) -> BrokerResult<Self> {
        Self::start_on_handle_with_memory(handle, followers, leader_tls, flow, None)
    }

    /// Like [`Self::start_on_handle`], sharing a broker-wide [`MemoryBudgetPool`].
    pub fn start_on_handle_with_memory(
        handle: Handle,
        followers: Vec<NetworkFollowerConfig>,
        leader_tls: ReplicaTlsMaterial,
        flow: FlowControlConfig,
        memory: Option<Arc<MemoryBudgetPool>>,
    ) -> BrokerResult<Self> {
        Self::start_on(
            RuntimeBinding::External(handle),
            followers,
            leader_tls,
            flow,
            memory,
        )
    }

    fn start_on(
        runtime: RuntimeBinding,
        followers: Vec<NetworkFollowerConfig>,
        leader_tls: ReplicaTlsMaterial,
        flow: FlowControlConfig,
        memory: Option<Arc<MemoryBudgetPool>>,
    ) -> BrokerResult<Self> {
        if flow.max_inflight_batches == 0 || flow.max_inflight_bytes == 0 {
            return Err(crate::BrokerError::InvalidConfig(
                "replica flow control windows must be non-zero".to_owned(),
            ));
        }
        let memory = match memory {
            Some(pool) => pool,
            None => MemoryBudgetPool::new(crate::memory_budget::MemoryBudgetConfig {
                per_follower_bytes: flow.max_inflight_bytes.max(1) as u64,
                catch_up_bytes: flow
                    .max_retransmission_bytes
                    .min(flow.max_inflight_bytes.max(1))
                    .max(1) as u64,
                ..crate::memory_budget::MemoryBudgetConfig::default()
            })
            .map_err(crate::BrokerError::InvalidConfig)?,
        };
        let connector = build_client_connector(leader_tls)?;
        let handle = runtime.handle();
        let mut channels = Vec::with_capacity(followers.len());
        let mut shutdown = Vec::with_capacity(followers.len());
        for config in followers {
            let (cmd_tx, cmd_rx) = mpsc::channel(flow.max_inflight_batches.max(1) * 2);
            let (stop_tx, stop_rx) = mpsc::channel(1);
            let budget = Arc::new(memory.open_follower());
            let channel = Arc::new(FollowerChannel {
                node_id: config.node_id,
                durable_offset: AtomicU64::new(0),
                connected: AtomicBool::new(false),
                cmd_tx,
                budget: Arc::clone(&budget),
            });
            let driver = FollowerDriver {
                config,
                connector: connector.clone(),
                flow: flow.clone(),
                channel: Arc::clone(&channel),
                budget,
                cmd_rx,
                stop_rx,
            };
            handle.spawn(driver.run());
            channels.push(channel);
            shutdown.push(stop_tx);
        }
        Ok(Self {
            runtime,
            followers: channels,
            flow,
            _shutdown: shutdown,
        })
    }

    pub fn follower_durable_offset(&self, node_id: Uuid) -> Option<u64> {
        self.followers
            .iter()
            .find(|follower| follower.node_id == node_id)
            .map(|follower| follower.durable_offset.load(Ordering::SeqCst))
    }

    pub fn follower_connected(&self, node_id: Uuid) -> Option<bool> {
        self.followers
            .iter()
            .find(|follower| follower.node_id == node_id)
            .map(|follower| follower.connected.load(Ordering::SeqCst))
    }

    /// Drop the live stream for `node_id` so the driver reconnects (tests).
    pub fn force_reconnect(&self, node_id: Uuid) -> bool {
        let Some(follower) = self
            .followers
            .iter()
            .find(|follower| follower.node_id == node_id)
        else {
            return false;
        };
        follower.connected.store(false, Ordering::SeqCst);
        follower
            .cmd_tx
            .try_send(FollowerCmd::DropConnection)
            .is_ok()
    }
}

impl ReplicaSet for NetworkedReplicaSet {
    fn replication_factor(&self) -> usize {
        1 + self.followers.len()
    }

    fn replicate_append_batch(
        &self,
        requests: &[ReplicaAppendRequest],
        leader_committed_offset: u64,
    ) -> ReplicaQuorumResult {
        self.runtime
            .block_on(self.replicate_append_batch_async(requests, leader_committed_offset))
    }

    fn propagate_committed_hwm(&self, update: &CommittedHwmUpdate) {
        for follower in &self.followers {
            let _ = follower.cmd_tx.try_send(FollowerCmd::PropagateHwm {
                update: update.clone(),
            });
        }
    }
}

impl NetworkedReplicaSet {
    async fn replicate_append_batch_async(
        &self,
        requests: &[ReplicaAppendRequest],
        leader_committed_offset: u64,
    ) -> ReplicaQuorumResult {
        let rf = self.replication_factor();
        let majority_followers = (rf / 2 + 1).saturating_sub(1);
        if self.followers.is_empty() {
            return ReplicaQuorumResult {
                follower_acks: 0,
                replication_factor: rf,
            };
        }
        let batch = Arc::new(requests.to_vec());
        let bytes = approx_batch_bytes(requests);
        let mut set = JoinSet::new();
        for follower in &self.followers {
            let follower = Arc::clone(follower);
            let batch = Arc::clone(&batch);
            let ack_timeout = self.flow.ack_timeout;
            set.spawn(async move {
                follower
                    .submit_and_wait(batch, leader_committed_offset, bytes, ack_timeout)
                    .await
            });
        }

        let mut follower_acks = 0usize;
        let mut finished = 0usize;
        let total = self.followers.len();
        while let Some(joined) = set.join_next().await {
            finished += 1;
            if let Ok(true) = joined {
                follower_acks += 1;
            }
            if follower_acks >= majority_followers || finished == total {
                set.abort_all();
                break;
            }
        }
        ReplicaQuorumResult {
            follower_acks,
            replication_factor: rf,
        }
    }
}

struct FollowerChannel {
    node_id: Uuid,
    durable_offset: AtomicU64,
    connected: AtomicBool,
    cmd_tx: mpsc::Sender<FollowerCmd>,
    budget: Arc<FollowerBudget>,
}

impl FollowerChannel {
    async fn submit_and_wait(
        &self,
        requests: Arc<Vec<ReplicaAppendRequest>>,
        leader_committed_offset: u64,
        bytes: usize,
        ack_timeout: Duration,
    ) -> bool {
        // Charge follower inflight budget before queueing. On reject, treat as
        // a missed ack (explicit overload) so a slow follower cannot grow memory.
        let reservation = match self.budget.try_reserve_inflight(bytes as u64) {
            Ok(reservation) => reservation,
            Err(BudgetRejectReason::ReplicaFollower | BudgetRejectReason::ProcessCeiling) => {
                return false;
            }
            Err(_) => return false,
        };
        let (response_tx, response_rx) = oneshot::channel();
        let cmd = FollowerCmd::Replicate {
            requests,
            leader_committed_offset,
            bytes,
            response_tx,
            catch_up_charged: false,
        };
        if self.cmd_tx.try_send(cmd).is_err() {
            // Channel / window pressure: treat as a missed ack so a slow
            // follower cannot stall the quorum wait. Reservation drops here.
            drop(reservation);
            return false;
        }
        // Driver owns the byte accounting via flow windows for the on-wire
        // path; release the ledger reservation when the waiter completes.
        let acked = match timeout(ack_timeout, response_rx).await {
            Ok(Ok(FollowerReplicateResult::Acked {
                local_committed_offset,
            })) => {
                self.durable_offset
                    .fetch_max(local_committed_offset, Ordering::SeqCst);
                local_committed_offset >= leader_committed_offset
            }
            _ => false,
        };
        drop(reservation);
        acked
    }
}

enum FollowerCmd {
    Replicate {
        requests: Arc<Vec<ReplicaAppendRequest>>,
        leader_committed_offset: u64,
        bytes: usize,
        response_tx: oneshot::Sender<FollowerReplicateResult>,
        /// True when this batch still owns a catch-up charge taken while it
        /// sat in the retransmission buffer. The charge TRANSFERS with the
        /// batch — releasing it while `pending` still holds the same record
        /// buffers would let a stalled follower hide most of
        /// `max_retransmission_bytes` from the process ceiling.
        catch_up_charged: bool,
    },
    PropagateHwm {
        update: CommittedHwmUpdate,
    },
    DropConnection,
}

enum FollowerReplicateResult {
    Acked { local_committed_offset: u64 },
}

struct Inflight {
    requests: Arc<Vec<ReplicaAppendRequest>>,
    leader_committed_offset: u64,
    bytes: usize,
    response_tx: oneshot::Sender<FollowerReplicateResult>,
}

struct BufferedBatch {
    requests: Arc<Vec<ReplicaAppendRequest>>,
    leader_committed_offset: u64,
    bytes: usize,
}

struct FollowerDriver {
    config: NetworkFollowerConfig,
    connector: TlsConnector,
    flow: FlowControlConfig,
    channel: Arc<FollowerChannel>,
    budget: Arc<FollowerBudget>,
    cmd_rx: mpsc::Receiver<FollowerCmd>,
    stop_rx: mpsc::Receiver<()>,
}

impl FollowerDriver {
    async fn run(mut self) {
        let mut next_request_id = 1u64;
        let mut retransmission = VecDeque::<BufferedBatch>::new();
        let mut retransmission_bytes = 0usize;
        let mut pending: VecDeque<FollowerCmd> = VecDeque::new();

        loop {
            match self
                .connect_and_session(
                    &mut next_request_id,
                    &mut retransmission,
                    &mut retransmission_bytes,
                    &mut pending,
                )
                .await
            {
                SessionOutcome::Shutdown => {
                    release_pending_catch_up(&mut pending, &self.budget);
                    return;
                }
                SessionOutcome::Disconnected => {
                    self.channel.connected.store(false, Ordering::SeqCst);
                    sleep(self.flow.reconnect_backoff).await;
                }
            }
        }
    }

    async fn connect_and_session(
        &mut self,
        next_request_id: &mut u64,
        retransmission: &mut VecDeque<BufferedBatch>,
        retransmission_bytes: &mut usize,
        pending: &mut VecDeque<FollowerCmd>,
    ) -> SessionOutcome {
        let tcp = match timeout(
            self.flow.connect_timeout,
            TcpStream::connect(self.config.addr),
        )
        .await
        {
            Ok(Ok(tcp)) => tcp,
            _ => return SessionOutcome::Disconnected,
        };
        let server_name = match ServerName::try_from(self.config.server_name.clone()) {
            Ok(name) => name,
            Err(_) => return SessionOutcome::Disconnected,
        };
        let mut stream = match timeout(
            self.flow.connect_timeout,
            self.connector.connect(server_name, tcp),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            _ => return SessionOutcome::Disconnected,
        };
        if assert_peer_uuid(peer_certs_client(&stream), self.config.node_id).is_err() {
            return SessionOutcome::Disconnected;
        }
        // Catch-up probe + bounded retransmission. Mark connected only after the
        // optional status exchange succeeds so tests do not observe a false ready.
        let range = retransmission
            .front()
            .and_then(|batch| batch.requests.first().map(|r| r.range.clone()))
            .or_else(|| {
                pending.iter().find_map(|cmd| match cmd {
                    FollowerCmd::Replicate { requests, .. } => {
                        requests.first().map(|r| r.range.clone())
                    }
                    _ => None,
                })
            });
        if let Some(range) = range {
            let request_id = *next_request_id;
            *next_request_id = next_request_id.wrapping_add(1);
            let status_frame = WireFrame {
                request_id,
                stream_id: 0,
                message: Message::ReplicaStatusRequest(ReplicaStatusRequest { range }),
            };
            if write_frame(&mut stream, &status_frame, REPLICA_LIMITS)
                .await
                .is_err()
            {
                return SessionOutcome::Disconnected;
            }
            let reply = match read_frame(&mut stream, REPLICA_LIMITS).await {
                Ok(Some(frame)) => frame,
                _ => return SessionOutcome::Disconnected,
            };
            if reply.request_id != request_id {
                return SessionOutcome::Disconnected;
            }
            match reply.message {
                Message::ReplicaStatusResponse(status) => {
                    self.channel
                        .durable_offset
                        .store(status.local_committed_offset, Ordering::SeqCst);
                    // Drop buffer entries already covered; retransmit the rest.
                    while let Some(front) = retransmission.front() {
                        if front.leader_committed_offset <= status.local_committed_offset {
                            if let Some(done) = retransmission.pop_front() {
                                *retransmission_bytes =
                                    retransmission_bytes.saturating_sub(done.bytes);
                                self.budget.release_catch_up(done.bytes as u64);
                            }
                        } else {
                            break;
                        }
                    }
                    // Move the remaining buffer back into pending for
                    // retransmission. The catch-up charge is released here
                    // and re-taken by `push_retransmission` when a batch is
                    // actually re-sent, so every byte is charged exactly once
                    // while it occupies the retransmission buffer.
                    drain_retransmission_to_pending(pending, retransmission, retransmission_bytes);
                }
                _ => return SessionOutcome::Disconnected,
            }
        }
        self.channel.connected.store(true, Ordering::SeqCst);

        let mut inflight: HashMap<u64, Inflight> = HashMap::new();
        let mut inflight_bytes = 0usize;

        loop {
            // Drain pending catch-up / deferred replicates while the window allows.
            while can_send(&inflight, inflight_bytes, &self.flow) {
                let Some(FollowerCmd::Replicate {
                    requests,
                    leader_committed_offset,
                    bytes,
                    response_tx,
                    catch_up_charged,
                }) = pending.pop_front()
                else {
                    break;
                };
                if !window_allows(inflight.len(), inflight_bytes, bytes, &self.flow) {
                    pending.push_front(FollowerCmd::Replicate {
                        requests,
                        leader_committed_offset,
                        bytes,
                        response_tx,
                        catch_up_charged,
                    });
                    break;
                }
                if let Err(outcome) = self
                    .send_replicate(
                        &mut stream,
                        &mut SessionSendState {
                            next_request_id,
                            inflight: &mut inflight,
                            inflight_bytes: &mut inflight_bytes,
                            retransmission,
                            retransmission_bytes,
                        },
                        OutboundBatch {
                            requests,
                            leader_committed_offset,
                            bytes,
                            response_tx,
                            catch_up_charged,
                        },
                    )
                    .await
                {
                    requeue_inflight(pending, &mut inflight, retransmission);
                    return outcome;
                }
            }

            tokio::select! {
                _ = self.stop_rx.recv() => {
                    fail_inflight(&mut inflight);
                    return SessionOutcome::Shutdown;
                }
                frame = read_frame(&mut stream, REPLICA_LIMITS), if !inflight.is_empty() => {
                    match frame {
                        Ok(Some(frame)) => {
                            if handle_response(
                                frame,
                                &mut inflight,
                                &mut inflight_bytes,
                                &self.channel,
                            ) == ResponseOutcome::Resync
                            {
                                requeue_inflight(pending, &mut inflight, retransmission);
                                return SessionOutcome::Disconnected;
                            }
                        }
                        _ => {
                            requeue_inflight(pending, &mut inflight, retransmission);
                            return SessionOutcome::Disconnected;
                        }
                    }
                }
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        None => {
                            fail_inflight(&mut inflight);
                            return SessionOutcome::Shutdown;
                        }
                        Some(FollowerCmd::DropConnection) => {
                            requeue_inflight(pending, &mut inflight, retransmission);
                            return SessionOutcome::Disconnected;
                        }
                        Some(FollowerCmd::PropagateHwm { update }) => {
                            let frame = WireFrame {
                                request_id: 0,
                                stream_id: 0,
                                message: Message::CommittedHwmUpdate(update),
                            };
                            if write_frame(&mut stream, &frame, REPLICA_LIMITS).await.is_err() {
                                requeue_inflight(pending, &mut inflight, retransmission);
                                return SessionOutcome::Disconnected;
                            }
                        }
                        Some(FollowerCmd::Replicate {
                            requests,
                            leader_committed_offset,
                            bytes,
                            response_tx,
                            catch_up_charged,
                        }) => {
                            if !window_allows(inflight.len(), inflight_bytes, bytes, &self.flow) {
                                // Slow follower: miss this ack rather than stall.
                                // A transferred charge dies with the dropped
                                // batch, so release it here.
                                if catch_up_charged {
                                    self.budget.release_catch_up(bytes as u64);
                                }
                                drop(response_tx);
                                continue;
                            }
                            if let Err(outcome) = self
                                .send_replicate(
                                    &mut stream,
                                    &mut SessionSendState {
                                        next_request_id,
                                        inflight: &mut inflight,
                                        inflight_bytes: &mut inflight_bytes,
                                        retransmission,
                                        retransmission_bytes,
                                    },
                                    OutboundBatch {
                                        requests,
                                        leader_committed_offset,
                                        bytes,
                                        response_tx,
                                        catch_up_charged,
                                    },
                                )
                                .await
                            {
                                requeue_inflight(pending, &mut inflight, retransmission);
                                return outcome;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn send_replicate(
        &self,
        stream: &mut ClientTlsStream<TcpStream>,
        state: &mut SessionSendState<'_>,
        batch: OutboundBatch,
    ) -> Result<(), SessionOutcome> {
        let OutboundBatch {
            requests,
            leader_committed_offset,
            bytes,
            response_tx,
            catch_up_charged,
        } = batch;
        let request_id = *state.next_request_id;
        *state.next_request_id = state.next_request_id.wrapping_add(1);
        let message = if requests.len() <= 1 {
            match requests.first() {
                Some(request) => Message::ReplicaAppendRequest(request.clone()),
                None => {
                    let local = self.channel.durable_offset.load(Ordering::SeqCst);
                    let _ = response_tx.send(FollowerReplicateResult::Acked {
                        local_committed_offset: local,
                    });
                    return Ok(());
                }
            }
        } else {
            Message::ReplicaAppendBatchRequest(ReplicaAppendBatchRequest {
                requests: requests.as_ref().clone(),
            })
        };
        let frame = WireFrame {
            request_id,
            stream_id: 0,
            message,
        };
        if write_frame(stream, &frame, REPLICA_LIMITS).await.is_err() {
            if catch_up_charged {
                // The batch never re-entered the buffer; its transferred
                // charge dies with this session.
                self.budget.release_catch_up(bytes as u64);
            }
            drop(response_tx);
            return Err(SessionOutcome::Disconnected);
        }
        push_retransmission(
            state.retransmission,
            state.retransmission_bytes,
            self.flow.max_retransmission_bytes,
            BufferedBatch {
                requests: Arc::clone(&requests),
                leader_committed_offset,
                bytes,
            },
            &self.budget,
            catch_up_charged,
        );
        *state.inflight_bytes = state.inflight_bytes.saturating_add(bytes);
        state.inflight.insert(
            request_id,
            Inflight {
                requests,
                leader_committed_offset,
                bytes,
                response_tx,
            },
        );
        Ok(())
    }
}

/// One batch handed to [`FollowerDriver::send_replicate`]. Grouped so the
/// send path keeps a readable signature as the batch grows fields.
struct OutboundBatch {
    requests: Arc<Vec<ReplicaAppendRequest>>,
    leader_committed_offset: u64,
    bytes: usize,
    response_tx: oneshot::Sender<FollowerReplicateResult>,
    catch_up_charged: bool,
}

struct SessionSendState<'a> {
    next_request_id: &'a mut u64,
    inflight: &'a mut HashMap<u64, Inflight>,
    inflight_bytes: &'a mut usize,
    retransmission: &'a mut VecDeque<BufferedBatch>,
    retransmission_bytes: &'a mut usize,
}

enum SessionOutcome {
    Shutdown,
    Disconnected,
}

fn can_send(
    inflight: &HashMap<u64, Inflight>,
    inflight_bytes: usize,
    flow: &FlowControlConfig,
) -> bool {
    inflight.len() < flow.max_inflight_batches && inflight_bytes < flow.max_inflight_bytes
}

fn window_allows(
    inflight_len: usize,
    inflight_bytes: usize,
    next_bytes: usize,
    flow: &FlowControlConfig,
) -> bool {
    // Always admit at least one in-flight batch so an oversized payload cannot
    // deadlock a follower channel.
    if inflight_len == 0 {
        return true;
    }
    inflight_len < flow.max_inflight_batches
        && inflight_bytes.saturating_add(next_bytes) <= flow.max_inflight_bytes
}

/// Whether this response means the session must re-synchronise.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ResponseOutcome {
    Continue,
    Resync,
}

fn handle_response(
    frame: WireFrame,
    inflight: &mut HashMap<u64, Inflight>,
    inflight_bytes: &mut usize,
    channel: &FollowerChannel,
) -> ResponseOutcome {
    let Some(entry) = inflight.remove(&frame.request_id) else {
        return ResponseOutcome::Continue;
    };
    *inflight_bytes = inflight_bytes.saturating_sub(entry.bytes);
    match frame.message {
        Message::ReplicaAppendResponse(ReplicaAppendResponse {
            local_committed_offset,
        }) => {
            channel
                .durable_offset
                .fetch_max(local_committed_offset, Ordering::SeqCst);
            let _ = entry.response_tx.send(FollowerReplicateResult::Acked {
                local_committed_offset,
            });
            ResponseOutcome::Continue
        }
        // A refusal MAY be a divergence signal rather than just a missed ack —
        // but only some of them are, and treating them alike thrashes.
        //
        // This used to drop the waiter and send the next batch, which the
        // follower refused identically — forever. A follower that misses one
        // batch is off the leader's contiguous sequence, and every subsequent
        // append fails the same base-offset check, so the replica was stranded
        // permanently at whatever offset it had reached. It stayed connected,
        // so the reconnect catch-up that exists precisely to repair this never
        // ran. In the failure that exposed it, a replica sat at offset 0
        // rejecting 124 consecutive batches while reporting a healthy session.
        //
        // Re-synchronising means dropping the session, which sends the loop
        // through the status probe: the follower reports where it actually is,
        // covered buffer entries are discarded, and the rest is retransmitted
        // from its true position. `reconnect_backoff` bounds the retry, so a
        // follower that is genuinely fenced by a newer epoch retries at a
        // fixed cadence rather than spinning — and recovers on its own once it
        // adopts that epoch, instead of needing a restart.
        // `Fenced` means the follower is serving a different epoch. It will
        // stop saying so on its own — a follower-side watcher adopts the
        // granted epoch within a poll interval — and it stored nothing, so its
        // position has not moved. Resyncing here would drop and re-establish
        // the session on every batch for the whole adoption window, and with
        // every follower doing it at once the leader cannot assemble a quorum
        // at all: the producer stalls at zero acks rather than briefly lagging.
        // Treat it as a plain miss and let the next batch decide.
        Message::Error(ErrorResponse {
            code: ErrorCode::Fenced,
            ..
        }) => {
            drop(entry.response_tx);
            ResponseOutcome::Continue
        }
        // Anything else is divergence: the follower is refusing on its own
        // terms, and the canonical case is a base-offset mismatch — it has
        // fallen off the leader's contiguous sequence and every subsequent
        // append fails the same check. That does not heal by waiting, which is
        // exactly how a replica ended up stranded at offset 0 for 124
        // consecutive batches while its session looked healthy.
        _ => {
            drop(entry.response_tx);
            ResponseOutcome::Resync
        }
    }
}

fn fail_inflight(inflight: &mut HashMap<u64, Inflight>) {
    for (_, entry) in inflight.drain() {
        drop(entry.response_tx);
    }
}

/// Preserve in-flight batches across reconnect so a dropped socket does not
/// permanently skip a follower. Batches still held by the retransmission
/// buffer stay there (charged exactly once) and are re-queued by the
/// reconnect catch-up drain; only batches the buffer no longer holds —
/// charge-failed or evicted — are re-queued here, so a re-send cannot
/// duplicate a buffer entry or charge the same bytes twice.
fn requeue_inflight(
    pending: &mut VecDeque<FollowerCmd>,
    inflight: &mut HashMap<u64, Inflight>,
    retransmission: &VecDeque<BufferedBatch>,
) {
    let mut batches: Vec<Inflight> = inflight.drain().map(|(_, entry)| entry).collect();
    // Preserve original send order by request_id.
    batches.sort_by_key(|entry| {
        entry
            .requests
            .first()
            .map(|request| request.expected_base_offset)
            .unwrap_or(0)
    });
    for entry in batches.into_iter().rev() {
        drop(entry.response_tx);
        if retransmission
            .iter()
            .any(|batch| Arc::ptr_eq(&batch.requests, &entry.requests))
        {
            continue;
        }
        pending.push_front(FollowerCmd::Replicate {
            requests: entry.requests,
            leader_committed_offset: entry.leader_committed_offset,
            bytes: entry.bytes,
            response_tx: oneshot::channel().0,
            // Batches still held in the retransmission buffer keep their
            // charge there (skipped above); anything reaching here was never
            // buffered, so it carries no charge.
            catch_up_charged: false,
        });
    }
}

fn push_retransmission(
    buffer: &mut VecDeque<BufferedBatch>,
    buffer_bytes: &mut usize,
    max_bytes: usize,
    batch: BufferedBatch,
    budget: &FollowerBudget,
    already_charged: bool,
) {
    // Prefer failing closed on catch-up budget before growing the buffer. A
    // batch carrying a transferred charge is already accounted for: charging
    // again would double-count it against the ceiling.
    if !already_charged && budget.try_charge_catch_up(batch.bytes as u64).is_err() {
        return;
    }
    buffer.push_back(batch);
    *buffer_bytes = buffer_bytes.saturating_add(buffer.back().map(|b| b.bytes).unwrap_or(0));
    while *buffer_bytes > max_bytes {
        if let Some(evicted) = buffer.pop_front() {
            *buffer_bytes = buffer_bytes.saturating_sub(evicted.bytes);
            budget.release_catch_up(evicted.bytes as u64);
        } else {
            break;
        }
    }
}

/// Move buffered batches back into `pending` for retransmission after a
/// reconnect, TRANSFERRING their catch-up charges with them. The bytes are
/// still owned (the same record buffers now sit in `pending`, without
/// producer-side inflight reservations), so releasing here would let a
/// stalled follower keep most of `max_retransmission_bytes` unaccounted
/// while other allocations are admitted past the process ceiling. Each
/// charge is released exactly once: when the bytes are acknowledged,
/// evicted from the buffer, or dropped with their session.
fn drain_retransmission_to_pending(
    pending: &mut VecDeque<FollowerCmd>,
    buffer: &mut VecDeque<BufferedBatch>,
    buffer_bytes: &mut usize,
) {
    while let Some(batch) = buffer.pop_back() {
        *buffer_bytes = buffer_bytes.saturating_sub(batch.bytes);
        pending.push_front(FollowerCmd::Replicate {
            requests: batch.requests,
            leader_committed_offset: batch.leader_committed_offset,
            bytes: batch.bytes,
            // Catch-up has no producer waiter; durable_offset still advances.
            response_tx: oneshot::channel().0,
            catch_up_charged: true,
        });
    }
}

/// Release any catch-up charges still carried by queued batches. Called when
/// a session ends so transferred charges never outlive the bytes.
fn release_pending_catch_up(pending: &mut VecDeque<FollowerCmd>, budget: &FollowerBudget) {
    for cmd in pending.iter_mut() {
        if let FollowerCmd::Replicate {
            bytes,
            catch_up_charged,
            ..
        } = cmd
        {
            if *catch_up_charged {
                budget.release_catch_up(*bytes as u64);
                *catch_up_charged = false;
            }
        }
    }
}

fn approx_batch_bytes(requests: &[ReplicaAppendRequest]) -> usize {
    requests
        .iter()
        .map(|request| {
            request
                .records
                .iter()
                .map(|record| record.key.len() + record.value.len() + 64)
                .sum::<usize>()
                .max(64)
        })
        .sum::<usize>()
        .max(64)
}

/// One-shot client for the replica-status RPC (#224).
///
/// The leader keeps a long-lived session per follower and reads status as part
/// of its catch-up handshake. This is the operator's path instead: connect, ask
/// one replica where its disk actually is, disconnect. Reusing the leader's
/// dialer for that would mean starting a replication session as a side effect
/// of running a status command.
///
/// The replica's certificate CN is checked against the node UUID the caller
/// expected, so `vtopctl node status` cannot silently report a different
/// replica's offsets after an address is reused or a config drifts.
pub struct ReplicaStatusClient {
    connector: TlsConnector,
    timeout: Duration,
}

impl ReplicaStatusClient {
    pub fn new(material: ReplicaTlsMaterial) -> BrokerResult<Self> {
        Ok(Self {
            connector: build_client_connector(material)?,
            timeout: Duration::from_secs(5),
        })
    }

    /// Deadline covering connect, handshake, and the round trip.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub async fn status(
        &self,
        addr: SocketAddr,
        server_name: &str,
        expected_node: Uuid,
        range: &RangeIdentity,
    ) -> BrokerResult<ReplicaStatusResponse> {
        let name = rustls::pki_types::ServerName::try_from(server_name.to_owned())
            .map_err(|error| {
                crate::BrokerError::InvalidConfig(format!("server name {server_name:?}: {error}"))
            })?
            .to_owned();
        // One deadline for the whole exchange: a replica whose disk has stopped
        // answering will also stop answering here, and an operator running a
        // status command during an incident needs it to return.
        timeout(self.timeout, async {
            let tcp = TcpStream::connect(addr)
                .await
                .map_err(|source| crate::BrokerError::Io {
                    path: PathBuf::from("replica-status"),
                    source,
                })?;
            let mut stream = self.connector.connect(name, tcp).await.map_err(|source| {
                crate::BrokerError::Io {
                    path: PathBuf::from("replica-status-tls"),
                    source,
                }
            })?;
            assert_peer_uuid(peer_certs_client(&stream), expected_node)?;
            let frame = WireFrame {
                request_id: 1,
                stream_id: 0,
                message: Message::ReplicaStatusRequest(ReplicaStatusRequest {
                    range: range.clone(),
                }),
            };
            write_frame(&mut stream, &frame, REPLICA_LIMITS).await?;
            let reply = read_frame(&mut stream, REPLICA_LIMITS)
                .await?
                .ok_or_else(|| {
                    crate::BrokerError::InvalidConfig(
                        "replica closed the session without answering".to_owned(),
                    )
                })?;
            match reply.message {
                Message::ReplicaStatusResponse(status) => Ok(status),
                Message::Error(error) => Err(crate::BrokerError::InvalidConfig(format!(
                    "replica refused the status request: {:?} {}",
                    error.code, error.message
                ))),
                other => Err(crate::BrokerError::InvalidConfig(format!(
                    "unexpected reply to a status request: {other:?}"
                ))),
            }
        })
        .await
        .map_err(|_| crate::BrokerError::Timeout("replica status"))?
    }

    /// Fence a replica and read it in the same round trip (#240).
    ///
    /// Every failure is an ERROR here, with no degraded path — the opposite of
    /// [`Self::epoch_history`], and deliberately so. "Unknown history" is a
    /// usable answer: it tells the caller not to reconcile by epoch. "Unknown
    /// whether it is fenced" is not, because the only thing a caller does with
    /// this reply is count the replica toward a promotion boundary, and a
    /// replica that was never fenced may still be taking a deposed leader's
    /// writes. Turning that into an empty or default value would put the moving
    /// target back into the quorum by another route.
    ///
    /// So an older peer that does not know the message, one whose metadata view
    /// has not yet caught up to the grant, and one that is simply unreachable
    /// all fail alike. The caller retries, or promotes on a quorum of the
    /// replicas it did manage to fence.
    pub async fn fence(
        &self,
        addr: SocketAddr,
        server_name: &str,
        expected_node: Uuid,
        range: &RangeIdentity,
        fencing_epoch: u64,
        leader_epoch_starts: &[vtop_protocol::ReplicaEpochStart],
    ) -> BrokerResult<vtop_protocol::ReplicaFenceResponse> {
        let name = rustls::pki_types::ServerName::try_from(server_name.to_owned())
            .map_err(|error| {
                crate::BrokerError::InvalidConfig(format!("server name {server_name:?}: {error}"))
            })?
            .to_owned();
        timeout(self.timeout, async {
            let tcp = TcpStream::connect(addr)
                .await
                .map_err(|source| crate::BrokerError::Io {
                    path: PathBuf::from("replica-fence"),
                    source,
                })?;
            let mut stream = self.connector.connect(name, tcp).await.map_err(|source| {
                crate::BrokerError::Io {
                    path: PathBuf::from("replica-fence-tls"),
                    source,
                }
            })?;
            assert_peer_uuid(peer_certs_client(&stream), expected_node)?;
            let frame = WireFrame {
                request_id: 1,
                stream_id: 0,
                message: Message::ReplicaFenceRequest(vtop_protocol::ReplicaFenceRequest {
                    range: range.clone(),
                    fencing_epoch,
                    leader_epoch_starts: leader_epoch_starts.to_vec(),
                }),
            };
            write_frame(&mut stream, &frame, REPLICA_LIMITS).await?;
            let reply = read_frame(&mut stream, REPLICA_LIMITS)
                .await?
                .ok_or_else(|| {
                    crate::BrokerError::InvalidConfig(
                        "replica closed the session without answering the fence".to_owned(),
                    )
                })?;
            match reply.message {
                Message::ReplicaFenceResponse(response) => {
                    // EXACTLY the requested epoch, not merely at least it.
                    //
                    // Lower is obvious: the replica is not fenced to the epoch
                    // being promoted at. Higher is the subtle one and is just
                    // as disqualifying — it means something granted a NEWER
                    // epoch between this replica adopting and answering, so
                    // this candidate has already been superseded and the
                    // snapshot describes a log fenced under someone else's
                    // grant. Counting it would establish a boundary from a
                    // measurement taken for a different leader.
                    if response.fencing_epoch != fencing_epoch {
                        return Err(crate::BrokerError::InvalidConfig(format!(
                            "replica reported epoch {} after being asked to fence at \
                             {fencing_epoch}; only an exact match is a fence at this epoch",
                            response.fencing_epoch
                        )));
                    }
                    Ok(response)
                }
                Message::Error(error) => Err(crate::BrokerError::InvalidConfig(format!(
                    "replica refused the fence: {:?} {}",
                    error.code, error.message
                ))),
                other => Err(crate::BrokerError::InvalidConfig(format!(
                    "unexpected reply to a fence request: {other:?}"
                ))),
            }
        })
        .await
        .map_err(|_| crate::BrokerError::Timeout("replica fence"))?
    }

    /// Ask a replica which fencing epoch wrote each stretch of its log (#240).
    ///
    /// A replica that cannot answer — one whose vector is absent or broken and
    /// refuses, or an older peer that does not know the message kind and simply
    /// drops the connection — yields an EMPTY history rather than an error.
    /// That is deliberate: "unknown" is a state promotion must handle anyway,
    /// and turning a peer's ignorance into a failed probe would make a
    /// mixed-version cluster unable to elect at all. The caller must never read
    /// empty as "this replica had no leadership changes".
    ///
    /// A malformed reply, a TLS failure, or a wrong peer identity is still an
    /// error: those are faults, not answers.
    pub async fn epoch_history(
        &self,
        addr: SocketAddr,
        server_name: &str,
        expected_node: Uuid,
        range: &RangeIdentity,
    ) -> BrokerResult<Vec<vtop_protocol::ReplicaEpochStart>> {
        let name = rustls::pki_types::ServerName::try_from(server_name.to_owned())
            .map_err(|error| {
                crate::BrokerError::InvalidConfig(format!("server name {server_name:?}: {error}"))
            })?
            .to_owned();
        timeout(self.timeout, async {
            let tcp = TcpStream::connect(addr)
                .await
                .map_err(|source| crate::BrokerError::Io {
                    path: PathBuf::from("replica-epoch-history"),
                    source,
                })?;
            let mut stream = self.connector.connect(name, tcp).await.map_err(|source| {
                crate::BrokerError::Io {
                    path: PathBuf::from("replica-epoch-history-tls"),
                    source,
                }
            })?;
            assert_peer_uuid(peer_certs_client(&stream), expected_node)?;
            let frame = WireFrame {
                request_id: 1,
                stream_id: 0,
                message: Message::ReplicaEpochHistoryRequest(
                    vtop_protocol::ReplicaEpochHistoryRequest {
                        range: range.clone(),
                    },
                ),
            };
            write_frame(&mut stream, &frame, REPLICA_LIMITS).await?;
            let Some(reply) = read_frame(&mut stream, REPLICA_LIMITS).await? else {
                // A peer that does not know kind 67 fails in ITS read_frame,
                // before any handler runs, and drops the connection without
                // writing a reply. So the mixed-version case arrives here as a
                // clean EOF, not as an Error frame — handling only the latter
                // would promise a degraded path and not deliver one.
                //
                // This conflates "too old to answer" with "died mid-request",
                // which is safe because it is not this call's job to tell them
                // apart: liveness is established by the status probe, which
                // still fails loudly. Both mean the same thing for the value
                // being returned — we do not know this replica's history — and
                // unknown only ever disables epoch reconciliation, never
                // authorises a truncation.
                return Ok(Vec::new());
            };
            match reply.message {
                Message::ReplicaEpochHistoryResponse(response) => Ok(response.epoch_starts),
                // Refusal is "unknown", not a probe failure — see above.
                Message::Error(_) => Ok(Vec::new()),
                other => Err(crate::BrokerError::InvalidConfig(format!(
                    "unexpected reply to an epoch-history request: {other:?}"
                ))),
            }
        })
        .await
        .map_err(|_| crate::BrokerError::Timeout("replica epoch history"))?
    }
}

fn build_server_acceptor(material: ReplicaTlsMaterial) -> BrokerResult<TlsAcceptor> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(material.trust_roots),
        Arc::clone(&provider),
    )
    .build()
    .map_err(|error| {
        crate::BrokerError::InvalidConfig(format!("replica client verifier: {error}"))
    })?;
    let config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_client_cert_verifier(verifier)
        .with_single_cert(material.certificate_chain, material.private_key)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn build_client_connector(material: ReplicaTlsMaterial) -> BrokerResult<TlsConnector> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(material.trust_roots)
        .with_client_auth_cert(material.certificate_chain, material.private_key)?;
    Ok(TlsConnector::from(Arc::new(config)))
}

fn peer_certs_client(stream: &ClientTlsStream<TcpStream>) -> Option<Vec<CertificateDer<'static>>> {
    let (_, conn) = stream.get_ref();
    conn.peer_certificates().map(|certs| certs.to_vec())
}

fn peer_uuid_from_server_stream(stream: &ServerTlsStream<TcpStream>) -> BrokerResult<Uuid> {
    let (_, conn) = stream.get_ref();
    let certs = conn.peer_certificates();
    let leaf = certs.and_then(|c| c.first()).ok_or_else(|| {
        crate::BrokerError::InvalidConfig("replica peer presented no certificate".to_owned())
    })?;
    uuid_from_cert(leaf)
}

fn assert_peer_uuid(
    certs: Option<Vec<CertificateDer<'static>>>,
    expected: Uuid,
) -> BrokerResult<()> {
    let leaf = certs.as_ref().and_then(|c| c.first()).ok_or_else(|| {
        crate::BrokerError::InvalidConfig("replica peer presented no certificate".to_owned())
    })?;
    let actual = uuid_from_cert(leaf)?;
    if actual != expected {
        return Err(crate::BrokerError::InvalidConfig(format!(
            "replica peer certificate CN maps to {actual}, expected {expected}"
        )));
    }
    Ok(())
}

fn uuid_from_cert(der: &CertificateDer<'_>) -> BrokerResult<Uuid> {
    use x509_parser::prelude::*;
    let (_, cert) = X509Certificate::from_der(der.as_ref()).map_err(|error| {
        crate::BrokerError::InvalidConfig(format!("parse replica leaf cert: {error}"))
    })?;
    let cn = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|attr| attr.as_str().ok())
        .ok_or_else(|| {
            crate::BrokerError::InvalidConfig("replica leaf cert missing CN".to_owned())
        })?;
    Uuid::parse_str(cn).map_err(|_| {
        crate::BrokerError::InvalidConfig(format!("replica leaf cert CN {cn:?} is not a UUID"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_budget::MemoryBudgetConfig;
    use vtop_protocol::{ProduceRecord, RangeIdentity};

    fn test_budget() -> (Arc<MemoryBudgetPool>, Arc<FollowerBudget>) {
        let pool = MemoryBudgetPool::new(MemoryBudgetConfig::default()).unwrap();
        let budget = Arc::new(pool.open_follower());
        (pool, budget)
    }

    fn test_batch(base_offset: u64, bytes: usize) -> BufferedBatch {
        BufferedBatch {
            requests: Arc::new(vec![ReplicaAppendRequest {
                range: RangeIdentity {
                    topic: "events.v1".to_owned(),
                    topic_epoch: 1,
                    range_id: Uuid::from_u128(0xC1),
                    range_generation: 0,
                },
                fencing_epoch: 1,
                leader_node_id: Uuid::from_u128(0xA1),
                expected_base_offset: base_offset,
                producer_id: Uuid::from_u128(0xB1),
                producer_epoch: 1,
                first_sequence: 0,
                records: vec![ProduceRecord {
                    timestamp_millis: 1,
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                }],
            }]),
            leader_committed_offset: base_offset + 1,
            bytes,
        }
    }

    #[test]
    fn reconnect_drain_releases_charge_until_resend() {
        let (_pool, budget) = test_budget();
        let mut buffer = VecDeque::new();
        let mut buffer_bytes = 0usize;
        let mut pending = VecDeque::new();
        push_retransmission(
            &mut buffer,
            &mut buffer_bytes,
            1_024,
            test_batch(0, 100),
            &budget,
            false,
        );
        assert_eq!(budget.catch_up_used_bytes(), 100);
        assert_eq!(buffer_bytes, 100);

        // Reconnect: buffered entries move to pending CARRYING their catch-up
        // charge — the bytes are still resident, now owned by `pending`.
        drain_retransmission_to_pending(&mut pending, &mut buffer, &mut buffer_bytes);
        assert!(buffer.is_empty());
        assert_eq!(buffer_bytes, 0);
        assert_eq!(budget.catch_up_used_bytes(), 100);
        assert_eq!(pending.len(), 1);

        // The re-send re-buffers without charging again (no double charge, no
        // duplicated buffer entry).
        let Some(FollowerCmd::Replicate {
            requests,
            leader_committed_offset,
            bytes,
            catch_up_charged,
            ..
        }) = pending.pop_front()
        else {
            panic!("expected replicate command");
        };
        assert!(catch_up_charged);
        push_retransmission(
            &mut buffer,
            &mut buffer_bytes,
            1_024,
            BufferedBatch {
                requests,
                leader_committed_offset,
                bytes,
            },
            &budget,
            catch_up_charged,
        );
        assert_eq!(budget.catch_up_used_bytes(), 100);
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer_bytes, 100);
    }

    #[test]
    fn reconnect_drain_transfers_catch_up_charges_instead_of_releasing_them() {
        let (_pool, budget) = test_budget();
        let mut buffer = VecDeque::new();
        let mut buffer_bytes = 0usize;
        for index in 0..3 {
            push_retransmission(
                &mut buffer,
                &mut buffer_bytes,
                1_024,
                test_batch(index, 100),
                &budget,
                false,
            );
        }
        assert_eq!(budget.catch_up_used_bytes(), 300);

        // Draining for retransmission must not release: `pending` now owns
        // the same record buffers, so the bytes are still resident.
        let mut pending = VecDeque::new();
        drain_retransmission_to_pending(&mut pending, &mut buffer, &mut buffer_bytes);
        assert_eq!(pending.len(), 3);
        assert_eq!(buffer_bytes, 0);
        assert_eq!(
            budget.catch_up_used_bytes(),
            300,
            "charges transfer with the batches"
        );
        assert!(pending.iter().all(|cmd| matches!(
            cmd,
            FollowerCmd::Replicate {
                catch_up_charged: true,
                ..
            }
        )));

        // Re-buffering a transferred batch must not double-charge it.
        if let Some(FollowerCmd::Replicate { bytes, .. }) = pending.pop_front() {
            push_retransmission(
                &mut buffer,
                &mut buffer_bytes,
                1_024,
                test_batch(9, bytes),
                &budget,
                true,
            );
        }
        assert_eq!(budget.catch_up_used_bytes(), 300);

        // Ending the session releases whatever the queue still carries.
        release_pending_catch_up(&mut pending, &budget);
        assert_eq!(budget.catch_up_used_bytes(), 100);
        release_pending_catch_up(&mut pending, &budget);
        assert_eq!(
            budget.catch_up_used_bytes(),
            100,
            "release is idempotent per batch"
        );
    }

    #[test]
    fn requeue_inflight_skips_batches_still_charged_in_buffer() {
        let (_pool, budget) = test_budget();
        let mut buffer = VecDeque::new();
        let mut buffer_bytes = 0usize;
        push_retransmission(
            &mut buffer,
            &mut buffer_bytes,
            1_024,
            test_batch(0, 100),
            &budget,
            false,
        );
        let buffered_requests = Arc::clone(&buffer.front().unwrap().requests);
        // An inflight entry the buffer does not hold (charge-failed / evicted).
        let unbuffered = test_batch(1, 100);

        let mut inflight = HashMap::new();
        let (tx_buffered, _rx) = oneshot::channel();
        inflight.insert(
            1,
            Inflight {
                requests: buffered_requests,
                leader_committed_offset: 1,
                bytes: 100,
                response_tx: tx_buffered,
            },
        );
        let (tx_unbuffered, _rx2) = oneshot::channel();
        inflight.insert(
            2,
            Inflight {
                requests: Arc::clone(&unbuffered.requests),
                leader_committed_offset: unbuffered.leader_committed_offset,
                bytes: unbuffered.bytes,
                response_tx: tx_unbuffered,
            },
        );

        let mut pending = VecDeque::new();
        requeue_inflight(&mut pending, &mut inflight, &buffer);

        // Only the batch missing from the buffer is re-queued; the buffered
        // one stays charged exactly once and is left to the reconnect drain.
        assert_eq!(pending.len(), 1);
        assert_eq!(budget.catch_up_used_bytes(), 100);
        match pending.pop_front() {
            Some(FollowerCmd::Replicate { requests, .. }) => {
                assert!(Arc::ptr_eq(&requests, &unbuffered.requests));
            }
            _ => panic!("expected replicate command"),
        }
    }
}

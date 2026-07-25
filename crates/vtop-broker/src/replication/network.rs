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
    ReplicaAppendBatchRequest, ReplicaAppendRequest, ReplicaAppendResponse, ReplicaStatusRequest,
    ReplicaStatusResponse, WireFrame, DEFAULT_MAX_FRAME_BYTES, DEFAULT_MAX_RECORDS,
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
        Message::ReplicaStatusRequest(request) => match handler.status(&request.range) {
            Ok(response) => Message::ReplicaStatusResponse(response),
            Err((code, message)) => error_message(code, message),
        },
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
                SessionOutcome::Shutdown => return,
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
                    for batch in retransmission.iter().rev() {
                        pending.push_front(FollowerCmd::Replicate {
                            requests: Arc::clone(&batch.requests),
                            leader_committed_offset: batch.leader_committed_offset,
                            bytes: batch.bytes,
                            // Catch-up has no producer waiter; durable_offset still advances.
                            response_tx: oneshot::channel().0,
                        });
                    }
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
                        requests,
                        leader_committed_offset,
                        bytes,
                        response_tx,
                    )
                    .await
                {
                    requeue_inflight(pending, &mut inflight);
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
                            handle_response(frame, &mut inflight, &mut inflight_bytes, &self.channel);
                        }
                        _ => {
                            requeue_inflight(pending, &mut inflight);
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
                            requeue_inflight(pending, &mut inflight);
                            return SessionOutcome::Disconnected;
                        }
                        Some(FollowerCmd::PropagateHwm { update }) => {
                            let frame = WireFrame {
                                request_id: 0,
                                stream_id: 0,
                                message: Message::CommittedHwmUpdate(update),
                            };
                            if write_frame(&mut stream, &frame, REPLICA_LIMITS).await.is_err() {
                                requeue_inflight(pending, &mut inflight);
                                return SessionOutcome::Disconnected;
                            }
                        }
                        Some(FollowerCmd::Replicate {
                            requests,
                            leader_committed_offset,
                            bytes,
                            response_tx,
                        }) => {
                            if !window_allows(inflight.len(), inflight_bytes, bytes, &self.flow) {
                                // Slow follower: miss this ack rather than stall.
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
                                    requests,
                                    leader_committed_offset,
                                    bytes,
                                    response_tx,
                                )
                                .await
                            {
                                requeue_inflight(pending, &mut inflight);
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
        requests: Arc<Vec<ReplicaAppendRequest>>,
        leader_committed_offset: u64,
        bytes: usize,
        response_tx: oneshot::Sender<FollowerReplicateResult>,
    ) -> Result<(), SessionOutcome> {
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

fn handle_response(
    frame: WireFrame,
    inflight: &mut HashMap<u64, Inflight>,
    inflight_bytes: &mut usize,
    channel: &FollowerChannel,
) {
    let Some(entry) = inflight.remove(&frame.request_id) else {
        return;
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
        }
        _ => {
            // Error / unexpected: waiter observes closed/timeout as miss.
            drop(entry.response_tx);
        }
    }
}

fn fail_inflight(inflight: &mut HashMap<u64, Inflight>) {
    for (_, entry) in inflight.drain() {
        drop(entry.response_tx);
    }
}

/// Preserve in-flight batches across reconnect so a dropped socket does not
/// permanently skip a follower that is still within the retransmission window.
fn requeue_inflight(pending: &mut VecDeque<FollowerCmd>, inflight: &mut HashMap<u64, Inflight>) {
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
        pending.push_front(FollowerCmd::Replicate {
            requests: entry.requests,
            leader_committed_offset: entry.leader_committed_offset,
            bytes: entry.bytes,
            response_tx: oneshot::channel().0,
        });
    }
}

fn push_retransmission(
    buffer: &mut VecDeque<BufferedBatch>,
    buffer_bytes: &mut usize,
    max_bytes: usize,
    batch: BufferedBatch,
    budget: &FollowerBudget,
) {
    // Prefer failing closed on catch-up budget before growing the buffer.
    if budget.try_charge_catch_up(batch.bytes as u64).is_err() {
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

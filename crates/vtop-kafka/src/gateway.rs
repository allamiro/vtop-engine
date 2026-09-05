//! The listener (#225): Kafka frames in, the bridge behind, and every gap a
//! refusal by name.
//!
//! What is served is exactly what the engine can honestly back today:
//! Metadata over the bridge's topics with one partition each, Produce of
//! uncompressed non-transactional v2 batches without headers, Fetch with the
//! long-poll emulated here (the broker returns immediately at the watermark),
//! ListOffsets LATEST and EARLIEST (the watermark and the retained floor;
//! by-timestamp has no index behind it), and — #457 — InitProducerId for
//! idempotent producers: a batch carrying the id and epoch minted here is
//! appended under that producer's own sequences, so a retry after a timeout
//! appends once. Everything else is refused with the code a client's retry
//! policy can act on, and the reason is logged where an operator reads —
//! never a silent drop, never a plausible lie.

use crate::api::{
    decode_fetch, decode_init_producer_id, decode_list_offsets, decode_metadata, decode_produce,
    encode_api_versions, encode_fetch, encode_init_producer_id, encode_list_offsets,
    encode_metadata, encode_produce, FetchPartitionResponse, FetchRequest, FetchTopicResponse,
    InitProducerIdRequest, ListOffsetsPartitionResponse, ListOffsetsRequest,
    ListOffsetsTopicResponse, MetadataBroker, MetadataRequest, MetadataResponse, MetadataTopic,
    ProducePartitionResponse, ProduceRequest, ProduceTopicResponse, TIMESTAMP_EARLIEST,
    TIMESTAMP_LATEST,
};
use crate::api_groups::{
    consumer_assignment_partitions, decode_find_coordinator, decode_heartbeat, decode_join_group,
    decode_leave_group, decode_offset_commit, decode_offset_fetch, decode_sync_group,
    encode_error_only, encode_find_coordinator, encode_join_group, encode_offset_commit,
    encode_offset_fetch, encode_sync_group, FindCoordinatorRequest, FindCoordinatorResponse,
    JoinGroupRequest, JoinGroupResponse, OffsetCommitRequest, OffsetCommitTopicResponse,
    OffsetFetchPartitionResponse, OffsetFetchRequest, OffsetFetchResponse,
    OffsetFetchTopicResponse, SyncGroupRequest, SyncGroupResponse, MAX_OFFSET_METADATA_BYTES,
};
use crate::bridge::{Bridge, Fetched, Sequenced};
use crate::groups::{Coordinator, GroupConfig, JoinRequest, Joined, SyncStanding};
use crate::lease::{LeaseState, LeaseView};
use crate::messages::{
    frame, write_response_header, ApiKey, ErrorCode, HeaderVerdict, RequestHeader,
};
use crate::offsets::{Committed, OffsetStore};
use crate::records::{BatchError, RecordBatch};
use crate::turnstile::Turnstile;
use crate::wire::{Decoder, Encoder};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// How the gateway presents itself and bounds what it does.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// What Metadata tells clients to connect to: this listener as clients
    /// reach it, which is not always the address it bound.
    pub advertised_host: String,
    pub advertised_port: i32,
    /// The broker id this gateway answers as; the leader of every partition.
    pub node_id: i32,
    pub cluster_id: Option<String>,
    /// A request frame above this is a closed connection, not an allocation.
    pub max_frame_bytes: usize,
    /// The longest a fetch waits for data, whatever the client asked: the
    /// broker returns immediately at the watermark, so `fetch.max.wait.ms`
    /// is emulated here by polling, and this caps it.
    pub max_fetch_wait: Duration,
    pub fetch_poll_interval: Duration,
    /// The longest a produce is waited for, whatever the client's
    /// `timeout_ms`: the append itself cannot be cancelled (a real backend
    /// fsyncs), so past this the client is told `REQUEST_TIMED_OUT` while the
    /// append runs on — and a retry may then duplicate, which is the
    /// single-writer limitation the native bridge documents.
    pub max_produce_wait: Duration,
    /// The longest a request body may take to arrive once its length is
    /// read (review): a frame is read as it comes, never allocated in full
    /// on the announced length, and a peer that announces a length and
    /// stops sending is closed here rather than held.
    pub frame_read_timeout: Duration,
    /// A connection with no request for this long is closed, as Kafka's
    /// own `connections.max.idle.ms` closes one.
    pub idle_timeout: Duration,
    /// Sessions open at once. One over is accepted and closed, with a
    /// warning, so what a peer can hold here is bounded by
    /// `max_sessions * max_frame_bytes` and not by how many sockets it opens.
    pub max_sessions: usize,
    /// How long `serve` waits, after the accept loop stops, for every
    /// session to finish the request it is in and close (review): an
    /// embedder that hands its range back after `serve` returns knows no
    /// append is still in flight through the gateway.
    pub drain_timeout: Duration,
    /// How the group coordinator bounds and paces itself (#457 slice 2).
    pub groups: GroupConfig,
    /// The group protocol's ceiling (review): the longest an offset store —
    /// or a bridge call made on the group protocol's behalf: the topics a
    /// commit, a fetch or an assignment is checked against, a commit's
    /// watermark — is waited for. A commit or a fetch not answered by then is
    /// `REQUEST_TIMED_OUT`, and the embedder's drain, which adds this to its
    /// budget, never returns while such a call can still be running. Under
    /// the shortest session by default (review): a member commits on the
    /// connection it heartbeats on, so a heartbeat queued behind a commit is
    /// read after it — 5 s under Kafka's 6 s minimum, as Kafka's own
    /// `offsets.commit.timeout.ms` sits under its `group.min.session.timeout.ms`.
    pub max_offset_wait: Duration,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            advertised_host: "127.0.0.1".to_owned(),
            advertised_port: 9092,
            node_id: 1,
            cluster_id: Some("vtop".to_owned()),
            max_frame_bytes: 32 * 1024 * 1024,
            max_fetch_wait: Duration::from_secs(5),
            fetch_poll_interval: Duration::from_millis(50),
            max_produce_wait: Duration::from_secs(30),
            frame_read_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(600),
            max_sessions: 256,
            drain_timeout: Duration::from_secs(5),
            groups: GroupConfig::default(),
            max_offset_wait: Duration::from_secs(5),
        }
    }
}

/// A frame's buffer starts this large and grows with the bytes received.
const INITIAL_FRAME_CAPACITY: usize = 64 * 1024;

/// What ListOffsets answers as the timestamp of a boundary lookup: unknown.
const TIMESTAMP_UNKNOWN: i64 = -1;

/// Resolves when `shutdown` reads `true`; never on a dropped sender, which
/// is not a signal (see `serve`).
async fn shutdown_signalled(shutdown: &mut tokio::sync::watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        if shutdown.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// A bridge call awaited only until `ceiling` (review). The call itself
/// cannot be cancelled — a real backend may be behind an fsync or a held
/// lock — so past the ceiling the client is told `REQUEST_TIMED_OUT` while
/// the call runs on, and the configured maximum is a bound the response
/// honours rather than a hope.
/// Work the gateway hands to another task — a bridge call on the blocking
/// pool, a store write driven past its request — counted so the drain can
/// wait for it (review): `serve` returning means no such work still holds
/// the bridge or the store, not merely that no session is open. A deadline
/// abandons the WAIT for a call, never the call, which cannot be cancelled.
#[derive(Default)]
struct Jobs {
    active: std::sync::atomic::AtomicUsize,
    idle: tokio::sync::Notify,
}

impl Jobs {
    fn blocking<T: Send + 'static>(
        self: &Arc<Self>,
        call: impl FnOnce() -> T + Send + 'static,
    ) -> tokio::task::JoinHandle<T> {
        self.active
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let jobs = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let _done = JobDone(jobs);
            call()
        })
    }

    fn spawn<T: Send + 'static>(
        self: &Arc<Self>,
        work: impl std::future::Future<Output = T> + Send + 'static,
    ) -> tokio::task::JoinHandle<T> {
        self.active
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let jobs = Arc::clone(self);
        tokio::spawn(async move {
            let _done = JobDone(jobs);
            work.await
        })
    }

    fn active(&self) -> usize {
        self.active.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Resolves once no job is running. The wakeup is registered before the
    /// count is read, so a job ending in between is not missed.
    async fn idle(&self) {
        loop {
            let notified = self.idle.notified();
            if self.active() == 0 {
                return;
            }
            notified.await;
        }
    }
}

struct JobDone(Arc<Jobs>);

impl Drop for JobDone {
    fn drop(&mut self) {
        if self
            .0
            .active
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            == 1
        {
            self.0.idle.notify_waiters();
        }
    }
}

async fn until<T>(
    ceiling: tokio::time::Instant,
    call: tokio::task::JoinHandle<Result<T, ErrorCode>>,
) -> Result<T, ErrorCode> {
    match tokio::time::timeout_at(ceiling, call).await {
        Ok(Ok(outcome)) => outcome,
        // Panicked, or past the ceiling: the same answer.
        Ok(Err(_)) | Err(_) => Err(ErrorCode::RequestTimedOut),
    }
}

/// What a request produced: a framed reply, or a closed connection with the
/// reason on the log — the protocol's own form of a refusal it cannot answer.
enum Answer {
    Reply(Vec<u8>),
    Close(String),
}

/// One idempotent producer on one topic, as the gateway orders its sets.
type ProducerKey = (String, i64, i16);

pub struct Gateway {
    /// Each idempotent producer's place in line (#457, review): the ticket is
    /// taken in the session's request order, BEFORE the append is handed to
    /// the blocking pool — a pool may start two queued tasks in either order,
    /// so a ticket taken inside the task could reverse two sets of one
    /// producer, a reordering the client never made and reads as fatal. The
    /// task then waits its turn on the pool, and a set behind a timed-out one
    /// waits until that append is done. An entry lives while a ticket is
    /// outstanding.
    producer_order: Arc<std::sync::Mutex<HashMap<ProducerKey, Arc<Turnstile>>>>,
    /// The consumer groups this gateway coordinates (#457 slice 2).
    groups: Arc<Coordinator>,
    /// Where committed offsets live; `None` refuses commits by name.
    offsets: Option<Arc<dyn OffsetStore>>,
    /// Whether this node still holds the range (review). `None` where nothing
    /// can take the range away — an embedder with no lease at all — and the
    /// gateway speaks for it unconditionally, as it did before this existed.
    lease: Option<Arc<dyn LeaseView>>,
    /// Each group's commits in turn (review): a commit is judged under its
    /// generation and then written, and a slow write must not let a later
    /// commit — a newer generation's, after the member lapsed — be judged,
    /// written and then overtaken by the older one landing last. One turn
    /// per group, held from the judgment through the store's answer under
    /// the request deadline; an entry lives while a turn is outstanding.
    commit_order: Arc<std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    /// Every call handed to another task, for the drain (review).
    jobs: Arc<Jobs>,
    /// Commits refused for want of a store, for a warning that does not
    /// repeat itself per request.
    offsetless_refusals: std::sync::atomic::AtomicU64,
    bridge: Arc<dyn Bridge>,
    config: GatewayConfig,
    sessions: Arc<tokio::sync::Semaphore>,
    refused_sessions: std::sync::atomic::AtomicU64,
    /// Set once `serve` stops accepting (review): a frame completed after
    /// that is dropped, never answered, so nothing starts a bridge call
    /// after the embedder was told the gateway is done.
    closed: std::sync::atomic::AtomicBool,
}

impl Gateway {
    /// The store committed offsets go to. Without one, OffsetCommit is
    /// refused by name and OffsetFetch answers that nothing is committed —
    /// both true — rather than the gateway remembering what it would forget.
    pub fn with_offsets(mut self, store: Arc<dyn OffsetStore>) -> Self {
        self.offsets = Some(store);
        self
    }

    /// The lease behind everything this gateway says about the range
    /// (review): once it is gone, the gateway stops claiming to lead the
    /// partition and to coordinate its groups, so a stock client refreshes
    /// its metadata and finds the node that holds it now. Without this, a
    /// fenced listener that stays reachable would keep answering
    /// FindCoordinator with itself and a consumer would retry there for good.
    pub fn with_lease(mut self, lease: Arc<dyn LeaseView>) -> Self {
        self.lease = Some(lease);
        self
    }

    /// Whether this gateway still speaks for its range. `Unknown` is not
    /// evidence of a handoff — a busy broker view is not a lost lease — so it
    /// keeps serving and the commit path answers retryably.
    fn speaks_for_the_range(&self) -> bool {
        !matches!(
            self.lease.as_ref().map(|lease| lease.lease()),
            Some(LeaseState::Gone)
        )
    }

    /// The refusal a fenced gateway gives the group protocol: retriable, and
    /// the client's next FindCoordinator (here or at another broker) is what
    /// finds the holder.
    fn fenced(&self, api: &str) -> Option<ErrorCode> {
        (!self.speaks_for_the_range()).then(|| {
            tracing::warn!(
                api,
                "kafka {api} refused: this node no longer holds the range's lease, so it does not \
                 coordinate its groups; the client finds the holder"
            );
            ErrorCode::CoordinatorNotAvailable
        })
    }

    pub fn new(bridge: Arc<dyn Bridge>, config: GatewayConfig) -> Self {
        if config.groups.min_session_timeout <= config.max_fetch_wait.max(config.max_offset_wait) {
            // Said once, at construction (review): a session may be shorter
            // than one long poll or one commit, and a heartbeat queued behind
            // either on the same connection is read after the session it
            // keeps alive has lapsed.
            tracing::warn!(
                min_session_timeout = ?config.groups.min_session_timeout,
                max_fetch_wait = ?config.max_fetch_wait,
                max_offset_wait = ?config.max_offset_wait,
                "kafka gateway: the minimum session timeout does not clear the fetch or offset ceiling; a heartbeat queued behind a long poll or a commit can outlive its session"
            );
        }
        let group_config = config.groups.clone();
        let sessions = Arc::new(tokio::sync::Semaphore::new(config.max_sessions.max(1)));
        Self {
            bridge,
            config,
            sessions,
            refused_sessions: std::sync::atomic::AtomicU64::new(0),
            producer_order: Arc::new(std::sync::Mutex::new(HashMap::new())),
            groups: Arc::new(Coordinator::new(group_config)),
            offsets: None,
            lease: None,
            commit_order: Arc::new(std::sync::Mutex::new(HashMap::new())),
            jobs: Arc::new(Jobs::default()),
            offsetless_refusals: std::sync::atomic::AtomicU64::new(0),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Serve until `shutdown` reads `true`. A dropped sender is not a signal
    /// (it would drain a healthy gateway an embedder merely stopped watching);
    /// only a real `true` stops the listener.
    pub async fn serve(
        self,
        listener: TcpListener,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> std::io::Result<()> {
        let gateway = Arc::new(self);
        // A receiver already reading `true` never changes again (review):
        // judged before the first accept, not after a change that will not
        // come.
        if *shutdown.borrow() {
            return Ok(());
        }
        // The coordinator's clock (#457 slice 2): lapsed sessions, rounds
        // whose window closed, minted ids nobody came back for. Stops with
        // the listener; a dropped shutdown sender is not a signal here either.
        let sweeper = {
            let groups = Arc::clone(&gateway.groups);
            let mut shutdown = shutdown.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_millis(250));
                let mut watching = true;
                loop {
                    tokio::select! {
                        _ = tick.tick() => groups.sweep(std::time::Instant::now()),
                        changed = shutdown.changed(), if watching => match changed {
                            Ok(()) if *shutdown.borrow() => break,
                            Ok(()) => {}
                            Err(_) => watching = false,
                        },
                    }
                }
            })
        };
        let mut watching = true;
        loop {
            tokio::select! {
                changed = shutdown.changed(), if watching => {
                    match changed {
                        Ok(()) if *shutdown.borrow() => break,
                        Ok(()) => {}
                        Err(_) => watching = false,
                    }
                }
                accepted = listener.accept() => {
                    let (socket, peer) = accepted?;
                    // One slot per session (review); none free is a closed
                    // socket, at once, and a warning that thins out as the
                    // refusals pile up rather than one line per attempt.
                    let permit = match Arc::clone(&gateway.sessions).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            let refused = gateway
                                .refused_sessions
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                                + 1;
                            if refused.is_power_of_two() {
                                tracing::warn!(
                                    %peer,
                                    open = gateway.config.max_sessions,
                                    refused,
                                    "kafka session refused: every session slot is open; connection closed"
                                );
                            }
                            drop(socket);
                            continue;
                        }
                    };
                    let gateway = Arc::clone(&gateway);
                    let session_shutdown = shutdown.clone();
                    tokio::spawn(async move {
                        let _slot = permit;
                        if let Err(error) = gateway.session(socket, session_shutdown).await {
                            tracing::debug!(%peer, %error, "kafka session ended");
                        }
                    });
                }
            }
        }
        // Closed before the drain (review): a request already read is
        // answered, one still arriving is dropped, and none starts a bridge
        // call after this. The drain then waits for the answers in flight:
        // every session closes between frames and its slot comes back.
        // Holding every slot is holding proof that no request is in flight;
        // past `drain_timeout` the embedder is told, and what is still in a
        // bridge call holds the broker's own lock, which the embedder's
        // final commit takes after it.
        sweeper.abort();
        // Parked joins and syncs are released now (review), so the drain
        // below is bounded by the produce and fetch ceilings, never by a
        // rebalance timeout.
        gateway.groups.shutdown();
        gateway
            .closed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // Every session ends within the ceilings — `max_produce_wait`,
        // `max_fetch_wait` and `max_offset_wait` bound the calls (review),
        // and shutdown is heard between frames — so waiting for every slot is
        // bounded by config,
        // not by hope (review): `drain_timeout` is where the wait is
        // reported, and the sum of the ceilings past it is where a session
        // still open is a bug, said at error. `serve` returning is the
        // proof the embedder relies on before it hands its range back.
        let slots = u32::try_from(gateway.config.max_sessions.max(1)).unwrap_or(u32::MAX);
        let open = |gateway: &Gateway| {
            slots - u32::try_from(gateway.sessions.available_permits()).unwrap_or(0)
        };
        let drain = gateway.sessions.acquire_many(slots);
        tokio::pin!(drain);
        if tokio::time::timeout(gateway.config.drain_timeout, &mut drain)
            .await
            .is_err()
        {
            tracing::warn!(
                open = open(&gateway),
                "kafka gateway still draining past drain_timeout: a session is inside a bridge \
                 call, which ends at its own ceiling"
            );
            let ceilings = gateway.config.max_produce_wait
                + gateway.config.max_fetch_wait
                + gateway.config.max_offset_wait
                + gateway.config.drain_timeout;
            if tokio::time::timeout(ceilings, &mut drain).await.is_err() {
                tracing::error!(
                    open = open(&gateway),
                    "kafka gateway stopped with sessions still open past every ceiling; a \
                     session that outlives its bridge call's ceiling is a bug"
                );
            }
        }
        // Then the work the sessions handed off (review): a bridge call a
        // deadline abandoned, a store write driven past its request. The
        // drain HOLDS until every one has ended (review): `serve` returning
        // is the embedder's proof that none still holds the bridge or the
        // store, so it is not given early. A call still running past every
        // ceiling is a bug, said at error while the wait goes on; what bounds
        // and reports a call that never ends is the embedder's own budget
        // around `serve`, not this returning without the proof.
        let ceilings = gateway.config.max_produce_wait
            + gateway.config.max_fetch_wait
            + gateway.config.max_offset_wait
            + gateway.config.drain_timeout;
        if tokio::time::timeout(ceilings, gateway.jobs.idle())
            .await
            .is_err()
        {
            tracing::error!(
                jobs = gateway.jobs.active(),
                "kafka gateway waiting on bridge or store calls still running past every \
                 ceiling; a call that outlives its ceiling is a bug — the drain holds until \
                 they end"
            );
            gateway.jobs.idle().await;
        }
        Ok(())
    }

    async fn session(
        self: Arc<Self>,
        mut socket: TcpStream,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> std::io::Result<()> {
        loop {
            // Between frames is where shutdown is heard (review): a request
            // already read is answered before the connection closes.
            let read = tokio::select! {
                read = tokio::time::timeout(self.config.idle_timeout, socket.read_i32()) => read,
                _ = shutdown_signalled(&mut shutdown) => {
                    tracing::debug!("kafka session closed on shutdown");
                    return Ok(());
                }
            };
            let len = match read {
                Err(_) => {
                    tracing::debug!("kafka session idle past the limit; connection closed");
                    return Ok(());
                }
                Ok(Ok(len)) => len,
                Ok(Err(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(())
                }
                Ok(Err(error)) => return Err(error),
            };
            if len < 0 || len as usize > self.config.max_frame_bytes {
                tracing::warn!(
                    len,
                    max = self.config.max_frame_bytes,
                    "kafka request frame refused: length outside the bound; connection closed"
                );
                return Ok(());
            }
            // The body as it arrives (review): the buffer grows with the
            // bytes received, never with the length announced, and the wait
            // for the rest of a frame has an end. A peer that announces
            // 32 MiB and stops holds what it sent, for `frame_read_timeout`.
            let len = len as usize;
            let mut body = Vec::with_capacity(len.min(INITIAL_FRAME_CAPACITY));
            let mut rest = (&mut socket).take(len as u64);
            // A frame still arriving when shutdown comes is dropped (review):
            // it was never a request, and waiting for its tail would hold
            // the drain for `frame_read_timeout`.
            let read = tokio::select! {
                read = tokio::time::timeout(self.config.frame_read_timeout, rest.read_to_end(&mut body)) => read,
                _ = shutdown_signalled(&mut shutdown) => {
                    tracing::debug!(len, received = body.len(), "kafka session closed on shutdown mid-frame");
                    return Ok(());
                }
            };
            match read {
                Err(_) => {
                    tracing::warn!(
                        len,
                        received = body.len(),
                        "kafka request body not delivered within the frame read timeout; connection closed"
                    );
                    return Ok(());
                }
                Ok(Err(error)) => return Err(error),
                // The peer went away mid-frame: a session over, not a request.
                Ok(Ok(received)) if received < len => return Ok(()),
                Ok(Ok(_)) => {}
            }
            // Read whole, but after the gateway closed (review): dropped, so
            // no bridge call starts past the point the embedder was told
            // none would.
            if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
                tracing::debug!("kafka request arrived after the gateway closed; dropped");
                return Ok(());
            }
            match self.answer(&body).await {
                Answer::Reply(bytes) => socket.write_all(&frame(&bytes)).await?,
                Answer::Close(reason) => {
                    tracing::warn!(%reason, "kafka request refused; connection closed");
                    return Ok(());
                }
            }
        }
    }

    async fn answer(&self, body: &[u8]) -> Answer {
        let mut d = Decoder::new(body);
        let header = match RequestHeader::decode(&mut d) {
            Ok(HeaderVerdict::Serve(header)) => header,
            Ok(HeaderVerdict::Refuse {
                header,
                code,
                reason,
            }) => {
                // ApiVersions is answered whatever the version, by protocol
                // rule: the v0 response carries the error and the served
                // ranges, so the client can downgrade. Anything else refused
                // at the header has no body this gateway can shape a reply
                // in, and is closed by name.
                if header.api_key == ApiKey::ApiVersions.as_i16() {
                    let mut out = Encoder::new();
                    write_response_header(&mut out, header.correlation_id, false);
                    encode_api_versions(&mut out, 0, code);
                    return Answer::Reply(out.into_vec());
                }
                return Answer::Close(format!("{reason} (correlation {})", header.correlation_id));
            }
            Err(error) => return Answer::Close(format!("malformed request header: {error}")),
        };
        let key =
            ApiKey::from_i16(header.api_key).expect("the header gate admits served keys only");
        let version = header.api_version;
        let mut out = Encoder::new();
        // No served version is flexible, so no response header carries
        // tagged fields; the gate above pins that.
        write_response_header(&mut out, header.correlation_id, false);
        let served = match key {
            ApiKey::ApiVersions => {
                encode_api_versions(&mut out, version, ErrorCode::None);
                Ok(None)
            }
            ApiKey::Metadata => match consumed(decode_metadata(&mut d, version), &d, "Metadata") {
                Ok(request) => match self.metadata(request).await {
                    Ok(response) => {
                        encode_metadata(&mut out, version, &response);
                        Ok(None)
                    }
                    Err(close) => Ok(Some(close)),
                },
                Err(error) => Err(error),
            },
            ApiKey::Produce => match consumed(decode_produce(&mut d, version), &d, "Produce") {
                Ok(request) => match self.produce(request).await {
                    Ok(topics) => {
                        encode_produce(&mut out, version, &topics);
                        Ok(None)
                    }
                    Err(close) => Ok(Some(close)),
                },
                Err(error) => Err(error),
            },
            ApiKey::Fetch => match consumed(decode_fetch(&mut d, version), &d, "Fetch") {
                Ok(request) => {
                    let topics = self.fetch(request).await;
                    encode_fetch(&mut out, version, &topics);
                    Ok(None)
                }
                Err(error) => Err(error),
            },
            ApiKey::ListOffsets => {
                match consumed(decode_list_offsets(&mut d, version), &d, "ListOffsets") {
                    Ok(request) => {
                        let topics = self.list_offsets(request).await;
                        encode_list_offsets(&mut out, version, &topics);
                        Ok(None)
                    }
                    Err(error) => Err(error),
                }
            }
            ApiKey::InitProducerId => {
                match consumed(
                    decode_init_producer_id(&mut d, version),
                    &d,
                    "InitProducerId",
                ) {
                    Ok(request) => {
                        let (error, producer_id, producer_epoch) = self.init_producer_id(request);
                        encode_init_producer_id(
                            &mut out,
                            version,
                            error,
                            producer_id,
                            producer_epoch,
                        );
                        Ok(None)
                    }
                    Err(error) => Err(error),
                }
            }
            ApiKey::FindCoordinator => {
                match consumed(
                    decode_find_coordinator(&mut d, version),
                    &d,
                    "FindCoordinator",
                ) {
                    Ok(request) => {
                        let response = self.find_coordinator(request);
                        encode_find_coordinator(&mut out, version, &response);
                        Ok(None)
                    }
                    Err(error) => Err(error),
                }
            }
            ApiKey::JoinGroup => {
                match consumed(decode_join_group(&mut d, version), &d, "JoinGroup") {
                    Ok(request) => {
                        let response = self
                            .join_group(request, version, header.client_id.as_deref())
                            .await;
                        encode_join_group(&mut out, version, &response);
                        Ok(None)
                    }
                    Err(error) => Err(error),
                }
            }
            ApiKey::SyncGroup => {
                match consumed(decode_sync_group(&mut d, version), &d, "SyncGroup") {
                    Ok(request) => {
                        let response = self.sync_group(request).await;
                        encode_sync_group(&mut out, version, &response);
                        Ok(None)
                    }
                    Err(error) => Err(error),
                }
            }
            ApiKey::Heartbeat => match consumed(decode_heartbeat(&mut d, version), &d, "Heartbeat")
            {
                Ok(request) => {
                    let error = self.fenced("Heartbeat").unwrap_or_else(|| {
                        self.groups
                            .heartbeat(&request.group_id, request.generation_id, &request.member_id)
                            .err()
                            .unwrap_or(ErrorCode::None)
                    });
                    encode_error_only(&mut out, version, error);
                    Ok(None)
                }
                Err(error) => Err(error),
            },
            ApiKey::LeaveGroup => {
                match consumed(decode_leave_group(&mut d, version), &d, "LeaveGroup") {
                    Ok(request) => {
                        let error = self.fenced("LeaveGroup").unwrap_or_else(|| {
                            self.groups
                                .leave(&request.group_id, &request.member_id)
                                .err()
                                .unwrap_or(ErrorCode::None)
                        });
                        encode_error_only(&mut out, version, error);
                        Ok(None)
                    }
                    Err(error) => Err(error),
                }
            }
            ApiKey::OffsetCommit => {
                match consumed(decode_offset_commit(&mut d, version), &d, "OffsetCommit") {
                    Ok(request) => {
                        let topics = self.offset_commit(request).await;
                        encode_offset_commit(&mut out, version, &topics);
                        Ok(None)
                    }
                    Err(error) => Err(error),
                }
            }
            ApiKey::OffsetFetch => {
                match consumed(decode_offset_fetch(&mut d, version), &d, "OffsetFetch") {
                    Ok(request) => {
                        let response = self.offset_fetch(request).await;
                        encode_offset_fetch(&mut out, version, &response);
                        Ok(None)
                    }
                    Err(error) => Err(error),
                }
            }
        };
        match served {
            Ok(None) => Answer::Reply(out.into_vec()),
            Ok(Some(close)) => Answer::Close(close),
            Err(error) => Answer::Close(format!(
                "malformed {} v{version} body (correlation {}): {error}",
                key.name(),
                header.correlation_id
            )),
        }
    }

    /// Off the runtime's threads (review): a real bridge answers from the
    /// broker, behind the lock an append holds across its fsync, and a
    /// Metadata storm must not park every worker behind it.
    async fn bounds(&self, name: &str) -> Result<(i64, i64), ErrorCode> {
        let bridge = Arc::clone(&self.bridge);
        let name = name.to_owned();
        until(
            tokio::time::Instant::now() + self.config.max_fetch_wait,
            self.jobs.blocking(move || bridge.bounds(&name)),
        )
        .await
    }

    /// `Err` closes the connection: a Metadata naming no topics has no slot
    /// for a code when the bridge does not enumerate its topics in time
    /// (audit) — an empty list would read as "no topics", which is not known.
    async fn metadata(&self, request: MetadataRequest) -> Result<MetadataResponse, String> {
        let names = match request.topics {
            Some(names) => names,
            // Under the same ceiling as the bounds lookups below (review): a
            // Metadata naming no topics waits on the bridge no longer than one
            // naming them all. Not done in time: the connection closes
            // (audit), the protocol's form of a refusal it cannot answer, and
            // the client's next metadata refresh asks again.
            None => match self
                .served_topics(tokio::time::Instant::now() + self.config.max_fetch_wait)
                .await
            {
                Ok(names) => names,
                Err(error) => {
                    return Err(format!(
                        "Metadata naming no topics: the bridge did not enumerate its topics \
                         within the ceiling ({error:?})"
                    ))
                }
            },
        };
        // A fenced gateway leads nothing (review): the partition is answered
        // `NOT_LEADER_OR_FOLLOWER` with no leader, which is the code every
        // stock client answers by refreshing its metadata elsewhere.
        let leads = self.speaks_for_the_range();
        let mut topics = Vec::with_capacity(names.len());
        for name in names {
            topics.push(match self.bounds(&name).await {
                Ok(_) if !leads => MetadataTopic {
                    error: ErrorCode::NotLeaderOrFollower,
                    name,
                    leader: None,
                },
                Ok(_) => MetadataTopic {
                    error: ErrorCode::None,
                    name,
                    leader: Some(self.config.node_id),
                },
                // Never created here, whatever `allow_auto_topic_creation`
                // said: a topic is a range the metadata plane granted, not
                // a name a producer typed.
                Err(ErrorCode::UnknownTopicOrPartition) => MetadataTopic {
                    error: ErrorCode::UnknownTopicOrPartition,
                    name,
                    leader: None,
                },
                // A topic the bridge knows but cannot vouch for right now
                // (review) — fenced, overloaded, storage trouble — keeps its
                // own code and no leader: the client retries its metadata,
                // instead of reading an existing range as absent.
                Err(error) => MetadataTopic {
                    error,
                    name,
                    leader: None,
                },
            });
        }
        Ok(MetadataResponse {
            brokers: vec![MetadataBroker {
                node_id: self.config.node_id,
                host: self.config.advertised_host.clone(),
                port: self.config.advertised_port,
            }],
            cluster_id: self.config.cluster_id.clone(),
            controller_id: self.config.node_id,
            topics,
        })
    }

    /// `Err` closes the connection: the one produce shape with no answer.
    /// The bridge's topics, off the runtime's threads (review): a backend's
    /// `topics()` may wait on a lock or storage — and no longer than the
    /// caller's deadline (review): an enumeration not done by then is
    /// `REQUEST_TIMED_OUT`, so no request waits on the bridge past its
    /// ceiling, and the drain's budget holds.
    async fn served_topics(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<String>, ErrorCode> {
        // Nothing is spawned past the deadline (review): a blocking task
        // cannot be cancelled, and a deadline the turn or the membership
        // check already spent must not leave one behind.
        if tokio::time::Instant::now() >= deadline {
            return Err(ErrorCode::RequestTimedOut);
        }
        let bridge = Arc::clone(&self.bridge);
        tokio::time::timeout_at(deadline, self.jobs.blocking(move || bridge.topics()))
            .await
            .map(|joined| joined.map_err(|_| ErrorCode::RequestTimedOut))
            .unwrap_or(Err(ErrorCode::RequestTimedOut))
    }

    /// FindCoordinator (#457 slice 2): this gateway is the coordinator of
    /// every group it serves — one range, one partition, one leader — so the
    /// answer is itself, for any group. A transaction key is out of scope, by
    /// name; an empty key names no group.
    fn find_coordinator(&self, request: FindCoordinatorRequest) -> FindCoordinatorResponse {
        let refused = |error: ErrorCode, message: &str| FindCoordinatorResponse {
            error,
            error_message: Some(message.to_owned()),
            node_id: -1,
            host: String::new(),
            port: -1,
        };
        if request.key_type != 0 {
            tracing::warn!(
                key = %request.key,
                key_type = request.key_type,
                "kafka FindCoordinator refused: transactions are out of scope (#457 non-goal); \
                 groups (key_type 0) are coordinated here"
            );
            return refused(
                ErrorCode::UnsupportedForMessageFormat,
                "transactions are out of scope; this gateway coordinates groups only",
            );
        }
        if request.key.is_empty() {
            return refused(
                ErrorCode::InvalidGroupId,
                "an empty group id names no group",
            );
        }
        // Only while the lease is this node's (review): a fenced listener
        // that named itself would hold a consumer at a node that cannot take
        // its commits.
        if let Some(error) = self.fenced("FindCoordinator") {
            return refused(
                error,
                "this node no longer holds the range's lease; ask another broker for the coordinator",
            );
        }
        FindCoordinatorResponse {
            error: ErrorCode::None,
            error_message: None,
            node_id: self.config.node_id,
            host: self.config.advertised_host.clone(),
            port: self.config.advertised_port,
        }
    }

    /// JoinGroup: held by the coordinator until the round completes.
    async fn join_group(
        &self,
        request: JoinGroupRequest,
        version: i16,
        client_id: Option<&str>,
    ) -> JoinGroupResponse {
        if let Some(error) = self.fenced("JoinGroup") {
            return JoinGroupResponse {
                error,
                generation_id: -1,
                protocol_name: String::new(),
                leader: String::new(),
                member_id: request.member_id,
                members: Vec::new(),
            };
        }
        let millis = |ms: i32| Duration::from_millis(u64::try_from(ms).unwrap_or(0));
        let member_id_sent = request.member_id.clone();
        let joined = self
            .groups
            .join(JoinRequest {
                group_id: request.group_id,
                member_id: request.member_id,
                client_id: client_id.unwrap_or("client").to_owned(),
                protocol_type: request.protocol_type,
                protocols: request.protocols,
                session_timeout: millis(request.session_timeout_ms),
                rebalance_timeout: millis(request.rebalance_timeout_ms),
                require_member_id: version >= 4,
            })
            .await;
        match joined {
            Ok(Joined::Complete(outcome)) => JoinGroupResponse {
                error: ErrorCode::None,
                generation_id: outcome.generation,
                protocol_name: outcome.protocol_name,
                leader: outcome.leader,
                member_id: outcome.member_id,
                members: outcome.members,
            },
            Ok(Joined::MemberIdRequired(member_id)) => JoinGroupResponse {
                error: ErrorCode::MemberIdRequired,
                generation_id: -1,
                protocol_name: String::new(),
                leader: String::new(),
                member_id,
                members: Vec::new(),
            },
            Err(error) => JoinGroupResponse {
                error,
                generation_id: -1,
                protocol_name: String::new(),
                leader: String::new(),
                member_id: member_id_sent,
                members: Vec::new(),
            },
        }
    }

    /// SyncGroup: the leader's assignments are the client-side assignor's,
    /// run through this gateway's topic map before any member sees them.
    async fn sync_group(&self, request: SyncGroupRequest) -> SyncGroupResponse {
        if let Some(error) = self.fenced("SyncGroup") {
            return SyncGroupResponse {
                error,
                assignment: Vec::new(),
            };
        }
        // Membership, generation, state and leadership first (review): a
        // stale member's retry hears the coordinator's own code, not a
        // verdict on assignments it is not entitled to make. Only the leader
        // of a completing round has its assignments judged, and only a
        // consumer group's are readable; `sync` judges again under its lock
        // when it applies them.
        match self
            .groups
            .check_sync(&request.group_id, request.generation_id, &request.member_id)
        {
            Err(error) => {
                return SyncGroupResponse {
                    error,
                    assignment: Vec::new(),
                }
            }
            Ok(SyncStanding::Leader)
                if self.groups.protocol_type(&request.group_id).as_deref() == Some("consumer") =>
            {
                // The check's bridge call waits on the group protocol's
                // ceiling (review), as a commit's does — and is made once per
                // request (review), not once per assignment.
                let deadline = tokio::time::Instant::now() + self.config.max_offset_wait;
                let members = self.groups.member_ids(&request.group_id);
                if let Err((error, member, reason)) = self
                    .assignments_served(&request.assignments, &members, deadline)
                    .await
                {
                    tracing::warn!(
                        group = %request.group_id,
                        member = %member,
                        ?error,
                        %reason,
                        "kafka SyncGroup refused"
                    );
                    // The round the assignment was for is over (review): the
                    // followers parked on it rejoin now, not at the sync
                    // deadline.
                    self.groups.assignment_refused(
                        &request.group_id,
                        request.generation_id,
                        &request.member_id,
                    );
                    return SyncGroupResponse {
                        error,
                        assignment: Vec::new(),
                    };
                }
            }
            Ok(_) => {}
        }
        match self
            .groups
            .sync(
                &request.group_id,
                request.generation_id,
                &request.member_id,
                request.assignments,
            )
            .await
        {
            Ok(assignment) => SyncGroupResponse {
                error: ErrorCode::None,
                assignment,
            },
            Err(error) => SyncGroupResponse {
                error,
                assignment: Vec::new(),
            },
        }
    }

    /// Every partition a consumer-protocol assignment names must be one this
    /// gateway serves: partition 0 of a topic behind the bridge — every
    /// assignment judged against ONE enumeration of the topics (review), made
    /// only when an assignment names anything, so a SyncGroup of a thousand
    /// assignments is one bridge call, not a thousand. An assignment that
    /// names more is `InconsistentGroupProtocol`, with the member it was for;
    /// a bridge that did not enumerate its topics by the deadline is
    /// `RequestTimedOut` — its own code, not the assignment's (review). Only
    /// assignments for current `members` are judged (review): one for an id
    /// that is not a member is ignored when applied, as Kafka ignores it, so
    /// its bytes carry no verdict for the group.
    async fn assignments_served(
        &self,
        assignments: &[(String, Vec<u8>)],
        members: &[String],
        deadline: tokio::time::Instant,
    ) -> Result<(), (ErrorCode, String, String)> {
        let mut decoded = Vec::new();
        // One budget for the request (review) — topic entries and partitions —
        // across every assignment: judged as each decodes, so nothing beyond
        // it is built.
        let mut budget = crate::api_groups::AssignmentBudget::default();
        // A member named twice: the last entry wins (review), as `sync`
        // applies them, so only the effective assignment is judged and charged.
        let mut effective: HashMap<&String, &Vec<u8>> = HashMap::new();
        for (member, assignment) in assignments {
            if members.contains(member) {
                effective.insert(member, assignment);
            }
        }
        let mut effective: Vec<(&String, &Vec<u8>)> = effective.into_iter().collect();
        effective.sort_by(|a, b| a.0.cmp(b.0));
        for (member, assignment) in effective {
            if assignment.is_empty() {
                continue;
            }
            let partitions =
                consumer_assignment_partitions(assignment, &mut budget).map_err(|error| {
                    (
                        ErrorCode::InconsistentGroupProtocol,
                        member.clone(),
                        format!("malformed assignment: {error}"),
                    )
                })?;
            decoded.push((member, partitions));
        }
        if decoded.is_empty() {
            return Ok(());
        }
        let served = self.served_topics(deadline).await.map_err(|error| {
            (
                error,
                String::new(),
                "the bridge did not enumerate its topics within the ceiling".to_owned(),
            )
        })?;
        for (member, topics) in decoded {
            for (topic, partitions) in topics {
                for partition in partitions {
                    if partition != 0 || !served.contains(&topic) {
                        return Err((
                            ErrorCode::InconsistentGroupProtocol,
                            member.clone(),
                            format!(
                                "partition {partition} of {topic:?}: this gateway serves partition 0 of {served:?}"
                            ),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn no_offset_store(&self, api: &str) {
        let refused = self
            .offsetless_refusals
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        if refused.is_power_of_two() {
            tracing::warn!(
                api,
                refused,
                "kafka {api} refused: this gateway has no durable offset store (the metadata \
                 plane's cursor is #457's next slice); a committed offset it would forget is not \
                 taken"
            );
        }
    }

    /// OffsetCommit: a member's commit under its generation, or a simple
    /// consumer's; every partition judged by name, then the store's. A
    /// group's commits take turns (review): judgment and write are one turn,
    /// held until every write has ENDED — not merely until the request's
    /// deadline (review): a write the deadline abandoned may still land, and
    /// nothing newer is judged or written before it has.
    async fn offset_commit(&self, request: OffsetCommitRequest) -> Vec<OffsetCommitTopicResponse> {
        // One deadline for the whole request (review): the cap bounds the
        // request, not each of up to 4 096 entries in turn — the turn, the
        // topic enumeration, the watermark lookups and the waits on the
        // store's writes all wait on it.
        let deadline = tokio::time::Instant::now() + self.config.max_offset_wait;
        let order = self.commit_turn(&request.group_id);
        let Ok(turn) = tokio::time::timeout_at(deadline, Arc::clone(&order).lock_owned()).await
        else {
            commit_turn_done(&self.commit_order, &request.group_id, order);
            return every_partition(&request, ErrorCode::RequestTimedOut);
        };
        let (topics, outstanding) = self.commit_in_turn(&request, deadline).await;
        if outstanding.is_empty() {
            drop(turn);
            commit_turn_done(&self.commit_order, &request.group_id, order);
        } else {
            // Writes the deadline abandoned: the turn goes with them and is
            // handed back when the last has ended; the drain waits for this.
            let orders = Arc::clone(&self.commit_order);
            let group = request.group_id.clone();
            self.jobs.spawn(async move {
                for write in outstanding {
                    let _ = write.await;
                }
                drop(turn);
                commit_turn_done(&orders, &group, order);
            });
        }
        topics
    }

    /// The group's turn for commits: one per group, made on first use.
    fn commit_turn(&self, group: &str) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(
            self.commit_order
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .entry(group.to_owned())
                .or_default(),
        )
    }

    /// The commit itself, under the group's turn: the answers, and the
    /// writes still running when the deadline passed.
    async fn commit_in_turn(
        &self,
        request: &OffsetCommitRequest,
        deadline: tokio::time::Instant,
    ) -> (
        Vec<OffsetCommitTopicResponse>,
        Vec<tokio::task::JoinHandle<Result<(), ErrorCode>>>,
    ) {
        let mut outstanding = Vec::new();
        // Membership first (review): a stale generation or an unknown member
        // is answered as such whatever the bridge does next — a client acts
        // on that code, rejoining or correcting its identity, and a bridge
        // error in its place would read as a transient storage fault.
        if let Err(error) =
            self.groups
                .check_commit(&request.group_id, request.generation_id, &request.member_id)
        {
            return (every_partition(request, error), outstanding);
        }
        // A commit naming a static instance (v7) is an unknown member's
        // (review), judged after the group id and the membership as every
        // group operation judges them: no static member exists on this
        // gateway — the versions that admit one are not served — so the
        // instance it names is not one this coordinator knows.
        if request.group_instance_id.is_some() {
            return (
                every_partition(request, ErrorCode::UnknownMemberId),
                outstanding,
            );
        }
        // No store (review): refused by name before any bridge work — the
        // answer can be nothing else, so a busy bridge must not turn a
        // deterministic refusal into a timeout, nor a client's routine
        // auto-commits into backend work that cannot succeed.
        let Some(store) = self.offsets.as_ref() else {
            self.no_offset_store("OffsetCommit");
            return (
                every_partition(request, ErrorCode::UnsupportedForMessageFormat),
                outstanding,
            );
        };
        // Judged by name first (review): what no bridge can change — a
        // partition this gateway never serves, a negative offset, metadata
        // over the cap — is decided before the bridge is asked anything, so a
        // deterministic refusal never turns into a bridge error, and a commit
        // that cannot succeed costs no bridge work.
        let by_name = |p: &crate::api_groups::OffsetCommitPartition| -> Option<ErrorCode> {
            if p.partition != 0 {
                Some(ErrorCode::UnknownTopicOrPartition)
            } else if p.offset < 0 {
                // -1 is the wire's "nothing committed"; a position below zero
                // is not one a consumer resumes from (review).
                Some(ErrorCode::OffsetOutOfRange)
            } else if p
                .metadata
                .as_ref()
                .is_some_and(|m| m.len() > MAX_OFFSET_METADATA_BYTES)
            {
                Some(ErrorCode::OffsetMetadataTooLarge)
            } else {
                None
            }
        };
        let mut verdicts: Vec<Vec<Option<ErrorCode>>> = request
            .topics
            .iter()
            .map(|topic| topic.partitions.iter().map(by_name).collect())
            .collect();
        let answered = |verdicts: &[Vec<Option<ErrorCode>>], fill: ErrorCode| {
            request
                .topics
                .iter()
                .zip(verdicts)
                .map(|(topic, decided)| OffsetCommitTopicResponse {
                    name: topic.name.clone(),
                    partitions: topic
                        .partitions
                        .iter()
                        .zip(decided)
                        .map(|(p, verdict)| (p.partition, verdict.unwrap_or(fill)))
                        .collect(),
                })
                .collect::<Vec<_>>()
        };
        let needs_bridge = verdicts
            .iter()
            .any(|decided| decided.iter().any(Option::is_none));
        if !needs_bridge {
            return (answered(&verdicts, ErrorCode::None), outstanding);
        }
        let served = match self.served_topics(deadline).await {
            Ok(served) => served,
            Err(error) => {
                // The bridge's code for what was left to it; what was decided
                // by name keeps its own.
                for decided in verdicts.iter_mut() {
                    for verdict in decided.iter_mut() {
                        verdict.get_or_insert(error);
                    }
                }
                return (answered(&verdicts, error), outstanding);
            }
        };
        // The watermark of every topic still to judge (review, #468): a
        // committed offset is the next one to consume, so it may reach the
        // watermark and no further — a position past what exists would be a
        // cursor metadata cannot bound, and a group lying itself out of
        // retention's protection. The head is known HERE, at the bridge, and
        // nowhere else. Each lookup waits on the request deadline (review): a
        // bridge blocked on its storage or its lock is not a wait the cap
        // leaves unbounded, and a watermark not given in time is a commit not
        // taken. A topic decided whole by name, or not served, is not looked
        // up.
        let mut watermarks: HashMap<String, Result<i64, ErrorCode>> = HashMap::new();
        for (topic, decided) in request.topics.iter().zip(&verdicts) {
            if decided.iter().all(Option::is_some)
                || !served.contains(&topic.name)
                || watermarks.contains_key(&topic.name)
            {
                continue;
            }
            // Nothing is spawned for a topic the deadline has already passed
            // (review): a blocking task cannot be cancelled, so a lookup that
            // ate the deadline must not leave one behind for each of up to
            // 1 024 topics — those are answered as the timed-out one was.
            let watermark = if tokio::time::Instant::now() >= deadline {
                Err(ErrorCode::RequestTimedOut)
            } else {
                let bridge = Arc::clone(&self.bridge);
                let name = topic.name.clone();
                tokio::time::timeout_at(
                    deadline,
                    self.jobs.blocking(move || bridge.high_watermark(&name)),
                )
                .await
                .map(|joined| joined.unwrap_or(Err(ErrorCode::RequestTimedOut)))
                .unwrap_or(Err(ErrorCode::RequestTimedOut))
            };
            watermarks.insert(topic.name.clone(), watermark);
        }
        let mut topics = Vec::with_capacity(request.topics.len());
        for (topic, decided) in request.topics.iter().zip(verdicts) {
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for (p, verdict) in topic.partitions.iter().zip(decided) {
                let watermark = watermarks.get(&topic.name);
                let refused = verdict.or_else(|| {
                    if !served.contains(&topic.name) {
                        Some(ErrorCode::UnknownTopicOrPartition)
                    } else if let Some(Err(error)) = watermark {
                        // A watermark the bridge refused, or did not give
                        // before the deadline: its code, not a guess.
                        Some(*error)
                    } else if matches!(watermark, Some(Ok(w)) if p.offset > *w) {
                        // Past the watermark: not a position a consumer can
                        // resume from.
                        Some(ErrorCode::OffsetOutOfRange)
                    } else {
                        None
                    }
                });
                let error = match refused {
                    Some(error) => error,
                    // Nothing is started past the deadline (review); a write
                    // started is driven to its end in a task of its own, so
                    // the deadline abandons the WAIT, never the write — which
                    // the turn then outlives.
                    None if tokio::time::Instant::now() >= deadline => ErrorCode::RequestTimedOut,
                    None => {
                        let store = Arc::clone(store);
                        let group = request.group_id.clone();
                        let name = topic.name.clone();
                        let partition = p.partition;
                        let committed = Committed {
                            offset: p.offset,
                            metadata: p.metadata.clone(),
                        };
                        let mut write = self.jobs.spawn(async move {
                            store.commit(&group, &name, partition, committed).await
                        });
                        match tokio::time::timeout_at(deadline, &mut write).await {
                            Ok(Ok(Ok(()))) => ErrorCode::None,
                            Ok(Ok(Err(error))) => error,
                            // Panicked: no answer to give but the timeout's.
                            Ok(Err(_)) => ErrorCode::RequestTimedOut,
                            Err(_) => {
                                outstanding.push(write);
                                ErrorCode::RequestTimedOut
                            }
                        }
                    }
                };
                partitions.push((p.partition, error));
            }
            topics.push(OffsetCommitTopicResponse {
                name: topic.name.clone(),
                partitions,
            });
        }
        (topics, outstanding)
    }

    /// OffsetFetch: what the store holds; without a store, nothing is
    /// committed anywhere, and -1 says so truthfully, so a client falls back
    /// to its reset policy instead of failing.
    async fn offset_fetch(&self, request: OffsetFetchRequest) -> OffsetFetchResponse {
        let deadline = tokio::time::Instant::now() + self.config.max_offset_wait;
        if request.group_id.is_empty() {
            // Every asked partition says so (review): v1 has no group-level
            // field to carry the code, and an empty answer there would read
            // as success.
            return OffsetFetchResponse {
                error: ErrorCode::InvalidGroupId,
                topics: every_asked(
                    request.topics.unwrap_or_default(),
                    ErrorCode::InvalidGroupId,
                ),
            };
        }
        // Named topics, or — a null list at v2+ — every partition the group
        // has COMMITTED (review): what the store holds for the group, not the
        // topics served; with no store, nothing is committed anywhere.
        let asked: Vec<(String, Vec<i32>)> = match request.topics {
            Some(asked) => asked,
            None => {
                // A null topic list at v2+: every partition the group has
                // COMMITTED, answered from the stored rows themselves
                // (review) — a committed offset for a topic this gateway no
                // longer serves is still what the group committed. With no
                // store, nothing is committed anywhere.
                let Some(store) = &self.offsets else {
                    return OffsetFetchResponse {
                        error: ErrorCode::None,
                        topics: Vec::new(),
                    };
                };
                // Bounded in what it answers (review): as many partitions as
                // a request may name; a group that has committed more is
                // refused by name — a client names the partitions it wants,
                // as every stock consumer does, and the null form is tooling's.
                let at_most = crate::api::MAX_PARTITIONS_PER_REQUEST;
                // The listing is handed to a task of its own, like a write
                // (audit): the deadline abandons the wait, the listing ends on
                // its own and counts in the drain.
                let listing = {
                    let store = Arc::clone(store);
                    let group = request.group_id.clone();
                    self.jobs
                        .spawn(async move { store.committed(&group, at_most).await })
                };
                return match tokio::time::timeout_at(deadline, listing)
                    .await
                    .map(|joined| joined.unwrap_or(Err(ErrorCode::RequestTimedOut)))
                    .unwrap_or(Err(ErrorCode::RequestTimedOut))
                {
                    Ok(rows) if rows.len() > at_most => {
                        tracing::warn!(
                            group = %request.group_id,
                            at_most,
                            "kafka OffsetFetch naming no topics: the group has committed more \
                             partitions than one answer carries; name the partitions wanted"
                        );
                        OffsetFetchResponse {
                            error: ErrorCode::InvalidRequest,
                            topics: Vec::new(),
                        }
                    }
                    Ok(rows) => {
                        // Grouped by topic in one pass (review): a map, not
                        // a scan of every topic seen so far for each row.
                        let mut by_topic: std::collections::BTreeMap<
                            String,
                            Vec<OffsetFetchPartitionResponse>,
                        > = std::collections::BTreeMap::new();
                        for (topic, partition, committed) in rows {
                            if !wire_carries_topic(&request.group_id, &topic) {
                                continue;
                            }
                            let metadata = carried_metadata(
                                &request.group_id,
                                &topic,
                                partition,
                                committed.metadata,
                            );
                            by_topic
                                .entry(topic)
                                .or_default()
                                .push(OffsetFetchPartitionResponse {
                                    partition,
                                    offset: committed.offset,
                                    metadata,
                                    error: ErrorCode::None,
                                });
                        }
                        let topics: Vec<OffsetFetchTopicResponse> = by_topic
                            .into_iter()
                            .map(|(name, partitions)| OffsetFetchTopicResponse { name, partitions })
                            .collect();
                        OffsetFetchResponse {
                            error: ErrorCode::None,
                            topics,
                        }
                    }
                    Err(error) => OffsetFetchResponse {
                        error,
                        topics: Vec::new(),
                    },
                };
            }
        };
        // A named partition is the store's answer (review), whatever this
        // gateway serves today: a committed offset for a topic that moved on
        // is still what the group committed, as the null-topic answer above
        // already holds, and a client naming its assignment must be able to
        // resume by it. The bridge is not consulted; nothing here waits on it.
        let mut topics = Vec::with_capacity(asked.len());
        for (name, parts) in asked {
            let mut partitions = Vec::with_capacity(parts.len());
            for partition in parts {
                let none = |error: ErrorCode| OffsetFetchPartitionResponse {
                    partition,
                    offset: -1,
                    metadata: None,
                    error,
                };
                // A read handed to a task of its own, like a write (audit):
                // the deadline abandons the WAIT, the read ends on its own and
                // counts in the drain; nothing is started past the deadline.
                let answer = match &self.offsets {
                    None => none(ErrorCode::None),
                    Some(_) if tokio::time::Instant::now() >= deadline => {
                        none(ErrorCode::RequestTimedOut)
                    }
                    Some(store) => {
                        let store = Arc::clone(store);
                        let group = request.group_id.clone();
                        let topic = name.clone();
                        let read = self
                            .jobs
                            .spawn(async move { store.fetch(&group, &topic, partition).await });
                        match tokio::time::timeout_at(deadline, read).await {
                            Ok(Ok(Ok(Some(committed)))) => OffsetFetchPartitionResponse {
                                partition,
                                offset: committed.offset,
                                metadata: carried_metadata(
                                    &request.group_id,
                                    &name,
                                    partition,
                                    committed.metadata,
                                ),
                                error: ErrorCode::None,
                            },
                            Ok(Ok(Ok(None))) => none(ErrorCode::None),
                            Ok(Ok(Err(error))) => none(error),
                            Ok(Err(_)) | Err(_) => none(ErrorCode::RequestTimedOut),
                        }
                    }
                };
                partitions.push(answer);
            }
            topics.push(OffsetFetchTopicResponse { name, partitions });
        }
        OffsetFetchResponse {
            error: ErrorCode::None,
            topics,
        }
    }

    /// An idempotent producer's id and epoch (#457): minted here, unique in
    /// this process and across its restarts on a sane clock, epoch zero. A
    /// transactional id is refused by name — transactions are out of scope —
    /// with the code the produce path gives them, so a client fails at the
    /// first step rather than at its first commit.
    fn init_producer_id(&self, request: InitProducerIdRequest) -> (ErrorCode, i64, i16) {
        if let Some(transactional_id) = request.transactional_id {
            tracing::warn!(
                %transactional_id,
                "kafka InitProducerId refused: transactions are out of scope (#457 non-goal); \
                 idempotence without a transactional id is served"
            );
            return (ErrorCode::UnsupportedForMessageFormat, -1, -1);
        }
        // The node id becomes the low byte of the minted id: an id that does
        // not fit is not truncated into another gateway's byte (review), it
        // is a gateway that cannot mint — refused by name, and the node's
        // config check refuses such an id before a gateway is ever built.
        let Ok(node_id) = u8::try_from(self.config.node_id) else {
            tracing::warn!(
                node_id = self.config.node_id,
                "kafka InitProducerId refused: node_id is outside 0..=255, the byte every minted \
                 producer id carries; this gateway cannot mint idempotent producers apart"
            );
            return (ErrorCode::UnsupportedForMessageFormat, -1, -1);
        };
        let producer_id = mint_producer_id(node_id);
        tracing::debug!(
            producer_id,
            "kafka InitProducerId: minted an idempotent producer id"
        );
        (ErrorCode::None, producer_id, 0)
    }

    async fn produce(&self, request: ProduceRequest) -> Result<Vec<ProduceTopicResponse>, String> {
        if request.acks == 0 {
            // acks=0 has no representation here: the broker acknowledges what
            // it durably appended and nothing else, and a client sending
            // acks=0 does not read the answer that would say so.
            return Err(
                "acks=0 refused by name: this gateway acknowledges what it durably appended, \
                 and acks=0 asks it not to; produce with acks=1 or acks=all (#225)"
                    .to_owned(),
            );
        }
        let refused_whole = request.transactional_id.as_ref().map(|id| {
            format!("transactional produce (transactional_id {id:?}) is out of scope (#225)")
        });
        // ONE deadline for the request (review): the client's `timeout_ms`
        // under the gateway's cap, shared by every partition — the appends
        // run one after another, and N of them must not take N budgets.
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(request.timeout_ms.max(1) as u64)
                .min(self.config.max_produce_wait);
        let mut topics = Vec::with_capacity(request.topics.len());
        for topic in request.topics {
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                let outcome = match &refused_whole {
                    Some(reason) => Err((ErrorCode::UnsupportedForMessageFormat, reason.clone())),
                    None => {
                        self.produce_partition(
                            &topic.name,
                            partition.index,
                            partition.records.as_deref(),
                            deadline,
                        )
                        .await
                    }
                };
                partitions.push(match outcome {
                    Ok(appended) => ProducePartitionResponse {
                        index: partition.index,
                        error: ErrorCode::None,
                        base_offset: appended.base_offset,
                        log_append_time_ms: appended.log_append_time_ms,
                        log_start_offset: appended.log_start_offset,
                        error_message: None,
                    },
                    Err((error, reason)) => {
                        tracing::warn!(
                            topic = %topic.name,
                            partition = partition.index,
                            code = error.as_i16(),
                            %reason,
                            "kafka produce refused"
                        );
                        ProducePartitionResponse {
                            index: partition.index,
                            error,
                            base_offset: -1,
                            log_append_time_ms: -1,
                            log_start_offset: -1,
                            error_message: Some(reason),
                        }
                    }
                });
            }
            topics.push(ProduceTopicResponse {
                name: topic.name,
                partitions,
            });
        }
        Ok(topics)
    }

    async fn produce_partition(
        &self,
        topic: &str,
        partition: i32,
        records: Option<&[u8]>,
        deadline: tokio::time::Instant,
    ) -> Result<crate::bridge::Appended, (ErrorCode, String)> {
        if partition != 0 {
            return Err((
                ErrorCode::UnknownTopicOrPartition,
                format!(
                    "partition {partition}: phase 1 serves one partition per topic, partition 0"
                ),
            ));
        }
        let Some(records) = records else {
            return Err((
                ErrorCode::InvalidRecord,
                "no records in the produce".to_owned(),
            ));
        };
        // EVERY batch the field carries (review): a RECORDS field may hold
        // several back to back, and the decoder stops at the first one's
        // declared length by design. All are decoded and judged before any
        // is appended, so a refusal appends nothing.
        let batches = split_batches(records)?;
        if batches.is_empty() {
            return Err((
                ErrorCode::InvalidRecord,
                "no batches in the produce".to_owned(),
            ));
        }
        for (which, batch) in batches.iter().enumerate() {
            if batch.records.is_empty() {
                // An empty batch has nothing to acknowledge (review): refused,
                // so no backend can answer it with an offset it never took.
                return Err((
                    ErrorCode::InvalidRecord,
                    format!("batch {which} carries no records"),
                ));
            }
            if let Some(index) = batch.records.iter().position(|r| !r.headers.is_empty()) {
                // Refused rather than dropped: a record's headers have
                // nowhere to go in the native log today, and losing them
                // silently is not a translation.
                return Err((
                    ErrorCode::InvalidRecord,
                    format!(
                        "batch {which} record {index} carries {} header(s), and the native log has \
                         nowhere to keep them (#225): headers are refused rather than dropped",
                        batch.records[index].headers.len()
                    ),
                ));
            }
        }
        // An idempotent set's identity, judged whole before the bridge sees
        // it (#457): one producer, contiguous sequences, or a refusal that
        // appends nothing.
        let sequenced = sequenced_identity(&batches)?;
        let bridge = Arc::clone(&self.bridge);
        let topic = topic.to_owned();
        // The bridge is synchronous and a real one fsyncs: off the runtime's
        // threads, the way the broker's own session loop does it. The SET is
        // one append (review) — whole or not at all — and the wait for it
        // is bounded by the client's `timeout_ms` under the gateway's cap:
        // the append cannot be cancelled, so past the bound the client is
        // told it timed out while the append runs on — and an idempotent
        // producer's retry of it then appends nothing. Judged BEFORE a place
        // in line is taken (review): a ticket taken and never served would
        // hold every later set of the producer forever.
        if tokio::time::Instant::now() >= deadline {
            return Err((
                ErrorCode::RequestTimedOut,
                "the request's deadline passed before this partition was reached".to_owned(),
            ));
        }
        // The place in line, taken now — in this session's request order —
        // and waited for on the pool (review). The ticket is taken INSIDE
        // the map's critical section: between finding the entry and taking
        // a ticket, a finished append could find the entry idle and remove
        // it, and a second entry for the same producer would be a second
        // queue running beside the first.
        let ordered = sequenced.map(|sequenced| {
            let key: ProducerKey = (
                topic.clone(),
                sequenced.producer_id,
                sequenced.producer_epoch,
            );
            let mut order = self
                .producer_order
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let entry = Arc::clone(order.entry(key.clone()).or_default());
            let ticket = entry.enter();
            drop(order);
            (key, entry, ticket)
        });
        let order = Arc::clone(&self.producer_order);
        let append = self.jobs.blocking(move || {
            let turn = ordered
                .as_ref()
                .map(|(_, turnstile, ticket)| turnstile.wait_turn(*ticket));
            let outcome = bridge.produce(&topic, &batches, sequenced);
            drop(turn);
            if let Some((key, turnstile, _)) = ordered {
                // Under the map lock, so a set arriving now either finds the
                // entry and takes a ticket (and it stays) or finds none.
                let mut order = order
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if turnstile.idle() {
                    order.remove(&key);
                }
            }
            outcome
        });
        match tokio::time::timeout_at(deadline, append).await {
            Ok(Ok(Ok(appended))) => Ok(appended),
            Ok(Ok(Err(code))) => {
                Err((code, format!("the bridge refused the append with {code:?}")))
            }
            Ok(Err(join)) => Err((
                ErrorCode::RequestTimedOut,
                format!("produce task failed: {join}"),
            )),
            Err(_) => Err((
                ErrorCode::RequestTimedOut,
                "the append did not finish by the request's deadline; it runs on, and a retry may \
                     duplicate what it appends (#225 single-writer limitation)"
                    .to_owned(),
            )),
        }
    }

    async fn fetch(&self, request: FetchRequest) -> Vec<FetchTopicResponse> {
        // Read committed is served as read uncommitted, honestly: with no
        // transactions anywhere the stable offset IS the high watermark, so
        // the two isolation levels see the same records.
        let wait = Duration::from_millis(request.max_wait_ms.max(0) as u64)
            .min(self.config.max_fetch_wait);
        let deadline = tokio::time::Instant::now() + wait;
        // Every bridge call of this request is awaited to the configured
        // maximum at most (review), whatever the client asked: a bridge
        // stuck behind a slow fsync answers `REQUEST_TIMED_OUT` at the
        // ceiling instead of holding the response open.
        let ceiling = tokio::time::Instant::now() + self.config.max_fetch_wait;
        let min_bytes = usize::try_from(request.min_bytes.max(0)).unwrap_or(usize::MAX);
        loop {
            let mut topics = Vec::with_capacity(request.topics.len());
            // ONE budget for the whole response (review), not one per
            // partition: the first partition with data keeps its
            // at-least-one-batch guarantee, and once the budget is spent the
            // rest report their watermarks without asking for records.
            let mut remaining = usize::try_from(request.max_bytes.max(1)).unwrap_or(usize::MAX);
            let mut total = 0_usize;
            let mut any_error = false;
            for topic in &request.topics {
                let mut partitions = Vec::with_capacity(topic.partitions.len());
                for partition in &topic.partitions {
                    let outcome = if partition.index != 0 {
                        Err(ErrorCode::UnknownTopicOrPartition)
                    } else if remaining == 0 {
                        // No budget left: the watermarks without records —
                        // but an offset outside the log is still refused
                        // (review), not masked as an empty success.
                        let bridge = Arc::clone(&self.bridge);
                        let name = topic.name.clone();
                        let offset = partition.fetch_offset;
                        until(
                            ceiling,
                            self.jobs.blocking(move || {
                                let (log_start_offset, high_watermark) = bridge.bounds(&name)?;
                                if offset < log_start_offset || offset > high_watermark {
                                    return Err(ErrorCode::OffsetOutOfRange);
                                }
                                Ok(Fetched {
                                    records: Vec::new(),
                                    high_watermark,
                                    log_start_offset,
                                })
                            }),
                        )
                        .await
                    } else {
                        let bridge = Arc::clone(&self.bridge);
                        let name = topic.name.clone();
                        let offset = partition.fetch_offset;
                        let budget = usize::try_from(partition.max_bytes.max(1))
                            .unwrap_or(usize::MAX)
                            .min(remaining);
                        until(
                            ceiling,
                            self.jobs
                                .blocking(move || bridge.fetch(&name, offset, budget)),
                        )
                        .await
                    };
                    partitions.push(match outcome {
                        Ok(Fetched {
                            records,
                            high_watermark,
                            log_start_offset,
                        }) => {
                            total += records.len();
                            remaining = remaining.saturating_sub(records.len());
                            FetchPartitionResponse {
                                index: partition.index,
                                error: ErrorCode::None,
                                high_watermark,
                                log_start_offset,
                                records,
                            }
                        }
                        Err(error) => {
                            any_error = true; // an error is an answer; do not wait on it
                            FetchPartitionResponse {
                                index: partition.index,
                                error,
                                high_watermark: -1,
                                log_start_offset: -1,
                                records: Vec::new(),
                            }
                        }
                    });
                }
                topics.push(FetchTopicResponse {
                    name: topic.name.clone(),
                    partitions,
                });
            }
            // The long poll, emulated (#225 surface map): the broker answers
            // "nothing yet" at once, so waiting for `max_wait_ms` happens
            // here, by asking again until `min_bytes` is met (review) — zero
            // returns at once — or the wait is up, when whatever is there is
            // the answer.
            if any_error || total >= min_bytes || tokio::time::Instant::now() >= deadline {
                return topics;
            }
            tokio::time::sleep(
                self.config
                    .fetch_poll_interval
                    .min(deadline - tokio::time::Instant::now()),
            )
            .await;
        }
    }

    async fn list_offsets(&self, request: ListOffsetsRequest) -> Vec<ListOffsetsTopicResponse> {
        let mut topics = Vec::with_capacity(request.topics.len());
        for topic in request.topics {
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                let (error, offset) = if partition.index != 0 {
                    (ErrorCode::UnknownTopicOrPartition, -1)
                } else if partition.timestamp == TIMESTAMP_LATEST
                    || partition.timestamp == TIMESTAMP_EARLIEST
                {
                    // LATEST is the watermark, EARLIEST the retained floor:
                    // both are one snapshot of the bridge's bounds, and
                    // `-o beginning` on a stock client is the latter.
                    match self.bounds(&topic.name).await {
                        Ok((log_start, high_watermark)) => (
                            ErrorCode::None,
                            if partition.timestamp == TIMESTAMP_LATEST {
                                high_watermark
                            } else {
                                log_start
                            },
                        ),
                        Err(error) => (error, -1),
                    }
                } else {
                    // By-timestamp has no index behind it: refused by name,
                    // not answered with a scan.
                    tracing::warn!(
                        topic = %topic.name,
                        timestamp = partition.timestamp,
                        "kafka ListOffsets refused: only LATEST (-1) and EARLIEST (-2) are served in phase 1 (#225)"
                    );
                    (ErrorCode::UnsupportedVersion, -1)
                };
                partitions.push(ListOffsetsPartitionResponse {
                    index: partition.index,
                    error,
                    // A boundary lookup has no timestamp of its own (review):
                    // Kafka answers -1, unknown, never the request's sentinel.
                    timestamp: if error == ErrorCode::None {
                        TIMESTAMP_UNKNOWN
                    } else {
                        partition.timestamp
                    },
                    offset,
                });
            }
            topics.push(ListOffsetsTopicResponse {
                name: topic.name,
                partitions,
            });
        }
        topics
    }
}

/// A decoded body must be the WHOLE body (review): trailing bytes are a
/// schema this gateway does not know — an extension, or a misframed request
/// — and accepting them silently would serve a request it did not read.
fn consumed<T>(
    decoded: Result<T, crate::wire::WireError>,
    d: &Decoder<'_>,
    api: &'static str,
) -> Result<T, crate::wire::WireError> {
    let request = decoded?;
    d.expect_consumed(api)?;
    Ok(request)
}

/// Every batch in a RECORDS field, each decoded and judged, in order.
/// A produce set's producer identity (#457): `None` when no batch names a
/// producer (the shared, non-idempotent path), the shared id, epoch and
/// first sequence when every batch names the same producer and their
/// sequences run contiguously across the set. Anything between is refused
/// with the code a client acts on: a set is idempotent whole or not at all.
fn sequenced_identity(batches: &[RecordBatch]) -> Result<Option<Sequenced>, (ErrorCode, String)> {
    let first = &batches[0];
    if first.producer_id < 0 {
        if first.producer_id != -1 {
            return Err((
                ErrorCode::InvalidRecord,
                format!(
                    "batch 0 names producer id {}, which is neither -1 (no producer) nor an id \
                     InitProducerId minted",
                    first.producer_id
                ),
            ));
        }
        if let Some(which) = batches.iter().position(|batch| batch.producer_id != -1) {
            return Err((
                ErrorCode::InvalidRecord,
                format!(
                    "batch {which} names producer {} while batch 0 names none: a set is idempotent \
                     whole or not at all",
                    batches[which].producer_id
                ),
            ));
        }
        return Ok(None);
    }
    if first.producer_epoch < 0 {
        return Err((
            ErrorCode::InvalidProducerEpoch,
            format!(
                "batch 0 names producer {} with epoch {}: an idempotent batch's epoch is 0 or more",
                first.producer_id, first.producer_epoch
            ),
        ));
    }
    if first.base_sequence < 0 {
        return Err((
            ErrorCode::OutOfOrderSequenceNumber,
            format!(
                "batch 0 names producer {} with base sequence {}: sequences start at 0",
                first.producer_id, first.base_sequence
            ),
        ));
    }
    let mut expected = i64::from(first.base_sequence);
    for (which, batch) in batches.iter().enumerate() {
        if (batch.producer_id, batch.producer_epoch) != (first.producer_id, first.producer_epoch) {
            return Err((
                ErrorCode::InvalidRecord,
                format!(
                    "batch {which} names producer {}/{} while batch 0 names {}/{}: one set, one \
                     producer",
                    batch.producer_id,
                    batch.producer_epoch,
                    first.producer_id,
                    first.producer_epoch
                ),
            ));
        }
        if i64::from(batch.base_sequence) != expected {
            return Err((
                ErrorCode::OutOfOrderSequenceNumber,
                format!(
                    "batch {which} starts at sequence {} where {expected} was expected: sequences \
                     run contiguously across a set",
                    batch.base_sequence
                ),
            ));
        }
        expected += batch.records.len() as i64;
    }
    if expected > i64::from(i32::MAX) {
        // Kafka's sequences wrap at i32::MAX; the native log's do not. A
        // producer that far into one session is refused rather than
        // wrapped into a sequence the log would read as a gap.
        return Err((
            ErrorCode::InvalidRecord,
            format!(
                "the set's sequences would pass {}: this gateway does not wrap a producer's \
                 sequences (#457)",
                i32::MAX
            ),
        ));
    }
    Ok(Some(Sequenced {
        producer_id: first.producer_id,
        producer_epoch: first.producer_epoch,
        first_sequence: first.base_sequence,
    }))
}

/// An idempotent producer id (#457): microseconds since the epoch, bumped
/// past the previous mint when two are minted in one instant — unique in
/// this process, and across its restarts on any sane clock — with this
/// gateway's node id in the low byte (review), so the leaders a range has
/// over its life mint apart even in the same microsecond: the identity a
/// batch carries lives in the range's replicated log, which every one of
/// them appends to in turn. The id is a byte by type, never by truncation.
fn mint_producer_id(node_id: u8) -> i64 {
    static LAST_MICROS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX >> 9))
        .unwrap_or(1);
    let mut previous = LAST_MICROS.load(std::sync::atomic::Ordering::SeqCst);
    loop {
        let candidate = now.max(previous.saturating_add(1));
        match LAST_MICROS.compare_exchange(
            previous,
            candidate,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
        ) {
            Ok(_) => return (candidate << 8) | i64::from(node_id),
            Err(seen) => previous = seen,
        }
    }
}

fn split_batches(mut bytes: &[u8]) -> Result<Vec<RecordBatch>, (ErrorCode, String)> {
    let mut batches = Vec::new();
    while !bytes.is_empty() {
        if bytes.len() < 12 {
            return Err((
                ErrorCode::InvalidRecord,
                format!("{} trailing byte(s) after the last batch", bytes.len()),
            ));
        }
        let declared = i32::from_be_bytes(bytes[8..12].try_into().expect("four bytes"));
        let len = usize::try_from(declared)
            .ok()
            .and_then(|n| n.checked_add(12))
            .filter(|n| *n <= bytes.len())
            .ok_or_else(|| {
                (
                    ErrorCode::InvalidRecord,
                    format!(
                        "batch {} declares {declared} byte(s) but {} remain",
                        batches.len(),
                        bytes.len() - 12
                    ),
                )
            })?;
        let batch = RecordBatch::decode(&bytes[..len]).map_err(|error| {
            let code = match &error {
                BatchError::Compressed { .. } => ErrorCode::UnsupportedCompressionType,
                BatchError::Transactional | BatchError::Control => {
                    ErrorCode::UnsupportedForMessageFormat
                }
                BatchError::CrcMismatch { .. } => ErrorCode::CorruptMessage,
                BatchError::UnsupportedMagic { .. } => ErrorCode::UnsupportedForMessageFormat,
                _ => ErrorCode::InvalidRecord,
            };
            (code, format!("batch {}: {error}", batches.len()))
        })?;
        batches.push(batch);
        bytes = &bytes[len..];
    }
    Ok(batches)
}

/// The turn is over; an entry nobody else holds is dropped, so the map
/// names the groups committing now, not every group ever named.
fn commit_turn_done(
    orders: &std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    group: &str,
    order: Arc<tokio::sync::Mutex<()>>,
) {
    let mut orders = orders
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // Ours and the map's: nobody else is waiting or holding.
    if Arc::strong_count(&order) == 2 {
        orders.remove(group);
    }
}

/// Whether a topic name a store returned can go on the wire at all (review):
/// a name over the STRING bound names nothing a client could ask for, so the
/// row is a store fault — skipped, and said at error — rather than the
/// encoder's bound ending the session.
fn wire_carries_topic(group: &str, topic: &str) -> bool {
    if topic.len() > crate::wire::MAX_STRING_BYTES {
        tracing::error!(
            group,
            bytes = topic.len(),
            cap = crate::wire::MAX_STRING_BYTES,
            "kafka OffsetFetch: the store returned a topic name the wire cannot carry; the row \
             is not answered — a store must hold the commit path's line"
        );
        return false;
    }
    true
}

/// Metadata a store returned, as the wire can carry it (review): the
/// gateway's own commits never store more than `MAX_OFFSET_METADATA_BYTES`,
/// and a store holds the same line — so metadata over it is a store fault,
/// not a position to hide. The offset is kept, the metadata is dropped and
/// the fault is said, rather than the encoder's string bound ending the
/// session.
fn carried_metadata(
    group: &str,
    topic: &str,
    partition: i32,
    metadata: Option<String>,
) -> Option<String> {
    match metadata {
        Some(m) if m.len() > MAX_OFFSET_METADATA_BYTES => {
            tracing::error!(
                group,
                topic,
                partition,
                bytes = m.len(),
                cap = MAX_OFFSET_METADATA_BYTES,
                "kafka OffsetFetch: the store returned metadata over the cap; the offset is \
                 answered without it — a store must hold the commit path's line"
            );
            None
        }
        other => other,
    }
}

/// Every partition an OffsetFetch asked for, answered with one code and no
/// offset.
fn every_asked(asked: Vec<(String, Vec<i32>)>, error: ErrorCode) -> Vec<OffsetFetchTopicResponse> {
    asked
        .into_iter()
        .map(|(name, parts)| OffsetFetchTopicResponse {
            name,
            partitions: parts
                .into_iter()
                .map(|partition| OffsetFetchPartitionResponse {
                    partition,
                    offset: -1,
                    metadata: None,
                    error,
                })
                .collect(),
        })
        .collect()
}

/// Every partition of an OffsetCommit answered with one code.
fn every_partition(
    request: &OffsetCommitRequest,
    error: ErrorCode,
) -> Vec<OffsetCommitTopicResponse> {
    request
        .topics
        .iter()
        .map(|topic| OffsetCommitTopicResponse {
            name: topic.name.clone(),
            partitions: topic
                .partitions
                .iter()
                .map(|p| (p.partition, error))
                .collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::MemoryBridge;
    use crate::records::{crc32c, Record};
    use std::net::SocketAddr;

    async fn start(bridge: Arc<dyn Bridge>) -> (SocketAddr, tokio::sync::watch::Sender<bool>) {
        start_with(bridge, |_| {}).await
    }

    async fn start_with(
        bridge: Arc<dyn Bridge>,
        tune: impl FnOnce(&mut GatewayConfig),
    ) -> (SocketAddr, tokio::sync::watch::Sender<bool>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::watch::channel(false);
        let mut config = GatewayConfig {
            advertised_port: addr.port() as i32,
            max_fetch_wait: Duration::from_millis(500),
            fetch_poll_interval: Duration::from_millis(10),
            ..GatewayConfig::default()
        };
        tune(&mut config);
        let gateway = Gateway::new(bridge, config);
        tokio::spawn(gateway.serve(listener, rx));
        (addr, tx)
    }

    /// A bridge whose FIRST append takes `pause` — a backend behind a slow
    /// quorum, once — over memory.
    struct SlowProduce {
        inner: MemoryBridge,
        pause: Duration,
        slept: std::sync::atomic::AtomicBool,
    }
    impl Bridge for SlowProduce {
        fn topics(&self) -> Vec<String> {
            self.inner.topics()
        }
        fn produce(
            &self,
            topic: &str,
            batches: &[RecordBatch],
            sequenced: Option<Sequenced>,
        ) -> Result<crate::bridge::Appended, ErrorCode> {
            if !self.slept.swap(true, std::sync::atomic::Ordering::SeqCst) {
                std::thread::sleep(self.pause);
            }
            self.inner.produce(topic, batches, sequenced)
        }
        fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode> {
            self.inner.fetch(topic, offset, max_bytes)
        }
        fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
            self.inner.bounds(topic)
        }
    }

    /// A bridge that delegates to memory after a pause — a backend behind
    /// a slow fsync — or answers `bounds` with a code of its own.
    struct Behind {
        inner: MemoryBridge,
        pause: Duration,
        bounds: Option<ErrorCode>,
    }
    impl Bridge for Behind {
        fn topics(&self) -> Vec<String> {
            self.inner.topics()
        }
        fn produce(
            &self,
            topic: &str,
            batches: &[RecordBatch],
            sequenced: Option<Sequenced>,
        ) -> Result<crate::bridge::Appended, ErrorCode> {
            self.inner.produce(topic, batches, sequenced)
        }
        fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode> {
            std::thread::sleep(self.pause);
            self.inner.fetch(topic, offset, max_bytes)
        }
        fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
            std::thread::sleep(self.pause);
            match self.bounds {
                Some(code) => Err(code),
                None => self.inner.bounds(topic),
            }
        }
    }

    /// A body is read as it arrives and waited for only so long (review):
    /// a peer that announces a frame and stops sending is closed, not held
    /// with the announced length allocated.
    #[tokio::test]
    async fn a_frame_announced_and_not_sent_is_closed_at_the_read_timeout() {
        let (addr, _stop) = start_with(Arc::new(MemoryBridge::with_topics(["events"])), |c| {
            c.frame_read_timeout = Duration::from_millis(100)
        })
        .await;
        let mut socket = TcpStream::connect(addr).await.unwrap();
        socket
            .write_all(&(16 * 1024 * 1024_i32).to_be_bytes())
            .await
            .unwrap();
        socket.write_all(&[0_u8; 8]).await.unwrap(); // and then nothing
        let started = std::time::Instant::now();
        let mut rest = Vec::new();
        let read =
            tokio::time::timeout(Duration::from_secs(3), socket.read_to_end(&mut rest)).await;
        assert!(matches!(read, Ok(Ok(0))), "closed by the gateway: {read:?}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "at the timeout, not later"
        );
    }

    /// Sessions over the cap are closed at once (review), and a slot freed
    /// is a slot served.
    #[tokio::test]
    async fn sessions_over_the_cap_are_closed_and_a_freed_slot_is_served() {
        let (addr, _stop) = start_with(Arc::new(MemoryBridge::with_topics(["events"])), |c| {
            c.max_sessions = 1
        })
        .await;
        let mut first = TcpStream::connect(addr).await.unwrap();
        assert!(exchange(&mut first, &request(18, 0, 1, &[]))
            .await
            .is_some());
        let mut second = TcpStream::connect(addr).await.unwrap();
        assert!(
            exchange(&mut second, &request(18, 0, 2, &[]))
                .await
                .is_none(),
            "the second session is refused while the first holds the slot"
        );
        drop(first);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut third = TcpStream::connect(addr).await.unwrap();
        assert!(
            exchange(&mut third, &request(18, 0, 3, &[]))
                .await
                .is_some(),
            "the slot the first session freed serves the third"
        );
    }

    /// Shutdown drains (review): a request in flight is answered, an idle
    /// session is closed, and `serve` returns only after both.
    #[tokio::test]
    async fn shutdown_answers_the_request_in_flight_closes_idle_sessions_and_then_returns() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, rx) = tokio::sync::watch::channel(false);
        let gateway = Gateway::new(
            Arc::new(MemoryBridge::with_topics(["events"])),
            GatewayConfig {
                advertised_port: addr.port() as i32,
                max_fetch_wait: Duration::from_secs(2),
                fetch_poll_interval: Duration::from_millis(10),
                ..GatewayConfig::default()
            },
        );
        let serving = tokio::spawn(gateway.serve(listener, rx));

        let mut idle = TcpStream::connect(addr).await.unwrap();
        assert!(exchange(&mut idle, &request(18, 0, 1, &[])).await.is_some());
        let mut busy = TcpStream::connect(addr).await.unwrap();
        // A long poll at the watermark: in flight when the signal comes.
        busy.write_all(&request(1, 4, 2, &fetch_body(4, "events", 0, 0, 600)))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        stop.send(true).unwrap();

        let started = std::time::Instant::now();
        let len = busy
            .read_i32()
            .await
            .expect("the in-flight fetch is answered");
        let mut reply = vec![0_u8; len as usize];
        busy.read_exact(&mut reply).await.unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(1_000),
            "answered when its own poll ends, not held past it: {:?}",
            started.elapsed()
        );
        let mut rest = Vec::new();
        assert_eq!(
            idle.read_to_end(&mut rest).await.unwrap(),
            0,
            "the idle session is closed"
        );
        tokio::time::timeout(Duration::from_secs(3), serving)
            .await
            .expect("serve returns once the sessions are gone")
            .unwrap()
            .unwrap();
    }

    /// ListOffsets EARLIEST is the retained floor and LATEST the watermark
    /// (review): what a stock consumer's `-o beginning` and `-o end` ask.
    #[tokio::test]
    async fn list_offsets_serves_the_retained_floor_as_earliest_and_the_watermark_as_latest() {
        let memory = Arc::new(MemoryBridge::with_topics(["events"]));
        let bridge: Arc<dyn Bridge> = memory.clone();
        let (addr, _stop) = start(bridge).await;
        let three = RecordBatch::decode(&batch_bytes(&["a", "b", "c"], false)).unwrap();
        memory.produce("events", &[three], None).unwrap();
        let (floor, watermark) = memory.bounds("events").unwrap();
        for (timestamp, expected, what) in [
            (TIMESTAMP_EARLIEST, floor, "EARLIEST is the retained floor"),
            (TIMESTAMP_LATEST, watermark, "LATEST is the watermark"),
        ] {
            let mut body = Encoder::new();
            body.i32(-1); // replica_id
            body.array_len(1);
            body.string("events");
            body.array_len(1);
            body.i32(0); // partition
            body.i64(timestamp);
            let reply = call(addr, 2, 1, 9, body.as_slice()).await.unwrap();
            let mut d = Decoder::new(&reply);
            assert_eq!(d.array_len("topics").unwrap(), Some(1));
            assert_eq!(d.string("name").unwrap(), "events");
            assert_eq!(d.array_len("partitions").unwrap(), Some(1));
            assert_eq!(d.i32("partition").unwrap(), 0);
            assert_eq!(d.i16("error").unwrap(), 0, "{what}");
            assert_eq!(
                d.i64("timestamp").unwrap(),
                TIMESTAMP_UNKNOWN,
                "a boundary lookup answers an unknown timestamp, not its own sentinel"
            );
            assert_eq!(d.i64("offset").unwrap(), expected, "{what}");
        }
    }

    /// A frame still arriving when shutdown comes is dropped, not answered
    /// (review), and the drain does not wait for its tail.
    #[tokio::test]
    async fn a_frame_arriving_across_shutdown_is_dropped_and_the_drain_does_not_wait_for_it() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, rx) = tokio::sync::watch::channel(false);
        let gateway = Gateway::new(
            Arc::new(MemoryBridge::with_topics(["events"])),
            GatewayConfig {
                advertised_port: addr.port() as i32,
                frame_read_timeout: Duration::from_secs(30),
                ..GatewayConfig::default()
            },
        );
        let serving = tokio::spawn(gateway.serve(listener, rx));
        let mut slow = TcpStream::connect(addr).await.unwrap();
        let whole = request(18, 0, 7, &[]);
        slow.write_all(&whole[..6]).await.unwrap(); // the length and a byte or two
        tokio::time::sleep(Duration::from_millis(50)).await;
        stop.send(true).unwrap();
        let started = std::time::Instant::now();
        tokio::time::timeout(Duration::from_secs(3), serving)
            .await
            .expect("serve returns without waiting out the frame read timeout")
            .unwrap()
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        // The rest of the frame finds a closed connection, not an answer.
        let _ = slow.write_all(&whole[6..]).await;
        let mut rest = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(2), slow.read_to_end(&mut rest)).await;
        assert!(
            matches!(read, Ok(Ok(0)) | Ok(Err(_))),
            "closed, not answered: {read:?}"
        );
    }

    /// A bridge stuck past the fetch ceiling answers `REQUEST_TIMED_OUT` at
    /// the ceiling (review), whatever `max_wait_ms` asked.
    #[tokio::test]
    async fn a_bridge_stuck_past_the_ceiling_times_the_fetch_out_at_the_ceiling() {
        let (addr, _stop) = start(Arc::new(Behind {
            inner: MemoryBridge::with_topics(["events"]),
            pause: Duration::from_millis(1_200),
            bounds: None,
        }))
        .await;
        let started = std::time::Instant::now();
        let reply = call(addr, 1, 4, 1, &fetch_body(4, "events", 0, 0, 0))
            .await
            .unwrap();
        let (error, _, _) = read_fetch(&reply, 4);
        assert_eq!(error, ErrorCode::RequestTimedOut.as_i16());
        assert!(
            started.elapsed() < Duration::from_millis(1_000),
            "answered at the 500 ms ceiling, not after the 1.2 s bridge: {:?}",
            started.elapsed()
        );
    }

    /// A topic the bridge knows but cannot vouch for keeps the bridge's code
    /// in Metadata (review): fenced is not absent.
    #[tokio::test]
    async fn metadata_keeps_a_transient_bridge_error_instead_of_calling_the_topic_unknown() {
        let (addr, _stop) = start(Arc::new(Behind {
            inner: MemoryBridge::with_topics(["events"]),
            pause: Duration::ZERO,
            bounds: Some(ErrorCode::NotLeaderOrFollower),
        }))
        .await;
        let mut body = Encoder::new();
        body.array_len(1);
        body.string("events");
        body.bool(true);
        let reply = call(addr, 3, 4, 2, body.as_slice()).await.unwrap();
        let mut d = Decoder::new(&reply);
        d.i32("throttle").unwrap();
        d.array_len("brokers").unwrap();
        d.i32("node").unwrap();
        d.string("host").unwrap();
        d.i32("port").unwrap();
        d.nullable_string("rack").unwrap();
        d.nullable_string("cluster").unwrap();
        d.i32("controller").unwrap();
        d.array_len("topics").unwrap();
        assert_eq!(
            d.i16("error").unwrap(),
            ErrorCode::NotLeaderOrFollower.as_i16()
        );
        assert_eq!(d.string("name").unwrap(), "events");
        d.bool("internal").unwrap();
        assert_eq!(d.array_len("partitions").unwrap(), Some(0));
    }

    fn request(key: i16, version: i16, correlation: i32, body: &[u8]) -> Vec<u8> {
        let mut e = Encoder::new();
        e.i16(key);
        e.i16(version);
        e.i32(correlation);
        e.nullable_string(Some("test-client"));
        e.raw(body);
        frame(e.as_slice())
    }

    /// Send one request; `None` when the gateway closed the connection
    /// instead of answering.
    async fn exchange(socket: &mut TcpStream, bytes: &[u8]) -> Option<Vec<u8>> {
        socket.write_all(bytes).await.unwrap();
        let len = socket.read_i32().await.ok()?;
        let mut body = vec![0_u8; len as usize];
        socket.read_exact(&mut body).await.ok()?;
        Some(body)
    }

    async fn call(
        addr: SocketAddr,
        key: i16,
        version: i16,
        correlation: i32,
        body: &[u8],
    ) -> Option<Vec<u8>> {
        let mut socket = TcpStream::connect(addr).await.unwrap();
        let reply = exchange(&mut socket, &request(key, version, correlation, body)).await?;
        let mut d = Decoder::new(&reply);
        assert_eq!(d.i32("correlation").unwrap(), correlation);
        Some(reply[4..].to_vec())
    }

    fn batch_bytes(values: &[&str], headers: bool) -> Vec<u8> {
        let records: Vec<Record> = values
            .iter()
            .enumerate()
            .map(|(i, v)| Record {
                offset: i as i64,
                timestamp_millis: 1_700_000_000_000 + i as i64,
                key: None,
                value: Some(v.as_bytes().to_vec()),
                headers: if headers {
                    vec![("h".to_owned(), Some(b"v".to_vec()))]
                } else {
                    Vec::new()
                },
            })
            .collect();
        RecordBatch::encode(0, -1, -1, -1, &records)
    }

    /// A batch as an idempotent producer sends it: its id, epoch and base
    /// sequence in the batch header.
    fn sequenced_batch_bytes(
        values: &[&str],
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
    ) -> Vec<u8> {
        let records: Vec<Record> = values
            .iter()
            .enumerate()
            .map(|(i, v)| Record {
                offset: i as i64,
                timestamp_millis: 1_700_000_000_000 + i as i64,
                key: None,
                value: Some(v.as_bytes().to_vec()),
                headers: Vec::new(),
            })
            .collect();
        RecordBatch::encode(0, producer_id, producer_epoch, base_sequence, &records)
    }
    fn init_producer_id_body(transactional: Option<&str>) -> Vec<u8> {
        let mut e = Encoder::new();
        e.nullable_string(transactional);
        e.i32(60_000);
        e.into_vec()
    }
    /// (error code, producer id, producer epoch).
    fn read_init_producer_id(reply: &[u8]) -> (i16, i64, i16) {
        let mut d = Decoder::new(reply);
        d.i32("throttle").unwrap();
        let error = d.i16("error").unwrap();
        let producer_id = d.i64("producer_id").unwrap();
        let producer_epoch = d.i16("producer_epoch").unwrap();
        assert!(d.is_empty(), "trailing bytes");
        (error, producer_id, producer_epoch)
    }
    fn produce_body(
        topic: &str,
        partition: i32,
        acks: i16,
        transactional: Option<&str>,
        records: &[u8],
    ) -> Vec<u8> {
        let mut e = Encoder::new();
        e.nullable_string(transactional);
        e.i16(acks);
        e.i32(1_500);
        e.array_len(1);
        e.string(topic);
        e.array_len(1);
        e.i32(partition);
        e.nullable_bytes(Some(records));
        e.into_vec()
    }

    /// (error code, base offset, message) of the first partition.
    fn read_produce(reply: &[u8], version: i16) -> (i16, i64, Option<String>) {
        let mut d = Decoder::new(reply);
        d.array_len("topics").unwrap();
        d.string("topic").unwrap();
        d.array_len("partitions").unwrap();
        d.i32("index").unwrap();
        let error = d.i16("error").unwrap();
        let base = d.i64("base").unwrap();
        if version >= 2 {
            d.i64("append_time").unwrap();
        }
        if version >= 5 {
            d.i64("log_start").unwrap();
        }
        let message = if version >= 8 {
            d.array_len("record_errors").unwrap();
            d.nullable_string("message").unwrap().map(str::to_owned)
        } else {
            None
        };
        (error, base, message)
    }

    fn fetch_body(
        version: i16,
        topic: &str,
        partition: i32,
        offset: i64,
        max_wait_ms: i32,
    ) -> Vec<u8> {
        fetch_body_sized(
            version,
            &[(topic, partition, offset)],
            max_wait_ms,
            1,
            1 << 20,
        )
    }

    fn fetch_body_sized(
        version: i16,
        partitions: &[(&str, i32, i64)],
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
    ) -> Vec<u8> {
        let mut e = Encoder::new();
        e.i32(-1);
        e.i32(max_wait_ms);
        e.i32(min_bytes);
        e.i32(max_bytes);
        e.i8(1); // read_committed: served as the watermark, honestly
        if version >= 7 {
            e.i32(0);
            e.i32(-1);
        }
        e.array_len(partitions.len());
        for (topic, partition, offset) in partitions {
            e.string(topic);
            e.array_len(1);
            e.i32(*partition);
            if version >= 9 {
                e.i32(-1);
            }
            e.i64(*offset);
            if version >= 5 {
                e.i64(0);
            }
            e.i32(1 << 20);
        }
        if version >= 7 {
            e.array_len(0);
        }
        if version >= 11 {
            e.string("");
        }
        e.into_vec()
    }

    /// (error, high watermark, records bytes) of the first partition.
    fn read_fetch(reply: &[u8], version: i16) -> (i16, i64, Vec<u8>) {
        read_fetch_all(reply, version).remove(0)
    }

    /// (error, high watermark, records bytes) of every partition, in order.
    fn read_fetch_all(reply: &[u8], version: i16) -> Vec<(i16, i64, Vec<u8>)> {
        let mut d = Decoder::new(reply);
        d.i32("throttle").unwrap();
        if version >= 7 {
            d.i16("error").unwrap();
            d.i32("session").unwrap();
        }
        let mut out = Vec::new();
        let topics = d.array_len("topics").unwrap().unwrap();
        for _ in 0..topics {
            d.string("topic").unwrap();
            let partitions = d.array_len("partitions").unwrap().unwrap();
            for _ in 0..partitions {
                d.i32("index").unwrap();
                let error = d.i16("error").unwrap();
                let hwm = d.i64("hwm").unwrap();
                d.i64("lso").unwrap();
                if version >= 5 {
                    d.i64("log_start").unwrap();
                }
                d.array_len("aborted").unwrap();
                if version >= 11 {
                    d.i32("preferred").unwrap();
                }
                let records = d.nullable_bytes("records").unwrap().unwrap_or(&[]).to_vec();
                out.push((error, hwm, records));
            }
        }
        out
    }

    fn decode_all(mut bytes: &[u8]) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        while !bytes.is_empty() {
            let len = i32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize + 12;
            out.push(RecordBatch::decode(&bytes[..len]).unwrap());
            bytes = &bytes[len..];
        }
        out
    }

    #[tokio::test]
    async fn api_versions_answers_every_version_and_names_the_served_ranges() {
        let (addr, _stop) = start(Arc::new(MemoryBridge::with_topics(["events"]))).await;
        // v3 is flexible and not served: answered anyway, in v0 form, with the
        // error and the ranges, so the client can downgrade.
        let mut refused = Encoder::new();
        refused.i16(18);
        refused.i16(3);
        refused.i32(9);
        refused.nullable_string(Some("c"));
        refused.i8(0); // header v2 tagged fields
        let mut socket = TcpStream::connect(addr).await.unwrap();
        let reply = exchange(&mut socket, &frame(refused.as_slice()))
            .await
            .unwrap();
        let mut d = Decoder::new(&reply);
        assert_eq!(d.i32("correlation").unwrap(), 9);
        assert_eq!(
            d.i16("error").unwrap(),
            ErrorCode::UnsupportedVersion.as_i16()
        );
        assert_eq!(d.array_len("keys").unwrap(), Some(13));

        // v0 on the same connection: served.
        let reply = exchange(&mut socket, &request(18, 0, 10, &[]))
            .await
            .unwrap();
        let mut d = Decoder::new(&reply);
        d.i32("correlation").unwrap();
        assert_eq!(d.i16("error").unwrap(), 0);
    }

    #[tokio::test]
    async fn metadata_names_this_gateway_as_the_leader_of_one_partition_per_topic() {
        let (addr, _stop) = start(Arc::new(MemoryBridge::with_topics(["events", "audit"]))).await;
        for version in [1_i16, 8] {
            let mut body = Encoder::new();
            body.i32(-1); // null: every topic
            if version >= 4 {
                body.bool(true);
            }
            if version >= 8 {
                body.bool(false);
                body.bool(false);
            }
            let reply = call(addr, 3, version, 1, body.as_slice()).await.unwrap();
            let mut d = Decoder::new(&reply);
            if version >= 3 {
                d.i32("throttle").unwrap();
            }
            assert_eq!(d.array_len("brokers").unwrap(), Some(1));
            assert_eq!(d.i32("node").unwrap(), 1);
            assert_eq!(d.string("host").unwrap(), "127.0.0.1");
            assert_eq!(d.i32("port").unwrap(), addr.port() as i32);
            d.nullable_string("rack").unwrap();
            if version >= 2 {
                assert_eq!(d.nullable_string("cluster").unwrap(), Some("vtop"));
            }
            assert_eq!(d.i32("controller").unwrap(), 1);
            assert_eq!(d.array_len("topics").unwrap(), Some(2));
            assert_eq!(d.i16("error").unwrap(), 0);
            assert_eq!(d.string("name").unwrap(), "audit", "sorted");
        }
        // A named unknown topic is unknown, never auto-created.
        let mut body = Encoder::new();
        body.array_len(1);
        body.string("nope");
        body.bool(true);
        let reply = call(addr, 3, 4, 2, body.as_slice()).await.unwrap();
        let mut d = Decoder::new(&reply);
        d.i32("throttle").unwrap();
        d.array_len("brokers").unwrap();
        d.i32("node").unwrap();
        d.string("host").unwrap();
        d.i32("port").unwrap();
        d.nullable_string("rack").unwrap();
        d.nullable_string("cluster").unwrap();
        d.i32("controller").unwrap();
        d.array_len("topics").unwrap();
        assert_eq!(
            d.i16("error").unwrap(),
            ErrorCode::UnknownTopicOrPartition.as_i16()
        );
        assert_eq!(d.string("name").unwrap(), "nope");
        d.bool("internal").unwrap();
        assert_eq!(d.array_len("partitions").unwrap(), Some(0));
    }

    #[tokio::test]
    async fn produce_fetch_and_list_offsets_round_trip_across_served_versions() {
        let bridge = Arc::new(MemoryBridge::with_topics(["events"]));
        let (addr, _stop) = start(bridge).await;

        let reply = call(
            addr,
            0,
            3,
            1,
            &produce_body("events", 0, -1, None, &batch_bytes(&["a", "b"], false)),
        )
        .await
        .unwrap();
        assert_eq!(read_produce(&reply, 3), (0, 0, None));
        let reply = call(
            addr,
            0,
            8,
            2,
            &produce_body("events", 0, 1, None, &batch_bytes(&["c"], false)),
        )
        .await
        .unwrap();
        assert_eq!(read_produce(&reply, 8), (0, 2, None));

        for version in [4_i16, 7, 11] {
            let reply = call(addr, 1, version, 3, &fetch_body(version, "events", 0, 0, 0))
                .await
                .unwrap();
            let (error, hwm, records) = read_fetch(&reply, version);
            assert_eq!((error, hwm), (0, 3));
            let batches = decode_all(&records);
            assert_eq!(batches.len(), 2);
            let values: Vec<&[u8]> = batches
                .iter()
                .flat_map(|b| b.records.iter())
                .map(|r| r.value.as_deref().unwrap())
                .collect();
            assert_eq!(values, vec![b"a".as_slice(), b"b", b"c"]);
            assert_eq!(batches[1].base_offset, 2);
        }

        for version in [1_i16, 5] {
            let mut body = Encoder::new();
            body.i32(-1);
            if version >= 2 {
                body.i8(0);
            }
            body.array_len(1);
            body.string("events");
            body.array_len(1);
            body.i32(0);
            if version >= 4 {
                body.i32(-1);
            }
            body.i64(TIMESTAMP_LATEST);
            let reply = call(addr, 2, version, 4, body.as_slice()).await.unwrap();
            let mut d = Decoder::new(&reply);
            if version >= 2 {
                d.i32("throttle").unwrap();
            }
            d.array_len("topics").unwrap();
            d.string("topic").unwrap();
            d.array_len("partitions").unwrap();
            d.i32("index").unwrap();
            assert_eq!(d.i16("error").unwrap(), 0);
            d.i64("timestamp").unwrap();
            assert_eq!(d.i64("offset").unwrap(), 3, "LATEST is the next offset");
        }
    }

    /// A RECORDS field holding two batches appends both (review): the
    /// decoder's stop at the first declared length is not the set's end.
    /// InitProducerId (#457) mints a distinct, increasing id per call with
    /// epoch zero, in v0 and v1; a transactional id is refused by name; the
    /// flexible v2 is outside the served range and closed at the header.
    #[tokio::test]
    async fn init_producer_id_mints_distinct_ids_and_refuses_a_transactional_one() {
        let (addr, _stop) = start(Arc::new(MemoryBridge::with_topics(["events"]))).await;
        let mut minted = Vec::new();
        for version in [0, 1] {
            let reply = call(addr, 22, version, 1, &init_producer_id_body(None))
                .await
                .unwrap();
            let (error, producer_id, producer_epoch) = read_init_producer_id(&reply);
            assert_eq!((error, producer_epoch), (0, 0));
            assert!(producer_id > 0);
            minted.push(producer_id);
        }
        assert!(minted[1] > minted[0], "distinct, increasing: {minted:?}");
        assert_eq!(
            minted[0] & 0xff,
            minted[1] & 0xff,
            "the low byte is this gateway's node id, on every mint"
        );
        assert!(
            minted[1] >> 8 > minted[0] >> 8,
            "the microseconds above it increase"
        );
        // A node id that does not fit the byte is not truncated into another
        // gateway's: it cannot mint, and says so.
        let (wide, _stop) = start_with(Arc::new(MemoryBridge::with_topics(["events"])), |c| {
            c.node_id = 257;
        })
        .await;
        let reply = call(wide, 22, 1, 4, &init_producer_id_body(None))
            .await
            .unwrap();
        assert_eq!(
            read_init_producer_id(&reply),
            (ErrorCode::UnsupportedForMessageFormat.as_i16(), -1, -1)
        );
        let reply = call(addr, 22, 1, 2, &init_producer_id_body(Some("tx-1")))
            .await
            .unwrap();
        assert_eq!(
            read_init_producer_id(&reply),
            (ErrorCode::UnsupportedForMessageFormat.as_i16(), -1, -1)
        );
        // v2 is flexible: refused at the header, and the connection closed.
        let mut refused = Encoder::new();
        refused.i16(22);
        refused.i16(2);
        refused.i32(3);
        refused.nullable_string(Some("c"));
        refused.i8(0);
        let mut socket = TcpStream::connect(addr).await.unwrap();
        assert!(exchange(&mut socket, &frame(refused.as_slice()))
            .await
            .is_none());
    }

    /// The acceptance (#457): an idempotent producer's retried batch appends
    /// once. Through the wire: the same bytes twice are one append and the
    /// same base offset; the next sequence appends; a gap, a mixed set, a
    /// non-contiguous set and a bad epoch are refused by the code a client
    /// acts on, and append nothing.
    #[tokio::test]
    async fn an_idempotent_retry_is_acknowledged_once_with_its_original_offset() {
        let (addr, _stop) = start(Arc::new(MemoryBridge::with_topics(["events"]))).await;
        let produce = |correlation: i32, set: Vec<u8>| async move {
            let reply = call(
                addr,
                0,
                8,
                correlation,
                &produce_body("events", 0, -1, None, &set),
            )
            .await
            .unwrap();
            read_produce(&reply, 8)
        };
        let watermark = || async move {
            let reply = call(addr, 1, 4, 99, &fetch_body(4, "events", 0, 0, 0))
                .await
                .unwrap();
            read_fetch(&reply, 4).1
        };
        let first = sequenced_batch_bytes(&["a", "b"], 7, 0, 0);
        assert_eq!(produce(1, first.clone()).await, (0, 0, None));
        assert_eq!(produce(2, first.clone()).await, (0, 0, None), "the retry");
        assert_eq!(watermark().await, 2, "appended once");
        assert_eq!(
            produce(3, sequenced_batch_bytes(&["c"], 7, 0, 2)).await,
            (0, 2, None)
        );
        let (error, _, message) = produce(4, sequenced_batch_bytes(&["x"], 7, 0, 5)).await;
        assert_eq!(error, ErrorCode::OutOfOrderSequenceNumber.as_i16());
        assert!(message.is_some());
        // Two batches in one set, contiguous: one identity, one append.
        let mut set = sequenced_batch_bytes(&["d"], 7, 0, 3);
        set.extend_from_slice(&sequenced_batch_bytes(&["e"], 7, 0, 4));
        assert_eq!(produce(5, set).await, (0, 3, None));
        assert_eq!(watermark().await, 5);
        // Not contiguous: refused whole.
        let mut set = sequenced_batch_bytes(&["f"], 7, 0, 5);
        set.extend_from_slice(&sequenced_batch_bytes(&["g"], 7, 0, 7));
        let (error, _, message) = produce(6, set).await;
        assert_eq!(error, ErrorCode::OutOfOrderSequenceNumber.as_i16());
        assert!(message.unwrap().contains("batch 1 starts at sequence 7"));
        // Mixed with a batch naming no producer: refused whole.
        let mut set = sequenced_batch_bytes(&["f"], 7, 0, 5);
        set.extend_from_slice(&batch_bytes(&["g"], false));
        let (error, _, message) = produce(7, set).await;
        assert_eq!(error, ErrorCode::InvalidRecord.as_i16());
        assert!(message.unwrap().contains("one set, one producer"));
        let (error, _, _) = produce(8, sequenced_batch_bytes(&["f"], 7, -1, 5)).await;
        assert_eq!(error, ErrorCode::InvalidProducerEpoch.as_i16());
        let (error, _, message) = produce(10, sequenced_batch_bytes(&["f"], -2, -1, -1)).await;
        assert_eq!(
            error,
            ErrorCode::InvalidRecord.as_i16(),
            "-2 is nobody's producer id"
        );
        assert!(message.unwrap().contains("neither -1"));
        assert_eq!(watermark().await, 5, "every refusal appended nothing");
        // A set naming no producer is the shared path, unchanged.
        assert_eq!(produce(9, batch_bytes(&["h"], false)).await, (0, 5, None));
    }

    /// A set behind a timed-out one waits for it and lands after (review):
    /// the first set's append outlives the gateway's produce cap, the client
    /// is told it timed out, and its next set arrives while the first is
    /// still appending. Its place in line was taken in request order, so it
    /// waits for the first set to land and then lands at the offset after it
    /// — never before it, where its sequence would be a gap. The wait is
    /// short of the second set's own deadline here; a client whose wait is
    /// not retries, and the retry meets the same order.
    #[tokio::test]
    async fn a_set_behind_a_timed_out_one_waits_for_it_and_lands_after() {
        let bridge = Arc::new(SlowProduce {
            inner: MemoryBridge::with_topics(["events"]),
            pause: Duration::from_millis(600),
            slept: Default::default(),
        });
        let (addr, _stop) = start_with(bridge, |c| {
            c.max_produce_wait = Duration::from_millis(400);
        })
        .await;
        let mut socket = TcpStream::connect(addr).await.unwrap();
        let first = produce_body(
            "events",
            0,
            -1,
            None,
            &sequenced_batch_bytes(&["a", "b"], 7, 0, 0),
        );
        let reply = exchange(&mut socket, &request(0, 8, 1, &first))
            .await
            .unwrap();
        assert_eq!(
            read_produce(&reply[4..], 8).0,
            ErrorCode::RequestTimedOut.as_i16(),
            "the first set outlives the cap"
        );
        let started = tokio::time::Instant::now();
        let second = produce_body(
            "events",
            0,
            -1,
            None,
            &sequenced_batch_bytes(&["c"], 7, 0, 2),
        );
        let reply = exchange(&mut socket, &request(0, 8, 2, &second))
            .await
            .unwrap();
        let (error, base, _) = read_produce(&reply[4..], 8);
        assert_eq!(
            (error, base),
            (0, 2),
            "landed after the first set, not before it"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(150),
            "it waited for the first set's append to finish"
        );
        let reply = call(addr, 1, 4, 3, &fetch_body(4, "events", 0, 0, 0))
            .await
            .unwrap();
        assert_eq!(read_fetch(&reply, 4).1, 3, "three records, in order");
    }

    // ---- the group protocol on the wire (#457 slice 2) ----
    async fn start_groups(
        bridge: Arc<dyn Bridge>,
        store: Option<Arc<dyn OffsetStore>>,
    ) -> (SocketAddr, tokio::sync::watch::Sender<bool>) {
        start_groups_tuned(bridge, store, |_| {}).await
    }
    async fn start_groups_tuned(
        bridge: Arc<dyn Bridge>,
        store: Option<Arc<dyn OffsetStore>>,
        tune: impl FnOnce(&mut GatewayConfig),
    ) -> (SocketAddr, tokio::sync::watch::Sender<bool>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::watch::channel(false);
        let mut config = GatewayConfig {
            advertised_port: addr.port() as i32,
            groups: GroupConfig {
                initial_rebalance_delay: Duration::from_millis(50),
                ..GroupConfig::default()
            },
            ..GatewayConfig::default()
        };
        tune(&mut config);
        let mut gateway = Gateway::new(bridge, config);
        if let Some(store) = store {
            gateway = gateway.with_offsets(store);
        }
        tokio::spawn(gateway.serve(listener, rx));
        (addr, tx)
    }
    fn find_coordinator_body(version: i16, key: &str, key_type: i8) -> Vec<u8> {
        let mut e = Encoder::new();
        e.string(key);
        if version >= 1 {
            e.i8(key_type);
        }
        e.into_vec()
    }
    /// (error, node id, host, port)
    fn read_find_coordinator(reply: &[u8], version: i16) -> (i16, i32, String, i32) {
        let mut d = Decoder::new(reply);
        if version >= 1 {
            d.i32("throttle").unwrap();
        }
        let error = d.i16("error").unwrap();
        if version >= 1 {
            d.nullable_string("message").unwrap();
        }
        let node = d.i32("node").unwrap();
        let host = d.string("host").unwrap().to_owned();
        let port = d.i32("port").unwrap();
        assert!(d.is_empty());
        (error, node, host, port)
    }
    fn join_body(version: i16, group: &str, member: &str, protocols: &[(&str, &[u8])]) -> Vec<u8> {
        let mut e = Encoder::new();
        e.string(group);
        e.i32(10_000);
        if version >= 1 {
            e.i32(10_000);
        }
        e.string(member);
        if version >= 5 {
            e.nullable_string(None);
        }
        e.string("consumer");
        e.array_len(protocols.len());
        for (name, metadata) in protocols {
            e.string(name);
            e.nullable_bytes(Some(metadata));
        }
        e.into_vec()
    }
    struct JoinRead {
        error: i16,
        generation: i32,
        protocol: String,
        leader: String,
        member_id: String,
        members: Vec<(String, Vec<u8>)>,
    }
    fn read_join(reply: &[u8], version: i16) -> JoinRead {
        let mut d = Decoder::new(reply);
        if version >= 2 {
            d.i32("throttle").unwrap();
        }
        let error = d.i16("error").unwrap();
        let generation = d.i32("generation").unwrap();
        let protocol = d.string("protocol").unwrap().to_owned();
        let leader = d.string("leader").unwrap().to_owned();
        let member_id = d.string("member").unwrap().to_owned();
        let count = d.array_len("members").unwrap().unwrap_or(0);
        let mut members = Vec::new();
        for _ in 0..count {
            let id = d.string("id").unwrap().to_owned();
            if version >= 5 {
                d.nullable_string("instance").unwrap();
            }
            let metadata = d
                .nullable_bytes("metadata")
                .unwrap()
                .unwrap_or_default()
                .to_vec();
            members.push((id, metadata));
        }
        assert!(d.is_empty(), "trailing bytes");
        JoinRead {
            error,
            generation,
            protocol,
            leader,
            member_id,
            members,
        }
    }
    fn sync_body(
        version: i16,
        group: &str,
        generation: i32,
        member: &str,
        assignments: &[(&str, &[u8])],
    ) -> Vec<u8> {
        let mut e = Encoder::new();
        e.string(group);
        e.i32(generation);
        e.string(member);
        if version >= 3 {
            e.nullable_string(None);
        }
        e.array_len(assignments.len());
        for (member, assignment) in assignments {
            e.string(member);
            e.nullable_bytes(Some(assignment));
        }
        e.into_vec()
    }
    fn read_sync(reply: &[u8], version: i16) -> (i16, Vec<u8>) {
        let mut d = Decoder::new(reply);
        if version >= 1 {
            d.i32("throttle").unwrap();
        }
        let error = d.i16("error").unwrap();
        let assignment = d
            .nullable_bytes("assignment")
            .unwrap()
            .unwrap_or_default()
            .to_vec();
        assert!(d.is_empty());
        (error, assignment)
    }
    fn heartbeat_body(version: i16, group: &str, generation: i32, member: &str) -> Vec<u8> {
        let mut e = Encoder::new();
        e.string(group);
        e.i32(generation);
        e.string(member);
        if version >= 3 {
            e.nullable_string(None);
        }
        e.into_vec()
    }
    fn leave_body(group: &str, member: &str) -> Vec<u8> {
        let mut e = Encoder::new();
        e.string(group);
        e.string(member);
        e.into_vec()
    }
    fn read_error_only(reply: &[u8], version: i16) -> i16 {
        let mut d = Decoder::new(reply);
        if version >= 1 {
            d.i32("throttle").unwrap();
        }
        let error = d.i16("error").unwrap();
        assert!(d.is_empty());
        error
    }
    /// A `ConsumerProtocolAssignment` as a leader's assignor writes it.
    fn consumer_assignment(topic: &str, partitions: &[i32]) -> Vec<u8> {
        let mut e = Encoder::new();
        e.i16(0);
        e.array_len(1);
        e.string(topic);
        e.array_len(partitions.len());
        for p in partitions {
            e.i32(*p);
        }
        e.nullable_bytes(None);
        e.into_vec()
    }
    #[allow(clippy::too_many_arguments)]
    fn offset_commit_body(
        version: i16,
        group: &str,
        generation: i32,
        member: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> Vec<u8> {
        let mut e = Encoder::new();
        e.string(group);
        e.i32(generation);
        e.string(member);
        if version >= 7 {
            e.nullable_string(None);
        }
        e.array_len(1);
        e.string(topic);
        e.array_len(1);
        e.i32(partition);
        e.i64(offset);
        if version >= 6 {
            e.i32(-1);
        }
        e.nullable_string(metadata);
        e.into_vec()
    }
    /// A commit of partition 0 of several topics, v7.
    fn offset_commit_many_body(group: &str, topics: &[(&str, i64)]) -> Vec<u8> {
        let mut e = Encoder::new();
        e.string(group);
        e.i32(-1);
        e.string("");
        e.nullable_string(None);
        e.array_len(topics.len());
        for (topic, offset) in topics {
            e.string(topic);
            e.array_len(1);
            e.i32(0);
            e.i64(*offset);
            e.i32(-1);
            e.nullable_string(None);
        }
        e.into_vec()
    }
    fn read_offset_commit(reply: &[u8], version: i16) -> Vec<(i32, i16)> {
        let mut d = Decoder::new(reply);
        if version >= 3 {
            d.i32("throttle").unwrap();
        }
        let topics = d.array_len("topics").unwrap().unwrap_or(0);
        let mut out = Vec::new();
        for _ in 0..topics {
            d.string("name").unwrap();
            let partitions = d.array_len("partitions").unwrap().unwrap_or(0);
            for _ in 0..partitions {
                out.push((d.i32("partition").unwrap(), d.i16("error").unwrap()));
            }
        }
        assert!(d.is_empty());
        out
    }
    fn offset_fetch_body(version: i16, group: &str, topics: Option<&[(&str, &[i32])]>) -> Vec<u8> {
        let mut e = Encoder::new();
        e.string(group);
        match topics {
            None => {
                assert!(version >= 2, "a null topic list is v2+");
                e.i32(-1);
            }
            Some(topics) => {
                e.array_len(topics.len());
                for (name, partitions) in topics {
                    e.string(name);
                    e.array_len(partitions.len());
                    for p in *partitions {
                        e.i32(*p);
                    }
                }
            }
        }
        e.into_vec()
    }
    type FetchedOffsets = Vec<(String, Vec<(i32, i64, Option<String>, i16)>)>;
    /// (group-level error, topics with (partition, offset, metadata, error))
    fn read_offset_fetch(reply: &[u8], version: i16) -> (i16, FetchedOffsets) {
        let mut d = Decoder::new(reply);
        if version >= 3 {
            d.i32("throttle").unwrap();
        }
        let count = d.array_len("topics").unwrap().unwrap_or(0);
        let mut topics = Vec::new();
        for _ in 0..count {
            let name = d.string("name").unwrap().to_owned();
            let partitions = d.array_len("partitions").unwrap().unwrap_or(0);
            let mut out = Vec::new();
            for _ in 0..partitions {
                let partition = d.i32("partition").unwrap();
                let offset = d.i64("offset").unwrap();
                if version >= 5 {
                    d.i32("epoch").unwrap();
                }
                let metadata = d.nullable_string("metadata").unwrap().map(str::to_owned);
                let error = d.i16("error").unwrap();
                out.push((partition, offset, metadata, error));
            }
            topics.push((name, out));
        }
        let error = if version >= 2 {
            d.i16("error").unwrap()
        } else {
            0
        };
        assert!(d.is_empty());
        (error, topics)
    }

    /// FindCoordinator names this gateway for every group, at every served
    /// version; a transaction key and an empty key are refused by name.
    #[tokio::test]
    async fn find_coordinator_names_this_gateway_for_every_group() {
        let (addr, _stop) =
            start_groups(Arc::new(MemoryBridge::with_topics(["events"])), None).await;
        for version in [0, 1, 2] {
            let reply = call(
                addr,
                10,
                version,
                1,
                &find_coordinator_body(version, "g", 0),
            )
            .await
            .unwrap();
            let (error, node, host, port) = read_find_coordinator(&reply, version);
            assert_eq!((error, node), (0, 1), "v{version}");
            assert_eq!((host.as_str(), port), ("127.0.0.1", i32::from(addr.port())));
        }
        let reply = call(addr, 10, 2, 2, &find_coordinator_body(2, "tx", 1))
            .await
            .unwrap();
        assert_eq!(
            read_find_coordinator(&reply, 2).0,
            ErrorCode::UnsupportedForMessageFormat.as_i16()
        );
        let reply = call(addr, 10, 1, 3, &find_coordinator_body(1, "", 0))
            .await
            .unwrap();
        assert_eq!(
            read_find_coordinator(&reply, 1).0,
            ErrorCode::InvalidGroupId.as_i16()
        );
        // v3 is flexible: outside the served range, closed at the header.
        let mut refused = Encoder::new();
        refused.i16(10);
        refused.i16(3);
        refused.i32(4);
        refused.nullable_string(Some("c"));
        refused.i8(0);
        let mut socket = TcpStream::connect(addr).await.unwrap();
        assert!(exchange(&mut socket, &frame(refused.as_slice()))
            .await
            .is_none());
    }

    /// One member over the wire: a v5 join without an id is told to come back
    /// with one; the rejoin completes with the member as leader at
    /// generation 1 and its own metadata in the member list; its sync hands
    /// back the assignment it wrote; heartbeats at the generation are fine, a
    /// stale generation is illegal, a stranger is unknown; a leave ends it.
    #[tokio::test]
    async fn a_group_forms_syncs_heartbeats_and_leaves_over_the_wire() {
        let (addr, _stop) =
            start_groups(Arc::new(MemoryBridge::with_topics(["events"])), None).await;
        let mut socket = TcpStream::connect(addr).await.unwrap();
        let protocols: &[(&str, &[u8])] = &[("range", b"sub-events")];
        let reply = exchange(
            &mut socket,
            &request(11, 4, 1, &join_body(4, "g", "", protocols)),
        )
        .await
        .unwrap();
        let first = read_join(&reply[4..], 4);
        assert_eq!(first.error, ErrorCode::MemberIdRequired.as_i16());
        assert!(
            first.member_id.starts_with("test-client-"),
            "{}",
            first.member_id
        );
        let id = first.member_id;
        let reply = exchange(
            &mut socket,
            &request(11, 4, 2, &join_body(4, "g", &id, protocols)),
        )
        .await
        .unwrap();
        let joined = read_join(&reply[4..], 4);
        assert_eq!((joined.error, joined.generation), (0, 1));
        assert_eq!(
            (joined.leader.as_str(), joined.member_id.as_str()),
            (id.as_str(), id.as_str())
        );
        assert_eq!(joined.protocol, "range");
        assert_eq!(joined.members, vec![(id.clone(), b"sub-events".to_vec())]);
        let assignment = consumer_assignment("events", &[0]);
        let reply = exchange(
            &mut socket,
            &request(14, 2, 3, &sync_body(2, "g", 1, &id, &[(&id, &assignment)])),
        )
        .await
        .unwrap();
        assert_eq!(read_sync(&reply[4..], 2), (0, assignment.clone()));
        let reply = exchange(
            &mut socket,
            &request(12, 2, 4, &heartbeat_body(2, "g", 1, &id)),
        )
        .await
        .unwrap();
        assert_eq!(read_error_only(&reply[4..], 2), 0);
        let reply = exchange(
            &mut socket,
            &request(12, 0, 5, &heartbeat_body(0, "g", 0, &id)),
        )
        .await
        .unwrap();
        assert_eq!(
            read_error_only(&reply[4..], 0),
            ErrorCode::IllegalGeneration.as_i16()
        );
        let reply = exchange(
            &mut socket,
            &request(12, 1, 6, &heartbeat_body(1, "g", 1, "nobody")),
        )
        .await
        .unwrap();
        assert_eq!(
            read_error_only(&reply[4..], 1),
            ErrorCode::UnknownMemberId.as_i16()
        );
        let reply = exchange(&mut socket, &request(13, 1, 7, &leave_body("g", &id)))
            .await
            .unwrap();
        assert_eq!(read_error_only(&reply[4..], 1), 0);
        let reply = exchange(
            &mut socket,
            &request(12, 2, 8, &heartbeat_body(2, "g", 1, &id)),
        )
        .await
        .unwrap();
        assert_eq!(
            read_error_only(&reply[4..], 2),
            ErrorCode::UnknownMemberId.as_i16(),
            "gone"
        );
    }

    /// Two members of one group over one partition (the acceptance's shape,
    /// on one range): both join one generation, the leader sees both, the
    /// follower's sync is parked until the leader's, the assignor gives the
    /// one partition to one member and none to the other; the leader leaves,
    /// the follower's heartbeat says rebalance, its rejoin leads generation
    /// 2, and an assignment naming a partition this gateway does not serve is
    /// refused.
    #[tokio::test]
    async fn two_members_one_partition_and_the_assignor_gives_it_to_one() {
        let (addr, _stop) =
            start_groups(Arc::new(MemoryBridge::with_topics(["events"])), None).await;
        let protocols: &[(&str, &[u8])] = &[("range", b"sub")];
        let mut a = TcpStream::connect(addr).await.unwrap();
        let mut b = TcpStream::connect(addr).await.unwrap();
        let join_a = request(11, 2, 1, &join_body(2, "g", "", protocols));
        let join_b = request(11, 2, 2, &join_body(2, "g", "", protocols));
        let (ra, rb) = tokio::join!(exchange(&mut a, &join_a), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            exchange(&mut b, &join_b).await
        });
        let ja = read_join(&ra.unwrap()[4..], 2);
        let jb = read_join(&rb.unwrap()[4..], 2);
        assert_eq!(
            (ja.error, jb.error, ja.generation, jb.generation),
            (0, 0, 1, 1)
        );
        assert_eq!(ja.leader, jb.leader);
        let (leader, follower, mut ls, mut fs) = if ja.leader == ja.member_id {
            (ja, jb, a, b)
        } else {
            (jb, ja, b, a)
        };
        assert_eq!(leader.members.len(), 2);
        assert!(follower.members.is_empty());
        let p0 = consumer_assignment("events", &[0]);
        let follower_sync = request(14, 1, 3, &sync_body(1, "g", 1, &follower.member_id, &[]));
        let leader_sync = request(
            14,
            1,
            4,
            &sync_body(
                1,
                "g",
                1,
                &leader.member_id,
                &[(&leader.member_id, &p0), (&follower.member_id, &[])],
            ),
        );
        let (rf, rl) = tokio::join!(exchange(&mut fs, &follower_sync), async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            exchange(&mut ls, &leader_sync).await
        });
        assert_eq!(
            read_sync(&rl.unwrap()[4..], 1),
            (0, p0.clone()),
            "the leader has the partition"
        );
        assert_eq!(
            read_sync(&rf.unwrap()[4..], 1),
            (0, Vec::new()),
            "the follower has none"
        );
        let reply = exchange(
            &mut fs,
            &request(12, 1, 5, &heartbeat_body(1, "g", 1, &follower.member_id)),
        )
        .await
        .unwrap();
        assert_eq!(read_error_only(&reply[4..], 1), 0);
        // The leader leaves; the follower must rejoin, and leads.
        let reply = exchange(
            &mut ls,
            &request(13, 1, 6, &leave_body("g", &leader.member_id)),
        )
        .await
        .unwrap();
        assert_eq!(read_error_only(&reply[4..], 1), 0);
        let reply = exchange(
            &mut fs,
            &request(12, 1, 7, &heartbeat_body(1, "g", 1, &follower.member_id)),
        )
        .await
        .unwrap();
        assert_eq!(
            read_error_only(&reply[4..], 1),
            ErrorCode::RebalanceInProgress.as_i16()
        );
        let reply = exchange(
            &mut fs,
            &request(11, 2, 8, &join_body(2, "g", &follower.member_id, protocols)),
        )
        .await
        .unwrap();
        let again = read_join(&reply[4..], 2);
        assert_eq!((again.error, again.generation), (0, 2));
        assert_eq!(again.leader, follower.member_id);
        // A partition this gateway does not serve: refused, and the round the
        // assignment was for is over (review) — the leader rejoins and
        // assigns again in the generation that follows.
        let p5 = consumer_assignment("events", &[5]);
        let reply = exchange(
            &mut fs,
            &request(
                14,
                1,
                9,
                &sync_body(
                    1,
                    "g",
                    2,
                    &follower.member_id,
                    &[(&follower.member_id, &p5)],
                ),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            read_sync(&reply[4..], 1).0,
            ErrorCode::InconsistentGroupProtocol.as_i16()
        );
        let reply = exchange(
            &mut fs,
            &request(
                14,
                1,
                10,
                &sync_body(
                    1,
                    "g",
                    2,
                    &follower.member_id,
                    &[(&follower.member_id, &p0)],
                ),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            read_sync(&reply[4..], 1).0,
            ErrorCode::RebalanceInProgress.as_i16(),
            "the round the refused assignment was for is over"
        );
        let reply = exchange(
            &mut fs,
            &request(
                11,
                2,
                11,
                &join_body(2, "g", &follower.member_id, protocols),
            ),
        )
        .await
        .unwrap();
        let third = read_join(&reply[4..], 2);
        assert_eq!((third.error, third.generation), (0, 3));
        let reply = exchange(
            &mut fs,
            &request(
                14,
                1,
                12,
                &sync_body(
                    1,
                    "g",
                    3,
                    &follower.member_id,
                    &[(&follower.member_id, &p0)],
                ),
            ),
        )
        .await
        .unwrap();
        assert_eq!(read_sync(&reply[4..], 1), (0, p0));
    }

    /// Committed offsets live in the store: a commit is read back, survives a
    /// gateway rebuilt over the same store, is judged by name per partition
    /// and by membership; without a store, a commit is refused by name and a
    /// fetch says nothing is committed — which is true.
    #[tokio::test]
    async fn offsets_live_in_the_store_and_are_refused_by_name_without_one() {
        let store: Arc<dyn OffsetStore> = Arc::new(crate::offsets::MemoryOffsetStore::default());
        let bridge = Arc::new(MemoryBridge::with_topics(["events", "audit"]));
        let (addr, _stop) = start_groups(bridge.clone(), Some(Arc::clone(&store))).await;
        // Fifty records, so a commit up to 50 is a position that exists.
        let values: Vec<String> = (0..50).map(|i| format!("v{i}")).collect();
        let refs: Vec<&str> = values.iter().map(String::as_str).collect();
        let reply = call(
            addr,
            0,
            8,
            100,
            &produce_body("events", 0, -1, None, &batch_bytes(&refs, false)),
        )
        .await
        .unwrap();
        assert_eq!(read_produce(&reply, 8).0, 0);
        // Past the watermark is not a position (review, #468); the watermark
        // itself — the next offset to consume — is.
        let reply = call(
            addr,
            8,
            7,
            101,
            &offset_commit_body(7, "g", -1, "", "events", 0, 51, None),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![(0, ErrorCode::OffsetOutOfRange.as_i16())]
        );
        let reply = call(
            addr,
            8,
            7,
            102,
            &offset_commit_body(7, "g", -1, "", "events", 0, 50, None),
        )
        .await
        .unwrap();
        assert_eq!(read_offset_commit(&reply, 7), vec![(0, 0)]);
        let reply = call(
            addr,
            8,
            7,
            1,
            &offset_commit_body(7, "g", -1, "", "events", 0, 42, Some("m")),
        )
        .await
        .unwrap();
        assert_eq!(read_offset_commit(&reply, 7), vec![(0, 0)]);
        let reply = call(
            addr,
            9,
            5,
            2,
            &offset_fetch_body(5, "g", Some(&[("events", &[0, 3])])),
        )
        .await
        .unwrap();
        let (error, topics) = read_offset_fetch(&reply, 5);
        assert_eq!(error, 0);
        assert_eq!(
            topics,
            vec![(
                "events".to_owned(),
                vec![(0, 42, Some("m".to_owned()), 0), (3, -1, None, 0)]
            )]
        );
        // A null topic list at v2+: what the group COMMITTED — events, not
        // the served-but-uncommitted audit.
        let reply = call(addr, 9, 2, 3, &offset_fetch_body(2, "g", None))
            .await
            .unwrap();
        assert_eq!(
            read_offset_fetch(&reply, 2).1,
            vec![("events".to_owned(), vec![(0, 42, Some("m".to_owned()), 0)])]
        );
        // A gateway that no longer serves the topic still answers a
        // null-topic fetch with what the group committed there.
        let (moved, _stop5) = start_groups(
            Arc::new(MemoryBridge::with_topics(["other"])),
            Some(Arc::clone(&store)),
        )
        .await;
        let reply = call(moved, 9, 2, 13, &offset_fetch_body(2, "g", None))
            .await
            .unwrap();
        assert_eq!(
            read_offset_fetch(&reply, 2).1,
            vec![("events".to_owned(), vec![(0, 42, Some("m".to_owned()), 0)])]
        );
        // A negative offset is not a position: refused, nothing stored.
        let reply = call(
            addr,
            8,
            7,
            12,
            &offset_commit_body(7, "g", -1, "", "events", 0, -5, None),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![(0, ErrorCode::OffsetOutOfRange.as_i16())]
        );
        // An empty group id commits nothing, even as a simple consumer.
        let reply = call(
            addr,
            8,
            7,
            11,
            &offset_commit_body(7, "", -1, "", "events", 0, 1, None),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![(0, ErrorCode::InvalidGroupId.as_i16())]
        );
        // Metadata over the cap; a membership the group does not know.
        let long = "x".repeat(MAX_OFFSET_METADATA_BYTES + 1);
        let reply = call(
            addr,
            8,
            7,
            4,
            &offset_commit_body(7, "g", -1, "", "events", 0, 43, Some(&long)),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![(0, ErrorCode::OffsetMetadataTooLarge.as_i16())]
        );
        let reply = call(
            addr,
            8,
            7,
            5,
            &offset_commit_body(7, "g", 7, "someone", "events", 0, 43, None),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![(0, ErrorCode::IllegalGeneration.as_i16())]
        );
        // A gateway rebuilt over the same store: the offset is the store's.
        let (again, _stop2) = start_groups(bridge, Some(store)).await;
        let reply = call(
            again,
            9,
            1,
            6,
            &offset_fetch_body(1, "g", Some(&[("events", &[0])])),
        )
        .await
        .unwrap();
        assert_eq!(read_offset_fetch(&reply, 1).1[0].1[0].1, 42);
        // No store: a commit is refused by name, a fetch has nothing.
        let (bare, _stop3) =
            start_groups(Arc::new(MemoryBridge::with_topics(["events"])), None).await;
        let reply = call(
            bare,
            8,
            7,
            7,
            &offset_commit_body(7, "g", -1, "", "events", 0, 0, None),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![(0, ErrorCode::UnsupportedForMessageFormat.as_i16())]
        );
        let reply = call(
            bare,
            9,
            5,
            8,
            &offset_fetch_body(5, "g", Some(&[("events", &[0])])),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_fetch(&reply, 5),
            (0, vec![("events".to_owned(), vec![(0, -1, None, 0)])])
        );
    }

    /// A bridge whose watermark lookup stalls: the commit is
    /// `REQUEST_TIMED_OUT` at the cap (review), not held for as long as the
    /// bridge takes — and the lookups it counts are ONE (review): once the
    /// deadline has passed, no further blocking task is spawned.
    struct StalledWatermarkBridge(MemoryBridge, std::sync::atomic::AtomicUsize);
    impl Bridge for StalledWatermarkBridge {
        fn topics(&self) -> Vec<String> {
            self.0.topics()
        }
        fn produce(
            &self,
            topic: &str,
            batches: &[RecordBatch],
            sequenced: Option<Sequenced>,
        ) -> Result<crate::bridge::Appended, ErrorCode> {
            self.0.produce(topic, batches, sequenced)
        }
        fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode> {
            self.0.fetch(topic, offset, max_bytes)
        }
        fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
            self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Far past the cap; short enough that the runtime's shutdown is
            // not held on it for long.
            std::thread::sleep(Duration::from_secs(2));
            self.0.bounds(topic)
        }
    }
    #[tokio::test]
    async fn a_stalled_watermark_lookup_is_timed_out_at_the_cap() {
        let topics: Vec<String> = (0..10).map(|i| format!("t{i}")).collect();
        let bridge = Arc::new(StalledWatermarkBridge(
            MemoryBridge::with_topics(topics.clone()),
            std::sync::atomic::AtomicUsize::new(0),
        ));
        let (addr, _stop) = start_groups_tuned(
            Arc::clone(&bridge) as Arc<dyn Bridge>,
            Some(Arc::new(crate::offsets::MemoryOffsetStore::default())),
            |c| c.max_offset_wait = Duration::from_millis(100),
        )
        .await;
        let started = tokio::time::Instant::now();
        let named: Vec<(&str, i64)> = topics.iter().map(|t| (t.as_str(), 0)).collect();
        let reply = call(addr, 8, 7, 1, &offset_commit_many_body("g", &named))
            .await
            .unwrap();
        let answers = read_offset_commit(&reply, 7);
        assert_eq!(answers.len(), 10);
        assert!(
            answers
                .iter()
                .all(|(_, error)| *error == ErrorCode::RequestTimedOut.as_i16()),
            "{answers:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "bounded by the cap, not by the bridge"
        );
        assert_eq!(
            bridge.1.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "one lookup ate the deadline; none was spawned after it"
        );
    }

    /// A bridge that counts its enumerations: a SyncGroup of several
    /// assignments is judged against one (review).
    struct CountingTopicsBridge(MemoryBridge, std::sync::atomic::AtomicUsize);
    impl Bridge for CountingTopicsBridge {
        fn topics(&self) -> Vec<String> {
            self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.0.topics()
        }
        fn produce(
            &self,
            topic: &str,
            batches: &[RecordBatch],
            sequenced: Option<Sequenced>,
        ) -> Result<crate::bridge::Appended, ErrorCode> {
            self.0.produce(topic, batches, sequenced)
        }
        fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode> {
            self.0.fetch(topic, offset, max_bytes)
        }
        fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
            self.0.bounds(topic)
        }
    }
    #[tokio::test]
    async fn a_sync_group_enumerates_the_topics_once() {
        let bridge = Arc::new(CountingTopicsBridge(
            MemoryBridge::with_topics(["events"]),
            std::sync::atomic::AtomicUsize::new(0),
        ));
        let (addr, _stop) = start_groups(Arc::clone(&bridge) as Arc<dyn Bridge>, None).await;
        let reply = call(
            addr,
            11,
            2,
            1,
            &join_body(2, "g", "", &[("range", b"m".as_slice())]),
        )
        .await
        .unwrap();
        let joined = read_join(&reply, 2);
        assert_eq!((joined.error, joined.generation), (0, 1));
        let before = bridge.1.load(std::sync::atomic::Ordering::SeqCst);
        let assignment = consumer_assignment("events", &[0]);
        let reply = call(
            addr,
            14,
            1,
            2,
            &sync_body(
                1,
                "g",
                1,
                &joined.member_id,
                &[
                    (joined.member_id.as_str(), assignment.as_slice()),
                    ("someone-else", assignment.as_slice()),
                    ("a-third", assignment.as_slice()),
                ],
            ),
        )
        .await
        .unwrap();
        assert_eq!(read_sync(&reply, 1).0, 0);
        assert_eq!(
            bridge.1.load(std::sync::atomic::Ordering::SeqCst) - before,
            1,
            "three assignments, one enumeration"
        );
    }

    /// A deadline already spent spawns no enumeration (review): the answer
    /// is `REQUEST_TIMED_OUT` and the bridge is not asked.
    #[tokio::test]
    async fn a_spent_deadline_spawns_no_enumeration() {
        let bridge = Arc::new(CountingTopicsBridge(
            MemoryBridge::with_topics(["events"]),
            std::sync::atomic::AtomicUsize::new(0),
        ));
        let gateway = Gateway::new(
            Arc::clone(&bridge) as Arc<dyn Bridge>,
            GatewayConfig::default(),
        );
        let spent = tokio::time::Instant::now() - Duration::from_millis(1);
        assert_eq!(
            gateway.served_topics(spent).await,
            Err(ErrorCode::RequestTimedOut)
        );
        assert_eq!(bridge.1.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(gateway
            .served_topics(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .is_ok());
        assert_eq!(bridge.1.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// A SyncGroup is judged on membership before its assignments (review):
    /// a stale generation carrying a malformed assignment is
    /// `ILLEGAL_GENERATION`, a stranger's is `UNKNOWN_MEMBER_ID`, and the
    /// bridge is not asked for either.
    #[tokio::test]
    async fn a_sync_group_is_judged_on_membership_before_its_assignments() {
        let bridge = Arc::new(CountingTopicsBridge(
            MemoryBridge::with_topics(["events"]),
            std::sync::atomic::AtomicUsize::new(0),
        ));
        let (addr, _stop) = start_groups(Arc::clone(&bridge) as Arc<dyn Bridge>, None).await;
        let reply = call(
            addr,
            11,
            2,
            1,
            &join_body(2, "g", "", &[("range", b"m".as_slice())]),
        )
        .await
        .unwrap();
        let joined = read_join(&reply, 2);
        assert_eq!((joined.error, joined.generation), (0, 1));
        let before = bridge.1.load(std::sync::atomic::Ordering::SeqCst);
        let garbage = b"not an assignment".to_vec();
        let reply = call(
            addr,
            14,
            1,
            2,
            &sync_body(
                1,
                "g",
                7,
                &joined.member_id,
                &[(joined.member_id.as_str(), garbage.as_slice())],
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            read_sync(&reply, 1).0,
            ErrorCode::IllegalGeneration.as_i16()
        );
        let reply = call(
            addr,
            14,
            1,
            3,
            &sync_body(1, "g", 1, "stranger", &[("stranger", garbage.as_slice())]),
        )
        .await
        .unwrap();
        assert_eq!(read_sync(&reply, 1).0, ErrorCode::UnknownMemberId.as_i16());
        assert_eq!(
            bridge.1.load(std::sync::atomic::Ordering::SeqCst),
            before,
            "no enumeration for a member not entitled to assign"
        );
    }

    /// A fenced gateway stops claiming the range (review): it no longer names
    /// itself the coordinator, no longer leads the partition in its Metadata,
    /// and refuses the group protocol as unavailable — so a stock client
    /// refreshes and finds the node that holds the lease now, rather than
    /// retrying for good at a listener that can take nothing. A view that is
    /// merely busy is not a handoff: the gateway keeps serving.
    #[tokio::test]
    async fn a_fenced_gateway_stops_claiming_the_range() {
        struct View(std::sync::Mutex<crate::lease::LeaseState>);
        impl crate::lease::LeaseView for View {
            fn lease(&self) -> crate::lease::LeaseState {
                *self.0.lock().unwrap()
            }
        }
        let view = Arc::new(View(std::sync::Mutex::new(crate::lease::LeaseState::Held(
            3,
        ))));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let gateway = Gateway::new(
            Arc::new(MemoryBridge::with_topics(["events"])),
            GatewayConfig {
                advertised_port: addr.port() as i32,
                groups: GroupConfig {
                    initial_rebalance_delay: Duration::from_millis(50),
                    ..GroupConfig::default()
                },
                ..GatewayConfig::default()
            },
        )
        .with_lease(Arc::clone(&view) as Arc<dyn crate::lease::LeaseView>);
        tokio::spawn(gateway.serve(listener, rx));
        // Metadata v4: topics, then two booleans the version carries.
        let named = || {
            let mut e = Encoder::new();
            e.array_len(1);
            e.string("events");
            e.bool(true);
            e.into_vec()
        };
        let topic_error = |reply: &[u8]| {
            let mut d = Decoder::new(reply);
            d.i32("throttle").unwrap();
            d.array_len("brokers").unwrap();
            d.i32("node").unwrap();
            d.string("host").unwrap();
            d.i32("port").unwrap();
            d.nullable_string("rack").unwrap();
            d.nullable_string("cluster").unwrap();
            d.i32("controller").unwrap();
            d.array_len("topics").unwrap();
            d.i16("topic error").unwrap()
        };
        // Holding: it is the coordinator and it leads the partition.
        let reply = call(addr, 10, 1, 1, &find_coordinator_body(1, "g", 0))
            .await
            .unwrap();
        assert_eq!(read_find_coordinator(&reply, 1).0, 0);
        let reply = call(addr, 3, 4, 2, &named()).await.unwrap();
        assert_eq!(topic_error(&reply), 0, "holding: it leads the partition");
        // Fenced: it claims nothing.
        *view.0.lock().unwrap() = crate::lease::LeaseState::Gone;
        let reply = call(addr, 10, 1, 3, &find_coordinator_body(1, "g", 0))
            .await
            .unwrap();
        assert_eq!(
            read_find_coordinator(&reply, 1).0,
            ErrorCode::CoordinatorNotAvailable.as_i16()
        );
        let reply = call(
            addr,
            11,
            2,
            4,
            &join_body(2, "g", "", &[("range", b"m".as_slice())]),
        )
        .await
        .unwrap();
        assert_eq!(
            read_join(&reply, 2).error,
            ErrorCode::CoordinatorNotAvailable.as_i16()
        );
        let reply = call(addr, 12, 1, 5, &heartbeat_body(1, "g", 1, "m"))
            .await
            .unwrap();
        assert_eq!(
            read_error_only(&reply, 1),
            ErrorCode::CoordinatorNotAvailable.as_i16()
        );
        let reply = call(addr, 13, 1, 6, &leave_body("g", "m")).await.unwrap();
        assert_eq!(
            read_error_only(&reply, 1),
            ErrorCode::CoordinatorNotAvailable.as_i16()
        );
        let reply = call(addr, 3, 4, 7, &named()).await.unwrap();
        assert_eq!(
            topic_error(&reply),
            ErrorCode::NotLeaderOrFollower.as_i16(),
            "a fenced gateway leads nothing"
        );
        // Busy is not a handoff.
        *view.0.lock().unwrap() = crate::lease::LeaseState::Unknown;
        let reply = call(addr, 10, 1, 8, &find_coordinator_body(1, "g", 0))
            .await
            .unwrap();
        assert_eq!(read_find_coordinator(&reply, 1).0, 0);
    }

    /// A named fetch is the store's answer whatever the gateway serves
    /// (review): an offset committed for a topic this gateway no longer
    /// serves is still what the group committed, and a client naming its
    /// assignment resumes by it.
    #[tokio::test]
    async fn a_named_fetch_is_the_stores_answer_whatever_the_gateway_serves() {
        let store = Arc::new(crate::offsets::MemoryOffsetStore::default());
        store
            .commit(
                "g",
                "audit",
                0,
                crate::offsets::Committed {
                    offset: 7,
                    metadata: None,
                },
            )
            .await
            .unwrap();
        let (addr, _stop) = start_groups(
            Arc::new(MemoryBridge::with_topics(["events"])),
            Some(store as Arc<dyn OffsetStore>),
        )
        .await;
        let reply = call(
            addr,
            9,
            5,
            1,
            &offset_fetch_body(5, "g", Some(&[("audit", &[0])])),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_fetch(&reply, 5),
            (0, vec![("audit".to_owned(), vec![(0, 7, None, 0)])])
        );
    }

    /// The default minimum session clears every wait a heartbeat can queue
    /// behind on one connection (review): the fetch ceiling and the offset
    /// ceiling, so the heartbeat is read before the session it keeps alive
    /// can lapse.
    #[test]
    fn the_default_minimum_session_clears_every_wait_a_heartbeat_can_queue_behind() {
        let gateway = GatewayConfig::default();
        let minimum = GroupConfig::default().min_session_timeout;
        assert!(minimum > gateway.max_fetch_wait);
        assert!(minimum > gateway.max_offset_wait);
    }

    /// The drain waits for a bridge call the deadline abandoned (review): a
    /// watermark lookup that sleeps past a 100 ms cap is answered
    /// `REQUEST_TIMED_OUT` at the cap, and `serve` returns only once the
    /// lookup itself has ended — never with the bridge still in a call.
    #[tokio::test]
    async fn the_drain_waits_for_a_bridge_call_the_deadline_abandoned() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::watch::channel(false);
        let config = GatewayConfig {
            advertised_port: addr.port() as i32,
            drain_timeout: Duration::from_millis(50),
            max_offset_wait: Duration::from_millis(100),
            ..GatewayConfig::default()
        };
        let bridge = Arc::new(StalledWatermarkBridge(
            MemoryBridge::with_topics(["events"]),
            std::sync::atomic::AtomicUsize::new(0),
        ));
        let gateway = Gateway::new(bridge as Arc<dyn Bridge>, config)
            .with_offsets(Arc::new(crate::offsets::MemoryOffsetStore::default()));
        let serve = tokio::spawn(gateway.serve(listener, rx));
        let begun = tokio::time::Instant::now();
        let reply = call(
            addr,
            8,
            7,
            1,
            &offset_commit_body(7, "g", -1, "", "events", 0, 0, None),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![(0, ErrorCode::RequestTimedOut.as_i16())]
        );
        tx.send(true).unwrap();
        serve.await.unwrap().unwrap();
        let ended = begun.elapsed();
        assert!(
            ended >= Duration::from_millis(1_500) && ended < Duration::from_secs(10),
            "returned only when the abandoned lookup ended: {ended:?}"
        );
    }

    /// The turn outlives a write the deadline abandoned (review): a write
    /// that takes 400 ms under a 150 ms cap is abandoned by its request but
    /// driven to its end; a commit behind it waits for the turn and is
    /// `REQUEST_TIMED_OUT` rather than written first; one after the write
    /// has landed succeeds. The store saw 10 then 30, never 20.
    #[tokio::test]
    async fn a_turn_outlives_the_write_the_deadline_abandoned() {
        let store = Arc::new(SlowFirstStore::default());
        let (addr, _stop) = start_groups_tuned(
            Arc::new(MemoryBridge::with_topics(["events"])),
            Some(Arc::clone(&store) as Arc<dyn OffsetStore>),
            |c| c.max_offset_wait = Duration::from_millis(150),
        )
        .await;
        let values: Vec<String> = (0..50).map(|i| format!("v{i}")).collect();
        let refs: Vec<&str> = values.iter().map(String::as_str).collect();
        let reply = call(
            addr,
            0,
            8,
            100,
            &produce_body("events", 0, -1, None, &batch_bytes(&refs, false)),
        )
        .await
        .unwrap();
        assert_eq!(read_produce(&reply, 8).0, 0);
        let abandoned = call(
            addr,
            8,
            7,
            1,
            &offset_commit_body(7, "g", -1, "", "events", 0, 10, None),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_commit(&abandoned, 7),
            vec![(0, ErrorCode::RequestTimedOut.as_i16())]
        );
        let behind = call(
            addr,
            8,
            7,
            2,
            &offset_commit_body(7, "g", -1, "", "events", 0, 20, None),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_commit(&behind, 7),
            vec![(0, ErrorCode::RequestTimedOut.as_i16())],
            "waited for the turn the abandoned write still holds"
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
        let after = call(
            addr,
            8,
            7,
            3,
            &offset_commit_body(7, "g", -1, "", "events", 0, 30, None),
        )
        .await
        .unwrap();
        assert_eq!(read_offset_commit(&after, 7), vec![(0, 0)]);
        let landed: Vec<i64> = store
            .landed
            .lock()
            .unwrap()
            .iter()
            .filter(|(g, _)| g == "g")
            .map(|(_, offset)| *offset)
            .collect();
        assert_eq!(
            landed,
            vec![10, 30],
            "the abandoned write landed first; 20 never did"
        );
        let reply = call(
            addr,
            9,
            5,
            4,
            &offset_fetch_body(5, "g", Some(&[("events", &[0])])),
        )
        .await
        .unwrap();
        assert_eq!(read_offset_fetch(&reply, 5).1[0].1[0].1, 30);
    }

    /// A null-topic fetch answers at most as many partitions as a request
    /// may name (review): a group with 4 097 committed partitions is refused
    /// by name, one with 4 096 is answered whole.
    #[tokio::test]
    async fn a_null_topic_fetch_over_the_partition_cap_is_refused_by_name() {
        let cap = crate::api::MAX_PARTITIONS_PER_REQUEST;
        let store = Arc::new(crate::offsets::MemoryOffsetStore::default());
        for (group, rows) in [("over", cap + 1), ("whole", cap)] {
            for i in 0..rows {
                store
                    .commit(
                        group,
                        &format!("t{i}"),
                        0,
                        crate::offsets::Committed {
                            offset: 1,
                            metadata: None,
                        },
                    )
                    .await
                    .unwrap();
            }
        }
        let (addr, _stop) = start_groups(
            Arc::new(MemoryBridge::with_topics(["events"])),
            Some(store as Arc<dyn OffsetStore>),
        )
        .await;
        let reply = call(addr, 9, 5, 1, &offset_fetch_body(5, "over", None))
            .await
            .unwrap();
        assert_eq!(
            read_offset_fetch(&reply, 5),
            (ErrorCode::InvalidRequest.as_i16(), Vec::new())
        );
        let reply = call(addr, 9, 5, 2, &offset_fetch_body(5, "whole", None))
            .await
            .unwrap();
        let (error, topics) = read_offset_fetch(&reply, 5);
        assert_eq!(error, 0);
        assert_eq!(topics.len(), cap, "answered whole");
    }

    /// A store that returns metadata over the cap: the offset is answered
    /// without it (review), by name and by the null-topic listing alike, and
    /// the session lives on.
    struct OversizeMetadataStore;
    #[async_trait::async_trait]
    impl OffsetStore for OversizeMetadataStore {
        async fn commit(
            &self,
            _: &str,
            _: &str,
            _: i32,
            _: crate::offsets::Committed,
        ) -> Result<(), ErrorCode> {
            Ok(())
        }
        async fn fetch(
            &self,
            _: &str,
            _: &str,
            _: i32,
        ) -> Result<Option<crate::offsets::Committed>, ErrorCode> {
            Ok(Some(crate::offsets::Committed {
                offset: 9,
                metadata: Some("m".repeat(40_000)),
            }))
        }
        async fn committed(
            &self,
            _: &str,
            _: usize,
        ) -> Result<Vec<(String, i32, crate::offsets::Committed)>, ErrorCode> {
            Ok(vec![(
                "events".to_owned(),
                0,
                crate::offsets::Committed {
                    offset: 9,
                    metadata: Some("m".repeat(40_000)),
                },
            )])
        }
    }
    /// A store whose listing carries a topic name over the STRING bound: the
    /// row is skipped, the rest answered, the session alive.
    struct OversizeNameStore;
    #[async_trait::async_trait]
    impl OffsetStore for OversizeNameStore {
        async fn commit(
            &self,
            _: &str,
            _: &str,
            _: i32,
            _: crate::offsets::Committed,
        ) -> Result<(), ErrorCode> {
            Ok(())
        }
        async fn fetch(
            &self,
            _: &str,
            _: &str,
            _: i32,
        ) -> Result<Option<crate::offsets::Committed>, ErrorCode> {
            Ok(None)
        }
        async fn committed(
            &self,
            _: &str,
            _: usize,
        ) -> Result<Vec<(String, i32, crate::offsets::Committed)>, ErrorCode> {
            let row = crate::offsets::Committed {
                offset: 3,
                metadata: None,
            };
            Ok(vec![
                ("n".repeat(40_000), 0, row.clone()),
                ("events".to_owned(), 0, row),
            ])
        }
    }
    #[tokio::test]
    async fn an_uncarriable_topic_name_from_the_store_is_skipped_not_fatal() {
        let (addr, _stop) = start_groups(
            Arc::new(MemoryBridge::with_topics(["events"])),
            Some(Arc::new(OversizeNameStore)),
        )
        .await;
        let reply = call(addr, 9, 5, 1, &offset_fetch_body(5, "g", None))
            .await
            .unwrap();
        assert_eq!(
            read_offset_fetch(&reply, 5),
            (0, vec![("events".to_owned(), vec![(0, 3, None, 0)])])
        );
    }

    #[tokio::test]
    async fn oversize_store_metadata_is_dropped_not_fatal() {
        let (addr, _stop) = start_groups(
            Arc::new(MemoryBridge::with_topics(["events"])),
            Some(Arc::new(OversizeMetadataStore)),
        )
        .await;
        let reply = call(
            addr,
            9,
            5,
            1,
            &offset_fetch_body(5, "g", Some(&[("events", &[0])])),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_fetch(&reply, 5),
            (0, vec![("events".to_owned(), vec![(0, 9, None, 0)])])
        );
        let reply = call(addr, 9, 5, 2, &offset_fetch_body(5, "g", None))
            .await
            .unwrap();
        assert_eq!(
            read_offset_fetch(&reply, 5),
            (0, vec![("events".to_owned(), vec![(0, 9, None, 0)])])
        );
    }

    /// A leader naming a member twice: the last entry wins (review), as the
    /// coordinator applies it, so a malformed earlier entry is not judged.
    #[tokio::test]
    async fn a_duplicate_assignment_entry_is_judged_last_wins() {
        let (addr, _stop) =
            start_groups(Arc::new(MemoryBridge::with_topics(["events"])), None).await;
        let reply = call(
            addr,
            11,
            2,
            1,
            &join_body(2, "g", "", &[("range", b"m".as_slice())]),
        )
        .await
        .unwrap();
        let joined = read_join(&reply, 2);
        assert_eq!((joined.error, joined.generation), (0, 1));
        let p0 = consumer_assignment("events", &[0]);
        let reply = call(
            addr,
            14,
            1,
            2,
            &sync_body(
                1,
                "g",
                1,
                &joined.member_id,
                &[
                    (joined.member_id.as_str(), b"not an assignment".as_slice()),
                    (joined.member_id.as_str(), p0.as_slice()),
                ],
            ),
        )
        .await
        .unwrap();
        assert_eq!(read_sync(&reply, 1), (0, p0));
    }

    /// An assignment for an id that is not a member is ignored, not judged
    /// (review): the leader's SyncGroup carrying a malformed one for a
    /// stranger is applied for the members it names.
    #[tokio::test]
    async fn a_foreign_assignment_is_ignored_not_judged() {
        let (addr, _stop) =
            start_groups(Arc::new(MemoryBridge::with_topics(["events"])), None).await;
        let reply = call(
            addr,
            11,
            2,
            1,
            &join_body(2, "g", "", &[("range", b"m".as_slice())]),
        )
        .await
        .unwrap();
        let joined = read_join(&reply, 2);
        assert_eq!((joined.error, joined.generation), (0, 1));
        let p0 = consumer_assignment("events", &[0]);
        let reply = call(
            addr,
            14,
            1,
            2,
            &sync_body(
                1,
                "g",
                1,
                &joined.member_id,
                &[
                    (joined.member_id.as_str(), p0.as_slice()),
                    ("stranger", b"not an assignment".as_slice()),
                ],
            ),
        )
        .await
        .unwrap();
        assert_eq!(read_sync(&reply, 1), (0, p0));
    }

    /// Static membership is not served (review): JoinGroup v5, SyncGroup v3
    /// and Heartbeat v3 — the versions that carry `group.instance.id` — are
    /// outside the served range and closed at the header, and an OffsetCommit
    /// v7 naming an instance is an unknown member's, no static member existing
    /// here.
    #[tokio::test]
    async fn static_membership_versions_are_not_served() {
        let (addr, _stop) = start_groups(
            Arc::new(MemoryBridge::with_topics(["events"])),
            Some(Arc::new(crate::offsets::MemoryOffsetStore::default())),
        )
        .await;
        let protocols: &[(&str, &[u8])] = &[("range", b"m")];
        assert!(call(addr, 11, 5, 1, &join_body(5, "g", "", protocols))
            .await
            .is_none());
        assert!(call(addr, 14, 3, 2, &sync_body(3, "g", 1, "m", &[]))
            .await
            .is_none());
        assert!(call(addr, 12, 3, 3, &heartbeat_body(3, "g", 1, "m"))
            .await
            .is_none());
        let mut e = Encoder::new();
        e.string("g");
        e.i32(-1);
        e.string("");
        e.nullable_string(Some("instance-1"));
        e.array_len(1);
        e.string("events");
        e.array_len(1);
        e.i32(0);
        e.i64(0);
        e.i32(-1);
        e.nullable_string(None);
        let reply = call(addr, 8, 7, 4, &e.into_vec()).await.unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![(0, ErrorCode::UnknownMemberId.as_i16())]
        );
        // The group id is judged first (review): empty, it is invalid whatever
        // instance the commit names.
        let mut e = Encoder::new();
        e.string("");
        e.i32(-1);
        e.string("");
        e.nullable_string(Some("instance-1"));
        e.array_len(1);
        e.string("events");
        e.array_len(1);
        e.i32(0);
        e.i64(0);
        e.i32(-1);
        e.nullable_string(None);
        let reply = call(addr, 8, 7, 5, &e.into_vec()).await.unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![(0, ErrorCode::InvalidGroupId.as_i16())]
        );
    }

    /// A Metadata naming no topics closes the connection when the bridge does
    /// not enumerate its topics in time (audit): it has no slot for a code,
    /// and an empty list would read as "no topics". Named topics still
    /// answer, per topic, at the bounds ceiling.
    #[tokio::test]
    async fn a_metadata_naming_no_topics_closes_when_the_bridge_does_not_answer() {
        let (addr, _stop) = start_groups_tuned(
            Arc::new(StalledTopicsBridge(MemoryBridge::with_topics(["events"]))),
            None,
            |c| c.max_fetch_wait = Duration::from_millis(100),
        )
        .await;
        let started = tokio::time::Instant::now();
        let mut all = Encoder::new();
        all.i32(-1);
        assert!(
            call(addr, 3, 1, 1, &all.into_vec()).await.is_none(),
            "closed, not answered with none"
        );
        assert!(started.elapsed() < Duration::from_secs(1), "at the ceiling");
        let mut named = Encoder::new();
        named.array_len(1);
        named.string("events");
        assert!(call(addr, 3, 1, 2, &named.into_vec()).await.is_some());
    }

    /// A commit refused by name asks the bridge nothing (review): a partition
    /// this gateway never serves, a negative offset and metadata over the cap
    /// are answered at once on a bridge whose enumeration stalls — and in a
    /// request mixing one of them with a partition left to judge, the one
    /// decided by name keeps its code while the other hears the bridge's.
    #[tokio::test]
    async fn a_commit_refused_by_name_asks_the_bridge_nothing() {
        let (addr, _stop) = start_groups_tuned(
            Arc::new(StalledTopicsBridge(MemoryBridge::with_topics(["events"]))),
            Some(Arc::new(crate::offsets::MemoryOffsetStore::default())),
            |c| c.max_offset_wait = Duration::from_millis(100),
        )
        .await;
        let started = tokio::time::Instant::now();
        let long = "x".repeat(MAX_OFFSET_METADATA_BYTES + 1);
        for (correlation, partition, offset, metadata, expected) in [
            (
                1,
                0,
                0,
                Some(long.as_str()),
                ErrorCode::OffsetMetadataTooLarge,
            ),
            (2, 3, 0, None, ErrorCode::UnknownTopicOrPartition),
            (3, 0, -5, None, ErrorCode::OffsetOutOfRange),
        ] {
            let reply = call(
                addr,
                8,
                7,
                correlation,
                &offset_commit_body(7, "g", -1, "", "events", partition, offset, metadata),
            )
            .await
            .unwrap();
            assert_eq!(
                read_offset_commit(&reply, 7),
                vec![(partition, expected.as_i16())]
            );
        }
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "decided by name, the bridge never asked: {:?}",
            started.elapsed()
        );
        // Mixed: one decided by name, one left to the bridge, which stalls.
        let mut e = Encoder::new();
        e.string("g");
        e.i32(-1);
        e.string("");
        e.nullable_string(None);
        e.array_len(1);
        e.string("events");
        e.array_len(2);
        e.i32(0);
        e.i64(0);
        e.i32(-1);
        e.nullable_string(Some(&long));
        e.i32(0);
        e.i64(0);
        e.i32(-1);
        e.nullable_string(None);
        let reply = call(addr, 8, 7, 4, &e.into_vec()).await.unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![
                (0, ErrorCode::OffsetMetadataTooLarge.as_i16()),
                (0, ErrorCode::RequestTimedOut.as_i16())
            ]
        );
    }

    /// Without a store, a commit is refused by name before the bridge is
    /// asked anything (review): the answer can be nothing else, and a busy
    /// bridge must not turn it into a timeout.
    #[tokio::test]
    async fn an_offsetless_commit_is_refused_before_the_bridge_is_asked() {
        let bridge = Arc::new(CountingTopicsBridge(
            MemoryBridge::with_topics(["events"]),
            std::sync::atomic::AtomicUsize::new(0),
        ));
        let (addr, _stop) = start_groups(Arc::clone(&bridge) as Arc<dyn Bridge>, None).await;
        let reply = call(
            addr,
            8,
            7,
            1,
            &offset_commit_body(7, "g", -1, "", "events", 0, 0, None),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![(0, ErrorCode::UnsupportedForMessageFormat.as_i16())]
        );
        assert_eq!(
            bridge.1.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "no enumeration for a commit that cannot be taken"
        );
    }

    /// An empty group id on OffsetFetch is refused per partition (review):
    /// v1 has no group-level field, so the code rides on every asked
    /// partition; v2+ carries it in both places.
    #[tokio::test]
    async fn an_empty_group_id_on_offset_fetch_is_refused_per_partition() {
        let (addr, _stop) = start_groups(
            Arc::new(MemoryBridge::with_topics(["events"])),
            Some(Arc::new(crate::offsets::MemoryOffsetStore::default())),
        )
        .await;
        for version in [1i16, 5] {
            let reply = call(
                addr,
                9,
                version,
                i32::from(version),
                &offset_fetch_body(version, "", Some(&[("events", &[0])])),
            )
            .await
            .unwrap();
            let (error, topics) = read_offset_fetch(&reply, version);
            assert_eq!(
                topics,
                vec![(
                    "events".to_owned(),
                    vec![(0, -1, None, ErrorCode::InvalidGroupId.as_i16())]
                )],
                "v{version}"
            );
            if version >= 2 {
                assert_eq!(error, ErrorCode::InvalidGroupId.as_i16());
            }
        }
    }

    /// A bridge whose topic enumeration stalls (review): a commit and a fetch
    /// are `REQUEST_TIMED_OUT` at the cap, every partition of them, not held
    /// for as long as the bridge takes.
    struct StalledTopicsBridge(MemoryBridge);
    impl Bridge for StalledTopicsBridge {
        fn topics(&self) -> Vec<String> {
            std::thread::sleep(Duration::from_secs(2));
            self.0.topics()
        }
        fn produce(
            &self,
            topic: &str,
            batches: &[RecordBatch],
            sequenced: Option<Sequenced>,
        ) -> Result<crate::bridge::Appended, ErrorCode> {
            self.0.produce(topic, batches, sequenced)
        }
        fn fetch(&self, topic: &str, offset: i64, max_bytes: usize) -> Result<Fetched, ErrorCode> {
            self.0.fetch(topic, offset, max_bytes)
        }
        fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
            self.0.bounds(topic)
        }
    }
    #[tokio::test]
    async fn a_stalled_topic_enumeration_is_timed_out_at_the_cap() {
        let (addr, _stop) = start_groups_tuned(
            Arc::new(StalledTopicsBridge(MemoryBridge::with_topics(["events"]))),
            Some(Arc::new(crate::offsets::MemoryOffsetStore::default())),
            |c| c.max_offset_wait = Duration::from_millis(100),
        )
        .await;
        let started = tokio::time::Instant::now();
        let reply = call(
            addr,
            8,
            7,
            1,
            &offset_commit_body(7, "g", -1, "", "events", 0, 0, None),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![(0, ErrorCode::RequestTimedOut.as_i16())]
        );
        // A stale membership is answered as such, not as the bridge's
        // timeout (review).
        let reply = call(
            addr,
            8,
            7,
            3,
            &offset_commit_body(7, "g", 7, "someone", "events", 0, 0, None),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![(0, ErrorCode::IllegalGeneration.as_i16())]
        );
        let reply = call(
            addr,
            9,
            5,
            2,
            &offset_fetch_body(5, "g", Some(&[("events", &[0])])),
        )
        .await
        .unwrap();
        // A fetch is the store's answer and never asks the bridge (review):
        // nothing committed, at once.
        assert_eq!(
            read_offset_fetch(&reply, 5),
            (0, vec![("events".to_owned(), vec![(0, -1, None, 0)])])
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "bounded by the cap, not by the bridge"
        );
    }

    /// A store whose first write for group "g" is slow (review): the group's
    /// commits take turns, so the slow one lands first and the one behind it
    /// last — never overtaken — while another group's commit is not held
    /// behind it.
    #[derive(Default)]
    struct SlowFirstStore {
        landed: std::sync::Mutex<Vec<(String, i64)>>,
        slowed: std::sync::atomic::AtomicBool,
    }
    #[async_trait::async_trait]
    impl OffsetStore for SlowFirstStore {
        async fn commit(
            &self,
            group: &str,
            _: &str,
            _: i32,
            committed: crate::offsets::Committed,
        ) -> Result<(), ErrorCode> {
            if group == "g" && !self.slowed.swap(true, std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
            self.landed
                .lock()
                .unwrap()
                .push((group.to_owned(), committed.offset));
            Ok(())
        }
        async fn fetch(
            &self,
            group: &str,
            _: &str,
            _: i32,
        ) -> Result<Option<crate::offsets::Committed>, ErrorCode> {
            Ok(self
                .landed
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|(g, _)| g == group)
                .map(|(_, offset)| crate::offsets::Committed {
                    offset: *offset,
                    metadata: None,
                }))
        }
        async fn committed(
            &self,
            _: &str,
            _: usize,
        ) -> Result<Vec<(String, i32, crate::offsets::Committed)>, ErrorCode> {
            Ok(Vec::new())
        }
    }
    #[tokio::test]
    async fn a_groups_commits_take_turns_and_another_groups_do_not_wait() {
        let store = Arc::new(SlowFirstStore::default());
        let bridge = Arc::new(MemoryBridge::with_topics(["events"]));
        let (addr, _stop) =
            start_groups(bridge, Some(Arc::clone(&store) as Arc<dyn OffsetStore>)).await;
        // Fifty records, so the commits below are positions that exist.
        let values: Vec<String> = (0..50).map(|i| format!("v{i}")).collect();
        let refs: Vec<&str> = values.iter().map(String::as_str).collect();
        let reply = call(
            addr,
            0,
            8,
            100,
            &produce_body("events", 0, -1, None, &batch_bytes(&refs, false)),
        )
        .await
        .unwrap();
        assert_eq!(read_produce(&reply, 8).0, 0);
        let slow = {
            let body = offset_commit_body(7, "g", -1, "", "events", 0, 10, None);
            tokio::spawn(async move { call(addr, 8, 7, 1, &body).await.unwrap() })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = tokio::time::Instant::now();
        let behind = {
            let body = offset_commit_body(7, "g", -1, "", "events", 0, 20, None);
            tokio::spawn(async move { call(addr, 8, 7, 2, &body).await.unwrap() })
        };
        let other = call(
            addr,
            8,
            7,
            3,
            &offset_commit_body(7, "h", -1, "", "events", 0, 5, None),
        )
        .await
        .unwrap();
        assert_eq!(read_offset_commit(&other, 7), vec![(0, 0)]);
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "another group's turn is its own"
        );
        assert_eq!(read_offset_commit(&slow.await.unwrap(), 7), vec![(0, 0)]);
        assert_eq!(read_offset_commit(&behind.await.unwrap(), 7), vec![(0, 0)]);
        let landed: Vec<i64> = store
            .landed
            .lock()
            .unwrap()
            .iter()
            .filter(|(g, _)| g == "g")
            .map(|(_, offset)| *offset)
            .collect();
        assert_eq!(landed, vec![10, 20], "in request order: the slow one first");
        let reply = call(
            addr,
            9,
            5,
            4,
            &offset_fetch_body(5, "g", Some(&[("events", &[0])])),
        )
        .await
        .unwrap();
        assert_eq!(read_offset_fetch(&reply, 5).1[0].1[0].1, 20);
    }

    /// A store whose write lands late: the request abandons its wait at the
    /// cap, the write is driven to its end, and the drain waits for it.
    #[derive(Default)]
    struct LateStore(std::sync::Mutex<Vec<i64>>);
    #[async_trait::async_trait]
    impl OffsetStore for LateStore {
        async fn commit(
            &self,
            _: &str,
            _: &str,
            _: i32,
            committed: crate::offsets::Committed,
        ) -> Result<(), ErrorCode> {
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            self.0.lock().unwrap().push(committed.offset);
            Ok(())
        }
        async fn fetch(
            &self,
            _: &str,
            _: &str,
            _: i32,
        ) -> Result<Option<crate::offsets::Committed>, ErrorCode> {
            Ok(None)
        }
        async fn committed(
            &self,
            _: &str,
            _: usize,
        ) -> Result<Vec<(String, i32, crate::offsets::Committed)>, ErrorCode> {
            Ok(Vec::new())
        }
    }

    /// The drain waits for an abandoned write to END (review), not merely
    /// for the offset ceiling: with produce, fetch and drain ceilings of
    /// 50 ms and an offset ceiling of 200 ms, a write that takes 1.5 s is
    /// abandoned by its request at the cap and `serve` returns only once it
    /// has landed — never with the store still inside the write.
    #[tokio::test]
    async fn the_drain_waits_for_an_abandoned_write_to_end() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::watch::channel(false);
        let config = GatewayConfig {
            advertised_port: addr.port() as i32,
            max_produce_wait: Duration::from_millis(50),
            max_fetch_wait: Duration::from_millis(50),
            drain_timeout: Duration::from_millis(50),
            max_offset_wait: Duration::from_millis(200),
            ..GatewayConfig::default()
        };
        let store = Arc::new(LateStore::default());
        let gateway = Gateway::new(Arc::new(MemoryBridge::with_topics(["events"])), config)
            .with_offsets(Arc::clone(&store) as Arc<dyn OffsetStore>);
        let serve = tokio::spawn(gateway.serve(listener, rx));
        let begun = tokio::time::Instant::now();
        let reply = call(
            addr,
            8,
            7,
            1,
            &offset_commit_body(7, "g", -1, "", "events", 0, 0, None),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![(0, ErrorCode::RequestTimedOut.as_i16())]
        );
        assert!(
            begun.elapsed() < Duration::from_secs(1),
            "abandoned at the cap"
        );
        tx.send(true).unwrap();
        serve.await.unwrap().unwrap();
        let ended = begun.elapsed();
        assert!(
            ended >= Duration::from_millis(1_400) && ended < Duration::from_secs(10),
            "returned only once the abandoned write ended: {ended:?}"
        );
        assert_eq!(
            *store.0.lock().unwrap(),
            vec![0],
            "the write landed before serve returned"
        );
    }

    /// A store that never answers (review): the commit and the fetch are
    /// `REQUEST_TIMED_OUT` at the gateway's offset cap, not held for good.
    struct HangingStore;
    #[async_trait::async_trait]
    impl OffsetStore for HangingStore {
        async fn commit(
            &self,
            _: &str,
            _: &str,
            _: i32,
            _: crate::offsets::Committed,
        ) -> Result<(), ErrorCode> {
            std::future::pending::<()>().await;
            Ok(())
        }
        async fn fetch(
            &self,
            _: &str,
            _: &str,
            _: i32,
        ) -> Result<Option<crate::offsets::Committed>, ErrorCode> {
            std::future::pending::<()>().await;
            Ok(None)
        }
        async fn committed(
            &self,
            _: &str,
            _: usize,
        ) -> Result<Vec<(String, i32, crate::offsets::Committed)>, ErrorCode> {
            std::future::pending::<()>().await;
            Ok(Vec::new())
        }
    }
    #[tokio::test]
    async fn a_store_that_never_answers_is_timed_out_at_the_cap() {
        let topics: Vec<String> = (0..10).map(|i| format!("t{i}")).collect();
        let mut served = vec!["events".to_owned()];
        served.extend(topics.iter().cloned());
        let (addr, _stop) = start_groups_tuned(
            Arc::new(MemoryBridge::with_topics(served)),
            Some(Arc::new(HangingStore)),
            |c| c.max_offset_wait = Duration::from_millis(100),
        )
        .await;
        let started = tokio::time::Instant::now();
        let reply = call(
            addr,
            8,
            7,
            1,
            &offset_commit_body(7, "g", -1, "", "events", 0, 0, None),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_commit(&reply, 7),
            vec![(0, ErrorCode::RequestTimedOut.as_i16())]
        );
        let reply = call(
            addr,
            9,
            5,
            2,
            &offset_fetch_body(5, "g", Some(&[("events", &[0])])),
        )
        .await
        .unwrap();
        assert_eq!(
            read_offset_fetch(&reply, 5).1[0].1[0].3,
            ErrorCode::RequestTimedOut.as_i16()
        );
        let reply = call(addr, 9, 5, 3, &offset_fetch_body(5, "g", None))
            .await
            .unwrap();
        assert_eq!(
            read_offset_fetch(&reply, 5).0,
            ErrorCode::RequestTimedOut.as_i16()
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "bounded by the cap, thrice"
        );
        // Many entries, one cap (review): the second and later entries find
        // the request's deadline spent and are timed out at once.
        let started = tokio::time::Instant::now();
        let many: Vec<(&str, i64)> = topics.iter().map(|t| (t.as_str(), 0)).collect();
        let reply = call(addr, 8, 7, 4, &offset_commit_many_body("g", &many))
            .await
            .unwrap();
        let answers = read_offset_commit(&reply, 7);
        assert_eq!(answers.len(), 10);
        assert_eq!(answers[0].1, ErrorCode::RequestTimedOut.as_i16());
        assert!(
            started.elapsed() < Duration::from_millis(1_500),
            "ten topics, one cap: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn every_batch_in_a_produce_set_is_appended_in_order() {
        let (addr, _stop) = start(Arc::new(MemoryBridge::with_topics(["events"]))).await;
        let mut set = batch_bytes(&["a", "b"], false);
        set.extend_from_slice(&batch_bytes(&["c"], false));
        let reply = call(addr, 0, 3, 1, &produce_body("events", 0, -1, None, &set))
            .await
            .unwrap();
        assert_eq!(
            read_produce(&reply, 3),
            (0, 0, None),
            "the first batch's base offset"
        );
        let reply = call(addr, 1, 4, 2, &fetch_body(4, "events", 0, 0, 0))
            .await
            .unwrap();
        let (_, hwm, records) = read_fetch(&reply, 4);
        assert_eq!(hwm, 3);
        let batches = decode_all(&records);
        assert_eq!((batches.len(), batches[1].base_offset), (2, 2));

        // A bad batch anywhere in the set appends nothing.
        let mut set = batch_bytes(&["d"], false);
        set.extend_from_slice(&batch_bytes(&["e"], true));
        let reply = call(addr, 0, 8, 3, &produce_body("events", 0, -1, None, &set))
            .await
            .unwrap();
        let (error, _, message) = read_produce(&reply, 8);
        assert_eq!(error, ErrorCode::InvalidRecord.as_i16());
        assert!(message.unwrap().starts_with("batch 1 record 0"));
        let reply = call(addr, 1, 4, 4, &fetch_body(4, "events", 0, 0, 0))
            .await
            .unwrap();
        assert_eq!(read_fetch(&reply, 4).1, 3, "nothing appended");
    }

    /// One byte budget per response (review): the second partition gets
    /// what the first left, and reports its watermark when nothing is left.
    #[tokio::test]
    async fn the_fetch_budget_is_spent_once_across_partitions() {
        let bridge = Arc::new(MemoryBridge::with_topics(["a", "b"]));
        let (addr, _stop) = start(bridge).await;
        let one = batch_bytes(&["0123456789"], false);
        call(addr, 0, 3, 1, &produce_body("a", 0, -1, None, &one))
            .await
            .unwrap();
        call(addr, 0, 3, 2, &produce_body("b", 0, -1, None, &one))
            .await
            .unwrap();
        let body = fetch_body_sized(4, &[("a", 0, 0), ("b", 0, 0)], 0, 1, one.len() as i32);
        let reply = call(addr, 1, 4, 3, &body).await.unwrap();
        let parts = read_fetch_all(&reply, 4);
        assert_eq!(parts.len(), 2);
        assert_eq!(
            (parts[0].0, parts[0].1, parts[0].2.len()),
            (0, 1, one.len())
        );
        assert_eq!(
            (parts[1].0, parts[1].1, parts[1].2.len()),
            (0, 1, 0),
            "budget spent: watermark only"
        );
        let body = fetch_body_sized(4, &[("a", 0, 0), ("b", 0, 0)], 0, 1, 2 * one.len() as i32);
        let reply = call(addr, 1, 4, 4, &body).await.unwrap();
        assert!(read_fetch_all(&reply, 4)
            .iter()
            .all(|p| p.2.len() == one.len()));
    }

    /// One deadline for the whole request (review): two slow partitions do
    /// not take two budgets.
    #[tokio::test]
    async fn a_produce_request_has_one_deadline_across_its_partitions() {
        struct Slow(MemoryBridge);
        impl Bridge for Slow {
            fn topics(&self) -> Vec<String> {
                self.0.topics()
            }
            fn produce(
                &self,
                topic: &str,
                batches: &[RecordBatch],
                sequenced: Option<Sequenced>,
            ) -> Result<crate::bridge::Appended, ErrorCode> {
                std::thread::sleep(Duration::from_millis(300));
                self.0.produce(topic, batches, sequenced)
            }
            fn fetch(
                &self,
                topic: &str,
                offset: i64,
                max_bytes: usize,
            ) -> Result<Fetched, ErrorCode> {
                self.0.fetch(topic, offset, max_bytes)
            }
            fn bounds(&self, topic: &str) -> Result<(i64, i64), ErrorCode> {
                self.0.bounds(topic)
            }
        }
        let (addr, _stop) = start(Arc::new(Slow(MemoryBridge::with_topics(["a", "b"])))).await;
        let one = batch_bytes(&["x"], false);
        let mut body = Encoder::new();
        body.nullable_string(None);
        body.i16(-1);
        body.i32(200); // timeout_ms
        body.array_len(2);
        for topic in ["a", "b"] {
            body.string(topic);
            body.array_len(1);
            body.i32(0);
            body.nullable_bytes(Some(&one));
        }
        let started = std::time::Instant::now();
        let reply = call(addr, 0, 8, 1, body.as_slice()).await.unwrap();
        let held = started.elapsed();
        assert!(
            held < Duration::from_millis(500),
            "one budget, not one per partition: {held:?}"
        );
        let mut d = Decoder::new(&reply);
        assert_eq!(d.array_len("topics").unwrap(), Some(2));
        let mut codes = Vec::new();
        for _ in 0..2 {
            d.string("topic").unwrap();
            d.array_len("partitions").unwrap();
            d.i32("index").unwrap();
            codes.push(d.i16("error").unwrap());
            d.i64("base").unwrap();
            d.i64("append_time").unwrap();
            d.i64("log_start").unwrap();
            d.array_len("record_errors").unwrap();
            d.nullable_string("message").unwrap();
        }
        assert_eq!(codes, vec![ErrorCode::RequestTimedOut.as_i16(); 2]);
    }

    /// An empty batch is refused (review): no backend answers it with an
    /// offset it never took.
    #[tokio::test]
    async fn an_empty_batch_is_refused() {
        let (addr, _stop) = start(Arc::new(MemoryBridge::with_topics(["events"]))).await;
        let empty = RecordBatch::encode(0, -1, -1, -1, &[]);
        let reply = call(addr, 0, 8, 1, &produce_body("events", 0, -1, None, &empty))
            .await
            .unwrap();
        let (error, _, message) = read_produce(&reply, 8);
        assert_eq!(error, ErrorCode::InvalidRecord.as_i16());
        assert!(message.unwrap().contains("no records"));
    }

    /// With the budget spent, an offset outside the log is still refused
    /// (review), not answered as an empty success.
    #[tokio::test]
    async fn a_spent_budget_does_not_mask_an_offset_out_of_range() {
        let bridge = Arc::new(MemoryBridge::with_topics(["a", "b"]));
        let (addr, _stop) = start(bridge).await;
        let one = batch_bytes(&["0123456789"], false);
        call(addr, 0, 3, 1, &produce_body("a", 0, -1, None, &one))
            .await
            .unwrap();
        call(addr, 0, 3, 2, &produce_body("b", 0, -1, None, &one))
            .await
            .unwrap();
        let body = fetch_body_sized(4, &[("a", 0, 0), ("b", 0, 9)], 0, 1, one.len() as i32);
        let reply = call(addr, 1, 4, 3, &body).await.unwrap();
        let parts = read_fetch_all(&reply, 4);
        assert_eq!(parts[0].0, 0);
        assert_eq!(parts[1].0, ErrorCode::OffsetOutOfRange.as_i16());
    }

    /// `min_bytes` gates the poll (review): above what exists it holds to
    /// the deadline and answers with what there is; zero answers at once.
    #[tokio::test]
    async fn min_bytes_holds_the_poll_and_zero_does_not() {
        let (addr, _stop) = start(Arc::new(MemoryBridge::with_topics(["events"]))).await;
        call(
            addr,
            0,
            3,
            1,
            &produce_body("events", 0, -1, None, &batch_bytes(&["a"], false)),
        )
        .await
        .unwrap();
        let started = std::time::Instant::now();
        let body = fetch_body_sized(4, &[("events", 0, 0)], 200, 1 << 20, 1 << 20);
        let reply = call(addr, 1, 4, 2, &body).await.unwrap();
        assert!(
            !read_fetch(&reply, 4).2.is_empty(),
            "what there is, at the deadline"
        );
        assert!(started.elapsed() >= Duration::from_millis(150));
        let started = std::time::Instant::now();
        let body = fetch_body_sized(4, &[("events", 0, 1)], 2_000, 0, 1 << 20);
        let reply = call(addr, 1, 4, 3, &body).await.unwrap();
        assert!(read_fetch(&reply, 4).2.is_empty());
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "min_bytes 0 returns at once"
        );
    }

    #[tokio::test]
    async fn a_fetch_at_the_watermark_long_polls_and_wakes_on_a_produce() {
        let bridge = Arc::new(MemoryBridge::with_topics(["events"]));
        let (addr, _stop) = start(bridge).await;
        call(
            addr,
            0,
            3,
            1,
            &produce_body("events", 0, -1, None, &batch_bytes(&["a"], false)),
        )
        .await
        .unwrap();

        // Nothing at offset 1: the fetch holds for max_wait, then answers empty.
        let started = std::time::Instant::now();
        let reply = call(addr, 1, 4, 2, &fetch_body(4, "events", 0, 1, 200))
            .await
            .unwrap();
        let (error, hwm, records) = read_fetch(&reply, 4);
        assert_eq!((error, hwm, records.len()), (0, 1, 0));
        let held = started.elapsed();
        assert!(
            held >= Duration::from_millis(150) && held < Duration::from_millis(600),
            "{held:?}"
        );

        // A produce during the poll wakes it before max_wait.
        let producer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            call(
                addr,
                0,
                3,
                3,
                &produce_body("events", 0, -1, None, &batch_bytes(&["b"], false)),
            )
            .await
            .unwrap();
        });
        let started = std::time::Instant::now();
        let reply = call(addr, 1, 4, 4, &fetch_body(4, "events", 0, 1, 2_000))
            .await
            .unwrap();
        let (_, hwm, records) = read_fetch(&reply, 4);
        assert_eq!(hwm, 2);
        assert_eq!(
            decode_all(&records)[0].records[0].value.as_deref(),
            Some(b"b".as_slice())
        );
        assert!(
            started.elapsed() < Duration::from_millis(1_000),
            "woken by the produce, not the cap"
        );
        producer.await.unwrap();

        // Beyond the watermark is out of range, at once.
        let reply = call(addr, 1, 4, 5, &fetch_body(4, "events", 0, 9, 200))
            .await
            .unwrap();
        assert_eq!(
            read_fetch(&reply, 4).0,
            ErrorCode::OffsetOutOfRange.as_i16()
        );
    }

    #[tokio::test]
    async fn every_gap_is_refused_by_name() {
        let (addr, _stop) = start(Arc::new(MemoryBridge::with_topics(["events"]))).await;
        let plain = batch_bytes(&["a"], false);

        // Headers have nowhere to go: refused, not dropped.
        let reply = call(
            addr,
            0,
            8,
            1,
            &produce_body("events", 0, -1, None, &batch_bytes(&["a"], true)),
        )
        .await
        .unwrap();
        let (error, _, message) = read_produce(&reply, 8);
        assert_eq!(error, ErrorCode::InvalidRecord.as_i16());
        assert!(message.unwrap().contains("header"));

        // Transactions are out of scope.
        let reply = call(
            addr,
            0,
            8,
            2,
            &produce_body("events", 0, -1, Some("tx-1"), &plain),
        )
        .await
        .unwrap();
        let (error, _, message) = read_produce(&reply, 8);
        assert_eq!(error, ErrorCode::UnsupportedForMessageFormat.as_i16());
        assert!(message.unwrap().contains("transactional"));

        // A compressed batch is refused with the compression code, not as corrupt.
        let mut compressed = plain.clone();
        compressed[21..23].copy_from_slice(&1_i16.to_be_bytes()); // gzip
        let crc = crc32c(&compressed[21..]);
        compressed[17..21].copy_from_slice(&crc.to_be_bytes());
        let reply = call(
            addr,
            0,
            3,
            3,
            &produce_body("events", 0, -1, None, &compressed),
        )
        .await
        .unwrap();
        assert_eq!(
            read_produce(&reply, 3).0,
            ErrorCode::UnsupportedCompressionType.as_i16()
        );

        // A corrupt batch is corrupt.
        let mut corrupt = plain.clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;
        let reply = call(
            addr,
            0,
            3,
            4,
            &produce_body("events", 0, -1, None, &corrupt),
        )
        .await
        .unwrap();
        assert_eq!(
            read_produce(&reply, 3).0,
            ErrorCode::CorruptMessage.as_i16()
        );

        // One partition per topic; an unknown topic is unknown.
        let reply = call(addr, 0, 3, 5, &produce_body("events", 1, -1, None, &plain))
            .await
            .unwrap();
        assert_eq!(
            read_produce(&reply, 3).0,
            ErrorCode::UnknownTopicOrPartition.as_i16()
        );
        let reply = call(addr, 0, 3, 6, &produce_body("nope", 0, -1, None, &plain))
            .await
            .unwrap();
        assert_eq!(
            read_produce(&reply, 3).0,
            ErrorCode::UnknownTopicOrPartition.as_i16()
        );

        // By-timestamp: LATEST and EARLIEST are served, nothing else.
        for timestamp in [1_700_000_000_000_i64] {
            let mut body = Encoder::new();
            body.i32(-1);
            body.array_len(1);
            body.string("events");
            body.array_len(1);
            body.i32(0);
            body.i64(timestamp);
            let reply = call(addr, 2, 1, 7, body.as_slice()).await.unwrap();
            let mut d = Decoder::new(&reply);
            d.array_len("topics").unwrap();
            d.string("topic").unwrap();
            d.array_len("partitions").unwrap();
            d.i32("index").unwrap();
            assert_eq!(
                d.i16("error").unwrap(),
                ErrorCode::UnsupportedVersion.as_i16()
            );
        }

        // Closed, by name: acks=0, an api key outside phase 1, a version below
        // the served range.
        assert!(
            call(addr, 0, 3, 8, &produce_body("events", 0, 0, None, &plain))
                .await
                .is_none()
        );
        assert!(call(addr, 19, 0, 9, &[]).await.is_none());
        assert!(call(addr, 0, 2, 10, &[]).await.is_none());
        // And a frame above the bound.
        let mut socket = TcpStream::connect(addr).await.unwrap();
        socket.write_all(&(i32::MAX).to_be_bytes()).await.unwrap();
        let mut probe = [0_u8; 1];
        assert!(matches!(socket.read(&mut probe).await, Ok(0) | Err(_)));
    }

    /// Trailing bytes after a served body close the connection (review): a
    /// schema this gateway does not know is not served by accident.
    #[tokio::test]
    async fn a_body_with_trailing_bytes_is_refused() {
        let (addr, _stop) = start(Arc::new(MemoryBridge::with_topics(["events"]))).await;
        let mut body = Encoder::new();
        body.i32(-1); // every topic
        body.i8(7); // not Metadata v1's schema
        assert!(call(addr, 3, 1, 1, body.as_slice()).await.is_none());
        // The same body without the tail is served.
        let mut body = Encoder::new();
        body.i32(-1);
        assert!(call(addr, 3, 1, 2, body.as_slice()).await.is_some());
    }

    /// A shutdown already signalled stops the listener before its first
    /// accept (review).
    #[tokio::test]
    async fn a_shutdown_signalled_before_serve_stops_it_at_once() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (tx, rx) = tokio::sync::watch::channel(true);
        let gateway = Gateway::new(
            Arc::new(MemoryBridge::with_topics(["events"])),
            GatewayConfig::default(),
        );
        tokio::time::timeout(Duration::from_secs(2), gateway.serve(listener, rx))
            .await
            .expect("returns without waiting for a change")
            .unwrap();
        drop(tx);
    }

    #[tokio::test]
    async fn shutdown_stops_the_listener_and_a_dropped_sender_does_not() {
        let (addr, stop) = start(Arc::new(MemoryBridge::with_topics(["events"]))).await;
        assert!(call(addr, 18, 0, 1, &[]).await.is_some());
        stop.send(true).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            TcpStream::connect(addr).await.is_err(),
            "the listener is gone"
        );

        let (addr, stop) = start(Arc::new(MemoryBridge::with_topics(["events"]))).await;
        drop(stop);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(call(addr, 18, 0, 1, &[]).await.is_some(), "still serving");
    }
}

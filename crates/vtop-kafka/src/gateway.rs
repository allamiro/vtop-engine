//! The listener (#225): Kafka frames in, the bridge behind, and every gap a
//! refusal by name.
//!
//! What phase 1 serves is exactly what the engine can honestly back today:
//! Metadata over the bridge's topics with one partition each, Produce of
//! uncompressed non-transactional v2 batches without headers, Fetch with the
//! long-poll emulated here (the broker returns immediately at the watermark),
//! and ListOffsets LATEST. Everything else is refused with the code a client's
//! retry policy can act on, and the reason is logged where an operator reads —
//! never a silent drop, never a plausible lie.

use crate::api::{
    decode_fetch, decode_list_offsets, decode_metadata, decode_produce, encode_api_versions,
    encode_fetch, encode_list_offsets, encode_metadata, encode_produce, FetchPartitionResponse,
    FetchRequest, FetchTopicResponse, ListOffsetsPartitionResponse, ListOffsetsRequest,
    ListOffsetsTopicResponse, MetadataBroker, MetadataRequest, MetadataResponse, MetadataTopic,
    ProducePartitionResponse, ProduceRequest, ProduceTopicResponse, TIMESTAMP_LATEST,
};
use crate::bridge::{Bridge, Fetched};
use crate::messages::{
    frame, write_response_header, ApiKey, ErrorCode, HeaderVerdict, RequestHeader,
};
use crate::records::{BatchError, RecordBatch};
use crate::wire::{Decoder, Encoder};
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
        }
    }
}

/// A frame's buffer starts this large and grows with the bytes received.
const INITIAL_FRAME_CAPACITY: usize = 64 * 1024;

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

pub struct Gateway {
    bridge: Arc<dyn Bridge>,
    config: GatewayConfig,
    sessions: Arc<tokio::sync::Semaphore>,
    refused_sessions: std::sync::atomic::AtomicU64,
}

impl Gateway {
    pub fn new(bridge: Arc<dyn Bridge>, config: GatewayConfig) -> Self {
        let sessions = Arc::new(tokio::sync::Semaphore::new(config.max_sessions.max(1)));
        Self {
            bridge,
            config,
            sessions,
            refused_sessions: std::sync::atomic::AtomicU64::new(0),
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
        // The drain (review): every session sees the same signal between
        // frames and closes — one mid-request answers it first — and their
        // slots come back. Holding every slot is holding proof that no
        // request is in flight; past `drain_timeout` the embedder is told,
        // and proceeds on its own judgement.
        let slots = u32::try_from(gateway.config.max_sessions.max(1)).unwrap_or(u32::MAX);
        if tokio::time::timeout(
            gateway.config.drain_timeout,
            gateway.sessions.acquire_many(slots),
        )
        .await
        .is_err()
        {
            tracing::warn!(
                open = slots - u32::try_from(gateway.sessions.available_permits()).unwrap_or(0),
                "kafka gateway stopped with sessions still open past the drain timeout"
            );
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
            match tokio::time::timeout(self.config.frame_read_timeout, rest.read_to_end(&mut body))
                .await
            {
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
                Ok(request) => {
                    let response = self.metadata(request).await;
                    encode_metadata(&mut out, version, &response);
                    Ok(None)
                }
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
            tokio::task::spawn_blocking(move || bridge.bounds(&name)),
        )
        .await
    }

    async fn metadata(&self, request: MetadataRequest) -> MetadataResponse {
        let names = match request.topics {
            Some(names) => names,
            None => {
                let bridge = Arc::clone(&self.bridge);
                tokio::task::spawn_blocking(move || bridge.topics())
                    .await
                    .unwrap_or_default()
            }
        };
        let mut topics = Vec::with_capacity(names.len());
        for name in names {
            topics.push(match self.bounds(&name).await {
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
        MetadataResponse {
            brokers: vec![MetadataBroker {
                node_id: self.config.node_id,
                host: self.config.advertised_host.clone(),
                port: self.config.advertised_port,
            }],
            cluster_id: self.config.cluster_id.clone(),
            controller_id: self.config.node_id,
            topics,
        }
    }

    /// `Err` closes the connection: the one produce shape with no answer.
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
        let bridge = Arc::clone(&self.bridge);
        let topic = topic.to_owned();
        // The bridge is synchronous and a real one fsyncs: off the runtime's
        // threads, the way the broker's own session loop does it. The SET is
        // one append (review) — whole or not at all — and the wait for it
        // is bounded by the client's `timeout_ms` under the gateway's cap:
        // the append cannot be cancelled, so past the bound the client is
        // told it timed out while the append runs on.
        if tokio::time::Instant::now() >= deadline {
            return Err((
                ErrorCode::RequestTimedOut,
                "the request's deadline passed before this partition was reached".to_owned(),
            ));
        }
        let append = tokio::task::spawn_blocking(move || bridge.produce(&topic, &batches));
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
                            tokio::task::spawn_blocking(move || {
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
                            tokio::task::spawn_blocking(move || {
                                bridge.fetch(&name, offset, budget)
                            }),
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
                } else if partition.timestamp == TIMESTAMP_LATEST {
                    match self.bounds(&topic.name).await {
                        Ok((_, high_watermark)) => (ErrorCode::None, high_watermark),
                        Err(error) => (error, -1),
                    }
                } else {
                    // EARLIEST has no exact accessor and by-timestamp no
                    // index behind it: refused by name, not answered with a
                    // scan.
                    tracing::warn!(
                        topic = %topic.name,
                        timestamp = partition.timestamp,
                        "kafka ListOffsets refused: only LATEST (-1) is served in phase 1 (#225)"
                    );
                    (ErrorCode::UnsupportedVersion, -1)
                };
                partitions.push(ListOffsetsPartitionResponse {
                    index: partition.index,
                    error,
                    timestamp: partition.timestamp,
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
        ) -> Result<crate::bridge::Appended, ErrorCode> {
            self.inner.produce(topic, batches)
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
        assert_eq!(d.array_len("keys").unwrap(), Some(5));

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
            ) -> Result<crate::bridge::Appended, ErrorCode> {
                std::thread::sleep(Duration::from_millis(300));
                self.0.produce(topic, batches)
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

        // EARLIEST and by-timestamp: only LATEST is served.
        for timestamp in [-2_i64, 1_700_000_000_000] {
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

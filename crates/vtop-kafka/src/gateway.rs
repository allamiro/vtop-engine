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
        }
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
}

impl Gateway {
    pub fn new(bridge: Arc<dyn Bridge>, config: GatewayConfig) -> Self {
        Self { bridge, config }
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
        let mut watching = true;
        loop {
            tokio::select! {
                changed = shutdown.changed(), if watching => {
                    match changed {
                        Ok(()) if *shutdown.borrow() => return Ok(()),
                        Ok(()) => {}
                        Err(_) => watching = false,
                    }
                }
                accepted = listener.accept() => {
                    let (socket, peer) = accepted?;
                    let gateway = Arc::clone(&gateway);
                    tokio::spawn(async move {
                        if let Err(error) = gateway.session(socket).await {
                            tracing::debug!(%peer, %error, "kafka session ended");
                        }
                    });
                }
            }
        }
    }

    async fn session(self: Arc<Self>, mut socket: TcpStream) -> std::io::Result<()> {
        loop {
            let len = match socket.read_i32().await {
                Ok(len) => len,
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(error) => return Err(error),
            };
            if len < 0 || len as usize > self.config.max_frame_bytes {
                tracing::warn!(
                    len,
                    max = self.config.max_frame_bytes,
                    "kafka request frame refused: length outside the bound; connection closed"
                );
                return Ok(());
            }
            let mut body = vec![0_u8; len as usize];
            socket.read_exact(&mut body).await?;
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
            ApiKey::Metadata => decode_metadata(&mut d, version).map(|request| {
                let response = self.metadata(request);
                encode_metadata(&mut out, version, &response);
                None
            }),
            ApiKey::Produce => match decode_produce(&mut d, version) {
                Ok(request) => match self.produce(request).await {
                    Ok(topics) => {
                        encode_produce(&mut out, version, &topics);
                        Ok(None)
                    }
                    Err(close) => Ok(Some(close)),
                },
                Err(error) => Err(error),
            },
            ApiKey::Fetch => match decode_fetch(&mut d, version) {
                Ok(request) => {
                    let topics = self.fetch(request).await;
                    encode_fetch(&mut out, version, &topics);
                    Ok(None)
                }
                Err(error) => Err(error),
            },
            ApiKey::ListOffsets => decode_list_offsets(&mut d, version).map(|request| {
                let topics = self.list_offsets(request);
                encode_list_offsets(&mut out, version, &topics);
                None
            }),
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

    fn has_topic(&self, name: &str) -> bool {
        self.bridge.high_watermark(name).is_ok()
    }

    fn metadata(&self, request: MetadataRequest) -> MetadataResponse {
        let names = request.topics.unwrap_or_else(|| self.bridge.topics());
        let topics = names
            .into_iter()
            .map(|name| {
                if self.has_topic(&name) {
                    MetadataTopic {
                        error: ErrorCode::None,
                        name,
                        leader: Some(self.config.node_id),
                    }
                } else {
                    // Never created here, whatever `allow_auto_topic_creation`
                    // said: a topic is a range the metadata plane granted, not
                    // a name a producer typed.
                    MetadataTopic {
                        error: ErrorCode::UnknownTopicOrPartition,
                        name,
                        leader: None,
                    }
                }
            })
            .collect();
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
                        log_start_offset: 0,
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
        let batch = RecordBatch::decode(records).map_err(|error| {
            let code = match &error {
                BatchError::Compressed { .. } => ErrorCode::UnsupportedCompressionType,
                BatchError::Transactional | BatchError::Control => {
                    ErrorCode::UnsupportedForMessageFormat
                }
                BatchError::CrcMismatch { .. } => ErrorCode::CorruptMessage,
                BatchError::UnsupportedMagic { .. } => ErrorCode::UnsupportedForMessageFormat,
                _ => ErrorCode::InvalidRecord,
            };
            (code, error.to_string())
        })?;
        if let Some(index) = batch.records.iter().position(|r| !r.headers.is_empty()) {
            // Refused rather than dropped: a record's headers have nowhere to
            // go in the native log today, and losing them silently is not a
            // translation.
            return Err((
                ErrorCode::InvalidRecord,
                format!(
                    "record {index} carries {} header(s), and the native log has nowhere to keep \
                     them (#225): headers are refused rather than dropped",
                    batch.records[index].headers.len()
                ),
            ));
        }
        let bridge = Arc::clone(&self.bridge);
        let topic = topic.to_owned();
        // The bridge is synchronous and a real one fsyncs: off the runtime's
        // threads, the way the broker's own session loop does it.
        tokio::task::spawn_blocking(move || bridge.produce(&topic, &batch))
            .await
            .map_err(|join| {
                (
                    ErrorCode::RequestTimedOut,
                    format!("produce task failed: {join}"),
                )
            })?
            .map_err(|code| (code, format!("the bridge refused the append with {code:?}")))
    }

    async fn fetch(&self, request: FetchRequest) -> Vec<FetchTopicResponse> {
        // Read committed is served as read uncommitted, honestly: with no
        // transactions anywhere the stable offset IS the high watermark, so
        // the two isolation levels see the same records.
        let wait = Duration::from_millis(request.max_wait_ms.max(0) as u64)
            .min(self.config.max_fetch_wait);
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let mut topics = Vec::with_capacity(request.topics.len());
            let mut any_data = false;
            for topic in &request.topics {
                let mut partitions = Vec::with_capacity(topic.partitions.len());
                for partition in &topic.partitions {
                    let outcome = if partition.index != 0 {
                        Err(ErrorCode::UnknownTopicOrPartition)
                    } else {
                        let bridge = Arc::clone(&self.bridge);
                        let name = topic.name.clone();
                        let (offset, max_bytes) = (partition.fetch_offset, partition.max_bytes);
                        let budget =
                            usize::try_from(max_bytes.min(request.max_bytes).max(1)).unwrap_or(1);
                        tokio::task::spawn_blocking(move || bridge.fetch(&name, offset, budget))
                            .await
                            .unwrap_or(Err(ErrorCode::RequestTimedOut))
                    };
                    partitions.push(match outcome {
                        Ok(Fetched {
                            records,
                            high_watermark,
                            log_start_offset,
                        }) => {
                            any_data |= !records.is_empty();
                            FetchPartitionResponse {
                                index: partition.index,
                                error: ErrorCode::None,
                                high_watermark,
                                log_start_offset,
                                records,
                            }
                        }
                        Err(error) => {
                            any_data = true; // an error is an answer; do not wait on it
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
            // here, by asking again until there is data or the wait is up.
            if any_data || tokio::time::Instant::now() >= deadline {
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

    fn list_offsets(&self, request: ListOffsetsRequest) -> Vec<ListOffsetsTopicResponse> {
        request
            .topics
            .into_iter()
            .map(|topic| {
                let partitions = topic
                    .partitions
                    .into_iter()
                    .map(|partition| {
                        let (error, offset) = if partition.index != 0 {
                            (ErrorCode::UnknownTopicOrPartition, -1)
                        } else if partition.timestamp == TIMESTAMP_LATEST {
                            match self.bridge.high_watermark(&topic.name) {
                                Ok(offset) => (ErrorCode::None, offset),
                                Err(error) => (error, -1),
                            }
                        } else {
                            // EARLIEST has no exact accessor and by-timestamp
                            // no index behind it: refused by name, not
                            // answered with a scan.
                            tracing::warn!(
                                topic = %topic.name,
                                timestamp = partition.timestamp,
                                "kafka ListOffsets refused: only LATEST (-1) is served in phase 1 (#225)"
                            );
                            (ErrorCode::UnsupportedVersion, -1)
                        };
                        ListOffsetsPartitionResponse {
                            index: partition.index,
                            error,
                            timestamp: partition.timestamp,
                            offset,
                        }
                    })
                    .collect();
                ListOffsetsTopicResponse {
                    name: topic.name,
                    partitions,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::MemoryBridge;
    use crate::records::{crc32c, Record};
    use std::net::SocketAddr;

    async fn start(bridge: Arc<dyn Bridge>) -> (SocketAddr, tokio::sync::watch::Sender<bool>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::watch::channel(false);
        let gateway = Gateway::new(
            bridge,
            GatewayConfig {
                advertised_port: addr.port() as i32,
                max_fetch_wait: Duration::from_millis(500),
                fetch_poll_interval: Duration::from_millis(10),
                ..GatewayConfig::default()
            },
        );
        tokio::spawn(gateway.serve(listener, rx));
        (addr, tx)
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
        let mut e = Encoder::new();
        e.i32(-1);
        e.i32(max_wait_ms);
        e.i32(1);
        e.i32(1 << 20);
        e.i8(1); // read_committed: served as the watermark, honestly
        if version >= 7 {
            e.i32(0);
            e.i32(-1);
        }
        e.array_len(1);
        e.string(topic);
        e.array_len(1);
        e.i32(partition);
        if version >= 9 {
            e.i32(-1);
        }
        e.i64(offset);
        if version >= 5 {
            e.i64(0);
        }
        e.i32(1 << 16);
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
        let mut d = Decoder::new(reply);
        d.i32("throttle").unwrap();
        if version >= 7 {
            d.i16("error").unwrap();
            d.i32("session").unwrap();
        }
        d.array_len("topics").unwrap();
        d.string("topic").unwrap();
        d.array_len("partitions").unwrap();
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
        (error, hwm, records)
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

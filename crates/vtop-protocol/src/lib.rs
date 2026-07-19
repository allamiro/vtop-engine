//! Versioned, allocation-bounded wire contract for the native VTOP broker.
//!
//! This crate owns message identity and framing only. TLS authentication,
//! authorization, storage, and request scheduling belong to `vtop-broker`.

use std::io;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

pub const PROTOCOL_MAJOR: u16 = 1;
pub const PROTOCOL_MINOR: u16 = 0;
pub const HEADER_LEN: usize = 64;
pub const MIN_FRAME_BYTES: u32 = HEADER_LEN as u32;
pub const DEFAULT_MAX_FRAME_BYTES: u32 = 8 * 1024 * 1024;
pub const ABSOLUTE_MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;
pub const DEFAULT_MAX_RECORDS: u32 = 4096;
pub const ABSOLUTE_MAX_RECORDS: u32 = 65_536;
pub const MAX_WINDOW_BYTES: u32 = ABSOLUTE_MAX_FRAME_BYTES;
pub const MAX_TOPIC_BYTES: usize = 249;
pub const MAX_ERROR_BYTES: usize = 4096;

const MAGIC: &[u8; 4] = b"VTPW";
const CHECKSUM_OFFSET: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtocolLimits {
    pub max_frame_bytes: u32,
    pub max_records: u32,
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            max_records: DEFAULT_MAX_RECORDS,
        }
    }
}

impl ProtocolLimits {
    pub fn validate(self) -> Result<Self, ProtocolError> {
        if !(MIN_FRAME_BYTES..=ABSOLUTE_MAX_FRAME_BYTES).contains(&self.max_frame_bytes) {
            return Err(ProtocolError::Limit(format!(
                "max_frame_bytes must be in {MIN_FRAME_BYTES}..={ABSOLUTE_MAX_FRAME_BYTES}"
            )));
        }
        if self.max_records == 0 || self.max_records > ABSOLUTE_MAX_RECORDS {
            return Err(ProtocolError::Limit(format!(
                "max_records must be in 1..={ABSOLUTE_MAX_RECORDS}"
            )));
        }
        Ok(self)
    }

    fn max_payload_bytes(self) -> usize {
        self.max_frame_bytes as usize - HEADER_LEN
    }
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("wire I/O: {0}")]
    Io(#[from] io::Error),
    #[error("unsupported protocol version {major}.{minor}")]
    UnsupportedVersion { major: u16, minor: u16 },
    #[error("unknown message kind {0}")]
    UnknownKind(u16),
    #[error("invalid wire frame: {0}")]
    InvalidFrame(String),
    #[error("protocol limit exceeded: {0}")]
    Limit(String),
    #[error("wire frame checksum mismatch")]
    ChecksumMismatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Role {
    Producer = 1,
    Consumer = 2,
    Peer = 3,
    Administrator = 4,
}

impl Role {
    fn decode(value: u8) -> Result<Self, ProtocolError> {
        match value {
            1 => Ok(Self::Producer),
            2 => Ok(Self::Consumer),
            3 => Ok(Self::Peer),
            4 => Ok(Self::Administrator),
            _ => Err(ProtocolError::InvalidFrame(format!(
                "unknown client role {value}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Durability {
    Buffered = 1,
    LocalFsync = 2,
}

impl Durability {
    fn decode(value: u8) -> Result<Self, ProtocolError> {
        match value {
            1 => Ok(Self::Buffered),
            2 => Ok(Self::LocalFsync),
            _ => Err(ProtocolError::InvalidFrame(format!(
                "unknown durability {value}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorCode {
    InvalidRequest = 1,
    UnsupportedVersion = 2,
    Unauthorized = 3,
    WrongCluster = 4,
    WrongRange = 5,
    Fenced = 6,
    SequenceConflict = 7,
    Overloaded = 8,
    Storage = 9,
    ProtocolViolation = 10,
}

impl ErrorCode {
    fn decode(value: u16) -> Result<Self, ProtocolError> {
        match value {
            1 => Ok(Self::InvalidRequest),
            2 => Ok(Self::UnsupportedVersion),
            3 => Ok(Self::Unauthorized),
            4 => Ok(Self::WrongCluster),
            5 => Ok(Self::WrongRange),
            6 => Ok(Self::Fenced),
            7 => Ok(Self::SequenceConflict),
            8 => Ok(Self::Overloaded),
            9 => Ok(Self::Storage),
            10 => Ok(Self::ProtocolViolation),
            _ => Err(ProtocolError::InvalidFrame(format!(
                "unknown error code {value}"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientHello {
    pub cluster_id: Uuid,
    pub principal_id: Uuid,
    pub role: Role,
    pub minimum_major: u16,
    pub maximum_major: u16,
    pub requested_max_frame_bytes: u32,
    pub requested_max_records: u32,
    pub requested_max_inflight_requests: u32,
    pub initial_window_bytes: u64,
    pub session_nonce: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerHello {
    pub cluster_id: Uuid,
    pub node_id: Uuid,
    pub selected_major: u16,
    pub selected_minor: u16,
    pub max_frame_bytes: u32,
    pub max_records: u32,
    pub max_inflight_requests: u32,
    pub initial_window_bytes: u64,
    pub session_nonce: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeIdentity {
    pub topic: String,
    pub topic_epoch: u64,
    pub range_id: Uuid,
    pub range_generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProduceRecord {
    pub timestamp_millis: i64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProduceRequest {
    pub range: RangeIdentity,
    /// Range-leader fencing epoch. A broker rejects any value other than its
    /// current grant before touching producer or segment state.
    pub fencing_epoch: u64,
    pub producer_id: Uuid,
    pub producer_epoch: u64,
    pub first_sequence: u64,
    pub durability: Durability,
    pub records: Vec<ProduceRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProduceOutcome {
    pub offset: u64,
    pub duplicate: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProduceResponse {
    pub outcomes: Vec<ProduceOutcome>,
    pub committed_next_offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchRequest {
    pub range: RangeIdentity,
    pub fencing_epoch: u64,
    pub start_offset: u64,
    pub max_bytes: u32,
    pub max_records: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchedRecord {
    pub offset: u64,
    pub timestamp_millis: i64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchResponse {
    pub records: Vec<FetchedRecord>,
    pub next_offset: u64,
    pub committed_high_watermark: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowUpdate {
    pub additional_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorResponse {
    pub code: ErrorCode,
    pub retryable: bool,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    ClientHello(ClientHello),
    ServerHello(ServerHello),
    ProduceRequest(ProduceRequest),
    ProduceResponse(ProduceResponse),
    FetchRequest(FetchRequest),
    FetchResponse(FetchResponse),
    WindowUpdate(WindowUpdate),
    Error(ErrorResponse),
    Ping,
    Pong,
}

impl Message {
    fn kind(&self) -> u16 {
        match self {
            Self::ClientHello(_) => 1,
            Self::ServerHello(_) => 2,
            Self::ProduceRequest(_) => 10,
            Self::ProduceResponse(_) => 11,
            Self::FetchRequest(_) => 20,
            Self::FetchResponse(_) => 21,
            Self::WindowUpdate(_) => 30,
            Self::Error(_) => 40,
            Self::Ping => 50,
            Self::Pong => 51,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireFrame {
    pub request_id: u64,
    pub stream_id: u64,
    pub message: Message,
}

pub fn encode_frame(frame: &WireFrame, limits: ProtocolLimits) -> Result<Vec<u8>, ProtocolError> {
    let limits = limits.validate()?;
    let payload_size = encoded_payload_size(&frame.message, limits)?;
    let frame_size = HEADER_LEN
        .checked_add(payload_size)
        .ok_or_else(|| ProtocolError::Limit("frame length overflow".to_owned()))?;
    if frame_size > limits.max_frame_bytes as usize {
        return Err(ProtocolError::Limit(format!(
            "frame is {frame_size} bytes; negotiated maximum is {}",
            limits.max_frame_bytes
        )));
    }
    // Size the exact output before copying caller-owned record bodies. This
    // prevents an oversized outbound value from being materialized and only
    // rejected after the allocation has already crossed the negotiated cap.
    let mut encoded = Vec::with_capacity(frame_size);
    encoded.resize(HEADER_LEN, 0);
    encode_message(&frame.message, &mut encoded)?;
    let payload_len = u32::try_from(encoded.len() - HEADER_LEN)
        .map_err(|_| ProtocolError::Limit("payload length exceeds u32".to_owned()))?;
    encoded[0..4].copy_from_slice(MAGIC);
    encoded[4..6].copy_from_slice(&PROTOCOL_MAJOR.to_be_bytes());
    encoded[6..8].copy_from_slice(&PROTOCOL_MINOR.to_be_bytes());
    encoded[8..10].copy_from_slice(&frame.message.kind().to_be_bytes());
    encoded[10..12].copy_from_slice(&0_u16.to_be_bytes());
    encoded[12..20].copy_from_slice(&frame.request_id.to_be_bytes());
    encoded[20..28].copy_from_slice(&frame.stream_id.to_be_bytes());
    encoded[28..32].copy_from_slice(&payload_len.to_be_bytes());
    let mut hasher = blake3::Hasher::new();
    hasher.update(&encoded[..CHECKSUM_OFFSET]);
    hasher.update(&encoded[HEADER_LEN..]);
    encoded[CHECKSUM_OFFSET..HEADER_LEN].copy_from_slice(hasher.finalize().as_bytes());
    Ok(encoded)
}

fn encoded_payload_size(message: &Message, limits: ProtocolLimits) -> Result<usize, ProtocolError> {
    fn add(total: &mut usize, value: usize) -> Result<(), ProtocolError> {
        *total = total
            .checked_add(value)
            .ok_or_else(|| ProtocolError::Limit("payload length overflow".to_owned()))?;
        Ok(())
    }
    fn bytes_size(value: &[u8]) -> Result<usize, ProtocolError> {
        let _ = u32::try_from(value.len())
            .map_err(|_| ProtocolError::Limit("byte string exceeds u32".to_owned()))?;
        4_usize
            .checked_add(value.len())
            .ok_or_else(|| ProtocolError::Limit("byte string length overflow".to_owned()))
    }
    fn range_size(range: &RangeIdentity) -> Result<usize, ProtocolError> {
        validate_topic(&range.topic)?;
        bytes_size(range.topic.as_bytes())?
            .checked_add(8 + 16 + 8)
            .ok_or_else(|| ProtocolError::Limit("range length overflow".to_owned()))
    }
    fn record_count(count: usize, limits: ProtocolLimits) -> Result<(), ProtocolError> {
        let count = u32::try_from(count)
            .map_err(|_| ProtocolError::Limit("record count exceeds u32".to_owned()))?;
        if count > limits.max_records {
            return Err(ProtocolError::Limit(format!(
                "record count is {count}; negotiated maximum is {}",
                limits.max_records
            )));
        }
        Ok(())
    }

    let total =
        match message {
            Message::ClientHello(_) => 16 + 16 + 1 + 2 + 2 + 4 + 4 + 4 + 8 + 32,
            Message::ServerHello(_) => 16 + 16 + 2 + 2 + 4 + 4 + 4 + 8 + 32,
            Message::WindowUpdate(_) => 8,
            Message::Ping | Message::Pong => 0,
            Message::Error(value) => {
                if value.message.len() > MAX_ERROR_BYTES {
                    return Err(ProtocolError::Limit(format!(
                        "error message exceeds {MAX_ERROR_BYTES} bytes"
                    )));
                }
                2 + 1 + bytes_size(value.message.as_bytes())?
            }
            Message::ProduceRequest(value) => {
                if value.records.is_empty() {
                    return Err(ProtocolError::InvalidFrame(
                        "produce request has no records".to_owned(),
                    ));
                }
                record_count(value.records.len(), limits)?;
                let mut size = range_size(&value.range)? + 8 + 16 + 8 + 8 + 1 + 4;
                for record in &value.records {
                    add(&mut size, 8)?;
                    add(&mut size, bytes_size(&record.key)?)?;
                    add(&mut size, bytes_size(&record.value)?)?;
                }
                size
            }
            Message::ProduceResponse(value) => {
                record_count(value.outcomes.len(), limits)?;
                4_usize
                    .checked_add(value.outcomes.len().checked_mul(9).ok_or_else(|| {
                        ProtocolError::Limit("produce response overflow".to_owned())
                    })?)
                    .and_then(|size| size.checked_add(8))
                    .ok_or_else(|| ProtocolError::Limit("produce response overflow".to_owned()))?
            }
            Message::FetchRequest(value) => {
                if value.max_bytes == 0
                    || value.max_records == 0
                    || value.max_records > limits.max_records
                {
                    return Err(ProtocolError::Limit(format!(
                        "fetch limits must be non-zero and max_records must not exceed {}",
                        limits.max_records
                    )));
                }
                range_size(&value.range)? + 8 + 8 + 4 + 4
            }
            Message::FetchResponse(value) => {
                record_count(value.records.len(), limits)?;
                let mut size = 4 + 8 + 8;
                for record in &value.records {
                    add(&mut size, 8 + 8)?;
                    add(&mut size, bytes_size(&record.key)?)?;
                    add(&mut size, bytes_size(&record.value)?)?;
                }
                size
            }
        };
    if total > limits.max_payload_bytes() {
        return Err(ProtocolError::Limit(format!(
            "payload is {total} bytes; negotiated maximum is {}",
            limits.max_payload_bytes()
        )));
    }
    Ok(total)
}

pub fn decode_frame(encoded: &[u8], limits: ProtocolLimits) -> Result<WireFrame, ProtocolError> {
    let limits = limits.validate()?;
    if encoded.len() < HEADER_LEN {
        return Err(ProtocolError::InvalidFrame(
            "truncated frame header".to_owned(),
        ));
    }
    if encoded.len() > limits.max_frame_bytes as usize {
        return Err(ProtocolError::Limit(format!(
            "frame is {} bytes; negotiated maximum is {}",
            encoded.len(),
            limits.max_frame_bytes
        )));
    }
    let header = decode_header(&encoded[..HEADER_LEN], limits)?;
    if encoded.len() != HEADER_LEN + header.payload_len {
        return Err(ProtocolError::InvalidFrame(format!(
            "declared payload is {} bytes but frame contains {}",
            header.payload_len,
            encoded.len() - HEADER_LEN
        )));
    }
    verify_checksum(&encoded[..HEADER_LEN], &encoded[HEADER_LEN..])?;
    let mut decoder = Decoder::new(&encoded[HEADER_LEN..]);
    let message = decode_message(header.kind, &mut decoder, limits)?;
    decoder.finish()?;
    Ok(WireFrame {
        request_id: header.request_id,
        stream_id: header.stream_id,
        message,
    })
}

pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    limits: ProtocolLimits,
) -> Result<Option<WireFrame>, ProtocolError> {
    let limits = limits.validate()?;
    let mut header = [0_u8; HEADER_LEN];
    let mut first = [0_u8; 1];
    match reader.read(&mut first).await? {
        0 => return Ok(None),
        1 => header[0] = first[0],
        _ => unreachable!(),
    }
    reader.read_exact(&mut header[1..]).await?;
    let decoded = decode_header(&header, limits)?;
    let mut payload = vec![0_u8; decoded.payload_len];
    reader.read_exact(&mut payload).await?;
    verify_checksum(&header, &payload)?;
    let mut decoder = Decoder::new(&payload);
    let message = decode_message(decoded.kind, &mut decoder, limits)?;
    decoder.finish()?;
    Ok(Some(WireFrame {
        request_id: decoded.request_id,
        stream_id: decoded.stream_id,
        message,
    }))
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &WireFrame,
    limits: ProtocolLimits,
) -> Result<(), ProtocolError> {
    let encoded = encode_frame(frame, limits)?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

struct Header {
    kind: u16,
    request_id: u64,
    stream_id: u64,
    payload_len: usize,
}

fn decode_header(header: &[u8], limits: ProtocolLimits) -> Result<Header, ProtocolError> {
    if header.len() != HEADER_LEN || &header[0..4] != MAGIC {
        return Err(ProtocolError::InvalidFrame(
            "invalid frame magic".to_owned(),
        ));
    }
    let major = u16::from_be_bytes(header[4..6].try_into().expect("fixed slice"));
    let minor = u16::from_be_bytes(header[6..8].try_into().expect("fixed slice"));
    if major != PROTOCOL_MAJOR || minor > PROTOCOL_MINOR {
        return Err(ProtocolError::UnsupportedVersion { major, minor });
    }
    let kind = u16::from_be_bytes(header[8..10].try_into().expect("fixed slice"));
    if !matches!(kind, 1 | 2 | 10 | 11 | 20 | 21 | 30 | 40 | 50 | 51) {
        return Err(ProtocolError::UnknownKind(kind));
    }
    let flags = u16::from_be_bytes(header[10..12].try_into().expect("fixed slice"));
    if flags != 0 {
        return Err(ProtocolError::InvalidFrame(format!(
            "unsupported frame flags {flags:#06x}"
        )));
    }
    let payload_len = u32::from_be_bytes(header[28..32].try_into().expect("fixed slice")) as usize;
    if payload_len > limits.max_payload_bytes() {
        return Err(ProtocolError::Limit(format!(
            "payload is {payload_len} bytes; negotiated maximum is {}",
            limits.max_payload_bytes()
        )));
    }
    Ok(Header {
        kind,
        request_id: u64::from_be_bytes(header[12..20].try_into().expect("fixed slice")),
        stream_id: u64::from_be_bytes(header[20..28].try_into().expect("fixed slice")),
        payload_len,
    })
}

fn verify_checksum(header: &[u8], payload: &[u8]) -> Result<(), ProtocolError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&header[..CHECKSUM_OFFSET]);
    hasher.update(payload);
    if header[CHECKSUM_OFFSET..HEADER_LEN] != *hasher.finalize().as_bytes() {
        return Err(ProtocolError::ChecksumMismatch);
    }
    Ok(())
}

fn encode_message(message: &Message, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
    match message {
        Message::ClientHello(value) => {
            put_uuid(out, value.cluster_id);
            put_uuid(out, value.principal_id);
            put_u8(out, value.role as u8);
            put_u16(out, value.minimum_major);
            put_u16(out, value.maximum_major);
            put_u32(out, value.requested_max_frame_bytes);
            put_u32(out, value.requested_max_records);
            put_u32(out, value.requested_max_inflight_requests);
            put_u64(out, value.initial_window_bytes);
            out.extend_from_slice(&value.session_nonce);
        }
        Message::ServerHello(value) => {
            put_uuid(out, value.cluster_id);
            put_uuid(out, value.node_id);
            put_u16(out, value.selected_major);
            put_u16(out, value.selected_minor);
            put_u32(out, value.max_frame_bytes);
            put_u32(out, value.max_records);
            put_u32(out, value.max_inflight_requests);
            put_u64(out, value.initial_window_bytes);
            out.extend_from_slice(&value.session_nonce);
        }
        Message::ProduceRequest(value) => {
            put_range(out, &value.range)?;
            put_u64(out, value.fencing_epoch);
            put_uuid(out, value.producer_id);
            put_u64(out, value.producer_epoch);
            put_u64(out, value.first_sequence);
            put_u8(out, value.durability as u8);
            put_u32(out, checked_count(value.records.len())?);
            for record in &value.records {
                put_i64(out, record.timestamp_millis);
                put_bytes(out, &record.key)?;
                put_bytes(out, &record.value)?;
            }
        }
        Message::ProduceResponse(value) => {
            put_u32(out, checked_count(value.outcomes.len())?);
            for outcome in &value.outcomes {
                put_u64(out, outcome.offset);
                put_u8(out, u8::from(outcome.duplicate));
            }
            put_u64(out, value.committed_next_offset);
        }
        Message::FetchRequest(value) => {
            put_range(out, &value.range)?;
            put_u64(out, value.fencing_epoch);
            put_u64(out, value.start_offset);
            put_u32(out, value.max_bytes);
            put_u32(out, value.max_records);
        }
        Message::FetchResponse(value) => {
            put_u32(out, checked_count(value.records.len())?);
            for record in &value.records {
                put_u64(out, record.offset);
                put_i64(out, record.timestamp_millis);
                put_bytes(out, &record.key)?;
                put_bytes(out, &record.value)?;
            }
            put_u64(out, value.next_offset);
            put_u64(out, value.committed_high_watermark);
        }
        Message::WindowUpdate(value) => put_u64(out, value.additional_bytes),
        Message::Error(value) => {
            put_u16(out, value.code as u16);
            put_u8(out, u8::from(value.retryable));
            if value.message.len() > MAX_ERROR_BYTES {
                return Err(ProtocolError::Limit(format!(
                    "error message exceeds {MAX_ERROR_BYTES} bytes"
                )));
            }
            put_string(out, &value.message)?;
        }
        Message::Ping | Message::Pong => {}
    }
    Ok(())
}

fn decode_message(
    kind: u16,
    decoder: &mut Decoder<'_>,
    limits: ProtocolLimits,
) -> Result<Message, ProtocolError> {
    Ok(match kind {
        1 => Message::ClientHello(ClientHello {
            cluster_id: decoder.uuid()?,
            principal_id: decoder.uuid()?,
            role: Role::decode(decoder.u8()?)?,
            minimum_major: decoder.u16()?,
            maximum_major: decoder.u16()?,
            requested_max_frame_bytes: decoder.u32()?,
            requested_max_records: decoder.u32()?,
            requested_max_inflight_requests: decoder.u32()?,
            initial_window_bytes: decoder.u64()?,
            session_nonce: decoder.array()?,
        }),
        2 => Message::ServerHello(ServerHello {
            cluster_id: decoder.uuid()?,
            node_id: decoder.uuid()?,
            selected_major: decoder.u16()?,
            selected_minor: decoder.u16()?,
            max_frame_bytes: decoder.u32()?,
            max_records: decoder.u32()?,
            max_inflight_requests: decoder.u32()?,
            initial_window_bytes: decoder.u64()?,
            session_nonce: decoder.array()?,
        }),
        10 => {
            let range = decoder.range()?;
            let fencing_epoch = decoder.u64()?;
            let producer_id = decoder.uuid()?;
            let producer_epoch = decoder.u64()?;
            let first_sequence = decoder.u64()?;
            let durability = Durability::decode(decoder.u8()?)?;
            let count = decoder.count(limits.max_records)?;
            if count == 0 {
                return Err(ProtocolError::InvalidFrame(
                    "produce request has no records".to_owned(),
                ));
            }
            let mut records = Vec::with_capacity(count);
            for _ in 0..count {
                records.push(ProduceRecord {
                    timestamp_millis: decoder.i64()?,
                    key: decoder.bytes()?,
                    value: decoder.bytes()?,
                });
            }
            Message::ProduceRequest(ProduceRequest {
                range,
                fencing_epoch,
                producer_id,
                producer_epoch,
                first_sequence,
                durability,
                records,
            })
        }
        11 => {
            let count = decoder.count(limits.max_records)?;
            let mut outcomes = Vec::with_capacity(count);
            for _ in 0..count {
                outcomes.push(ProduceOutcome {
                    offset: decoder.u64()?,
                    duplicate: decoder.boolean()?,
                });
            }
            Message::ProduceResponse(ProduceResponse {
                outcomes,
                committed_next_offset: decoder.u64()?,
            })
        }
        20 => {
            let range = decoder.range()?;
            let fencing_epoch = decoder.u64()?;
            let start_offset = decoder.u64()?;
            let max_bytes = decoder.u32()?;
            let max_records = decoder.u32()?;
            if max_bytes == 0 || max_records == 0 || max_records > limits.max_records {
                return Err(ProtocolError::Limit(format!(
                    "fetch limits must be non-zero and max_records must not exceed {}",
                    limits.max_records
                )));
            }
            Message::FetchRequest(FetchRequest {
                range,
                fencing_epoch,
                start_offset,
                max_bytes,
                max_records,
            })
        }
        21 => {
            let count = decoder.count(limits.max_records)?;
            let mut records = Vec::with_capacity(count);
            for _ in 0..count {
                records.push(FetchedRecord {
                    offset: decoder.u64()?,
                    timestamp_millis: decoder.i64()?,
                    key: decoder.bytes()?,
                    value: decoder.bytes()?,
                });
            }
            Message::FetchResponse(FetchResponse {
                records,
                next_offset: decoder.u64()?,
                committed_high_watermark: decoder.u64()?,
            })
        }
        30 => Message::WindowUpdate(WindowUpdate {
            additional_bytes: decoder.u64()?,
        }),
        40 => {
            let code = ErrorCode::decode(decoder.u16()?)?;
            let retryable = decoder.boolean()?;
            let message = decoder.string(MAX_ERROR_BYTES)?;
            Message::Error(ErrorResponse {
                code,
                retryable,
                message,
            })
        }
        50 => Message::Ping,
        51 => Message::Pong,
        other => return Err(ProtocolError::UnknownKind(other)),
    })
}

fn checked_count(count: usize) -> Result<u32, ProtocolError> {
    u32::try_from(count).map_err(|_| ProtocolError::Limit("record count exceeds u32".to_owned()))
}

fn put_range(out: &mut Vec<u8>, range: &RangeIdentity) -> Result<(), ProtocolError> {
    put_topic(out, &range.topic)?;
    put_u64(out, range.topic_epoch);
    put_uuid(out, range.range_id);
    put_u64(out, range.range_generation);
    Ok(())
}

fn validate_topic(topic: &str) -> Result<(), ProtocolError> {
    if topic.is_empty() || topic.len() > MAX_TOPIC_BYTES {
        return Err(ProtocolError::Limit(format!(
            "topic length must be in 1..={MAX_TOPIC_BYTES} bytes"
        )));
    }
    if topic
        .bytes()
        .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')))
    {
        return Err(ProtocolError::InvalidFrame(
            "topic contains unsupported characters".to_owned(),
        ));
    }
    Ok(())
}

fn put_topic(out: &mut Vec<u8>, topic: &str) -> Result<(), ProtocolError> {
    validate_topic(topic)?;
    put_string(out, topic)
}

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<(), ProtocolError> {
    put_bytes(out, value.as_bytes())
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), ProtocolError> {
    let length = u32::try_from(value.len())
        .map_err(|_| ProtocolError::Limit("byte string exceeds u32".to_owned()))?;
    put_u32(out, length);
    out.extend_from_slice(value);
    Ok(())
}

fn put_uuid(out: &mut Vec<u8>, value: Uuid) {
    out.extend_from_slice(value.as_bytes());
}
fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}
fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_be_bytes());
}

struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| ProtocolError::InvalidFrame("field length overflow".to_owned()))?;
        let value = self.bytes.get(self.position..end).ok_or_else(|| {
            ProtocolError::InvalidFrame(format!("truncated field at byte {}", self.position))
        })?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, ProtocolError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ProtocolError> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("fixed slice"),
        ))
    }
    fn u32(&mut self) -> Result<u32, ProtocolError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("fixed slice"),
        ))
    }
    fn u64(&mut self) -> Result<u64, ProtocolError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("fixed slice"),
        ))
    }
    fn i64(&mut self) -> Result<i64, ProtocolError> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().expect("fixed slice"),
        ))
    }
    fn uuid(&mut self) -> Result<Uuid, ProtocolError> {
        Uuid::from_slice(self.take(16)?)
            .map_err(|error| ProtocolError::InvalidFrame(format!("invalid UUID: {error}")))
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], ProtocolError> {
        Ok(self.take(N)?.try_into().expect("validated fixed slice"))
    }
    fn boolean(&mut self) -> Result<bool, ProtocolError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(ProtocolError::InvalidFrame(format!(
                "invalid boolean {other}"
            ))),
        }
    }
    fn bytes(&mut self) -> Result<Vec<u8>, ProtocolError> {
        let length = self.u32()? as usize;
        Ok(self.take(length)?.to_vec())
    }
    fn string(&mut self, maximum: usize) -> Result<String, ProtocolError> {
        let length = self.u32()? as usize;
        if length > maximum {
            return Err(ProtocolError::Limit(format!(
                "string is {length} bytes; maximum is {maximum}"
            )));
        }
        String::from_utf8(self.take(length)?.to_vec())
            .map_err(|error| ProtocolError::InvalidFrame(format!("invalid UTF-8: {error}")))
    }
    fn topic(&mut self) -> Result<String, ProtocolError> {
        let topic = self.string(MAX_TOPIC_BYTES)?;
        if topic.is_empty()
            || topic
                .bytes()
                .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')))
        {
            return Err(ProtocolError::InvalidFrame("invalid topic name".to_owned()));
        }
        Ok(topic)
    }
    fn range(&mut self) -> Result<RangeIdentity, ProtocolError> {
        Ok(RangeIdentity {
            topic: self.topic()?,
            topic_epoch: self.u64()?,
            range_id: self.uuid()?,
            range_generation: self.u64()?,
        })
    }
    fn count(&mut self, maximum: u32) -> Result<usize, ProtocolError> {
        let count = self.u32()?;
        if count > maximum {
            return Err(ProtocolError::Limit(format!(
                "record count is {count}; negotiated maximum is {maximum}"
            )));
        }
        Ok(count as usize)
    }
    fn finish(self) -> Result<(), ProtocolError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(ProtocolError::InvalidFrame(format!(
                "{} trailing payload bytes",
                self.bytes.len() - self.position
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> ProtocolLimits {
        ProtocolLimits {
            max_frame_bytes: 4096,
            max_records: 8,
        }
    }

    fn produce() -> WireFrame {
        WireFrame {
            request_id: 7,
            stream_id: 9,
            message: Message::ProduceRequest(ProduceRequest {
                range: range(),
                fencing_epoch: 11,
                producer_id: Uuid::from_u128(6),
                producer_epoch: 7,
                first_sequence: 8,
                durability: Durability::LocalFsync,
                records: vec![ProduceRecord {
                    timestamp_millis: -9,
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                }],
            }),
        }
    }

    fn range() -> RangeIdentity {
        RangeIdentity {
            topic: "audit.events".to_owned(),
            topic_epoch: 3,
            range_id: Uuid::from_u128(4),
            range_generation: 5,
        }
    }

    #[test]
    fn produce_frame_round_trips_deterministically() {
        let first = encode_frame(&produce(), limits()).unwrap();
        let second = encode_frame(&produce(), limits()).unwrap();
        assert_eq!(first, second);
        assert_eq!(decode_frame(&first, limits()).unwrap(), produce());
    }

    #[test]
    fn every_message_family_round_trips() {
        let messages = vec![
            Message::ClientHello(ClientHello {
                cluster_id: Uuid::from_u128(1),
                principal_id: Uuid::from_u128(9),
                role: Role::Producer,
                minimum_major: 1,
                maximum_major: 1,
                requested_max_frame_bytes: 4096,
                requested_max_records: 8,
                requested_max_inflight_requests: 4,
                initial_window_bytes: 1024,
                session_nonce: [1; 32],
            }),
            Message::ServerHello(ServerHello {
                cluster_id: Uuid::from_u128(1),
                node_id: Uuid::from_u128(2),
                selected_major: 1,
                selected_minor: 0,
                max_frame_bytes: 4096,
                max_records: 8,
                max_inflight_requests: 4,
                initial_window_bytes: 1024,
                session_nonce: [2; 32],
            }),
            produce().message,
            Message::ProduceResponse(ProduceResponse {
                outcomes: vec![ProduceOutcome {
                    offset: 11,
                    duplicate: false,
                }],
                committed_next_offset: 12,
            }),
            Message::FetchRequest(FetchRequest {
                range: range(),
                fencing_epoch: 11,
                start_offset: 10,
                max_bytes: 1000,
                max_records: 2,
            }),
            Message::FetchResponse(FetchResponse {
                records: vec![FetchedRecord {
                    offset: 10,
                    timestamp_millis: 20,
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                }],
                next_offset: 11,
                committed_high_watermark: 11,
            }),
            Message::WindowUpdate(WindowUpdate {
                additional_bytes: 2048,
            }),
            Message::Error(ErrorResponse {
                code: ErrorCode::Overloaded,
                retryable: true,
                message: "retry later".to_owned(),
            }),
            Message::Ping,
            Message::Pong,
        ];
        for (request_id, message) in messages.into_iter().enumerate() {
            let frame = WireFrame {
                request_id: request_id as u64,
                stream_id: 0,
                message,
            };
            let encoded = encode_frame(&frame, limits()).unwrap();
            assert_eq!(decode_frame(&encoded, limits()).unwrap(), frame);
        }
    }

    #[test]
    fn rejects_corruption_trailing_bytes_and_oversized_lengths_before_allocation() {
        let encoded = encode_frame(&produce(), limits()).unwrap();
        for index in [0, 10, HEADER_LEN, encoded.len() - 1] {
            let mut corrupt = encoded.clone();
            corrupt[index] ^= 1;
            assert!(decode_frame(&corrupt, limits()).is_err(), "index {index}");
        }

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(matches!(
            decode_frame(&trailing, limits()),
            Err(ProtocolError::InvalidFrame(_))
        ));

        let mut oversized = [0_u8; HEADER_LEN];
        oversized[..4].copy_from_slice(MAGIC);
        oversized[4..6].copy_from_slice(&PROTOCOL_MAJOR.to_be_bytes());
        oversized[8..10].copy_from_slice(&50_u16.to_be_bytes());
        oversized[28..32].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            decode_header(&oversized, limits()),
            Err(ProtocolError::Limit(_))
        ));
    }

    #[test]
    fn rejects_oversized_outbound_payload_before_encoding_it() {
        let mut frame = produce();
        let Message::ProduceRequest(request) = &mut frame.message else {
            unreachable!()
        };
        request.records[0].value = vec![0_u8; limits().max_frame_bytes as usize];
        assert!(matches!(
            encode_frame(&frame, limits()),
            Err(ProtocolError::Limit(_))
        ));
    }

    #[tokio::test]
    async fn async_reader_handles_eof_and_fragmented_input() {
        let encoded = encode_frame(&produce(), limits()).unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(32);
        let expected = produce();
        let task = tokio::spawn(async move {
            for chunk in encoded.chunks(3) {
                writer.write_all(chunk).await.unwrap();
            }
        });
        assert_eq!(
            read_frame(&mut reader, limits()).await.unwrap(),
            Some(expected)
        );
        task.await.unwrap();
        assert_eq!(read_frame(&mut reader, limits()).await.unwrap(), None);
    }
}

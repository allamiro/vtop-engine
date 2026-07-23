//! VTPM peer/admin wire protocol — stage-5 PR 3.
//!
//! Consensus-shaped messages live here, not in `vtop-protocol` (frozen client
//! produce/fetch). Every frame is magic-tagged, length-bounded, and
//! BLAKE3-checksummed with the same field-by-field discipline as the rest of
//! `vtop-meta` (big-endian, trailing-byte rejection, no serde).
//!
//! # Frame layout
//!
//! ```text
//! magic "VTPM"           4
//! version u16            2   (= 1)
//! kind u16               2
//! payload_len u32        4
//! BLAKE3-32              32  (over magic..payload_len + payload)
//! payload                payload_len
//! ```
//!
//! Log indexes on the wire are **meta indexes** (`raft_index + 1`), matching
//! the durable store. The raft network adapter translates at the boundary.
//!
//! # Determinism / limitations
//!
//! Codecs are pure and deterministic. Live mTLS I/O is not: TCP scheduling,
//! TLS record boundaries, and wall-clock timeouts are best-effort. Tests that
//! exercise the network mark that honesty explicitly.

use crate::command::{MetadataCommand, MetadataResponse, MAX_ERROR_DETAIL_BYTES};
use crate::keys::MetaNodeId;
use crate::storage::hardstate::HardState;
use crate::storage::log::{MetaLogEntry, MetaLogPayload, MetaMembership};
use crate::wire::{put_bounded_str, put_u16, put_u32, put_u64, put_u8, CodecError, Reader};
use std::io;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Wire magic distinguishing meta peer/admin frames from client `VTPW`.
pub const VTPM_MAGIC: &[u8; 4] = b"VTPM";
/// Current VTPM protocol version.
pub const VTPM_VERSION: u16 = 1;

/// Absolute ceiling for one framed message (header + payload).
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
/// Bound for a single install-snapshot chunk.
pub const MAX_SNAPSHOT_CHUNK_BYTES: usize = 4 * 1024 * 1024;
/// Bound for the consensus engine's snapshot id string.
pub const MAX_SNAPSHOT_ID_BYTES: usize = 256;
/// Bound for entries carried in one AppendEntries request.
pub const MAX_APPEND_ENTRIES: usize = 1024;
/// Bound for the server-state display string in admin status.
pub const MAX_SERVER_STATE_BYTES: usize = 32;

const HEADER_LEN: usize = 4 + 2 + 2 + 4 + 32;
const CHECKSUM_OFFSET: usize = 4 + 2 + 2 + 4;

pub const KIND_VOTE_REQ: u16 = 1;
pub const KIND_VOTE_RESP: u16 = 2;
pub const KIND_APPEND_REQ: u16 = 3;
pub const KIND_APPEND_RESP: u16 = 4;
pub const KIND_INSTALL_REQ: u16 = 5;
pub const KIND_INSTALL_RESP: u16 = 6;
pub const KIND_ADMIN_STATUS_REQ: u16 = 10;
pub const KIND_ADMIN_STATUS_RESP: u16 = 11;
pub const KIND_ADMIN_PROPOSE_REQ: u16 = 12;
pub const KIND_ADMIN_PROPOSE_RESP: u16 = 13;
pub const KIND_ADMIN_ERROR: u16 = 14;

/// Transport / framing errors distinct from codec parse failures.
#[derive(Debug, Error)]
pub enum TransportError {
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("frame is {actual} bytes; the bound is {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("invalid VTPM frame magic")]
    BadMagic,
    #[error("unsupported VTPM version {0}")]
    BadVersion(u16),
    #[error("VTPM frame checksum mismatch")]
    ChecksumMismatch,
    #[error("tls: {0}")]
    Tls(String),
    #[error("identity: {0}")]
    Identity(String),
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("peer closed the connection")]
    Closed,
    #[error("unexpected response kind {0}")]
    UnexpectedKind(u16),
    #[error("{0}")]
    Protocol(String),
}

pub type TransportResult<T> = Result<T, TransportError>;

/// A decoded VTPM frame header + payload bytes (checksum already verified).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VtpmFrame {
    pub kind: u16,
    pub payload: Vec<u8>,
}

impl VtpmFrame {
    pub fn encode(&self) -> TransportResult<Vec<u8>> {
        if self.payload.len() > MAX_FRAME_BYTES.saturating_sub(HEADER_LEN) {
            return Err(TransportError::FrameTooLarge {
                actual: self.payload.len() + HEADER_LEN,
                maximum: MAX_FRAME_BYTES,
            });
        }
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len());
        out.extend_from_slice(VTPM_MAGIC);
        put_u16(&mut out, VTPM_VERSION);
        put_u16(&mut out, self.kind);
        put_u32(&mut out, self.payload.len() as u32);
        out.resize(HEADER_LEN, 0);
        out.extend_from_slice(&self.payload);
        let mut hasher = blake3::Hasher::new();
        hasher.update(&out[..CHECKSUM_OFFSET]);
        hasher.update(&out[HEADER_LEN..]);
        out[CHECKSUM_OFFSET..HEADER_LEN].copy_from_slice(hasher.finalize().as_bytes());
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> TransportResult<Self> {
        if bytes.len() < HEADER_LEN {
            return Err(CodecError::Truncated("VTPM header").into());
        }
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(TransportError::FrameTooLarge {
                actual: bytes.len(),
                maximum: MAX_FRAME_BYTES,
            });
        }
        if &bytes[0..4] != VTPM_MAGIC {
            return Err(TransportError::BadMagic);
        }
        let version = u16::from_be_bytes(bytes[4..6].try_into().expect("fixed"));
        if version != VTPM_VERSION {
            return Err(TransportError::BadVersion(version));
        }
        let kind = u16::from_be_bytes(bytes[6..8].try_into().expect("fixed"));
        let payload_len = u32::from_be_bytes(bytes[8..12].try_into().expect("fixed")) as usize;
        if bytes.len() != HEADER_LEN + payload_len {
            return Err(CodecError::InvalidValue {
                what: "VTPM frame length",
                reason: "declared payload length does not match frame size",
            }
            .into());
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(&bytes[..CHECKSUM_OFFSET]);
        hasher.update(&bytes[HEADER_LEN..]);
        if hasher.finalize().as_bytes() != &bytes[CHECKSUM_OFFSET..HEADER_LEN] {
            return Err(TransportError::ChecksumMismatch);
        }
        Ok(Self {
            kind,
            payload: bytes[HEADER_LEN..].to_vec(),
        })
    }
}

/// Read one length-delimited VTPM frame from an async stream.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> TransportResult<VtpmFrame> {
    let mut header = [0_u8; HEADER_LEN];
    if let Err(error) = reader.read_exact(&mut header).await {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            return Err(TransportError::Closed);
        }
        return Err(error.into());
    }
    if &header[0..4] != VTPM_MAGIC {
        return Err(TransportError::BadMagic);
    }
    let version = u16::from_be_bytes(header[4..6].try_into().expect("fixed"));
    if version != VTPM_VERSION {
        return Err(TransportError::BadVersion(version));
    }
    let payload_len = u32::from_be_bytes(header[8..12].try_into().expect("fixed")) as usize;
    let total = HEADER_LEN.saturating_add(payload_len);
    if total > MAX_FRAME_BYTES {
        return Err(TransportError::FrameTooLarge {
            actual: total,
            maximum: MAX_FRAME_BYTES,
        });
    }
    let mut payload = vec![0_u8; payload_len];
    reader.read_exact(&mut payload).await?;
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(&payload);
    VtpmFrame::decode(&bytes)
}

/// Write one VTPM frame to an async stream and flush.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &VtpmFrame,
) -> TransportResult<()> {
    let encoded = frame.encode()?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared field codecs (HardState / LogId / MetaLogEntry body)
// ---------------------------------------------------------------------------

fn put_hard_state(out: &mut Vec<u8>, state: &HardState) {
    put_u64(out, state.term);
    match state.voted_for {
        Some(MetaNodeId(id)) => {
            put_u8(out, 1);
            put_u64(out, id);
        }
        None => {
            put_u8(out, 0);
            put_u64(out, 0);
        }
    }
    put_u8(out, u8::from(state.vote_committed));
}

fn take_hard_state(reader: &mut Reader<'_>) -> Result<HardState, CodecError> {
    let term = reader.u64("vote term")?;
    let present = reader.flag("voted_for present")?;
    let voted_raw = reader.u64("voted_for")?;
    let voted_for = if present {
        Some(MetaNodeId(voted_raw))
    } else if voted_raw == 0 {
        None
    } else {
        return Err(CodecError::InvalidValue {
            what: "voted_for",
            reason: "absent vote must carry zero id",
        });
    };
    Ok(HardState {
        term,
        voted_for,
        vote_committed: reader.flag("vote committed")?,
    })
}

/// Meta-index log id on the wire: `(term, meta_index)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WireLogId {
    pub term: u64,
    pub index: u64,
}

fn put_optional_log_id(out: &mut Vec<u8>, value: Option<WireLogId>) {
    match value {
        Some(id) => {
            put_u8(out, 1);
            put_u64(out, id.term);
            put_u64(out, id.index);
        }
        None => put_u8(out, 0),
    }
}

fn take_optional_log_id(reader: &mut Reader<'_>) -> Result<Option<WireLogId>, CodecError> {
    if !reader.flag("log id present")? {
        return Ok(None);
    }
    Ok(Some(WireLogId {
        term: reader.u64("log id term")?,
        index: reader.u64("log id index")?,
    }))
}

fn put_log_entry(out: &mut Vec<u8>, entry: &MetaLogEntry) -> Result<(), CodecError> {
    put_u64(out, entry.term);
    put_u64(out, entry.index);
    let (kind, payload) = match &entry.payload {
        MetaLogPayload::Normal(command) => (1_u8, command.encode()?),
        MetaLogPayload::Membership(membership) => (2_u8, membership.encode()?),
        MetaLogPayload::Blank => (3_u8, Vec::new()),
    };
    put_u8(out, kind);
    put_u32(out, payload.len() as u32);
    out.extend_from_slice(&payload);
    Ok(())
}

fn take_log_entry(reader: &mut Reader<'_>) -> Result<MetaLogEntry, CodecError> {
    let term = reader.u64("entry term")?;
    let index = reader.u64("entry index")?;
    let kind = reader.u8("entry kind")?;
    let payload_len = reader.u32("entry payload len")? as usize;
    if payload_len > MAX_SNAPSHOT_CHUNK_BYTES {
        return Err(CodecError::BoundExceeded {
            what: "log entry payload",
            actual: payload_len,
            maximum: MAX_SNAPSHOT_CHUNK_BYTES,
        });
    }
    let payload_bytes = reader.take(payload_len, "entry payload")?;
    let payload = match kind {
        1 => MetaLogPayload::Normal(MetadataCommand::decode(payload_bytes)?),
        2 => MetaLogPayload::Membership(MetaMembership::decode(payload_bytes)?),
        3 => {
            if !payload_bytes.is_empty() {
                return Err(CodecError::InvalidValue {
                    what: "blank entry",
                    reason: "payload must be empty",
                });
            }
            MetaLogPayload::Blank
        }
        other => {
            return Err(CodecError::UnknownTag {
                what: "log entry kind",
                tag: u32::from(other),
            })
        }
    };
    Ok(MetaLogEntry {
        term,
        index,
        payload,
    })
}

// ---------------------------------------------------------------------------
// Peer RPC payloads
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerVoteRequest {
    pub vote: HardState,
    pub last_log_id: Option<WireLogId>,
}

impl PeerVoteRequest {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::with_capacity(48);
        put_hard_state(&mut out, &self.vote);
        put_optional_log_id(&mut out, self.last_log_id);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let vote = take_hard_state(&mut reader)?;
        let last_log_id = take_optional_log_id(&mut reader)?;
        reader.finish()?;
        Ok(Self { vote, last_log_id })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerVoteResponse {
    pub vote: HardState,
    pub vote_granted: bool,
    pub last_log_id: Option<WireLogId>,
}

impl PeerVoteResponse {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::with_capacity(48);
        put_hard_state(&mut out, &self.vote);
        put_u8(&mut out, u8::from(self.vote_granted));
        put_optional_log_id(&mut out, self.last_log_id);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let vote = take_hard_state(&mut reader)?;
        let vote_granted = reader.flag("vote granted")?;
        let last_log_id = take_optional_log_id(&mut reader)?;
        reader.finish()?;
        Ok(Self {
            vote,
            vote_granted,
            last_log_id,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerAppendRequest {
    pub vote: HardState,
    pub prev_log_id: Option<WireLogId>,
    pub entries: Vec<MetaLogEntry>,
    pub leader_commit: Option<WireLogId>,
}

impl PeerAppendRequest {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        if self.entries.len() > MAX_APPEND_ENTRIES {
            return Err(CodecError::BoundExceeded {
                what: "append entries",
                actual: self.entries.len(),
                maximum: MAX_APPEND_ENTRIES,
            });
        }
        let mut out = Vec::new();
        put_hard_state(&mut out, &self.vote);
        put_optional_log_id(&mut out, self.prev_log_id);
        put_u16(&mut out, self.entries.len() as u16);
        for entry in &self.entries {
            put_log_entry(&mut out, entry)?;
        }
        put_optional_log_id(&mut out, self.leader_commit);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let vote = take_hard_state(&mut reader)?;
        let prev_log_id = take_optional_log_id(&mut reader)?;
        let count = reader.u16("append entry count")? as usize;
        if count > MAX_APPEND_ENTRIES {
            return Err(CodecError::BoundExceeded {
                what: "append entries",
                actual: count,
                maximum: MAX_APPEND_ENTRIES,
            });
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(take_log_entry(&mut reader)?);
        }
        let leader_commit = take_optional_log_id(&mut reader)?;
        reader.finish()?;
        Ok(Self {
            vote,
            prev_log_id,
            entries,
            leader_commit,
        })
    }
}

/// AppendEntries response kinds. PartialSuccess carries an optional matching
/// log id (meta index).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerAppendResponse {
    Success,
    PartialSuccess(Option<WireLogId>),
    Conflict,
    HigherVote(HardState),
}

impl PeerAppendResponse {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::new();
        match self {
            Self::Success => put_u8(&mut out, 1),
            Self::PartialSuccess(id) => {
                put_u8(&mut out, 2);
                put_optional_log_id(&mut out, *id);
            }
            Self::Conflict => put_u8(&mut out, 3),
            Self::HigherVote(vote) => {
                put_u8(&mut out, 4);
                put_hard_state(&mut out, vote);
            }
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let kind = reader.u8("append response kind")?;
        let response = match kind {
            1 => Self::Success,
            2 => Self::PartialSuccess(take_optional_log_id(&mut reader)?),
            3 => Self::Conflict,
            4 => Self::HigherVote(take_hard_state(&mut reader)?),
            other => {
                return Err(CodecError::UnknownTag {
                    what: "append response kind",
                    tag: u32::from(other),
                })
            }
        };
        reader.finish()?;
        Ok(response)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerInstallRequest {
    pub vote: HardState,
    pub last_log_id: Option<WireLogId>,
    /// Log id of the membership config itself — may differ from `last_log_id`
    /// when normal entries follow the membership entry in the snapshot.
    pub membership_log_id: Option<WireLogId>,
    pub last_membership: MetaMembership,
    pub snapshot_id: String,
    pub offset: u64,
    pub data: Vec<u8>,
    pub done: bool,
}

impl PeerInstallRequest {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        if self.data.len() > MAX_SNAPSHOT_CHUNK_BYTES {
            return Err(CodecError::BoundExceeded {
                what: "snapshot chunk",
                actual: self.data.len(),
                maximum: MAX_SNAPSHOT_CHUNK_BYTES,
            });
        }
        let mut out = Vec::new();
        put_hard_state(&mut out, &self.vote);
        put_optional_log_id(&mut out, self.last_log_id);
        put_optional_log_id(&mut out, self.membership_log_id);
        let membership = self.last_membership.encode()?;
        put_u32(&mut out, membership.len() as u32);
        out.extend_from_slice(&membership);
        put_bounded_str(
            &mut out,
            &self.snapshot_id,
            MAX_SNAPSHOT_ID_BYTES,
            "snapshot id",
        )?;
        put_u64(&mut out, self.offset);
        put_u32(&mut out, self.data.len() as u32);
        out.extend_from_slice(&self.data);
        put_u8(&mut out, u8::from(self.done));
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let vote = take_hard_state(&mut reader)?;
        let last_log_id = take_optional_log_id(&mut reader)?;
        let membership_log_id = take_optional_log_id(&mut reader)?;
        let membership_len = reader.u32("membership len")? as usize;
        let membership_bytes = reader.take(membership_len, "membership")?;
        let last_membership = MetaMembership::decode(membership_bytes)?;
        let snapshot_id = reader.bounded_str(MAX_SNAPSHOT_ID_BYTES, "snapshot id")?;
        let offset = reader.u64("snapshot offset")?;
        let data_len = reader.u32("snapshot data len")? as usize;
        if data_len > MAX_SNAPSHOT_CHUNK_BYTES {
            return Err(CodecError::BoundExceeded {
                what: "snapshot chunk",
                actual: data_len,
                maximum: MAX_SNAPSHOT_CHUNK_BYTES,
            });
        }
        let data = reader.take(data_len, "snapshot data")?.to_vec();
        let done = reader.flag("snapshot done")?;
        reader.finish()?;
        Ok(Self {
            vote,
            last_log_id,
            membership_log_id,
            last_membership,
            snapshot_id,
            offset,
            data,
            done,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerInstallResponse {
    pub vote: HardState,
}

impl PeerInstallResponse {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::with_capacity(24);
        put_hard_state(&mut out, &self.vote);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let vote = take_hard_state(&mut reader)?;
        reader.finish()?;
        Ok(Self { vote })
    }
}

// ---------------------------------------------------------------------------
// Admin payloads
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct AdminStatusRequest;

impl AdminStatusRequest {
    pub fn encode(&self) -> Vec<u8> {
        Vec::new()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Reader::new(bytes).finish()?;
        Ok(Self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminStatusResponse {
    pub node_id: MetaNodeId,
    pub current_term: u64,
    pub vote: HardState,
    pub current_leader: Option<MetaNodeId>,
    pub server_state: String,
    pub last_applied: Option<WireLogId>,
    pub membership: MetaMembership,
}

impl AdminStatusResponse {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::new();
        put_u64(&mut out, self.node_id.0);
        put_u64(&mut out, self.current_term);
        put_hard_state(&mut out, &self.vote);
        match self.current_leader {
            Some(MetaNodeId(id)) => {
                put_u8(&mut out, 1);
                put_u64(&mut out, id);
            }
            None => put_u8(&mut out, 0),
        }
        put_bounded_str(
            &mut out,
            &self.server_state,
            MAX_SERVER_STATE_BYTES,
            "server state",
        )?;
        put_optional_log_id(&mut out, self.last_applied);
        let membership = self.membership.encode()?;
        put_u32(&mut out, membership.len() as u32);
        out.extend_from_slice(&membership);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let node_id = MetaNodeId(reader.u64("node id")?);
        let current_term = reader.u64("current term")?;
        let vote = take_hard_state(&mut reader)?;
        let current_leader = if reader.flag("leader present")? {
            Some(MetaNodeId(reader.u64("leader id")?))
        } else {
            None
        };
        let server_state = reader.bounded_str(MAX_SERVER_STATE_BYTES, "server state")?;
        let last_applied = take_optional_log_id(&mut reader)?;
        let membership_len = reader.u32("membership len")? as usize;
        let membership = MetaMembership::decode(reader.take(membership_len, "membership")?)?;
        reader.finish()?;
        Ok(Self {
            node_id,
            current_term,
            vote,
            current_leader,
            server_state,
            last_applied,
            membership,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminProposeRequest {
    pub command: MetadataCommand,
}

impl AdminProposeRequest {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        self.command.encode()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        Ok(Self {
            command: MetadataCommand::decode(bytes)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminProposeResponse {
    pub log_id: WireLogId,
    pub response: MetadataResponse,
}

impl AdminProposeResponse {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::new();
        put_u64(&mut out, self.log_id.term);
        put_u64(&mut out, self.log_id.index);
        let response = self.response.encode()?;
        put_u32(&mut out, response.len() as u32);
        out.extend_from_slice(&response);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let log_id = WireLogId {
            term: reader.u64("commit term")?,
            index: reader.u64("commit index")?,
        };
        let response_len = reader.u32("response len")? as usize;
        let response = MetadataResponse::decode(reader.take(response_len, "response")?)?;
        reader.finish()?;
        Ok(Self { log_id, response })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminError {
    pub message: String,
}

impl AdminError {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::new();
        put_bounded_str(
            &mut out,
            &self.message,
            MAX_ERROR_DETAIL_BYTES,
            "admin error",
        )?;
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let message = reader.bounded_str(MAX_ERROR_DETAIL_BYTES, "admin error")?;
        reader.finish()?;
        Ok(Self { message })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{CommandEnvelope, NodeState};
    use uuid::Uuid;

    fn sample_vote() -> HardState {
        HardState {
            term: 7,
            voted_for: Some(MetaNodeId(2)),
            vote_committed: true,
        }
    }

    fn sample_entry() -> MetaLogEntry {
        MetaLogEntry {
            term: 7,
            index: 3,
            payload: MetaLogPayload::Normal(MetadataCommand::SetNodeState {
                env: CommandEnvelope {
                    request_id: Uuid::from_u128(0xdead_beef),
                    issued_at_ms: 1,
                },
                node_uuid: Uuid::from_u128(9),
                state: NodeState::Active,
                expected_generation: 1,
            }),
        }
    }

    #[test]
    fn vtpm_frame_round_trip_and_rejects_corruption() {
        let frame = VtpmFrame {
            kind: KIND_VOTE_REQ,
            payload: PeerVoteRequest {
                vote: sample_vote(),
                last_log_id: Some(WireLogId { term: 6, index: 2 }),
            }
            .encode()
            .unwrap(),
        };
        let encoded = frame.encode().unwrap();
        assert_eq!(VtpmFrame::decode(&encoded).unwrap(), frame);

        let mut bad_magic = encoded.clone();
        bad_magic[0] = b'X';
        assert!(matches!(
            VtpmFrame::decode(&bad_magic),
            Err(TransportError::BadMagic)
        ));

        let mut bad_checksum = encoded.clone();
        bad_checksum[CHECKSUM_OFFSET] ^= 0xff;
        assert!(matches!(
            VtpmFrame::decode(&bad_checksum),
            Err(TransportError::ChecksumMismatch)
        ));

        let mut truncated = encoded;
        truncated.truncate(HEADER_LEN - 1);
        assert!(VtpmFrame::decode(&truncated).is_err());
    }

    #[test]
    fn peer_rpc_payloads_round_trip() {
        let vote_req = PeerVoteRequest {
            vote: sample_vote(),
            last_log_id: None,
        };
        assert_eq!(
            PeerVoteRequest::decode(&vote_req.encode().unwrap()).unwrap(),
            vote_req
        );

        let vote_resp = PeerVoteResponse {
            vote: sample_vote(),
            vote_granted: true,
            last_log_id: Some(WireLogId { term: 1, index: 1 }),
        };
        assert_eq!(
            PeerVoteResponse::decode(&vote_resp.encode().unwrap()).unwrap(),
            vote_resp
        );

        let append = PeerAppendRequest {
            vote: sample_vote(),
            prev_log_id: Some(WireLogId { term: 7, index: 2 }),
            entries: vec![
                sample_entry(),
                MetaLogEntry {
                    term: 7,
                    index: 4,
                    payload: MetaLogPayload::Blank,
                },
            ],
            leader_commit: Some(WireLogId { term: 7, index: 2 }),
        };
        assert_eq!(
            PeerAppendRequest::decode(&append.encode().unwrap()).unwrap(),
            append
        );

        for response in [
            PeerAppendResponse::Success,
            PeerAppendResponse::PartialSuccess(Some(WireLogId { term: 1, index: 2 })),
            PeerAppendResponse::Conflict,
            PeerAppendResponse::HigherVote(sample_vote()),
        ] {
            assert_eq!(
                PeerAppendResponse::decode(&response.encode().unwrap()).unwrap(),
                response
            );
        }

        let install = PeerInstallRequest {
            vote: sample_vote(),
            last_log_id: Some(WireLogId { term: 7, index: 9 }),
            membership_log_id: Some(WireLogId { term: 7, index: 4 }),
            last_membership: MetaMembership {
                voters: vec![MetaNodeId(1), MetaNodeId(2)],
                learners: vec![],
            },
            snapshot_id: "snap-1".to_owned(),
            offset: 0,
            data: vec![1, 2, 3, 4],
            done: true,
        };
        assert_eq!(
            PeerInstallRequest::decode(&install.encode().unwrap()).unwrap(),
            install
        );
        let install_resp = PeerInstallResponse {
            vote: sample_vote(),
        };
        assert_eq!(
            PeerInstallResponse::decode(&install_resp.encode().unwrap()).unwrap(),
            install_resp
        );
    }

    #[test]
    fn admin_payloads_round_trip() {
        let status = AdminStatusResponse {
            node_id: MetaNodeId(1),
            current_term: 4,
            vote: sample_vote(),
            current_leader: Some(MetaNodeId(1)),
            server_state: "Leader".to_owned(),
            last_applied: Some(WireLogId { term: 4, index: 10 }),
            membership: MetaMembership {
                voters: vec![MetaNodeId(1), MetaNodeId(2), MetaNodeId(3)],
                learners: vec![],
            },
        };
        assert_eq!(
            AdminStatusResponse::decode(&status.encode().unwrap()).unwrap(),
            status
        );

        let propose = AdminProposeRequest {
            command: MetadataCommand::SetNodeState {
                env: CommandEnvelope {
                    request_id: Uuid::from_u128(1),
                    issued_at_ms: 0,
                },
                node_uuid: Uuid::from_u128(2),
                state: NodeState::Draining,
                expected_generation: 3,
            },
        };
        assert_eq!(
            AdminProposeRequest::decode(&propose.encode().unwrap()).unwrap(),
            propose
        );
    }

    #[test]
    fn decode_rejects_trailing_bytes_and_unknown_tags() {
        let mut trailing = PeerVoteRequest {
            vote: sample_vote(),
            last_log_id: None,
        }
        .encode()
        .unwrap();
        trailing.push(0);
        assert!(matches!(
            PeerVoteRequest::decode(&trailing),
            Err(CodecError::Trailing(1))
        ));

        assert!(matches!(
            PeerAppendResponse::decode(&[9]),
            Err(CodecError::UnknownTag { .. })
        ));
    }
}

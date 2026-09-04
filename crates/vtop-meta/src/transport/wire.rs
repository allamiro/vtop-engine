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
use crate::wire::{
    put_bounded_str, put_bytes32, put_i64, put_u16, put_u32, put_u64, put_u8, put_uuid, CodecError,
    Reader,
};
use std::io;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

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
pub const KIND_ADMIN_INIT_REQ: u16 = 15;
pub const KIND_ADMIN_ADD_LEARNER_REQ: u16 = 16;
pub const KIND_ADMIN_CHANGE_MEMBERSHIP_REQ: u16 = 17;
pub const KIND_ADMIN_MEMBERSHIP_RESP: u16 = 18;
// Linearizable range-lease read (#223). A candidate must be able to see
// whether a lease is still live before it tries to take one; without this it
// can only guess and learn from a rejection, which means it cannot distinguish
// "the leader is healthy" from "my generation was stale".
pub const KIND_ADMIN_READ_RANGE_LEASE_REQ: u16 = 19;
pub const KIND_ADMIN_READ_RANGE_LEASE_RESP: u16 = 20;
pub const KIND_ADMIN_READ_SEGMENT_PLACEMENT_REQ: u16 = 21;
pub const KIND_ADMIN_READ_SEGMENT_PLACEMENT_RESP: u16 = 22;
pub const KIND_ADMIN_READ_RANGE_TRANSITIONS_REQ: u16 = 23;
pub const KIND_ADMIN_READ_RANGE_TRANSITIONS_RESP: u16 = 24;

/// The most transition records one read returns; a longer chain is paged
/// with `from_epoch`.
pub const MAX_TRANSITIONS_PER_READ: usize = 256;

/// Bound for node ids carried in one admin membership request. This is
/// intentionally tighter than the storage codec's compatibility ceiling.
pub const MAX_ADMIN_MEMBERSHIP_NODES: usize = 64;

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
    /// The client authenticated but is not permitted to make this request
    /// (#238). Distinct from [`TransportError::Identity`], which means the
    /// certificate itself could not be read: this one carries a known caller
    /// and a refused action, so an operator debugging a denial can tell
    /// "wrong certificate" from "wrong permissions".
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("{0}")]
    Protocol(String),
    /// The peer refused because it does not lead; ask `leader` instead.
    ///
    /// Its own variant rather than a [`TransportError::Protocol`] carrying the
    /// same words, because this is the one refusal a client can DO something
    /// about, and only a type makes that actionable. Folded into `Protocol` it
    /// was indistinguishable from a genuine rejection, so no caller ever
    /// retried and a non-leader was a permanent dead end (#292).
    #[error("{message}")]
    NotLeader {
        message: String,
        leader: Option<MetaNodeId>,
    },
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

/// The segment record itself, as of the same fenced read.
///
/// CARRIED ALONGSIDE THE PLACEMENT because the flow needs both and they are
/// different records. All four replacement commands take
/// `--expected-segment-generation` as well as
/// `--expected-placement-generation`, and returning only the latter left the
/// caller exactly where they started for the other half: retaining a prior
/// `Ack` or guessing until a write was accepted.
///
/// `content_root` is here so an operator can compare the proof they are about
/// to submit against what metadata already believes, before spending a round
/// trip through consensus to be told they disagree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdminSegmentView {
    pub segment_generation: u64,
    pub base_offset: u64,
    pub next_offset: u64,
    pub content_root: [u8; 32],
    /// The same tags the durable encoding uses, so the two cannot drift apart
    /// in meaning.
    pub state_tag: u8,
    pub sealed_by_epoch: u64,
}

impl AdminSegmentView {
    /// The state name an operator reads, or `None` for a tag this build does
    /// not know.
    ///
    /// `None` rather than a guess: a newer leader can commit a state this
    /// binary has never heard of, and printing the nearest known name would be
    /// a confident lie about a segment's lifecycle.
    pub fn state_name(&self) -> Option<&'static str> {
        Some(match self.state_tag {
            1 => "SealedUnverified",
            2 => "Verified",
            3 => "Repairing",
            4 => "RetirePlanned",
            5 => "Retired",
            6 => "Quarantined",
            7 => "RetentionPlanned",
            8 => "RetentionExpired",
            _ => return None,
        })
    }
}

/// Which range's transition chain to read, from which epoch, how many at
/// most (#240 item 5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdminReadRangeTransitionsRequest {
    pub topic_uuid: Uuid,
    pub range_uuid: Uuid,
    /// The first epoch to return; the chain is read upward from here.
    pub from_epoch: u64,
    /// Clamped to [`MAX_TRANSITIONS_PER_READ`] by the server.
    pub limit: u16,
}

impl AdminReadRangeTransitionsRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_uuid(&mut out, self.topic_uuid);
        put_uuid(&mut out, self.range_uuid);
        put_u64(&mut out, self.from_epoch);
        put_u16(&mut out, self.limit);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let request = Self {
            topic_uuid: reader.uuid("topic uuid")?,
            range_uuid: reader.uuid("range uuid")?,
            from_epoch: reader.u64("from epoch")?,
            limit: reader.u16("limit")?,
        };
        reader.finish()?;
        Ok(request)
    }
}

/// One transition record as the admin wire carries it: the state machine's
/// [`crate::state::RangeTransitionRecord`], field for field, encoded with
/// the same primitives the durable value uses so the two cannot drift in
/// meaning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminTransitionView {
    pub epoch_from: u64,
    pub epoch_to: u64,
    pub holder_from: Option<Uuid>,
    pub holder_to: Uuid,
    pub grant: crate::state::GrantKind,
    pub granted_at_ms: i64,
    pub granted_apply_index: u64,
    pub outcome: crate::state::TransitionOutcome,
}

impl From<crate::state::RangeTransitionRecord> for AdminTransitionView {
    fn from(record: crate::state::RangeTransitionRecord) -> Self {
        Self {
            epoch_from: record.epoch_from,
            epoch_to: record.epoch_to,
            holder_from: record.holder_from,
            holder_to: record.holder_to,
            grant: record.grant,
            granted_at_ms: record.granted_at_ms,
            granted_apply_index: record.granted_apply_index,
            outcome: record.outcome,
        }
    }
}

impl AdminTransitionView {
    fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_u64(out, self.epoch_from);
        put_u64(out, self.epoch_to);
        match self.holder_from {
            None => put_u8(out, 0),
            Some(holder) => {
                put_u8(out, 1);
                put_uuid(out, holder);
            }
        }
        put_uuid(out, self.holder_to);
        put_u8(
            out,
            match self.grant {
                crate::state::GrantKind::Election => 1,
                crate::state::GrantKind::Administrative => 2,
            },
        );
        put_i64(out, self.granted_at_ms);
        put_u64(out, self.granted_apply_index);
        match &self.outcome {
            crate::state::TransitionOutcome::Pending => put_u8(out, 0),
            crate::state::TransitionOutcome::Reported {
                outcome,
                reported_at_ms,
                reported_apply_index,
            } => {
                put_u8(out, 1);
                crate::command::encode_promotion_outcome(out, outcome)?;
                put_i64(out, *reported_at_ms);
                put_u64(out, *reported_apply_index);
            }
        }
        Ok(())
    }

    fn decode_from(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        Ok(Self {
            epoch_from: reader.u64("transition epoch from")?,
            epoch_to: reader.u64("transition epoch to")?,
            holder_from: if reader.flag("transition holder-from presence")? {
                Some(reader.uuid("transition holder from")?)
            } else {
                None
            },
            holder_to: reader.uuid("transition holder to")?,
            grant: match reader.u8("transition grant kind")? {
                1 => crate::state::GrantKind::Election,
                2 => crate::state::GrantKind::Administrative,
                other => {
                    return Err(CodecError::UnknownTag {
                        what: "transition grant kind",
                        tag: u32::from(other),
                    })
                }
            },
            granted_at_ms: reader.i64("transition granted at")?,
            granted_apply_index: reader.u64("transition granted apply index")?,
            outcome: if reader.flag("transition outcome presence")? {
                crate::state::TransitionOutcome::Reported {
                    outcome: crate::command::decode_promotion_outcome(reader)?,
                    reported_at_ms: reader.i64("transition reported at")?,
                    reported_apply_index: reader.u64("transition reported apply index")?,
                }
            } else {
                crate::state::TransitionOutcome::Pending
            },
        })
    }
}

/// A range's transition chain, as of a linearizable read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminReadRangeTransitionsResponse {
    /// False when the range does not exist. A range that exists with no
    /// transitions has simply never been granted.
    pub found: bool,
    /// In epoch order, from the requested epoch upward.
    pub transitions: Vec<AdminTransitionView>,
    pub read_at_applied_index: u64,
}

impl AdminReadRangeTransitionsResponse {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        if self.transitions.len() > MAX_TRANSITIONS_PER_READ {
            return Err(CodecError::BoundExceeded {
                what: "transitions per read",
                actual: self.transitions.len(),
                maximum: MAX_TRANSITIONS_PER_READ,
            });
        }
        let mut out = Vec::new();
        put_u8(&mut out, u8::from(self.found));
        put_u16(&mut out, self.transitions.len() as u16);
        for transition in &self.transitions {
            transition.encode_into(&mut out)?;
        }
        put_u64(&mut out, self.read_at_applied_index);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let found = reader.flag("found")?;
        let count = reader.u16("transition count")? as usize;
        if count > MAX_TRANSITIONS_PER_READ {
            return Err(CodecError::BoundExceeded {
                what: "transitions per read",
                actual: count,
                maximum: MAX_TRANSITIONS_PER_READ,
            });
        }
        let mut transitions = Vec::with_capacity(count);
        for _ in 0..count {
            transitions.push(AdminTransitionView::decode_from(&mut reader)?);
        }
        let response = Self {
            found,
            transitions,
            read_at_applied_index: reader.u64("read at applied index")?,
        };
        reader.finish()?;
        Ok(response)
    }
}

/// Which segment to read the placement for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdminReadSegmentPlacementRequest {
    pub topic_uuid: Uuid,
    pub range_uuid: Uuid,
    pub segment_uuid: Uuid,
    /// Ask what the deterministic placement WOULD be at this factor. Zero
    /// means "do not compute one".
    ///
    /// Necessary because `CommitSegmentPlacement` compares a proposal
    /// positionally against `select_replicas` over the currently Active nodes,
    /// and an operator can see neither the candidate set nor the algorithm. A
    /// FIRST placement was therefore uncomputable from outside: there was no
    /// prior placement to read, and the only route to the right list was to
    /// guess an order and resubmit until one was accepted.
    pub for_replication_factor: u8,
}

impl AdminReadSegmentPlacementRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_uuid(&mut out, self.topic_uuid);
        put_uuid(&mut out, self.range_uuid);
        put_uuid(&mut out, self.segment_uuid);
        put_u8(&mut out, self.for_replication_factor);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let request = Self {
            topic_uuid: reader.uuid("topic uuid")?,
            range_uuid: reader.uuid("range uuid")?,
            segment_uuid: reader.uuid("segment uuid")?,
            for_replication_factor: reader.u8("for replication factor")?,
        };
        reader.finish()?;
        Ok(request)
    }
}

/// An open rebalance move, if one is in flight.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdminRebalanceIntentView {
    pub from_node_uuid: Uuid,
    pub to_node_uuid: Uuid,
    pub placement_generation_at_proposal: u64,
}

/// A segment's placement plus the tokens needed to act on it.
///
/// Carries the generation because every command in the replacement flow is a
/// compare-and-swap against it, and the replica list IN PLACEMENT ORDER
/// because `CommitSegmentPlacement` compares proposals positionally. Reading
/// either without the other leaves the caller guessing at half the input,
/// which is the situation this read exists to remove.
///
/// The open intent is here for the same reason. It is not decoration: while
/// one stands, the segment refuses placement updates, further proposals and
/// retention planning, so an operator who cannot see it has no way to tell a
/// stale generation from a blocked segment — the two produce similar-looking
/// rejections and want opposite responses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminReadSegmentPlacementResponse {
    /// False when no placement has been committed for this segment. Distinct
    /// from a placement that exists with an empty replica list, which would be
    /// a placement somebody committed and then emptied.
    pub found: bool,
    pub generation: u64,
    /// The durability TARGET, never inferred from the list length: the list
    /// runs at RF + 1 while a rebalance is in flight and below it after a
    /// plain retirement.
    pub declared_replication_factor: u8,
    /// In placement order. `CommitSegmentPlacement` compares positionally, so
    /// the order is part of the value rather than a presentation detail.
    pub replica_nodes: Vec<Uuid>,
    pub rebalance_intent: Option<AdminRebalanceIntentView>,
    /// The segment record. `None` when no segment by that id exists, which is
    /// a different answer from a segment with no placement.
    pub segment: Option<AdminSegmentView>,
    /// What the deterministic placement would be at the requested factor, or
    /// why it cannot be computed.
    ///
    /// `None` when the request did not ask. `Err` carries the algorithm's own
    /// refusal — most often too few distinct failure domains, which is the
    /// answer an operator most needs, because the remedy is
    /// `set-node-placement-attrs` on nodes that currently have no domain and
    /// nothing else in the flow would have said so.
    pub proposal: Option<Result<Vec<Uuid>, String>>,
    /// The applied index the read was fenced at, so a caller can tell an
    /// answer from a newer state machine apart from a replayed older one.
    pub read_at_applied_index: u64,
}

impl AdminReadSegmentPlacementResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_u8(&mut out, u8::from(self.found));
        put_u64(&mut out, self.generation);
        put_u8(&mut out, self.declared_replication_factor);
        put_u32(&mut out, self.replica_nodes.len() as u32);
        for node in &self.replica_nodes {
            put_uuid(&mut out, *node);
        }
        match &self.rebalance_intent {
            None => put_u8(&mut out, 0),
            Some(intent) => {
                put_u8(&mut out, 1);
                put_uuid(&mut out, intent.from_node_uuid);
                put_uuid(&mut out, intent.to_node_uuid);
                put_u64(&mut out, intent.placement_generation_at_proposal);
            }
        }
        match &self.segment {
            None => put_u8(&mut out, 0),
            Some(segment) => {
                put_u8(&mut out, 1);
                put_u64(&mut out, segment.segment_generation);
                put_u64(&mut out, segment.base_offset);
                put_u64(&mut out, segment.next_offset);
                put_bytes32(&mut out, &segment.content_root);
                put_u8(&mut out, segment.state_tag);
                put_u64(&mut out, segment.sealed_by_epoch);
            }
        }
        match &self.proposal {
            None => put_u8(&mut out, 0),
            Some(Ok(nodes)) => {
                put_u8(&mut out, 1);
                put_u32(&mut out, nodes.len() as u32);
                for node in nodes {
                    put_uuid(&mut out, *node);
                }
            }
            Some(Err(reason)) => {
                put_u8(&mut out, 2);
                // TRUNCATED rather than refused. `encode` has no error channel,
                // and this string is diagnostic prose from the placement
                // algorithm — a reason clipped at 512 bytes still tells the
                // operator which remedy they need, while a failure to encode
                // would replace a usable answer with none at all. Cut on a char
                // boundary so the result stays valid UTF-8.
                let mut clipped = reason.as_str();
                while clipped.len() > MAX_PROPOSAL_REASON {
                    clipped = &clipped[..clipped
                        .char_indices()
                        .take_while(|(index, _)| *index < MAX_PROPOSAL_REASON)
                        .last()
                        .map_or(0, |(index, _)| index)];
                }
                let _ = put_bounded_str(
                    &mut out,
                    clipped,
                    MAX_PROPOSAL_REASON,
                    "placement proposal refusal",
                );
            }
        }
        put_u64(&mut out, self.read_at_applied_index);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let found = reader.flag("found")?;
        let generation = reader.u64("placement generation")?;
        let declared_replication_factor = reader.u8("declared replication factor")?;
        let count = reader.u32("replica count")?;
        // CHECKED BEFORE THE LOOP, not inside it. Rejecting only after the
        // first 1024 UUIDs have been read makes the bound a limit on what is
        // decoded rather than on what may be CLAIMED — a peer sending an
        // absurd count with no body still gets the decoder to allocate and
        // work through it, and the refusal it eventually earns is "truncated",
        // which describes the symptom rather than the lie.
        if count > MAX_REPLICA_NODES {
            return Err(CodecError::BoundExceeded {
                what: "replica node list",
                actual: count as usize,
                maximum: MAX_REPLICA_NODES as usize,
            });
        }
        let mut replica_nodes = Vec::with_capacity(count as usize);
        for _ in 0..count {
            replica_nodes.push(reader.uuid("replica node uuid")?);
        }
        let rebalance_intent = match reader.u8("rebalance intent presence")? {
            0 => None,
            1 => Some(AdminRebalanceIntentView {
                from_node_uuid: reader.uuid("rebalance from uuid")?,
                to_node_uuid: reader.uuid("rebalance to uuid")?,
                placement_generation_at_proposal: reader.u64("rebalance placement generation")?,
            }),
            other => {
                return Err(CodecError::UnknownTag {
                    what: "rebalance intent presence",
                    tag: u32::from(other),
                })
            }
        };
        let segment = match reader.u8("segment presence")? {
            0 => None,
            1 => Some(AdminSegmentView {
                segment_generation: reader.u64("segment generation")?,
                base_offset: reader.u64("segment base offset")?,
                next_offset: reader.u64("segment next offset")?,
                content_root: reader.bytes32("segment content root")?,
                state_tag: reader.u8("segment state tag")?,
                sealed_by_epoch: reader.u64("segment sealed by epoch")?,
            }),
            other => {
                return Err(CodecError::UnknownTag {
                    what: "segment presence",
                    tag: u32::from(other),
                })
            }
        };
        let proposal = match reader.u8("proposal presence")? {
            0 => None,
            1 => {
                let count = reader.u32("proposed replica count")?;
                if count > MAX_REPLICA_NODES {
                    return Err(CodecError::BoundExceeded {
                        what: "proposed replica node list",
                        actual: count as usize,
                        maximum: MAX_REPLICA_NODES as usize,
                    });
                }
                let mut nodes = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    nodes.push(reader.uuid("proposed replica node uuid")?);
                }
                Some(Ok(nodes))
            }
            2 => Some(Err(
                reader.bounded_str(MAX_PROPOSAL_REASON, "placement proposal refusal")?
            )),
            other => {
                return Err(CodecError::UnknownTag {
                    what: "proposal presence",
                    tag: u32::from(other),
                })
            }
        };
        let read_at_applied_index = reader.u64("read at applied index")?;
        reader.finish()?;
        Ok(Self {
            found,
            generation,
            declared_replication_factor,
            replica_nodes,
            rebalance_intent,
            segment,
            proposal,
            read_at_applied_index,
        })
    }
}

/// A ceiling on the replica list a peer may claim to be sending.
///
/// Not a policy limit on replication factor — it is far above any real one.
/// It exists so `decode` cannot be told to reserve an enormous vector before
/// it has read anything that would justify the size.
const MAX_REPLICA_NODES: u32 = 1024;

/// A ceiling on the placement algorithm's refusal text.
///
/// Generous for a sentence naming failure domains, and small enough that a
/// peer cannot make a reader hold an unbounded string.
const MAX_PROPOSAL_REASON: usize = 512;

/// Which range to read the lease for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdminReadRangeLeaseRequest {
    pub topic_uuid: Uuid,
    pub range_uuid: Uuid,
}

impl AdminReadRangeLeaseRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_uuid(&mut out, self.topic_uuid);
        put_uuid(&mut out, self.range_uuid);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let request = Self {
            topic_uuid: reader.uuid("topic uuid")?,
            range_uuid: reader.uuid("range uuid")?,
        };
        reader.finish()?;
        Ok(request)
    }
}

/// The lease a range currently holds, as of a linearizable read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdminLeaseView {
    pub holder_node_uuid: Uuid,
    pub fencing_epoch: u64,
    /// `None` for an administrative grant, which never expires.
    pub expires_at_ms: Option<i64>,
}

/// A range's lease state plus the CAS token needed to act on it.
///
/// `range_generation` is carried because acquisition is a compare-and-swap
/// against it: reading the lease without the token it must be paired with
/// would leave a candidate guessing, which is the situation this read exists
/// to remove.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdminReadRangeLeaseResponse {
    /// False when the range does not exist. Distinct from "exists with no
    /// lease", which is a range nobody currently leads.
    pub found: bool,
    pub range_generation: u64,
    /// Highest epoch ever minted for the range; never rewinds, even on
    /// release.
    pub fencing_epoch: u64,
    pub lease: Option<AdminLeaseView>,
    /// The applied index the read was fenced at, so a caller can tell an
    /// answer from a newer state machine apart from a replayed older one.
    pub read_at_applied_index: u64,
}

impl AdminReadRangeLeaseResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_u8(&mut out, u8::from(self.found));
        put_u64(&mut out, self.range_generation);
        put_u64(&mut out, self.fencing_epoch);
        match &self.lease {
            None => put_u8(&mut out, 0),
            Some(lease) => {
                // Presence 1 = no deadline, 2 = deadline, mirroring the
                // durable encoding in `MetaValue::Range` so the two cannot
                // drift apart in meaning.
                match lease.expires_at_ms {
                    None => put_u8(&mut out, 1),
                    Some(_) => put_u8(&mut out, 2),
                }
                put_uuid(&mut out, lease.holder_node_uuid);
                put_u64(&mut out, lease.fencing_epoch);
                if let Some(expires_at_ms) = lease.expires_at_ms {
                    put_i64(&mut out, expires_at_ms);
                }
            }
        }
        put_u64(&mut out, self.read_at_applied_index);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let found = reader.flag("found")?;
        let range_generation = reader.u64("range generation")?;
        let fencing_epoch = reader.u64("fencing epoch")?;
        let lease = match reader.u8("lease presence")? {
            0 => None,
            presence @ (1 | 2) => Some(AdminLeaseView {
                holder_node_uuid: reader.uuid("lease holder uuid")?,
                fencing_epoch: reader.u64("lease fencing epoch")?,
                expires_at_ms: match presence {
                    2 => Some(reader.i64("lease expiry ms")?),
                    _ => None,
                },
            }),
            _ => {
                return Err(CodecError::InvalidValue {
                    what: "lease presence",
                    reason: "lease presence byte must be 0, 1, or 2",
                })
            }
        };
        let read_at_applied_index = reader.u64("read at applied index")?;
        reader.finish()?;
        Ok(Self {
            found,
            range_generation,
            fencing_epoch,
            lease,
            read_at_applied_index,
        })
    }
}

fn put_node_id_list(out: &mut Vec<u8>, ids: &[u64], what: &'static str) -> Result<(), CodecError> {
    if ids.len() > MAX_ADMIN_MEMBERSHIP_NODES {
        return Err(CodecError::BoundExceeded {
            what,
            actual: ids.len(),
            maximum: MAX_ADMIN_MEMBERSHIP_NODES,
        });
    }
    put_u32(out, ids.len() as u32);
    for id in ids {
        put_u64(out, *id);
    }
    Ok(())
}

fn take_node_id_list(reader: &mut Reader<'_>, what: &'static str) -> Result<Vec<u64>, CodecError> {
    let len = reader.u32(what)? as usize;
    if len > MAX_ADMIN_MEMBERSHIP_NODES {
        return Err(CodecError::BoundExceeded {
            what,
            actual: len,
            maximum: MAX_ADMIN_MEMBERSHIP_NODES,
        });
    }
    let mut ids = Vec::with_capacity(len);
    for _ in 0..len {
        ids.push(reader.u64(what)?);
    }
    Ok(ids)
}

/// Bootstrap a fresh Raft group with these voter ids (one-shot; the target
/// node must have an empty log). #215 live-cluster surface.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminInitRequest {
    pub members: Vec<u64>,
}

impl AdminInitRequest {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::new();
        put_node_id_list(&mut out, &self.members, "init members")?;
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let members = take_node_id_list(&mut reader, "init members")?;
        reader.finish()?;
        Ok(Self { members })
    }
}

/// Add a learner that starts replicating the log without joining quorum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminAddLearnerRequest {
    pub node_id: u64,
}

impl AdminAddLearnerRequest {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::new();
        put_u64(&mut out, self.node_id);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let node_id = reader.u64("learner id")?;
        reader.finish()?;
        Ok(Self { node_id })
    }
}

/// Replace the voter set through a joint-consensus transition. Removed
/// voters can no longer form a quorum or accept leadership proposals.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminChangeMembershipRequest {
    pub voters: Vec<u64>,
    /// Keep removed voters as learners instead of dropping them entirely.
    pub retain_removed_as_learners: bool,
}

impl AdminChangeMembershipRequest {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::new();
        put_node_id_list(&mut out, &self.voters, "membership voters")?;
        put_u8(&mut out, u8::from(self.retain_removed_as_learners));
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let voters = take_node_id_list(&mut reader, "membership voters")?;
        let retain = match reader.u8("retain flag")? {
            0 => false,
            1 => true,
            _ => {
                return Err(CodecError::InvalidValue {
                    what: "retain flag",
                    reason: "expected 0 or 1",
                })
            }
        };
        reader.finish()?;
        Ok(Self {
            voters,
            retain_removed_as_learners: retain,
        })
    }
}

/// Shared success reply for init/add-learner/change-membership: the
/// membership as the proposing node now sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdminMembershipResponse {
    pub membership: MetaMembership,
}

impl AdminMembershipResponse {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::new();
        let membership = self.membership.encode()?;
        put_u32(&mut out, membership.len() as u32);
        out.extend_from_slice(&membership);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let membership_len = reader.u32("membership len")? as usize;
        let membership = MetaMembership::decode(reader.take(membership_len, "membership")?)?;
        reader.finish()?;
        Ok(Self { membership })
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
    /// Set when the refusal was "you asked the wrong node": the metadata node
    /// the caller should ask instead, or `None` when even the answering node
    /// does not know who leads.
    ///
    /// A MACHINE-READABLE redirect. The message has always said as much in
    /// prose, and prose is not something a client can act on without matching
    /// English against a consensus-engine version it does not control — so nothing
    /// did, and every request from a node that was not co-located with the
    /// Raft leader failed permanently (#292).
    ///
    /// An id rather than an address, because an id is all Raft has: this type
    /// config uses `EmptyNode`, so no peer address exists to send. The caller
    /// resolves it against the peer list it was configured with.
    pub not_leader: Option<NotLeaderHint>,
}

/// Who to ask instead, when a node refuses because it does not lead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotLeaderHint {
    /// `None` means the answering node has no leader either — a genuine
    /// election gap, which a caller should retry rather than redirect.
    pub leader: Option<MetaNodeId>,
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
        // NOTHING APPENDED when there is no redirect, which keeps the common
        // case byte-identical to the pre-#292 encoding.
        //
        // This is not tidiness, it is version skew. An older decoder ends with
        // `finish()`, which rejects trailing bytes — so unconditionally
        // appending a tag would make an old client fail to read EVERY admin
        // error, not just a redirect. It would lose the message at exactly the
        // moment the message is the only thing it has. Emitting the extension
        // only when there is something to say means an old client keeps working
        // for every ordinary error, and the one frame it cannot read is the one
        // it could never have acted on anyway.
        //
        // Two tags rather than one, so "a redirect to a known node" and "a
        // redirect with no leader yet" stay distinct. Collapsing them would make
        // an election gap look like a routing decision, and a client would chase
        // a leader that does not exist.
        match self.not_leader {
            None => {}
            Some(NotLeaderHint { leader: None }) => out.push(1),
            Some(NotLeaderHint { leader: Some(id) }) => {
                out.push(2);
                out.extend_from_slice(&id.0.to_be_bytes());
            }
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let message = reader.bounded_str(MAX_ERROR_DETAIL_BYTES, "admin error")?;
        // TOLERANT of the older encoding, which had no redirect at all. A
        // frame that stops here is a pre-#292 peer, and "it did not tell us"
        // is exactly `None` — the same thing that field means for any error
        // that is not a redirect. Refusing it instead would turn a version skew
        // into an unreadable error, which is the worst moment to lose the
        // message.
        let not_leader = if reader.remaining() == 0 {
            None
        } else {
            match reader.u8("admin error redirect tag")? {
                // 0 was emitted by an earlier revision of this branch to mean
                // "not a redirect". Nothing writes it now — absence carries that
                // meaning — but it decodes, so a peer built from that revision
                // stays readable.
                0 => None,
                1 => Some(NotLeaderHint { leader: None }),
                2 => Some(NotLeaderHint {
                    leader: Some(MetaNodeId(reader.u64("admin error leader id")?)),
                }),
                other => {
                    return Err(CodecError::UnknownTag {
                        what: "admin error redirect",
                        tag: u32::from(other),
                    })
                }
            }
        };
        reader.finish()?;
        Ok(Self {
            message,
            not_leader,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The redirect round-trips in all three of its states, and an OLD frame
    /// still decodes.
    ///
    /// Old frames matter because the field is additive: a pre-#292 peer sends
    /// a message and nothing else, and refusing that would turn a version skew
    /// into an unreadable error — at the exact moment the message is what you
    /// need. "It did not tell us" is `None`, the same as any non-redirect.
    #[test]
    fn admin_error_round_trips_its_redirect_and_still_reads_the_older_encoding() {
        for not_leader in [
            None,
            Some(NotLeaderHint { leader: None }),
            Some(NotLeaderHint {
                leader: Some(MetaNodeId(7)),
            }),
        ] {
            let error = AdminError {
                message: "nope".to_owned(),
                not_leader,
            };
            let decoded = AdminError::decode(&error.encode().unwrap()).unwrap();
            assert_eq!(decoded, error);
        }

        // BYTE-IDENTICAL to the pre-#292 encoding when there is no redirect,
        // which is what keeps an older peer able to read ordinary errors. If
        // this ever regresses, every old client loses every error message.
        let mut legacy_shape = Vec::new();
        put_bounded_str(
            &mut legacy_shape,
            "nope",
            MAX_ERROR_DETAIL_BYTES,
            "admin error",
        )
        .unwrap();
        assert_eq!(
            AdminError {
                message: "nope".to_owned(),
                not_leader: None,
            }
            .encode()
            .unwrap(),
            legacy_shape,
            "a non-redirect error must encode exactly as it did before the \
             redirect field existed, or an older decoder rejects it as trailing \
             bytes and loses the message"
        );

        // The pre-#292 encoding: a bounded string and nothing after it.
        let mut legacy = Vec::new();
        put_bounded_str(
            &mut legacy,
            "older peer",
            MAX_ERROR_DETAIL_BYTES,
            "admin error",
        )
        .unwrap();
        let decoded = AdminError::decode(&legacy).expect("an older frame must still decode");
        assert_eq!(decoded.message, "older peer");
        assert_eq!(
            decoded.not_leader, None,
            "an older peer told us nothing about leadership, which is not the \
             same as telling us it leads"
        );

        // And an unknown tag is still refused rather than guessed at.
        let mut bogus = Vec::new();
        put_bounded_str(&mut bogus, "x", MAX_ERROR_DETAIL_BYTES, "admin error").unwrap();
        bogus.push(9);
        assert!(AdminError::decode(&bogus).is_err());
    }

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
                joint_outgoing: None,
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

    /// The transition read round-trips in both outcome shapes and refuses a
    /// page longer than the bound (#240 item 5).
    #[test]
    fn range_transitions_read_round_trips_and_is_bounded() {
        let request = AdminReadRangeTransitionsRequest {
            topic_uuid: Uuid::from_u128(20),
            range_uuid: Uuid::from_u128(21),
            from_epoch: 3,
            limit: 16,
        };
        assert_eq!(
            AdminReadRangeTransitionsRequest::decode(&request.encode()).unwrap(),
            request
        );
        let pending = AdminTransitionView {
            epoch_from: 0,
            epoch_to: 1,
            holder_from: None,
            holder_to: Uuid::from_u128(10),
            grant: crate::state::GrantKind::Election,
            granted_at_ms: 1_000,
            granted_apply_index: 4,
            outcome: crate::state::TransitionOutcome::Pending,
        };
        let served = AdminTransitionView {
            epoch_from: 1,
            epoch_to: 2,
            holder_from: Some(Uuid::from_u128(10)),
            holder_to: Uuid::from_u128(11),
            grant: crate::state::GrantKind::Administrative,
            granted_at_ms: 2_000,
            granted_apply_index: 9,
            outcome: crate::state::TransitionOutcome::Reported {
                outcome: crate::command::PromotionOutcome::Served {
                    boundary_offset: Some(401),
                    sealed_prefix_end: Some(300),
                    quorum: vec![crate::command::QuorumAnswer {
                        node_uuid: Uuid::from_u128(11),
                        offset: 401,
                    }],
                    votes: 1,
                    required: 1,
                },
                reported_at_ms: 2_500,
                reported_apply_index: 12,
            },
        };
        let response = AdminReadRangeTransitionsResponse {
            found: true,
            transitions: vec![pending, served],
            read_at_applied_index: 12,
        };
        assert_eq!(
            AdminReadRangeTransitionsResponse::decode(&response.encode().unwrap()).unwrap(),
            response
        );
        let mut oversized = response.clone();
        oversized.transitions =
            vec![oversized.transitions[0].clone(); MAX_TRANSITIONS_PER_READ + 1];
        assert!(matches!(
            oversized.encode(),
            Err(CodecError::BoundExceeded { .. })
        ));
    }

    #[test]
    fn admin_payloads_round_trip() {
        let read = AdminReadRangeLeaseRequest {
            topic_uuid: Uuid::from_u128(7),
            range_uuid: Uuid::from_u128(8),
        };
        assert_eq!(
            AdminReadRangeLeaseRequest::decode(&read.encode()).unwrap(),
            read
        );

        // All three lease shapes: absent, administrative (no deadline), and
        // election-granted (with one). The middle case is the one a naive
        // codec collapses into the third.
        for lease in [
            None,
            Some(AdminLeaseView {
                holder_node_uuid: Uuid::from_u128(9),
                fencing_epoch: 4,
                expires_at_ms: None,
            }),
            Some(AdminLeaseView {
                holder_node_uuid: Uuid::from_u128(9),
                fencing_epoch: 4,
                expires_at_ms: Some(1_700_000_000_000),
            }),
        ] {
            let response = AdminReadRangeLeaseResponse {
                found: true,
                range_generation: 12,
                fencing_epoch: 4,
                lease,
                read_at_applied_index: 99,
            };
            assert_eq!(
                AdminReadRangeLeaseResponse::decode(&response.encode()).unwrap(),
                response
            );
        }

        // A range that does not exist is not the same answer as a range with
        // no lease, and the codec must keep them apart.
        let missing = AdminReadRangeLeaseResponse {
            found: false,
            range_generation: 0,
            fencing_epoch: 0,
            lease: None,
            read_at_applied_index: 99,
        };
        assert_eq!(
            AdminReadRangeLeaseResponse::decode(&missing.encode()).unwrap(),
            missing
        );

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
                joint_outgoing: None,
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

        let init = AdminInitRequest {
            members: vec![1, 2, 3],
        };
        assert_eq!(
            AdminInitRequest::decode(&init.encode().unwrap()).unwrap(),
            init
        );

        let learner = AdminAddLearnerRequest { node_id: 4 };
        assert_eq!(
            AdminAddLearnerRequest::decode(&learner.encode().unwrap()).unwrap(),
            learner
        );

        let change = AdminChangeMembershipRequest {
            voters: vec![1, 2, 3, 4],
            retain_removed_as_learners: true,
        };
        assert_eq!(
            AdminChangeMembershipRequest::decode(&change.encode().unwrap()).unwrap(),
            change
        );

        let membership = AdminMembershipResponse {
            membership: MetaMembership {
                voters: vec![MetaNodeId(1), MetaNodeId(2), MetaNodeId(3), MetaNodeId(4)],
                learners: vec![(MetaNodeId(5), String::new())],
                joint_outgoing: Some(vec![MetaNodeId(1), MetaNodeId(2), MetaNodeId(3)]),
            },
        };
        assert_eq!(
            AdminMembershipResponse::decode(&membership.encode().unwrap()).unwrap(),
            membership
        );

        let oversized = AdminInitRequest {
            members: vec![1; MAX_ADMIN_MEMBERSHIP_NODES + 1],
        };
        assert!(matches!(
            oversized.encode(),
            Err(CodecError::BoundExceeded { .. })
        ));

        let mut noncanonical_flag = change.encode().unwrap();
        *noncanonical_flag.last_mut().unwrap() = 2;
        assert!(matches!(
            AdminChangeMembershipRequest::decode(&noncanonical_flag),
            Err(CodecError::InvalidValue { .. })
        ));
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

    /// Every field survives a round trip, including the two the replacement
    /// flow cannot work without.
    ///
    /// The replica list is checked for ORDER, not just membership: the state
    /// machine compares proposals positionally, so a codec that preserved the
    /// set while permuting it would break every multi-replica placement while
    /// looking correct in any test that sorted first.
    #[test]
    fn a_segment_placement_response_round_trips_with_its_order_intact() {
        let response = AdminReadSegmentPlacementResponse {
            found: true,
            generation: 9,
            declared_replication_factor: 3,
            replica_nodes: vec![
                Uuid::from_u128(0x33),
                Uuid::from_u128(0x11),
                Uuid::from_u128(0x22),
            ],
            rebalance_intent: Some(AdminRebalanceIntentView {
                from_node_uuid: Uuid::from_u128(0x11),
                to_node_uuid: Uuid::from_u128(0x44),
                placement_generation_at_proposal: 8,
            }),
            segment: Some(AdminSegmentView {
                segment_generation: 4,
                base_offset: 1_000,
                next_offset: 2_000,
                content_root: [0xAB; 32],
                state_tag: 2,
                sealed_by_epoch: 6,
            }),
            proposal: Some(Ok(vec![Uuid::from_u128(0x22), Uuid::from_u128(0x33)])),
            read_at_applied_index: 4_321,
        };
        let decoded = AdminReadSegmentPlacementResponse::decode(&response.encode()).unwrap();
        assert_eq!(decoded, response);
        assert_eq!(
            decoded.replica_nodes,
            vec![
                Uuid::from_u128(0x33),
                Uuid::from_u128(0x11),
                Uuid::from_u128(0x22)
            ],
            "placement order is the value, not a presentation detail"
        );

        // No intent, no placement: the shape an operator sees before the first
        // commit, and the one most likely to be mis-encoded because it is all
        // zeroes and absences.
        let empty = AdminReadSegmentPlacementResponse {
            found: false,
            generation: 0,
            declared_replication_factor: 0,
            replica_nodes: Vec::new(),
            rebalance_intent: None,
            segment: None,
            proposal: Some(Err(
                "only 1 distinct failure domain for 3 replicas".to_owned()
            )),
            read_at_applied_index: 7,
        };
        assert_eq!(
            AdminReadSegmentPlacementResponse::decode(&empty.encode()).unwrap(),
            empty
        );
    }

    /// A claimed replica count far beyond any real one is refused before the
    /// decoder allocates on the strength of it.
    #[test]
    fn a_placement_response_claiming_an_absurd_replica_count_is_refused() {
        let mut bytes = Vec::new();
        put_u8(&mut bytes, 1);
        put_u64(&mut bytes, 1);
        put_u8(&mut bytes, 3);
        put_u32(&mut bytes, u32::MAX);
        let error = AdminReadSegmentPlacementResponse::decode(&bytes)
            .expect_err("a length no cluster could produce must not be trusted");
        assert!(
            matches!(error, CodecError::BoundExceeded { .. }),
            "expected a bound error, got {error}"
        );
    }

    #[test]
    fn a_segment_placement_request_round_trips() {
        let request = AdminReadSegmentPlacementRequest {
            topic_uuid: Uuid::from_u128(1),
            range_uuid: Uuid::from_u128(2),
            segment_uuid: Uuid::from_u128(3),
            for_replication_factor: 3,
        };
        assert_eq!(
            AdminReadSegmentPlacementRequest::decode(&request.encode()).unwrap(),
            request
        );
    }

    /// An unknown state tag is reported as unknown rather than mapped to the
    /// nearest name a stale binary happens to know.
    ///
    /// A newer leader can commit a lifecycle state this build has never heard
    /// of. Guessing would tell an operator a segment is `Verified` when it is
    /// something else entirely — and they would act on it.
    #[test]
    fn an_unknown_segment_state_tag_is_not_guessed_at() {
        let known = AdminSegmentView {
            segment_generation: 1,
            base_offset: 0,
            next_offset: 1,
            content_root: [0; 32],
            state_tag: 4,
            sealed_by_epoch: 1,
        };
        assert_eq!(known.state_name(), Some("RetirePlanned"));

        let future = AdminSegmentView {
            state_tag: 99,
            ..known
        };
        assert_eq!(
            future.state_name(),
            None,
            "a tag from a newer leader must read as unknown, not as the nearest known state"
        );
    }
}

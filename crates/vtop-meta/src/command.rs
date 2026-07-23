//! Metadata commands, responses, and their hand-coded wire codec.
//!
//! A command is what the (future) raft log replicates; `apply` in
//! [`crate::state`] consumes exactly these types. The codec is the durable
//! byte format for `Normal` log entries, so it follows the crate's codec
//! discipline: `kind:u16` tag, big-endian integers, length-delimited bounded
//! strings, canonical option encoding, and trailing-byte rejection.
//!
//! Determinism contract: every id in a command (topic, range, segment, key,
//! request) is proposer-supplied, never allocated by the state machine, and
//! `issued_at_ms` is advisory — recorded for operators, never read by
//! `apply`.

use crate::keys::MAX_TOPIC_NAME_BYTES;
use crate::wire::{
    put_bounded_str, put_bytes32, put_i64, put_u16, put_u64, put_u8, put_uuid, CodecError, Reader,
};
use thiserror::Error;
use uuid::Uuid;

/// Node addresses are host:port style strings, bounded like every other
/// variable-length field so a command can never smuggle unbounded bytes.
pub const MAX_NODE_ADDR_BYTES: usize = 256;

/// Bound for the human-readable detail carried by deterministic rejections.
pub const MAX_ERROR_DETAIL_BYTES: usize = 256;

const COMMAND_KIND_REGISTER_NODE: u16 = 1;
const COMMAND_KIND_SET_NODE_STATE: u16 = 2;
const COMMAND_KIND_CREATE_TOPIC: u16 = 3;
const COMMAND_KIND_GRANT_RANGE_LEASE: u16 = 4;
const COMMAND_KIND_RELEASE_RANGE_LEASE: u16 = 5;
const COMMAND_KIND_REGISTER_SEALED_SEGMENT: u16 = 6;
const COMMAND_KIND_MARK_SEGMENT_VERIFIED: u16 = 7;
const COMMAND_KIND_PUT_KEY_RECORD: u16 = 8;

const RESPONSE_KIND_ACK: u16 = 1;
const RESPONSE_KIND_TOPIC_CREATED: u16 = 2;
const RESPONSE_KIND_LEASE_GRANTED: u16 = 3;
const RESPONSE_KIND_REJECTED: u16 = 4;

const ERROR_KIND_GENERATION_MISMATCH: u16 = 1;
const ERROR_KIND_EPOCH_MISMATCH: u16 = 2;
const ERROR_KIND_ALREADY_EXISTS: u16 = 3;
const ERROR_KIND_NOT_FOUND: u16 = 4;
const ERROR_KIND_INVALID_TRANSITION: u16 = 5;
const ERROR_KIND_LIMIT: u16 = 6;

/// Common prefix of every command. `request_id` keys the exactly-once dedup
/// table; `issued_at_ms` comes from the proposer's advisory clock and is
/// never read by `apply`, so wall-clock skew cannot change state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommandEnvelope {
    pub request_id: Uuid,
    pub issued_at_ms: i64,
}

/// Lifecycle state of a registered node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeState {
    Active,
    Draining,
    Dead,
}

impl NodeState {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeState::Active => "active",
            NodeState::Draining => "draining",
            NodeState::Dead => "dead",
        }
    }

    fn wire_tag(self) -> u8 {
        match self {
            NodeState::Active => 1,
            NodeState::Draining => 2,
            NodeState::Dead => 3,
        }
    }

    pub(crate) fn from_wire(tag: u8) -> Result<Self, CodecError> {
        match tag {
            1 => Ok(NodeState::Active),
            2 => Ok(NodeState::Draining),
            3 => Ok(NodeState::Dead),
            other => Err(CodecError::UnknownTag {
                what: "node state",
                tag: u32::from(other),
            }),
        }
    }
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The full deterministic command set of stage-5 PR 1.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataCommand {
    RegisterNode {
        env: CommandEnvelope,
        node_uuid: Uuid,
        addr: String,
        /// `None` expects the node to be absent (first registration);
        /// `Some(generation)` is a CAS re-registration of an existing node.
        expected_generation: Option<u64>,
    },
    SetNodeState {
        env: CommandEnvelope,
        node_uuid: Uuid,
        state: NodeState,
        expected_generation: u64,
    },
    CreateTopic {
        env: CommandEnvelope,
        name: String,
        topic_uuid: Uuid,
        root_range_uuid: Uuid,
    },
    GrantRangeLease {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        holder_node_uuid: Uuid,
        expected_range_generation: u64,
    },
    ReleaseRangeLease {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        expected_fencing_epoch: u64,
    },
    RegisterSealedSegment {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        segment_generation: u64,
        base_offset: u64,
        next_offset: u64,
        content_root: [u8; 32],
        sealed_by_epoch: u64,
        expected_range_generation: u64,
    },
    MarkSegmentVerified {
        env: CommandEnvelope,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        content_root: [u8; 32],
        expected_generation: u64,
    },
    PutKeyRecord {
        env: CommandEnvelope,
        key_uuid: Uuid,
        scheme: u16,
        public_material_digest: [u8; 32],
    },
}

impl MetadataCommand {
    pub fn envelope(&self) -> &CommandEnvelope {
        match self {
            MetadataCommand::RegisterNode { env, .. }
            | MetadataCommand::SetNodeState { env, .. }
            | MetadataCommand::CreateTopic { env, .. }
            | MetadataCommand::GrantRangeLease { env, .. }
            | MetadataCommand::ReleaseRangeLease { env, .. }
            | MetadataCommand::RegisterSealedSegment { env, .. }
            | MetadataCommand::MarkSegmentVerified { env, .. }
            | MetadataCommand::PutKeyRecord { env, .. } => env,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::with_capacity(96);
        match self {
            MetadataCommand::RegisterNode {
                env,
                node_uuid,
                addr,
                expected_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_REGISTER_NODE);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *node_uuid);
                put_bounded_str(&mut out, addr, MAX_NODE_ADDR_BYTES, "node address")?;
                encode_optional_u64(&mut out, *expected_generation);
            }
            MetadataCommand::SetNodeState {
                env,
                node_uuid,
                state,
                expected_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_SET_NODE_STATE);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *node_uuid);
                put_u8(&mut out, state.wire_tag());
                put_u64(&mut out, *expected_generation);
            }
            MetadataCommand::CreateTopic {
                env,
                name,
                topic_uuid,
                root_range_uuid,
            } => {
                put_u16(&mut out, COMMAND_KIND_CREATE_TOPIC);
                encode_envelope(&mut out, env);
                put_bounded_str(&mut out, name, MAX_TOPIC_NAME_BYTES, "topic name")?;
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *root_range_uuid);
            }
            MetadataCommand::GrantRangeLease {
                env,
                topic_uuid,
                range_uuid,
                holder_node_uuid,
                expected_range_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_GRANT_RANGE_LEASE);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *holder_node_uuid);
                put_u64(&mut out, *expected_range_generation);
            }
            MetadataCommand::ReleaseRangeLease {
                env,
                topic_uuid,
                range_uuid,
                expected_fencing_epoch,
            } => {
                put_u16(&mut out, COMMAND_KIND_RELEASE_RANGE_LEASE);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_u64(&mut out, *expected_fencing_epoch);
            }
            MetadataCommand::RegisterSealedSegment {
                env,
                topic_uuid,
                range_uuid,
                segment_uuid,
                segment_generation,
                base_offset,
                next_offset,
                content_root,
                sealed_by_epoch,
                expected_range_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_REGISTER_SEALED_SEGMENT);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
                put_u64(&mut out, *segment_generation);
                put_u64(&mut out, *base_offset);
                put_u64(&mut out, *next_offset);
                put_bytes32(&mut out, content_root);
                put_u64(&mut out, *sealed_by_epoch);
                put_u64(&mut out, *expected_range_generation);
            }
            MetadataCommand::MarkSegmentVerified {
                env,
                topic_uuid,
                range_uuid,
                segment_uuid,
                content_root,
                expected_generation,
            } => {
                put_u16(&mut out, COMMAND_KIND_MARK_SEGMENT_VERIFIED);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *topic_uuid);
                put_uuid(&mut out, *range_uuid);
                put_uuid(&mut out, *segment_uuid);
                put_bytes32(&mut out, content_root);
                put_u64(&mut out, *expected_generation);
            }
            MetadataCommand::PutKeyRecord {
                env,
                key_uuid,
                scheme,
                public_material_digest,
            } => {
                put_u16(&mut out, COMMAND_KIND_PUT_KEY_RECORD);
                encode_envelope(&mut out, env);
                put_uuid(&mut out, *key_uuid);
                put_u16(&mut out, *scheme);
                put_bytes32(&mut out, public_material_digest);
            }
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let command = Self::decode_from(&mut reader)?;
        reader.finish()?;
        Ok(command)
    }

    fn decode_from(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        let kind = reader.u16("command kind")?;
        match kind {
            COMMAND_KIND_REGISTER_NODE => {
                let env = decode_envelope(reader)?;
                Ok(MetadataCommand::RegisterNode {
                    env,
                    node_uuid: reader.uuid("node uuid")?,
                    addr: {
                        let addr = reader.bounded_str(MAX_NODE_ADDR_BYTES, "node address")?;
                        if addr.is_empty() {
                            return Err(CodecError::InvalidValue {
                                what: "node address",
                                reason: "must not be empty",
                            });
                        }
                        addr
                    },
                    expected_generation: decode_optional_u64(reader, "expected generation")?,
                })
            }
            COMMAND_KIND_SET_NODE_STATE => Ok(MetadataCommand::SetNodeState {
                env: decode_envelope(reader)?,
                node_uuid: reader.uuid("node uuid")?,
                state: NodeState::from_wire(reader.u8("node state")?)?,
                expected_generation: reader.u64("expected generation")?,
            }),
            COMMAND_KIND_CREATE_TOPIC => {
                let env = decode_envelope(reader)?;
                let name = reader.bounded_str(MAX_TOPIC_NAME_BYTES, "topic name")?;
                if name.is_empty() {
                    return Err(CodecError::InvalidValue {
                        what: "topic name",
                        reason: "must not be empty",
                    });
                }
                Ok(MetadataCommand::CreateTopic {
                    env,
                    name,
                    topic_uuid: reader.uuid("topic uuid")?,
                    root_range_uuid: reader.uuid("root range uuid")?,
                })
            }
            COMMAND_KIND_GRANT_RANGE_LEASE => Ok(MetadataCommand::GrantRangeLease {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                holder_node_uuid: reader.uuid("holder node uuid")?,
                expected_range_generation: reader.u64("expected range generation")?,
            }),
            COMMAND_KIND_RELEASE_RANGE_LEASE => Ok(MetadataCommand::ReleaseRangeLease {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                expected_fencing_epoch: reader.u64("expected fencing epoch")?,
            }),
            COMMAND_KIND_REGISTER_SEALED_SEGMENT => Ok(MetadataCommand::RegisterSealedSegment {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
                segment_generation: reader.u64("segment generation")?,
                base_offset: reader.u64("base offset")?,
                next_offset: reader.u64("next offset")?,
                content_root: reader.bytes32("content root")?,
                sealed_by_epoch: reader.u64("sealed-by epoch")?,
                expected_range_generation: reader.u64("expected range generation")?,
            }),
            COMMAND_KIND_MARK_SEGMENT_VERIFIED => Ok(MetadataCommand::MarkSegmentVerified {
                env: decode_envelope(reader)?,
                topic_uuid: reader.uuid("topic uuid")?,
                range_uuid: reader.uuid("range uuid")?,
                segment_uuid: reader.uuid("segment uuid")?,
                content_root: reader.bytes32("content root")?,
                expected_generation: reader.u64("expected generation")?,
            }),
            COMMAND_KIND_PUT_KEY_RECORD => Ok(MetadataCommand::PutKeyRecord {
                env: decode_envelope(reader)?,
                key_uuid: reader.uuid("key uuid")?,
                scheme: reader.u16("key scheme")?,
                public_material_digest: reader.bytes32("public material digest")?,
            }),
            other => Err(CodecError::UnknownTag {
                what: "command kind",
                tag: u32::from(other),
            }),
        }
    }
}

/// Deterministic rejection values. These are semantic outcomes of `apply`,
/// never I/O errors, so replaying the same log always reproduces them.
/// Convention: `expected` is what the proposer claimed, `actual` is the
/// authoritative value in the state machine.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum MetadataError {
    #[error("generation mismatch: proposer expected {expected}, state holds {actual}")]
    GenerationMismatch { expected: u64, actual: u64 },
    #[error("epoch mismatch: proposer expected {expected}, state holds {actual}")]
    EpochMismatch { expected: u64, actual: u64 },
    #[error("record already exists")]
    AlreadyExists,
    #[error("record not found")]
    NotFound,
    #[error("invalid transition: {0}")]
    InvalidTransition(String),
    #[error("limit violated: {0}")]
    Limit(String),
}

impl MetadataError {
    /// Build an `InvalidTransition` whose detail is truncated to the wire
    /// bound at a character boundary, so the error always encodes.
    pub fn invalid_transition(detail: impl Into<String>) -> Self {
        MetadataError::InvalidTransition(bound_detail(detail.into()))
    }

    /// Build a `Limit` with the same bounded-detail guarantee.
    pub fn limit(detail: impl Into<String>) -> Self {
        MetadataError::Limit(bound_detail(detail.into()))
    }

    fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), CodecError> {
        match self {
            MetadataError::GenerationMismatch { expected, actual } => {
                put_u16(out, ERROR_KIND_GENERATION_MISMATCH);
                put_u64(out, *expected);
                put_u64(out, *actual);
            }
            MetadataError::EpochMismatch { expected, actual } => {
                put_u16(out, ERROR_KIND_EPOCH_MISMATCH);
                put_u64(out, *expected);
                put_u64(out, *actual);
            }
            MetadataError::AlreadyExists => put_u16(out, ERROR_KIND_ALREADY_EXISTS),
            MetadataError::NotFound => put_u16(out, ERROR_KIND_NOT_FOUND),
            MetadataError::InvalidTransition(detail) => {
                put_u16(out, ERROR_KIND_INVALID_TRANSITION);
                put_bounded_str(out, detail, MAX_ERROR_DETAIL_BYTES, "error detail")?;
            }
            MetadataError::Limit(detail) => {
                put_u16(out, ERROR_KIND_LIMIT);
                put_bounded_str(out, detail, MAX_ERROR_DETAIL_BYTES, "error detail")?;
            }
        }
        Ok(())
    }

    fn decode_from(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        let kind = reader.u16("error kind")?;
        match kind {
            ERROR_KIND_GENERATION_MISMATCH => Ok(MetadataError::GenerationMismatch {
                expected: reader.u64("expected generation")?,
                actual: reader.u64("actual generation")?,
            }),
            ERROR_KIND_EPOCH_MISMATCH => Ok(MetadataError::EpochMismatch {
                expected: reader.u64("expected epoch")?,
                actual: reader.u64("actual epoch")?,
            }),
            ERROR_KIND_ALREADY_EXISTS => Ok(MetadataError::AlreadyExists),
            ERROR_KIND_NOT_FOUND => Ok(MetadataError::NotFound),
            ERROR_KIND_INVALID_TRANSITION => Ok(MetadataError::InvalidTransition(
                reader.bounded_str(MAX_ERROR_DETAIL_BYTES, "error detail")?,
            )),
            ERROR_KIND_LIMIT => Ok(MetadataError::Limit(
                reader.bounded_str(MAX_ERROR_DETAIL_BYTES, "error detail")?,
            )),
            other => Err(CodecError::UnknownTag {
                what: "error kind",
                tag: u32::from(other),
            }),
        }
    }
}

fn bound_detail(mut detail: String) -> String {
    if detail.len() > MAX_ERROR_DETAIL_BYTES {
        let mut cut = MAX_ERROR_DETAIL_BYTES;
        while !detail.is_char_boundary(cut) {
            cut -= 1;
        }
        detail.truncate(cut);
    }
    detail
}

/// What `apply` returns and what the dedup table stores.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataResponse {
    Ack {
        generation: u64,
    },
    TopicCreated {
        topic_uuid: Uuid,
        topic_epoch: u64,
        root_range_uuid: Uuid,
    },
    LeaseGranted {
        fencing_epoch: u64,
    },
    Rejected(MetadataError),
}

impl MetadataResponse {
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::with_capacity(48);
        self.encode_into(&mut out)?;
        Ok(out)
    }

    pub(crate) fn encode_into(&self, out: &mut Vec<u8>) -> Result<(), CodecError> {
        match self {
            MetadataResponse::Ack { generation } => {
                put_u16(out, RESPONSE_KIND_ACK);
                put_u64(out, *generation);
            }
            MetadataResponse::TopicCreated {
                topic_uuid,
                topic_epoch,
                root_range_uuid,
            } => {
                put_u16(out, RESPONSE_KIND_TOPIC_CREATED);
                put_uuid(out, *topic_uuid);
                put_u64(out, *topic_epoch);
                put_uuid(out, *root_range_uuid);
            }
            MetadataResponse::LeaseGranted { fencing_epoch } => {
                put_u16(out, RESPONSE_KIND_LEASE_GRANTED);
                put_u64(out, *fencing_epoch);
            }
            MetadataResponse::Rejected(error) => {
                put_u16(out, RESPONSE_KIND_REJECTED);
                error.encode_into(out)?;
            }
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let response = Self::decode_from(&mut reader)?;
        reader.finish()?;
        Ok(response)
    }

    pub(crate) fn decode_from(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        let kind = reader.u16("response kind")?;
        match kind {
            RESPONSE_KIND_ACK => Ok(MetadataResponse::Ack {
                generation: reader.u64("generation")?,
            }),
            RESPONSE_KIND_TOPIC_CREATED => Ok(MetadataResponse::TopicCreated {
                topic_uuid: reader.uuid("topic uuid")?,
                topic_epoch: reader.u64("topic epoch")?,
                root_range_uuid: reader.uuid("root range uuid")?,
            }),
            RESPONSE_KIND_LEASE_GRANTED => Ok(MetadataResponse::LeaseGranted {
                fencing_epoch: reader.u64("fencing epoch")?,
            }),
            RESPONSE_KIND_REJECTED => Ok(MetadataResponse::Rejected(MetadataError::decode_from(
                reader,
            )?)),
            other => Err(CodecError::UnknownTag {
                what: "response kind",
                tag: u32::from(other),
            }),
        }
    }
}

fn encode_envelope(out: &mut Vec<u8>, env: &CommandEnvelope) {
    put_uuid(out, env.request_id);
    put_i64(out, env.issued_at_ms);
}

fn decode_envelope(reader: &mut Reader<'_>) -> Result<CommandEnvelope, CodecError> {
    Ok(CommandEnvelope {
        request_id: reader.uuid("request id")?,
        issued_at_ms: reader.i64("issued-at millis")?,
    })
}

/// Canonical option encoding: presence byte, then the value only if present.
fn encode_optional_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_u64(out, value);
        }
    }
}

fn decode_optional_u64(
    reader: &mut Reader<'_>,
    what: &'static str,
) -> Result<Option<u64>, CodecError> {
    if reader.flag(what)? {
        Ok(Some(reader.u64(what)?))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(request: u128) -> CommandEnvelope {
        CommandEnvelope {
            request_id: Uuid::from_u128(request),
            issued_at_ms: 1_750_000_000_000,
        }
    }

    fn every_command() -> Vec<MetadataCommand> {
        vec![
            MetadataCommand::RegisterNode {
                env: envelope(1),
                node_uuid: Uuid::from_u128(10),
                addr: "10.0.0.1:9200".to_owned(),
                expected_generation: None,
            },
            MetadataCommand::RegisterNode {
                env: envelope(2),
                node_uuid: Uuid::from_u128(10),
                addr: "10.0.0.2:9200".to_owned(),
                expected_generation: Some(4),
            },
            MetadataCommand::SetNodeState {
                env: envelope(3),
                node_uuid: Uuid::from_u128(10),
                state: NodeState::Draining,
                expected_generation: 5,
            },
            MetadataCommand::CreateTopic {
                env: envelope(4),
                name: "events.v1".to_owned(),
                topic_uuid: Uuid::from_u128(20),
                root_range_uuid: Uuid::from_u128(21),
            },
            MetadataCommand::GrantRangeLease {
                env: envelope(5),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                holder_node_uuid: Uuid::from_u128(10),
                expected_range_generation: 0,
            },
            MetadataCommand::ReleaseRangeLease {
                env: envelope(6),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                expected_fencing_epoch: 1,
            },
            MetadataCommand::RegisterSealedSegment {
                env: envelope(7),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                segment_generation: 0,
                base_offset: 0,
                next_offset: 128,
                content_root: [7; 32],
                sealed_by_epoch: 1,
                expected_range_generation: 2,
            },
            MetadataCommand::MarkSegmentVerified {
                env: envelope(8),
                topic_uuid: Uuid::from_u128(20),
                range_uuid: Uuid::from_u128(21),
                segment_uuid: Uuid::from_u128(30),
                content_root: [7; 32],
                expected_generation: 0,
            },
            MetadataCommand::PutKeyRecord {
                env: envelope(9),
                key_uuid: Uuid::from_u128(40),
                scheme: 1,
                public_material_digest: [9; 32],
            },
        ]
    }

    fn every_response() -> Vec<MetadataResponse> {
        vec![
            MetadataResponse::Ack { generation: 3 },
            MetadataResponse::TopicCreated {
                topic_uuid: Uuid::from_u128(20),
                topic_epoch: 2,
                root_range_uuid: Uuid::from_u128(21),
            },
            MetadataResponse::LeaseGranted { fencing_epoch: 9 },
            MetadataResponse::Rejected(MetadataError::GenerationMismatch {
                expected: 1,
                actual: 2,
            }),
            MetadataResponse::Rejected(MetadataError::EpochMismatch {
                expected: 3,
                actual: 4,
            }),
            MetadataResponse::Rejected(MetadataError::AlreadyExists),
            MetadataResponse::Rejected(MetadataError::NotFound),
            MetadataResponse::Rejected(MetadataError::invalid_transition("dead -> active")),
            MetadataResponse::Rejected(MetadataError::limit("too many ranges")),
        ]
    }

    #[test]
    fn every_command_and_response_round_trips_byte_exactly() {
        for command in every_command() {
            let encoded = command.encode().unwrap();
            assert_eq!(MetadataCommand::decode(&encoded).unwrap(), command);
        }
        for response in every_response() {
            let encoded = response.encode().unwrap();
            assert_eq!(MetadataResponse::decode(&encoded).unwrap(), response);
        }
    }

    #[test]
    fn decode_rejects_trailing_bytes_truncation_and_unknown_kinds() {
        for command in every_command() {
            let mut trailing = command.encode().unwrap();
            trailing.push(0);
            assert_eq!(
                MetadataCommand::decode(&trailing),
                Err(CodecError::Trailing(1))
            );

            let encoded = command.encode().unwrap();
            let mut truncated = encoded.clone();
            truncated.pop();
            assert!(
                matches!(
                    MetadataCommand::decode(&truncated),
                    Err(CodecError::Truncated(_) | CodecError::InvalidUtf8(_))
                ),
                "{command:?}"
            );
        }
        assert_eq!(
            MetadataCommand::decode(&[0, 99]),
            Err(CodecError::UnknownTag {
                what: "command kind",
                tag: 99,
            })
        );
        assert_eq!(
            MetadataResponse::decode(&[0, 99]),
            Err(CodecError::UnknownTag {
                what: "response kind",
                tag: 99,
            })
        );

        let mut rejected = MetadataResponse::Rejected(MetadataError::NotFound)
            .encode()
            .unwrap();
        rejected.push(1);
        assert_eq!(
            MetadataResponse::decode(&rejected),
            Err(CodecError::Trailing(1))
        );
    }

    #[test]
    fn oversized_and_empty_bounded_strings_are_rejected_by_the_codec() {
        let command = MetadataCommand::RegisterNode {
            env: envelope(1),
            node_uuid: Uuid::from_u128(10),
            addr: "x".repeat(MAX_NODE_ADDR_BYTES + 1),
            expected_generation: None,
        };
        assert!(matches!(
            command.encode(),
            Err(CodecError::BoundExceeded { .. })
        ));

        // An empty address survives encode but the decoder rejects it, so it
        // can never round-trip into apply.
        let empty_addr = MetadataCommand::RegisterNode {
            env: envelope(1),
            node_uuid: Uuid::from_u128(10),
            addr: String::new(),
            expected_generation: None,
        };
        assert!(matches!(
            MetadataCommand::decode(&empty_addr.encode().unwrap()),
            Err(CodecError::InvalidValue { .. })
        ));

        let long_topic = MetadataCommand::CreateTopic {
            env: envelope(2),
            name: "y".repeat(MAX_TOPIC_NAME_BYTES + 1),
            topic_uuid: Uuid::from_u128(20),
            root_range_uuid: Uuid::from_u128(21),
        };
        assert!(matches!(
            long_topic.encode(),
            Err(CodecError::BoundExceeded { .. })
        ));
    }

    #[test]
    fn error_detail_constructors_truncate_at_a_character_boundary() {
        // 255 ASCII bytes then a 2-byte character straddling the 256 bound.
        let detail = format!("{}é", "a".repeat(255));
        let MetadataError::InvalidTransition(bounded) = MetadataError::invalid_transition(detail)
        else {
            panic!("constructor changed variant");
        };
        assert_eq!(bounded.len(), 255);
        assert!(
            MetadataResponse::Rejected(MetadataError::InvalidTransition(bounded))
                .encode()
                .is_ok()
        );

        let MetadataError::Limit(long) = MetadataError::limit("z".repeat(400)) else {
            panic!("constructor changed variant");
        };
        assert_eq!(long.len(), MAX_ERROR_DETAIL_BYTES);
    }

    #[test]
    fn v1_create_topic_command_matches_golden_vector() {
        let command = MetadataCommand::CreateTopic {
            env: CommandEnvelope {
                request_id: Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
                issued_at_ms: 0x0102_0304_0506_0708,
            },
            name: "audit.v1".to_owned(),
            topic_uuid: Uuid::parse_str("ffeeddcc-bbaa-9988-7766-554433221100").unwrap(),
            root_range_uuid: Uuid::parse_str("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0").unwrap(),
        };
        let encoded = command.encode().unwrap();
        let hex: String = encoded.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(
            hex,
            concat!(
                "000300112233445566778899aabbccddeeff0102030405060708",
                "000861756469742e7631",
                "ffeeddccbbaa99887766554433221100",
                "0f1e2d3c4b5a69788796a5b4c3d2e1f0"
            )
        );
    }
}

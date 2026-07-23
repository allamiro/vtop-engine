//! Deterministic metadata state machine.
//!
//! `apply` is a pure function of (current state, apply index, command): it
//! reads no clock, no RNG, and no environment. Every id is proposer-supplied
//! (collisions reject with `AlreadyExists`), optimistic concurrency uses
//! per-record generations, and fencing epochs are strictly monotonic so a
//! stale leaseholder can never publish under a current epoch.
//!
//! Exactly-once semantics: a FIFO dedup table of the last
//! [`DEDUP_CAPACITY`] request ids returns the stored original response for a
//! replayed request, and the table itself is part of the snapshot encoding so
//! dedup survives snapshot/restore identically on every replica.

use crate::command::{
    MetadataCommand, MetadataError, MetadataResponse, NodeState, RangeAssignment,
    MAX_ASSIGNED_RANGES, MAX_NODE_ADDR_BYTES,
};
use crate::keys::{validate_group_name, validate_topic_name, MetaKey};
use crate::wire::{
    put_bounded_str, put_bytes32, put_u16, put_u32, put_u64, put_u8, put_uuid, CodecError, Reader,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
use uuid::Uuid;

/// FIFO capacity of the request dedup table, evicted in apply order.
pub const DEDUP_CAPACITY: usize = 65_536;

/// Version byte stream prefix of the state-machine snapshot payload.
const SNAPSHOT_PAYLOAD_VERSION: u16 = 1;

const MAX_SNAPSHOT_KEY_BYTES: usize = 256;
const MAX_SNAPSHOT_VALUE_BYTES: usize = 1024;
const MAX_SNAPSHOT_RESPONSE_BYTES: usize = 1024;
const MAX_SNAPSHOT_RECORDS: u32 = 1 << 24;

const VALUE_TAG_NODE: u8 = 1;
const VALUE_TAG_TOPIC: u8 = 2;
const VALUE_TAG_TOPIC_NAME: u8 = 3;
const VALUE_TAG_RANGE: u8 = 4;
const VALUE_TAG_SEGMENT: u8 = 5;
const VALUE_TAG_KEY: u8 = 6;
const VALUE_TAG_GROUP: u8 = 7;
const VALUE_TAG_GROUP_NAME: u8 = 8;
const VALUE_TAG_GROUP_MEMBER: u8 = 9;
const VALUE_TAG_GROUP_CURSOR: u8 = 10;

/// A registered broker/controller node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeRecord {
    pub addr: String,
    pub state: NodeState,
    pub generation: u64,
}

/// A topic incarnation. `topic_epoch` is allocated by the state machine as
/// `prior epoch for this name + 1`, so recreating a name always fences every
/// earlier incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicRecord {
    pub name: String,
    pub topic_epoch: u64,
    pub generation: u64,
}

/// The name index entry: which incarnation currently owns a topic name and
/// the highest epoch ever allocated for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicNameRecord {
    pub topic_uuid: Uuid,
    pub latest_epoch: u64,
}

/// An outstanding range lease. `granted_apply_index` records *when* in log
/// order the lease was granted — the only place the apply index enters state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseRecord {
    pub holder_node_uuid: Uuid,
    pub fencing_epoch: u64,
    pub granted_apply_index: u64,
}

/// A key range of a topic. `fencing_epoch` only ever moves forward: every
/// grant increments it, and a release never rewinds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeRecord {
    pub generation: u64,
    pub key_prefix: u64,
    pub key_prefix_bits: u8,
    pub fencing_epoch: u64,
    pub lease: Option<LeaseRecord>,
}

/// Verification lifecycle of a sealed segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SegmentState {
    SealedUnverified,
    Verified,
}

/// A sealed segment registered against a range under a fencing epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentRecord {
    pub segment_generation: u64,
    pub base_offset: u64,
    pub next_offset: u64,
    pub content_root: [u8; 32],
    pub state: SegmentState,
    pub sealed_by_epoch: u64,
}

/// Lifecycle of a public-key record. Only `Active` exists in this slice;
/// revocation arrives with the security slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyState {
    Active,
}

/// A registered public-key record. Immutable once written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRecord {
    pub scheme: u16,
    pub public_material_digest: [u8; 32],
    pub state: KeyState,
}

/// A consumer group incarnation. `generation` bumps on join/leave/assign.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerGroupRecord {
    pub name: String,
    pub generation: u64,
}

/// Name index entry for consumer groups.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupNameRecord {
    pub group_uuid: Uuid,
}

/// A joined consumer-group member and its durable range assignment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupMemberRecord {
    pub generation: u64,
    pub assigned: Vec<RangeAssignment>,
}

/// Lineage-aware durable cursor checkpoint for one group/topic/range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorCheckpointRecord {
    pub topic_epoch: u64,
    pub range_generation: u64,
    pub segment_uuid: Uuid,
    pub segment_generation: u64,
    pub segment_root: [u8; 32],
    pub record_offset: u64,
    pub record_index: u64,
    pub lineage_transition_id: Option<Uuid>,
    pub checkpoint_generation: u64,
    pub committed_by_member: Uuid,
}

/// A typed record value stored under an encoded [`MetaKey`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetaValue {
    Node(NodeRecord),
    Topic(TopicRecord),
    TopicName(TopicNameRecord),
    Range(RangeRecord),
    Segment(SegmentRecord),
    Key(KeyRecord),
    Group(ConsumerGroupRecord),
    GroupName(GroupNameRecord),
    GroupMember(GroupMemberRecord),
    GroupCursor(CursorCheckpointRecord),
}

impl MetaValue {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::with_capacity(64);
        match self {
            MetaValue::Node(node) => {
                put_u8(&mut out, VALUE_TAG_NODE);
                put_bounded_str(&mut out, &node.addr, MAX_NODE_ADDR_BYTES, "node address")?;
                put_u8(&mut out, node_state_tag(node.state));
                put_u64(&mut out, node.generation);
            }
            MetaValue::Topic(topic) => {
                put_u8(&mut out, VALUE_TAG_TOPIC);
                put_bounded_str(
                    &mut out,
                    &topic.name,
                    crate::keys::MAX_TOPIC_NAME_BYTES,
                    "topic name",
                )?;
                put_u64(&mut out, topic.topic_epoch);
                put_u64(&mut out, topic.generation);
            }
            MetaValue::TopicName(name) => {
                put_u8(&mut out, VALUE_TAG_TOPIC_NAME);
                put_uuid(&mut out, name.topic_uuid);
                put_u64(&mut out, name.latest_epoch);
            }
            MetaValue::Range(range) => {
                put_u8(&mut out, VALUE_TAG_RANGE);
                put_u64(&mut out, range.generation);
                put_u64(&mut out, range.key_prefix);
                put_u8(&mut out, range.key_prefix_bits);
                put_u64(&mut out, range.fencing_epoch);
                match &range.lease {
                    None => put_u8(&mut out, 0),
                    Some(lease) => {
                        put_u8(&mut out, 1);
                        put_uuid(&mut out, lease.holder_node_uuid);
                        put_u64(&mut out, lease.fencing_epoch);
                        put_u64(&mut out, lease.granted_apply_index);
                    }
                }
            }
            MetaValue::Segment(segment) => {
                put_u8(&mut out, VALUE_TAG_SEGMENT);
                put_u64(&mut out, segment.segment_generation);
                put_u64(&mut out, segment.base_offset);
                put_u64(&mut out, segment.next_offset);
                put_bytes32(&mut out, &segment.content_root);
                put_u8(
                    &mut out,
                    match segment.state {
                        SegmentState::SealedUnverified => 1,
                        SegmentState::Verified => 2,
                    },
                );
                put_u64(&mut out, segment.sealed_by_epoch);
            }
            MetaValue::Key(key) => {
                put_u8(&mut out, VALUE_TAG_KEY);
                put_u16(&mut out, key.scheme);
                put_bytes32(&mut out, &key.public_material_digest);
                put_u8(
                    &mut out,
                    match key.state {
                        KeyState::Active => 1,
                    },
                );
            }
            MetaValue::Group(group) => {
                put_u8(&mut out, VALUE_TAG_GROUP);
                put_bounded_str(
                    &mut out,
                    &group.name,
                    crate::keys::MAX_GROUP_NAME_BYTES,
                    "group name",
                )?;
                put_u64(&mut out, group.generation);
            }
            MetaValue::GroupName(name) => {
                put_u8(&mut out, VALUE_TAG_GROUP_NAME);
                put_uuid(&mut out, name.group_uuid);
            }
            MetaValue::GroupMember(member) => {
                put_u8(&mut out, VALUE_TAG_GROUP_MEMBER);
                put_u64(&mut out, member.generation);
                encode_assigned_ranges(&mut out, &member.assigned)?;
            }
            MetaValue::GroupCursor(cursor) => {
                put_u8(&mut out, VALUE_TAG_GROUP_CURSOR);
                put_u64(&mut out, cursor.topic_epoch);
                put_u64(&mut out, cursor.range_generation);
                put_uuid(&mut out, cursor.segment_uuid);
                put_u64(&mut out, cursor.segment_generation);
                put_bytes32(&mut out, &cursor.segment_root);
                put_u64(&mut out, cursor.record_offset);
                put_u64(&mut out, cursor.record_index);
                match cursor.lineage_transition_id {
                    None => put_u8(&mut out, 0),
                    Some(id) => {
                        put_u8(&mut out, 1);
                        put_uuid(&mut out, id);
                    }
                }
                put_u64(&mut out, cursor.checkpoint_generation);
                put_uuid(&mut out, cursor.committed_by_member);
            }
        }
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let tag = reader.u8("record value tag")?;
        let value = match tag {
            VALUE_TAG_NODE => MetaValue::Node(NodeRecord {
                addr: reader.bounded_str(MAX_NODE_ADDR_BYTES, "node address")?,
                state: NodeState::from_wire(reader.u8("node state")?)?,
                generation: reader.u64("node generation")?,
            }),
            VALUE_TAG_TOPIC => MetaValue::Topic(TopicRecord {
                name: reader.bounded_str(crate::keys::MAX_TOPIC_NAME_BYTES, "topic name")?,
                topic_epoch: reader.u64("topic epoch")?,
                generation: reader.u64("topic generation")?,
            }),
            VALUE_TAG_TOPIC_NAME => MetaValue::TopicName(TopicNameRecord {
                topic_uuid: reader.uuid("topic uuid")?,
                latest_epoch: reader.u64("latest topic epoch")?,
            }),
            VALUE_TAG_RANGE => {
                let generation = reader.u64("range generation")?;
                let key_prefix = reader.u64("key prefix")?;
                let key_prefix_bits = reader.u8("key prefix bits")?;
                let fencing_epoch = reader.u64("fencing epoch")?;
                let lease = if reader.flag("lease presence")? {
                    Some(LeaseRecord {
                        holder_node_uuid: reader.uuid("lease holder uuid")?,
                        fencing_epoch: reader.u64("lease fencing epoch")?,
                        granted_apply_index: reader.u64("lease apply index")?,
                    })
                } else {
                    None
                };
                MetaValue::Range(RangeRecord {
                    generation,
                    key_prefix,
                    key_prefix_bits,
                    fencing_epoch,
                    lease,
                })
            }
            VALUE_TAG_SEGMENT => MetaValue::Segment(SegmentRecord {
                segment_generation: reader.u64("segment generation")?,
                base_offset: reader.u64("base offset")?,
                next_offset: reader.u64("next offset")?,
                content_root: reader.bytes32("content root")?,
                state: match reader.u8("segment state")? {
                    1 => SegmentState::SealedUnverified,
                    2 => SegmentState::Verified,
                    other => {
                        return Err(CodecError::UnknownTag {
                            what: "segment state",
                            tag: u32::from(other),
                        });
                    }
                },
                sealed_by_epoch: reader.u64("sealed-by epoch")?,
            }),
            VALUE_TAG_KEY => MetaValue::Key(KeyRecord {
                scheme: reader.u16("key scheme")?,
                public_material_digest: reader.bytes32("public material digest")?,
                state: match reader.u8("key state")? {
                    1 => KeyState::Active,
                    other => {
                        return Err(CodecError::UnknownTag {
                            what: "key state",
                            tag: u32::from(other),
                        });
                    }
                },
            }),
            VALUE_TAG_GROUP => MetaValue::Group(ConsumerGroupRecord {
                name: reader.bounded_str(crate::keys::MAX_GROUP_NAME_BYTES, "group name")?,
                generation: reader.u64("group generation")?,
            }),
            VALUE_TAG_GROUP_NAME => MetaValue::GroupName(GroupNameRecord {
                group_uuid: reader.uuid("group uuid")?,
            }),
            VALUE_TAG_GROUP_MEMBER => MetaValue::GroupMember(GroupMemberRecord {
                generation: reader.u64("member generation")?,
                assigned: decode_assigned_ranges(&mut reader)?,
            }),
            VALUE_TAG_GROUP_CURSOR => {
                let topic_epoch = reader.u64("topic epoch")?;
                let range_generation = reader.u64("range generation")?;
                let segment_uuid = reader.uuid("segment uuid")?;
                let segment_generation = reader.u64("segment generation")?;
                let segment_root = reader.bytes32("segment root")?;
                let record_offset = reader.u64("record offset")?;
                let record_index = reader.u64("record index")?;
                let lineage_transition_id = if reader.flag("lineage transition presence")? {
                    Some(reader.uuid("lineage transition id")?)
                } else {
                    None
                };
                MetaValue::GroupCursor(CursorCheckpointRecord {
                    topic_epoch,
                    range_generation,
                    segment_uuid,
                    segment_generation,
                    segment_root,
                    record_offset,
                    record_index,
                    lineage_transition_id,
                    checkpoint_generation: reader.u64("checkpoint generation")?,
                    committed_by_member: reader.uuid("committed-by member")?,
                })
            }
            other => {
                return Err(CodecError::UnknownTag {
                    what: "record value tag",
                    tag: u32::from(other),
                });
            }
        };
        reader.finish()?;
        Ok(value)
    }
}

fn node_state_tag(state: NodeState) -> u8 {
    match state {
        NodeState::Active => 1,
        NodeState::Draining => 2,
        NodeState::Dead => 3,
    }
}

/// The deterministic metadata state machine.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetaStateMachine {
    records: BTreeMap<Vec<u8>, MetaValue>,
    dedup_order: VecDeque<Uuid>,
    dedup_responses: HashMap<Uuid, MetadataResponse>,
}

impl MetaStateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up a record by typed key.
    pub fn record(&self, key: &MetaKey) -> Option<&MetaValue> {
        self.records.get(&key.encode())
    }

    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    pub fn dedup_len(&self) -> usize {
        self.dedup_order.len()
    }

    /// Apply one command at `apply_index`. Pure and deterministic: identical
    /// (state, index, command) triples always produce identical responses
    /// and identical successor states on every replica.
    pub fn apply(&mut self, apply_index: u64, command: &MetadataCommand) -> MetadataResponse {
        let request_id = command.envelope().request_id;
        if let Some(original) = self.dedup_responses.get(&request_id) {
            return original.clone();
        }
        let response = self.apply_inner(apply_index, command);
        self.remember(request_id, response.clone());
        response
    }

    fn remember(&mut self, request_id: Uuid, response: MetadataResponse) {
        if self.dedup_order.len() == DEDUP_CAPACITY {
            if let Some(evicted) = self.dedup_order.pop_front() {
                self.dedup_responses.remove(&evicted);
            }
        }
        self.dedup_order.push_back(request_id);
        self.dedup_responses.insert(request_id, response);
    }

    fn apply_inner(&mut self, apply_index: u64, command: &MetadataCommand) -> MetadataResponse {
        // Check order is fixed and part of the contract: existence, then
        // epoch fencing, then generation CAS, then semantic guards.
        match command {
            MetadataCommand::RegisterNode {
                node_uuid,
                addr,
                expected_generation,
                ..
            } => self.register_node(*node_uuid, addr, *expected_generation),
            MetadataCommand::SetNodeState {
                node_uuid,
                state,
                expected_generation,
                ..
            } => self.set_node_state(*node_uuid, *state, *expected_generation),
            MetadataCommand::CreateTopic {
                name,
                topic_uuid,
                root_range_uuid,
                ..
            } => self.create_topic(name, *topic_uuid, *root_range_uuid),
            MetadataCommand::GrantRangeLease {
                topic_uuid,
                range_uuid,
                holder_node_uuid,
                expected_range_generation,
                ..
            } => self.grant_range_lease(
                apply_index,
                *topic_uuid,
                *range_uuid,
                *holder_node_uuid,
                *expected_range_generation,
            ),
            MetadataCommand::ReleaseRangeLease {
                topic_uuid,
                range_uuid,
                expected_fencing_epoch,
                ..
            } => self.release_range_lease(*topic_uuid, *range_uuid, *expected_fencing_epoch),
            MetadataCommand::RegisterSealedSegment {
                topic_uuid,
                range_uuid,
                segment_uuid,
                segment_generation,
                base_offset,
                next_offset,
                content_root,
                sealed_by_epoch,
                expected_range_generation,
                ..
            } => self.register_sealed_segment(
                *topic_uuid,
                *range_uuid,
                *segment_uuid,
                *segment_generation,
                *base_offset,
                *next_offset,
                *content_root,
                *sealed_by_epoch,
                *expected_range_generation,
            ),
            MetadataCommand::MarkSegmentVerified {
                topic_uuid,
                range_uuid,
                segment_uuid,
                content_root,
                expected_generation,
                ..
            } => self.mark_segment_verified(
                *topic_uuid,
                *range_uuid,
                *segment_uuid,
                *content_root,
                *expected_generation,
            ),
            MetadataCommand::PutKeyRecord {
                key_uuid,
                scheme,
                public_material_digest,
                ..
            } => self.put_key_record(*key_uuid, *scheme, *public_material_digest),
            MetadataCommand::CreateConsumerGroup {
                name, group_uuid, ..
            } => self.create_consumer_group(name, *group_uuid),
            MetadataCommand::JoinConsumerGroup {
                group_uuid,
                member_uuid,
                expected_group_generation,
                ..
            } => self.join_consumer_group(*group_uuid, *member_uuid, *expected_group_generation),
            MetadataCommand::LeaveConsumerGroup {
                group_uuid,
                member_uuid,
                expected_member_generation,
                ..
            } => self.leave_consumer_group(*group_uuid, *member_uuid, *expected_member_generation),
            MetadataCommand::AssignMemberRanges {
                group_uuid,
                member_uuid,
                ranges,
                expected_member_generation,
                ..
            } => self.assign_member_ranges(
                *group_uuid,
                *member_uuid,
                ranges,
                *expected_member_generation,
            ),
            MetadataCommand::CommitGroupCursor {
                group_uuid,
                member_uuid,
                topic_uuid,
                range_uuid,
                topic_epoch,
                range_generation,
                segment_uuid,
                segment_generation,
                segment_root,
                record_offset,
                record_index,
                lineage_transition_id,
                expected_checkpoint_generation,
                ..
            } => self.commit_group_cursor(CommitCursorArgs {
                group_uuid: *group_uuid,
                member_uuid: *member_uuid,
                topic_uuid: *topic_uuid,
                range_uuid: *range_uuid,
                topic_epoch: *topic_epoch,
                range_generation: *range_generation,
                segment_uuid: *segment_uuid,
                segment_generation: *segment_generation,
                segment_root: *segment_root,
                record_offset: *record_offset,
                record_index: *record_index,
                lineage_transition_id: *lineage_transition_id,
                expected_checkpoint_generation: *expected_checkpoint_generation,
            }),
        }
    }

    fn register_node(
        &mut self,
        node_uuid: Uuid,
        addr: &str,
        expected_generation: Option<u64>,
    ) -> MetadataResponse {
        if addr.is_empty() || addr.len() > MAX_NODE_ADDR_BYTES {
            return reject(MetadataError::limit(format!(
                "node address must be 1..={MAX_NODE_ADDR_BYTES} bytes, got {}",
                addr.len()
            )));
        }
        let key = MetaKey::Node { node_uuid }.encode();
        match (self.records.get_mut(&key), expected_generation) {
            (None, None) => {
                self.records.insert(
                    key,
                    MetaValue::Node(NodeRecord {
                        addr: addr.to_owned(),
                        state: NodeState::Active,
                        generation: 0,
                    }),
                );
                MetadataResponse::Ack { generation: 0 }
            }
            (None, Some(_)) => reject(MetadataError::NotFound),
            (Some(_), None) => reject(MetadataError::AlreadyExists),
            (Some(MetaValue::Node(node)), Some(expected)) => {
                if node.generation != expected {
                    return reject(MetadataError::GenerationMismatch {
                        expected,
                        actual: node.generation,
                    });
                }
                node.addr = addr.to_owned();
                node.state = NodeState::Active;
                node.generation += 1;
                MetadataResponse::Ack {
                    generation: node.generation,
                }
            }
            (Some(_), Some(_)) => unreachable!("node keys only ever hold node records"),
        }
    }

    fn set_node_state(
        &mut self,
        node_uuid: Uuid,
        target: NodeState,
        expected_generation: u64,
    ) -> MetadataResponse {
        let key = MetaKey::Node { node_uuid }.encode();
        let Some(MetaValue::Node(node)) = self.records.get_mut(&key) else {
            return reject(MetadataError::NotFound);
        };
        if node.generation != expected_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_generation,
                actual: node.generation,
            });
        }
        // Guarded transitions, vtop-core style: Dead is terminal (rejoining
        // is RegisterNode's CAS path), and same-state writes are rejected so
        // a lost-then-retried command cannot silently burn a generation.
        let allowed = matches!(
            (node.state, target),
            (NodeState::Active, NodeState::Draining)
                | (NodeState::Active, NodeState::Dead)
                | (NodeState::Draining, NodeState::Active)
                | (NodeState::Draining, NodeState::Dead)
        );
        if !allowed {
            return reject(MetadataError::invalid_transition(format!(
                "node state {} -> {} is not allowed",
                node.state, target
            )));
        }
        node.state = target;
        node.generation += 1;
        MetadataResponse::Ack {
            generation: node.generation,
        }
    }

    fn create_topic(
        &mut self,
        name: &str,
        topic_uuid: Uuid,
        root_range_uuid: Uuid,
    ) -> MetadataResponse {
        if validate_topic_name(name).is_err() {
            return reject(MetadataError::limit(format!(
                "topic name must be 1..={} bytes, got {}",
                crate::keys::MAX_TOPIC_NAME_BYTES,
                name.len()
            )));
        }
        let topic_key = MetaKey::Topic { topic_uuid }.encode();
        if self.records.contains_key(&topic_key) {
            return reject(MetadataError::AlreadyExists);
        }
        let range_key = MetaKey::Range {
            topic_uuid,
            range_uuid: root_range_uuid,
        }
        .encode();
        if self.records.contains_key(&range_key) {
            return reject(MetadataError::AlreadyExists);
        }
        // Epoch allocation is the one piece of state the SM computes itself:
        // the highest epoch ever used for this name, plus one. Recreating a
        // name therefore fences every earlier incarnation, which is why the
        // name record survives and is rebound rather than treated as a
        // conflict.
        let name_key = MetaKey::TopicByName {
            name: name.to_owned(),
        }
        .encode();
        let prior_epoch = match self.records.get(&name_key) {
            Some(MetaValue::TopicName(record)) => record.latest_epoch,
            Some(_) => unreachable!("topic-name keys only ever hold name records"),
            None => 0,
        };
        let topic_epoch = prior_epoch + 1;
        self.records.insert(
            name_key,
            MetaValue::TopicName(TopicNameRecord {
                topic_uuid,
                latest_epoch: topic_epoch,
            }),
        );
        self.records.insert(
            topic_key,
            MetaValue::Topic(TopicRecord {
                name: name.to_owned(),
                topic_epoch,
                generation: 0,
            }),
        );
        // The root range covers the full key interval: prefix 0 with 0
        // prefix bits, generation 0, no fencing history yet.
        self.records.insert(
            range_key,
            MetaValue::Range(RangeRecord {
                generation: 0,
                key_prefix: 0,
                key_prefix_bits: 0,
                fencing_epoch: 0,
                lease: None,
            }),
        );
        MetadataResponse::TopicCreated {
            topic_uuid,
            topic_epoch,
            root_range_uuid,
        }
    }

    fn grant_range_lease(
        &mut self,
        apply_index: u64,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        holder_node_uuid: Uuid,
        expected_range_generation: u64,
    ) -> MetadataResponse {
        match self.record(&MetaKey::Node {
            node_uuid: holder_node_uuid,
        }) {
            None => return reject(MetadataError::NotFound),
            Some(MetaValue::Node(node)) => {
                if node.state != NodeState::Active {
                    return reject(MetadataError::invalid_transition(format!(
                        "lease holder {holder_node_uuid} is {}, not active",
                        node.state
                    )));
                }
            }
            Some(_) => unreachable!("node keys only ever hold node records"),
        }
        let range_key = MetaKey::Range {
            topic_uuid,
            range_uuid,
        }
        .encode();
        let Some(MetaValue::Range(range)) = self.records.get_mut(&range_key) else {
            return reject(MetadataError::NotFound);
        };
        if range.generation != expected_range_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_range_generation,
                actual: range.generation,
            });
        }
        // Strict monotonicity is the fencing invariant: a grant always mints
        // a fresh, higher epoch, even when it steals the lease from a live
        // holder.
        let Some(fencing_epoch) = range.fencing_epoch.checked_add(1) else {
            return reject(MetadataError::limit("fencing epoch space is exhausted"));
        };
        range.fencing_epoch = fencing_epoch;
        range.lease = Some(LeaseRecord {
            holder_node_uuid,
            fencing_epoch,
            granted_apply_index: apply_index,
        });
        range.generation += 1;
        MetadataResponse::LeaseGranted { fencing_epoch }
    }

    fn release_range_lease(
        &mut self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        expected_fencing_epoch: u64,
    ) -> MetadataResponse {
        let range_key = MetaKey::Range {
            topic_uuid,
            range_uuid,
        }
        .encode();
        let Some(MetaValue::Range(range)) = self.records.get_mut(&range_key) else {
            return reject(MetadataError::NotFound);
        };
        if range.fencing_epoch != expected_fencing_epoch {
            return reject(MetadataError::EpochMismatch {
                expected: expected_fencing_epoch,
                actual: range.fencing_epoch,
            });
        }
        if range.lease.is_none() {
            return reject(MetadataError::invalid_transition(
                "range holds no lease to release",
            ));
        }
        // Release clears the lease but never rewinds the fencing epoch.
        range.lease = None;
        range.generation += 1;
        MetadataResponse::Ack {
            generation: range.generation,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn register_sealed_segment(
        &mut self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        segment_generation: u64,
        base_offset: u64,
        next_offset: u64,
        content_root: [u8; 32],
        sealed_by_epoch: u64,
        expected_range_generation: u64,
    ) -> MetadataResponse {
        let segment_key = MetaKey::Segment {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let range_key = MetaKey::Range {
            topic_uuid,
            range_uuid,
        }
        .encode();
        let Some(MetaValue::Range(range)) = self.records.get(&range_key) else {
            return reject(MetadataError::NotFound);
        };
        // Sealing is an act of the current leaseholder. Without a live
        // lease there is no authority to publish at all: a fresh range and
        // a just-released range both sit at a "matching" epoch with no
        // holder, and neither may accept a segment.
        let Some(lease) = range.lease.as_ref() else {
            return reject(MetadataError::invalid_transition(
                "range holds no active lease to seal under",
            ));
        };
        // The epoch gate: a sealer fenced by a newer grant must not be able
        // to publish, however stale or fresh its CAS token is.
        if sealed_by_epoch != lease.fencing_epoch {
            return reject(MetadataError::EpochMismatch {
                expected: sealed_by_epoch,
                actual: lease.fencing_epoch,
            });
        }
        if range.generation != expected_range_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_range_generation,
                actual: range.generation,
            });
        }
        if next_offset < base_offset {
            return reject(MetadataError::invalid_transition(format!(
                "segment offsets regress: next {next_offset} < base {base_offset}"
            )));
        }
        if self.records.contains_key(&segment_key) {
            return reject(MetadataError::AlreadyExists);
        }
        self.records.insert(
            segment_key,
            MetaValue::Segment(SegmentRecord {
                segment_generation,
                base_offset,
                next_offset,
                content_root,
                state: SegmentState::SealedUnverified,
                sealed_by_epoch,
            }),
        );
        let Some(MetaValue::Range(range)) = self.records.get_mut(&range_key) else {
            unreachable!("range record was present above and apply is single-threaded");
        };
        range.generation += 1;
        MetadataResponse::Ack {
            generation: range.generation,
        }
    }

    fn mark_segment_verified(
        &mut self,
        topic_uuid: Uuid,
        range_uuid: Uuid,
        segment_uuid: Uuid,
        content_root: [u8; 32],
        expected_generation: u64,
    ) -> MetadataResponse {
        let segment_key = MetaKey::Segment {
            topic_uuid,
            range_uuid,
            segment_uuid,
        }
        .encode();
        let Some(MetaValue::Segment(segment)) = self.records.get_mut(&segment_key) else {
            return reject(MetadataError::NotFound);
        };
        if segment.segment_generation != expected_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_generation,
                actual: segment.segment_generation,
            });
        }
        if segment.content_root != content_root {
            return reject(MetadataError::invalid_transition(
                "content root does not match the sealed segment",
            ));
        }
        if segment.state == SegmentState::Verified {
            return reject(MetadataError::invalid_transition(
                "segment is already verified",
            ));
        }
        // The registration accepts any proposer-supplied generation, so the
        // ceiling must be rejected deterministically here rather than
        // wrapping (or panicking every replica in checked builds).
        let Some(next_generation) = segment.segment_generation.checked_add(1) else {
            return reject(MetadataError::limit(
                "segment generation space is exhausted",
            ));
        };
        segment.state = SegmentState::Verified;
        segment.segment_generation = next_generation;
        MetadataResponse::Ack {
            generation: next_generation,
        }
    }

    fn put_key_record(
        &mut self,
        key_uuid: Uuid,
        scheme: u16,
        public_material_digest: [u8; 32],
    ) -> MetadataResponse {
        let key = MetaKey::Key { key_uuid }.encode();
        if self.records.contains_key(&key) {
            return reject(MetadataError::AlreadyExists);
        }
        self.records.insert(
            key,
            MetaValue::Key(KeyRecord {
                scheme,
                public_material_digest,
                state: KeyState::Active,
            }),
        );
        MetadataResponse::Ack { generation: 0 }
    }

    fn create_consumer_group(&mut self, name: &str, group_uuid: Uuid) -> MetadataResponse {
        if validate_group_name(name).is_err() {
            return reject(MetadataError::limit(format!(
                "group name must be 1..={} bytes, got {}",
                crate::keys::MAX_GROUP_NAME_BYTES,
                name.len()
            )));
        }
        let group_key = MetaKey::Group { group_uuid }.encode();
        if self.records.contains_key(&group_key) {
            return reject(MetadataError::AlreadyExists);
        }
        let name_key = MetaKey::GroupByName {
            name: name.to_owned(),
        }
        .encode();
        if self.records.contains_key(&name_key) {
            return reject(MetadataError::AlreadyExists);
        }
        self.records.insert(
            name_key,
            MetaValue::GroupName(GroupNameRecord { group_uuid }),
        );
        self.records.insert(
            group_key,
            MetaValue::Group(ConsumerGroupRecord {
                name: name.to_owned(),
                generation: 0,
            }),
        );
        MetadataResponse::GroupCreated {
            group_uuid,
            generation: 0,
        }
    }

    fn join_consumer_group(
        &mut self,
        group_uuid: Uuid,
        member_uuid: Uuid,
        expected_group_generation: u64,
    ) -> MetadataResponse {
        let group_key = MetaKey::Group { group_uuid }.encode();
        let Some(MetaValue::Group(group)) = self.records.get(&group_key) else {
            return reject(MetadataError::NotFound);
        };
        if group.generation != expected_group_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_group_generation,
                actual: group.generation,
            });
        }
        let member_key = MetaKey::GroupMember {
            group_uuid,
            member_uuid,
        }
        .encode();
        if self.records.contains_key(&member_key) {
            return reject(MetadataError::AlreadyExists);
        }
        let Some(next_group_generation) = group.generation.checked_add(1) else {
            return reject(MetadataError::limit("group generation space is exhausted"));
        };
        self.records.insert(
            member_key,
            MetaValue::GroupMember(GroupMemberRecord {
                generation: 0,
                assigned: Vec::new(),
            }),
        );
        let Some(MetaValue::Group(group)) = self.records.get_mut(&group_key) else {
            unreachable!("group record was present above");
        };
        group.generation = next_group_generation;
        MetadataResponse::MemberJoined {
            member_generation: 0,
            group_generation: next_group_generation,
        }
    }

    fn leave_consumer_group(
        &mut self,
        group_uuid: Uuid,
        member_uuid: Uuid,
        expected_member_generation: u64,
    ) -> MetadataResponse {
        let group_key = MetaKey::Group { group_uuid }.encode();
        let Some(MetaValue::Group(group)) = self.records.get(&group_key) else {
            return reject(MetadataError::NotFound);
        };
        let Some(next_group_generation) = group.generation.checked_add(1) else {
            return reject(MetadataError::limit("group generation space is exhausted"));
        };
        let member_key = MetaKey::GroupMember {
            group_uuid,
            member_uuid,
        }
        .encode();
        let Some(MetaValue::GroupMember(member)) = self.records.get(&member_key) else {
            return reject(MetadataError::NotFound);
        };
        if member.generation != expected_member_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_member_generation,
                actual: member.generation,
            });
        }
        self.records.remove(&member_key);
        let Some(MetaValue::Group(group)) = self.records.get_mut(&group_key) else {
            unreachable!("group record was present above");
        };
        group.generation = next_group_generation;
        MetadataResponse::Ack {
            generation: next_group_generation,
        }
    }

    fn assign_member_ranges(
        &mut self,
        group_uuid: Uuid,
        member_uuid: Uuid,
        ranges: &[RangeAssignment],
        expected_member_generation: u64,
    ) -> MetadataResponse {
        if ranges.len() > MAX_ASSIGNED_RANGES {
            return reject(MetadataError::limit(format!(
                "assigned ranges must be <= {MAX_ASSIGNED_RANGES}, got {}",
                ranges.len()
            )));
        }
        let mut seen = BTreeMap::new();
        for assignment in ranges {
            let range_key = MetaKey::Range {
                topic_uuid: assignment.topic_uuid,
                range_uuid: assignment.range_uuid,
            }
            .encode();
            if !matches!(self.records.get(&range_key), Some(MetaValue::Range(_))) {
                return reject(MetadataError::NotFound);
            }
            if seen
                .insert((assignment.topic_uuid, assignment.range_uuid), ())
                .is_some()
            {
                return reject(MetadataError::invalid_transition(
                    "assigned ranges contain a duplicate topic/range pair",
                ));
            }
        }
        // Exclusive ownership: a range may be assigned to at most one live
        // member of the group. Overlapping assignment during rebalance is
        // rejected rather than allowing concurrent cursor commits.
        for (key_bytes, value) in &self.records {
            let Ok(MetaKey::GroupMember {
                group_uuid: other_group,
                member_uuid: other_member,
            }) = MetaKey::decode(key_bytes)
            else {
                continue;
            };
            if other_group != group_uuid || other_member == member_uuid {
                continue;
            }
            let MetaValue::GroupMember(other) = value else {
                continue;
            };
            for assignment in ranges {
                if other.assigned.iter().any(|held| {
                    held.topic_uuid == assignment.topic_uuid
                        && held.range_uuid == assignment.range_uuid
                }) {
                    return reject(MetadataError::invalid_transition(
                        "range is already assigned to another group member",
                    ));
                }
            }
        }
        let group_key = MetaKey::Group { group_uuid }.encode();
        if !matches!(self.records.get(&group_key), Some(MetaValue::Group(_))) {
            return reject(MetadataError::NotFound);
        }
        let member_key = MetaKey::GroupMember {
            group_uuid,
            member_uuid,
        }
        .encode();
        let Some(MetaValue::GroupMember(member)) = self.records.get_mut(&member_key) else {
            return reject(MetadataError::NotFound);
        };
        if member.generation != expected_member_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: expected_member_generation,
                actual: member.generation,
            });
        }
        let Some(next_member_generation) = member.generation.checked_add(1) else {
            return reject(MetadataError::limit("member generation space is exhausted"));
        };
        member.assigned = ranges.to_vec();
        member.generation = next_member_generation;
        let Some(MetaValue::Group(group)) = self.records.get_mut(&group_key) else {
            unreachable!("group record was present above");
        };
        let Some(next_group_generation) = group.generation.checked_add(1) else {
            return reject(MetadataError::limit("group generation space is exhausted"));
        };
        group.generation = next_group_generation;
        MetadataResponse::Ack {
            generation: next_member_generation,
        }
    }

    fn commit_group_cursor(&mut self, args: CommitCursorArgs) -> MetadataResponse {
        let group_key = MetaKey::Group {
            group_uuid: args.group_uuid,
        }
        .encode();
        if !matches!(self.records.get(&group_key), Some(MetaValue::Group(_))) {
            return reject(MetadataError::NotFound);
        }
        let member_key = MetaKey::GroupMember {
            group_uuid: args.group_uuid,
            member_uuid: args.member_uuid,
        }
        .encode();
        let Some(MetaValue::GroupMember(member)) = self.records.get(&member_key) else {
            return reject(MetadataError::NotFound);
        };
        let owns_range = member.assigned.iter().any(|assignment| {
            assignment.topic_uuid == args.topic_uuid && assignment.range_uuid == args.range_uuid
        });
        if !owns_range {
            return reject(MetadataError::invalid_transition(
                "member is not assigned the cursor topic/range",
            ));
        }

        let topic_key = MetaKey::Topic {
            topic_uuid: args.topic_uuid,
        }
        .encode();
        let Some(MetaValue::Topic(topic)) = self.records.get(&topic_key) else {
            return reject(MetadataError::NotFound);
        };
        if topic.topic_epoch != args.topic_epoch {
            return reject(MetadataError::EpochMismatch {
                expected: args.topic_epoch,
                actual: topic.topic_epoch,
            });
        }

        let range_key = MetaKey::Range {
            topic_uuid: args.topic_uuid,
            range_uuid: args.range_uuid,
        }
        .encode();
        let Some(MetaValue::Range(range)) = self.records.get(&range_key) else {
            return reject(MetadataError::NotFound);
        };
        if range.generation != args.range_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: args.range_generation,
                actual: range.generation,
            });
        }

        let segment_key = MetaKey::Segment {
            topic_uuid: args.topic_uuid,
            range_uuid: args.range_uuid,
            segment_uuid: args.segment_uuid,
        }
        .encode();
        let Some(MetaValue::Segment(segment)) = self.records.get(&segment_key) else {
            return reject(MetadataError::NotFound);
        };
        if segment.segment_generation != args.segment_generation {
            return reject(MetadataError::GenerationMismatch {
                expected: args.segment_generation,
                actual: segment.segment_generation,
            });
        }
        if segment.content_root != args.segment_root {
            return reject(MetadataError::invalid_transition(
                "segment root does not match the registered segment",
            ));
        }
        if args.record_offset < segment.base_offset || args.record_offset > segment.next_offset {
            return reject(MetadataError::invalid_transition(format!(
                "record offset {} is outside sealed segment [{}, {}]",
                args.record_offset, segment.base_offset, segment.next_offset
            )));
        }

        let cursor_key = MetaKey::GroupCursor {
            group_uuid: args.group_uuid,
            topic_uuid: args.topic_uuid,
            range_uuid: args.range_uuid,
        }
        .encode();
        match (
            self.records.get(&cursor_key).cloned(),
            args.expected_checkpoint_generation,
        ) {
            (None, None) => {
                self.records.insert(
                    cursor_key,
                    MetaValue::GroupCursor(CursorCheckpointRecord {
                        topic_epoch: args.topic_epoch,
                        range_generation: args.range_generation,
                        segment_uuid: args.segment_uuid,
                        segment_generation: args.segment_generation,
                        segment_root: args.segment_root,
                        record_offset: args.record_offset,
                        record_index: args.record_index,
                        lineage_transition_id: args.lineage_transition_id,
                        checkpoint_generation: 0,
                        committed_by_member: args.member_uuid,
                    }),
                );
                MetadataResponse::CursorCommitted {
                    checkpoint_generation: 0,
                }
            }
            (None, Some(_)) => reject(MetadataError::NotFound),
            (Some(_), None) => reject(MetadataError::AlreadyExists),
            (Some(MetaValue::GroupCursor(existing)), Some(expected)) => {
                if existing.checkpoint_generation != expected {
                    return reject(MetadataError::GenerationMismatch {
                        expected,
                        actual: existing.checkpoint_generation,
                    });
                }
                if existing.topic_epoch != args.topic_epoch {
                    return reject(MetadataError::EpochMismatch {
                        expected: args.topic_epoch,
                        actual: existing.topic_epoch,
                    });
                }
                if let Err(error) = cursor_is_forward_or_equal(&existing, &args) {
                    return reject(error);
                }
                let Some(next_generation) = existing.checkpoint_generation.checked_add(1) else {
                    return reject(MetadataError::limit(
                        "checkpoint generation space is exhausted",
                    ));
                };
                self.records.insert(
                    cursor_key,
                    MetaValue::GroupCursor(CursorCheckpointRecord {
                        topic_epoch: args.topic_epoch,
                        range_generation: args.range_generation,
                        segment_uuid: args.segment_uuid,
                        segment_generation: args.segment_generation,
                        segment_root: args.segment_root,
                        record_offset: args.record_offset,
                        record_index: args.record_index,
                        lineage_transition_id: args.lineage_transition_id,
                        checkpoint_generation: next_generation,
                        committed_by_member: args.member_uuid,
                    }),
                );
                MetadataResponse::CursorCommitted {
                    checkpoint_generation: next_generation,
                }
            }
            (Some(_), Some(_)) => unreachable!("cursor keys only hold cursor records"),
        }
    }

    /// Encode the full state — sorted records plus the dedup FIFO — as one
    /// canonical byte string. Identical states always produce identical
    /// bytes, which the snapshot determinism tests pin.
    pub fn encode_snapshot(&self) -> Result<Vec<u8>, CodecError> {
        let mut out = Vec::with_capacity(64 + self.records.len() * 64);
        put_u16(&mut out, SNAPSHOT_PAYLOAD_VERSION);
        let record_count = u32::try_from(self.records.len())
            .ok()
            .filter(|count| *count <= MAX_SNAPSHOT_RECORDS)
            .ok_or(CodecError::BoundExceeded {
                what: "snapshot record count",
                actual: self.records.len(),
                maximum: MAX_SNAPSHOT_RECORDS as usize,
            })?;
        put_u32(&mut out, record_count);
        for (key, value) in &self.records {
            if key.len() > MAX_SNAPSHOT_KEY_BYTES {
                return Err(CodecError::BoundExceeded {
                    what: "snapshot key",
                    actual: key.len(),
                    maximum: MAX_SNAPSHOT_KEY_BYTES,
                });
            }
            put_u16(&mut out, key.len() as u16);
            out.extend_from_slice(key);
            let encoded = value.encode()?;
            put_u32(&mut out, encoded.len() as u32);
            out.extend_from_slice(&encoded);
        }
        put_u32(&mut out, self.dedup_order.len() as u32);
        for request_id in &self.dedup_order {
            let response = self
                .dedup_responses
                .get(request_id)
                .expect("dedup order and dedup responses always agree");
            put_uuid(&mut out, *request_id);
            let encoded = response.encode()?;
            put_u32(&mut out, encoded.len() as u32);
            out.extend_from_slice(&encoded);
        }
        Ok(out)
    }

    /// Decode a snapshot payload, enforcing canonical form: strictly
    /// ascending unique keys, value tags that match their key category,
    /// bounded lengths, and no trailing bytes.
    pub fn decode_snapshot(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut reader = Reader::new(bytes);
        let version = reader.u16("snapshot payload version")?;
        if version != SNAPSHOT_PAYLOAD_VERSION {
            return Err(CodecError::UnknownTag {
                what: "snapshot payload version",
                tag: u32::from(version),
            });
        }
        let record_count = reader.u32("snapshot record count")?;
        if record_count > MAX_SNAPSHOT_RECORDS {
            return Err(CodecError::BoundExceeded {
                what: "snapshot record count",
                actual: record_count as usize,
                maximum: MAX_SNAPSHOT_RECORDS as usize,
            });
        }
        let mut records = BTreeMap::new();
        let mut previous_key: Option<Vec<u8>> = None;
        for _ in 0..record_count {
            let key_len = reader.u16("snapshot key length")? as usize;
            if key_len > MAX_SNAPSHOT_KEY_BYTES {
                return Err(CodecError::BoundExceeded {
                    what: "snapshot key",
                    actual: key_len,
                    maximum: MAX_SNAPSHOT_KEY_BYTES,
                });
            }
            let key = reader.take(key_len, "snapshot key")?.to_vec();
            if previous_key.as_ref().is_some_and(|prior| *prior >= key) {
                return Err(CodecError::InvalidValue {
                    what: "snapshot key order",
                    reason: "keys must be strictly ascending",
                });
            }
            let typed_key = MetaKey::decode(&key)?;
            let value_len = reader.u32("snapshot value length")? as usize;
            if value_len > MAX_SNAPSHOT_VALUE_BYTES {
                return Err(CodecError::BoundExceeded {
                    what: "snapshot value",
                    actual: value_len,
                    maximum: MAX_SNAPSHOT_VALUE_BYTES,
                });
            }
            let value = MetaValue::decode(reader.take(value_len, "snapshot value")?)?;
            if !key_matches_value(&typed_key, &value) {
                return Err(CodecError::InvalidValue {
                    what: "snapshot record",
                    reason: "value type does not match its key category",
                });
            }
            previous_key = Some(key.clone());
            records.insert(key, value);
        }
        let dedup_count = reader.u32("dedup entry count")?;
        if dedup_count as usize > DEDUP_CAPACITY {
            return Err(CodecError::BoundExceeded {
                what: "dedup entry count",
                actual: dedup_count as usize,
                maximum: DEDUP_CAPACITY,
            });
        }
        let mut dedup_order = VecDeque::with_capacity(dedup_count as usize);
        let mut dedup_responses = HashMap::with_capacity(dedup_count as usize);
        for _ in 0..dedup_count {
            let request_id = reader.uuid("dedup request id")?;
            let response_len = reader.u32("dedup response length")? as usize;
            if response_len > MAX_SNAPSHOT_RESPONSE_BYTES {
                return Err(CodecError::BoundExceeded {
                    what: "dedup response",
                    actual: response_len,
                    maximum: MAX_SNAPSHOT_RESPONSE_BYTES,
                });
            }
            let response = MetadataResponse::decode(reader.take(response_len, "dedup response")?)?;
            if dedup_responses.insert(request_id, response).is_some() {
                return Err(CodecError::InvalidValue {
                    what: "dedup entry",
                    reason: "request id appears twice in the FIFO",
                });
            }
            dedup_order.push_back(request_id);
        }
        reader.finish()?;
        Ok(Self {
            records,
            dedup_order,
            dedup_responses,
        })
    }
}

fn key_matches_value(key: &MetaKey, value: &MetaValue) -> bool {
    matches!(
        (key, value),
        (MetaKey::Node { .. }, MetaValue::Node(_))
            | (MetaKey::Topic { .. }, MetaValue::Topic(_))
            | (MetaKey::TopicByName { .. }, MetaValue::TopicName(_))
            | (MetaKey::Range { .. }, MetaValue::Range(_))
            | (MetaKey::Segment { .. }, MetaValue::Segment(_))
            | (MetaKey::Key { .. }, MetaValue::Key(_))
            | (MetaKey::Group { .. }, MetaValue::Group(_))
            | (MetaKey::GroupByName { .. }, MetaValue::GroupName(_))
            | (MetaKey::GroupMember { .. }, MetaValue::GroupMember(_))
            | (MetaKey::GroupCursor { .. }, MetaValue::GroupCursor(_))
    )
}

struct CommitCursorArgs {
    group_uuid: Uuid,
    member_uuid: Uuid,
    topic_uuid: Uuid,
    range_uuid: Uuid,
    topic_epoch: u64,
    range_generation: u64,
    segment_uuid: Uuid,
    segment_generation: u64,
    segment_root: [u8; 32],
    record_offset: u64,
    record_index: u64,
    lineage_transition_id: Option<Uuid>,
    expected_checkpoint_generation: Option<u64>,
}

fn cursor_is_forward_or_equal(
    existing: &CursorCheckpointRecord,
    args: &CommitCursorArgs,
) -> Result<(), MetadataError> {
    if args.segment_uuid == existing.segment_uuid {
        if args.segment_generation != existing.segment_generation
            || args.segment_root != existing.segment_root
        {
            return Err(MetadataError::invalid_transition(
                "same segment identity changed generation or root",
            ));
        }
        if args.record_offset < existing.record_offset
            || (args.record_offset == existing.record_offset
                && args.record_index < existing.record_index)
        {
            return Err(MetadataError::invalid_transition(
                "cursor moved backward within the same segment",
            ));
        }
        return Ok(());
    }
    // Different segment: require a non-decreasing record offset as a coarse
    // forward signal until split/merge transition evidence lands in a later
    // slice. Equal offsets across segment identity changes are rejected.
    if args.record_offset < existing.record_offset {
        return Err(MetadataError::invalid_transition(
            "cursor moved backward across segment identity",
        ));
    }
    Ok(())
}

fn encode_assigned_ranges(out: &mut Vec<u8>, ranges: &[RangeAssignment]) -> Result<(), CodecError> {
    if ranges.len() > MAX_ASSIGNED_RANGES {
        return Err(CodecError::BoundExceeded {
            what: "assigned ranges",
            actual: ranges.len(),
            maximum: MAX_ASSIGNED_RANGES,
        });
    }
    put_u16(out, ranges.len() as u16);
    for range in ranges {
        put_uuid(out, range.topic_uuid);
        put_uuid(out, range.range_uuid);
    }
    Ok(())
}

fn decode_assigned_ranges(reader: &mut Reader<'_>) -> Result<Vec<RangeAssignment>, CodecError> {
    let count = reader.u16("assigned range count")? as usize;
    if count > MAX_ASSIGNED_RANGES {
        return Err(CodecError::BoundExceeded {
            what: "assigned ranges",
            actual: count,
            maximum: MAX_ASSIGNED_RANGES,
        });
    }
    let mut ranges = Vec::with_capacity(count);
    for _ in 0..count {
        ranges.push(RangeAssignment {
            topic_uuid: reader.uuid("assigned topic uuid")?,
            range_uuid: reader.uuid("assigned range uuid")?,
        });
    }
    Ok(ranges)
}

fn reject(error: MetadataError) -> MetadataResponse {
    MetadataResponse::Rejected(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandEnvelope;

    fn envelope(request: u128) -> CommandEnvelope {
        CommandEnvelope {
            request_id: Uuid::from_u128(request),
            issued_at_ms: 0,
        }
    }

    #[test]
    fn snapshot_round_trip_preserves_records_and_dedup_fifo_byte_exactly() {
        let mut machine = MetaStateMachine::new();
        machine.apply(
            1,
            &MetadataCommand::RegisterNode {
                env: envelope(1),
                node_uuid: Uuid::from_u128(10),
                addr: "n1:9200".to_owned(),
                expected_generation: None,
            },
        );
        machine.apply(
            2,
            &MetadataCommand::CreateTopic {
                env: envelope(2),
                name: "events.v1".to_owned(),
                topic_uuid: Uuid::from_u128(20),
                root_range_uuid: Uuid::from_u128(21),
            },
        );
        let encoded = machine.encode_snapshot().unwrap();
        let decoded = MetaStateMachine::decode_snapshot(&encoded).unwrap();
        assert_eq!(decoded.encode_snapshot().unwrap(), encoded);
        assert_eq!(decoded.record_count(), machine.record_count());
        assert_eq!(decoded.dedup_len(), machine.dedup_len());
    }

    #[test]
    fn snapshot_decode_rejects_unsorted_keys_trailing_bytes_and_unknown_versions() {
        let mut machine = MetaStateMachine::new();
        machine.apply(
            1,
            &MetadataCommand::PutKeyRecord {
                env: envelope(1),
                key_uuid: Uuid::from_u128(40),
                scheme: 1,
                public_material_digest: [1; 32],
            },
        );
        let encoded = machine.encode_snapshot().unwrap();

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert_eq!(
            MetaStateMachine::decode_snapshot(&trailing),
            Err(CodecError::Trailing(1))
        );

        let mut future = encoded.clone();
        future[..2].copy_from_slice(&2_u16.to_be_bytes());
        assert_eq!(
            MetaStateMachine::decode_snapshot(&future),
            Err(CodecError::UnknownTag {
                what: "snapshot payload version",
                tag: 2,
            })
        );

        // Duplicate the single record: the second key is not strictly above
        // the first, so canonical form is violated.
        let mut machine_two = MetaStateMachine::new();
        machine_two.apply(
            1,
            &MetadataCommand::PutKeyRecord {
                env: envelope(1),
                key_uuid: Uuid::from_u128(40),
                scheme: 1,
                public_material_digest: [1; 32],
            },
        );
        let single = machine_two.encode_snapshot().unwrap();
        let record_bytes = &single[6..single.len() - 4 - 16 - 4 - single_dedup_len(&machine_two)];
        let mut duplicated = single[..6].to_vec();
        duplicated[2..6].copy_from_slice(&2_u32.to_be_bytes());
        duplicated.extend_from_slice(record_bytes);
        duplicated.extend_from_slice(record_bytes);
        duplicated.extend_from_slice(&single[6 + record_bytes.len()..]);
        assert!(matches!(
            MetaStateMachine::decode_snapshot(&duplicated),
            Err(CodecError::InvalidValue {
                what: "snapshot key order",
                ..
            })
        ));
    }

    fn single_dedup_len(machine: &MetaStateMachine) -> usize {
        machine
            .dedup_responses
            .values()
            .next()
            .unwrap()
            .encode()
            .unwrap()
            .len()
    }
}

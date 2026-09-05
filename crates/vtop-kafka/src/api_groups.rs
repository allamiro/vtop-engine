//! The group protocol's requests and responses on the wire (#457, slice 2):
//! FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup,
//! OffsetCommit and OffsetFetch, at the versions the header gate serves —
//! every one of them classic (non-flexible), the same rule the phase-1 APIs
//! follow. Decoders bound every array against the request-wide ceilings, so
//! a hostile length is a refusal and never an allocation.

use crate::api::{bounded, partitions_within};
use crate::messages::ErrorCode;
use crate::wire::{Decoder, Encoder, WireError};

/// Assignors one member may offer, at most.
pub const MAX_PROTOCOLS: usize = 64;
/// Members one SyncGroup may assign, at most.
pub const MAX_ASSIGNMENTS: usize = 1024;
/// Metadata one member may attach to one protocol, or one commit, at most.
pub const MAX_MEMBER_METADATA_BYTES: usize = 64 * 1024;
/// Protocol names and metadata one JoinGroup may carry across ALL its
/// protocols, at most (review): the per-protocol cap bounds one field; this
/// bounds what one member makes the coordinator retain until the round
/// completes — the names are kept beside the metadata, so they count.
pub const MAX_JOIN_METADATA_BYTES: usize = 64 * 1024;
pub const MAX_OFFSET_METADATA_BYTES: usize = 4096;

fn bounded_bytes(
    d: &mut Decoder<'_>,
    field: &'static str,
    limit: usize,
) -> Result<Vec<u8>, WireError> {
    let bytes = d.nullable_bytes(field)?.unwrap_or_default();
    if bytes.len() > limit {
        return Err(WireError::TooMany {
            field,
            declared: bytes.len(),
            limit,
        });
    }
    Ok(bytes.to_vec())
}

// --------------------------------------------------------------- FindCoordinator

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorRequest {
    pub key: String,
    /// 0 = group, 1 = transaction (v1+; v0 has groups only).
    pub key_type: i8,
}

pub fn decode_find_coordinator(
    d: &mut Decoder<'_>,
    version: i16,
) -> Result<FindCoordinatorRequest, WireError> {
    let key = d.string("findCoordinator.key")?.to_owned();
    let key_type = if version >= 1 {
        d.i8("findCoordinator.keyType")?
    } else {
        0
    };
    Ok(FindCoordinatorRequest { key, key_type })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorResponse {
    pub error: ErrorCode,
    pub error_message: Option<String>,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

pub fn encode_find_coordinator(out: &mut Encoder, version: i16, r: &FindCoordinatorResponse) {
    if version >= 1 {
        out.i32(0); // throttle_time_ms
    }
    out.i16(r.error.as_i16());
    if version >= 1 {
        out.nullable_string(r.error_message.as_deref());
    }
    out.i32(r.node_id);
    out.string(&r.host);
    out.i32(r.port);
}

// --------------------------------------------------------------- JoinGroup

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupRequest {
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub protocol_type: String,
    pub protocols: Vec<(String, Vec<u8>)>,
}

pub fn decode_join_group(d: &mut Decoder<'_>, version: i16) -> Result<JoinGroupRequest, WireError> {
    let group_id = d.string("joinGroup.groupId")?.to_owned();
    let session_timeout_ms = d.i32("joinGroup.sessionTimeoutMs")?;
    let rebalance_timeout_ms = if version >= 1 {
        d.i32("joinGroup.rebalanceTimeoutMs")?
    } else {
        session_timeout_ms
    };
    let member_id = d.string("joinGroup.memberId")?.to_owned();
    let group_instance_id = if version >= 5 {
        d.nullable_string("joinGroup.groupInstanceId")?
            .map(str::to_owned)
    } else {
        None
    };
    let protocol_type = d.string("joinGroup.protocolType")?.to_owned();
    let count = bounded(d, "joinGroup.protocols", MAX_PROTOCOLS)?;
    let mut protocols = Vec::with_capacity(count);
    let mut retained = 0;
    for _ in 0..count {
        let name = d.string("joinGroup.protocols.name")?.to_owned();
        let metadata = bounded_bytes(d, "joinGroup.protocols.metadata", MAX_MEMBER_METADATA_BYTES)?;
        retained += name.len() + metadata.len();
        if retained > MAX_JOIN_METADATA_BYTES {
            return Err(WireError::TooMany {
                field: "joinGroup.protocols (names and metadata, all protocols)",
                declared: retained,
                limit: MAX_JOIN_METADATA_BYTES,
            });
        }
        protocols.push((name, metadata));
    }
    Ok(JoinGroupRequest {
        group_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        member_id,
        group_instance_id,
        protocol_type,
        protocols,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupResponse {
    pub error: ErrorCode,
    pub generation_id: i32,
    pub protocol_name: String,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<(String, Vec<u8>)>,
}

pub fn encode_join_group(out: &mut Encoder, version: i16, r: &JoinGroupResponse) {
    if version >= 2 {
        out.i32(0); // throttle_time_ms
    }
    out.i16(r.error.as_i16());
    out.i32(r.generation_id);
    out.string(&r.protocol_name);
    out.string(&r.leader);
    out.string(&r.member_id);
    out.array_len(r.members.len());
    for (member_id, metadata) in &r.members {
        out.string(member_id);
        if version >= 5 {
            out.nullable_string(None); // group_instance_id: static membership is not honoured
        }
        out.nullable_bytes(Some(metadata));
    }
}

// --------------------------------------------------------------- SyncGroup

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncGroupRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub assignments: Vec<(String, Vec<u8>)>,
}

pub fn decode_sync_group(d: &mut Decoder<'_>, version: i16) -> Result<SyncGroupRequest, WireError> {
    let group_id = d.string("syncGroup.groupId")?.to_owned();
    let generation_id = d.i32("syncGroup.generationId")?;
    let member_id = d.string("syncGroup.memberId")?.to_owned();
    let group_instance_id = if version >= 3 {
        d.nullable_string("syncGroup.groupInstanceId")?
            .map(str::to_owned)
    } else {
        None
    };
    let count = bounded(d, "syncGroup.assignments", MAX_ASSIGNMENTS)?;
    let mut assignments = Vec::with_capacity(count);
    for _ in 0..count {
        let member = d.string("syncGroup.assignments.memberId")?.to_owned();
        let assignment = bounded_bytes(
            d,
            "syncGroup.assignments.assignment",
            MAX_MEMBER_METADATA_BYTES,
        )?;
        assignments.push((member, assignment));
    }
    Ok(SyncGroupRequest {
        group_id,
        generation_id,
        member_id,
        group_instance_id,
        assignments,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncGroupResponse {
    pub error: ErrorCode,
    pub assignment: Vec<u8>,
}

pub fn encode_sync_group(out: &mut Encoder, version: i16, r: &SyncGroupResponse) {
    if version >= 1 {
        out.i32(0); // throttle_time_ms
    }
    out.i16(r.error.as_i16());
    out.nullable_bytes(Some(&r.assignment));
}

// --------------------------------------------------------------- Heartbeat / LeaveGroup

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
}

pub fn decode_heartbeat(d: &mut Decoder<'_>, version: i16) -> Result<HeartbeatRequest, WireError> {
    let group_id = d.string("heartbeat.groupId")?.to_owned();
    let generation_id = d.i32("heartbeat.generationId")?;
    let member_id = d.string("heartbeat.memberId")?.to_owned();
    if version >= 3 {
        let _ = d.nullable_string("heartbeat.groupInstanceId")?;
    }
    Ok(HeartbeatRequest {
        group_id,
        generation_id,
        member_id,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    pub member_id: String,
}

pub fn decode_leave_group(
    d: &mut Decoder<'_>,
    _version: i16,
) -> Result<LeaveGroupRequest, WireError> {
    let group_id = d.string("leaveGroup.groupId")?.to_owned();
    let member_id = d.string("leaveGroup.memberId")?.to_owned();
    Ok(LeaveGroupRequest {
        group_id,
        member_id,
    })
}

/// Heartbeat and LeaveGroup answer the same way: an error code, behind a
/// throttle from v1 on.
pub fn encode_error_only(out: &mut Encoder, version: i16, error: ErrorCode) {
    if version >= 1 {
        out.i32(0); // throttle_time_ms
    }
    out.i16(error.as_i16());
}

// --------------------------------------------------------------- OffsetCommit

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitPartition {
    pub partition: i32,
    pub offset: i64,
    pub metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitTopic {
    pub name: String,
    pub partitions: Vec<OffsetCommitPartition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    /// v7: a static member's instance id. No static member exists on this
    /// gateway (review), so a commit naming one is an unknown member's.
    pub group_instance_id: Option<String>,
    pub topics: Vec<OffsetCommitTopic>,
}

pub fn decode_offset_commit(
    d: &mut Decoder<'_>,
    version: i16,
) -> Result<OffsetCommitRequest, WireError> {
    let group_id = d.string("offsetCommit.groupId")?.to_owned();
    let generation_id = d.i32("offsetCommit.generationId")?;
    let member_id = d.string("offsetCommit.memberId")?.to_owned();
    // v5 is the floor (see the version table): the per-commit retention time
    // of v2 to v4 is not a field this decoder is asked to read.
    let group_instance_id = if version >= 7 {
        d.nullable_string("offsetCommit.groupInstanceId")?
            .map(str::to_owned)
    } else {
        None
    };
    let topic_count = bounded(d, "offsetCommit.topics", crate::api::MAX_TOPICS)?;
    let mut topics = Vec::with_capacity(topic_count);
    let mut total = 0;
    for _ in 0..topic_count {
        let name = d.string("offsetCommit.topics.name")?.to_owned();
        let partition_count = partitions_within(d, "offsetCommit.partitions", &mut total)?;
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            let partition = d.i32("offsetCommit.partitions.partitionIndex")?;
            let offset = d.i64("offsetCommit.partitions.committedOffset")?;
            if version >= 6 {
                let _ = d.i32("offsetCommit.partitions.committedLeaderEpoch")?;
            }
            let metadata = d
                .nullable_string("offsetCommit.partitions.committedMetadata")?
                .map(str::to_owned);
            partitions.push(OffsetCommitPartition {
                partition,
                offset,
                metadata,
            });
        }
        topics.push(OffsetCommitTopic { name, partitions });
    }
    Ok(OffsetCommitRequest {
        group_id,
        generation_id,
        member_id,
        group_instance_id,
        topics,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitTopicResponse {
    pub name: String,
    pub partitions: Vec<(i32, ErrorCode)>,
}

pub fn encode_offset_commit(out: &mut Encoder, version: i16, topics: &[OffsetCommitTopicResponse]) {
    if version >= 3 {
        out.i32(0); // throttle_time_ms
    }
    out.array_len(topics.len());
    for topic in topics {
        out.string(&topic.name);
        out.array_len(topic.partitions.len());
        for (partition, error) in &topic.partitions {
            out.i32(*partition);
            out.i16(error.as_i16());
        }
    }
}

// --------------------------------------------------------------- OffsetFetch

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchRequest {
    pub group_id: String,
    /// `None` (v2+): every partition the group has committed.
    pub topics: Option<Vec<(String, Vec<i32>)>>,
}

pub fn decode_offset_fetch(
    d: &mut Decoder<'_>,
    version: i16,
) -> Result<OffsetFetchRequest, WireError> {
    let group_id = d.string("offsetFetch.groupId")?.to_owned();
    let declared = d.array_len("offsetFetch.topics")?;
    let topics = match declared {
        None if version >= 2 => None,
        None => Some(Vec::new()),
        Some(count) => {
            if count > crate::api::MAX_TOPICS {
                return Err(WireError::TooMany {
                    field: "offsetFetch.topics",
                    declared: count,
                    limit: crate::api::MAX_TOPICS,
                });
            }
            let mut topics = Vec::with_capacity(count);
            let mut total = 0;
            for _ in 0..count {
                let name = d.string("offsetFetch.topics.name")?.to_owned();
                let partition_count =
                    partitions_within(d, "offsetFetch.partitionIndexes", &mut total)?;
                let mut partitions = Vec::with_capacity(partition_count);
                for _ in 0..partition_count {
                    partitions.push(d.i32("offsetFetch.partitionIndexes.partition")?);
                }
                topics.push((name, partitions));
            }
            Some(topics)
        }
    };
    Ok(OffsetFetchRequest { group_id, topics })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchPartitionResponse {
    pub partition: i32,
    /// -1 when nothing is committed.
    pub offset: i64,
    pub metadata: Option<String>,
    pub error: ErrorCode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopicResponse {
    pub name: String,
    pub partitions: Vec<OffsetFetchPartitionResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchResponse {
    /// Group-level (v2+); per-partition codes carry the rest.
    pub error: ErrorCode,
    pub topics: Vec<OffsetFetchTopicResponse>,
}

pub fn encode_offset_fetch(out: &mut Encoder, version: i16, r: &OffsetFetchResponse) {
    if version >= 3 {
        out.i32(0); // throttle_time_ms
    }
    out.array_len(r.topics.len());
    for topic in &r.topics {
        out.string(&topic.name);
        out.array_len(topic.partitions.len());
        for p in &topic.partitions {
            out.i32(p.partition);
            out.i64(p.offset);
            if version >= 5 {
                out.i32(-1); // committed_leader_epoch: none kept
            }
            out.nullable_string(p.metadata.as_deref());
            out.i16(p.error.as_i16());
        }
    }
    if version >= 2 {
        out.i16(r.error.as_i16());
    }
}

/// What a SyncGroup's assignments have spent, across all of them (review):
/// topic entries against `MAX_TOPICS`, partitions against
/// `MAX_PARTITIONS_PER_REQUEST`.
#[derive(Debug, Default, Clone, Copy)]
pub struct AssignmentBudget {
    pub topics: usize,
    pub partitions: usize,
}

/// The consumer protocol's assignment, as a leader's assignor writes it
/// (`ConsumerProtocolAssignment`): a version, the topics with their
/// partitions, and opaque user data. Decoded only to be checked against the
/// gateway's topic map — the bytes themselves pass through to the member.
/// `budget` is what the request has spent so far across EVERY assignment
/// (review): one SyncGroup has one budget of topic entries and one of
/// partitions, not one per assignment, so a leader cannot materialize a
/// thousand budgets' worth of either. Each topic name is kept once with its
/// partitions (review): a 32 KiB name over 4 096 partitions is one name, not
/// 4 096 copies — what an assignment costs is what it carries.
pub fn consumer_assignment_partitions(
    bytes: &[u8],
    budget: &mut AssignmentBudget,
) -> Result<Vec<(String, Vec<i32>)>, WireError> {
    let mut d = Decoder::new(bytes);
    let _version = d.i16("assignment.version")?;
    let topic_count = bounded(&mut d, "assignment.topics", crate::api::MAX_TOPICS)?;
    budget.topics += topic_count;
    if budget.topics > crate::api::MAX_TOPICS {
        return Err(WireError::TooMany {
            field: "assignment.topics (all assignments)",
            declared: budget.topics,
            limit: crate::api::MAX_TOPICS,
        });
    }
    let mut out: Vec<(String, Vec<i32>)> = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = d.string("assignment.topics.name")?.to_owned();
        let count = partitions_within(
            &mut d,
            "assignment.topics.partitions",
            &mut budget.partitions,
        )?;
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            partitions.push(d.i32("assignment.topics.partition")?);
        }
        out.push((name, partitions));
    }
    // user_data may follow; an assignor that wrote none is not malformed.
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn join_v1(protocols: &[(&str, usize)]) -> Vec<u8> {
        let mut e = Encoder::new();
        e.string("g");
        e.i32(10_000);
        e.i32(10_000);
        e.string("");
        e.string("consumer");
        e.array_len(protocols.len());
        for (name, bytes) in protocols {
            e.string(name);
            e.nullable_bytes(Some(&vec![0u8; *bytes]));
        }
        e.into_vec()
    }

    /// An assignment of `topics` topics with `per_topic` partitions each —
    /// every topic under the per-topic cap, the whole under the request cap.
    fn assignment_of(topics: usize, per_topic: i32) -> Vec<u8> {
        let mut e = Encoder::new();
        e.i16(0);
        e.array_len(topics);
        for t in 0..topics {
            e.string(&format!("t{t}"));
            e.array_len(per_topic as usize);
            for p in 0..per_topic {
                e.i32(p);
            }
        }
        e.nullable_bytes(None);
        e.into_vec()
    }

    /// One budget for the whole SyncGroup (review): two assignments of 3 000
    /// partitions each decode alone and are refused together, and so do two
    /// of 600 topic entries carrying no partition at all.
    #[test]
    fn assignments_share_one_budget_of_topics_and_partitions() {
        let one = assignment_of(3, 1_000);
        let mut alone = AssignmentBudget::default();
        let decoded = consumer_assignment_partitions(&one, &mut alone).unwrap();
        assert_eq!(decoded.len(), 3, "each name once");
        assert_eq!(decoded.iter().map(|(_, p)| p.len()).sum::<usize>(), 3_000);
        let mut shared = AssignmentBudget::default();
        assert!(consumer_assignment_partitions(&one, &mut shared).is_ok());
        assert!(matches!(
            consumer_assignment_partitions(&one, &mut shared),
            Err(WireError::TooMany { limit, .. }) if limit == crate::api::MAX_PARTITIONS_PER_REQUEST
        ));
        let entries = assignment_of(600, 0);
        let mut shared = AssignmentBudget::default();
        assert_eq!(
            consumer_assignment_partitions(&entries, &mut shared)
                .unwrap()
                .len(),
            600
        );
        assert!(matches!(
            consumer_assignment_partitions(&entries, &mut shared),
            Err(WireError::TooMany { limit, .. }) if limit == crate::api::MAX_TOPICS
        ));
    }

    /// Every protocol under the per-field cap, and the request over the
    /// budget across them (review): refused at the wire, as one field over
    /// its cap is; one protocol within the budget decodes.
    #[test]
    fn join_metadata_is_budgeted_across_protocols() {
        let over = join_v1(&[("range", 40 * 1024), ("roundrobin", 40 * 1024)]);
        assert!(matches!(
            decode_join_group(&mut Decoder::new(&over), 1),
            Err(WireError::TooMany { limit, .. }) if limit == MAX_JOIN_METADATA_BYTES
        ));
        let within = join_v1(&[("range", 40 * 1024)]);
        let request = decode_join_group(&mut Decoder::new(&within), 1).unwrap();
        assert_eq!(request.protocols.len(), 1);
        assert_eq!(request.protocols[0].1.len(), 40 * 1024);
        // Names count too (review): they are retained beside the metadata.
        let long = "n".repeat(30_000);
        let names = join_v1(&[(&long, 0), (&long, 0), (&long, 0)]);
        assert!(matches!(
            decode_join_group(&mut Decoder::new(&names), 1),
            Err(WireError::TooMany { limit, .. }) if limit == MAX_JOIN_METADATA_BYTES
        ));
    }
}

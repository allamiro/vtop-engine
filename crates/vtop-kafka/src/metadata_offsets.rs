//! Committed offsets as lineage-bound cursors on the metadata plane (#457,
//! slice 2b): the [`OffsetStore`] a node's gateway gets when it holds its
//! range under a lease.
//!
//! A Kafka group commits a position; the plane keeps it as an UNPINNED head
//! cursor (#468) — no segment named, bound to the topic epoch and the range's
//! lineage generation, so the same integer can never be mistaken for a record
//! of a recreated topic or an evolved range. The gateway serving the range is
//! the plane's member: one member per group per range, whichever node holds
//! the lease, since the plane gives a range to at most one live member and a
//! per-node identity would collide with itself after a failover.
//!
//! The store speaks to the plane through [`CursorPlane`]: `AdminClient` on a
//! node, a state machine in memory in the tests. Every write is a CAS the
//! plane judges — group generation, member generation, checkpoint generation,
//! lineage — and every mismatch carries the plane's actual, which the store
//! adopts and retries a bounded number of times: the plane is the truth, the
//! store's cache a guess. What a consumer commits with its offset — the
//! metadata string — is not kept on the plane in this slice: the position is,
//! a fetch answers it with no metadata, and the store says so once.
//!
//! Every commit is FENCED by the range's lease (review): it carries this
//! node and the fencing epoch the broker holds the range at, and the plane
//! takes it only from the range's current leaseholder at that epoch. A node
//! whose lease moved on — stolen, or lapsed and re-granted while its Kafka
//! listener stayed reachable — cannot move a group's position after its
//! successor has: the store refuses before asking the plane when the broker
//! says the lease is gone, and the plane refuses when it knows better.

use crate::messages::ErrorCode;
use crate::offsets::{Committed, OffsetStore};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use vtop_meta::command::derived_group_uuid;
use vtop_meta::transport::{AdminGroupCursorView, AdminReadGroupCursorResponse};
use vtop_meta::{CommandEnvelope, MetadataCommand, MetadataError, MetadataResponse};

/// The plane as the store speaks to it: one proposal, one read. A node's
/// `AdminClient` implements it; a test's implementation holds a
/// `MetaStateMachine` in memory.
#[async_trait::async_trait]
pub trait CursorPlane: Send + Sync + 'static {
    async fn propose(&self, command: MetadataCommand) -> Result<MetadataResponse, PlaneError>;
    async fn read_group_cursor(
        &self,
        group_uuid: Uuid,
        topic_uuid: Uuid,
        range_uuid: Uuid,
    ) -> Result<AdminReadGroupCursorResponse, PlaneError>;
}

pub use crate::lease::{LeaseState, LeaseView};

/// The plane could not be reached, or refused at the transport: the
/// transport's own words.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaneError(pub String);

#[async_trait::async_trait]
impl CursorPlane for vtop_meta::AdminClient {
    async fn propose(&self, command: MetadataCommand) -> Result<MetadataResponse, PlaneError> {
        vtop_meta::AdminClient::propose(self, command)
            .await
            .map(|answer| answer.response)
            .map_err(|error| PlaneError(error.to_string()))
    }

    async fn read_group_cursor(
        &self,
        group_uuid: Uuid,
        topic_uuid: Uuid,
        range_uuid: Uuid,
    ) -> Result<AdminReadGroupCursorResponse, PlaneError> {
        vtop_meta::AdminClient::read_group_cursor(self, group_uuid, topic_uuid, range_uuid)
            .await
            .map_err(|error| PlaneError(error.to_string()))
    }
}

/// The range this gateway serves, as the plane names it.
#[derive(Debug, Clone)]
pub struct RangeIdentity {
    /// The cluster id every group UUID is derived under, so every node of the
    /// cluster and the operator's tool agree on it without a lookup.
    pub cluster_id: Uuid,
    /// The Kafka topic name this range is served as — the only name a commit
    /// or a fetch here can be about.
    pub topic: String,
    pub topic_uuid: Uuid,
    pub range_uuid: Uuid,
    /// The topic's epoch, the identity a cursor is bound to: a recreated topic
    /// has another, and its cursors mean nothing here.
    pub topic_epoch: u64,
    /// This node, as the plane knows the range's leaseholder.
    pub holder_node_uuid: Uuid,
    /// The Kafka partition this range is served as (#457 slice 3). A gateway
    /// leading partition 2 commits and fetches for partition 2, and the store
    /// answers for that partition alone — zero on a single-partition topic,
    /// which is every deployment that names no topology.
    pub partition: i32,
}

/// A partition this coordinator can store a cursor on besides its own
/// (#457 slice 4c): a range another node leads, whose identity the topology
/// named. The fence on a commit for it is still THIS node's lease.
#[derive(Debug, Clone)]
pub struct PartitionRange {
    pub topic: String,
    pub partition: i32,
    pub topic_uuid: Uuid,
    pub range_uuid: Uuid,
    pub topic_epoch: u64,
}

/// How many times a CAS is retried with the plane's actual before the store
/// gives up: each mismatch carries the value that would have succeeded, so
/// one retry usually does it, and a bound keeps a plane that keeps moving
/// from holding the request past its deadline.
const CAS_ATTEMPTS: usize = 4;

/// What the store remembers of a group on one range: the plane's identities
/// for it and the last CAS tokens it saw. A guess, corrected by every answer.
#[derive(Debug, Clone, Default)]
struct RangeCursorState {
    /// The membership ladder — group, member, assignment — has been walked
    /// and stood; a `NotFound` from the plane unsets it.
    member_stands: bool,
    /// The last checkpoint generation the plane confirmed, if any.
    checkpoint: Option<u64>,
    /// The range's lineage generation as last confirmed.
    lineage: u64,
}

/// What the store remembers of a group: one cursor state per range the
/// coordinator has committed on, this node's included.
#[derive(Debug, Clone, Default)]
struct GroupState {
    ranges: HashMap<Uuid, RangeCursorState>,
}

/// Where a commit or a fetch lands: this node's range, or a peer's.
struct CursorTarget {
    topic_uuid: Uuid,
    range_uuid: Uuid,
    topic_epoch: u64,
    /// `true` when the fence is this node's own range lease on a range it
    /// does not lead (#457 slice 4c).
    coordinated: bool,
}

pub struct MetadataOffsetStore {
    plane: Arc<dyn CursorPlane>,
    lease: Arc<dyn LeaseView>,
    range: RangeIdentity,
    /// Other partitions this coordinator stores cursors on (#457 slice 4c).
    peers: Vec<PartitionRange>,
    groups: Mutex<HashMap<String, GroupState>>,
    /// The metadata warning is given once per store, not per commit.
    metadata_dropped_said: AtomicBool,
}

impl MetadataOffsetStore {
    pub fn new(
        plane: Arc<dyn CursorPlane>,
        lease: Arc<dyn LeaseView>,
        range: RangeIdentity,
    ) -> Self {
        Self {
            plane,
            lease,
            range,
            peers: Vec::new(),
            groups: Mutex::new(HashMap::new()),
            metadata_dropped_said: AtomicBool::new(false),
        }
    }

    /// The partitions this coordinator stores besides its own (#457 slice
    /// 4c). A topology that named no range identity for a peer is simply
    /// absent here: a commit for that partition is `UNKNOWN_TOPIC_OR_PARTITION`
    /// by name, not a guess at its UUID.
    pub fn with_peers(mut self, peers: Vec<PartitionRange>) -> Self {
        self.peers = peers;
        self
    }

    fn group_uuid(&self, group: &str) -> Uuid {
        derived_group_uuid(self.range.cluster_id, group)
    }

    /// The gateway serving `range_uuid`, as the plane's member of the group:
    /// one per group per range, whichever node holds that range's lease —
    /// or, for a coordinated commit, whichever coordinator first stood the
    /// member against it. Derived from the TARGET range so a later leader of
    /// that partition names the same member.
    fn member_uuid_for(&self, group_uuid: Uuid, range_uuid: Uuid) -> Uuid {
        Uuid::new_v5(&group_uuid, range_uuid.as_bytes())
    }

    fn cursor_target(&self, topic: &str, partition: i32) -> Option<CursorTarget> {
        if self.is_this_range(topic, partition) {
            return Some(CursorTarget {
                topic_uuid: self.range.topic_uuid,
                range_uuid: self.range.range_uuid,
                topic_epoch: self.range.topic_epoch,
                coordinated: false,
            });
        }
        self.peers
            .iter()
            .find(|peer| peer.topic == topic && peer.partition == partition)
            .map(|peer| CursorTarget {
                topic_uuid: peer.topic_uuid,
                range_uuid: peer.range_uuid,
                topic_epoch: peer.topic_epoch,
                coordinated: true,
            })
    }

    fn range_state(&self, group: &str, range_uuid: Uuid) -> RangeCursorState {
        self.groups
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(group)
            .and_then(|state| state.ranges.get(&range_uuid))
            .cloned()
            .unwrap_or_default()
    }

    fn remember_range(&self, group: &str, range_uuid: Uuid, range: RangeCursorState) {
        self.groups
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(group.to_owned())
            .or_default()
            .ranges
            .insert(range_uuid, range);
    }

    fn envelope() -> CommandEnvelope {
        CommandEnvelope {
            request_id: Uuid::new_v4(),
            issued_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since| since.as_millis() as i64)
                .unwrap_or(0),
        }
    }

    async fn propose(&self, command: MetadataCommand) -> Result<MetadataResponse, ErrorCode> {
        self.plane
            .propose(command)
            .await
            .map_err(|PlaneError(error)| {
                tracing::warn!(
                    range = %self.range.range_uuid,
                    error,
                    "kafka offsets: the metadata plane did not answer; the client retries"
                );
                ErrorCode::CoordinatorNotAvailable
            })
    }

    /// The membership this gateway needs, in one fenced step (#457 slice 2b,
    /// slice 4c): the plane makes the group, the member and the range
    /// assignment stand. On this node's own range the fence is that range's
    /// lease; on a partition it does not lead the fence is the lease it
    /// DOES hold. Walked once per group per range per store, and again when
    /// the plane says what stood is gone.
    async fn ensure_member(
        &self,
        group: &str,
        group_uuid: Uuid,
        fencing_epoch: u64,
        target: &CursorTarget,
    ) -> Result<(), ErrorCode> {
        let member_uuid = self.member_uuid_for(group_uuid, target.range_uuid);
        let command = if target.coordinated {
            MetadataCommand::EnsureGroupMemberCoordinated {
                env: Self::envelope(),
                name: group.to_owned(),
                group_uuid,
                member_uuid,
                topic_uuid: target.topic_uuid,
                range_uuid: target.range_uuid,
                coordinator_topic_uuid: self.range.topic_uuid,
                coordinator_range_uuid: self.range.range_uuid,
                holder_node_uuid: self.range.holder_node_uuid,
                fencing_epoch,
            }
        } else {
            MetadataCommand::EnsureGroupMemberForRange {
                env: Self::envelope(),
                name: group.to_owned(),
                group_uuid,
                member_uuid,
                topic_uuid: target.topic_uuid,
                range_uuid: target.range_uuid,
                holder_node_uuid: self.range.holder_node_uuid,
                fencing_epoch,
            }
        };
        let answer = self.propose(command).await?;
        match answer {
            MetadataResponse::Ack { .. } => Ok(()),
            MetadataResponse::Rejected(MetadataError::InvalidTransition(reason))
                if reason == vtop_meta::command::NOT_LEASEHOLDER =>
            {
                tracing::warn!(
                    group,
                    range = %self.range.range_uuid,
                    fencing_epoch,
                    "kafka offsets: the plane does not hold this node as the range's leaseholder at \
                     this epoch; the client finds its coordinator again"
                );
                Err(ErrorCode::NotCoordinator)
            }
            MetadataResponse::Rejected(MetadataError::InvalidTransition(reason)) => {
                // Another member of the group holds this range on the plane:
                // a gateway of another node still standing after a handoff,
                // or an operator's assignment. Not ours to take; the client
                // retries, and the plane's record says whose.
                tracing::warn!(
                    group,
                    range = %self.range.range_uuid,
                    reason,
                    "kafka offsets: the range is held by another member on the plane; commits wait for it"
                );
                Err(ErrorCode::CoordinatorNotAvailable)
            }
            MetadataResponse::Rejected(MetadataError::NotFound) => {
                tracing::warn!(
                    group,
                    range = %self.range.range_uuid,
                    "kafka offsets: the plane does not know this range; a commit cannot be a cursor on it"
                );
                Err(ErrorCode::UnknownTopicOrPartition)
            }
            MetadataResponse::Rejected(MetadataError::AlreadyExists) => {
                tracing::warn!(
                    group,
                    "kafka offsets: the plane holds this group's name for another group"
                );
                Err(ErrorCode::InvalidGroupId)
            }
            MetadataResponse::Rejected(MetadataError::Limit(reason)) => {
                tracing::warn!(
                    group,
                    reason,
                    "kafka offsets: the plane refuses the membership"
                );
                Err(ErrorCode::InvalidGroupId)
            }
            other => Err(self.unexpected("EnsureGroupMemberForRange", group, other)),
        }
    }

    fn unexpected(&self, step: &str, group: &str, answer: MetadataResponse) -> ErrorCode {
        tracing::warn!(
            step,
            group,
            range = %self.range.range_uuid,
            ?answer,
            "kafka offsets: the plane answered with what this step has no rule for; the client retries"
        );
        ErrorCode::CoordinatorNotAvailable
    }

    fn kept_moving(&self, step: &str, group: &str) -> ErrorCode {
        tracing::warn!(
            step,
            group,
            range = %self.range.range_uuid,
            attempts = CAS_ATTEMPTS,
            "kafka offsets: the plane's generation kept moving under the CAS; the client retries"
        );
        ErrorCode::CoordinatorNotAvailable
    }

    /// The group's cursor on `target` under THAT topic epoch: a cursor of
    /// another epoch is a recreated topic's, naming a position in a topic
    /// that is gone.
    async fn read(
        &self,
        group_uuid: Uuid,
        target: &CursorTarget,
    ) -> Result<Option<AdminGroupCursorView>, ErrorCode> {
        let cursor = self.read_any(group_uuid, target).await?;
        Ok(cursor.filter(|cursor| {
            let current = cursor.topic_epoch == target.topic_epoch;
            if !current {
                tracing::debug!(
                    %group_uuid,
                    cursor_epoch = cursor.topic_epoch,
                    topic_epoch = target.topic_epoch,
                    "kafka offsets: a cursor of another topic epoch is not a position here"
                );
            }
            current
        }))
    }

    /// A cursor read, as the wire answers it: whatever the group has on this
    /// range, whichever topic epoch it belongs to.
    async fn read_any(
        &self,
        group_uuid: Uuid,
        target: &CursorTarget,
    ) -> Result<Option<AdminGroupCursorView>, ErrorCode> {
        let view = self
            .plane
            .read_group_cursor(group_uuid, target.topic_uuid, target.range_uuid)
            .await
            .map_err(|PlaneError(error)| {
                tracing::warn!(
                    range = %target.range_uuid,
                    error,
                    "kafka offsets: the metadata plane did not answer a read; the client retries"
                );
                ErrorCode::CoordinatorNotAvailable
            })?;
        Ok(view.cursor)
    }

    fn is_this_range(&self, topic: &str, partition: i32) -> bool {
        partition == self.range.partition && topic == self.range.topic
    }
}

#[async_trait::async_trait]
impl OffsetStore for MetadataOffsetStore {
    async fn commit(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
        committed: Committed,
    ) -> Result<(), ErrorCode> {
        // The commit path's lines, held here too (review): the listener
        // judges them first, the store for any other caller.
        let Some(target) = self.cursor_target(topic, partition) else {
            return Err(ErrorCode::UnknownTopicOrPartition);
        };
        if committed.offset < 0 {
            return Err(ErrorCode::OffsetOutOfRange);
        }
        if committed
            .metadata
            .as_ref()
            .is_some_and(|m| m.len() > crate::api_groups::MAX_OFFSET_METADATA_BYTES)
        {
            return Err(ErrorCode::OffsetMetadataTooLarge);
        }
        if vtop_meta::validate_group_name(group).is_err() {
            return Err(ErrorCode::InvalidGroupId);
        }
        if committed.metadata.as_ref().is_some_and(|m| !m.is_empty())
            && !self.metadata_dropped_said.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                group,
                "kafka offsets: a commit carries metadata; the plane keeps the position, not the \
                 metadata, in this slice — a fetch answers the offset with none (said once)"
            );
        }
        // The lease, before anything else (review): a node that does not
        // hold the range takes no commit for it, and says which code.
        let fencing_epoch = match self.lease.lease() {
            LeaseState::Held(epoch) => epoch,
            LeaseState::Gone => {
                tracing::warn!(
                    group,
                    range = %self.range.range_uuid,
                    "kafka offsets: this node does not hold the range's lease; the commit is refused \
                     and the client finds its coordinator again"
                );
                return Err(ErrorCode::NotCoordinator);
            }
            LeaseState::Unknown => {
                tracing::debug!(
                    group,
                    range = %self.range.range_uuid,
                    "kafka offsets: the broker's lease view is busy; the commit is not taken now and \
                     the client retries"
                );
                return Err(ErrorCode::CoordinatorNotAvailable);
            }
        };
        let group_uuid = self.group_uuid(group);
        let member_uuid = self.member_uuid_for(group_uuid, target.range_uuid);
        let mut state = self.range_state(group, target.range_uuid);
        if !state.member_stands {
            self.ensure_member(group, group_uuid, fencing_epoch, &target)
                .await?;
            state.member_stands = true;
            self.remember_range(group, target.range_uuid, state.clone());
        }
        let mut re_stood = false;
        for _ in 0..CAS_ATTEMPTS {
            let answer = self
                .propose(if target.coordinated {
                    MetadataCommand::CommitGroupCursorCoordinated {
                        env: Self::envelope(),
                        group_uuid,
                        member_uuid,
                        topic_uuid: target.topic_uuid,
                        range_uuid: target.range_uuid,
                        coordinator_topic_uuid: self.range.topic_uuid,
                        coordinator_range_uuid: self.range.range_uuid,
                        topic_epoch: target.topic_epoch,
                        range_generation: state.lineage,
                        segment_uuid: Uuid::nil(),
                        segment_generation: 0,
                        segment_root: [0; 32],
                        record_offset: committed.offset as u64,
                        record_index: 0,
                        lineage_transition_id: None,
                        expected_checkpoint_generation: state.checkpoint,
                        holder_node_uuid: self.range.holder_node_uuid,
                        fencing_epoch,
                    }
                } else {
                    MetadataCommand::CommitGroupCursorFenced {
                        env: Self::envelope(),
                        group_uuid,
                        member_uuid,
                        topic_uuid: target.topic_uuid,
                        range_uuid: target.range_uuid,
                        topic_epoch: target.topic_epoch,
                        range_generation: state.lineage,
                        segment_uuid: Uuid::nil(),
                        segment_generation: 0,
                        segment_root: [0; 32],
                        record_offset: committed.offset as u64,
                        record_index: 0,
                        lineage_transition_id: None,
                        expected_checkpoint_generation: state.checkpoint,
                        holder_node_uuid: self.range.holder_node_uuid,
                        fencing_epoch,
                    }
                })
                .await?;
            match answer {
                MetadataResponse::CursorCommitted {
                    checkpoint_generation,
                } => {
                    state.checkpoint = Some(checkpoint_generation);
                    self.remember_range(group, target.range_uuid, state);
                    return Ok(());
                }
                MetadataResponse::Rejected(MetadataError::LineageMismatch { actual, .. }) => {
                    // The range evolved: the cursor is bound to the lineage the
                    // plane has now.
                    state.lineage = actual;
                }
                MetadataResponse::Rejected(MetadataError::GenerationMismatch {
                    actual, ..
                }) => {
                    // Committed from elsewhere since — a gateway of another
                    // node, an operator: the plane's checkpoint is the token.
                    state.checkpoint = Some(actual);
                }
                MetadataResponse::Rejected(MetadataError::AlreadyExists) => {
                    // A cursor stands and the store guessed none: read the
                    // checkpoint the CAS needs — unfiltered (review), because
                    // a cursor of another topic epoch is what the plane is
                    // refusing here, and filtering it out would leave the
                    // guess unchanged and the commit looping to no purpose.
                    match self.read_any(group_uuid, &target).await? {
                        Some(cursor) if cursor.topic_epoch == target.topic_epoch => {
                            state.checkpoint = Some(cursor.checkpoint_generation);
                        }
                        Some(cursor) => {
                            tracing::warn!(
                                group,
                                cursor_epoch = cursor.topic_epoch,
                                topic_epoch = target.topic_epoch,
                                "kafka offsets: the group's cursor on this range belongs to another \
                                 topic epoch; this topic was recreated under it"
                            );
                            self.remember_range(group, target.range_uuid, state);
                            return Err(ErrorCode::UnknownTopicOrPartition);
                        }
                        None => {
                            // It stood a moment ago and does not now: the
                            // client retries and the next attempt sees it.
                            self.remember_range(group, target.range_uuid, state);
                            return Err(ErrorCode::CoordinatorNotAvailable);
                        }
                    }
                }
                MetadataResponse::Rejected(MetadataError::InvalidTransition(reason))
                    if reason == vtop_meta::command::NOT_ASSIGNED && !re_stood =>
                {
                    // The member no longer holds this range — an operator
                    // moved it, or a rebalance on the plane did (review): the
                    // membership is stood again, not the position refused.
                    state.member_stands = false;
                    self.ensure_member(group, group_uuid, fencing_epoch, &target)
                        .await?;
                    state.member_stands = true;
                    re_stood = true;
                }
                MetadataResponse::Rejected(MetadataError::InvalidTransition(reason))
                    if reason == vtop_meta::command::NOT_ASSIGNED =>
                {
                    tracing::warn!(
                        group,
                        range = %target.range_uuid,
                        "kafka offsets: the member does not hold this range on the plane; the client \
                         finds its coordinator again"
                    );
                    self.remember_range(group, target.range_uuid, state);
                    return Err(ErrorCode::CoordinatorNotAvailable);
                }
                MetadataResponse::Rejected(MetadataError::NotFound) if !re_stood => {
                    // The group or the member is gone from the plane since it
                    // was stood — make it stand once more.
                    state.member_stands = false;
                    self.ensure_member(group, group_uuid, fencing_epoch, &target)
                        .await?;
                    state.member_stands = true;
                    re_stood = true;
                }
                MetadataResponse::Rejected(MetadataError::NotFound) => {
                    // Stood a moment ago and still not found: the plane does
                    // not know an unpinned cursor — it predates #468 — and a
                    // committed offset cannot become one there.
                    tracing::warn!(
                        group,
                        range = %target.range_uuid,
                        "kafka offsets: the metadata plane refuses an unpinned cursor; it predates head \
                         cursors, and commits are refused by name until it is upgraded"
                    );
                    self.remember_range(group, target.range_uuid, state);
                    return Err(ErrorCode::UnsupportedForMessageFormat);
                }
                MetadataResponse::Rejected(MetadataError::EpochMismatch { .. }) => {
                    tracing::warn!(
                        group,
                        topic_epoch = target.topic_epoch,
                        "kafka offsets: the plane's cursor is bound to another topic epoch; this topic was \
                         recreated under it"
                    );
                    self.remember_range(group, target.range_uuid, state);
                    return Err(ErrorCode::UnknownTopicOrPartition);
                }
                MetadataResponse::Rejected(MetadataError::InvalidTransition(reason))
                    if reason == vtop_meta::command::NOT_LEASEHOLDER =>
                {
                    // The plane knows better than the broker's view: the
                    // range is held elsewhere, or at another epoch.
                    tracing::warn!(
                        group,
                        range = %self.range.range_uuid,
                        fencing_epoch,
                        "kafka offsets: the plane does not hold this node as the range's leaseholder at \
                         this epoch; the commit is refused and the client finds its coordinator again"
                    );
                    self.remember_range(group, target.range_uuid, state);
                    return Err(ErrorCode::NotCoordinator);
                }
                MetadataResponse::Rejected(MetadataError::InvalidTransition(reason)) => {
                    // A position the plane will not take — behind a cursor
                    // pinned by the recovery protocol, most often.
                    tracing::warn!(
                        group,
                        offset = committed.offset,
                        reason,
                        "kafka offsets: the plane refuses the position"
                    );
                    self.remember_range(group, target.range_uuid, state);
                    return Err(ErrorCode::OffsetOutOfRange);
                }
                MetadataResponse::Rejected(MetadataError::Limit(reason)) => {
                    tracing::warn!(group, reason, "kafka offsets: the plane refuses the commit");
                    self.remember_range(group, target.range_uuid, state);
                    return Err(ErrorCode::InvalidRequest);
                }
                other => {
                    self.remember_range(group, target.range_uuid, state);
                    return Err(self.unexpected("CommitGroupCursor", group, other));
                }
            }
        }
        self.remember_range(group, target.range_uuid, state);
        Err(self.kept_moving("CommitGroupCursor", group))
    }

    async fn fetch(
        &self,
        group: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<Committed>, ErrorCode> {
        let Some(target) = self.cursor_target(topic, partition) else {
            return Ok(None);
        };
        if vtop_meta::validate_group_name(group).is_err() {
            return Ok(None);
        }
        let group_uuid = self.group_uuid(group);
        let Some(cursor) = self.read(group_uuid, &target).await? else {
            return Ok(None);
        };
        // What the plane confirmed is what the next commit's CAS names.
        let mut state = self.range_state(group, target.range_uuid);
        state.checkpoint = Some(cursor.checkpoint_generation);
        state.lineage = cursor.range_generation;
        self.remember_range(group, target.range_uuid, state);
        // A position the wire cannot carry is not one to answer with
        // (review): a Kafka offset is an i64, and a silent clamp would read
        // as a legitimate position at the ceiling.
        let Ok(offset) = i64::try_from(cursor.record_offset) else {
            tracing::error!(
                group,
                record_offset = cursor.record_offset,
                "kafka offsets: the plane's cursor is past what a Kafka offset can carry"
            );
            return Err(ErrorCode::OffsetOutOfRange);
        };
        Ok(Some(Committed {
            offset,
            metadata: None,
        }))
    }

    async fn committed(
        &self,
        group: &str,
        at_most: usize,
    ) -> Result<Vec<(String, i32, Committed)>, ErrorCode> {
        // This node's partition first, then each peer the topology named
        // (#457 slice 4c): a coordinator answers every partition it can
        // store. Stops one over the caller's bound, as MemoryOffsetStore
        // does, so the listener can say the group has committed more.
        let cap = at_most.saturating_add(1);
        let mut rows = Vec::new();
        if let Some(committed) = self
            .fetch(group, &self.range.topic, self.range.partition)
            .await?
        {
            rows.push((self.range.topic.clone(), self.range.partition, committed));
        }
        for peer in &self.peers {
            if rows.len() >= cap {
                break;
            }
            if let Some(committed) = self.fetch(group, &peer.topic, peer.partition).await? {
                rows.push((peer.topic.clone(), peer.partition, committed));
            }
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicU64;
    use vtop_meta::{MetaKey, MetaStateMachine, MetaValue};

    const TOPIC_UUID: Uuid = Uuid::from_u128(0x20);
    const RANGE_UUID: Uuid = Uuid::from_u128(0x21);
    const PEER_TOPIC: Uuid = Uuid::from_u128(0x22);
    const PEER_RANGE: Uuid = Uuid::from_u128(0x23);
    const CLUSTER_ID: Uuid = Uuid::from_u128(0x99);
    const NODE_A: Uuid = Uuid::from_u128(0x10);
    const NODE_B: Uuid = Uuid::from_u128(0x11);

    /// A lease view the test sets: what the broker would say.
    struct FixedLease(Mutex<LeaseState>);
    impl FixedLease {
        fn holding(epoch: u64) -> Arc<Self> {
            Arc::new(Self(Mutex::new(LeaseState::Held(epoch))))
        }
        fn set(&self, state: LeaseState) {
            *self.0.lock().unwrap() = state;
        }
    }
    impl LeaseView for FixedLease {
        fn lease(&self) -> LeaseState {
            *self.0.lock().unwrap()
        }
    }

    /// The real state machine, in memory: every proposal applied at the next
    /// index, every read answered from its records.
    struct MemoryPlane {
        machine: Mutex<MetaStateMachine>,
        index: AtomicU64,
    }

    impl MemoryPlane {
        fn with_range() -> Self {
            let plane = Self {
                machine: Mutex::new(MetaStateMachine::new()),
                index: AtomicU64::new(1),
            };
            for (node, addr) in [(NODE_A, "n1:9200"), (NODE_B, "n2:9200")] {
                plane.apply(MetadataCommand::RegisterNode {
                    env: MetadataOffsetStore::envelope(),
                    node_uuid: node,
                    addr: addr.to_owned(),
                    expected_generation: None,
                });
            }
            plane.apply(MetadataCommand::CreateTopic {
                env: MetadataOffsetStore::envelope(),
                name: "events.v1".to_owned(),
                topic_uuid: TOPIC_UUID,
                root_range_uuid: RANGE_UUID,
            });
            plane
        }

        fn with_peer_topic(self) -> Self {
            self.apply(MetadataCommand::CreateTopic {
                env: MetadataOffsetStore::envelope(),
                name: "events.v1.p1".to_owned(),
                topic_uuid: PEER_TOPIC,
                root_range_uuid: PEER_RANGE,
            });
            self
        }

        /// The range's lease goes to `holder`; the epoch the plane minted.
        fn grant(&self, holder: Uuid) -> u64 {
            let generation = match self.machine.lock().unwrap().record(&MetaKey::Range {
                topic_uuid: TOPIC_UUID,
                range_uuid: RANGE_UUID,
            }) {
                Some(MetaValue::Range(range)) => range.generation,
                other => panic!("a range record, not {other:?}"),
            };
            match self.apply(MetadataCommand::AcquireRangeLease {
                env: MetadataOffsetStore::envelope(),
                topic_uuid: TOPIC_UUID,
                range_uuid: RANGE_UUID,
                holder_node_uuid: holder,
                expected_range_generation: generation,
                lease_duration_ms: 60_000,
            }) {
                MetadataResponse::LeaseGranted { fencing_epoch } => fencing_epoch,
                other => panic!("a grant, not {other:?}"),
            }
        }

        fn release(&self, fencing_epoch: u64) {
            assert!(matches!(
                self.apply(MetadataCommand::ReleaseRangeLease {
                    env: MetadataOffsetStore::envelope(),
                    topic_uuid: TOPIC_UUID,
                    range_uuid: RANGE_UUID,
                    expected_fencing_epoch: fencing_epoch,
                }),
                MetadataResponse::Ack { .. }
            ));
        }

        fn apply(&self, command: MetadataCommand) -> MetadataResponse {
            let index = self.index.fetch_add(1, Ordering::SeqCst);
            self.machine.lock().unwrap().apply(index, &command)
        }

        fn cursor(&self, group_uuid: Uuid) -> Option<vtop_meta::CursorCheckpointRecord> {
            match self.machine.lock().unwrap().record(&MetaKey::GroupCursor {
                group_uuid,
                topic_uuid: TOPIC_UUID,
                range_uuid: RANGE_UUID,
            }) {
                Some(MetaValue::GroupCursor(record)) => Some(record.clone()),
                _ => None,
            }
        }
    }

    #[async_trait::async_trait]
    impl CursorPlane for MemoryPlane {
        async fn propose(&self, command: MetadataCommand) -> Result<MetadataResponse, PlaneError> {
            Ok(self.apply(command))
        }
        async fn read_group_cursor(
            &self,
            group_uuid: Uuid,
            topic_uuid: Uuid,
            range_uuid: Uuid,
        ) -> Result<AdminReadGroupCursorResponse, PlaneError> {
            let machine = self.machine.lock().unwrap();
            let group_found = matches!(
                machine.record(&MetaKey::Group { group_uuid }),
                Some(MetaValue::Group(_))
            );
            let cursor = match machine.record(&MetaKey::GroupCursor {
                group_uuid,
                topic_uuid,
                range_uuid,
            }) {
                Some(MetaValue::GroupCursor(record)) => Some(AdminGroupCursorView::from(record)),
                _ => None,
            };
            Ok(AdminReadGroupCursorResponse {
                group_found,
                cursor,
                read_at_applied_index: self.index.load(Ordering::SeqCst),
            })
        }
    }

    fn identity() -> RangeIdentity {
        identity_of(NODE_A)
    }

    fn identity_of(holder_node_uuid: Uuid) -> RangeIdentity {
        RangeIdentity {
            cluster_id: CLUSTER_ID,
            topic: "events".to_owned(),
            topic_uuid: TOPIC_UUID,
            range_uuid: RANGE_UUID,
            topic_epoch: 1,
            holder_node_uuid,
            partition: 0,
        }
    }

    fn at(offset: i64) -> Committed {
        Committed {
            offset,
            metadata: None,
        }
    }

    /// Commits become unpinned cursors on the plane and fetches read them:
    /// forward, equal, and back (a rewind is the group's decision); the row
    /// listing is the one row; another topic or an unknown group is nothing.
    #[tokio::test]
    async fn commits_become_unpinned_cursors_and_fetches_read_them() {
        let plane = Arc::new(MemoryPlane::with_range());
        let epoch = plane.grant(NODE_A);
        let store = MetadataOffsetStore::new(
            Arc::clone(&plane) as Arc<dyn CursorPlane>,
            FixedLease::holding(epoch),
            identity(),
        );
        assert_eq!(
            store.fetch("g", "events", 0).await,
            Ok(None),
            "nothing committed"
        );
        store.commit("g", "events", 0, at(10)).await.unwrap();
        assert_eq!(store.fetch("g", "events", 0).await, Ok(Some(at(10))));
        store.commit("g", "events", 0, at(20)).await.unwrap();
        store.commit("g", "events", 0, at(20)).await.unwrap();
        store.commit("g", "events", 0, at(5)).await.unwrap();
        assert_eq!(
            store.fetch("g", "events", 0).await,
            Ok(Some(at(5))),
            "a rewind is a position"
        );
        let record = plane
            .cursor(derived_group_uuid(CLUSTER_ID, "g"))
            .expect("a cursor on the plane");
        assert_eq!(
            (
                record.segment_uuid,
                record.record_offset,
                record.topic_epoch,
                record.checkpoint_generation
            ),
            (Uuid::nil(), 5, 1, 3),
            "unpinned, at the offset, in the epoch, four commits in"
        );
        assert_eq!(
            store.committed("g", 10).await.unwrap(),
            vec![("events".to_owned(), 0, at(5))]
        );
        assert_eq!(
            store.fetch("g", "audit", 0).await,
            Ok(None),
            "another topic is not this range"
        );
        assert_eq!(store.fetch("g", "events", 1).await, Ok(None));
        assert_eq!(store.fetch("nobody", "events", 0).await, Ok(None));
        assert!(store.committed("nobody", 10).await.unwrap().is_empty());
    }

    /// A store rebuilt over the same plane — a node restarted, or the range
    /// handed to another node — finds the group, the member and the cursor
    /// standing: every ladder step is idempotent, and the CAS is learned from
    /// the plane's own answer.
    #[tokio::test]
    async fn a_store_rebuilt_over_the_plane_resumes_the_groups_position() {
        let plane = Arc::new(MemoryPlane::with_range());
        let epoch = plane.grant(NODE_A);
        let first = MetadataOffsetStore::new(
            Arc::clone(&plane) as Arc<dyn CursorPlane>,
            FixedLease::holding(epoch),
            identity(),
        );
        first.commit("g", "events", 0, at(40)).await.unwrap();
        let second = MetadataOffsetStore::new(
            Arc::clone(&plane) as Arc<dyn CursorPlane>,
            FixedLease::holding(epoch),
            identity(),
        );
        assert_eq!(
            second.fetch("g", "events", 0).await,
            Ok(Some(at(40))),
            "resumed from the plane"
        );
        second.commit("g", "events", 0, at(41)).await.unwrap();
        assert_eq!(first.fetch("g", "events", 0).await, Ok(Some(at(41))));
        // The first store's cached checkpoint is stale now; its next commit
        // learns the plane's and lands.
        first.commit("g", "events", 0, at(42)).await.unwrap();
        assert_eq!(second.fetch("g", "events", 0).await, Ok(Some(at(42))));
        let record = plane.cursor(derived_group_uuid(CLUSTER_ID, "g")).unwrap();
        assert_eq!(record.checkpoint_generation, 2);
    }

    /// A node whose lease moved on cannot move the group's position (review):
    /// once another node holds the range, the old holder's commit is refused
    /// by the plane as not the leaseholder's — `NOT_COORDINATOR` — whatever
    /// CAS token it learned; and a broker view that says the lease is gone
    /// refuses before the plane is asked at all.
    #[tokio::test]
    async fn a_node_whose_lease_moved_on_cannot_move_the_position() {
        let plane = Arc::new(MemoryPlane::with_range());
        let epoch_a = plane.grant(NODE_A);
        let view_a = FixedLease::holding(epoch_a);
        let store_a = MetadataOffsetStore::new(
            Arc::clone(&plane) as Arc<dyn CursorPlane>,
            Arc::clone(&view_a) as Arc<dyn LeaseView>,
            identity_of(NODE_A),
        );
        store_a.commit("g", "events", 0, at(10)).await.unwrap();
        // The lease moves to node b.
        plane.release(epoch_a);
        let epoch_b = plane.grant(NODE_B);
        let store_b = MetadataOffsetStore::new(
            Arc::clone(&plane) as Arc<dyn CursorPlane>,
            FixedLease::holding(epoch_b),
            identity_of(NODE_B),
        );
        store_b.commit("g", "events", 0, at(20)).await.unwrap();
        // Node a's broker still believes in its lease: the plane knows better.
        assert_eq!(
            store_a.commit("g", "events", 0, at(5)).await,
            Err(ErrorCode::NotCoordinator)
        );
        assert_eq!(
            store_b.fetch("g", "events", 0).await,
            Ok(Some(at(20))),
            "unmoved"
        );
        // Node a's broker learns the lease is gone: refused without a plane call.
        view_a.set(LeaseState::Gone);
        let before = plane.index.load(Ordering::SeqCst);
        assert_eq!(
            store_a.commit("g", "events", 0, at(5)).await,
            Err(ErrorCode::NotCoordinator)
        );
        assert_eq!(
            plane.index.load(Ordering::SeqCst),
            before,
            "the plane was not asked"
        );
        // A view that cannot answer at once is not a lost lease (review): the
        // commit is retryable, and the plane is still not asked.
        view_a.set(LeaseState::Unknown);
        assert_eq!(
            store_a.commit("g", "events", 0, at(5)).await,
            Err(ErrorCode::CoordinatorNotAvailable)
        );
        assert_eq!(
            plane.index.load(Ordering::SeqCst),
            before,
            "the plane was not asked"
        );
        // Reads are the plane's truth for anyone.
        assert_eq!(store_a.fetch("g", "events", 0).await, Ok(Some(at(20))));
    }

    /// The commit path's lines hold at the store, before the plane is asked:
    /// another topic, another partition, a negative offset, metadata over the
    /// cap, a group name the plane would not take.
    #[tokio::test]
    async fn the_store_holds_the_commit_paths_lines_before_the_plane() {
        let plane = Arc::new(ScriptedPlane::default());
        let store = MetadataOffsetStore::new(
            Arc::clone(&plane) as Arc<dyn CursorPlane>,
            FixedLease::holding(7),
            identity(),
        );
        assert_eq!(
            store.commit("g", "audit", 0, at(1)).await,
            Err(ErrorCode::UnknownTopicOrPartition)
        );
        assert_eq!(
            store.commit("g", "events", 3, at(1)).await,
            Err(ErrorCode::UnknownTopicOrPartition)
        );
        assert_eq!(
            store.commit("g", "events", 0, at(-1)).await,
            Err(ErrorCode::OffsetOutOfRange)
        );
        assert_eq!(
            store
                .commit(
                    "g",
                    "events",
                    0,
                    Committed {
                        offset: 1,
                        metadata: Some(
                            "x".repeat(crate::api_groups::MAX_OFFSET_METADATA_BYTES + 1)
                        ),
                    }
                )
                .await,
            Err(ErrorCode::OffsetMetadataTooLarge)
        );
        assert_eq!(
            store
                .commit(
                    &"n".repeat(vtop_meta::MAX_GROUP_NAME_BYTES + 1),
                    "events",
                    0,
                    at(1)
                )
                .await,
            Err(ErrorCode::InvalidGroupId)
        );
        assert!(
            plane.sent.lock().unwrap().is_empty(),
            "the plane was never asked"
        );
    }

    /// A plane whose answers are scripted, and which records what it was
    /// asked: the retry ladders are exercised step by step.
    #[derive(Default)]
    struct ScriptedPlane {
        answers: Mutex<VecDeque<Result<MetadataResponse, PlaneError>>>,
        sent: Mutex<Vec<MetadataCommand>>,
        read: Mutex<Option<Result<AdminReadGroupCursorResponse, PlaneError>>>,
    }

    impl ScriptedPlane {
        fn answer(&self, answer: Result<MetadataResponse, PlaneError>) {
            self.answers.lock().unwrap().push_back(answer);
        }
        fn rejected(&self, error: MetadataError) {
            self.answer(Ok(MetadataResponse::Rejected(error)));
        }
    }

    #[async_trait::async_trait]
    impl CursorPlane for ScriptedPlane {
        async fn propose(&self, command: MetadataCommand) -> Result<MetadataResponse, PlaneError> {
            self.sent.lock().unwrap().push(command);
            self.answers
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err(PlaneError("unscripted".to_owned())))
        }
        async fn read_group_cursor(
            &self,
            _: Uuid,
            _: Uuid,
            _: Uuid,
        ) -> Result<AdminReadGroupCursorResponse, PlaneError> {
            self.read
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| Err(PlaneError("unscripted read".to_owned())))
        }
    }

    fn ack() -> MetadataResponse {
        MetadataResponse::Ack { generation: 1 }
    }
    fn cursor_committed(g: u64) -> MetadataResponse {
        MetadataResponse::CursorCommitted {
            checkpoint_generation: g,
        }
    }

    /// Every CAS the plane refuses carries its actual, and the store adopts
    /// it: the group generation on the join, the member generation on the
    /// assignment, the lineage and the checkpoint on the commit.
    #[tokio::test]
    async fn retries_follow_the_planes_actuals() {
        let plane = Arc::new(ScriptedPlane::default());
        plane.answer(Ok(ack()));
        plane.rejected(MetadataError::LineageMismatch {
            expected: 0,
            actual: 4,
        });
        plane.rejected(MetadataError::GenerationMismatch {
            expected: 0,
            actual: 7,
        });
        plane.answer(Ok(cursor_committed(8)));
        let store = MetadataOffsetStore::new(
            Arc::clone(&plane) as Arc<dyn CursorPlane>,
            FixedLease::holding(7),
            identity(),
        );
        store.commit("g", "events", 0, at(10)).await.unwrap();
        {
            let sent = plane.sent.lock().unwrap();
            assert_eq!(
                sent.len(),
                4,
                "one membership step, then the commit and its retries"
            );
            assert!(matches!(
                &sent[0],
                MetadataCommand::EnsureGroupMemberForRange {
                    holder_node_uuid: NODE_A,
                    fencing_epoch: 7,
                    ..
                }
            ));
            assert!(matches!(
                &sent[1],
                MetadataCommand::CommitGroupCursorFenced {
                    range_generation: 0,
                    expected_checkpoint_generation: None,
                    segment_uuid,
                    record_offset: 10,
                holder_node_uuid: NODE_A,
                fencing_epoch: 7,
                    ..
                } if segment_uuid.is_nil()
            ));
            assert!(matches!(
                &sent[3],
                MetadataCommand::CommitGroupCursorFenced {
                    range_generation: 4,
                    expected_checkpoint_generation: Some(7),
                    ..
                }
            ));
        }
        // The next commit needs no ladder and names what the plane confirmed.
        plane.answer(Ok(cursor_committed(9)));
        store.commit("g", "events", 0, at(11)).await.unwrap();
        {
            let sent = plane.sent.lock().unwrap();
            assert_eq!(sent.len(), 5);
            assert!(matches!(
                &sent[4],
                MetadataCommand::CommitGroupCursorFenced {
                    range_generation: 4,
                    expected_checkpoint_generation: Some(8),
                    ..
                }
            ));
        }
    }

    /// The plane's faults are the gateway's codes: unreachable is
    /// `COORDINATOR_NOT_AVAILABLE` (the client retries); a plane that does
    /// not know an unpinned cursor after the member stands is
    /// `UNSUPPORTED_FOR_MESSAGE_FORMAT`; another topic epoch is
    /// `UNKNOWN_TOPIC_OR_PARTITION`; a position the plane refuses is
    /// `OFFSET_OUT_OF_RANGE`; a group name it refuses is `INVALID_GROUP_ID`.
    #[tokio::test]
    async fn plane_faults_are_the_gateways_codes() {
        let plane = Arc::new(ScriptedPlane::default());
        let store = MetadataOffsetStore::new(
            Arc::clone(&plane) as Arc<dyn CursorPlane>,
            FixedLease::holding(7),
            identity(),
        );
        // Unreachable at the first step.
        plane.answer(Err(PlaneError("connection refused".to_owned())));
        assert_eq!(
            store.commit("g", "events", 0, at(1)).await,
            Err(ErrorCode::CoordinatorNotAvailable)
        );
        // The membership stands, the commit is NotFound, it stands again, and
        // the commit is NotFound again: the plane predates head cursors.
        for _ in 0..2 {
            plane.answer(Ok(ack()));
            plane.rejected(MetadataError::NotFound);
        }
        assert_eq!(
            store.commit("g", "events", 0, at(1)).await,
            Err(ErrorCode::UnsupportedForMessageFormat)
        );
        // Standing now; the plane's cursor is another epoch's.
        plane.rejected(MetadataError::EpochMismatch {
            expected: 1,
            actual: 2,
        });
        assert_eq!(
            store.commit("g", "events", 0, at(1)).await,
            Err(ErrorCode::UnknownTopicOrPartition)
        );
        plane.rejected(MetadataError::invalid_transition(
            "cursor moved backward across a pin change",
        ));
        assert_eq!(
            store.commit("g", "events", 0, at(1)).await,
            Err(ErrorCode::OffsetOutOfRange)
        );
        // A name the plane refuses at the membership step.
        plane.rejected(MetadataError::limit("group name"));
        assert_eq!(
            store.commit("h", "events", 0, at(1)).await,
            Err(ErrorCode::InvalidGroupId)
        );
        // A read the plane does not answer.
        *plane.read.lock().unwrap() = Some(Err(PlaneError("timeout".to_owned())));
        assert_eq!(
            store.fetch("g", "events", 0).await,
            Err(ErrorCode::CoordinatorNotAvailable)
        );
    }

    /// A cursor standing under another topic epoch is not a position here:
    /// the fetch answers nothing, and the next commit starts this epoch's.
    #[tokio::test]
    async fn a_cursor_of_another_epoch_is_not_a_position_here() {
        let plane = Arc::new(ScriptedPlane::default());
        let stale = AdminGroupCursorView {
            topic_epoch: 7,
            range_generation: 0,
            segment_uuid: Uuid::nil(),
            segment_generation: 0,
            segment_root: [0; 32],
            record_offset: 99,
            record_index: 0,
            lineage_transition_id: None,
            checkpoint_generation: 4,
            committed_by_member: Uuid::nil(),
        };
        *plane.read.lock().unwrap() = Some(Ok(AdminReadGroupCursorResponse {
            group_found: true,
            cursor: Some(stale),
            read_at_applied_index: 12,
        }));
        let store = MetadataOffsetStore::new(
            Arc::clone(&plane) as Arc<dyn CursorPlane>,
            FixedLease::holding(7),
            identity(),
        );
        assert_eq!(store.fetch("g", "events", 0).await, Ok(None));
    }

    fn peer() -> PartitionRange {
        PartitionRange {
            topic: "events".to_owned(),
            partition: 1,
            topic_uuid: PEER_TOPIC,
            range_uuid: PEER_RANGE,
            topic_epoch: 1,
        }
    }

    /// A coordinator commits a cursor on a partition it does not lead
    /// (#457 slice 4c), fenced by the lease it does hold; losing that lease
    /// refuses the next commit on the foreign partition too.
    #[tokio::test]
    async fn a_coordinator_commits_on_a_partition_it_does_not_lead() {
        let plane = Arc::new(MemoryPlane::with_range().with_peer_topic());
        let epoch = plane.grant(NODE_A);
        let lease = FixedLease::holding(epoch);
        let store = MetadataOffsetStore::new(
            Arc::clone(&plane) as Arc<dyn CursorPlane>,
            Arc::clone(&lease) as Arc<dyn LeaseView>,
            identity(),
        )
        .with_peers(vec![peer()]);
        assert_eq!(
            store.commit("g", "events", 1, at(9)).await,
            Ok(()),
            "the coordinator stores a cursor on a range it does not lead"
        );
        assert_eq!(store.fetch("g", "events", 1).await, Ok(Some(at(9))));
        assert_eq!(
            store.fetch("g", "events", 0).await,
            Ok(None),
            "the coordinator's own partition was not written"
        );
        let rows = store.committed("g", 8).await.unwrap();
        assert_eq!(rows, vec![("events".to_owned(), 1, at(9))]);
        plane.release(epoch);
        lease.set(LeaseState::Gone);
        assert_eq!(
            store.commit("g", "events", 1, at(10)).await,
            Err(ErrorCode::NotCoordinator),
            "losing the coordinator's own lease refuses every partition"
        );
    }

    /// A partition the topology did not identify is unknown, not guessed.
    #[tokio::test]
    async fn a_peer_without_identity_is_unknown() {
        let plane = Arc::new(MemoryPlane::with_range());
        let epoch = plane.grant(NODE_A);
        let store = MetadataOffsetStore::new(
            Arc::clone(&plane) as Arc<dyn CursorPlane>,
            FixedLease::holding(epoch),
            identity(),
        );
        assert_eq!(
            store.commit("g", "events", 1, at(1)).await,
            Err(ErrorCode::UnknownTopicOrPartition)
        );
    }
}

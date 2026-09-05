//! Consumer groups on one gateway (#457, slice 2): the classic group
//! protocol's coordinator, in memory.
//!
//! This gateway is the coordinator of every group it serves — one range,
//! one partition, one leader — so a group's whole life is here: members
//! join, the coordinator holds each join until every member of the round is
//! in (or the round's deadline passes), the round completes with a new
//! generation, a leader and a protocol, the leader's SyncGroup hands out the
//! assignment the CLIENT-SIDE assignor computed, heartbeats keep a member
//! alive, and a leave or a missed session ends it. Membership is ephemeral
//! by design: a coordinator that restarts has empty groups and every member
//! rejoins, as it would with a Kafka broker that moved the group. What must
//! outlive the coordinator — committed offsets — is the offset store's
//! business, not this module's.
//!
//! Every code a client acts on is the protocol's own: `UNKNOWN_MEMBER_ID`
//! when a member is not one, `ILLEGAL_GENERATION` when its generation is
//! stale, `REBALANCE_IN_PROGRESS` when it must rejoin, `MEMBER_ID_REQUIRED`
//! (KIP-394) for a first join that must come back with the id minted here,
//! `INCONSISTENT_GROUP_PROTOCOL` when members cannot agree, and the bounds
//! by name. Nothing here is a guess a client would act on wrongly.

use crate::messages::ErrorCode;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Where a SyncGroup stands (review): only a completing round's leader has
/// assignments worth reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncStanding {
    /// The leader of a completing round: its assignments are judged.
    Leader,
    /// A follower, or a stable member syncing again: its assignments are
    /// not read.
    Other,
}

/// How the coordinator bounds itself and paces a rebalance.
#[derive(Debug, Clone)]
pub struct GroupConfig {
    /// Groups this gateway will hold at once; one over is refused as
    /// `COORDINATOR_NOT_AVAILABLE`, retriable, with a warning.
    pub max_groups: usize,
    /// Members one group may hold; one over is `GROUP_MAX_SIZE_REACHED`.
    pub max_members: usize,
    /// The session timeouts a member may ask for, inclusive; outside them
    /// is `INVALID_SESSION_TIMEOUT`. The minimum must clear every wait one
    /// connection can serialize a heartbeat behind — the gateway's fetch
    /// ceiling, 5 s by default (review): Kafka's own default minimum, 6 s
    /// (`group.min.session.timeout.ms`), does.
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
    /// The longest a round, or a sync parked on it, waits — whatever a member
    /// asked for (review): a member's rebalance timeout is honored up to this
    /// and no further, or an `i32::MAX` from an unauthenticated peer would
    /// park followers, sockets and session permits for 24 days.
    pub max_rebalance_timeout: Duration,
    /// How long a NEW group's first rebalance waits for further joiners
    /// before completing with whoever is in (Kafka's
    /// `group.initial.rebalance.delay.ms`): a consumer fleet starting
    /// together becomes one generation, not one per member.
    pub initial_rebalance_delay: Duration,
}

impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            max_groups: 256,
            max_members: 64,
            min_session_timeout: Duration::from_secs(6),
            max_session_timeout: Duration::from_secs(300),
            // Kafka's default `max.poll.interval.ms`, the client-side figure
            // that becomes the rebalance timeout.
            max_rebalance_timeout: Duration::from_secs(300),
            initial_rebalance_delay: Duration::from_millis(500),
        }
    }
}

/// What a member's join came to once its round completed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinOutcome {
    pub generation: i32,
    pub protocol_name: String,
    pub leader: String,
    pub member_id: String,
    /// Every member's id and the metadata it sent for the chosen protocol —
    /// for the leader, whose assignor needs them; empty for everyone else.
    pub members: Vec<(String, Vec<u8>)>,
}

/// A join's answer: the round completed, or (KIP-394) the member must come
/// back with the id minted for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Joined {
    Complete(JoinOutcome),
    MemberIdRequired(String),
}

/// What a member asks for when it joins.
#[derive(Debug, Clone)]
pub struct JoinRequest {
    pub group_id: String,
    /// Empty on a first join.
    pub member_id: String,
    pub client_id: String,
    pub protocol_type: String,
    /// The assignors the member supports, in its order of preference, with
    /// the subscription metadata each would use.
    pub protocols: Vec<(String, Vec<u8>)>,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    /// JoinGroup v4 and above: a first join without an id is answered with
    /// one to come back with, rather than admitted at once.
    pub require_member_id: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

struct Member {
    session_timeout: Duration,
    rebalance_timeout: Duration,
    last_seen: Instant,
    /// Monotonic across the group's life: the leader is the earliest joiner
    /// still present.
    joined_at: u64,
    protocols: Vec<(String, Vec<u8>)>,
    assignment: Option<Vec<u8>>,
    /// Joined the round under way; a member that has not by the deadline is
    /// dropped when the round completes.
    in_round: bool,
    join_waiter: Option<oneshot::Sender<Result<JoinOutcome, ErrorCode>>>,
    sync_waiter: Option<oneshot::Sender<Result<Vec<u8>, ErrorCode>>>,
}

struct Round {
    /// Not before this: a new group's first round waits for more joiners.
    not_before: Instant,
    /// Not after this: whoever has not rejoined by then is out.
    deadline: Instant,
}

struct Group {
    generation: i32,
    state: State,
    protocol_type: Option<String>,
    protocol_name: Option<String>,
    leader: Option<String>,
    members: HashMap<String, Member>,
    /// Ids minted for first joins that must come back (KIP-394), and when
    /// they were minted; one that never comes back expires.
    minted: HashMap<String, Instant>,
    round: Option<Round>,
    /// While the leader's SyncGroup is awaited: the members' longest
    /// rebalance timeout from the round's completion (review). A leader that
    /// has not synced by then is out, and the group rebalances without it.
    sync_deadline: Option<Instant>,
    next_join_seq: u64,
}

impl Group {
    fn new() -> Self {
        Self {
            generation: 0,
            state: State::Empty,
            protocol_type: None,
            protocol_name: None,
            leader: None,
            members: HashMap::new(),
            minted: HashMap::new(),
            round: None,
            sync_deadline: None,
            next_join_seq: 0,
        }
    }
}

/// The coordinator: every group this gateway serves.
pub struct Coordinator {
    config: GroupConfig,
    groups: Mutex<HashMap<String, Group>>,
    /// Set once by [`Coordinator::shutdown`]: every parked wait was released
    /// and no join is admitted again.
    closed: std::sync::atomic::AtomicBool,
}

/// How long a minted id waits for its member to come back.
const MINTED_ID_TTL: Duration = Duration::from_secs(30);

/// How much of the client id a minted member id carries (review): enough to
/// read on a log, and never a member id past the wire's string limit however
/// long the client id was.
const MEMBER_ID_PREFIX_CHARS: usize = 64;

impl Coordinator {
    pub fn new(config: GroupConfig) -> Self {
        Self {
            config,
            groups: Mutex::new(HashMap::new()),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The listener is stopping (review): every parked join and sync is
    /// released now — its holder hears `REBALANCE_IN_PROGRESS` and will find
    /// its coordinator elsewhere — and no join is admitted again, so the
    /// drain that follows is bounded by the produce and fetch ceilings and
    /// never by a rebalance timeout.
    pub fn shutdown(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        let mut groups = self.lock();
        for group in groups.values_mut() {
            for member in group.members.values_mut() {
                member.join_waiter = None;
                if let Some(parked) = member.sync_waiter.take() {
                    let _ = parked.send(Err(ErrorCode::RebalanceInProgress));
                }
            }
        }
        groups.clear();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Group>> {
        self.groups
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// A member joins. Held until the round completes — every member of the
    /// round is in, or the round's deadline passes — and answered with the
    /// generation, the leader, the protocol, and (for the leader) the
    /// members; or, for a first join at a version that requires it, with the
    /// id to come back with.
    pub async fn join(&self, request: JoinRequest) -> Result<Joined, ErrorCode> {
        if request.group_id.is_empty() {
            return Err(ErrorCode::InvalidGroupId);
        }
        if request.session_timeout < self.config.min_session_timeout
            || request.session_timeout > self.config.max_session_timeout
        {
            return Err(ErrorCode::InvalidSessionTimeout);
        }
        if request.protocols.is_empty() {
            return Err(ErrorCode::InconsistentGroupProtocol);
        }
        let now = Instant::now();
        let (receiver, deadline) = {
            let mut groups = self.lock();
            // Judged under the lock `shutdown` takes (review): a join that
            // read "open" and was descheduled cannot park a group after the
            // coordinator closed.
            if self.is_closed() {
                return Err(ErrorCode::CoordinatorNotAvailable);
            }
            let group = match groups.get_mut(&request.group_id) {
                Some(group) => group,
                None => {
                    if groups.len() >= self.config.max_groups {
                        tracing::warn!(
                            group = %request.group_id,
                            open = self.config.max_groups,
                            "kafka JoinGroup refused: every group slot is held"
                        );
                        return Err(ErrorCode::CoordinatorNotAvailable);
                    }
                    groups
                        .entry(request.group_id.clone())
                        .or_insert_with(Group::new)
                }
            };
            group
                .minted
                .retain(|_, minted_at| now.duration_since(*minted_at) < MINTED_ID_TTL);
            let member_id = if request.member_id.is_empty() {
                if group.members.len() + group.minted.len() >= self.config.max_members {
                    return Err(ErrorCode::GroupMaxSizeReached);
                }
                let prefix: String = request
                    .client_id
                    .chars()
                    .take(MEMBER_ID_PREFIX_CHARS)
                    .collect();
                let minted = format!("{prefix}-{}", uuid::Uuid::new_v4());
                if request.require_member_id {
                    group.minted.insert(minted.clone(), now);
                    return Ok(Joined::MemberIdRequired(minted));
                }
                minted
            } else if group.members.contains_key(&request.member_id) {
                request.member_id.clone()
            } else if group.minted.remove(&request.member_id).is_some() {
                if group.members.len() >= self.config.max_members {
                    return Err(ErrorCode::GroupMaxSizeReached);
                }
                request.member_id.clone()
            } else {
                return Err(ErrorCode::UnknownMemberId);
            };
            // One protocol type per group, and a protocol every member has.
            if let Some(protocol_type) = &group.protocol_type {
                if *protocol_type != request.protocol_type {
                    return Err(ErrorCode::InconsistentGroupProtocol);
                }
            }
            let offered: Vec<&str> = request.protocols.iter().map(|(n, _)| n.as_str()).collect();
            // One protocol EVERY member supports (review): the intersection
            // over the whole group, not this member against each other one —
            // {a,b}, {b,c} and {a,c} agree pairwise and on nothing.
            let common: Vec<&str> = group
                .members
                .iter()
                .filter(|(id, _)| **id != member_id)
                .fold(offered.clone(), |common, (_, member)| {
                    common
                        .into_iter()
                        .filter(|name| member.protocols.iter().any(|(n, _)| n == name))
                        .collect()
                });
            let compatible = !common.is_empty();
            if !compatible {
                return Err(ErrorCode::InconsistentGroupProtocol);
            }
            // An unchanged rejoin replays the current join result (audit, as
            // Kafka's coordinator has it): while the round is completing, for
            // every member — a retry after a lost response is not a rebalance,
            // and the leader's carries the members' metadata again; once the
            // group is stable, for a follower, while the leader's rejoin forces
            // a rebalance (it may have seen a change in what it assigns by).
            // Changed protocols are a rebalance in either state.
            let unchanged = group
                .members
                .get(&member_id)
                .is_some_and(|m| m.protocols == request.protocols);
            let is_leader = group.leader.as_deref() == Some(member_id.as_str());
            let replay = unchanged
                && match group.state {
                    State::CompletingRebalance => true,
                    State::Stable => !is_leader,
                    State::Empty | State::PreparingRebalance => false,
                };
            if replay {
                let protocol = group.protocol_name.clone().unwrap_or_default();
                let members = if is_leader {
                    group
                        .members
                        .iter()
                        .map(|(id, m)| {
                            let metadata = m
                                .protocols
                                .iter()
                                .find(|(n, _)| *n == protocol)
                                .map(|(_, meta)| meta.clone())
                                .unwrap_or_default();
                            (id.clone(), metadata)
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                let member = group.members.get_mut(&member_id).expect("checked above");
                member.last_seen = now;
                return Ok(Joined::Complete(JoinOutcome {
                    generation: group.generation,
                    protocol_name: protocol,
                    leader: group.leader.clone().unwrap_or_default(),
                    member_id,
                    members,
                }));
            }
            group.protocol_type = Some(request.protocol_type.clone());
            let (tx, rx) = oneshot::channel();
            let was_empty = group.members.is_empty();
            let seq = group.next_join_seq;
            group.next_join_seq += 1;
            // Honored up to the cap (review), before it becomes any deadline.
            let rebalance_timeout = request
                .rebalance_timeout
                .min(self.config.max_rebalance_timeout);
            let is_new = !group.members.contains_key(&member_id);
            let member = group.members.entry(member_id).or_insert_with(|| Member {
                session_timeout: request.session_timeout,
                rebalance_timeout,
                last_seen: now,
                joined_at: seq,
                protocols: Vec::new(),
                assignment: None,
                in_round: false,
                join_waiter: None,
                sync_waiter: None,
            });
            member.session_timeout = request.session_timeout;
            member.rebalance_timeout = rebalance_timeout;
            member.last_seen = now;
            member.protocols = request.protocols;
            member.in_round = true;
            // A join over a join: the earlier waiter is dropped, and its
            // holder learns the group moved on.
            member.join_waiter = Some(tx);
            if let Some(parked) = member.sync_waiter.take() {
                let _ = parked.send(Err(ErrorCode::RebalanceInProgress));
            }
            begin_round(group, now, was_empty, self.config.initial_rebalance_delay);
            // A new group's window extends for each late joiner (audit), as
            // Kafka's initial delayed join does: a fleet starting over a few
            // seconds becomes one generation, not one per straggler. The
            // window never passes the round's deadline, which parked joins
            // already wait on.
            if is_new {
                if let Some(round) = group.round.as_mut() {
                    if round.not_before > now {
                        round.not_before =
                            (now + self.config.initial_rebalance_delay).min(round.deadline);
                    }
                }
            }
            let deadline = group.round.as_ref().map(|r| r.deadline).unwrap_or(now);
            maybe_complete(group, now);
            (rx, deadline)
        };
        // Held until the round completes; the sweeper completes a round
        // whose window closed. The wait outlives the deadline by a little,
        // so a completion at the deadline is heard.
        match tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline + Duration::from_secs(5)),
            receiver,
        )
        .await
        {
            Ok(Ok(Ok(outcome))) => Ok(Joined::Complete(outcome)),
            Ok(Ok(Err(error))) => Err(error),
            Ok(Err(_)) | Err(_) => Err(ErrorCode::RebalanceInProgress),
        }
    }

    /// The group's protocol type, for the listener to know how to read an
    /// assignment.
    pub fn protocol_type(&self, group_id: &str) -> Option<String> {
        self.lock()
            .get(group_id)
            .and_then(|group| group.protocol_type.clone())
    }

    /// The group's members right now — what a leader's assignments are
    /// judged for (review): an assignment for an id that is not a member is
    /// ignored when applied, so it is not judged either.
    pub fn member_ids(&self, group_id: &str) -> Vec<String> {
        self.lock()
            .get(group_id)
            .map(|group| group.members.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// The leader's SyncGroup carries every member's assignment; a
    /// follower's waits for it. Both are answered with the caller's own.
    pub async fn sync(
        &self,
        group_id: &str,
        generation: i32,
        member_id: &str,
        assignments: Vec<(String, Vec<u8>)>,
    ) -> Result<Vec<u8>, ErrorCode> {
        if group_id.is_empty() {
            return Err(ErrorCode::InvalidGroupId);
        }
        let (receiver, deadline) = {
            let mut groups = self.lock();
            let group = groups.get_mut(group_id).ok_or(ErrorCode::UnknownMemberId)?;
            if !group.members.contains_key(member_id) {
                return Err(ErrorCode::UnknownMemberId);
            }
            if group.generation != generation {
                return Err(ErrorCode::IllegalGeneration);
            }
            match group.state {
                State::Empty | State::PreparingRebalance => {
                    return Err(ErrorCode::RebalanceInProgress)
                }
                State::Stable => {
                    let member = group.members.get_mut(member_id).expect("checked above");
                    member.last_seen = Instant::now();
                    return Ok(member.assignment.clone().unwrap_or_default());
                }
                State::CompletingRebalance => {}
            }
            let is_leader = group.leader.as_deref() == Some(member_id);
            if is_leader {
                let mut by_member: HashMap<String, Vec<u8>> = assignments.into_iter().collect();
                // Applied as Kafka's coordinator applies a leader's map
                // (review): a member the leader omits gets an empty
                // assignment, an id that is not a member is ignored — and
                // both are said, since a partition nobody holds is worth a
                // line.
                let missing: Vec<String> = group
                    .members
                    .keys()
                    .filter(|id| !by_member.contains_key(*id))
                    .cloned()
                    .collect();
                let unknown: Vec<String> = by_member
                    .keys()
                    .filter(|id| !group.members.contains_key(*id))
                    .cloned()
                    .collect();
                if !missing.is_empty() || !unknown.is_empty() {
                    tracing::warn!(
                        group = %group_id,
                        ?missing,
                        ?unknown,
                        "kafka SyncGroup: the leader's assignment omits members or names \
                         non-members; the omitted get empty assignments, as Kafka gives them"
                    );
                }
                for (id, member) in group.members.iter_mut() {
                    let assignment = by_member.remove(id).unwrap_or_default();
                    member.assignment = Some(assignment.clone());
                    member.last_seen = Instant::now();
                    if let Some(parked) = member.sync_waiter.take() {
                        let _ = parked.send(Ok(assignment));
                    }
                }
                group.state = State::Stable;
                group.sync_deadline = None;
                return Ok(group
                    .members
                    .get(member_id)
                    .and_then(|m| m.assignment.clone())
                    .unwrap_or_default());
            }
            let deadline = group.sync_deadline.unwrap_or_else(Instant::now);
            let member = group.members.get_mut(member_id).expect("checked above");
            member.last_seen = Instant::now();
            let (tx, rx) = oneshot::channel();
            member.sync_waiter = Some(tx);
            (rx, deadline)
        };
        match tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline + Duration::from_secs(5)),
            receiver,
        )
        .await
        {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) | Err(_) => Err(ErrorCode::RebalanceInProgress),
        }
    }

    /// Where a SyncGroup stands before its assignments are read (review):
    /// the same judgment `sync` makes under its lock — group, member,
    /// generation, state — answered first, so a stale retry hears the
    /// coordinator's own code and no assignment is decoded for a member not
    /// entitled to make one.
    pub fn check_sync(
        &self,
        group_id: &str,
        generation: i32,
        member_id: &str,
    ) -> Result<SyncStanding, ErrorCode> {
        // An empty group id is invalid in every group operation (review), as
        // Kafka's coordinator has it: `INVALID_GROUP_ID`, never a verdict on
        // a member's identity.
        if group_id.is_empty() {
            return Err(ErrorCode::InvalidGroupId);
        }
        let groups = self.lock();
        let group = groups.get(group_id).ok_or(ErrorCode::UnknownMemberId)?;
        if !group.members.contains_key(member_id) {
            return Err(ErrorCode::UnknownMemberId);
        }
        if group.generation != generation {
            return Err(ErrorCode::IllegalGeneration);
        }
        match group.state {
            State::Empty | State::PreparingRebalance => Err(ErrorCode::RebalanceInProgress),
            State::Stable => Ok(SyncStanding::Other),
            State::CompletingRebalance => Ok(if group.leader.as_deref() == Some(member_id) {
                SyncStanding::Leader
            } else {
                SyncStanding::Other
            }),
        }
    }

    /// The leader's assignment was refused before it was applied (review):
    /// the round it was for is over — every member rejoins, and a follower
    /// parked on the assignment hears `REBALANCE_IN_PROGRESS` now rather than
    /// at the sync deadline. The leader keeps its membership, and may assign
    /// again in the round that follows.
    pub fn assignment_refused(&self, group_id: &str, generation: i32, member_id: &str) {
        let mut groups = self.lock();
        let Some(group) = groups.get_mut(group_id) else {
            return;
        };
        if group.generation != generation
            || group.state != State::CompletingRebalance
            || group.leader.as_deref() != Some(member_id)
        {
            return;
        }
        for member in group.members.values_mut() {
            member.in_round = false;
        }
        begin_round(
            group,
            Instant::now(),
            false,
            self.config.initial_rebalance_delay,
        );
    }

    /// A member is alive. In a round it is told to rejoin.
    pub fn heartbeat(
        &self,
        group_id: &str,
        generation: i32,
        member_id: &str,
    ) -> Result<(), ErrorCode> {
        if group_id.is_empty() {
            return Err(ErrorCode::InvalidGroupId);
        }
        let mut groups = self.lock();
        let group = groups.get_mut(group_id).ok_or(ErrorCode::UnknownMemberId)?;
        let member = group
            .members
            .get_mut(member_id)
            .ok_or(ErrorCode::UnknownMemberId)?;
        if group.generation != generation {
            return Err(ErrorCode::IllegalGeneration);
        }
        member.last_seen = Instant::now();
        // As Kafka's coordinator answers (audit): a heartbeat while the
        // assignment is awaited is fine — the member has its generation and
        // is waiting on the leader, not on a round — and one during a round
        // says so, so the member rejoins.
        match group.state {
            State::Stable | State::CompletingRebalance => Ok(()),
            State::PreparingRebalance | State::Empty => Err(ErrorCode::RebalanceInProgress),
        }
    }

    /// A member leaves; the others rebalance.
    pub fn leave(&self, group_id: &str, member_id: &str) -> Result<(), ErrorCode> {
        if group_id.is_empty() {
            return Err(ErrorCode::InvalidGroupId);
        }
        let mut groups = self.lock();
        let group = groups.get_mut(group_id).ok_or(ErrorCode::UnknownMemberId)?;
        let Some(mut gone) = group.members.remove(member_id) else {
            return Err(ErrorCode::UnknownMemberId);
        };
        if let Some(parked) = gone.sync_waiter.take() {
            let _ = parked.send(Err(ErrorCode::UnknownMemberId));
        }
        drop(gone);
        member_left(group, Instant::now(), self.config.initial_rebalance_delay);
        Ok(())
    }

    /// Whether a commit under this membership is in order, as Kafka's
    /// coordinator judges it (audit). A simple consumer's (generation -1, no
    /// member) is taken only where no managed group stands — the group
    /// unknown, or empty — so an unmanaged writer never moves a managed
    /// group's positions. A member's must name its group's generation, and is
    /// taken while the group is stable OR preparing a rebalance — the
    /// revoke-and-commit path every stock consumer takes before it rejoins —
    /// and refused `REBALANCE_IN_PROGRESS` only while the assignment is
    /// awaited. Every member's commit is proof of life, as Kafka's is.
    pub fn check_commit(
        &self,
        group_id: &str,
        generation: i32,
        member_id: &str,
    ) -> Result<(), ErrorCode> {
        if group_id.is_empty() {
            return Err(ErrorCode::InvalidGroupId);
        }
        let simple = generation == -1 && member_id.is_empty();
        let mut groups = self.lock();
        let Some(group) = groups.get_mut(group_id) else {
            // No group: a simple consumer's is taken; a member's names a
            // generation nothing here has.
            return if simple {
                Ok(())
            } else {
                Err(ErrorCode::IllegalGeneration)
            };
        };
        if simple {
            return if group.state == State::Empty {
                Ok(())
            } else {
                Err(ErrorCode::UnknownMemberId)
            };
        }
        let member = group
            .members
            .get_mut(member_id)
            .ok_or(ErrorCode::UnknownMemberId)?;
        if group.generation != generation {
            return Err(ErrorCode::IllegalGeneration);
        }
        member.last_seen = Instant::now();
        match group.state {
            State::Stable | State::PreparingRebalance => Ok(()),
            State::CompletingRebalance => Err(ErrorCode::RebalanceInProgress),
            // Unreachable: an empty group has no member to have found.
            State::Empty => Err(ErrorCode::UnknownMemberId),
        }
    }

    /// The clock's work: members whose session lapsed go, rounds whose
    /// window closed complete, minted ids nobody came back for expire.
    /// Called every few hundred milliseconds by the listener.
    pub fn sweep(&self, now: Instant) {
        let mut groups = self.lock();
        for group in groups.values_mut() {
            group
                .minted
                .retain(|_, minted_at| now.duration_since(*minted_at) < MINTED_ID_TTL);
            let lapsed: Vec<String> = group
                .members
                .iter()
                .filter(|(_, m)| {
                    // A member in a round is waiting on us, not the other way.
                    !(group.state == State::PreparingRebalance && m.in_round)
                        // While the leader's assignment is awaited, a parked
                        // follower cannot heartbeat — its session is blocked on
                        // the SyncGroup (review); the sync deadline bounds it.
                        && group.state != State::CompletingRebalance
                        && now.duration_since(m.last_seen) > m.session_timeout
                })
                .map(|(id, _)| id.clone())
                .collect();
            // A round whose leader never synced (review): the leader is out,
            // the rest rebalance, and every parked sync hears it.
            if group.state == State::CompletingRebalance
                && group.sync_deadline.is_some_and(|deadline| now > deadline)
            {
                if let Some(leader) = group.leader.clone() {
                    if let Some(mut gone) = group.members.remove(&leader) {
                        if let Some(parked) = gone.sync_waiter.take() {
                            let _ = parked.send(Err(ErrorCode::RebalanceInProgress));
                        }
                        tracing::info!(member = %leader, "kafka group leader never synced within the rebalance timeout; the group rebalances");
                    }
                }
                // The round that follows releases every parked sync.
                group.sync_deadline = None;
                member_left(group, now, self.config.initial_rebalance_delay);
            }
            for id in lapsed {
                if let Some(mut gone) = group.members.remove(&id) {
                    if let Some(parked) = gone.sync_waiter.take() {
                        let _ = parked.send(Err(ErrorCode::UnknownMemberId));
                    }
                    tracing::info!(member = %id, "kafka group member's session lapsed; the group rebalances");
                }
                member_left(group, now, self.config.initial_rebalance_delay);
            }
            maybe_complete(group, now);
        }
        groups.retain(|_, group| !(group.members.is_empty() && group.minted.is_empty()));
    }

    #[cfg(test)]
    fn generation(&self, group_id: &str) -> Option<i32> {
        self.lock().get(group_id).map(|g| g.generation)
    }

    #[cfg(test)]
    fn last_seen(&self, group_id: &str, member_id: &str) -> Option<Instant> {
        self.lock()
            .get(group_id)
            .and_then(|g| g.members.get(member_id))
            .map(|m| m.last_seen)
    }
}

/// A round begins, or the one under way is extended to this member.
fn begin_round(group: &mut Group, now: Instant, was_empty: bool, initial_delay: Duration) {
    let longest = group
        .members
        .values()
        .map(|m| m.rebalance_timeout)
        .max()
        .unwrap_or(Duration::from_secs(60));
    match group.state {
        State::PreparingRebalance => {
            // The round under way keeps its deadline (review): every parked
            // join waits on the deadline it saw, and a late rejoin must not
            // move it past them and turn their wait into a churn.
        }
        State::Empty | State::CompletingRebalance | State::Stable => {
            group.state = State::PreparingRebalance;
            group.round = Some(Round {
                not_before: if was_empty { now + initial_delay } else { now },
                deadline: now + longest,
            });
            // Everyone must rejoin; the joiner already has. A SyncGroup
            // parked on the round this one supersedes hears it NOW (review):
            // a follower's connection is serial, so a parked sync it never
            // hears back on is a member that cannot rejoin, and would lapse
            // healthy at its session timeout.
            group.sync_deadline = None;
            for member in group.members.values_mut() {
                if let Some(parked) = member.sync_waiter.take() {
                    let _ = parked.send(Err(ErrorCode::RebalanceInProgress));
                }
            }
        }
    }
}

/// A member is gone: an empty group rests, a group with members rebalances.
fn member_left(group: &mut Group, now: Instant, initial_delay: Duration) {
    if group.members.is_empty() {
        group.state = State::Empty;
        group.leader = None;
        group.protocol_name = None;
        group.protocol_type = None;
        group.round = None;
        return;
    }
    if group.state != State::PreparingRebalance {
        for member in group.members.values_mut() {
            member.in_round = false;
        }
        begin_round(group, now, false, initial_delay);
    }
}

/// Completes the round if it can: every member is in and the window opened,
/// or the deadline passed (whoever is not in is out).
fn maybe_complete(group: &mut Group, now: Instant) {
    if group.state != State::PreparingRebalance {
        return;
    }
    let Some(round) = group.round.as_ref() else {
        return;
    };
    let all_in = group.members.values().all(|m| m.in_round);
    let window_open = now >= round.not_before;
    let deadline_passed = now >= round.deadline;
    if !(deadline_passed || (all_in && window_open)) {
        return;
    }
    // Those who did not rejoin are out.
    group.members.retain(|_, m| m.in_round);
    group.generation = group.generation.wrapping_add(1).max(1);
    group.round = None;
    if group.members.is_empty() {
        group.state = State::Empty;
        group.leader = None;
        group.protocol_name = None;
        group.protocol_type = None;
        return;
    }
    // The leader: the earliest joiner still present, unless the previous
    // leader is.
    let leader = match group
        .leader
        .as_ref()
        .filter(|id| group.members.contains_key(*id))
    {
        Some(id) => id.clone(),
        None => group
            .members
            .iter()
            .min_by_key(|(_, m)| m.joined_at)
            .map(|(id, _)| id.clone())
            .expect("members is not empty"),
    };
    // The protocol (review): of those every member supports, the one most
    // members put first — each member votes for the first candidate in its
    // own order, as Kafka's coordinator tallies it — a tie going to the
    // leader's preference.
    let candidates: Vec<String> = group.members[&leader]
        .protocols
        .iter()
        .map(|(name, _)| name.clone())
        .filter(|name| {
            group
                .members
                .values()
                .all(|m| m.protocols.iter().any(|(n, _)| n == name))
        })
        .collect();
    let protocol = {
        let mut votes: HashMap<&str, usize> = HashMap::new();
        for member in group.members.values() {
            if let Some((name, _)) = member
                .protocols
                .iter()
                .find(|(n, _)| candidates.iter().any(|c| c == n))
            {
                *votes.entry(name.as_str()).or_insert(0) += 1;
            }
        }
        // `max_by_key` keeps the LAST maximum, so the candidates are walked
        // from the leader's least preferred: a tie lands on its first.
        candidates
            .iter()
            .rev()
            .max_by_key(|name| votes.get(name.as_str()).copied().unwrap_or(0))
            .cloned()
    };
    let Some(protocol) = protocol else {
        // No common protocol: everyone hears WHY (review) — admission checks
        // the group-wide intersection, so this is not reached today, and
        // the day it is the answer is the protocol's own, not a rebalance
        // that would never complete — and the group rests, forgetting its
        // protocol type with its members.
        for member in group.members.values_mut() {
            if let Some(waiter) = member.join_waiter.take() {
                let _ = waiter.send(Err(ErrorCode::InconsistentGroupProtocol));
            }
        }
        group.members.clear();
        group.state = State::Empty;
        group.leader = None;
        group.protocol_name = None;
        group.protocol_type = None;
        return;
    };
    let members: Vec<(String, Vec<u8>)> = group
        .members
        .iter()
        .map(|(id, m)| {
            let metadata = m
                .protocols
                .iter()
                .find(|(n, _)| *n == protocol)
                .map(|(_, meta)| meta.clone())
                .unwrap_or_default();
            (id.clone(), metadata)
        })
        .collect();
    group.leader = Some(leader.clone());
    group.protocol_name = Some(protocol.clone());
    group.state = State::CompletingRebalance;
    // The leader's assignment is owed within the members' longest rebalance
    // timeout (review); a follower waits that long for it, and the sweeper
    // ends a round whose leader never synced.
    let longest = group
        .members
        .values()
        .map(|m| m.rebalance_timeout)
        .max()
        .unwrap_or(Duration::from_secs(60));
    group.sync_deadline = Some(now + longest);
    let generation = group.generation;
    for (id, member) in group.members.iter_mut() {
        member.in_round = false;
        member.assignment = None;
        // Heartbeats were held while the join was parked (review): the
        // session starts again from the join that succeeded.
        member.last_seen = now;
        if let Some(waiter) = member.join_waiter.take() {
            let _ = waiter.send(Ok(JoinOutcome {
                generation,
                protocol_name: protocol.clone(),
                leader: leader.clone(),
                member_id: id.clone(),
                members: if *id == leader {
                    members.clone()
                } else {
                    Vec::new()
                },
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn config() -> GroupConfig {
        GroupConfig {
            initial_rebalance_delay: Duration::from_millis(50),
            // The lab's sessions are short; the default minimum is Kafka's.
            min_session_timeout: Duration::from_secs(1),
            ..GroupConfig::default()
        }
    }

    /// The default minimum session is Kafka's (review): a second is refused,
    /// six are not.
    #[tokio::test]
    async fn the_default_minimum_session_is_kafkas() {
        let coordinator = Coordinator::new(GroupConfig::default());
        let mut short = request("g", "", "a");
        short.session_timeout = Duration::from_secs(1);
        assert_eq!(
            coordinator.join(short).await,
            Err(ErrorCode::InvalidSessionTimeout)
        );
        assert_eq!(
            GroupConfig::default().min_session_timeout,
            Duration::from_secs(6)
        );
    }

    fn request(group: &str, member: &str, client: &str) -> JoinRequest {
        JoinRequest {
            group_id: group.to_owned(),
            member_id: member.to_owned(),
            client_id: client.to_owned(),
            protocol_type: "consumer".to_owned(),
            protocols: vec![("range".to_owned(), format!("meta-{client}").into_bytes())],
            session_timeout: Duration::from_secs(10),
            rebalance_timeout: Duration::from_secs(10),
            require_member_id: false,
        }
    }

    /// A sweeper the tests drive by hand, as the listener drives it by clock.
    async fn complete_by_sweeping(coordinator: Arc<Coordinator>, until: Duration) {
        let started = Instant::now();
        while started.elapsed() < until {
            tokio::time::sleep(Duration::from_millis(10)).await;
            coordinator.sweep(Instant::now());
        }
    }

    /// One member: a first join at v4 is told to come back with an id; the
    /// rejoin completes after the initial window with the member as leader
    /// at generation 1; the leader's sync hands out the assignment; a
    /// heartbeat at the generation is fine, at a stale one is illegal, from
    /// a stranger is unknown.
    #[tokio::test]
    async fn one_member_forms_a_group_and_leads_it() {
        let coordinator = Arc::new(Coordinator::new(config()));
        let mut first = request("g", "", "c1");
        first.require_member_id = true;
        let Joined::MemberIdRequired(id) = coordinator.join(first).await.unwrap() else {
            panic!("a first join at v4 is answered with an id to come back with");
        };
        assert!(id.starts_with("c1-"));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_millis(400),
        ));
        let Joined::Complete(outcome) = coordinator.join(request("g", &id, "c1")).await.unwrap()
        else {
            panic!("the rejoin completes");
        };
        sweeper.abort();
        assert_eq!(outcome.generation, 1);
        assert_eq!(outcome.leader, id);
        assert_eq!(outcome.member_id, id);
        assert_eq!(outcome.protocol_name, "range");
        assert_eq!(outcome.members, vec![(id.clone(), b"meta-c1".to_vec())]);
        let assignment = coordinator
            .sync("g", 1, &id, vec![(id.clone(), b"assign-c1".to_vec())])
            .await
            .unwrap();
        assert_eq!(assignment, b"assign-c1");
        assert_eq!(coordinator.heartbeat("g", 1, &id), Ok(()));
        assert_eq!(
            coordinator.heartbeat("g", 0, &id),
            Err(ErrorCode::IllegalGeneration)
        );
        assert_eq!(
            coordinator.heartbeat("g", 1, "nobody"),
            Err(ErrorCode::UnknownMemberId)
        );
        assert_eq!(
            coordinator.join(request("g", "stranger", "c9")).await,
            Err(ErrorCode::UnknownMemberId),
            "an id this coordinator never minted"
        );
        assert_eq!(coordinator.check_commit("g", 1, &id), Ok(()));
        assert_eq!(
            coordinator.check_commit("g", -1, ""),
            Err(ErrorCode::UnknownMemberId),
            "a simple consumer's commit is refused while the group is managed (audit): Kafka takes it only for an empty group"
        );
        assert_eq!(
            coordinator.check_commit("g", 0, &id),
            Err(ErrorCode::IllegalGeneration)
        );
    }

    /// Two members joining inside the window are one generation: the first
    /// leads and sees both members' metadata, the follower's sync waits for
    /// the leader's and gets its share; a leave turns the survivor's next
    /// heartbeat into a rebalance, and its rejoin is generation 2 with itself
    /// as leader.
    #[tokio::test]
    async fn two_members_share_a_generation_and_a_leave_rebalances() {
        let coordinator = Arc::new(Coordinator::new(config()));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_secs(2),
        ));
        let (a, b) = tokio::join!(coordinator.join(request("g", "", "a")), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            coordinator.join(request("g", "", "b")).await
        });
        let Joined::Complete(a) = a.unwrap() else {
            panic!()
        };
        let Joined::Complete(b) = b.unwrap() else {
            panic!()
        };
        assert_eq!((a.generation, b.generation), (1, 1));
        assert_eq!(a.leader, b.leader);
        let (leader, follower) = if a.leader == a.member_id {
            (a, b)
        } else {
            (b, a)
        };
        assert_eq!(leader.members.len(), 2, "the leader sees both");
        assert!(follower.members.is_empty(), "the follower sees none");
        // The follower syncs first and waits; the leader's sync releases it.
        let (follower_share, leader_share) = tokio::join!(
            coordinator.sync("g", 1, &follower.member_id, Vec::new()),
            async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                coordinator
                    .sync(
                        "g",
                        1,
                        &leader.member_id,
                        vec![
                            (leader.member_id.clone(), b"p0".to_vec()),
                            (follower.member_id.clone(), Vec::new()),
                        ],
                    )
                    .await
            }
        );
        assert_eq!(leader_share.unwrap(), b"p0");
        assert_eq!(follower_share.unwrap(), b"");
        assert_eq!(coordinator.heartbeat("g", 1, &follower.member_id), Ok(()));
        // The follower leaves; the leader is told to rejoin.
        coordinator.leave("g", &follower.member_id).unwrap();
        assert_eq!(
            coordinator.heartbeat("g", 1, &leader.member_id),
            Err(ErrorCode::RebalanceInProgress)
        );
        assert_eq!(
            coordinator.check_commit("g", 1, &leader.member_id),
            Ok(()),
            "a commit during the round is taken, as Kafka takes it (audit): the revoke-and-commit path"
        );
        let Joined::Complete(again) = coordinator
            .join(request("g", &leader.member_id, "a"))
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(again.generation, 2);
        assert_eq!(again.leader, leader.member_id);
        assert_eq!(again.members.len(), 1);
        sweeper.abort();
        assert_eq!(coordinator.generation("g"), Some(2));
    }

    /// The protocol every member supports is the group-wide intersection
    /// (review): {a,b} and {b,c} agree on b; {a,c} agrees with each of them on
    /// something and with both on nothing, and is refused rather than admitted
    /// into a round that cannot complete.
    #[tokio::test]
    async fn a_protocol_must_be_shared_by_the_whole_group() {
        let coordinator = Arc::new(Coordinator::new(config()));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_secs(2),
        ));
        let with = |client: &str, names: &[&str]| {
            let mut request = request("g", "", client);
            request.protocols = names.iter().map(|n| (n.to_string(), Vec::new())).collect();
            request
        };
        let (a, b) = tokio::join!(coordinator.join(with("a", &["a", "b"])), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            coordinator.join(with("b", &["b", "c"])).await
        });
        let Joined::Complete(a) = a.unwrap() else {
            panic!()
        };
        let Joined::Complete(_) = b.unwrap() else {
            panic!()
        };
        assert_eq!(a.protocol_name, "b", "the one they share");
        assert_eq!(
            coordinator.join(with("c", &["a", "c"])).await,
            Err(ErrorCode::InconsistentGroupProtocol),
            "pairwise yes, group-wide no"
        );
        sweeper.abort();
    }

    /// A stale generation keeps nobody alive (review): a heartbeat or a
    /// commit the coordinator refuses does not refresh the member's session.
    #[tokio::test]
    async fn a_refused_heartbeat_does_not_refresh_the_session() {
        let coordinator = Arc::new(Coordinator::new(config()));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_secs(1),
        ));
        let Joined::Complete(a) = coordinator.join(request("g", "", "a")).await.unwrap() else {
            panic!()
        };
        coordinator
            .sync(
                "g",
                1,
                &a.member_id,
                vec![(a.member_id.clone(), Vec::new())],
            )
            .await
            .unwrap();
        sweeper.abort();
        let seen = coordinator.last_seen("g", &a.member_id).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            coordinator.heartbeat("g", 0, &a.member_id),
            Err(ErrorCode::IllegalGeneration)
        );
        assert_eq!(
            coordinator.check_commit("g", 0, &a.member_id),
            Err(ErrorCode::IllegalGeneration)
        );
        assert_eq!(
            coordinator.last_seen("g", &a.member_id),
            Some(seen),
            "not refreshed"
        );
        assert_eq!(coordinator.heartbeat("g", 1, &a.member_id), Ok(()));
        assert!(
            coordinator.last_seen("g", &a.member_id).unwrap() > seen,
            "refreshed"
        );
    }

    /// An empty group forgets its protocol type with its members (review):
    /// the next joiner may bring another.
    #[tokio::test]
    async fn an_empty_group_forgets_its_protocol_type() {
        let coordinator = Arc::new(Coordinator::new(config()));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_secs(2),
        ));
        let Joined::Complete(a) = coordinator.join(request("g", "", "a")).await.unwrap() else {
            panic!()
        };
        coordinator.leave("g", &a.member_id).unwrap();
        let mut connect = request("g", "", "b");
        connect.protocol_type = "connect".to_owned();
        assert!(matches!(
            coordinator.join(connect).await,
            Ok(Joined::Complete(_))
        ));
        sweeper.abort();
    }

    /// A minted member id stays within the wire's string limit however long
    /// the client id was (review), and other negative generations are not
    /// the simple-consumer sentinel.
    #[tokio::test]
    async fn a_minted_member_id_is_bounded_and_only_minus_one_is_a_simple_consumer() {
        let coordinator = Arc::new(Coordinator::new(config()));
        let mut long = request("g", "", &"x".repeat(40_000));
        long.require_member_id = true;
        let Joined::MemberIdRequired(id) = coordinator.join(long).await.unwrap() else {
            panic!()
        };
        assert!(id.len() <= MEMBER_ID_PREFIX_CHARS + 1 + 36, "{}", id.len());
        assert!(
            coordinator.check_commit("g", -2, "").is_err(),
            "-2 is not the sentinel"
        );
        assert_eq!(coordinator.check_commit("g", -1, ""), Ok(()));
    }

    /// A follower parked on the leader's assignment cannot heartbeat — its
    /// session is blocked on the SyncGroup — so its session does not lapse
    /// while the round completes (review): the leader syncs after the
    /// follower's session timeout, and the follower still gets its share.
    #[tokio::test]
    async fn a_parked_follower_outlives_its_session_timeout_until_the_leader_syncs() {
        let coordinator = Arc::new(Coordinator::new(config()));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_secs(4),
        ));
        let mut a = request("g", "", "a");
        a.session_timeout = Duration::from_secs(1);
        a.rebalance_timeout = Duration::from_secs(5);
        let mut b = request("g", "", "b");
        b.session_timeout = Duration::from_secs(1);
        b.rebalance_timeout = Duration::from_secs(5);
        let (ja, jb) = tokio::join!(coordinator.join(a), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            coordinator.join(b).await
        });
        let Joined::Complete(ja) = ja.unwrap() else {
            panic!()
        };
        let Joined::Complete(jb) = jb.unwrap() else {
            panic!()
        };
        let (leader, follower) = if ja.leader == ja.member_id {
            (ja, jb)
        } else {
            (jb, ja)
        };
        let (share, _) = tokio::join!(
            coordinator.sync("g", 1, &follower.member_id, Vec::new()),
            async {
                // Longer than the follower's session, shorter than the round.
                tokio::time::sleep(Duration::from_millis(1_400)).await;
                coordinator
                    .sync(
                        "g",
                        1,
                        &leader.member_id,
                        vec![(follower.member_id.clone(), b"p0".to_vec())],
                    )
                    .await
                    .unwrap()
            }
        );
        assert_eq!(share.unwrap(), b"p0", "still a member, and assigned");
        sweeper.abort();
    }

    /// A round that supersedes a completing one releases every parked
    /// SyncGroup at once (review): a follower's connection is serial, so a
    /// parked sync it never hears back on is a member that cannot rejoin,
    /// and would lapse healthy at its session timeout.
    #[tokio::test]
    async fn a_new_round_releases_a_parked_sync_at_once() {
        let coordinator = Arc::new(Coordinator::new(config()));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_secs(2),
        ));
        let (ja, jb) = tokio::join!(coordinator.join(request("g", "", "a")), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            coordinator.join(request("g", "", "b")).await
        });
        let Joined::Complete(ja) = ja.unwrap() else {
            panic!()
        };
        let Joined::Complete(jb) = jb.unwrap() else {
            panic!()
        };
        let follower = if ja.leader == ja.member_id { jb } else { ja };
        let started = Instant::now();
        let (parked, _) = tokio::join!(
            coordinator.sync("g", 1, &follower.member_id, Vec::new()),
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                // A third member's join supersedes the completing round; its
                // own join parks for the new round and is not awaited here.
                let coordinator = Arc::clone(&coordinator);
                tokio::spawn(async move { coordinator.join(request("g", "", "c")).await });
            }
        );
        assert_eq!(parked, Err(ErrorCode::RebalanceInProgress));
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "released at the transition, not at the sync deadline"
        );
        sweeper.abort();
    }

    /// A rebalance timeout is honored up to the cap and no further (review):
    /// two members asking for an hour, a leader that never syncs — the
    /// parked follower is released at the cap, not held for the hour.
    #[tokio::test]
    async fn a_rebalance_timeout_is_capped_before_it_becomes_a_deadline() {
        let coordinator = Arc::new(Coordinator::new(GroupConfig {
            max_rebalance_timeout: Duration::from_millis(300),
            ..config()
        }));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_secs(4),
        ));
        let mut a = request("g", "", "a");
        a.rebalance_timeout = Duration::from_secs(3_600);
        let mut b = request("g", "", "b");
        b.rebalance_timeout = Duration::from_secs(3_600);
        let (ja, jb) = tokio::join!(coordinator.join(a), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            coordinator.join(b).await
        });
        let Joined::Complete(ja) = ja.unwrap() else {
            panic!()
        };
        let Joined::Complete(jb) = jb.unwrap() else {
            panic!()
        };
        let follower = if ja.leader == ja.member_id { jb } else { ja };
        let started = Instant::now();
        let parked = coordinator
            .sync("g", 1, &follower.member_id, Vec::new())
            .await;
        assert_eq!(parked, Err(ErrorCode::RebalanceInProgress));
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "released at the cap, not after the hour asked for"
        );
        sweeper.abort();
    }

    /// The protocol is the members' vote (review), not the leader's first
    /// common one: a leader preferring range and two followers preferring
    /// roundrobin negotiate roundrobin; one against one is a tie, and the
    /// leader's preference stands.
    #[tokio::test]
    async fn the_protocol_is_the_members_vote_not_the_leaders_first() {
        let ranked = |group: &str, client: &str, first: &str, second: &str| {
            let mut request = request(group, "", client);
            request.protocols = vec![
                (first.to_owned(), Vec::new()),
                (second.to_owned(), Vec::new()),
            ];
            request
        };
        let coordinator = Arc::new(Coordinator::new(config()));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_secs(2),
        ));
        let (a, b, c) = tokio::join!(
            coordinator.join(ranked("g", "a", "range", "roundrobin")),
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                coordinator
                    .join(ranked("g", "b", "roundrobin", "range"))
                    .await
            },
            async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                coordinator
                    .join(ranked("g", "c", "roundrobin", "range"))
                    .await
            }
        );
        for joined in [a, b, c] {
            let Joined::Complete(outcome) = joined.unwrap() else {
                panic!()
            };
            assert_eq!(outcome.protocol_name, "roundrobin");
        }
        let (a, b) = tokio::join!(
            coordinator.join(ranked("h", "a", "range", "roundrobin")),
            async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                coordinator
                    .join(ranked("h", "b", "roundrobin", "range"))
                    .await
            }
        );
        for joined in [a, b] {
            let Joined::Complete(outcome) = joined.unwrap() else {
                panic!()
            };
            assert_eq!(outcome.protocol_name, "range", "a tie is the leader's");
        }
        sweeper.abort();
    }

    /// A refused assignment releases the parked followers (review): the
    /// round is over at once, the leader is still a member and hears the
    /// rebalance like everyone.
    #[tokio::test]
    async fn a_refused_assignment_releases_the_parked_followers() {
        let coordinator = Arc::new(Coordinator::new(config()));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_secs(2),
        ));
        let (ja, jb) = tokio::join!(coordinator.join(request("g", "", "a")), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            coordinator.join(request("g", "", "b")).await
        });
        let Joined::Complete(ja) = ja.unwrap() else {
            panic!()
        };
        let Joined::Complete(jb) = jb.unwrap() else {
            panic!()
        };
        let (leader, follower) = if ja.leader == ja.member_id {
            (ja, jb)
        } else {
            (jb, ja)
        };
        let started = Instant::now();
        let (parked, _) = tokio::join!(
            coordinator.sync("g", 1, &follower.member_id, Vec::new()),
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                coordinator.assignment_refused("g", 1, &leader.member_id);
            }
        );
        assert_eq!(parked, Err(ErrorCode::RebalanceInProgress));
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(
            coordinator.heartbeat("g", 1, &leader.member_id),
            Err(ErrorCode::RebalanceInProgress),
            "still a member, asked to rejoin"
        );
        sweeper.abort();
    }

    /// A leader's map is applied as Kafka applies it (review): a member it
    /// omits gets an empty assignment, an id that is not a member is
    /// ignored, and the group is stable.
    #[tokio::test]
    async fn a_leaders_map_is_applied_as_kafka_applies_it() {
        let coordinator = Arc::new(Coordinator::new(config()));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_millis(400),
        ));
        let Joined::Complete(only) = coordinator.join(request("g", "", "a")).await.unwrap() else {
            panic!()
        };
        sweeper.abort();
        let own = coordinator
            .sync(
                "g",
                1,
                &only.member_id,
                vec![("stranger".to_owned(), b"p0".to_vec())],
            )
            .await
            .unwrap();
        assert!(own.is_empty(), "omitted: an empty assignment");
        assert_eq!(
            coordinator.heartbeat("g", 1, &only.member_id),
            Ok(()),
            "stable"
        );
    }

    /// An unchanged rejoin replays the join result while the round is
    /// completing (audit), as Kafka's does — the follower's without members,
    /// the leader's with them, no round begun; once the group is stable the
    /// leader's rejoin is a rebalance.
    #[tokio::test]
    async fn an_unchanged_rejoin_replays_while_completing_and_a_stable_leaders_rebalances() {
        let coordinator = Arc::new(Coordinator::new(config()));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_secs(2),
        ));
        let (ja, jb) = tokio::join!(coordinator.join(request("g", "", "a")), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            coordinator.join(request("g", "", "b")).await
        });
        let Joined::Complete(ja) = ja.unwrap() else {
            panic!()
        };
        let Joined::Complete(jb) = jb.unwrap() else {
            panic!()
        };
        let (leader, follower) = if ja.leader == ja.member_id {
            (ja, jb)
        } else {
            (jb, ja)
        };
        let client_of = |id: &str| if id.starts_with("a-") { "a" } else { "b" };
        // Completing: both replay, the leader with the members.
        let started = Instant::now();
        let Joined::Complete(again) = coordinator
            .join(request(
                "g",
                &follower.member_id,
                client_of(&follower.member_id),
            ))
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!((again.generation, again.members.len()), (1, 0));
        let Joined::Complete(again) = coordinator
            .join(request(
                "g",
                &leader.member_id,
                client_of(&leader.member_id),
            ))
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(
            (again.generation, again.members.len()),
            (1, 2),
            "the leader's replay carries the members"
        );
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "answered at once, no round"
        );
        assert_eq!(
            coordinator.heartbeat("g", 1, &follower.member_id),
            Ok(()),
            "no round begun"
        );
        // Stable: the leader syncs; its rejoin is now a rebalance.
        coordinator
            .sync("g", 1, &leader.member_id, Vec::new())
            .await
            .unwrap();
        let rejoin = {
            let coordinator = Arc::clone(&coordinator);
            let id = leader.member_id.clone();
            let client = client_of(&leader.member_id);
            tokio::spawn(async move { coordinator.join(request("g", &id, client)).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            coordinator.heartbeat("g", 1, &follower.member_id),
            Err(ErrorCode::RebalanceInProgress),
            "a stable leader's rejoin is a rebalance"
        );
        rejoin.abort();
        sweeper.abort();
    }

    /// A simple consumer commits only where no managed group stands (audit),
    /// as Kafka's coordinator has it: an unknown group takes it, a group with
    /// members refuses it as an unknown member, and the group left empty
    /// takes it again.
    #[tokio::test]
    async fn a_simple_consumer_commits_only_where_no_managed_group_stands() {
        let coordinator = Arc::new(Coordinator::new(config()));
        assert_eq!(coordinator.check_commit("g", -1, ""), Ok(()), "no group");
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_millis(400),
        ));
        let Joined::Complete(only) = coordinator.join(request("g", "", "a")).await.unwrap() else {
            panic!()
        };
        sweeper.abort();
        assert_eq!(
            coordinator.check_commit("g", -1, ""),
            Err(ErrorCode::UnknownMemberId),
            "managed"
        );
        coordinator.leave("g", &only.member_id).unwrap();
        assert_eq!(coordinator.check_commit("g", -1, ""), Ok(()), "empty again");
    }

    /// A new group's window extends for each late joiner (audit), as Kafka's
    /// initial delayed join does: three members starting 100 ms apart under a
    /// 150 ms window are one generation, not two.
    #[tokio::test]
    async fn a_new_groups_window_extends_for_each_late_joiner() {
        let coordinator = Arc::new(Coordinator::new(GroupConfig {
            initial_rebalance_delay: Duration::from_millis(150),
            ..config()
        }));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_secs(3),
        ));
        let (a, b, c) = tokio::join!(
            coordinator.join(request("g", "", "a")),
            async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                coordinator.join(request("g", "", "b")).await
            },
            async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                coordinator.join(request("g", "", "c")).await
            }
        );
        for joined in [a, b, c] {
            let Joined::Complete(outcome) = joined.unwrap() else {
                panic!()
            };
            assert_eq!(outcome.generation, 1, "one generation for the whole fleet");
        }
        sweeper.abort();
    }

    /// An empty group id is invalid in every group operation (review): a
    /// commit is refused before the simple-consumer bypass, so nothing is
    /// stored under a group that names none, and a sync, a heartbeat and a
    /// leave hear `INVALID_GROUP_ID` rather than a verdict on the member.
    #[tokio::test]
    async fn an_empty_group_id_is_invalid_in_every_group_operation() {
        let coordinator = Coordinator::new(config());
        assert_eq!(
            coordinator.check_commit("", -1, ""),
            Err(ErrorCode::InvalidGroupId)
        );
        assert_eq!(coordinator.check_commit("g", -1, ""), Ok(()));
        assert_eq!(
            coordinator.check_sync("", 1, "m"),
            Err(ErrorCode::InvalidGroupId)
        );
        assert_eq!(
            coordinator.sync("", 1, "m", Vec::new()).await,
            Err(ErrorCode::InvalidGroupId)
        );
        assert_eq!(
            coordinator.heartbeat("", 1, "m"),
            Err(ErrorCode::InvalidGroupId)
        );
        assert_eq!(coordinator.leave("", "m"), Err(ErrorCode::InvalidGroupId));
        assert_eq!(
            coordinator.heartbeat("g", 1, "m"),
            Err(ErrorCode::UnknownMemberId),
            "a named group nobody joined: the member is what is unknown"
        );
    }

    /// Shutdown releases a parked join at once (review), so the listener's
    /// drain is bounded by the produce and fetch ceilings and never by a
    /// rebalance timeout, and admits nobody afterwards.
    #[tokio::test]
    async fn shutdown_releases_a_parked_join_and_admits_nobody_after() {
        let coordinator = Arc::new(Coordinator::new(GroupConfig {
            initial_rebalance_delay: Duration::from_secs(60),
            ..config()
        }));
        let parked = {
            let coordinator = Arc::clone(&coordinator);
            tokio::spawn(async move { coordinator.join(request("g", "", "a")).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        let started = Instant::now();
        coordinator.shutdown();
        assert_eq!(
            parked.await.unwrap(),
            Err(ErrorCode::RebalanceInProgress),
            "released, not held for the round's window"
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(
            coordinator.join(request("g", "", "b")).await,
            Err(ErrorCode::CoordinatorNotAvailable)
        );
    }

    /// A member's session starts again when its round completes (review):
    /// a round held longer than the session timeout does not evict the
    /// member at the first sweep after completion. And a leader that never
    /// syncs is out at the rebalance deadline, its parked followers told to
    /// rejoin.
    #[tokio::test]
    async fn sessions_restart_at_completion_and_a_leader_that_never_syncs_is_out() {
        let coordinator = Arc::new(Coordinator::new(GroupConfig {
            initial_rebalance_delay: Duration::from_millis(1_200),
            ..config()
        }));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_secs(4),
        ));
        let mut short = request("g", "", "a");
        short.session_timeout = Duration::from_secs(1);
        short.rebalance_timeout = Duration::from_millis(400);
        let mut other = request("g", "", "b");
        other.rebalance_timeout = Duration::from_millis(400);
        let (a, b) = tokio::join!(coordinator.join(short), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            coordinator.join(other).await
        });
        let Joined::Complete(a) = a.unwrap() else {
            panic!()
        };
        let Joined::Complete(b) = b.unwrap() else {
            panic!()
        };
        // The round outlived a's 1 s session; a is still a member.
        coordinator.sweep(Instant::now());
        assert_eq!(
            coordinator.heartbeat("g", 1, &a.member_id),
            Ok(()),
            "known; a heartbeat while the assignment is awaited is fine, as Kafka has it (audit)"
        );
        assert_eq!(
            coordinator.check_commit("g", 1, &a.member_id),
            Err(ErrorCode::RebalanceInProgress),
            "a commit while the assignment is awaited is not (audit)"
        );
        // The leader never syncs: at the rebalance deadline it is out, and a
        // follower parked on the assignment is told to rejoin.
        let follower = if a.leader == a.member_id { &b } else { &a };
        let parked = coordinator
            .sync("g", 1, &follower.member_id, Vec::new())
            .await;
        assert_eq!(parked, Err(ErrorCode::RebalanceInProgress));
        let leader = if a.leader == a.member_id { &a } else { &b };
        assert_eq!(
            coordinator.heartbeat("g", 1, &leader.member_id),
            Err(ErrorCode::UnknownMemberId),
            "the leader that never synced is out"
        );
        sweeper.abort();
    }

    /// A member whose session lapses is swept out and the group rebalances;
    /// protocols nobody shares are refused; the member cap holds.
    #[tokio::test]
    async fn a_lapsed_session_rebalances_and_the_bounds_hold() {
        let coordinator = Arc::new(Coordinator::new(GroupConfig {
            max_members: 2,
            ..config()
        }));
        let sweeper = tokio::spawn(complete_by_sweeping(
            Arc::clone(&coordinator),
            Duration::from_secs(2),
        ));
        let mut short = request("g", "", "a");
        short.session_timeout = Duration::from_secs(1);
        let (a, b) = tokio::join!(coordinator.join(short), async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            coordinator.join(request("g", "", "b")).await
        });
        let Joined::Complete(a) = a.unwrap() else {
            panic!()
        };
        let Joined::Complete(b) = b.unwrap() else {
            panic!()
        };
        let leader = if a.leader == a.member_id { &a } else { &b };
        coordinator
            .sync("g", 1, &leader.member_id, Vec::new())
            .await
            .unwrap();
        assert_eq!(
            coordinator.join(request("g", "", "c")).await,
            Err(ErrorCode::GroupMaxSizeReached)
        );
        let mut odd = request("g", "", "d");
        odd.protocols = vec![("sticky".to_owned(), Vec::new())];
        // Refused for the protocol before the cap is even consulted? No: the
        // cap is judged first, so drop one member to see the protocol refusal.
        coordinator.leave("g", &b.member_id).unwrap();
        assert_eq!(
            coordinator.join(odd).await,
            Err(ErrorCode::InconsistentGroupProtocol),
            "no protocol in common with the members present"
        );
        // `a` (session 1 s) stops heartbeating: swept out after its session.
        tokio::time::sleep(Duration::from_millis(1_300)).await;
        coordinator.sweep(Instant::now());
        assert_eq!(
            coordinator.heartbeat("g", 1, &a.member_id),
            Err(ErrorCode::UnknownMemberId),
            "swept out"
        );
        sweeper.abort();
    }
}

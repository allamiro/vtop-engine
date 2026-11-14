//! Which metadata commands are audited, and the records the admin endpoint
//! and the state machine hand to the process's auditor (#694).
//!
//! # What is audited
//!
//! An explicit list of operator command kinds ([`command_audit`]), plus every
//! refusal. Not "every cluster-scoped command": that class also holds the
//! consumer-group commands the Kafka gateway proposes at client rate. Not the
//! node-initiated commands either (lease acquire and renew, promotion
//! reports, version reports, the fenced group cursors, a leaseholder's
//! split): they run continuously and are recorded as transitions already.
//! The match in [`command_audit`] has no wildcard arm, so a command added
//! later does not compile until it is put on one side of the list
//! deliberately.
//!
//! # Where a record comes from
//!
//! Each audited command is recorded twice on the cluster plane, joined by the
//! command's request id:
//!
//! * at the **edge**, by the admin endpoint, which knows the caller's verified
//!   certificate identity, the authorization decision and the outcome;
//! * **applied**, by every replica's state-machine wrapper as it applies the
//!   committed entry, with the log index (never from the pure `apply`, which
//!   also runs when a log is replayed at open). Applied records are written
//!   at least once; a reader dedupes on the log index.
//!
//! The actor is in the edge record only: making it durable in the
//! replicated log is a format change of its own (a later issue).
//!
//! # Why a hook
//!
//! This crate is the deterministic state machine and keeps its dependency
//! surface small (no serde). It builds plain [`AuditRecord`]s; the process
//! installs an [`Auditor`] that turns them into its audit records
//! (`vtop-node` writes `vtop_observe::audit` records). With none installed a
//! record is printed as one line on stderr, so it is never silently lost.

use crate::command::MetadataCommand;
use crate::transport::authz::AdminIdentity;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

/// The audit actions of the cluster plane; the same names as
/// `vtop_observe::audit::Action`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditAction {
    ClusterInit,
    ClusterMembership,
    ClusterEnableFeatures,
    ClusterCreateTopic,
    ClusterPropose,
    PromotionAttest,
}

impl AuditAction {
    pub fn as_str(self) -> &'static str {
        match self {
            AuditAction::ClusterInit => "cluster.init",
            AuditAction::ClusterMembership => "cluster.membership",
            AuditAction::ClusterEnableFeatures => "cluster.enable_features",
            AuditAction::ClusterCreateTopic => "cluster.create_topic",
            AuditAction::ClusterPropose => "cluster.propose",
            AuditAction::PromotionAttest => "promotion.attest",
        }
    }
}

/// Where the record was written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditStage {
    /// The admin endpoint, deciding and forwarding the request.
    Edge,
    /// A replica applying the committed entry.
    Applied,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditOutcome {
    Success,
    Failure,
    Refused,
}

/// Who the record is about.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuditActor {
    /// The caller at the edge, as its certificate identifies it.
    Caller(AdminIdentity),
    /// The replica applying the entry: this metadata node, when known.
    Replica(Option<u64>),
}

/// What an audited command carries into its record: its action, its name,
/// what it acts on, and its parameters (an allowlist: identifiers and small
/// values, never a URI, a digest or key material).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandAudit {
    pub action: AuditAction,
    pub command: &'static str,
    pub target_kind: &'static str,
    pub target_id: Option<String>,
    pub params: Vec<(&'static str, String)>,
}

/// One record for the auditor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditRecord {
    pub stage: AuditStage,
    pub action: AuditAction,
    pub actor: AuditActor,
    pub target_kind: &'static str,
    pub target_id: Option<String>,
    pub params: Vec<(&'static str, String)>,
    pub outcome: AuditOutcome,
    /// Why, for a failure or a refusal.
    pub reason: Option<String>,
    pub request_id: Option<Uuid>,
    /// The metadata log index the entry was committed at.
    pub log_index: Option<u64>,
}

impl AuditRecord {
    /// The record for `command` with `audit`'s description.
    pub fn for_command(
        stage: AuditStage,
        actor: AuditActor,
        command: &MetadataCommand,
        audit: CommandAudit,
        outcome: AuditOutcome,
    ) -> Self {
        let mut params = vec![("command", audit.command.to_owned())];
        params.extend(audit.params);
        AuditRecord {
            stage,
            action: audit.action,
            actor,
            target_kind: audit.target_kind,
            target_id: audit.target_id,
            params,
            outcome,
            reason: None,
            request_id: Some(command.envelope().request_id),
            log_index: None,
        }
    }

    /// A one-line form, for the fallback with no auditor installed.
    pub fn describe(&self) -> String {
        let actor = match &self.actor {
            AuditActor::Caller(identity) => identity.describe(),
            AuditActor::Replica(Some(id)) => format!("replica meta node {id}"),
            AuditActor::Replica(None) => "a replica".to_owned(),
        };
        let mut line = format!(
            "audit: {} {} ({:?}) by {actor} on {} {}",
            self.action.as_str(),
            match self.outcome {
                AuditOutcome::Success => "success",
                AuditOutcome::Failure => "failure",
                AuditOutcome::Refused => "refused",
            },
            self.stage,
            self.target_kind,
            self.target_id.as_deref().unwrap_or("-"),
        );
        for (name, value) in &self.params {
            line.push_str(&format!(" {name}={value}"));
        }
        if let Some(id) = self.request_id {
            line.push_str(&format!(" request_id={id}"));
        }
        if let Some(index) = self.log_index {
            line.push_str(&format!(" log_index={index}"));
        }
        if let Some(reason) = &self.reason {
            line.push_str(&format!(": {reason}"));
        }
        line
    }
}

/// The process's sink for cluster audit records.
pub trait Auditor: Send + Sync {
    /// Record it. Never blocks on the network and never fails the caller:
    /// a sink that cannot write counts and reports that itself.
    fn record(&self, record: &AuditRecord);
}

static AUDITOR: RwLock<Option<Arc<dyn Auditor>>> = RwLock::new(None);

/// Install the process's auditor (replacing any earlier one).
pub fn install(auditor: Arc<dyn Auditor>) {
    *AUDITOR.write().unwrap_or_else(|e| e.into_inner()) = Some(auditor);
}

/// Hand `record` to the installed auditor, or print it on stderr.
pub fn emit(record: &AuditRecord) {
    let auditor = AUDITOR.read().unwrap_or_else(|e| e.into_inner()).clone();
    match auditor {
        Some(auditor) => auditor.record(record),
        None => eprintln!("{}", record.describe()),
    }
}

/// The command's name, for records and messages. Exhaustive.
pub fn command_name(command: &MetadataCommand) -> &'static str {
    match command {
        MetadataCommand::RegisterNode { .. } => "RegisterNode",
        MetadataCommand::SetNodeState { .. } => "SetNodeState",
        MetadataCommand::CreateTopic { .. } => "CreateTopic",
        MetadataCommand::GrantRangeLease { .. } => "GrantRangeLease",
        MetadataCommand::ReleaseRangeLease { .. } => "ReleaseRangeLease",
        MetadataCommand::AcquireRangeLease { .. } => "AcquireRangeLease",
        MetadataCommand::RenewRangeLease { .. } => "RenewRangeLease",
        MetadataCommand::ReportPromotionOutcome { .. } => "ReportPromotionOutcome",
        MetadataCommand::SplitRange { .. } => "SplitRange",
        MetadataCommand::RegisterSealedSegment { .. } => "RegisterSealedSegment",
        MetadataCommand::MarkSegmentVerified { .. } => "MarkSegmentVerified",
        MetadataCommand::PutKeyRecord { .. } => "PutKeyRecord",
        MetadataCommand::CreateConsumerGroup { .. } => "CreateConsumerGroup",
        MetadataCommand::JoinConsumerGroup { .. } => "JoinConsumerGroup",
        MetadataCommand::LeaveConsumerGroup { .. } => "LeaveConsumerGroup",
        MetadataCommand::AssignMemberRanges { .. } => "AssignMemberRanges",
        MetadataCommand::CommitGroupCursor { .. } => "CommitGroupCursor",
        MetadataCommand::HeartbeatMember { .. } => "HeartbeatMember",
        MetadataCommand::EnsureGroupMemberForRange { .. } => "EnsureGroupMemberForRange",
        MetadataCommand::CommitGroupCursorFenced { .. } => "CommitGroupCursorFenced",
        MetadataCommand::CommitGroupCursorCoordinated { .. } => "CommitGroupCursorCoordinated",
        MetadataCommand::EnsureGroupMemberCoordinated { .. } => "EnsureGroupMemberCoordinated",
        MetadataCommand::ExpireStaleMember { .. } => "ExpireStaleMember",
        MetadataCommand::SetNodePlacementAttrs { .. } => "SetNodePlacementAttrs",
        MetadataCommand::CommitSegmentPlacement { .. } => "CommitSegmentPlacement",
        MetadataCommand::CommitReplacementProof { .. } => "CommitReplacementProof",
        MetadataCommand::PlanReplicaRetirement { .. } => "PlanReplicaRetirement",
        MetadataCommand::ConfirmReplicaRetired { .. } => "ConfirmReplicaRetired",
        MetadataCommand::ProposeRebalance { .. } => "ProposeRebalance",
        MetadataCommand::CancelRebalance { .. } => "CancelRebalance",
        MetadataCommand::CommitTierEvidence { .. } => "CommitTierEvidence",
        MetadataCommand::SetTopicRetentionPolicy { .. } => "SetTopicRetentionPolicy",
        MetadataCommand::PlanRetention { .. } => "PlanRetention",
        MetadataCommand::ConfirmRetentionExpired { .. } => "ConfirmRetentionExpired",
        MetadataCommand::CancelRetention { .. } => "CancelRetention",
        MetadataCommand::SetFeatureLevel { .. } => "SetFeatureLevel",
        MetadataCommand::SetRangeReplicaSet { .. } => "SetRangeReplicaSet",
        MetadataCommand::AttestUnprovenPromotion { .. } => "AttestUnprovenPromotion",
        MetadataCommand::ReportNodeVersion { .. } => "ReportNodeVersion",
    }
}

fn range_target(topic: &Uuid, range: &Uuid) -> Option<String> {
    Some(format!("{topic}/{range}"))
}

/// Whether `command` is audited, and what its record carries. Exhaustive,
/// with no wildcard: see the module docs.
pub fn command_audit(command: &MetadataCommand) -> Option<CommandAudit> {
    let name = command_name(command);
    let propose = |target_kind: &'static str,
                   target_id: Option<String>,
                   params: Vec<(&'static str, String)>| {
        Some(CommandAudit {
            action: AuditAction::ClusterPropose,
            command: name,
            target_kind,
            target_id,
            params,
        })
    };
    match command {
        // ---- Audited: what an operator asks the cluster to become ----
        MetadataCommand::SetFeatureLevel { feature_level, .. } => Some(CommandAudit {
            action: AuditAction::ClusterEnableFeatures,
            command: name,
            target_kind: "cluster",
            target_id: None,
            params: vec![("level", feature_level.to_string())],
        }),
        MetadataCommand::CreateTopic {
            name: topic,
            topic_uuid,
            root_range_uuid,
            ..
        } => Some(CommandAudit {
            action: AuditAction::ClusterCreateTopic,
            command: name,
            target_kind: "topic",
            target_id: Some(topic_uuid.to_string()),
            params: vec![
                ("name", topic.clone()),
                ("topic_uuid", topic_uuid.to_string()),
                ("root_range_uuid", root_range_uuid.to_string()),
            ],
        }),
        MetadataCommand::AttestUnprovenPromotion {
            topic_uuid,
            range_uuid,
            replica_node_uuid,
            operator,
            ..
        } => Some(CommandAudit {
            action: AuditAction::PromotionAttest,
            command: name,
            target_kind: "range",
            target_id: range_target(topic_uuid, range_uuid),
            params: vec![
                ("topic_uuid", topic_uuid.to_string()),
                ("range_uuid", range_uuid.to_string()),
                ("replica_node_uuid", replica_node_uuid.to_string()),
                ("operator", operator.clone()),
            ],
        }),
        MetadataCommand::RegisterNode { node_uuid, .. } => propose(
            "node",
            Some(node_uuid.to_string()),
            vec![("node_uuid", node_uuid.to_string())],
        ),
        MetadataCommand::SetNodeState {
            node_uuid, state, ..
        } => propose(
            "node",
            Some(node_uuid.to_string()),
            vec![
                ("node_uuid", node_uuid.to_string()),
                ("state", state.as_str().to_owned()),
            ],
        ),
        MetadataCommand::SetNodePlacementAttrs {
            node_uuid,
            failure_domain,
            ..
        } => propose(
            "node",
            Some(node_uuid.to_string()),
            vec![
                ("node_uuid", node_uuid.to_string()),
                ("failure_domain", failure_domain.clone()),
            ],
        ),
        MetadataCommand::GrantRangeLease {
            topic_uuid,
            range_uuid,
            holder_node_uuid,
            ..
        } => propose(
            "range",
            range_target(topic_uuid, range_uuid),
            vec![
                ("topic_uuid", topic_uuid.to_string()),
                ("range_uuid", range_uuid.to_string()),
                ("node_uuid", holder_node_uuid.to_string()),
            ],
        ),
        // Proposed by the lease agent when a node drains, as well as by an
        // operator: rare either way, and it hands a range's leadership back.
        MetadataCommand::ReleaseRangeLease {
            topic_uuid,
            range_uuid,
            expected_fencing_epoch,
            ..
        } => propose(
            "range",
            range_target(topic_uuid, range_uuid),
            vec![
                ("topic_uuid", topic_uuid.to_string()),
                ("range_uuid", range_uuid.to_string()),
                ("fencing_epoch", expected_fencing_epoch.to_string()),
            ],
        ),
        MetadataCommand::SetRangeReplicaSet {
            topic_uuid,
            range_uuid,
            members,
            ..
        } => propose(
            "range",
            range_target(topic_uuid, range_uuid),
            vec![
                ("topic_uuid", topic_uuid.to_string()),
                ("range_uuid", range_uuid.to_string()),
                ("members", join(members)),
            ],
        ),
        MetadataCommand::SetTopicRetentionPolicy {
            topic_uuid,
            unarchived_deletion_allowed,
            ..
        } => propose(
            "topic",
            Some(topic_uuid.to_string()),
            vec![
                ("topic_uuid", topic_uuid.to_string()),
                (
                    "unarchived_deletion_allowed",
                    unarchived_deletion_allowed.to_string(),
                ),
            ],
        ),
        // The PutKeyRecord's identifier only: the public material's digest
        // is not for a record.
        MetadataCommand::PutKeyRecord { key_uuid, .. } => propose(
            "key",
            Some(key_uuid.to_string()),
            vec![("key_id", key_uuid.to_string())],
        ),
        MetadataCommand::ProposeRebalance {
            topic_uuid,
            range_uuid,
            segment_uuid,
            from_node_uuid,
            to_node_uuid,
            ..
        } => propose(
            "segment",
            Some(segment_uuid.to_string()),
            vec![
                ("topic_uuid", topic_uuid.to_string()),
                ("range_uuid", range_uuid.to_string()),
                ("segment_uuid", segment_uuid.to_string()),
                ("from_node", from_node_uuid.to_string()),
                ("to_node", to_node_uuid.to_string()),
            ],
        ),
        MetadataCommand::PlanReplicaRetirement {
            topic_uuid,
            range_uuid,
            segment_uuid,
            retiring_node_uuid,
            ..
        }
        | MetadataCommand::ConfirmReplicaRetired {
            topic_uuid,
            range_uuid,
            segment_uuid,
            retiring_node_uuid,
            ..
        } => propose(
            "segment",
            Some(segment_uuid.to_string()),
            vec![
                ("topic_uuid", topic_uuid.to_string()),
                ("range_uuid", range_uuid.to_string()),
                ("segment_uuid", segment_uuid.to_string()),
                ("node_uuid", retiring_node_uuid.to_string()),
            ],
        ),
        MetadataCommand::CommitReplacementProof {
            topic_uuid,
            range_uuid,
            segment_uuid,
            source_node_uuid,
            destination_node_uuid,
            ..
        } => propose(
            "segment",
            Some(segment_uuid.to_string()),
            vec![
                ("topic_uuid", topic_uuid.to_string()),
                ("range_uuid", range_uuid.to_string()),
                ("segment_uuid", segment_uuid.to_string()),
                ("from_node", source_node_uuid.to_string()),
                ("to_node", destination_node_uuid.to_string()),
            ],
        ),
        MetadataCommand::CommitSegmentPlacement {
            topic_uuid,
            range_uuid,
            segment_uuid,
            replica_nodes: members,
            ..
        } => propose(
            "segment",
            Some(segment_uuid.to_string()),
            vec![
                ("topic_uuid", topic_uuid.to_string()),
                ("range_uuid", range_uuid.to_string()),
                ("segment_uuid", segment_uuid.to_string()),
                ("members", join(members)),
            ],
        ),
        // Segment registration, verification and tier evidence are proposed
        // by `vtopctl meta` and `vtopctl tier`, at operator rate. The tier
        // evidence's object URI is never recorded (it can carry a query).
        MetadataCommand::RegisterSealedSegment {
            topic_uuid,
            range_uuid,
            segment_uuid,
            ..
        }
        | MetadataCommand::MarkSegmentVerified {
            topic_uuid,
            range_uuid,
            segment_uuid,
            ..
        }
        | MetadataCommand::CancelRebalance {
            topic_uuid,
            range_uuid,
            segment_uuid,
            ..
        }
        | MetadataCommand::CommitTierEvidence {
            topic_uuid,
            range_uuid,
            segment_uuid,
            ..
        }
        | MetadataCommand::PlanRetention {
            topic_uuid,
            range_uuid,
            segment_uuid,
            ..
        }
        | MetadataCommand::ConfirmRetentionExpired {
            topic_uuid,
            range_uuid,
            segment_uuid,
            ..
        }
        | MetadataCommand::CancelRetention {
            topic_uuid,
            range_uuid,
            segment_uuid,
            ..
        } => propose(
            "segment",
            Some(segment_uuid.to_string()),
            vec![
                ("topic_uuid", topic_uuid.to_string()),
                ("range_uuid", range_uuid.to_string()),
                ("segment_uuid", segment_uuid.to_string()),
            ],
        ),

        // ---- Not audited: node-initiated, continuous, or at client rate ----
        // A node's own lease, its promotion report, its version report and a
        // leaseholder's split: proposed by the nodes themselves, all the
        // time, and recorded as range transitions already.
        MetadataCommand::AcquireRangeLease { .. }
        | MetadataCommand::RenewRangeLease { .. }
        | MetadataCommand::ReportPromotionOutcome { .. }
        | MetadataCommand::ReportNodeVersion { .. }
        | MetadataCommand::SplitRange { .. }
        // The consumer-group commands: the Kafka gateway proposes them for
        // its clients, at client rate.
        | MetadataCommand::CreateConsumerGroup { .. }
        | MetadataCommand::JoinConsumerGroup { .. }
        | MetadataCommand::LeaveConsumerGroup { .. }
        | MetadataCommand::AssignMemberRanges { .. }
        | MetadataCommand::CommitGroupCursor { .. }
        | MetadataCommand::HeartbeatMember { .. }
        | MetadataCommand::EnsureGroupMemberForRange { .. }
        | MetadataCommand::CommitGroupCursorFenced { .. }
        | MetadataCommand::CommitGroupCursorCoordinated { .. }
        | MetadataCommand::EnsureGroupMemberCoordinated { .. }
        | MetadataCommand::ExpireStaleMember { .. } => None,
    }
}

/// The record for a refused request, whatever the command: a refusal is
/// audited even for a command whose success is not.
pub fn refusal(identity: &AdminIdentity, command: &MetadataCommand, reason: String) -> AuditRecord {
    let mut record = match command_audit(command) {
        Some(audit) => AuditRecord::for_command(
            AuditStage::Edge,
            AuditActor::Caller(identity.clone()),
            command,
            audit,
            AuditOutcome::Refused,
        ),
        None => AuditRecord {
            stage: AuditStage::Edge,
            action: AuditAction::ClusterPropose,
            actor: AuditActor::Caller(identity.clone()),
            target_kind: "cluster",
            target_id: None,
            params: vec![("command", command_name(command).to_owned())],
            outcome: AuditOutcome::Refused,
            reason: None,
            request_id: Some(command.envelope().request_id),
            log_index: None,
        },
    };
    record.reason = Some(reason);
    record
}

fn join(nodes: &[Uuid]) -> String {
    nodes
        .iter()
        .map(Uuid::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// A recording auditor for tests: keeps every record. Install one and pick
/// the records of your own request ids; the auditor is process-wide.
#[derive(Default)]
pub struct Recording(std::sync::Mutex<Vec<AuditRecord>>);

impl Recording {
    /// The process's recording auditor, installed on first use.
    pub fn installed() -> Arc<Recording> {
        static RECORDING: std::sync::OnceLock<Arc<Recording>> = std::sync::OnceLock::new();
        RECORDING
            .get_or_init(|| {
                let recording = Arc::new(Recording::default());
                install(recording.clone());
                recording
            })
            .clone()
    }

    /// The records carrying `request_id`.
    pub fn for_request(&self, request_id: Uuid) -> Vec<AuditRecord> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|r| r.request_id == Some(request_id))
            .cloned()
            .collect()
    }

    /// Every record so far.
    pub fn all(&self) -> Vec<AuditRecord> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl Auditor for Recording {
    fn record(&self, record: &AuditRecord) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(record.clone());
    }
}

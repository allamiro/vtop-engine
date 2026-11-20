//! The node's audit records (#694): the metadata plane's edge and applied
//! records, and the serving side of a sealed-segment transfer, written as
//! `vtop_observe::audit` records to the log stream and, when
//! `VTOP_AUDIT_LOG` names one, the node's audit file.

use std::sync::Arc;
use uuid::Uuid;
use vtop_meta::audit::{AuditAction, AuditActor, AuditOutcome, AuditRecord, AuditStage, Auditor};
use vtop_meta::transport::authz::AdminIdentity;
use vtop_observe::audit::{Action, Actor, ActorKind, AuditEvent, Outcome, Stage, Target};

/// Writes the metadata plane's records for this node.
pub struct NodeAuditor {
    /// This process, named in each record (`meta-<id>`).
    engine: String,
}

impl NodeAuditor {
    pub fn new(engine: impl Into<String>) -> Self {
        Self {
            engine: engine.into(),
        }
    }
}

impl Auditor for NodeAuditor {
    fn record(&self, record: &AuditRecord) {
        // A file sink that cannot be written is logged and counted by the
        // sink, and the record is in the log stream: the command already
        // ran (or was refused) and the node goes on serving.
        let _ = vtop_observe::audit::record(&to_event(record, &self.engine));
    }
}

/// Install this node's auditor for the metadata plane.
pub fn install_meta(node_id: u64) {
    vtop_meta::audit::install(Arc::new(NodeAuditor::new(format!("meta-{node_id}"))));
}

fn action(action: AuditAction) -> Action {
    match action {
        AuditAction::ClusterInit => Action::ClusterInit,
        AuditAction::ClusterMembership => Action::ClusterMembership,
        AuditAction::ClusterEnableFeatures => Action::ClusterEnableFeatures,
        AuditAction::ClusterCreateTopic => Action::ClusterCreateTopic,
        AuditAction::ClusterPropose => Action::ClusterPropose,
        AuditAction::PromotionAttest => Action::PromotionAttest,
    }
}

fn actor(actor: &AuditActor) -> Actor {
    match actor {
        AuditActor::Caller(AdminIdentity::Named(cn)) => {
            Actor::certificate(ActorKind::Certificate, cn)
        }
        AuditActor::Caller(AdminIdentity::Node(uuid)) => {
            Actor::certificate(ActorKind::Node, &uuid.to_string())
        }
        AuditActor::Caller(AdminIdentity::MetaNode(id)) => {
            Actor::certificate(ActorKind::Node, &format!("meta-{id}"))
        }
        AuditActor::Caller(AdminIdentity::Anonymous) => Actor::anonymous(),
        AuditActor::Replica(Some(id)) => Actor::replica(&format!("meta-{id}")),
        AuditActor::Replica(None) => Actor::replica("meta-unknown"),
    }
}

/// The `vtop_observe` record for a metadata-plane record. Parameters off the
/// action's allowlist are dropped (the list in `vtop_observe::audit` is the
/// contract; the test below checks none is dropped today).
pub fn to_event(record: &AuditRecord, engine: &str) -> AuditEvent {
    let action = action(record.action);
    let mut event = AuditEvent::new(
        action,
        match record.stage {
            AuditStage::Edge => Stage::Edge,
            AuditStage::Applied => Stage::Applied,
        },
        actor(&record.actor),
        Target::new(record.target_kind, record.target_id.clone()),
        match record.outcome {
            AuditOutcome::Success => Outcome::Success,
            AuditOutcome::Failure => Outcome::Failure,
            AuditOutcome::Refused => Outcome::Refused,
        },
    )
    .engine(engine);
    for (name, value) in &record.params {
        if action.params().contains(name) {
            event = event.param(name, value);
        }
    }
    if let Some(reason) = &record.reason {
        event = event.reason(reason);
    }
    if let Some(id) = record.request_id {
        event = event.request_id(id);
    }
    if let Some(index) = record.log_index {
        event = event.log_index(index);
    }
    event
}

/// The serving leader's record of a sealed-segment transfer request
/// (`vtopctl node repair` on the other end): who asked, from where, and
/// whether this leader let them. `verified` is whether the peer's identity
/// came from a certificate the replica plane checked, or was only declared
/// on a plaintext plane.
pub fn transfer(
    leader: Uuid,
    peer: Uuid,
    verified: bool,
    request: &str,
    range: Option<String>,
    outcome: Outcome,
    reason: Option<&str>,
) {
    let actor = if verified {
        Actor::certificate(ActorKind::Node, &peer.to_string())
    } else {
        Actor::claimed_node(&peer.to_string())
    };
    let mut event = AuditEvent::new(
        Action::NodeRepair,
        Stage::Edge,
        actor,
        Target::new("range", range),
        outcome,
    )
    .engine(leader)
    .param("side", "serving")
    .param("request", request)
    .param("from", leader);
    if let Some(reason) = reason {
        event = event.reason(reason);
    }
    let _ = vtop_observe::audit::record(&event);
}

#[cfg(test)]
mod tests {
    use super::*;
    use vtop_meta::{CommandEnvelope, MetadataCommand};

    fn env() -> CommandEnvelope {
        CommandEnvelope {
            request_id: Uuid::from_u128(0x694),
            issued_at_ms: 0,
        }
    }

    /// Every parameter the metadata plane hands over is on the record's
    /// allowlist, so the conversion drops nothing.
    #[test]
    fn no_metadata_parameter_is_dropped() {
        let commands = [
            MetadataCommand::SetFeatureLevel {
                env: env(),
                feature_level: 8,
            },
            MetadataCommand::CreateTopic {
                env: env(),
                name: "events".into(),
                topic_uuid: Uuid::from_u128(1),
                root_range_uuid: Uuid::from_u128(2),
            },
            MetadataCommand::AttestUnprovenPromotion {
                env: env(),
                topic_uuid: Uuid::from_u128(1),
                range_uuid: Uuid::from_u128(2),
                replica_node_uuid: Uuid::from_u128(3),
                operator: "ops".into(),
            },
            MetadataCommand::SetNodeState {
                env: env(),
                node_uuid: Uuid::from_u128(3),
                state: vtop_meta::NodeState::Draining,
                expected_generation: 1,
            },
            MetadataCommand::ProposeRebalance {
                env: env(),
                topic_uuid: Uuid::from_u128(1),
                range_uuid: Uuid::from_u128(2),
                segment_uuid: Uuid::from_u128(4),
                from_node_uuid: Uuid::from_u128(5),
                to_node_uuid: Uuid::from_u128(6),
                expected_placement_generation: 1,
            },
            MetadataCommand::SetRangeReplicaSet {
                env: env(),
                topic_uuid: Uuid::from_u128(1),
                range_uuid: Uuid::from_u128(2),
                members: vec![Uuid::from_u128(5), Uuid::from_u128(6)],
                expected_generation: 1,
            },
            MetadataCommand::SetTopicRetentionPolicy {
                env: env(),
                topic_uuid: Uuid::from_u128(1),
                unarchived_deletion_allowed: true,
                expected_generation: None,
            },
            MetadataCommand::SetNodePlacementAttrs {
                env: env(),
                node_uuid: Uuid::from_u128(3),
                failure_domain: "rack-1".into(),
                placement_weight: 1,
                expected_generation: 1,
            },
            MetadataCommand::ReleaseRangeLease {
                env: env(),
                topic_uuid: Uuid::from_u128(1),
                range_uuid: Uuid::from_u128(2),
                expected_fencing_epoch: 4,
            },
        ];
        for command in &commands {
            let audit = vtop_meta::audit::command_audit(command).expect("audited");
            let record = AuditRecord::for_command(
                AuditStage::Edge,
                AuditActor::Caller(AdminIdentity::Named("ops".into())),
                command,
                audit,
                AuditOutcome::Success,
            );
            let event = to_event(&record, "meta-1");
            for (name, _) in &record.params {
                assert!(
                    event.params.contains_key(*name),
                    "{} param {name} is not on {}'s allowlist",
                    vtop_meta::audit::command_name(command),
                    event.action
                );
            }
            assert_eq!(
                event.correlation.request_id.as_deref(),
                Some(&*Uuid::from_u128(0x694).to_string())
            );
        }
        for (action, params) in [
            (
                AuditAction::ClusterInit,
                vec![("members", "1,2,3".to_owned())],
            ),
            (
                AuditAction::ClusterMembership,
                vec![
                    ("operation", "change_membership".to_owned()),
                    ("voters", "1,2".to_owned()),
                    ("retain_removed_as_learners", "false".to_owned()),
                    ("node_id", "4".to_owned()),
                ],
            ),
        ] {
            let record = AuditRecord {
                stage: AuditStage::Edge,
                action,
                actor: AuditActor::Caller(AdminIdentity::Anonymous),
                target_kind: "cluster",
                target_id: None,
                params: params.clone(),
                outcome: AuditOutcome::Refused,
                reason: Some("no".into()),
                request_id: None,
                log_index: None,
            };
            let event = to_event(&record, "meta-1");
            assert_eq!(event.params.len(), params.len());
            assert_eq!(event.actor.kind, ActorKind::Anonymous);
        }
    }

    #[test]
    fn identities_become_actors_with_their_source() {
        let named = actor(&AuditActor::Caller(AdminIdentity::Named("ops-bob".into())));
        assert_eq!(named.kind, ActorKind::Certificate);
        assert_eq!(named.source, "certificate");
        assert_eq!(named.name.as_deref(), Some("ops-bob"));
        let replica = actor(&AuditActor::Replica(Some(2)));
        assert_eq!(replica.source, "raft");
        assert_eq!(replica.name.as_deref(), Some("meta-2"));
        assert_eq!(
            actor(&AuditActor::Caller(AdminIdentity::Anonymous)).source,
            "none"
        );
    }

    /// The serving side of `vtopctl node repair` records the peer that asked,
    /// as its certificate names it (or as it only declared itself on a
    /// plaintext plane), and a refusal with its reason.
    #[test]
    fn the_serving_leader_records_the_peer_identity() {
        #[derive(Clone, Default)]
        struct Captured(Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Captured {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
            type Writer = Captured;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }
        use tracing_subscriber::layer::SubscriberExt;
        let captured = Captured::default();
        let layer = vtop_observe::logging::log_layer_with_writer(
            "error",
            None,
            true,
            false,
            captured.clone(),
        );
        let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        let leader = Uuid::from_u128(0x1ead);
        let peer = Uuid::from_u128(0xbee);
        transfer(
            leader,
            peer,
            true,
            "list_sealed_segments",
            Some("events/r1".into()),
            Outcome::Refused,
            Some("not authorized to pull sealed segments"),
        );
        transfer(
            leader,
            peer,
            false,
            "seal_tail",
            None,
            Outcome::Success,
            None,
        );
        let text = String::from_utf8_lossy(&captured.0.lock().unwrap()).into_owned();
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 2, "{text}");
        assert!(
            lines[0].contains("\"outcome\":\"refused\"")
                && lines[0].contains(&format!("node {peer}"))
                && lines[0].contains("not authorized to pull"),
            "{}",
            lines[0]
        );
        assert!(
            lines[1].contains("(declared, not verified)"),
            "a plaintext peer's identity is a claim: {}",
            lines[1]
        );
    }
}

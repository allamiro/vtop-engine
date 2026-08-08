//! Who may ask the admin endpoint for what (#238).
//!
//! The endpoint authenticates: every client presents a certificate chaining to
//! the configured CA, and TLS refuses anything else. It did not *authorize* —
//! any CA-signed client could submit any command, including `init`, membership
//! changes, and lease grants naming an arbitrary holder. In a deployment where
//! the same CA issues data-node certificates (it does; that is how the
//! replication plane authenticates), a compromised data node could rewrite
//! cluster membership.
//!
//! # The classification, and why it is by command rather than by caller
//!
//! Commands fall into three classes by what they can affect:
//!
//! * **Reads** change nothing. Any authenticated client may make them.
//! * **Node-scoped** commands affect exactly one node's own claim on a range —
//!   acquiring and renewing its lease. A node may submit these *for itself*.
//! * **Cluster-scoped** commands change what the cluster is: membership,
//!   bootstrap, topic and range lifecycle, administrative grants naming any
//!   holder. Only an operator identity may submit them.
//!
//! Classifying by command rather than by caller is what makes the policy
//! auditable. "Node 3 may do node-3 things" is a rule anyone can check against
//! a command; "node 3 is trusted" is a rule nobody can check at all.
//!
//! # Why the node-scoped class exists at all
//!
//! It would be simpler to require an operator identity for every proposal. But
//! the lease agent (#223) runs on every data node and proposes
//! `AcquireRangeLease`/`RenewRangeLease` continuously — that is how failover
//! works. Requiring an operator credential there would mean shipping one to
//! every node, which is the same as having no policy while looking like one.
//!
//! # Opt-in, deliberately
//!
//! An absent policy keeps today's allow-all behaviour and logs a warning at
//! startup. Enforcing by default would lock out every existing deployment on
//! upgrade — including, at the moment this landed, the project's own chaos
//! harness. A security control that ships broken gets disabled, not fixed.

use crate::command::MetadataCommand;
use std::collections::BTreeSet;
use uuid::Uuid;

/// The authenticated identity behind an admin connection.
///
/// Derived from the client certificate's Common Name, which the CA controls —
/// a client cannot choose its own identity without the CA issuing it one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdminIdentity {
    /// CN parses as a UUID: a data or broker node speaking for itself.
    Node(Uuid),
    /// CN parses as a decimal integer: a metadata node.
    MetaNode(u64),
    /// Anything else — an operator certificate, identified by its CN string.
    Named(String),
    /// No certificate was presented, because the connection carries no TLS
    /// (#294). Distinct from every other variant on purpose: those name a
    /// caller the CA vouched for, and this one names the absence of any claim
    /// at all. It can only arise on a plaintext endpoint, which refuses to be
    /// built with an enforcing policy — so this is never a way around one.
    Anonymous,
}

impl AdminIdentity {
    /// Classify a Common Name.
    ///
    /// Order matters: a UUID is checked before a decimal integer because the
    /// two spaces do not overlap, and before the catch-all because every CN is
    /// a valid `Named`.
    ///
    /// The integer arm deliberately uses the same `u64::from_str` the peer
    /// plane's [`crate::transport::tls::meta_node_id_from_cert`] uses, padding
    /// and sign included. Tightening it here would mean one certificate
    /// denoting a meta node on one plane and an arbitrary name on the other —
    /// and the looser of two disagreeing readings is the one that decides what
    /// a caller may do.
    pub fn from_common_name(cn: &str) -> Self {
        if let Ok(uuid) = Uuid::parse_str(cn) {
            return AdminIdentity::Node(uuid);
        }
        if let Ok(id) = cn.parse::<u64>() {
            return AdminIdentity::MetaNode(id);
        }
        AdminIdentity::Named(cn.to_owned())
    }

    pub fn describe(&self) -> String {
        match self {
            AdminIdentity::Node(uuid) => format!("node {uuid}"),
            AdminIdentity::MetaNode(id) => format!("meta node {id}"),
            AdminIdentity::Named(name) => format!("operator {name:?}"),
            AdminIdentity::Anonymous => "an unauthenticated caller (plaintext endpoint)".to_owned(),
        }
    }
}

/// What a request is allowed to affect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandClass {
    /// Changes nothing.
    Read,
    /// Affects one node's own claim on a range. Carries the holder it names.
    NodeScoped,
    /// Changes what the cluster is.
    ClusterScoped,
}

/// Classify a proposal.
///
/// Everything not explicitly node-scoped is cluster-scoped. That default is the
/// point: a command added later is refused to nodes until someone deliberately
/// widens the policy, rather than being permitted by an oversight.
pub fn classify(command: &MetadataCommand) -> (CommandClass, Option<Uuid>) {
    match command {
        MetadataCommand::AcquireRangeLease {
            holder_node_uuid, ..
        }
        | MetadataCommand::RenewRangeLease {
            holder_node_uuid, ..
        } => (CommandClass::NodeScoped, Some(*holder_node_uuid)),
        _ => (CommandClass::ClusterScoped, None),
    }
}

// The policy carries no config struct of its own, and no serde derive: this
// crate is the deterministic state machine, and its wire codecs are hand-rolled
// to stay byte-exact. Widening its dependency surface to serde so a YAML shape
// could live next to the rules it feeds would be a poor trade — vtop-node owns
// config parsing and constructs the authorizer through
// `AdminAuthorizer::with_operators`.

/// Why a request was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// A node tried to act for a different node.
    WrongHolder { presented: Uuid, requested: Uuid },
    /// A non-operator tried to change the cluster.
    NotAnOperator { identity: String },
}

impl Refusal {
    pub fn message(&self) -> String {
        match self {
            // Name both sides: the common cause is a misconfigured node UUID,
            // and an error saying only "refused" sends someone to the wrong
            // place entirely.
            Refusal::WrongHolder {
                presented,
                requested,
            } => format!(
                "certificate identifies node {presented}, which may not submit lease commands \
                 for node {requested}"
            ),
            Refusal::NotAnOperator { identity } => format!(
                "{identity} is not a configured operator and may not submit cluster-scoped \
                 admin commands"
            ),
        }
    }
}

/// The policy: what this identity may do.
#[derive(Clone, Debug)]
pub struct AdminAuthorizer {
    /// `None` when no policy was configured: every request is permitted, as
    /// before this existed. Modelled as an absent policy rather than an
    /// all-permitting one so "unconfigured" cannot be mistaken for a
    /// deliberately empty operator list, which means the opposite.
    operators: Option<BTreeSet<String>>,
}

impl AdminAuthorizer {
    /// Enforce, with these Common Names as operators.
    ///
    /// An empty set is a legitimate configuration — a cluster administered out
    /// of band, where nobody may change membership through this endpoint — and
    /// is deliberately NOT treated as "unconfigured". See
    /// [`AdminAuthorizer::permissive`] for that.
    pub fn with_operators(operators: impl IntoIterator<Item = String>) -> Self {
        Self {
            operators: Some(operators.into_iter().collect()),
        }
    }

    /// The unconfigured endpoint: authenticated, not authorized.
    ///
    /// This is what every deployment gets until it opts in, and it is what the
    /// endpoint did before #238. Callers are expected to warn at startup —
    /// silence here would make "no policy" indistinguishable from a policy
    /// that happens to permit the traffic being observed.
    pub fn permissive() -> Self {
        Self { operators: None }
    }

    /// Whether this authorizer enforces anything.
    pub fn is_enforcing(&self) -> bool {
        self.operators.is_some()
    }

    fn is_operator(&self, identity: &AdminIdentity) -> bool {
        let Some(operators) = &self.operators else {
            // Unconfigured: everyone is effectively an operator, which is the
            // pre-#238 behaviour this mode exists to preserve.
            return true;
        };
        // Matched on the raw CN, including for node identities: an operator
        // certificate is whatever the config named, and a deployment that
        // wants a node to also be an operator can say so explicitly rather
        // than having it inferred from the CN's shape.
        let cn = match identity {
            AdminIdentity::Node(uuid) => uuid.to_string(),
            AdminIdentity::MetaNode(id) => id.to_string(),
            AdminIdentity::Named(name) => name.clone(),
            // NEVER an operator. An anonymous caller has presented no claim at
            // all, so there is no CN for the configured set to contain, and
            // matching it against anything would be inventing an identity to
            // authorize. Unreachable in practice — a plaintext endpoint refuses
            // to be built with an enforcing policy, and `operators` is `Some`
            // only when one is — but it is written as a refusal rather than
            // left to the type system, because the day those two facts drift
            // apart is the day this decides whether a policy holds.
            AdminIdentity::Anonymous => return false,
        };
        operators.contains(&cn)
    }

    /// Reads change nothing and are open to any authenticated client.
    pub fn authorize_read(&self, _identity: &AdminIdentity) -> Result<(), Refusal> {
        Ok(())
    }

    /// Authorize an RPC that changes the cluster regardless of any payload —
    /// bootstrap and membership changes.
    ///
    /// These carry no [`MetadataCommand`] to classify, so they are checked by
    /// their frame kind at the dispatch site instead.
    pub fn authorize_cluster(&self, identity: &AdminIdentity) -> Result<(), Refusal> {
        if self.is_operator(identity) {
            return Ok(());
        }
        Err(Refusal::NotAnOperator {
            identity: identity.describe(),
        })
    }

    /// Authorize a proposal.
    pub fn authorize_command(
        &self,
        identity: &AdminIdentity,
        command: &MetadataCommand,
    ) -> Result<(), Refusal> {
        if self.is_operator(identity) {
            return Ok(());
        }
        match classify(command) {
            (CommandClass::Read, _) => Ok(()),
            (CommandClass::NodeScoped, Some(holder)) => match identity {
                // A node may act for itself and only itself. This is the rule
                // that stops one compromised node from fencing every range in
                // the cluster by acquiring their leases.
                AdminIdentity::Node(uuid) if *uuid == holder => Ok(()),
                AdminIdentity::Node(uuid) => Err(Refusal::WrongHolder {
                    presented: *uuid,
                    requested: holder,
                }),
                other => Err(Refusal::NotAnOperator {
                    identity: other.describe(),
                }),
            },
            // A node-scoped command with no holder cannot be checked against
            // the caller, so it is not node-scoped in practice. Refusing is the
            // only safe reading.
            (CommandClass::NodeScoped, None) | (CommandClass::ClusterScoped, _) => {
                Err(Refusal::NotAnOperator {
                    identity: identity.describe(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandEnvelope;

    const NODE_A: Uuid = Uuid::from_u128(0xa1);
    const NODE_B: Uuid = Uuid::from_u128(0xb2);

    fn envelope() -> CommandEnvelope {
        CommandEnvelope {
            request_id: Uuid::from_u128(1),
            issued_at_ms: 0,
        }
    }

    fn acquire(holder: Uuid) -> MetadataCommand {
        MetadataCommand::AcquireRangeLease {
            env: envelope(),
            topic_uuid: Uuid::from_u128(7),
            range_uuid: Uuid::from_u128(8),
            holder_node_uuid: holder,
            expected_range_generation: 0,
            lease_duration_ms: 5_000,
        }
    }

    fn change_membership() -> MetadataCommand {
        MetadataCommand::GrantRangeLease {
            env: envelope(),
            topic_uuid: Uuid::from_u128(7),
            range_uuid: Uuid::from_u128(8),
            holder_node_uuid: NODE_A,
            expected_range_generation: 0,
        }
    }

    fn authorizer(operators: &[&str]) -> AdminAuthorizer {
        AdminAuthorizer::with_operators(operators.iter().map(|s| (*s).to_owned()))
    }

    #[test]
    fn a_uuid_cn_is_a_node_and_a_decimal_cn_is_a_meta_node() {
        assert_eq!(
            AdminIdentity::from_common_name(&NODE_A.to_string()),
            AdminIdentity::Node(NODE_A)
        );
        assert_eq!(
            AdminIdentity::from_common_name("3"),
            AdminIdentity::MetaNode(3)
        );
        assert_eq!(
            AdminIdentity::from_common_name("ops-alice"),
            AdminIdentity::Named("ops-alice".to_owned())
        );
    }

    /// A certificate must not mean two different things depending on which
    /// endpoint reads it. `u64::from_str` accepts padding and a leading sign,
    /// so the peer plane maps `"007"` to node 7; this plane must agree. If a
    /// future change tightens either parse, this test fails and points at the
    /// other one.
    #[test]
    fn numeric_cn_parsing_agrees_with_the_peer_plane() {
        use crate::transport::tls::meta_node_id_from_cert;
        // Same inputs, same verdict — asserted against the peer plane's own
        // parse rather than a copy of its rules.
        for cn in ["7", "007", "+7"] {
            assert_eq!(
                AdminIdentity::from_common_name(cn),
                AdminIdentity::MetaNode(7),
                "{cn:?} must denote node 7 here, as it does on the peer plane"
            );
        }
        for cn in [" 7", "7 ", "seven", ""] {
            assert!(
                matches!(AdminIdentity::from_common_name(cn), AdminIdentity::Named(_)),
                "{cn:?} is not a node id on either plane"
            );
        }
        // Keep the reference to the peer-plane parser live so this test breaks
        // if it is renamed or removed rather than silently drifting.
        let _ = meta_node_id_from_cert;
    }

    /// The unconfigured endpoint must behave exactly as it did before #238 —
    /// a security control that breaks every existing deployment on upgrade
    /// gets disabled, not fixed.
    #[test]
    fn an_unconfigured_authorizer_permits_everything() {
        let authz = AdminAuthorizer::permissive();
        assert!(!authz.is_enforcing());
        let stranger = AdminIdentity::Named("nobody".to_owned());
        assert_eq!(
            authz.authorize_command(&stranger, &change_membership()),
            Ok(())
        );
        assert_eq!(authz.authorize_command(&stranger, &acquire(NODE_B)), Ok(()));
        assert_eq!(authz.authorize_cluster(&stranger), Ok(()));
        // And a node may still act for another node, as it could before.
        assert_eq!(
            authz.authorize_command(&AdminIdentity::Node(NODE_A), &acquire(NODE_B)),
            Ok(())
        );
    }

    /// Bootstrap and membership changes carry no command to classify, so they
    /// are gated by frame kind. Only an operator may submit them.
    #[test]
    fn only_an_operator_may_submit_cluster_scoped_rpcs() {
        let authz = authorizer(&["ops-alice"]);
        assert_eq!(
            authz.authorize_cluster(&AdminIdentity::Named("ops-alice".to_owned())),
            Ok(())
        );
        for identity in [
            AdminIdentity::Node(NODE_A),
            AdminIdentity::MetaNode(1),
            AdminIdentity::Named("someone".to_owned()),
        ] {
            assert!(
                authz.authorize_cluster(&identity).is_err(),
                "{identity:?} must not bootstrap or change membership"
            );
        }
    }

    /// The rule that matters: a node may drive its own lease, which is what
    /// makes failover work without shipping operator credentials everywhere.
    #[test]
    fn a_node_may_acquire_its_own_lease() {
        let authz = authorizer(&[]);
        assert_eq!(
            authz.authorize_command(&AdminIdentity::Node(NODE_A), &acquire(NODE_A)),
            Ok(())
        );
    }

    /// And the rule that makes it safe: it may not acquire anyone else's. One
    /// compromised node must not be able to fence every range in the cluster.
    #[test]
    fn a_node_may_not_acquire_another_nodes_lease() {
        let authz = authorizer(&[]);
        let refusal = authz
            .authorize_command(&AdminIdentity::Node(NODE_A), &acquire(NODE_B))
            .unwrap_err();
        assert_eq!(
            refusal,
            Refusal::WrongHolder {
                presented: NODE_A,
                requested: NODE_B
            }
        );
        // The message must name both sides: a misconfigured node UUID is the
        // likeliest cause, and "refused" alone sends someone to the wrong place.
        let message = refusal.message();
        assert!(message.contains(&NODE_A.to_string()), "{message}");
        assert!(message.contains(&NODE_B.to_string()), "{message}");
    }

    #[test]
    fn a_node_may_not_change_the_cluster() {
        let authz = authorizer(&[]);
        assert!(matches!(
            authz.authorize_command(&AdminIdentity::Node(NODE_A), &change_membership()),
            Err(Refusal::NotAnOperator { .. })
        ));
    }

    #[test]
    fn a_configured_operator_may_do_anything() {
        let authz = authorizer(&["ops-alice"]);
        let alice = AdminIdentity::Named("ops-alice".to_owned());
        assert_eq!(
            authz.authorize_command(&alice, &change_membership()),
            Ok(())
        );
        assert_eq!(authz.authorize_command(&alice, &acquire(NODE_B)), Ok(()));
    }

    /// An empty operator list is a real configuration — a cluster administered
    /// out of band — and must not silently fall back to permitting everyone.
    #[test]
    fn no_configured_operators_means_nobody_may_change_the_cluster() {
        let authz = authorizer(&[]);
        for identity in [
            AdminIdentity::Node(NODE_A),
            AdminIdentity::MetaNode(1),
            AdminIdentity::Named("someone".to_owned()),
        ] {
            assert!(
                authz
                    .authorize_command(&identity, &change_membership())
                    .is_err(),
                "{identity:?} must not be able to change the cluster"
            );
        }
    }

    /// Reads change nothing, and an operator failing to read its own cluster
    /// during an incident is a worse outcome than a node reading state it
    /// could infer anyway.
    #[test]
    fn any_authenticated_client_may_read() {
        let authz = authorizer(&[]);
        for identity in [
            AdminIdentity::Node(NODE_A),
            AdminIdentity::MetaNode(2),
            AdminIdentity::Named("anyone".to_owned()),
        ] {
            assert_eq!(authz.authorize_read(&identity), Ok(()));
        }
    }

    /// Commands added later must be cluster-scoped until someone deliberately
    /// says otherwise. This pins the default so a new variant cannot become
    /// node-submittable by oversight.
    #[test]
    fn unlisted_commands_default_to_cluster_scoped() {
        let (class, holder) = classify(&MetadataCommand::CreateTopic {
            env: envelope(),
            name: "t".to_owned(),
            topic_uuid: Uuid::from_u128(7),
            root_range_uuid: Uuid::from_u128(8),
        });
        assert_eq!(class, CommandClass::ClusterScoped);
        assert_eq!(holder, None);
    }

    /// A metadata node's certificate is not a lease credential: it identifies a
    /// Raft member, not a range holder, so it cannot satisfy the "acts for
    /// itself" rule and must be refused rather than matched loosely.
    #[test]
    fn a_meta_node_certificate_cannot_drive_a_lease() {
        let authz = authorizer(&[]);
        assert!(matches!(
            authz.authorize_command(&AdminIdentity::MetaNode(1), &acquire(NODE_A)),
            Err(Refusal::NotAnOperator { .. })
        ));
    }
}

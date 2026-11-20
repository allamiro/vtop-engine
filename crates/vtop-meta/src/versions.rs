//! What each data node runs, and whether the cluster may require it (#634).
//!
//! A metadata feature level used to be judged on the metadata tier alone: an
//! operator raised it once every VOTER ran a release that knew it. Data nodes
//! were invisible to that decision, so behaviour that needs every replica of
//! a range (a fenced replica that keeps records a candidate merely lacks,
//! #611; a promotion that records lineage verdicts, #577) held only on the
//! nodes already upgraded, and nothing enforced the upgrade order.
//!
//! Data nodes now report what they run
//! ([`crate::MetadataCommand::ReportNodeVersion`]), and a raise above
//! [`NODE_VERSION_REPORTS_FEATURE_LEVEL`] is refused while any node that
//! counts has not reported that level or higher. This is the finalize step of the upgrade Kafka
//! (`metadata.version`), CockroachDB (cluster versions) and etcd use: roll
//! the binaries, see every node report the new release, then finalize.
//!
//! The judgement lives here, in one function, because two places make it:
//! `vtopctl meta enable-features` before it proposes (so the refusal can
//! name every node), and the state machine when it applies the raise (so a
//! node that joins between that read and the proposal cannot slip past it).

use crate::command::{NodeState, NODE_VERSION_REPORTS_FEATURE_LEVEL};
use uuid::Uuid;

/// What a data node reported it runs, as metadata recorded it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeVersionRecord {
    /// The node's release, such as `0.8.0`.
    pub release: String,
    /// The highest replica-plane protocol it speaks.
    pub max_replica_protocol: u16,
    /// The highest metadata feature level it implements.
    pub supported_feature_level: u32,
}

/// One registered node, as a feature raise judges it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeVersionEntry {
    pub node_uuid: Uuid,
    pub state: NodeState,
    /// Holds a range's lease, or is named in a range's recorded replica set:
    /// a range member metadata can name.
    pub range_member: bool,
    /// `None`: this node has never reported.
    pub report: Option<NodeVersionRecord>,
}

impl NodeVersionEntry {
    /// Whether a raise waits for this node.
    ///
    /// A range member metadata can name counts, and so does any node that
    /// has reported - only a data node reports, and a follower of a range
    /// without a recorded replica set is known to metadata ONLY by its
    /// report. A node marked dead does not count: that is how an operator
    /// takes a node that will never return out of the judgement.
    ///
    /// A registered node that is neither does not count. It may be a
    /// placeholder, or a node metadata cannot tie to any range; if it does
    /// serve one on an older release, the replica plane's version handshake
    /// refuses it once the level is raised.
    pub fn counted(&self) -> bool {
        self.state != NodeState::Dead && (self.range_member || self.report.is_some())
    }
}

/// Every registered node and what it reported, with the level in force.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterVersions {
    pub feature_level: u32,
    pub nodes: Vec<NodeVersionEntry>,
}

/// A node a raise must wait for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeatureBlocker {
    pub node_uuid: Uuid,
    /// `None`: never reported. A node on a release before the report cannot.
    pub reported: Option<NodeVersionRecord>,
}

impl ClusterVersions {
    /// The nodes that must report at least `level` before it may be raised,
    /// in node-UUID order. Empty means the raise may proceed.
    ///
    /// Always empty at or below [`NODE_VERSION_REPORTS_FEATURE_LEVEL`]: a
    /// node cannot report until that level is in force, so nothing could
    /// vouch for it, and those levels change nothing on the replica plane.
    pub fn blockers(&self, level: u32) -> Vec<FeatureBlocker> {
        if level <= NODE_VERSION_REPORTS_FEATURE_LEVEL {
            return Vec::new();
        }
        let mut blockers: Vec<FeatureBlocker> = self
            .nodes
            .iter()
            .filter(|node| node.counted())
            .filter(|node| {
                node.report
                    .as_ref()
                    .is_none_or(|report| report.supported_feature_level < level)
            })
            .map(|node| FeatureBlocker {
                node_uuid: node.node_uuid,
                reported: node.report.clone(),
            })
            .collect();
        blockers.sort_by_key(|blocker| blocker.node_uuid);
        blockers
    }
}

impl std::fmt::Display for FeatureBlocker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.reported {
            Some(report) => write!(
                formatter,
                "{} (release {}, supports level {})",
                self.node_uuid, report.release, report.supported_feature_level
            ),
            None => write!(formatter, "{} (never reported)", self.node_uuid),
        }
    }
}

/// Why a raise to `level` is refused, naming every node it waits for.
pub fn describe_blockers(level: u32, blockers: &[FeatureBlocker]) -> String {
    let named: Vec<String> = blockers.iter().map(ToString::to_string).collect();
    // The remedy FIRST: the state machine's refusal is cut at
    // `MAX_ERROR_DETAIL_BYTES`, and a long list would cut it off (review).
    let (count, verb) = match blockers.len() {
        1 => ("1 node has".to_owned(), "it"),
        n => (format!("{n} nodes have"), "them"),
    };
    format!(
        "feature level {level} needs every counted node to report it; upgrade {verb} (or mark \
         a node that will not return dead) and raise again. {count} not: {}",
        named.join(", ")
    )
}

/// The first feature level a leader raises only on the metadata members'
/// own word (#706).
///
/// Levels up to 7 were raised on the operator's: nothing asked the voters,
/// and the releases that implement 7 (v0.8.0, v0.9.0) cannot answer the
/// question, so asking them would refuse a raise they support. Level 8 ships
/// in the same release as the question, so no release that implements 8 is
/// unable to answer it, and from 8 on every member must: a voter that cannot
/// apply a level rejects it deterministically, its view of the level
/// diverges for good, and the first command of that level is one it cannot
/// decode (review of #758: v0.9.0 implements 7, not 8).
pub const FIRST_MEMBER_CHECKED_FEATURE_LEVEL: u32 = 8;

/// Bound on the reason an unreachable member is named with, so the members
/// after it survive the admin error bound.
const MAX_UNREACHABLE_REASON_BYTES: usize = 48;

fn clip_reason(reason: &str) -> &str {
    if reason.len() <= MAX_UNREACHABLE_REASON_BYTES {
        return reason;
    }
    let mut end = MAX_UNREACHABLE_REASON_BYTES;
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    &reason[..end]
}

/// What one metadata member answered when the leader asked what it runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemberVersion {
    /// It answered, as `node_id`.
    Reported {
        node_id: crate::keys::MetaNodeId,
        release: String,
        supported_feature_level: u32,
    },
    /// It took the request and closed the connection without an answer: a
    /// release that predates the question (v0.9.0 or earlier).
    NoAnswer,
    /// It could not be reached; the reason is for the operator.
    Unreachable(String),
}

/// A metadata member a raise must wait for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberBlocker {
    pub member: crate::keys::MetaNodeId,
    pub answer: MemberVersion,
}

impl std::fmt::Display for MemberBlocker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.answer {
            MemberVersion::Reported {
                node_id,
                release,
                supported_feature_level,
            } if *node_id == self.member => write!(
                formatter,
                "{} (release {release}, supports {supported_feature_level})",
                self.member
            ),
            MemberVersion::Reported { node_id, .. } => write!(
                formatter,
                "{} (node {node_id} answered at its address)",
                self.member
            ),
            // Any close after the request, so not only an old release: a
            // member that refused the request closes the same way.
            MemberVersion::NoAnswer => write!(
                formatter,
                "{} (closed without answering: v0.9.0 or older, or it refused)",
                self.member
            ),
            MemberVersion::Unreachable(reason) => {
                write!(
                    formatter,
                    "{} (unreachable: {})",
                    self.member,
                    clip_reason(reason)
                )
            }
        }
    }
}

/// The metadata members that must answer before `level` may be raised:
/// those that gave no answer or could not be reached first, then those that
/// answered too low, each in member order. Empty means the raise may
/// proceed.
///
/// Always empty below [`FIRST_MEMBER_CHECKED_FEATURE_LEVEL`]. From it on a
/// member counts as ready only when it answered AS ITSELF with a level at or
/// above `level`: silence, an unreachable member and an answer from another
/// node at its address all hold the raise back, because the cost of a wrong
/// "yes" is a voter that can append nothing more.
pub fn member_blockers(
    level: u32,
    answers: &[(crate::keys::MetaNodeId, MemberVersion)],
) -> Vec<MemberBlocker> {
    if level < FIRST_MEMBER_CHECKED_FEATURE_LEVEL {
        return Vec::new();
    }
    let mut blockers: Vec<MemberBlocker> = answers
        .iter()
        .filter(|(member, answer)| {
            !matches!(
                answer,
                MemberVersion::Reported {
                    node_id,
                    supported_feature_level,
                    ..
                } if node_id == member && *supported_feature_level >= level
            )
        })
        .map(|(member, answer)| MemberBlocker {
            member: *member,
            answer: answer.clone(),
        })
        .collect();
    // The silent and the unreachable first: a refusal is cut at
    // `MAX_ERROR_DETAIL_BYTES`, and they are the ones an operator cannot
    // find from a version listing.
    blockers.sort_by_key(|blocker| {
        (
            match blocker.answer {
                MemberVersion::NoAnswer => 0,
                MemberVersion::Unreachable(_) => 1,
                MemberVersion::Reported { .. } => 2,
            },
            blocker.member,
        )
    });
    blockers
}

/// Why a raise to `level` is refused, naming every member it waits for.
///
/// What happened, what was done and what to do come BEFORE the list: an
/// admin error is cut at [`crate::command::MAX_ERROR_DETAIL_BYTES`], and the
/// list is what may be cut.
pub fn describe_member_blockers(level: u32, enabled: u32, blockers: &[MemberBlocker]) -> String {
    let named: Vec<String> = blockers.iter().map(ToString::to_string).collect();
    let (count, claim) = match blockers.len() {
        1 => ("1 metadata member".to_owned(), "it supports"),
        n => (format!("{n} metadata members"), "they support"),
    };
    format!(
        "feature level {level} not raised: {count} did not show {claim} it. Nothing was \
         proposed; the level stays {enabled}. Upgrade them, then raise again: {}",
        named.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(level: u32) -> Option<NodeVersionRecord> {
        Some(NodeVersionRecord {
            release: format!("0.{level}.0"),
            max_replica_protocol: 2,
            supported_feature_level: level,
        })
    }

    fn node(
        id: u128,
        state: NodeState,
        member: bool,
        report: Option<NodeVersionRecord>,
    ) -> NodeVersionEntry {
        NodeVersionEntry {
            node_uuid: Uuid::from_u128(id),
            state,
            range_member: member,
            report,
        }
    }

    /// Who counts: a range member or a reporter, unless dead. A placeholder
    /// that never reported and serves no range does not hold a raise back.
    #[test]
    fn the_raise_waits_for_live_members_and_reporters_only() {
        let versions = ClusterVersions {
            feature_level: 7,
            nodes: vec![
                node(1, NodeState::Active, true, report(8)),
                node(2, NodeState::Active, true, None),
                node(3, NodeState::Draining, false, report(7)),
                node(4, NodeState::Dead, true, report(7)),
                node(5, NodeState::Active, false, None),
            ],
        };
        let blockers = versions.blockers(8);
        assert_eq!(
            blockers.iter().map(|b| b.node_uuid).collect::<Vec<_>>(),
            vec![Uuid::from_u128(2), Uuid::from_u128(3)],
            "the silent member and the lagging reporter, not the dead node or the placeholder"
        );
        let why = describe_blockers(8, &blockers);
        assert!(why.contains("never reported"), "{why}");
        assert!(why.contains("supports level 7"), "{why}");
        assert!(
            versions.blockers(9).len() == 3,
            "at 9 the level-8 node blocks too"
        );
    }

    /// #634 review: the state machine cuts its refusal at
    /// `MAX_ERROR_DETAIL_BYTES`, so what to do comes before the list, and
    /// survives however many nodes the list names.
    #[test]
    fn the_remedy_survives_the_refusal_bound() {
        let blockers: Vec<FeatureBlocker> = (1..=12)
            .map(|id| FeatureBlocker {
                node_uuid: Uuid::from_u128(id),
                reported: None,
            })
            .collect();
        let why = describe_blockers(8, &blockers);
        let cut = &why[..crate::command::MAX_ERROR_DETAIL_BYTES.min(why.len())];
        assert!(cut.contains("raise again"), "{cut}");
        assert!(why.contains("12 nodes have not"), "{why}");
        assert!(describe_blockers(8, &blockers[..1]).contains("1 node has not"));
    }

    fn reported(id: u64, level: u32) -> MemberVersion {
        MemberVersion::Reported {
            node_id: crate::keys::MetaNodeId(id),
            release: "0.10.0".to_owned(),
            supported_feature_level: level,
        }
    }

    /// #706: from level 8 every metadata member must answer as itself with
    /// that level or higher. An old member's silence, an unreachable member
    /// and another node answering at a member's address all hold it back;
    /// the silent first, then the unreachable, then those that answered low.
    #[test]
    fn a_raise_from_level_8_waits_for_every_metadata_member() {
        use crate::keys::MetaNodeId;
        let answers = vec![
            (MetaNodeId(1), reported(1, 8)),
            (MetaNodeId(2), reported(2, 9)),
            (MetaNodeId(3), MemberVersion::NoAnswer),
            (MetaNodeId(4), reported(4, 7)),
            (
                MetaNodeId(5),
                MemberVersion::Unreachable("connection refused".into()),
            ),
            (MetaNodeId(6), reported(7, 8)),
        ];
        let blockers = member_blockers(8, &answers);
        assert_eq!(
            blockers.iter().map(|b| b.member.0).collect::<Vec<_>>(),
            vec![3, 5, 4, 6],
            "the silent, then the unreachable, then those that answered"
        );
        let why = describe_member_blockers(8, 7, &blockers);
        assert!(why.contains("not raised"), "{why}");
        assert!(
            why.contains("Nothing was proposed; the level stays 7"),
            "{why}"
        );
        assert!(why.contains("Upgrade them, then raise again"), "{why}");
        assert!(
            why.contains("3 (closed without answering: v0.9.0 or older, or it refused)"),
            "{why}"
        );
        assert!(why.contains("4 (release 0.10.0, supports 7)"), "{why}");
        assert!(why.contains("5 (unreachable: connection refused)"), "{why}");
        assert!(why.contains("6 (node 7 answered at its address)"), "{why}");
        assert!(
            member_blockers(8, &answers[..2]).is_empty(),
            "members that answered 8 and 9 let a raise to 8 through"
        );
        assert_eq!(member_blockers(9, &answers[..2]).len(), 1);
    }

    /// Levels up to 7 were raised on the operator's word, and v0.8.0 and
    /// v0.9.0 implement 7 but cannot answer: asking would refuse a raise
    /// they can apply. v0.9.0 implements 7, not 8 (review of #758), so the
    /// check starts at 8.
    #[test]
    fn levels_below_8_are_not_judged_on_the_metadata_members() {
        let answers = vec![(crate::keys::MetaNodeId(3), MemberVersion::NoAnswer)];
        for level in 1..FIRST_MEMBER_CHECKED_FEATURE_LEVEL {
            assert!(member_blockers(level, &answers).is_empty(), "level {level}");
        }
        assert_eq!(
            FIRST_MEMBER_CHECKED_FEATURE_LEVEL,
            crate::command::PROMOTE_UNPROVEN_FEATURE_LEVEL
        );
        assert_eq!(member_blockers(8, &answers).len(), 1);
    }

    /// The admin error is cut at `MAX_ERROR_DETAIL_BYTES`: what happened,
    /// what was done and what to do survive however many members it names,
    /// and a long unreachable reason is clipped so the members after it fit.
    #[test]
    fn the_member_refusal_keeps_its_remedy_inside_the_error_bound() {
        let blockers: Vec<MemberBlocker> = (1..=9)
            .map(|id| MemberBlocker {
                member: crate::keys::MetaNodeId(id),
                answer: MemberVersion::Unreachable("x".repeat(80)),
            })
            .collect();
        let why = describe_member_blockers(8, 7, &blockers);
        let cut = &why[..crate::command::MAX_ERROR_DETAIL_BYTES.min(why.len())];
        assert!(cut.contains("Upgrade them, then raise again"), "{cut}");
        assert!(cut.contains("level stays 7"), "{cut}");
        assert!(why.contains("9 metadata members"), "{why}");
        assert!(!why.contains(&"x".repeat(49)), "reasons are clipped: {why}");
        assert!(describe_member_blockers(8, 7, &blockers[..1]).contains("1 metadata member "));

        // The rolling-upgrade shape: an unreachable member with a long TLS
        // reason and an old one that closes the connection. The old one is
        // named first, so it survives the bound.
        let rolling = member_blockers(
            8,
            &[
                (
                    crate::keys::MetaNodeId(2),
                    MemberVersion::Unreachable(format!(
                        "tls: peer connect: invalid peer certificate: {}",
                        "expired ".repeat(20)
                    )),
                ),
                (crate::keys::MetaNodeId(3), MemberVersion::NoAnswer),
            ],
        );
        let why = describe_member_blockers(8, 4, &rolling);
        let cut = &why[..crate::command::MAX_ERROR_DETAIL_BYTES.min(why.len())];
        assert!(cut.contains("again: 3 (closed without answering"), "{cut}");
        assert!(cut.contains("2 (unreachable: tls"), "{cut}");
    }

    /// Nothing can vouch for a data node before reports exist, so the level
    /// that introduces them is never held back by them.
    #[test]
    fn levels_up_to_the_report_level_are_not_judged_on_data_nodes() {
        let versions = ClusterVersions {
            feature_level: 6,
            nodes: vec![node(1, NodeState::Active, true, None)],
        };
        assert!(versions
            .blockers(NODE_VERSION_REPORTS_FEATURE_LEVEL)
            .is_empty());
        assert_eq!(
            versions
                .blockers(NODE_VERSION_REPORTS_FEATURE_LEVEL + 1)
                .len(),
            1
        );
    }
}

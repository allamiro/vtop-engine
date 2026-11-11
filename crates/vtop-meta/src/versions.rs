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

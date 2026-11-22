//! The `vtop-node` command tree and the node's metric families, against the
//! committed snapshots in `docs/surface/` (#719).
//!
//! A node registers its collectors as its roles come up, each around live
//! state (a broker, a follower, the Raft core), so no single call registers
//! them all. Here every collector type is built over a minimal stand-in and
//! described, alongside what `NodeObservability::new` registers itself
//! (`node_info`, the otel and audit counters). A family added to an
//! existing collector is in the snapshot with no edit here. A new collector
//! type is caught by `every_collector_type_is_in_the_snapshot`, which reads
//! the crate's sources for `impl Collector for`.

use super::*;
use std::path::PathBuf;
use vtop_broker::replication::ClusterCommittedOffset;
use vtop_broker::{MetaFencingEpoch, ProducerEpochJournal};

fn snapshot(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/surface")
        .join(name)
}

fn range() -> vtop_protocol::RangeIdentity {
    vtop_protocol::RangeIdentity {
        topic: "telemetry".into(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(7),
        range_generation: 0,
    }
}

fn active_segment(dir: &std::path::Path) -> vtop_log::ActiveSegment {
    let range = range();
    let descriptor = vtop_log::SegmentDescriptor {
        segment_id: Uuid::from_u128(9),
        topic: range.topic.clone(),
        topic_epoch: range.topic_epoch,
        lineage: vtop_log::RangeLineage {
            range_id: range.range_id,
            generation: range.range_generation,
            key_range: vtop_log::KeyRange::full(),
            parents: Vec::new(),
        },
        base_offset: 0,
    };
    vtop_log::ActiveSegment::create(
        dir.join("range.active"),
        descriptor,
        vtop_log::SegmentConfig::default(),
    )
    .unwrap()
}

struct NoReplica;
impl ReplicaObservation for NoReplica {
    fn try_local_offsets(&self) -> Option<(u64, u64)> {
        None
    }
    fn cluster_committed_offset(&self) -> Option<u64> {
        None
    }
    fn try_meta_fencing_epoch(&self) -> Option<(u64, bool)> {
        None
    }
    fn held_fencing_epoch(&self) -> u64 {
        0
    }
    fn is_leading(&self) -> bool {
        false
    }
}

struct NoCursors;
impl GroupCursorSource for NoCursors {
    fn group_cursor_positions(
        &self,
        _limit: usize,
    ) -> Option<(Vec<vtop_meta::GroupCursorPosition>, bool)> {
        Some((Vec::new(), false))
    }
}

/// Every collector type the node registers, by type name, each over a
/// stand-in for the live state it reads. The names are what
/// `every_collector_type_is_in_the_snapshot` compares with the sources.
fn every_collector(dir: &std::path::Path) -> Vec<(&'static str, Box<dyn Collector>)> {
    let leader_dir = dir.join("leader");
    let follower_dir = dir.join("follower");
    std::fs::create_dir_all(&leader_dir).unwrap();
    std::fs::create_dir_all(&follower_dir).unwrap();
    let broker = LocalBroker::new(
        active_segment(&leader_dir),
        ProducerEpochJournal::open(leader_dir.join("epochs")).unwrap(),
        range(),
        4,
    )
    .unwrap()
    // Group commit on: its families are emitted only while it is, and the
    // surface is everything a node can serve.
    .with_group_commit(vtop_broker::group_commit::GroupCommitConfig {
        max_delay: std::time::Duration::ZERO,
        ..Default::default()
    })
    .unwrap();
    let follower = InProcessFollower::new(
        Uuid::from_u128(2),
        active_segment(&follower_dir),
        ProducerEpochJournal::open(follower_dir.join("epochs")).unwrap(),
        range(),
        1,
        MetaFencingEpoch::new(1),
        ClusterCommittedOffset::new(0),
    )
    .unwrap();
    vec![
        (
            "BrokerCollector",
            Box::new(BrokerCollector::new(Arc::new(broker), None, Vec::new()).unwrap()),
        ),
        (
            "FollowerCollector",
            Box::new(FollowerCollector::new(Arc::new(follower)).unwrap()),
        ),
        (
            "CandidateCollector",
            Box::new(CandidateCollector::new(Arc::new(NoReplica), &range()).unwrap()),
        ),
        (
            "ServerCollector",
            Box::new(ServerCollector::new(Arc::new(ServerMetrics::new())).unwrap()),
        ),
        (
            "PromotionRefusalCollector",
            Box::new(PromotionRefusalCollector::new(Arc::default(), &range()).unwrap()),
        ),
        (
            "KafkaRefusalCollector",
            Box::new(KafkaRefusalCollector::new(Arc::default()).unwrap()),
        ),
        (
            "SegmentRecoveryCollector",
            Box::new(SegmentRecoveryCollector::new(None).unwrap()),
        ),
        (
            "MetaGroupCursorCollector",
            Box::new(MetaGroupCursorCollector::new(Arc::new(NoCursors)).unwrap()),
        ),
        // MetaRaftCollector needs a running Raft core; what it describes and
        // emits is exactly its MetaRaftGauges', so the gauges stand in for it.
        ("MetaRaftGauges", Box::new(MetaRaftGauges::new().unwrap())),
        // MetaAuditCollector needs a running Raft core as well; it describes
        // and emits exactly its MetaAuditMetrics' families (#706).
        (
            "MetaAuditMetrics",
            Box::new(MetaAuditMetrics::new().unwrap()),
        ),
        (
            "ConnectionLimitCollector",
            Box::new(ConnectionLimitCollector::new(ConnectionLimits::default()).unwrap()),
        ),
    ]
}

/// Collector types whose families reach the registry through another type
/// in `every_collector`.
const COVERED_BY_ANOTHER: [(&str, &str); 2] = [
    ("MetaRaftCollector", "MetaRaftGauges"),
    ("MetaAuditCollector", "MetaAuditMetrics"),
];

/// Every family a node can export. Each collector goes into a registry of
/// its own: a leader's, a follower's and a candidate's collectors export the
/// same range families under the same names, and a node registers one of
/// them per range, so one registry would refuse the second. A name exported
/// by two collectors must mean the same family in both, or a dashboard built
/// against one role reads the other wrong.
#[test]
fn the_node_metric_families_match_their_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    // The node's own registry: node_info, and the otel and audit counters
    // `NodeObservability::new` registers.
    let observability = NodeObservability::new("node", "1").unwrap();
    let mut families: std::collections::BTreeMap<String, (vtop_surface::Family, &str)> =
        std::collections::BTreeMap::new();
    let mut add = |found: Vec<vtop_surface::Family>, from: &'static str| {
        for family in found {
            match families.get(&family.name) {
                Some((seen, by)) => assert_eq!(
                    seen, &family,
                    "{by} and {from} both export {}, as different families",
                    family.name
                ),
                None => {
                    families.insert(family.name.clone(), (family, from));
                }
            }
        }
    };
    add(
        observability
            .registry
            .families()
            .unwrap_or_else(|e| panic!("{e}")),
        "NodeObservability::new",
    );
    for (name, collector) in every_collector(dir.path()) {
        let registry = Registry::new_custom(Some("vtop".into()), None).unwrap();
        registry
            .register(collector)
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        add(registry.families().unwrap_or_else(|e| panic!("{e}")), name);
    }
    let lines = families.values().map(|(family, _)| family.line());
    if let Err(message) = vtop_surface::check(&snapshot("metrics-node.txt"), lines) {
        panic!("{message}");
    }
}

/// A collector type the test above does not build would leave its families
/// out of the snapshot without anything failing. So every `impl Collector
/// for X` in this crate must be built there, or be covered by a type that is.
#[test]
fn every_collector_type_is_in_the_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let built: std::collections::BTreeSet<&str> = every_collector(dir.path())
        .into_iter()
        .map(|(name, _)| name)
        .chain(COVERED_BY_ANOTHER.iter().map(|(name, _)| *name))
        .collect();
    let mut found = std::collections::BTreeSet::new();
    let mut pending = vec![PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src")];
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            pending.extend(
                std::fs::read_dir(&path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path()),
            );
            continue;
        }
        if path.extension().is_none_or(|ext| ext != "rs") {
            continue;
        }
        for line in std::fs::read_to_string(&path).unwrap().lines() {
            let line = line.trim_start();
            if !line.starts_with("impl") {
                continue;
            }
            if let Some((_, rest)) = line.split_once("Collector for ") {
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                // The stand-ins in this file's tests are not node collectors.
                if !name.is_empty() && !path.ends_with("surface_snapshot.rs") {
                    found.insert((name, path.display().to_string()));
                }
            }
        }
    }
    let missing: Vec<String> = found
        .iter()
        .filter(|(name, _)| !built.contains(name.as_str()))
        .map(|(name, path)| format!("{name} ({path})"))
        .collect();
    assert!(
        missing.is_empty(),
        "collector types {missing:?} are not built by `every_collector` in \
         crates/vtop-node/src/observe/surface_snapshot.rs, so their families are \
         missing from docs/surface/metrics-node.txt. Add each one there, then \
         regenerate with scripts/update-surface-snapshots.sh"
    );
    assert!(
        found.len() >= built.len(),
        "the scan found {} collector types but every_collector builds {}: the \
         scan is no longer reading the sources it should",
        found.len(),
        built.len()
    );
}

#[test]
fn the_vtop_node_command_tree_matches_its_snapshot() {
    use clap::CommandFactory;
    let lines = vtop_surface::command_lines(&crate::Cli::command());
    if let Err(message) = vtop_surface::check(&snapshot("vtop-node.txt"), lines) {
        panic!("{message}");
    }
}

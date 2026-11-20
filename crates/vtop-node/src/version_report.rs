//! A data node's account of what it runs, and its view of the level the
//! cluster finalized (#634).
//!
//! Two jobs, one loop, on the lease renewal cadence:
//!
//! * REPORT `(release, max_replica_protocol, supported_feature_level)` to
//!   metadata, until metadata accepts it. A tier below the report's level
//!   refuses it before it reaches the log, and the node asks again next
//!   round; once accepted the record is durable and the loop stops sending
//!   it - a report that has not changed carries nothing, and the proposals
//!   would share one Raft queue with every lease renewal. A new release
//!   means a new process, which reports again.
//! * READ the finalized level, and publish it to this node's replica plane
//!   ([`ClusterFeatureLevel`]), which holds every peer to it. A read the
//!   tier cannot serve leaves the last level standing: levels only rise, so
//!   an old reading is a lower bound, never a wrong one.
//!
//! A node with no metadata configured (a static range) runs neither, and its
//! replica plane refuses no peer.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use uuid::Uuid;
use vtop_broker::replication::{ClusterFeatureLevel, ReplicaVersionPolicy};
use vtop_meta::command::CommandEnvelope;
use vtop_meta::{AdminClient, MetadataCommand, MetadataError, MetadataResponse};

/// The finalized level as THIS node reads it, shared by its replica server,
/// its leader's follower streams, and its one-shot replica clients.
///
/// Keyed by node rather than held in one cell, so tests that run several
/// nodes in one process keep their views apart.
pub fn cluster_level_for(node_uuid: Uuid) -> ClusterFeatureLevel {
    static LEVELS: OnceLock<Mutex<HashMap<Uuid, ClusterFeatureLevel>>> = OnceLock::new();
    LEVELS
        .get_or_init(Default::default)
        .lock()
        .expect("cluster level registry")
        .entry(node_uuid)
        .or_default()
        .clone()
}

/// How this node opens and serves replica sessions.
pub fn version_policy(node_uuid: Uuid) -> ReplicaVersionPolicy {
    ReplicaVersionPolicy::new(node_uuid, cluster_level_for(node_uuid))
}

/// What this build reports.
pub fn this_release(node_uuid: Uuid) -> MetadataCommand {
    MetadataCommand::ReportNodeVersion {
        env: CommandEnvelope {
            request_id: Uuid::new_v4(),
            // Advisory for this command: only the lease pair is judged by it.
            issued_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since| i64::try_from(since.as_millis()).unwrap_or(i64::MAX))
                .unwrap_or(0),
        },
        node_uuid,
        release: env!("CARGO_PKG_VERSION").to_owned(),
        max_replica_protocol: vtop_protocol::REPLICA_PROTOCOL_MAX,
        supported_feature_level: vtop_meta::SUPPORTED_FEATURE_LEVEL,
    }
}

/// What one report attempt came to.
#[derive(Debug, PartialEq, Eq)]
pub enum Reported {
    /// Recorded; the level in force came back with it.
    Accepted { feature_level: u32 },
    /// The tier is not yet at the report's level (or cannot decode it).
    NotYet,
    /// Metadata has no record of this node: it must be registered before a
    /// raise can count it.
    Unregistered,
    /// Anything else, said as it was said.
    Failed(String),
}

pub async fn report_once(admin: &AdminClient, node_uuid: Uuid) -> Reported {
    match admin.propose(this_release(node_uuid)).await {
        Ok(receipt) => match receipt.response {
            MetadataResponse::Ack { generation } => Reported::Accepted {
                feature_level: u32::try_from(generation).unwrap_or(u32::MAX),
            },
            MetadataResponse::Rejected(MetadataError::NotFound) => Reported::Unregistered,
            other => Reported::Failed(format!("{other:?}")),
        },
        Err(error) if vtop_meta::transport::refuses_version_report(&error) => Reported::NotYet,
        Err(error) => Reported::Failed(error.to_string()),
    }
}

/// The longest pause, in rounds, between reports metadata refused.
const MAX_BACKOFF_ROUNDS: u32 = 32;

/// Read the finalized level once, within `budget`, into `level`. A read the
/// tier cannot serve leaves the level as it was: levels only rise, so the
/// last one read is a lower bound.
///
/// Returns `false` when the budget ran out. The client is then pointed at
/// its next candidate (#634 review): it moves its preference only on an
/// answer, so an endpoint that accepts and never answers would otherwise be
/// asked first by every later request.
pub async fn read_level(
    admin: &AdminClient,
    level: &ClusterFeatureLevel,
    budget: Duration,
) -> bool {
    match tokio::time::timeout(budget, admin.read_cluster_versions(false)).await {
        Ok(Ok(read)) => {
            level.observe(read.versions.feature_level);
            true
        }
        Ok(Err(_)) => true,
        Err(_) => {
            admin.prefer_next_candidate();
            false
        }
    }
}

/// Report until accepted, and keep `level` current, every `interval` until
/// `shutdown` fires.
///
/// `budget` bounds one round's RPCs, the read and the report together, and
/// is NOT the polling cadence (#634 review): a cadence shorter than the
/// admin client's per-candidate allowance would cancel every round before
/// it could fail over past a stalled metadata endpoint. The data node gives
/// it the lease duration, the budget its lease watcher gives its reads.
/// Rounds never overlap: the next starts `interval` after this one ends.
pub async fn run(
    admin: AdminClient,
    node_uuid: Uuid,
    level: ClusterFeatureLevel,
    interval: Duration,
    budget: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut reported = false;
    // Rounds to sit out before the next report, and the next such pause. A
    // refusal by the state machine (an unregistered node, a malformed
    // report) is a COMMITTED log entry, so it is retried on a doubling pause
    // rather than every round (review). A tier below the report level
    // refuses before the log, and that is asked every round.
    let mut skip_rounds = 0_u32;
    let mut backoff = 1_u32;
    // Each condition is said once when it starts, not every round: a tier
    // below the report level can stay there for a whole rollout.
    let mut said: Option<&'static str> = None;
    let mut say = |what: &'static str, line: String| {
        if said != Some(what) {
            eprintln!("{line}");
            said = Some(what);
        }
    };
    loop {
        if *shutdown.borrow() {
            return;
        }
        // The level FIRST (review): the replica plane judges peers against
        // it, and a report that hangs must not leave it unread for a round.
        let round = tokio::time::Instant::now() + budget;
        let read_in_time = read_level(&admin, &level, budget).await;
        if !reported && skip_rounds > 0 {
            skip_rounds -= 1;
        } else if !reported && read_in_time {
            match tokio::time::timeout_at(round, report_once(&admin, node_uuid)).await {
                Ok(Reported::Accepted { feature_level }) => {
                    reported = true;
                    level.observe(feature_level);
                    println!(
                        "node_version_reported node={node_uuid} release={} \
                         supported_feature_level={} feature_level={feature_level}",
                        env!("CARGO_PKG_VERSION"),
                        vtop_meta::SUPPORTED_FEATURE_LEVEL
                    );
                }
                Ok(Reported::NotYet) => say(
                    "not-yet",
                    format!(
                        "node {node_uuid}: metadata is below feature level {}, so this node \
                         cannot report its release yet; it keeps asking (#634)",
                        vtop_meta::NODE_VERSION_REPORTS_FEATURE_LEVEL
                    ),
                ),
                Ok(Reported::Unregistered) => {
                    say(
                        "unregistered",
                        format!(
                            "warning: node {node_uuid} is not registered in metadata, so its \
                             release report is refused and a feature raise cannot count it; \
                             register it (vtopctl meta register-node) (#634)"
                        ),
                    );
                    skip_rounds = backoff;
                    backoff = (backoff * 2).min(MAX_BACKOFF_ROUNDS);
                }
                Ok(Reported::Failed(why)) => {
                    say(
                        "failed",
                        format!("warning: node {node_uuid}: version report failed: {why}"),
                    );
                    skip_rounds = backoff;
                    backoff = (backoff * 2).min(MAX_BACKOFF_ROUNDS);
                }
                // Out of time: start the next round at the next candidate.
                Err(_) => admin.prefer_next_candidate(),
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            changed = shutdown.changed() => {
                // A closed watch never fires again; without this the loop
                // would spin on it instead of waiting out the interval.
                if changed.is_err() {
                    tokio::time::sleep(interval).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use vtop_meta::transport::{
        AdminMembershipResponse, AdminProposeResponse, AdminReadClusterVersionsRequest,
        AdminReadClusterVersionsResponse, AdminReadSegmentPlacementRequest,
        AdminReadSegmentPlacementResponse, TransportError, TransportResult,
    };

    /// A metadata tier at `level`, gating proposals the way a release does
    /// and recording every report it was asked for.
    struct Tier {
        level: AtomicU32,
        registered: std::sync::atomic::AtomicBool,
        /// How long a proposal takes to answer.
        propose_delay: Duration,
        reports: std::sync::Mutex<Vec<MetadataCommand>>,
    }

    fn unused() -> TransportError {
        TransportError::Protocol("not served by this fake".into())
    }

    #[async_trait::async_trait]
    impl vtop_meta::AdminHandler for Tier {
        async fn status(&self) -> TransportResult<vtop_meta::AdminStatusResponse> {
            Ok(vtop_meta::transport::stub_status(vtop_meta::MetaNodeId(1)))
        }
        async fn propose(&self, command: MetadataCommand) -> TransportResult<AdminProposeResponse> {
            self.reports.lock().unwrap().push(command.clone());
            tokio::time::sleep(self.propose_delay).await;
            let level = self.level.load(Ordering::SeqCst);
            if command.feature_level() > level {
                return Err(TransportError::Protocol(
                    vtop_meta::ConsensusError::FeatureNotEnabled {
                        needed: command.feature_level(),
                        enabled: level,
                    }
                    .to_string(),
                ));
            }
            Ok(AdminProposeResponse {
                log_id: vtop_meta::WireLogId { term: 1, index: 1 },
                response: if self.registered.load(Ordering::SeqCst) {
                    MetadataResponse::Ack {
                        generation: u64::from(level),
                    }
                } else {
                    MetadataResponse::Rejected(MetadataError::NotFound)
                },
            })
        }
        async fn init(&self, _: Vec<u64>) -> TransportResult<AdminMembershipResponse> {
            Err(unused())
        }
        async fn add_learner(&self, _: u64) -> TransportResult<AdminMembershipResponse> {
            Err(unused())
        }
        async fn change_membership(
            &self,
            _: Vec<u64>,
            _: bool,
        ) -> TransportResult<AdminMembershipResponse> {
            Err(unused())
        }
        async fn read_range_lease(
            &self,
            _: vtop_meta::AdminReadRangeLeaseRequest,
        ) -> TransportResult<vtop_meta::AdminReadRangeLeaseResponse> {
            Err(unused())
        }
        async fn read_segment_placement(
            &self,
            _: AdminReadSegmentPlacementRequest,
        ) -> TransportResult<AdminReadSegmentPlacementResponse> {
            Err(unused())
        }
        async fn read_cluster_versions(
            &self,
            _: AdminReadClusterVersionsRequest,
        ) -> TransportResult<AdminReadClusterVersionsResponse> {
            Ok(AdminReadClusterVersionsResponse {
                versions: vtop_meta::ClusterVersions {
                    feature_level: self.level.load(Ordering::SeqCst),
                    nodes: Vec::new(),
                },
                truncated: false,
                read_at_applied_index: 1,
            })
        }
    }

    async fn serve(tier: Arc<Tier>) -> (AdminClient, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = vtop_meta::AdminServer::plaintext(
            tier as Arc<dyn vtop_meta::AdminHandler>,
            vtop_meta::transport::AdminAuthorizer::permissive(),
        )
        .unwrap();
        let serving = tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        let client = AdminClient::plaintext(vec![vtop_meta::AdminCandidate {
            node_id: Some(vtop_meta::MetaNodeId(1)),
            endpoint,
            host: None,
            server_name: String::new(),
            plaintext: true,
        }])
        .unwrap();
        (client, serving)
    }

    async fn until(within: Duration, mut done: impl FnMut() -> bool) -> bool {
        let deadline = tokio::time::Instant::now() + within;
        while tokio::time::Instant::now() < deadline {
            if done() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        done()
    }

    /// #634: a data node upgraded ahead of its metadata tier keeps asking to
    /// report while the tier refuses it, learns the level the tier runs at
    /// regardless, reports once the level that records it is raised - and
    /// then stops sending a report that has not changed.
    #[tokio::test]
    async fn a_data_node_reports_once_the_tier_can_record_it_and_then_stops() {
        let tier = Arc::new(Tier {
            level: AtomicU32::new(vtop_meta::LINEAGE_VERDICTS_FEATURE_LEVEL),
            registered: true.into(),
            propose_delay: Duration::ZERO,
            reports: Default::default(),
        });
        let (client, serving) = serve(Arc::clone(&tier)).await;
        let node = Uuid::from_u128(0x634);
        let level = ClusterFeatureLevel::unknown();
        let (stop, shutdown) = tokio::sync::watch::channel(false);
        let reporter = tokio::spawn(run(
            client,
            node,
            level.clone(),
            Duration::from_millis(20),
            Duration::from_secs(5),
            shutdown,
        ));

        assert!(
            until(Duration::from_secs(5), || tier
                .reports
                .lock()
                .unwrap()
                .len()
                >= 3)
            .await,
            "the node must keep asking while the tier refuses"
        );
        assert!(
            until(Duration::from_secs(5), || level.get()
                == vtop_meta::LINEAGE_VERDICTS_FEATURE_LEVEL)
            .await,
            "the level is read whether or not the report lands"
        );

        tier.level.store(
            vtop_meta::NODE_VERSION_REPORTS_FEATURE_LEVEL,
            Ordering::SeqCst,
        );
        assert!(
            until(Duration::from_secs(5), || level.get()
                == vtop_meta::NODE_VERSION_REPORTS_FEATURE_LEVEL)
            .await
        );
        // Several more rounds: an accepted report is not sent again.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let reports = tier.reports.lock().unwrap().clone();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            tier.reports.lock().unwrap().len(),
            reports.len(),
            "a report the tier accepted was sent again"
        );
        let Some(MetadataCommand::ReportNodeVersion {
            node_uuid,
            release,
            max_replica_protocol,
            supported_feature_level,
            ..
        }) = reports.last()
        else {
            panic!("{reports:?}")
        };
        assert_eq!(*node_uuid, node);
        assert_eq!(release, env!("CARGO_PKG_VERSION"));
        assert_eq!(*max_replica_protocol, vtop_protocol::REPLICA_PROTOCOL_MAX);
        assert_eq!(*supported_feature_level, vtop_meta::SUPPORTED_FEATURE_LEVEL);

        stop.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), reporter)
            .await
            .expect("the reporter stops on shutdown")
            .unwrap();
        serving.abort();
    }

    /// A node metadata has no record of is told so: an operator must
    /// register it before a raise can count it.
    #[tokio::test]
    async fn an_unregistered_node_is_told_so() {
        let tier = Arc::new(Tier {
            level: AtomicU32::new(vtop_meta::NODE_VERSION_REPORTS_FEATURE_LEVEL),
            registered: false.into(),
            propose_delay: Duration::ZERO,
            reports: Default::default(),
        });
        let (client, serving) = serve(Arc::clone(&tier)).await;
        assert_eq!(
            report_once(&client, Uuid::from_u128(0x634)).await,
            Reported::Unregistered
        );
        serving.abort();
    }

    /// #634 review: the level must be read before anything else the loop
    /// does. It used to come after the report, so a report that hung kept
    /// the replica plane judging peers against an unread level (0, refusing
    /// nobody) for a whole round.
    #[tokio::test]
    async fn the_level_is_read_before_a_report_that_hangs() {
        let tier = Arc::new(Tier {
            level: AtomicU32::new(vtop_meta::NODE_VERSION_REPORTS_FEATURE_LEVEL),
            registered: true.into(),
            propose_delay: Duration::from_secs(30),
            reports: Default::default(),
        });
        let (client, serving) = serve(Arc::clone(&tier)).await;
        let level = ClusterFeatureLevel::unknown();
        let (_stop, shutdown) = tokio::sync::watch::channel(false);
        let reporter = tokio::spawn(run(
            client,
            Uuid::from_u128(0x634),
            level.clone(),
            Duration::from_secs(5),
            Duration::from_secs(5),
            shutdown,
        ));
        assert!(
            until(Duration::from_secs(1), || level.get()
                == vtop_meta::NODE_VERSION_REPORTS_FEATURE_LEVEL)
            .await,
            "the level was not read while a report hung"
        );
        reporter.abort();
        serving.abort();
    }

    /// #634 review: a refusal by the state machine is a COMMITTED log entry,
    /// so an unregistered node retrying at the renewal cadence wrote one per
    /// round, forever. It backs off, and still reports once registered.
    #[tokio::test]
    async fn an_unregistered_node_backs_off_and_reports_once_registered() {
        let tier = Arc::new(Tier {
            level: AtomicU32::new(vtop_meta::NODE_VERSION_REPORTS_FEATURE_LEVEL),
            registered: false.into(),
            propose_delay: Duration::ZERO,
            reports: Default::default(),
        });
        let (client, serving) = serve(Arc::clone(&tier)).await;
        let (_stop, shutdown) = tokio::sync::watch::channel(false);
        let reporter = tokio::spawn(run(
            client,
            Uuid::from_u128(0x634),
            ClusterFeatureLevel::unknown(),
            Duration::from_millis(20),
            Duration::from_secs(5),
            shutdown,
        ));
        tokio::time::sleep(Duration::from_millis(600)).await;
        let refused = tier.reports.lock().unwrap().len();
        assert!(
            (1..=8).contains(&refused),
            "{refused} refused reports in 600 ms at a 20 ms cadence: each one is a log entry"
        );
        tier.registered.store(true, Ordering::SeqCst);
        let before = refused;
        assert!(
            until(Duration::from_secs(5), || tier
                .reports
                .lock()
                .unwrap()
                .len()
                > before)
            .await,
            "a node registered later must still report"
        );
        reporter.abort();
        serving.abort();
    }

    /// #634 review (Codex): a metadata endpoint that accepts and then never
    /// answers must not hold the reporter forever. A round that runs out of
    /// time used to leave the client preferring the same endpoint - it moves
    /// only on an answer - so every round asked it again, the report never
    /// landed, and the level stayed unread. The healthy second endpoint must
    /// get the report within a few rounds.
    #[tokio::test]
    async fn a_stalled_first_endpoint_does_not_keep_the_report_from_the_next() {
        let hung = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hung_endpoint = hung.local_addr().unwrap();
        // Accepts, holds the connection, never reads or answers.
        let holding = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = hung.accept().await {
                held.push(stream);
            }
        });
        let tier = Arc::new(Tier {
            level: AtomicU32::new(vtop_meta::NODE_VERSION_REPORTS_FEATURE_LEVEL),
            registered: true.into(),
            propose_delay: Duration::ZERO,
            reports: Default::default(),
        });
        let (single, serving) = serve(Arc::clone(&tier)).await;
        drop(single);
        let healthy_endpoint = {
            // `serve` bound its own listener; reach it through a fresh one.
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = listener.local_addr().unwrap();
            let server = vtop_meta::AdminServer::plaintext(
                Arc::clone(&tier) as Arc<dyn vtop_meta::AdminHandler>,
                vtop_meta::transport::AdminAuthorizer::permissive(),
            )
            .unwrap();
            tokio::spawn(async move {
                let _ = server.serve(listener).await;
            });
            endpoint
        };
        let candidate = |id: u64, endpoint| vtop_meta::AdminCandidate {
            node_id: Some(vtop_meta::MetaNodeId(id)),
            endpoint,
            host: None,
            server_name: String::new(),
            plaintext: true,
        };
        let client = AdminClient::plaintext(vec![
            candidate(1, hung_endpoint),
            candidate(2, healthy_endpoint),
        ])
        .unwrap();
        let level = ClusterFeatureLevel::unknown();
        let (_stop, shutdown) = tokio::sync::watch::channel(false);
        let reporter = tokio::spawn(run(
            client,
            Uuid::from_u128(0x634),
            level.clone(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            shutdown,
        ));
        assert!(
            until(Duration::from_secs(8), || !tier
                .reports
                .lock()
                .unwrap()
                .is_empty())
            .await,
            "the healthy endpoint never received the report: every round asked the stalled one"
        );
        assert!(
            until(Duration::from_secs(4), || level.get()
                == vtop_meta::NODE_VERSION_REPORTS_FEATURE_LEVEL)
            .await
        );
        reporter.abort();
        serving.abort();
        holding.abort();
    }
}

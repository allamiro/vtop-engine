//! `vtopctl node` — operator client for the native data plane (#224).
//!
//! Admin parity with `vtopctl meta`: that command answers "what does the
//! metadata group think", and this one answers "where has each replica's disk
//! actually got to". Both take a small YAML describing an mTLS endpoint; key
//! material stays on disk as PEM paths and is never embedded in the config.
//!
//! # Why this is not just a metrics query
//!
//! The `/metrics` endpoint (#224) reports each node's own view of itself, which
//! is the right shape for dashboards and alerts. It is the wrong shape for an
//! incident, because it requires every node to be scraped and healthy enough to
//! serve HTTP. This command goes replica-to-replica over the replication plane
//! the leader already uses, so it still answers when the observability endpoint
//! is unreachable or was never configured — and it reports the replicas it
//! *could not* reach as findings rather than failing the whole command.

use clap::{Args, Subcommand};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;
use vtop_broker::replication::{ReplicaStatusClient, ReplicaTlsMaterial};
use vtop_meta::{resolve_endpoint, TlsMaterial};
use vtop_protocol::RangeIdentity;

#[derive(Subcommand, Debug)]
pub enum NodeCommand {
    /// Ask every configured replica for its committed offset and report lag.
    Status {
        #[command(flatten)]
        common: NodeCommonArgs,
        /// Per-replica deadline covering connect, TLS, and the round trip.
        ///
        /// Rejects 0: a zero deadline expires before the connection can be
        /// made, so a perfectly healthy cluster would be reported entirely
        /// unreachable.
        #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=3600))]
        timeout_seconds: u64,
    },
}

#[derive(Args, Debug)]
pub struct NodeCommonArgs {
    /// Path to a node client YAML (range, replicas, and PEM paths).
    #[arg(long)]
    pub config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RangeConfig {
    topic: String,
    topic_epoch: u64,
    range_id: Uuid,
    range_generation: u64,
}

impl RangeConfig {
    fn identity(&self) -> RangeIdentity {
        RangeIdentity {
            topic: self.topic.clone(),
            topic_epoch: self.topic_epoch,
            range_id: self.range_id,
            range_generation: self.range_generation,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ReplicaRole {
    Leader,
    #[default]
    Follower,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplicaConfig {
    /// Must equal the CN of the replica's certificate; the connection is
    /// refused otherwise, so a reused address cannot silently answer for
    /// another node.
    node_uuid: Uuid,
    /// `host:port` of the replica's replication listener.
    addr: String,
    server_name: String,
    #[serde(default)]
    role: ReplicaRole,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeClientConfig {
    range: RangeConfig,
    ca_cert: PathBuf,
    /// Operator certificate for the replication plane.
    ///
    /// Its CN **must be a UUID**. That is not this command's rule: the replica
    /// listener identifies every peer by a UUID CN before dispatching a frame,
    /// so a certificate with a human-readable CN is refused at the transport
    /// before the status request is ever read. Issue the operator certificate
    /// from the same CA with a UUID subject.
    client_cert: PathBuf,
    client_key: PathBuf,
    replicas: Vec<ReplicaConfig>,
}

/// One replica's answer, or why it did not give one.
struct ReplicaReport {
    node_uuid: Uuid,
    addr: String,
    role: ReplicaRole,
    outcome: Result<vtop_protocol::ReplicaStatusResponse, String>,
}

fn load_config(path: &Path) -> Result<NodeClientConfig, String> {
    let text =
        fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let config: NodeClientConfig = serde_yaml::from_str(&text)
        .map_err(|error| format!("parse {}: {error}", path.display()))?;
    if config.replicas.is_empty() {
        return Err(format!("{} lists no replicas", path.display()));
    }
    if config
        .replicas
        .iter()
        .filter(|replica| replica.role == ReplicaRole::Leader)
        .count()
        > 1
    {
        return Err(format!(
            "{} marks more than one replica as leader; lag would be measured against \
             an ambiguous reference",
            path.display()
        ));
    }
    Ok(config)
}

/// Build the client fresh per replica: `ReplicaTlsMaterial` is consumed when a
/// connector is built, and a status command is not on any hot path.
fn client(config: &NodeClientConfig, timeout: Duration) -> Result<ReplicaStatusClient, String> {
    let material =
        TlsMaterial::from_pem_files(&config.client_cert, &config.client_key, &config.ca_cert)
            .map_err(|error| error.to_string())?;
    ReplicaStatusClient::new(ReplicaTlsMaterial {
        certificate_chain: material.certificate_chain,
        private_key: material.private_key,
        trust_roots: material.trust_roots,
    })
    .map(|client| client.with_timeout(timeout))
    .map_err(|error| error.to_string())
}

async fn collect(
    config: &NodeClientConfig,
    timeout: Duration,
) -> Result<Vec<ReplicaReport>, String> {
    let range = config.range.identity();
    let mut reports = Vec::with_capacity(config.replicas.len());
    for replica in &config.replicas {
        // A fresh client per replica so one unusable endpoint cannot abort the
        // sweep before the healthy replicas have been asked.
        let endpoint = resolve_endpoint(&replica.addr).map_err(|error| error.to_string());
        let outcome = match (client(config, timeout), endpoint) {
            (Ok(client), Ok(addr)) => client
                .status(addr, &replica.server_name, replica.node_uuid, &range)
                .await
                .map_err(|error| error.to_string()),
            (Err(error), _) | (_, Err(error)) => Err(error),
        };
        reports.push(ReplicaReport {
            node_uuid: replica.node_uuid,
            addr: replica.addr.clone(),
            role: replica.role,
            outcome,
        });
    }
    Ok(reports)
}

/// The offset lag is measured against.
///
/// The declared leader when there is one, because that is the replica whose
/// commit boundary defines the range. Otherwise the highest committed offset
/// any replica reported — stated explicitly in the output, because "lag against
/// the furthest-ahead replica" is a weaker claim than "lag against the leader"
/// and an operator must not mistake one for the other.
struct Reference {
    offset: u64,
    source: ReferenceSource,
}

/// Where the reference offset came from — reported, never inferred by the
/// reader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReferenceSource {
    /// The declared leader answered. The strong claim.
    Leader,
    /// No leader was configured, so the furthest-ahead replica is the
    /// reference.
    NoLeaderConfigured,
    /// A leader WAS configured and did not answer. Distinct from the case
    /// above because it is an outage or a misconfiguration, not a deliberate
    /// leaderless query — printing them identically would hide the failure
    /// during the exact fallback it describes.
    LeaderUnreachable,
}

fn reference(reports: &[ReplicaReport]) -> Option<Reference> {
    let declared = reports
        .iter()
        .find(|report| report.role == ReplicaRole::Leader);
    if let Some(leader) = declared {
        if let Ok(status) = &leader.outcome {
            return Some(Reference {
                offset: status.local_committed_offset,
                source: ReferenceSource::Leader,
            });
        }
    }
    let fallback = match declared {
        Some(_) => ReferenceSource::LeaderUnreachable,
        None => ReferenceSource::NoLeaderConfigured,
    };
    reports
        .iter()
        .filter_map(|report| report.outcome.as_ref().ok())
        .map(|status| status.local_committed_offset)
        .max()
        .map(|offset| Reference {
            offset,
            source: fallback,
        })
}

fn print_text(config: &NodeClientConfig, reports: &[ReplicaReport], reference: Option<&Reference>) {
    println!(
        "range topic={} epoch={} id={} generation={}",
        config.range.topic,
        config.range.topic_epoch,
        config.range.range_id,
        config.range.range_generation
    );
    match reference.map(|r| (r.source, r.offset)) {
        Some((ReferenceSource::Leader, offset)) => {
            println!("lag measured against the leader at offset {offset}")
        }
        Some((ReferenceSource::NoLeaderConfigured, offset)) => println!(
            "no leader declared; lag measured against the furthest-ahead replica at offset {offset}"
        ),
        Some((ReferenceSource::LeaderUnreachable, offset)) => println!(
            "WARNING: the declared leader did not answer; lag measured against the \
             furthest-ahead replica at offset {offset}"
        ),
        None => println!("no replica answered; lag cannot be measured"),
    }
    for report in reports {
        let role = match report.role {
            ReplicaRole::Leader => "leader",
            ReplicaRole::Follower => "follower",
        };
        match &report.outcome {
            Ok(status) => {
                let lag = reference
                    .map(|reference| {
                        reference
                            .offset
                            .saturating_sub(status.local_committed_offset)
                            .to_string()
                    })
                    .unwrap_or_else(|| "unknown".to_owned());
                println!(
                    "  {role} {} {} committed={} next={} lag={lag}",
                    report.node_uuid,
                    report.addr,
                    status.local_committed_offset,
                    status.next_offset
                );
            }
            Err(error) => println!(
                "  {role} {} {} UNREACHABLE: {error}",
                report.node_uuid, report.addr
            ),
        }
    }
}

fn json_output(
    config: &NodeClientConfig,
    reports: &[ReplicaReport],
    reference: Option<&Reference>,
) -> serde_json::Value {
    serde_json::json!({
        "range": {
            "topic": config.range.topic,
            "topic_epoch": config.range.topic_epoch,
            "range_id": config.range.range_id,
            "range_generation": config.range.range_generation,
        },
        "reference_offset": reference.map(|r| r.offset),
        "reference_is_leader": reference.map(|r| r.source == ReferenceSource::Leader),
        "reference_source": reference.map(|r| match r.source {
            ReferenceSource::Leader => "leader",
            ReferenceSource::NoLeaderConfigured => "no_leader_configured",
            ReferenceSource::LeaderUnreachable => "leader_unreachable",
        }),
        "replicas": reports
            .iter()
            .map(|report| {
                serde_json::json!({
                    "node_uuid": report.node_uuid,
                    "addr": report.addr,
                    "role": match report.role {
                        ReplicaRole::Leader => "leader",
                        ReplicaRole::Follower => "follower",
                    },
                    "reachable": report.outcome.is_ok(),
                    "committed_offset": report.outcome.as_ref().ok().map(|s| s.local_committed_offset),
                    "next_offset": report.outcome.as_ref().ok().map(|s| s.next_offset),
                    "lag_records": report.outcome.as_ref().ok().and_then(|s| {
                        reference.map(|r| r.offset.saturating_sub(s.local_committed_offset))
                    }),
                    "error": report.outcome.as_ref().err(),
                })
            })
            .collect::<Vec<_>>(),
    })
}

/// Dispatch `vtopctl node` and return a process exit code.
pub async fn run(command: NodeCommand, json: bool) -> i32 {
    match run_inner(command, json).await {
        Ok(code) => code,
        Err(message) => {
            eprintln!("error: {message}");
            1
        }
    }
}

async fn run_inner(command: NodeCommand, json: bool) -> Result<i32, String> {
    match command {
        NodeCommand::Status {
            common,
            timeout_seconds,
        } => {
            let config = load_config(&common.config)?;
            let reports = collect(&config, Duration::from_secs(timeout_seconds)).await?;
            let reference = reference(&reports);
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json_output(
                        &config,
                        &reports,
                        reference.as_ref()
                    ))
                    .map_err(|error| error.to_string())?
                );
            } else {
                print_text(&config, &reports, reference.as_ref());
            }
            // Exit non-zero when any replica could not be reached. The report
            // is still printed in full: a partial picture is what an operator
            // needs mid-incident, but a script must not read "two of three
            // replicas answered" as success.
            Ok(i32::from(
                reports.iter().any(|report| report.outcome.is_err()),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vtop_protocol::ReplicaStatusResponse;

    fn config() -> NodeClientConfig {
        NodeClientConfig {
            range: RangeConfig {
                topic: "telemetry".into(),
                topic_epoch: 1,
                range_id: Uuid::from_u128(7),
                range_generation: 0,
            },
            ca_cert: PathBuf::from("ca.pem"),
            client_cert: PathBuf::from("client.pem"),
            client_key: PathBuf::from("client.key"),
            replicas: Vec::new(),
        }
    }

    fn report(role: ReplicaRole, committed: Option<u64>) -> ReplicaReport {
        ReplicaReport {
            node_uuid: Uuid::from_u128(committed.unwrap_or(0) as u128 + 1),
            addr: "127.0.0.1:9300".into(),
            role,
            outcome: match committed {
                Some(offset) => Ok(ReplicaStatusResponse {
                    local_committed_offset: offset,
                    next_offset: offset,
                }),
                None => Err("connection refused".into()),
            },
        }
    }

    #[test]
    fn lag_is_measured_against_the_declared_leader() {
        let reports = vec![
            report(ReplicaRole::Follower, Some(120)),
            report(ReplicaRole::Leader, Some(100)),
        ];
        let reference = reference(&reports).unwrap();
        assert_eq!(reference.offset, 100);
        assert!(
            reference.source == ReferenceSource::Leader,
            "a follower reading ahead of the leader must not become the reference; \
             the leader's boundary is what defines the range"
        );
    }

    #[test]
    fn without_a_leader_the_furthest_ahead_replica_is_the_reference_and_says_so() {
        let reports = vec![
            report(ReplicaRole::Follower, Some(80)),
            report(ReplicaRole::Follower, Some(140)),
        ];
        let reference = reference(&reports).unwrap();
        assert_eq!(reference.offset, 140);
        assert_eq!(
            reference.source,
            ReferenceSource::NoLeaderConfigured,
            "the weaker claim must be flagged as weaker"
        );
    }

    /// An unreachable leader must not silently promote a follower into the
    /// reference role without the output saying so.
    #[test]
    fn an_unreachable_leader_falls_back_to_a_flagged_reference() {
        let reports = vec![
            report(ReplicaRole::Leader, None),
            report(ReplicaRole::Follower, Some(60)),
        ];
        let reference = reference(&reports).unwrap();
        assert_eq!(reference.offset, 60);
        assert_eq!(
            reference.source,
            ReferenceSource::LeaderUnreachable,
            "a leader that was configured but did not answer is an outage, not a \
             deliberate leaderless query; reporting them identically hides the failure"
        );
    }

    #[test]
    fn nothing_answering_yields_no_reference_rather_than_zero() {
        let reports = vec![report(ReplicaRole::Leader, None)];
        assert!(
            reference(&reports).is_none(),
            "a reference of 0 would report every replica as perfectly caught up"
        );
    }

    #[test]
    fn json_reports_unreachable_replicas_instead_of_omitting_them() {
        let reports = vec![
            report(ReplicaRole::Leader, Some(100)),
            report(ReplicaRole::Follower, None),
        ];
        let reference = reference(&reports);
        let value = json_output(&config(), &reports, reference.as_ref());
        let replicas = value["replicas"].as_array().unwrap();
        assert_eq!(replicas.len(), 2);
        assert_eq!(replicas[1]["reachable"], serde_json::json!(false));
        assert!(replicas[1]["error"].is_string());
        assert_eq!(replicas[0]["lag_records"], serde_json::json!(0));
    }

    #[test]
    fn two_declared_leaders_are_rejected_rather_than_silently_picking_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.yaml");
        std::fs::write(
            &path,
            r#"
range:
  topic: telemetry
  topic_epoch: 1
  range_id: 00000000-0000-0000-0000-000000000007
  range_generation: 0
ca_cert: ca.pem
client_cert: client.pem
client_key: client.key
replicas:
  - node_uuid: 00000000-0000-0000-0000-000000000001
    addr: "127.0.0.1:9300"
    server_name: a
    role: leader
  - node_uuid: 00000000-0000-0000-0000-000000000002
    addr: "127.0.0.1:9301"
    server_name: b
    role: leader
"#,
        )
        .unwrap();
        let error = load_config(&path).unwrap_err();
        assert!(error.contains("more than one replica as leader"), "{error}");
    }

    #[test]
    fn a_config_with_no_replicas_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.yaml");
        std::fs::write(
            &path,
            r#"
range:
  topic: telemetry
  topic_epoch: 1
  range_id: 00000000-0000-0000-0000-000000000007
  range_generation: 0
ca_cert: ca.pem
client_cert: client.pem
client_key: client.key
replicas: []
"#,
        )
        .unwrap();
        assert!(load_config(&path).unwrap_err().contains("no replicas"));
    }
}

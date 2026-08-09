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
use vtop_broker::replication::{ReplicaStatusClient, ReplicaTlsMaterial, SegmentTransferClient};
use vtop_log::env::Env;
use vtop_log::{SegmentReceiver, SegmentSet};
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
    /// Rebuild a replica's data directory from a leader's sealed prefix (#301).
    ///
    /// The road back for a replica that fell below the leader's retransmission
    /// window. The leader can only replay what its bounded buffer still holds,
    /// so a follower behind that window cannot catch up through the append path
    /// at any speed — the records it needs are gone from memory. #270 built the
    /// transfer plane for exactly this and nothing invoked it.
    ///
    /// OPERATES ON A STOPPED NODE'S DIRECTORY, deliberately. The receiver
    /// sweeps debris on the grounds that everything it holds is re-fetchable
    /// from the leader, and that reasoning does not hold for a range being
    /// appended to — a mid-roll orphan there is recovery state, not rubbish. So
    /// this refuses a live range rather than racing it, and the workflow is:
    /// stop the replica (or start with an empty disk), repair, start it.
    Repair {
        #[command(flatten)]
        common: NodeCommonArgs,
        /// The replica to pull from. Must be a node that HOLDS the range —
        /// normally the leader, which is the only one obliged to serve a
        /// transfer.
        #[arg(long)]
        from: Uuid,
        /// The data directory to rebuild. Created if absent.
        #[arg(long)]
        into: PathBuf,
        /// Fencing epoch the transfer is requested under. A transfer served by
        /// a deposed leader would repair this replica onto a history the
        /// cluster has already moved past, so the epoch is checked per chunk on
        /// the serving side and must match what metadata granted.
        #[arg(long)]
        fencing_epoch: u64,
        #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=3600))]
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
        NodeCommand::Repair {
            common,
            from,
            into,
            fencing_epoch,
            timeout_seconds,
        } => {
            let config = load_config(&common.config)?;
            repair(
                &config,
                from,
                &into,
                fencing_epoch,
                Duration::from_secs(timeout_seconds),
                json,
            )
            .await
        }
    }
}

/// How far the repaired prefix is behind the source, or why the question has no
/// answer.
///
/// A free function so the arithmetic can be tested without a cluster. It is one
/// subtraction, and it decides whether the operator is told to start the
/// replica or to delete the directory — too much weight for a line that
/// otherwise only ever runs at the end of a live repair.
fn remaining_gap(source_tip: u64, adopted_next_offset: u64) -> Result<u64, String> {
    // NOT `saturating_sub`. A source tip BELOW the adopted prefix means the
    // source was truncated or replaced between the final chunk and the status
    // call, so the directory holds a suffix the source no longer has.
    // Saturating that to zero turns the most serious result this command can
    // produce — a replica carrying records the range has disowned — into the
    // one that reads as perfect success, and the operator is told to start it.
    if source_tip < adopted_next_offset {
        return Err(format!(
            "the source is at offset {source_tip} but the adopted prefix already ends at \
             {adopted_next_offset}, so this directory holds {} record(s) the source does not. \
             That cannot happen while a repair is merely behind: the source was truncated or \
             replaced between the last chunk and this check, and the transferred prefix belongs \
             to a history that no longer exists. Starting a replica from it would serve records \
             the range has disowned.",
            adopted_next_offset - source_tip
        ));
    }
    Ok(source_tip - adopted_next_offset)
}

/// Pull a leader's sealed prefix into `into` and adopt it into a servable
/// range.
///
/// Two steps that must not be split across invocations. A transferred prefix on
/// its own is bytes `SegmentSet::open_in` refuses to open — its tail was sealed
/// without a successor — so a repair that stopped after the transfer would
/// leave a directory a node cannot start against, and the operator with no
/// signal that anything was missing.
///
/// The exit codes are distinct because the remedies are: `0` the replica is
/// current, `1` it is behind by a measured gap, `2` the prefix is adopted but
/// the gap could not be measured. Only `0` means "start it and stop looking",
/// and `2` in particular must not be read as a failed transfer — repeating the
/// repair is the wrong response to it.
async fn repair(
    config: &NodeClientConfig,
    from: Uuid,
    into: &Path,
    fencing_epoch: u64,
    timeout: Duration,
    json: bool,
) -> Result<i32, String> {
    let source = config
        .replicas
        .iter()
        .find(|replica| replica.node_uuid == from)
        .ok_or_else(|| {
            format!(
                "no replica {from} in {}; --from must name a node the config lists, \
                 because its address and certificate name come from there",
                common_config_hint()
            )
        })?;

    // EMPTY, or a directory a previous repair owned. Not "empty" alone, and not
    // "anything goes".
    //
    // Two failures make an unconditional overwrite unsafe. A stopped replica's
    // directory still holds its `.active` file, so repairing in place would
    // race or clobber real data — and the receiver cannot tell a stopped
    // process from a running one. And a directory carrying sealed bundles the
    // leader has since truncated away would adopt them, reporting a successful
    // repair onto records the current leader does not have.
    //
    // But refusing every non-empty directory throws away the transfer's own
    // idempotent resume: an interrupted repair leaves staged debris and
    // installed bundles, and `transfer_sealed_prefix` is built to skip what
    // already landed and verified. Making an operator delete a partially
    // transferred prefix and re-download it after any transient network
    // failure is a real cost on a large range, and it grows with the size of
    // the thing being repaired — precisely when a retry matters most.
    //
    // So a marker file records that a repair owns this directory. Its presence
    // means these bytes came from a repair and nothing else, which is exactly
    // the condition under which resuming is safe.
    const OWNER_MARKER: &str = ".vtop-repair-owned";
    let marker = into.join(OWNER_MARKER);
    match std::fs::read_dir(into) {
        Ok(mut entries) => {
            if entries.next().is_some() && !marker.exists() {
                return Err(format!(
                    "{} is not empty and was not created by a previous repair. Repair rebuilds a \
                     range from scratch and must not run over existing data: a stopped replica's \
                     own directory still holds its active segment, and segments left by \
                     something else may have been truncated away by the leader since — adopting \
                     those would report success onto records the leader no longer has. Repair \
                     into a fresh directory and start the replica against that.",
                    into.display()
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(into)
                .map_err(|error| format!("create {}: {error}", into.display()))?;
        }
        Err(error) => return Err(format!("read {}: {error}", into.display())),
    }
    // A MARKED directory that already holds an adopted tail is a different
    // situation from a marked one that does not, and the two want opposite
    // answers. Without the tail, the last run stopped partway and resuming is
    // exactly right. With it, the last run FINISHED: the range is live, and
    // `SegmentReceiver::open` refuses to sweep it — correctly, because sweeping
    // a live range deletes recovery state. That refusal reads as an internal
    // complaint about a directory the operator was told they could reuse, so it
    // is caught here where the reason can be stated instead.
    //
    // This matters because re-running repair is the natural reflex when a gap
    // is reported, and it is the wrong move: a gap lives in the source's ACTIVE
    // segment, which never transfers, so no second repair can shrink it. Only
    // the replica catching up from the leader can.
    if marker.exists() {
        let adopted = std::fs::read_dir(into)
            .map_err(|error| format!("read {}: {error}", into.display()))?
            .filter_map(Result::ok)
            .find(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.ends_with(".active"))
            });
        if let Some(entry) = adopted {
            return Err(format!(
                "{} was repaired already — it holds an adopted range with an active segment at \
                 {}, so this repair finished and running it again would re-seed a live range. If \
                 a gap was reported, another repair cannot close it: those records are in the \
                 source's active segment, which never transfers, and only the replica catching \
                 up from the leader will bring them over. Start the replica against this \
                 directory. If it is refused for a base-offset mismatch then the gap was too \
                 large to catch up, and the remedy is a fresh repair into an EMPTY directory \
                 rather than a second one over this.",
                into.display(),
                entry.path().display()
            ));
        }
    }
    // Written BEFORE any bytes land, so an interruption at any point after this
    // leaves a directory a retry recognises. Writing it afterwards would make
    // the very failures worth resuming from unresumable.
    std::fs::write(&marker, b"vtop repair destination\n")
        .map_err(|error| format!("mark {} as repair-owned: {error}", into.display()))?;

    let material =
        TlsMaterial::from_pem_files(&config.client_cert, &config.client_key, &config.ca_cert)
            .map_err(|error| error.to_string())?;
    // Loaded twice because building a connector CONSUMES the material, and the
    // gap check below needs its own client.
    let status_material =
        TlsMaterial::from_pem_files(&config.client_cert, &config.client_key, &config.ca_cert)
            .map_err(|error| error.to_string())?;
    let client = SegmentTransferClient::new(ReplicaTlsMaterial {
        certificate_chain: material.certificate_chain,
        private_key: material.private_key,
        trust_roots: material.trust_roots,
    })
    .map_err(|error| error.to_string())?
    .with_request_timeout(timeout);

    let env = Env::real();
    // Refuses a directory holding an active segment, which is the guard that
    // keeps this from being run against a live range.
    let receiver = SegmentReceiver::open(&env, into).map_err(|error| {
        format!(
            "{error}: repair rebuilds a directory rather than joining one, so the replica \
             must be stopped, or starting from an empty disk, before it runs"
        )
    })?;
    let addr = resolve_endpoint(&source.addr).map_err(|error| error.to_string())?;
    let installed = client
        .transfer_sealed_prefix(
            addr,
            &source.server_name,
            source.node_uuid,
            &config.range.identity(),
            fencing_epoch,
            &receiver,
        )
        .await
        .map_err(|error| format!("transfer from {from}: {error}"))?;

    // ADOPT, so the result is a range and not a pile of segments. The successor
    // id is minted here because nothing else in this command owns one, and a
    // fresh id is correct: this tail has never existed anywhere else.
    let set = SegmentSet::adopt_in(&env, into, Uuid::new_v4()).map_err(|error| {
        format!(
            "adopt the transferred prefix in {}: {error}",
            into.display()
        )
    })?;
    let sealed = set.sealed().len();
    let next_offset = set.next_offset();
    drop(set);

    // A REPAIR CAN LEAVE THE REPLICA STILL STRANDED, and saying "repaired"
    // without checking would be the most expensive kind of wrong answer.
    //
    // Only SEALED segments transfer — the active tail is still being appended
    // to, so its bytes can be superseded by a truncation mid-copy. But the tail
    // is bounded at 8 GiB by default while the leader's retransmission buffer
    // is 8 MiB, a factor of a thousand. So the rebuilt replica starts at the
    // end of the sealed prefix and the leader may be far beyond it, inside a
    // tail that catch-up cannot replay. The replica restarts, is refused for a
    // base-offset mismatch, and an operator who was told "repaired" has no
    // reason to look here.
    //
    // The gap is measured and reported rather than assumed away. This asks the
    // source where it actually is instead of trusting the listing, because the
    // listing describes the sealed prefix and the question is about the tail.
    let status_client = ReplicaStatusClient::new(ReplicaTlsMaterial {
        certificate_chain: status_material.certificate_chain,
        private_key: status_material.private_key,
        trust_roots: status_material.trust_roots,
    })
    .map_err(|error| error.to_string())?
    .with_timeout(timeout);
    let source_tip = status_client
        .status(
            addr,
            &source.server_name,
            source.node_uuid,
            &config.range.identity(),
        )
        .await
        .map(|status| status.next_offset);

    // THE PREFIX IS ALREADY ADOPTED by the time this runs, so a status failure
    // is not a failed repair and must not be reported as one. Returning an
    // error here said "the prefix landed, but..." and exited as though the
    // command had not worked, which pushes an operator toward deleting the
    // directory and re-downloading a prefix that is intact — the most expensive
    // wrong move available, and its cost grows with the size of the range.
    //
    // What actually happened is narrower: the range is servable and the gap is
    // UNKNOWN. That deserves a non-zero exit, because an unmeasured gap is not
    // a measured zero, but the guidance is to measure it rather than to repeat
    // the transfer.
    let source_tip = match source_tip {
        Ok(tip) => tip,
        Err(error) => {
            let message = format!(
                "the prefix landed and was adopted, but {from}'s current offset could not be \
                 read, so whether the replica can still catch up is UNKNOWN: {error}"
            );
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "from": from.to_string(),
                        "directory": into.display().to_string(),
                        "segments_installed": installed.len(),
                        "sealed_segments": sealed,
                        "next_offset": next_offset,
                        "remaining_gap": serde_json::Value::Null,
                        "error": message,
                    })
                );
            } else {
                eprintln!("warning: {message}");
                eprintln!(
                    "The directory is repaired and servable. Do NOT repair it again — that would \
                     re-download an intact prefix. Read {from}'s offset with `vtopctl node \
                     status` and compare it against {next_offset}, or start the replica: if the \
                     gap is too large it is refused for a base-offset mismatch rather than \
                     serving records it does not have."
                );
            }
            return Ok(2);
        }
    };
    let gap = remaining_gap(source_tip, next_offset).map_err(|error| {
        format!(
            "{error} Delete {} and repair again once {from} is stable.",
            into.display()
        )
    })?;

    if json {
        println!(
            "{}",
            serde_json::json!({
                "from": from.to_string(),
                "directory": into.display().to_string(),
                "segments_installed": installed.len(),
                "sealed_segments": sealed,
                "next_offset": next_offset,
                "source_next_offset": source_tip,
                "remaining_gap": gap,
            })
        );
    } else {
        println!("repaired {} from {from}", into.display());
        println!("  segments transferred : {}", installed.len());
        println!("  sealed segments      : {sealed}");
        println!("  tail begins at offset: {next_offset}");
        println!("  source is at offset  : {source_tip}");
        println!("  remaining gap        : {gap} record(s)");
        if gap == 0 {
            println!("start the replica against this directory to resume replication");
        } else {
            println!(
                "\nThe {gap} record(s) between this prefix and {from}'s current position are in \
                 its ACTIVE segment, which does not transfer — a tail is still being appended to, \
                 so its bytes can be superseded by a truncation mid-copy. They must be replayed \
                 from the leader's retransmission buffer when the replica starts, and that buffer \
                 is bounded (8 MiB by default). If the gap exceeds it the replica will be refused \
                 for a base-offset mismatch and this repair will not have been enough."
            );
        }
    }
    // NON-ZERO when a gap remains. A script must not read "the transfer
    // succeeded" as "the replica is back", and a human should have to look.
    Ok(i32::from(gap > 0))
}

fn common_config_hint() -> &'static str {
    "the node client config"
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

    /// A source ahead of the prefix is the ordinary case, and the gap is exact.
    #[test]
    fn the_gap_is_the_distance_from_the_adopted_prefix_to_the_source() {
        assert_eq!(remaining_gap(1_000, 940).unwrap(), 60);
        assert_eq!(
            remaining_gap(940, 940).unwrap(),
            0,
            "a source sitting exactly at the prefix end is caught up, not behind"
        );
    }

    /// A source BEHIND the adopted prefix is refused rather than reported as
    /// caught up.
    ///
    /// This is the case `saturating_sub` silently turned into success: the
    /// destination holds records the source disowned, and clamping to zero made
    /// the worst outcome the command can reach indistinguishable from the best
    /// one — the operator is told to start a replica that would serve a history
    /// the range no longer has.
    #[test]
    fn a_source_behind_the_adopted_prefix_is_refused_not_clamped_to_zero() {
        let error = remaining_gap(900, 940)
            .expect_err("a prefix ahead of its own source is divergence, not progress");
        assert!(
            error.contains("40 record(s) the source does not"),
            "the message must quantify the divergence so the operator can judge it: {error}"
        );
        // One record apart is still divergence. The check is not a tolerance.
        assert!(remaining_gap(939, 940).is_err());
    }
}

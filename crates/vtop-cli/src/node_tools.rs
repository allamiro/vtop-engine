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
        /// Ask the leader to SEAL its active tail first (#306), so the
        /// transferred prefix reaches the leader's position at the seal
        /// rather than wherever the range last happened to roll. Without
        /// this, up to a whole segment bound of records stays behind in a
        /// tail that only the append path can deliver. Fenced like the
        /// transfer itself; sealing early costs one shorter segment.
        #[arg(long)]
        seal_tail: bool,
        #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=3600))]
        timeout_seconds: u64,
    },
    /// Change a range's roll thresholds by rolling once (#314).
    ///
    /// A segment's limits live in its header and are carried forward at every
    /// roll, so editing the node's YAML changes nothing for a range that
    /// already exists. This command is the contract for changing them: it
    /// validates the new thresholds under the range's ACTUAL format, seals
    /// the tail, opens a successor under the new limits, and exits. No
    /// existing header is touched — every sealed segment keeps describing
    /// exactly the records it holds — and because the new limits live in the
    /// new tail's header, reopen, adoption, and cross-segment truncation all
    /// carry them without remembering anything.
    ///
    /// OPERATES ON A STOPPED NODE'S DIRECTORY, like `repair`: a roll races a
    /// live appender, and the node reads its thresholds once at open. The
    /// workflow is: stop the node, reconfigure, start it — and update the
    /// node YAML to match, so a LATER recreation from empty starts at the
    /// same limits.
    ReconfigureRange {
        /// The data directory of the stopped node whose range to reconfigure.
        #[arg(long)]
        data_dir: PathBuf,
        /// New per-record ceiling in bytes. Absent thresholds keep the
        /// tail's current value.
        #[arg(long)]
        max_record_bytes: Option<u32>,
        /// New per-group ceiling in bytes. Must fit one framed record; the
        /// frame overhead differs between the v1 and v2 formats and is
        /// checked against the range's actual one.
        #[arg(long)]
        max_group_bytes: Option<u64>,
        /// New per-segment byte ceiling — the roll threshold.
        #[arg(long)]
        max_segment_bytes: Option<u64>,
        /// New per-segment record ceiling.
        #[arg(long)]
        max_segment_records: Option<u64>,
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

/// Each replica's epoch vector (#240) for the transitions audit's cross-check:
/// which fencing epoch wrote each stretch of its log, or why it could not be
/// asked. Same config as `node status`, one fresh client per replica.
pub(crate) async fn epoch_vectors(
    path: &Path,
    timeout: Duration,
) -> Result<Vec<crate::meta_tools::ReplicaEpochVector>, String> {
    let config = load_config(path)?;
    let range = config.range.identity();
    let mut vectors = Vec::with_capacity(config.replicas.len());
    for replica in &config.replicas {
        let endpoint = resolve_endpoint(&replica.addr).map_err(|error| error.to_string());
        let outcome = match (client(&config, timeout), endpoint) {
            (Ok(client), Ok(addr)) => client
                .epoch_history(addr, &replica.server_name, replica.node_uuid, &range)
                .await
                .map_err(|error| error.to_string()),
            (Err(error), _) | (_, Err(error)) => Err(error),
        };
        vectors.push((replica.node_uuid, replica.addr.clone(), outcome));
    }
    Ok(vectors)
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
            seal_tail,
            timeout_seconds,
        } => {
            let config = load_config(&common.config)?;
            repair(
                &config,
                from,
                &into,
                fencing_epoch,
                seal_tail,
                Duration::from_secs(timeout_seconds),
                json,
            )
            .await
        }
        NodeCommand::ReconfigureRange {
            data_dir,
            max_record_bytes,
            max_group_bytes,
            max_segment_bytes,
            max_segment_records,
        } => reconfigure_range(
            &data_dir,
            vtop_log::RollThresholds {
                max_record_bytes,
                max_group_bytes,
                max_segment_bytes,
                max_segment_records,
            },
            json,
        ),
    }
}

/// The four thresholds of one header, format-independent, for reporting.
fn limits_of(set: &SegmentSet) -> (u16, u32, u64, u64, u64) {
    match set.active().config_v2() {
        Some(config) => (
            vtop_log::FORMAT_VERSION_V2,
            config.max_record_bytes,
            config.max_group_bytes,
            config.max_segment_bytes,
            config.max_segment_records,
        ),
        None => {
            let config = set.active().config();
            (
                1,
                config.max_record_bytes,
                config.max_group_bytes,
                config.max_segment_bytes,
                config.max_segment_records,
            )
        }
    }
}

fn reconfigure_range(
    data_dir: &Path,
    thresholds: vtop_log::RollThresholds,
    json: bool,
) -> Result<i32, String> {
    if thresholds == vtop_log::RollThresholds::default() {
        return Err(
            "no threshold given; pass at least one of --max-record-bytes, --max-group-bytes, \
             --max-segment-bytes, --max-segment-records"
                .to_owned(),
        );
    }
    // THE SAME LOCK REPAIR TAKES, deliberately: both commands mutate the
    // directory's segment layout, and two of them interleaving would race
    // each other exactly as two repairs would. Sharing the file serializes
    // them, and `flock` means a crashed command releases it.
    let lock_handle = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(data_dir.join(LOCK_FILE))
        .map_err(|error| format!("open the lock in {}: {error}", data_dir.display()))?;
    rustix::fs::flock(
        &lock_handle,
        rustix::fs::FlockOperation::NonBlockingLockExclusive,
    )
    .map_err(|error| {
        format!(
            "another repair or reconfigure is already running against {} ({error}); wait for \
             it to finish",
            data_dir.display()
        )
    })?;

    let env = Env::real();
    // RERUNNING THIS COMMAND IS THE RECOVERY. An interruption between a
    // roll's two steps — this command's own roll included — leaves the tail
    // sealed with no successor, a layout `open_in` refuses because extending
    // a range is a writer's decision. This command has the standing to be
    // that writer: adopt a fresh tail at the sealed end (it inherits the
    // sealed header's OLD limits, exactly as an uninterrupted roll's
    // successor would have started from them), and the reconfigure below
    // then rewrites the empty tail in place under the new ones. Without
    // this, a routine threshold change interrupted at the wrong instant
    // would strand the range behind a tool the operator has no reason to
    // know about.
    let mut resumed = false;
    let mut set = match SegmentSet::open_in(&env, data_dir) {
        Ok(Some(set)) => set,
        Ok(None) => {
            return Err(format!(
                "no range found in {}; reconfigure changes an EXISTING range's thresholds — a \
                 range not yet created takes its limits from the node YAML at first start",
                data_dir.display()
            ));
        }
        Err(vtop_log::LogError::TailSealedWithoutSuccessor { .. }) => {
            resumed = true;
            // VALIDATE-THEN-ADOPT, in that order: adoption mints a tail,
            // and a resume that mutated the directory before discovering
            // the thresholds invalid would leave the range changed by a
            // command that reported failure.
            SegmentSet::adopt_for_reconfigure(&env, data_dir, thresholds, Uuid::new_v4()).map_err(
                |error| {
                    format!(
                        "resume the interrupted roll in {}: {error}",
                        data_dir.display()
                    )
                },
            )?
        }
        Err(error) => {
            return Err(format!("open the range in {}: {error}", data_dir.display()));
        }
    };
    if resumed && !json {
        println!(
            "resumed: an interrupted roll left this range's tail sealed without a successor; \
             a fresh tail was adopted at its end before reconfiguring"
        );
    }

    let before = limits_of(&set);
    let outcome = set
        .reconfigure_minting(thresholds)
        .map_err(|error| error.to_string())?;
    let after = limits_of(&set);

    let describe = |limits: (u16, u32, u64, u64, u64)| {
        serde_json::json!({
            "max_record_bytes": limits.1,
            "max_group_bytes": limits.2,
            "max_segment_bytes": limits.3,
            "max_segment_records": limits.4,
        })
    };
    let (outcome_name, successor_base) = match outcome {
        vtop_log::ReconfigureOutcome::Unchanged => ("unchanged", None),
        vtop_log::ReconfigureOutcome::Rolled { successor_base } => ("rolled", Some(successor_base)),
        vtop_log::ReconfigureOutcome::RewrittenInPlace => ("rewritten_in_place", None),
    };
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "outcome": outcome_name,
                "resumed_from_sealed_tail": resumed,
                "format_version": before.0,
                "successor_base": successor_base,
                "previous": describe(before),
                "current": describe(after),
            }))
            .map_err(|error| error.to_string())?
        );
    } else {
        match outcome {
            vtop_log::ReconfigureOutcome::Unchanged => {
                println!("unchanged: the tail already runs these thresholds; nothing was sealed")
            }
            vtop_log::ReconfigureOutcome::Rolled { successor_base } => println!(
                "rolled: tail sealed, successor opened at offset {successor_base} under the \
                 new thresholds"
            ),
            vtop_log::ReconfigureOutcome::RewrittenInPlace => println!(
                "rewritten in place: the tail holds no records, so its header now carries the \
                 new thresholds without sealing an empty segment"
            ),
        }
        println!(
            "thresholds (format v{}): record {} -> {}, group {} -> {}, segment bytes {} -> {}, \
             segment records {} -> {}",
            before.0, before.1, after.1, before.2, after.2, before.3, after.3, before.4, after.4
        );
        println!(
            "remember to update the node YAML to match: a recreation from an empty directory \
             starts from the YAML, not from this range's headers"
        );
    }
    Ok(0)
}

/// The three files `node repair` writes into a destination, and the only names
/// exempt from the emptiness check.
///
/// EXACT NAMES, not a `.vtop-repair-` prefix. A prefix exempts files this
/// command never wrote — and since log discovery ignores anything it does not
/// recognise, a destination someone else left `.vtop-repair-notes` in would
/// pass as empty and be repaired over. The exemption exists for bookkeeping
/// this command is known to have created, so it should name exactly that.
const OWNER_MARKER: &str = ".vtop-repair-owned";
/// DEFINED BY THE LOG, not by this command. Every opener enforces it —
/// `SegmentSet::open_in` and `adopt_in` both refuse — so a condemned directory
/// cannot be served by a node that was merely restarted, which is what a
/// supervisor or a rescheduled pod does without asking anyone. Repair writes
/// the verdict; it does not own it.
const DIVERGED_MARKER: &str = vtop_log::CONDEMNED_MARKER;
const LOCK_FILE: &str = ".vtop-repair-lock";

/// Whether a destination holds anything other than repair's own bookkeeping.
///
/// REPAIR'S OWN FILES DO NOT COUNT AS CONTENT. The lock is taken before the
/// marker is written, so a process that dies in between would otherwise leave a
/// directory that is non-empty and unmarked — refused forever for holding
/// nothing but a file this command created. Filtering by prefix keeps the order
/// of those two writes from mattering at all, which is better than getting the
/// order right once.
fn directory_is_occupied(into: &Path, entries: std::fs::ReadDir) -> Result<bool, String> {
    for entry in entries {
        // PROPAGATED, never skipped. A per-entry failure — a stale NFS handle, a
        // transient I/O error — silently dropped would end the scan early and
        // report a directory holding old sealed artifacts as empty. A later,
        // luckier scan would then repair over them, which is precisely the
        // history-clobbering this check exists to prevent. An unreadable
        // directory is a condition to report, not to interpret.
        let entry = entry.map_err(|error| {
            format!(
                "list {}: {error}. Refusing to judge whether the directory is empty from a \
                 partial listing — an entry this scan could not read may be a sealed segment, \
                 and repairing over one would report success onto records the source no longer \
                 has.",
                into.display()
            )
        })?;
        match entry.file_name().to_str() {
            Some(OWNER_MARKER | DIVERGED_MARKER | LOCK_FILE) => continue,
            // Non-UTF-8 is somebody else's file by definition — this command
            // only ever writes ASCII names — so it counts.
            _ => return Ok(true),
        }
    }
    Ok(false)
}

/// Write the divergence verdict so it survives losing the host.
///
/// FSYNCED, file and parent directory both. `write` returning says the page
/// cache holds the bytes, not the disk — so a crash moments later can leave the
/// adopted tail and the ownership marker (written long before, with time to
/// reach the platter) while this file never existed. The next run then reads a
/// marked directory with an adopted range, takes the completed-repair branch,
/// and tells the operator to start a replica holding the disowned suffix.
///
/// That is the exact outcome this whole mechanism exists to prevent, and a
/// power loss during a storage incident is not an exotic pairing — it is the
/// same class of event that produced the divergence.
fn record_divergence(diverged: &Path, detail: &str) -> std::io::Result<()> {
    use std::io::Write as _;

    let mut file = std::fs::File::create(diverged)?;
    file.write_all(detail.as_bytes())?;
    file.sync_all()?;
    // The DIRECTORY ENTRY needs its own sync: the contents can be durable while
    // the name pointing at them is not, which loses the file just as
    // completely.
    let parent = diverged
        .parent()
        .filter(|path| !path.as_os_str().is_empty());
    if let Some(parent) = parent {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// Refuse a directory an earlier repair condemned as diverged.
///
/// A free function so a test can reach it without a cluster. The verdict is the
/// only piece of repair state that must survive the process that reached it:
/// by the time divergence is detected the tail is already adopted and the
/// ownership marker already says the directory is reusable, so a later run sees
/// two facts that read as a finished repair and would tell the operator to
/// start the replica — the single action the verdict exists to forbid.
fn check_prior_divergence(into: &Path, diverged: &Path) -> Result<(), String> {
    if !diverged.exists() {
        return Ok(());
    }
    let detail = std::fs::read_to_string(diverged).unwrap_or_default();
    Err(format!(
        "{} was found to have DIVERGED from its source by an earlier repair, and that verdict \
         stands until the directory is cleared. Do not start a replica against it: it holds \
         records the source no longer has, and serving them would hand out a history the range \
         has disowned. Delete {} and repair again once the source is stable.\n\nThe earlier \
         finding was:\n{}",
        into.display(),
        into.display(),
        detail.trim()
    ))
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
    seal_tail: bool,
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
    let marker = into.join(OWNER_MARKER);
    let diverged = into.join(DIVERGED_MARKER);
    match std::fs::read_dir(into) {
        Ok(entries) => {
            // REPAIR'S OWN BOOKKEEPING DOES NOT COUNT AS CONTENT. The lock is
            // taken before the marker is written, so a process that dies in
            // between would otherwise leave a directory that is non-empty and
            // unmarked — refused forever by the branch below, for holding
            // nothing but a file this command created. Exempting the three
            // names it writes keeps the order of those two writes from
            // mattering at all, which beats getting the order right once.
            let occupied = directory_is_occupied(into, entries)?;
            if occupied && !marker.exists() {
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
    // ONE REPAIR AT A TIME PER DESTINATION. The ownership marker makes a
    // directory reusable, which is what a resume needs, and by itself that is
    // also what lets a SECOND repair start while the first is still running.
    // Both would then transfer against their own listing of the source; if the
    // source rolls in between, the later one installs a newly sealed segment at
    // a base offset the earlier one has already adopted a tail over, leaving two
    // primaries for the same offsets in a directory the first command may still
    // report as repaired.
    //
    // `flock` rather than a lock file's existence, because an advisory lock is
    // released by the kernel when the process dies. A presence-based lock
    // survives a crash and turns every interrupted repair into a directory that
    // needs a manual unlock — reintroducing, on the recovery path, exactly the
    // "delete it and start over" the marker exists to avoid.
    let lock_handle = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(into.join(LOCK_FILE))
        .map_err(|error| format!("open the repair lock in {}: {error}", into.display()))?;
    rustix::fs::flock(
        &lock_handle,
        rustix::fs::FlockOperation::NonBlockingLockExclusive,
    )
    .map_err(|error| {
        format!(
            "another repair is already running against {} ({error}). Two repairs sharing a \
                 destination transfer against their own listings of the source, and if it rolls \
                 between them the later one installs a segment at an offset the earlier one has \
                 already adopted over — leaving two primaries for the same records. Wait for the \
                 running repair to finish.",
            into.display()
        )
    })?;

    // A DIVERGENCE VERDICT OUTLIVES THE COMMAND THAT REACHED IT. When the final
    // check finds the source behind this prefix, the tail has already been
    // adopted — so without this the next run sees a marked directory with an
    // adopted range and takes the branch below, which says the repair finished
    // and to start the replica. That is precisely the action the divergence
    // check exists to prevent: it would serve a suffix the source disowned.
    //
    // A rejected verdict must therefore be recorded where the next invocation
    // will look, and only clearing the directory clears it.
    check_prior_divergence(into, &diverged)?;

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
    // the replica catching up from the leader can — or a fresh repair into an
    // empty directory with --seal-tail, which moves the sealed prefix to the
    // leader's position first (#306).
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
                 a gap was reported, another repair over this directory cannot close it: those \
                 records are in the source's active segment, which never transfers. Start the \
                 replica against this directory and let it catch up. If it is refused for a \
                 base-offset mismatch then the gap was too large to catch up, and the remedy is \
                 a fresh repair into an EMPTY directory with --seal-tail, which seals the \
                 leader's tail so the transferred prefix reaches its position (#306).",
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
    // BEFORE the transfer, so the listing that drives it already includes
    // the freshly sealed tail (#306). Ordering is the point: sealing after
    // the transfer would close the gap for the NEXT repair, not this one.
    let mut sealed_tail_end: Option<u64> = None;
    if seal_tail {
        let (sealed_end, records_sealed) = client
            .seal_tail(
                addr,
                &source.server_name,
                source.node_uuid,
                &config.range.identity(),
                fencing_epoch,
            )
            .await
            .map_err(|error| format!("seal the tail on {from}: {error}"))?;
        sealed_tail_end = Some(sealed_end);
        if !json {
            if records_sealed > 0 {
                println!(
                    "the leader sealed {records_sealed} tail record(s); the sealed prefix now \
                     reaches offset {sealed_end}"
                );
            } else {
                println!(
                    "the leader's tail was already empty; the sealed prefix already reaches \
                     offset {sealed_end}"
                );
            }
        }
    }
    let transferred = client
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
    let installed = transferred.installed;
    // The install bound for the epoch history is what THIS transfer verified
    // (#407): the source can seal new segments between the transfer's own
    // listing and the fenced proof listing below, and a bound taken from the
    // later listing would install lineage for records that were never
    // copied — exactly the claims-what-it-does-not-hold poisoning the
    // installer's truncation exists to prevent.
    let transferred_end = transferred.prefix_end;

    // THE LINEAGE TRAVELS WITH THE RECORDS (#315). The transfer carries the
    // bytes; this carries the history that lets the replica prove which
    // epochs produced them. Without it the first leader transition asks the
    // replica to reconcile, it can show nothing, and epoch-qualified
    // truncation erases the entire repair — the operator was told "repaired"
    // and the range is back at zero.
    //
    // Fetched from the same source as the segments, which extends exactly
    // the trust the transfer already required: a follower's journal is
    // leader-derived in normal replication too. The next fence still
    // compares this history honestly and truncates at any genuine
    // divergence.
    //
    // Installed BEFORE the adopt so a crash between the two is re-runnable:
    // the receiver refuses a directory with an adopted tail, so anything
    // that must happen on the repair path has to happen before the tail is
    // minted.
    let status_client = ReplicaStatusClient::new(ReplicaTlsMaterial {
        certificate_chain: status_material.certificate_chain,
        private_key: status_material.private_key,
        trust_roots: status_material.trust_roots,
    })
    .map_err(|error| error.to_string())?
    .with_timeout(timeout);
    let history = status_client
        .epoch_history(
            addr,
            &source.server_name,
            source.node_uuid,
            &config.range.identity(),
        )
        .await
        // An error fails the repair rather than installing records without
        // lineage: the transfer is resumable, so a retry is cheap, and a
        // "repaired" replica that cannot prove its history is the very state
        // this command exists to remove.
        .map_err(|error| format!("epoch history from {from}: {error}"))?;
    // THE HISTORY IS BOUNDED AND FENCED by a second, fenced listing. The
    // epoch-history request itself carries no fencing epoch, so on its own
    // the repair could pair segment bytes from one source state with lineage
    // from another — a source deposed between the last chunk and the fetch.
    // Epochs are strictly monotonic, so a fenced call SUCCEEDING here proves
    // the source held the credential epoch across the transfer, the history
    // fetch, and this moment. The listing is that PROOF and nothing more —
    // the epoch-history install is bounded by the transfer's own verified
    // end, because this listing can already include segments the source
    // sealed after the transfer listed its prefix (#407).
    let listing = client
        .list_sealed_segments(
            addr,
            &source.server_name,
            source.node_uuid,
            &config.range.identity(),
            fencing_epoch,
        )
        .await
        .map_err(|error| {
            format!(
                "the source no longer serves epoch {fencing_epoch} ({error}); the fetched \
                 history cannot be trusted to describe the transferred bytes — repair again \
                 once the source is stable"
            )
        })?;
    // The proof listing's contents beyond the proof itself are deliberately
    // unused: `transferred_end` is the bound everything below works from.
    drop(listing);
    // The seal's promise is checked against what the TRANSFER actually held:
    // retention runs after every append on the leader, so a produce landing
    // between the seal and the transfer's listing can reclaim the freshly
    // sealed segment under a bytes bound smaller than it. Nothing lies —
    // the gap is measured and reported below — but the operator deserves
    // the CAUSE named rather than a mysteriously shorter prefix (#306
    // review).
    if let (Some(promised), Some(listed)) = (sealed_tail_end, transferred_end) {
        if listed < promised && !json {
            println!(
                "note: the leader sealed through offset {promised}, but the transfer listing \
                 reaches only {listed} — the leader's retention reclaimed sealed segments \
                 between the two (its bound is smaller than what was sealed). The repair \
                 carries what remained; the reported gap below includes the reclaimed records"
            );
        }
    }
    if let Some(newest) = history.last() {
        if newest.epoch > fencing_epoch {
            return Err(format!(
                "the source's history reaches epoch {} but this repair was authorized under \
                 epoch {fencing_epoch}; the source has moved on — repair again with a current \
                 fencing epoch",
                newest.epoch
            ));
        }
    }
    if history.is_empty() {
        // A legal answer (the client's contract maps a peer that does not
        // know the request to an empty vector), but it leaves this replica
        // unable to prove lineage; say so now instead of letting the
        // operator find out at the next failover.
        eprintln!(
            "warning: {from} reported no epoch history; the repaired replica will answer \
             \"unknown\" at its next reconciliation instead of proving its lineage"
        );
    } else if let Some(tail) = transferred_end {
        let entries: Vec<vtop_broker::fencing_epochs::EpochStart> = history
            .iter()
            .map(|entry| vtop_broker::fencing_epochs::EpochStart {
                epoch: entry.epoch,
                start_offset: entry.start_offset,
            })
            .collect();
        vtop_broker::fencing_epochs::install_transferred_history(
            &env,
            into.join("fencing-epochs"),
            &entries,
            tail,
        )
        .map_err(|error| format!("install epoch history in {}: {error}", into.display()))?;
    }

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
    // (The client was built before the adopt, where it also fetched the epoch
    // history.)
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
                        // BOTH null, not just the gap. Automation reading this
                        // asks for a field that is always present in the success
                        // shape; dropping one on the error path makes the two
                        // shapes different objects and a consumer indexing the
                        // field fails on the branch it most needs to handle.
                        "source_next_offset": serde_json::Value::Null,
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
    let gap = match remaining_gap(source_tip, next_offset) {
        Ok(gap) => gap,
        Err(error) => {
            // RECORDED BEFORE RETURNING, because the tail is already adopted and
            // the marker already says this directory is reusable. Without a
            // durable verdict the next invocation reads those two facts as a
            // finished repair and tells the operator to start the replica —
            // undoing this diagnosis with the one instruction it forbids.
            let detail = format!("{error}\n(source {from}, checked against offset {next_offset})");
            if let Err(write_error) = record_divergence(&diverged, &detail) {
                return Err(format!(
                    "{error} Delete {} and repair again once {from} is stable. (This verdict could \
                     ALSO not be recorded in the directory — {write_error} — so a later repair \
                     will not know about it. Delete the directory now rather than relying on being \
                     warned again.)",
                    into.display()
                ));
            }
            return Err(format!(
                "{error} Delete {} and repair again once {from} is stable.",
                into.display()
            ));
        }
    };

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
                 for a base-offset mismatch and this repair will not have been enough.{}",
                if seal_tail {
                    "\nThe tail WAS sealed for this repair, so these records arrived after the \
                     seal. Starting the replica lets it replay them from the leader; if the gap \
                     exceeds the retransmission buffer, run a FRESH repair into an empty \
                     directory with --seal-tail — this directory is adopted now, and a second \
                     repair over it is refused."
                } else {
                    "\nRe-running with --seal-tail into an empty directory would seal the tail \
                     first, so the transferred prefix reaches the leader's position (#306)."
                }
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

    /// A recorded divergence verdict outlives the command that reached it.
    ///
    /// This is the case that made the fix necessary: divergence is detected
    /// AFTER the tail is adopted, so the directory then looks exactly like a
    /// finished repair — marked, with an active segment. Without a durable
    /// verdict the next run reads it that way and tells the operator to start
    /// the replica, which would serve the very suffix the source disowned.
    #[test]
    fn a_recorded_divergence_verdict_refuses_a_later_repair() {
        let dir = tempfile::tempdir().unwrap();
        let diverged = dir.path().join(".vtop-repair-diverged");

        check_prior_divergence(dir.path(), &diverged)
            .expect("a directory nothing has condemned is repairable");

        std::fs::write(
            &diverged,
            b"the source is at offset 900 but the adopted prefix ends at 940",
        )
        .unwrap();
        let error = check_prior_divergence(dir.path(), &diverged)
            .expect_err("a condemned directory must not be repaired or started");
        assert!(
            error.contains("Do not start a replica"),
            "the verdict must contradict the start guidance explicitly: {error}"
        );
        assert!(
            error.contains("offset 900"),
            "the original finding must be carried forward, not just its existence: {error}"
        );
    }

    /// Repair's own bookkeeping is not content; anything else is.
    #[test]
    fn only_files_repair_did_not_write_make_a_directory_occupied() {
        let dir = tempfile::tempdir().unwrap();
        let occupied =
            |path: &Path| directory_is_occupied(path, std::fs::read_dir(path).unwrap()).unwrap();

        assert!(!occupied(dir.path()), "an empty directory is repairable");

        // All three of repair's own files, including the lock that is created
        // BEFORE the marker — the window a crash can leave behind.
        for name in [
            ".vtop-repair-owned",
            ".vtop-repair-lock",
            ".vtop-repair-diverged",
        ] {
            std::fs::write(dir.path().join(name), b"").unwrap();
        }
        assert!(
            !occupied(dir.path()),
            "a directory holding only files this command wrote must stay repairable, or a crash \
             between taking the lock and writing the marker strands it forever"
        );

        // A file that merely LOOKS like repair bookkeeping is not. This command
        // writes exactly three names, and log discovery ignores whatever it
        // does not recognise — so exempting the prefix would let a destination
        // somebody else left notes in pass as empty and be repaired over.
        std::fs::write(dir.path().join(".vtop-repair-notes"), b"not ours").unwrap();
        assert!(
            occupied(dir.path()),
            "only the three names this command writes are exempt, not the prefix"
        );
        std::fs::remove_file(dir.path().join(".vtop-repair-notes")).unwrap();

        std::fs::write(
            dir.path().join("00000000000000000000.segment"),
            b"real data",
        )
        .unwrap();
        assert!(
            occupied(dir.path()),
            "a sealed segment is somebody's history and must block an unmarked repair"
        );
    }

    /// The verdict is on the disk, not merely in the page cache.
    #[test]
    fn the_divergence_verdict_is_written_durably() {
        let dir = tempfile::tempdir().unwrap();
        let diverged = dir.path().join(".vtop-repair-diverged");
        record_divergence(&diverged, "source at 900, prefix ends at 940").unwrap();
        assert_eq!(
            std::fs::read_to_string(&diverged).unwrap(),
            "source at 900, prefix ends at 940"
        );
        // And the precheck built on it agrees, so the two halves are wired
        // together rather than each correct alone.
        check_prior_divergence(dir.path(), &diverged)
            .expect_err("a recorded verdict must refuse the next repair");
    }

    // ----- reconfigure-range (#314) ----------------------------------------

    fn create_range(directory: &std::path::Path) -> SegmentSet {
        SegmentSet::create_in(
            &Env::real(),
            directory,
            vtop_log::SegmentDescriptor {
                segment_id: Uuid::from_u128(81),
                topic: "telemetry".into(),
                topic_epoch: 1,
                lineage: vtop_log::RangeLineage::root(Uuid::from_u128(82)),
                base_offset: 0,
            },
            vtop_log::SegmentConfig {
                max_record_bytes: 256,
                max_group_bytes: 512,
                max_segment_bytes: 512,
                max_segment_records: 100,
                index_stride: 2,
            },
        )
        .expect("the test range must build through the real create path")
    }

    /// The operator path end to end: a directory built through the real
    /// create path, reconfigured through the command's own function, opened
    /// again to prove the change is what a restarted node will read.
    #[test]
    fn reconfigure_range_applies_and_reports_through_the_operator_path() {
        let directory = tempfile::tempdir().unwrap();
        drop(create_range(directory.path()));

        let code = reconfigure_range(
            directory.path(),
            vtop_log::RollThresholds {
                max_segment_bytes: Some(4096),
                max_group_bytes: Some(1024),
                ..Default::default()
            },
            false,
        )
        .expect("reconfiguring a healthy stopped range must succeed");
        assert_eq!(code, 0);

        let reopened = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the reconfigured range must still open");
        assert_eq!(
            reopened.active().config().max_segment_bytes,
            4096,
            "the next node start reads the limits this command just wrote"
        );
    }

    /// The P1 the review caught: an interruption between a roll's seal and
    /// its successor-create leaves a tail-less range `open_in` refuses.
    /// Rerunning the command must BE the recovery, or a routine threshold
    /// change interrupted at the wrong instant strands the range.
    #[test]
    fn reconfigure_range_resumes_a_range_whose_tail_was_sealed_without_a_successor() {
        let directory = tempfile::tempdir().unwrap();
        let set = create_range(directory.path());
        drop(set);
        // Stage the crash layout through the real seal path: recover the
        // tail, append, and seal it with no successor — exactly what an
        // interruption between roll_in's two steps leaves behind.
        let active_path = directory
            .path()
            .join(format!("{}.active", vtop_log::segment_stem(0)));
        let mut active =
            vtop_log::ActiveSegment::recover(&active_path).expect("the created tail must recover");
        active
            .append_group(
                &[vtop_log::LogRecord {
                    producer_id: Uuid::from_u128(83),
                    producer_epoch: 0,
                    sequence: 0,
                    timestamp_millis: 1_700_000_000_000,
                    attributes: 0,
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                }],
                vtop_log::Durability::Fsync,
            )
            .unwrap();
        drop(
            active
                .seal()
                .expect("sealing the tail stages the crash layout"),
        );

        let code = reconfigure_range(
            directory.path(),
            vtop_log::RollThresholds {
                max_segment_bytes: Some(4096),
                max_group_bytes: Some(1024),
                ..Default::default()
            },
            false,
        )
        .expect("rerunning reconfigure-range must recover the interrupted layout, not refuse it");
        assert_eq!(code, 0);

        let reopened = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the resumed range must open as a normal range again");
        assert_eq!(
            reopened.active().config().max_segment_bytes,
            4096,
            "the adopted tail must carry the reconfigured limits"
        );
        assert_eq!(
            reopened.sealed().len(),
            1,
            "the sealed segment from before the interruption is still the prefix"
        );
        assert_eq!(
            reopened.next_offset(),
            1,
            "the record sealed before the interruption is still the range's content"
        );
    }

    #[test]
    fn reconfigure_range_refuses_an_empty_override() {
        let directory = tempfile::tempdir().unwrap();
        drop(create_range(directory.path()));
        let error = reconfigure_range(directory.path(), vtop_log::RollThresholds::default(), false)
            .expect_err("an override that names no threshold has nothing to apply");
        assert!(
            error.contains("--max-"),
            "the refusal must name the flags that fix it, got: {error}"
        );
    }

    #[test]
    fn reconfigure_range_refuses_a_directory_holding_no_range() {
        let directory = tempfile::tempdir().unwrap();
        let error = reconfigure_range(
            directory.path(),
            vtop_log::RollThresholds {
                max_segment_bytes: Some(4096),
                ..Default::default()
            },
            false,
        )
        .expect_err("an empty directory holds nothing to reconfigure");
        assert!(
            error.contains("node YAML"),
            "the refusal must point a not-yet-created range at the YAML path, got: {error}"
        );
    }
}

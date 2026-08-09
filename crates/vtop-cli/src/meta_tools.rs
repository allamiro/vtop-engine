//! `vtopctl meta` — admin client for the metadata Raft group.
//!
//! Unlike `segment` tools these take `--config`: a small YAML describing the
//! admin mTLS endpoint. Secrets stay on disk as PEM paths; the YAML itself
//! never embeds key material.

use clap::{Args, Subcommand};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use vtop_meta::command::{CommandEnvelope, NodeState, MAX_NODE_ADDR_BYTES};
use vtop_meta::{
    resolve_endpoint, AdminCandidate, AdminClient, AdminStatusResponse, MetaNodeId,
    MetadataCommand, MetadataResponse, TlsMaterial, WireLogId,
};

#[derive(Subcommand, Debug)]
pub enum MetaCommand {
    /// Show Raft status and membership from the admin endpoint.
    Status {
        #[command(flatten)]
        common: MetaCommonArgs,
    },
    /// Show the current voter/learner membership.
    Membership {
        #[command(flatten)]
        common: MetaCommonArgs,
    },
    /// Bootstrap a fresh Raft group with these voter ids (#215). One-shot:
    /// every listed node must be running with an empty log.
    Init {
        #[command(flatten)]
        common: MetaCommonArgs,
        /// Comma-separated voter node ids, e.g. `1,2,3`.
        #[arg(long, value_delimiter = ',', required = true)]
        members: Vec<u64>,
    },
    /// Add a learner that replicates the log without joining quorum (#215).
    AddLearner {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        node_id: u64,
    },
    /// Replace the voter set via joint consensus (#215). Removed voters are
    /// fenced: they cannot commit after losing membership.
    ChangeMembership {
        #[command(flatten)]
        common: MetaCommonArgs,
        /// Comma-separated voter node ids, e.g. `1,2,3,4,5`.
        #[arg(long, value_delimiter = ',', required = true)]
        voters: Vec<u64>,
        /// Keep removed voters as learners instead of dropping them.
        #[arg(long, default_value_t = false)]
        retain_removed_as_learners: bool,
    },
    /// Create a topic and its root range.
    CreateTopic {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        name: String,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        root_range_uuid: Uuid,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Read a range's current lease through a linearizable fence (#223).
    ///
    /// The read an election makes before it acts: it reports the holder, the
    /// epoch, and the deadline, so a candidate can tell "the leader is healthy"
    /// apart from "my view was stale".
    RangeLease {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
    },
    /// Propose `RegisterNode` through the Consensus façade.
    RegisterNode {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        node_uuid: Uuid,
        #[arg(long)]
        addr: String,
        #[arg(long)]
        expected_generation: Option<u64>,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Propose `SetNodeState` through the Consensus façade.
    SetNodeState {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        node_uuid: Uuid,
        #[arg(long, value_parser = parse_node_state)]
        state: NodeState,
        #[arg(long)]
        expected_generation: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Propose raw `CommitTierEvidence` (escape hatch for evidence produced
    /// by tooling other than `vtopctl tier copy`).
    CommitTierEvidence {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        #[arg(long)]
        expected_generation: u64,
        /// Sealed content root as 64 hex characters.
        #[arg(long, value_parser = parse_hash32)]
        content_root: [u8; 32],
        #[arg(long)]
        byte_length: u64,
        #[arg(long)]
        backend_id: String,
        #[arg(long)]
        object_uri: String,
        /// Immutable segment-object version id (omit only for unversioned backends).
        #[arg(long)]
        object_version_id: Option<String>,
        /// Immutable manifest version id (omit only for unversioned backends).
        #[arg(long)]
        manifest_version_id: Option<String>,
        /// BLAKE3 digest (64 hex characters) of the canonical manifest core.
        #[arg(long, value_parser = parse_hash32)]
        manifest_core_digest: [u8; 32],
        #[arg(long)]
        verifier_node_uuid: Uuid,
        #[arg(long)]
        fencing_epoch: u64,
        #[arg(long)]
        verified_term: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Propose `SetTopicRetentionPolicy` (create with no
    /// --expected-generation; CAS-update with one).
    SetRetentionPolicy {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        /// Allow retention planning WITHOUT tier evidence for this topic.
        #[arg(long)]
        allow_unarchived_deletion: bool,
        #[arg(long)]
        expected_generation: Option<u64>,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Propose `PlanRetention` (Verified -> RetentionPlanned).
    PlanRetention {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        #[arg(long)]
        expected_generation: u64,
        #[arg(long)]
        fencing_epoch: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Propose `ConfirmRetentionExpired` after every local replica has been
    /// physically deleted (RetentionPlanned -> RetentionExpired).
    ConfirmRetentionExpired {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        #[arg(long)]
        expected_generation: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
    /// Deprecated: propose `CancelRetention`. The state machine always
    /// rejects it (fails closed) since #184; retained only for wire
    /// compatibility.
    CancelRetention {
        #[command(flatten)]
        common: MetaCommonArgs,
        #[arg(long)]
        topic_uuid: Uuid,
        #[arg(long)]
        range_uuid: Uuid,
        #[arg(long)]
        segment_uuid: Uuid,
        #[arg(long)]
        expected_generation: u64,
        #[arg(long, default_value_t = 0)]
        issued_at_ms: i64,
        #[arg(long)]
        request_id: Option<Uuid>,
    },
}

#[derive(Args, Debug)]
pub struct MetaCommonArgs {
    /// Path to a meta admin client YAML (endpoint + PEM paths).
    #[arg(long)]
    pub config: PathBuf,
}

#[derive(Debug, Deserialize)]
struct MetaAdminConfig {
    /// `host:port` of the admin mTLS listener to ask FIRST.
    endpoint: String,
    /// rustls server name (usually matches a SAN on the server cert).
    #[serde(default = "default_server_name")]
    server_name: String,
    ca_cert: PathBuf,
    client_cert: PathBuf,
    client_key: PathBuf,
    /// Every other metadata node this command may be redirected to (#292).
    ///
    /// Reads and writes on this plane must reach the RAFT LEADER, and a
    /// non-leader answers with a redirect naming who to ask. With a single
    /// endpoint there is nowhere to go, so `vtopctl meta create-topic` against
    /// whichever node an operator happened to name fails outright roughly two
    /// times in three — and which node leads depends on an election, so it is
    /// not even stable between invocations.
    ///
    /// Optional and empty by default: a single-node group's only node is
    /// always its leader, so every existing config keeps working untouched.
    #[serde(default)]
    peers: Vec<MetaAdminPeer>,
}

/// One more metadata node `vtopctl` may follow a redirect to.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetaAdminPeer {
    /// The metadata node id, as Raft knows it. Required, because a redirect
    /// names an id: without it the client can only rotate through peers
    /// hopefully rather than going straight to the leader it was told about.
    node_id: u64,
    endpoint: String,
    /// Empty uses the top-level `server_name`, which is right under a shared
    /// SAN and wrong with per-node certificates — so it is per peer here.
    #[serde(default)]
    server_name: String,
}

fn default_server_name() -> String {
    "localhost".to_owned()
}

/// Parse a 64-hex-character BLAKE3 digest / content root into raw bytes.
pub(crate) fn parse_hash32(value: &str) -> Result<[u8; 32], String> {
    blake3::Hash::from_hex(value)
        .map(|hash| *hash.as_bytes())
        .map_err(|_| format!("{value:?} is not a 64-hex-character digest"))
}

fn parse_node_state(value: &str) -> Result<NodeState, String> {
    match value {
        "active" => Ok(NodeState::Active),
        "draining" => Ok(NodeState::Draining),
        "dead" => Ok(NodeState::Dead),
        other => Err(format!(
            "unknown node state {other:?}; expected active|draining|dead"
        )),
    }
}

fn load_admin_config(path: &Path) -> Result<MetaAdminConfig, String> {
    let text =
        fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    serde_yaml::from_str(&text).map_err(|error| format!("parse {}: {error}", path.display()))
}

/// Report that the configured endpoint was not the leader.
///
/// Called from EVERY arm that builds a client, not just the ones that obviously
/// write. `init`, `add-learner`, `change-membership` and the lease read all go
/// through the same dispatch loop and can all be redirected, so leaving them out
/// made the diagnostic quietly untrue for exactly the commands an operator runs
/// while something is already going wrong.
///
/// To stderr, so it never contaminates `--json` output that a script parses.
/// Worth saying at all because it is actionable: an operator pointing at a node
/// that is never the leader is paying an extra round trip on every command, and
/// nothing else would tell them.
fn note_redirects(client: &AdminClient) {
    let followed = client.redirects_followed();
    if followed > 0 {
        eprintln!(
            "note: followed {followed} leader redirect(s); the configured endpoint is not the \
             metadata leader"
        );
    }
}

fn connect(config: &MetaAdminConfig) -> Result<AdminClient, String> {
    let material =
        TlsMaterial::from_pem_files(&config.client_cert, &config.client_key, &config.ca_cert)
            .map_err(|error| error.to_string())?;
    // The configured endpoint first — it is what the operator named, and under
    // co-location it is usually the closest node — then everywhere a redirect
    // could point.
    let mut candidates = vec![AdminCandidate {
        // No id for the primary: it is tried first regardless, and a redirect
        // naming it matches whichever peer entry covers the same node.
        node_id: None,
        endpoint: resolve_endpoint(&config.endpoint).map_err(|error| error.to_string())?,
        server_name: config.server_name.clone(),
        plaintext: false,
    }];
    for peer in &config.peers {
        candidates.push(AdminCandidate {
            node_id: Some(MetaNodeId(peer.node_id)),
            endpoint: resolve_endpoint(&peer.endpoint).map_err(|error| error.to_string())?,
            server_name: if peer.server_name.is_empty() {
                config.server_name.clone()
            } else {
                peer.server_name.clone()
            },
            plaintext: false,
        });
    }
    AdminClient::with_candidates(material, candidates).map_err(|error| error.to_string())
}

/// Dispatch `vtopctl meta` and return a process exit code.
pub async fn run(command: MetaCommand, json: bool) -> i32 {
    match run_inner(command, json).await {
        Ok(()) => 0,
        Err(message) => {
            eprintln!("error: {message}");
            1
        }
    }
}

async fn run_inner(command: MetaCommand, json: bool) -> Result<(), String> {
    match command {
        MetaCommand::Status { common } => {
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            let status = client.status().await.map_err(|error| error.to_string())?;
            note_redirects(&client);
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&status_json(&status))
                        .map_err(|error| error.to_string())?
                );
            } else {
                print_status(&status);
            }
            Ok(())
        }
        MetaCommand::Init { common, members } => {
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            let response = client
                .init(members)
                .await
                .map_err(|error| error.to_string())?;
            note_redirects(&client);
            print_membership_change(&response.membership, "initialized", json)
        }
        MetaCommand::AddLearner { common, node_id } => {
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            let response = client
                .add_learner(node_id)
                .await
                .map_err(|error| error.to_string())?;
            note_redirects(&client);
            print_membership_change(&response.membership, "learner added", json)
        }
        MetaCommand::ChangeMembership {
            common,
            voters,
            retain_removed_as_learners,
        } => {
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            let response = client
                .change_membership(voters, retain_removed_as_learners)
                .await
                .map_err(|error| error.to_string())?;
            note_redirects(&client);
            print_membership_change(&response.membership, "membership changed", json)
        }
        MetaCommand::Membership { common } => {
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            let status = client.status().await.map_err(|error| error.to_string())?;
            note_redirects(&client);
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&membership_json(&status.membership))
                        .map_err(|error| error.to_string())?
                );
            } else {
                println!("voters:");
                for MetaNodeId(id) in &status.membership.voters {
                    println!("  {id}");
                }
                if !status.membership.learners.is_empty() {
                    println!("learners:");
                    for (MetaNodeId(id), addr) in &status.membership.learners {
                        println!("  {id}  {addr}");
                    }
                }
                if let Some(outgoing) = &status.membership.joint_outgoing {
                    println!("joint outgoing voters:");
                    for MetaNodeId(id) in outgoing {
                        println!("  {id}");
                    }
                }
            }
            Ok(())
        }
        MetaCommand::CreateTopic {
            common,
            name,
            topic_uuid,
            root_range_uuid,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::CreateTopic {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                name,
                topic_uuid,
                root_range_uuid,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::RangeLease {
            common,
            topic_uuid,
            range_uuid,
        } => {
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            let view = client
                .read_range_lease(topic_uuid, range_uuid)
                .await
                .map_err(|error| error.to_string())?;
            note_redirects(&client);
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "found": view.found,
                        "range_generation": view.range_generation,
                        "fencing_epoch": view.fencing_epoch,
                        "read_at_applied_index": view.read_at_applied_index,
                        "lease": view.lease.map(|lease| serde_json::json!({
                            "holder_node_uuid": lease.holder_node_uuid,
                            "fencing_epoch": lease.fencing_epoch,
                            "expires_at_ms": lease.expires_at_ms,
                        })),
                    }))
                    .map_err(|error| error.to_string())?
                );
            } else if !view.found {
                println!("range not found");
            } else {
                match view.lease {
                    None => println!(
                        "range generation={} fencing_epoch={} lease=none",
                        view.range_generation, view.fencing_epoch
                    ),
                    Some(lease) => println!(
                        "range generation={} fencing_epoch={} holder={} lease_epoch={} expires_at_ms={}",
                        view.range_generation,
                        view.fencing_epoch,
                        lease.holder_node_uuid,
                        lease.fencing_epoch,
                        lease
                            .expires_at_ms
                            .map(|ms| ms.to_string())
                            .unwrap_or_else(|| "never".to_owned()),
                    ),
                }
            }
            Ok(())
        }
        MetaCommand::RegisterNode {
            common,
            node_uuid,
            addr,
            expected_generation,
            issued_at_ms,
            request_id,
        } => {
            if addr.is_empty() || addr.len() > MAX_NODE_ADDR_BYTES {
                return Err(format!("addr must be 1..={MAX_NODE_ADDR_BYTES} bytes"));
            }
            let command = MetadataCommand::RegisterNode {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                node_uuid,
                addr,
                expected_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::SetNodeState {
            common,
            node_uuid,
            state,
            expected_generation,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::SetNodeState {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                node_uuid,
                state,
                expected_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::CommitTierEvidence {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            expected_generation,
            content_root,
            byte_length,
            backend_id,
            object_uri,
            object_version_id,
            manifest_version_id,
            manifest_core_digest,
            verifier_node_uuid,
            fencing_epoch,
            verified_term,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::CommitTierEvidence {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation: expected_generation,
                content_root,
                byte_length,
                backend_id,
                object_uri,
                object_version_id,
                manifest_version_id,
                manifest_core_digest,
                verification_method: vtop_meta::VerificationMethod::AuthenticatedContentRoot,
                verifier_node_uuid,
                fencing_epoch,
                verified_term,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::SetRetentionPolicy {
            common,
            topic_uuid,
            allow_unarchived_deletion,
            expected_generation,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::SetTopicRetentionPolicy {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                unarchived_deletion_allowed: allow_unarchived_deletion,
                expected_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::PlanRetention {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            expected_generation,
            fencing_epoch,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::PlanRetention {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation: expected_generation,
                fencing_epoch,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::ConfirmRetentionExpired {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            expected_generation,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::ConfirmRetentionExpired {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation: expected_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
        MetaCommand::CancelRetention {
            common,
            topic_uuid,
            range_uuid,
            segment_uuid,
            expected_generation,
            issued_at_ms,
            request_id,
        } => {
            let command = MetadataCommand::CancelRetention {
                env: CommandEnvelope {
                    request_id: request_id.unwrap_or_else(Uuid::new_v4),
                    issued_at_ms,
                },
                topic_uuid,
                range_uuid,
                segment_uuid,
                expected_segment_generation: expected_generation,
            };
            propose_and_print(&common.config, command, json).await
        }
    }
}

pub(crate) async fn propose_and_print(
    config_path: &Path,
    command: MetadataCommand,
    json: bool,
) -> Result<(), String> {
    let config = load_admin_config(config_path)?;
    let client = connect(&config)?;
    let response = client
        .propose(command)
        .await
        .map_err(|error| error.to_string())?;
    note_redirects(&client);
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&propose_json(&response.log_id, &response.response))
                .map_err(|error| error.to_string())?
        );
    } else {
        println!(
            "committed term={} index={}",
            response.log_id.term, response.log_id.index
        );
        print_response(&response.response);
    }
    if matches!(response.response, MetadataResponse::Rejected(_)) {
        return Err("metadata command was rejected".to_owned());
    }
    Ok(())
}

fn status_json(status: &AdminStatusResponse) -> serde_json::Value {
    serde_json::json!({
        "node_id": status.node_id.0,
        "current_term": status.current_term,
        "vote": {
            "term": status.vote.term,
            "voted_for": status.vote.voted_for.map(|MetaNodeId(id)| id),
            "vote_committed": status.vote.vote_committed,
        },
        "current_leader": status.current_leader.map(|MetaNodeId(id)| id),
        "server_state": status.server_state,
        "last_applied": status.last_applied.map(|WireLogId { term, index }| {
            serde_json::json!({ "term": term, "index": index })
        }),
        "membership": membership_json(&status.membership),
    })
}

fn membership_json(membership: &vtop_meta::MetaMembership) -> serde_json::Value {
    let voters: Vec<_> = membership.voters.iter().map(|MetaNodeId(id)| *id).collect();
    let learners: Vec<_> = membership
        .learners
        .iter()
        .map(|(MetaNodeId(id), addr)| serde_json::json!({ "id": id, "addr": addr }))
        .collect();
    let joint_outgoing_voters = membership.joint_outgoing.as_ref().map(|outgoing| {
        outgoing
            .iter()
            .map(|MetaNodeId(id)| *id)
            .collect::<Vec<_>>()
    });
    serde_json::json!({
        "voters": voters,
        "learners": learners,
        "joint_outgoing_voters": joint_outgoing_voters,
    })
}

fn print_membership_change(
    membership: &vtop_meta::MetaMembership,
    action: &str,
    json: bool,
) -> Result<(), String> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "status": "ok",
                "action": action,
                "membership": membership_json(membership),
            }))
            .map_err(|error| error.to_string())?
        );
    } else {
        let voters: Vec<_> = membership.voters.iter().map(ToString::to_string).collect();
        println!("{action}; voters: {}", voters.join(", "));
    }
    Ok(())
}

fn propose_json(log_id: &WireLogId, response: &MetadataResponse) -> serde_json::Value {
    serde_json::json!({
        "log_id": { "term": log_id.term, "index": log_id.index },
        "response": format!("{response:?}"),
    })
}

fn print_status(status: &AdminStatusResponse) {
    println!("node_id:        {}", status.node_id);
    println!("term:           {}", status.current_term);
    println!("server_state:   {}", status.server_state);
    println!(
        "leader:         {}",
        status
            .current_leader
            .map(|id| id.to_string())
            .unwrap_or_else(|| "-".to_owned())
    );
    if let Some(applied) = status.last_applied {
        println!(
            "last_applied:   term={} index={}",
            applied.term, applied.index
        );
    } else {
        println!("last_applied:   -");
    }
    print!("voters:         ");
    let voters: Vec<_> = status
        .membership
        .voters
        .iter()
        .map(|id| id.to_string())
        .collect();
    println!("{}", voters.join(", "));
    if let Some(outgoing) = &status.membership.joint_outgoing {
        let outgoing: Vec<_> = outgoing.iter().map(ToString::to_string).collect();
        println!("joint outgoing: {}", outgoing.join(", "));
    }
}

fn print_response(response: &MetadataResponse) {
    match response {
        MetadataResponse::Ack { generation } => println!("ack generation={generation}"),
        MetadataResponse::TopicCreated {
            topic_uuid,
            topic_epoch,
            root_range_uuid,
        } => println!(
            "topic_created uuid={topic_uuid} epoch={topic_epoch} root_range={root_range_uuid}"
        ),
        MetadataResponse::LeaseGranted { fencing_epoch } => {
            println!("lease_granted fencing_epoch={fencing_epoch}")
        }
        MetadataResponse::GroupCreated {
            group_uuid,
            generation,
        } => println!("group_created uuid={group_uuid} generation={generation}"),
        MetadataResponse::MemberJoined {
            member_generation,
            group_generation,
        } => println!(
            "member_joined member_generation={member_generation} group_generation={group_generation}"
        ),
        MetadataResponse::CursorCommitted {
            checkpoint_generation,
        } => println!("cursor_committed checkpoint_generation={checkpoint_generation}"),
        MetadataResponse::Rejected(error) => println!("rejected: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_node_state_accepts_canonical_names() {
        assert_eq!(parse_node_state("active").unwrap(), NodeState::Active);
        assert_eq!(parse_node_state("draining").unwrap(), NodeState::Draining);
        assert!(parse_node_state("online").is_err());
    }

    #[test]
    fn meta_admin_config_deserializes() {
        let yaml = r#"
endpoint: "127.0.0.1:9701"
ca_cert: /tmp/ca.pem
client_cert: /tmp/client.pem
client_key: /tmp/client.key
"#;
        let config: MetaAdminConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.endpoint, "127.0.0.1:9701");
        assert_eq!(config.server_name, "localhost");
    }
}

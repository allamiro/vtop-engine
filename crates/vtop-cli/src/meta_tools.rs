//! `vtopctl meta` — admin client for the metadata Raft group.
//!
//! Unlike `segment` tools these take `--config`: a small YAML describing the
//! admin mTLS endpoint. Secrets stay on disk as PEM paths; the YAML itself
//! never embeds key material.

use clap::{Args, Subcommand};
use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use vtop_meta::command::{CommandEnvelope, NodeState, MAX_NODE_ADDR_BYTES};
use vtop_meta::{
    AdminClient, AdminStatusResponse, MetaNodeId, MetadataCommand, MetadataResponse, TlsMaterial,
    WireLogId,
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
}

#[derive(Args, Debug)]
pub struct MetaCommonArgs {
    /// Path to a meta admin client YAML (endpoint + PEM paths).
    #[arg(long)]
    pub config: PathBuf,
}

#[derive(Debug, Deserialize)]
struct MetaAdminConfig {
    /// `host:port` of the admin mTLS listener.
    endpoint: String,
    /// rustls server name (usually matches a SAN on the server cert).
    #[serde(default = "default_server_name")]
    server_name: String,
    ca_cert: PathBuf,
    client_cert: PathBuf,
    client_key: PathBuf,
}

fn default_server_name() -> String {
    "localhost".to_owned()
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

fn connect(config: &MetaAdminConfig) -> Result<AdminClient, String> {
    let endpoint: SocketAddr = config
        .endpoint
        .parse()
        .map_err(|error| format!("endpoint {}: {error}", config.endpoint))?;
    let material =
        TlsMaterial::from_pem_files(&config.client_cert, &config.client_key, &config.ca_cert)
            .map_err(|error| error.to_string())?;
    AdminClient::new(material, endpoint, config.server_name.clone())
        .map_err(|error| error.to_string())
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
        MetaCommand::Membership { common } => {
            let config = load_admin_config(&common.config)?;
            let client = connect(&config)?;
            let status = client.status().await.map_err(|error| error.to_string())?;
            if json {
                let voters: Vec<_> = status
                    .membership
                    .voters
                    .iter()
                    .map(|MetaNodeId(id)| *id)
                    .collect();
                let learners: Vec<_> = status
                    .membership
                    .learners
                    .iter()
                    .map(|(MetaNodeId(id), addr)| serde_json::json!({ "id": id, "addr": addr }))
                    .collect();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "voters": voters,
                        "learners": learners,
                    }))
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
    }
}

async fn propose_and_print(
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
    Ok(())
}

fn status_json(status: &AdminStatusResponse) -> serde_json::Value {
    let voters: Vec<_> = status
        .membership
        .voters
        .iter()
        .map(|MetaNodeId(id)| *id)
        .collect();
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
        "membership": { "voters": voters },
    })
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

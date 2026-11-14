//! The client's audit records of cluster commands (#694), through the real
//! binary against an in-process admin endpoint: `vtopctl meta` records who
//! asked, for what, and what came back, under the request id the metadata
//! nodes record the same command by.

use async_trait::async_trait;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use vtop_meta::transport::{
    stub_status, AdminAuthorizer, AdminHandler, AdminMembershipResponse, AdminProposeResponse,
    AdminReadRangeLeaseRequest, AdminReadRangeLeaseResponse, AdminReadSegmentPlacementRequest,
    AdminReadSegmentPlacementResponse, AdminServer, AdminStatusResponse, TransportError,
    TransportResult, WireLogId,
};
use vtop_meta::{MetaNodeId, MetadataCommand, MetadataResponse};

/// Commits every proposal at index 41, or refuses it as an endpoint whose
/// policy does not allow the caller would; refuses membership changes.
struct Endpoint {
    refuse: bool,
}

#[async_trait]
impl AdminHandler for Endpoint {
    async fn status(&self) -> TransportResult<AdminStatusResponse> {
        Ok(stub_status(MetaNodeId(1)))
    }
    async fn propose(&self, _: MetadataCommand) -> TransportResult<AdminProposeResponse> {
        if self.refuse {
            return Err(TransportError::Unauthorized(
                "operator \"ops-bob\" is not a configured operator".to_owned(),
            ));
        }
        Ok(AdminProposeResponse {
            log_id: WireLogId { term: 2, index: 41 },
            response: MetadataResponse::Ack { generation: 1 },
        })
    }
    async fn init(&self, _: Vec<u64>) -> TransportResult<AdminMembershipResponse> {
        Err(TransportError::Protocol("already initialized".to_owned()))
    }
    async fn add_learner(&self, _: u64) -> TransportResult<AdminMembershipResponse> {
        Err(TransportError::Protocol("unused".to_owned()))
    }
    async fn change_membership(
        &self,
        _: Vec<u64>,
        _: bool,
    ) -> TransportResult<AdminMembershipResponse> {
        Err(TransportError::Protocol("unused".to_owned()))
    }
    async fn read_range_lease(
        &self,
        _: AdminReadRangeLeaseRequest,
    ) -> TransportResult<AdminReadRangeLeaseResponse> {
        Err(TransportError::Protocol("unused".to_owned()))
    }
    async fn read_segment_placement(
        &self,
        _: AdminReadSegmentPlacementRequest,
    ) -> TransportResult<AdminReadSegmentPlacementResponse> {
        Err(TransportError::Protocol("unused".to_owned()))
    }
}

async fn serve(refuse: bool, dir: &Path) -> (std::path::PathBuf, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = AdminServer::plaintext(
        Arc::new(Endpoint { refuse }) as Arc<dyn AdminHandler>,
        AdminAuthorizer::permissive(),
    )
    .unwrap();
    let task = tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    let config = dir.join("meta.yaml");
    std::fs::write(
        &config,
        format!("endpoint: \"{addr}\"\ntransport: plaintext\n"),
    )
    .unwrap();
    (config, task)
}

async fn vtopctl(args: Vec<String>, audit_log: &Path) -> Output {
    let audit_log = audit_log.to_owned();
    tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_vtopctl"))
            .args(&args)
            .env("VTOP_AUDIT_LOG", &audit_log)
            .env_remove("RUST_LOG")
            .env_remove("VTOP_LOG_FORMAT")
            .stdin(Stdio::null())
            .output()
            .expect("spawn vtopctl")
    })
    .await
    .unwrap()
}

fn records(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// `meta enable-features` and `meta create-topic` leave the client's record:
/// the local actor, the command, the request id the metadata nodes share,
/// and the index it committed at.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_meta_command_leaves_the_clients_record_under_its_request_id() {
    let dir = tempfile::tempdir().unwrap();
    let (config, task) = serve(false, dir.path()).await;
    let audit_log = dir.path().join("audit.jsonl");
    let request_id = "00000000-0000-0000-0000-000000000694";
    let out = vtopctl(
        vec![
            "meta".into(),
            "enable-features".into(),
            "--config".into(),
            config.display().to_string(),
            "--level".into(),
            "2".into(),
            "--request-id".into(),
            request_id.into(),
        ],
        &audit_log,
    )
    .await;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let file = records(&audit_log);
    assert_eq!(file.len(), 1, "{file:#?}");
    let record = &file[0];
    assert_eq!(record["action"], "cluster.enable_features");
    assert_eq!(record["stage"], "client");
    assert_eq!(record["outcome"], "success");
    assert_eq!(record["actor"]["kind"], "os");
    assert_eq!(record["params"]["level"], "2");
    assert_eq!(record["params"]["command"], "SetFeatureLevel");
    assert_eq!(record["correlation"]["request_id"], request_id);
    assert_eq!(record["correlation"]["log_index"], 41);
    task.abort();
}

/// Refused by the endpoint: the client records `refused` with the reason; a
/// bootstrap that fails records `failure`. Each is one record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_or_failed_cluster_command_is_recorded_as_such() {
    let dir = tempfile::tempdir().unwrap();
    let (config, task) = serve(true, dir.path()).await;
    let audit_log = dir.path().join("audit.jsonl");
    let out = vtopctl(
        vec![
            "meta".into(),
            "create-topic".into(),
            "--config".into(),
            config.display().to_string(),
            "--name".into(),
            "events.v1".into(),
            "--topic-uuid".into(),
            "00000000-0000-0000-0000-000000000001".into(),
            "--root-range-uuid".into(),
            "00000000-0000-0000-0000-000000000002".into(),
        ],
        &audit_log,
    )
    .await;
    assert!(!out.status.success());
    let out = vtopctl(
        vec![
            "meta".into(),
            "init".into(),
            "--config".into(),
            config.display().to_string(),
            "--members".into(),
            "1,2,3".into(),
        ],
        &audit_log,
    )
    .await;
    assert!(!out.status.success());
    let file = records(&audit_log);
    assert_eq!(file.len(), 2, "{file:#?}");
    assert_eq!(file[0]["action"], "cluster.create_topic");
    assert_eq!(file[0]["outcome"], "refused");
    assert!(file[0]["reason"]
        .as_str()
        .unwrap()
        .contains("not a configured operator"));
    assert_eq!(file[0]["params"]["name"], "events.v1");
    assert_eq!(file[1]["action"], "cluster.init");
    assert_eq!(file[1]["outcome"], "failure");
    assert_eq!(file[1]["params"]["members"], "1,2,3");
    task.abort();
}

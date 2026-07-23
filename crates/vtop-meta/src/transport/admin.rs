//! Admin/control surface over VTPM mTLS.
//!
//! Operators (and `vtopctl meta`) talk to a single admin endpoint that forwards
//! status and propose requests through the [`crate::raft::consensus::Consensus`]
//! façade. Openraft types never cross this boundary.

use super::tls::{server_name, TlsMaterial};
use super::wire::{
    read_frame, write_frame, AdminError, AdminProposeRequest, AdminProposeResponse,
    AdminStatusRequest, AdminStatusResponse, TransportError, TransportResult, VtpmFrame,
    KIND_ADMIN_ERROR, KIND_ADMIN_PROPOSE_REQ, KIND_ADMIN_PROPOSE_RESP, KIND_ADMIN_STATUS_REQ,
    KIND_ADMIN_STATUS_RESP,
};
use crate::command::MetadataCommand;
use crate::keys::MetaNodeId;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// Backend for admin RPCs — typically [`crate::raft::consensus::OpenraftConsensus`].
#[async_trait]
pub trait AdminHandler: Send + Sync {
    async fn status(&self) -> TransportResult<AdminStatusResponse>;
    async fn propose(&self, command: MetadataCommand) -> TransportResult<AdminProposeResponse>;
}

/// mTLS admin server.
pub struct AdminServer {
    acceptor: TlsAcceptor,
    handler: Arc<dyn AdminHandler>,
}

impl AdminServer {
    pub fn new(material: TlsMaterial, handler: Arc<dyn AdminHandler>) -> TransportResult<Self> {
        Ok(Self {
            acceptor: super::tls::build_server_acceptor(material)?,
            handler,
        })
    }

    pub async fn serve(self, listener: TcpListener) -> TransportResult<()> {
        loop {
            let (tcp, _) = listener.accept().await?;
            let acceptor = self.acceptor.clone();
            let handler = Arc::clone(&self.handler);
            tokio::spawn(async move {
                let _ = serve_admin_connection(acceptor, tcp, handler).await;
            });
        }
    }
}

async fn serve_admin_connection(
    acceptor: TlsAcceptor,
    tcp: TcpStream,
    handler: Arc<dyn AdminHandler>,
) -> TransportResult<()> {
    let mut stream = acceptor
        .accept(tcp)
        .await
        .map_err(|error| TransportError::Tls(format!("admin accept: {error}")))?;
    loop {
        let frame = match read_frame(&mut stream).await {
            Ok(frame) => frame,
            Err(TransportError::Closed) => return Ok(()),
            Err(error) => return Err(error),
        };
        let response = match dispatch_admin(handler.as_ref(), frame).await {
            Ok(frame) => frame,
            Err(error) => VtpmFrame {
                kind: KIND_ADMIN_ERROR,
                payload: AdminError {
                    message: truncate_error(&error.to_string()),
                }
                .encode()?,
            },
        };
        write_frame(&mut stream, &response).await?;
    }
}

fn truncate_error(message: &str) -> String {
    let max = crate::command::MAX_ERROR_DETAIL_BYTES;
    if message.len() <= max {
        message.to_owned()
    } else {
        message.chars().take(max).collect()
    }
}

async fn dispatch_admin(
    handler: &dyn AdminHandler,
    frame: VtpmFrame,
) -> TransportResult<VtpmFrame> {
    match frame.kind {
        KIND_ADMIN_STATUS_REQ => {
            AdminStatusRequest::decode(&frame.payload)?;
            let response = handler.status().await?;
            Ok(VtpmFrame {
                kind: KIND_ADMIN_STATUS_RESP,
                payload: response.encode()?,
            })
        }
        KIND_ADMIN_PROPOSE_REQ => {
            let request = AdminProposeRequest::decode(&frame.payload)?;
            let response = handler.propose(request.command).await?;
            Ok(VtpmFrame {
                kind: KIND_ADMIN_PROPOSE_RESP,
                payload: response.encode()?,
            })
        }
        other => Err(TransportError::UnexpectedKind(other)),
    }
}

/// Client for the admin endpoint.
pub struct AdminClient {
    connector: TlsConnector,
    server_name: String,
    endpoint: SocketAddr,
}

impl AdminClient {
    pub fn new(
        material: TlsMaterial,
        endpoint: SocketAddr,
        server_name: impl Into<String>,
    ) -> TransportResult<Self> {
        Ok(Self {
            connector: super::tls::build_client_connector(material)?,
            server_name: server_name.into(),
            endpoint,
        })
    }

    pub async fn status(&self) -> TransportResult<AdminStatusResponse> {
        let frame = self
            .round_trip(VtpmFrame {
                kind: KIND_ADMIN_STATUS_REQ,
                payload: AdminStatusRequest.encode(),
            })
            .await?;
        match frame.kind {
            KIND_ADMIN_STATUS_RESP => Ok(AdminStatusResponse::decode(&frame.payload)?),
            KIND_ADMIN_ERROR => {
                let error = AdminError::decode(&frame.payload)?;
                Err(TransportError::Protocol(error.message))
            }
            other => Err(TransportError::UnexpectedKind(other)),
        }
    }

    pub async fn propose(&self, command: MetadataCommand) -> TransportResult<AdminProposeResponse> {
        let frame = self
            .round_trip(VtpmFrame {
                kind: KIND_ADMIN_PROPOSE_REQ,
                payload: AdminProposeRequest { command }.encode()?,
            })
            .await?;
        match frame.kind {
            KIND_ADMIN_PROPOSE_RESP => Ok(AdminProposeResponse::decode(&frame.payload)?),
            KIND_ADMIN_ERROR => {
                let error = AdminError::decode(&frame.payload)?;
                Err(TransportError::Protocol(error.message))
            }
            other => Err(TransportError::UnexpectedKind(other)),
        }
    }

    async fn round_trip(&self, request: VtpmFrame) -> TransportResult<VtpmFrame> {
        let tcp = TcpStream::connect(self.endpoint).await?;
        let name = server_name(&self.server_name)?;
        let mut stream = self
            .connector
            .connect(name, tcp)
            .await
            .map_err(|error| TransportError::Tls(format!("admin connect: {error}")))?;
        write_frame(&mut stream, &request).await?;
        read_frame(&mut stream).await
    }
}

/// Convenience: build a static status response for tests / stubs.
pub fn stub_status(node_id: MetaNodeId) -> AdminStatusResponse {
    use crate::storage::hardstate::HardState;
    use crate::storage::log::MetaMembership;
    AdminStatusResponse {
        node_id,
        current_term: 0,
        vote: HardState::default(),
        current_leader: None,
        server_state: "Learner".to_owned(),
        last_applied: None,
        membership: MetaMembership::default(),
    }
}

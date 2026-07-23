//! Framed mTLS peer connection and request/response exchange.
//!
//! A connection is one TCP+TLS session that carries VTPM request/response
//! pairs. The [`crate::raft::network`] adapter opens a short-lived
//! connection per RPC in this slice (no connection pool yet).

use super::tls::{assert_peer_identity, server_name, TlsMaterial};
use super::wire::{
    read_frame, write_frame, PeerAppendRequest, PeerAppendResponse, PeerInstallRequest,
    PeerInstallResponse, PeerVoteRequest, PeerVoteResponse, TransportError, TransportResult,
    VtpmFrame, KIND_APPEND_REQ, KIND_APPEND_RESP, KIND_INSTALL_REQ, KIND_INSTALL_RESP,
    KIND_VOTE_REQ, KIND_VOTE_RESP,
};
use crate::keys::MetaNodeId;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::server::TlsStream as ServerTlsStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// Application handler for peer RPCs (implemented by the raft adapter).
#[async_trait]
pub trait PeerRpcHandler: Send + Sync {
    async fn handle_vote(&self, request: PeerVoteRequest) -> TransportResult<PeerVoteResponse>;
    async fn handle_append(
        &self,
        request: PeerAppendRequest,
    ) -> TransportResult<PeerAppendResponse>;
    async fn handle_install(
        &self,
        request: PeerInstallRequest,
    ) -> TransportResult<PeerInstallResponse>;
}

/// Accept mTLS peer connections and dispatch VTPM RPCs to `handler`.
pub struct PeerServer {
    acceptor: TlsAcceptor,
    local_id: MetaNodeId,
    handler: Arc<dyn PeerRpcHandler>,
}

impl PeerServer {
    pub fn new(
        material: TlsMaterial,
        local_id: MetaNodeId,
        handler: Arc<dyn PeerRpcHandler>,
    ) -> TransportResult<Self> {
        Ok(Self {
            acceptor: super::tls::build_server_acceptor(material)?,
            local_id,
            handler,
        })
    }

    /// Serve until the listener is closed. Spawns one task per accepted
    /// connection. Not deterministic under wall-clock scheduling.
    pub async fn serve(self, listener: TcpListener) -> TransportResult<()> {
        loop {
            let (tcp, peer_addr) = listener.accept().await?;
            let acceptor = self.acceptor.clone();
            let handler = Arc::clone(&self.handler);
            let local_id = self.local_id;
            tokio::spawn(async move {
                if let Err(error) = serve_connection(acceptor, tcp, handler, local_id).await {
                    tracing_peer_error(peer_addr, error);
                }
            });
        }
    }
}

fn tracing_peer_error(peer: SocketAddr, error: TransportError) {
    // Avoid a tracing dependency in this crate: stderr is enough for the
    // skeleton; operators wire structured logs at the process boundary.
    let _ = (peer, error);
}

async fn serve_connection(
    acceptor: TlsAcceptor,
    tcp: TcpStream,
    handler: Arc<dyn PeerRpcHandler>,
    _local_id: MetaNodeId,
) -> TransportResult<()> {
    let mut stream = acceptor
        .accept(tcp)
        .await
        .map_err(|error| TransportError::Tls(format!("peer accept handshake: {error}")))?;
    // Authenticated client cert is required by the verifier; extract id for
    // logging / future ACL. Peer RPCs are authorized by membership, not CN
    // alone — the raft core rejects unknown voters.
    let _peer_id = peer_id_from_server_stream(&stream)?;
    loop {
        let frame = match read_frame(&mut stream).await {
            Ok(frame) => frame,
            Err(TransportError::Closed) => return Ok(()),
            Err(error) => return Err(error),
        };
        let response = dispatch_peer_rpc(handler.as_ref(), frame).await?;
        write_frame(&mut stream, &response).await?;
    }
}

fn peer_id_from_server_stream(stream: &ServerTlsStream<TcpStream>) -> TransportResult<MetaNodeId> {
    let (_, conn) = stream.get_ref();
    let certs = conn.peer_certificates();
    let leaf = certs
        .and_then(|c| c.first())
        .ok_or_else(|| TransportError::Identity("peer presented no certificate".to_owned()))?;
    super::tls::meta_node_id_from_cert(leaf)
}

async fn dispatch_peer_rpc(
    handler: &dyn PeerRpcHandler,
    frame: VtpmFrame,
) -> TransportResult<VtpmFrame> {
    match frame.kind {
        KIND_VOTE_REQ => {
            let request = PeerVoteRequest::decode(&frame.payload)?;
            let response = handler.handle_vote(request).await?;
            Ok(VtpmFrame {
                kind: KIND_VOTE_RESP,
                payload: response.encode()?,
            })
        }
        KIND_APPEND_REQ => {
            let request = PeerAppendRequest::decode(&frame.payload)?;
            let response = handler.handle_append(request).await?;
            Ok(VtpmFrame {
                kind: KIND_APPEND_RESP,
                payload: response.encode()?,
            })
        }
        KIND_INSTALL_REQ => {
            let request = PeerInstallRequest::decode(&frame.payload)?;
            let response = handler.handle_install(request).await?;
            Ok(VtpmFrame {
                kind: KIND_INSTALL_RESP,
                payload: response.encode()?,
            })
        }
        other => Err(TransportError::UnexpectedKind(other)),
    }
}

/// Client-side peer RPC helper: connect, exchange one request/response, close.
pub struct PeerClient {
    connector: TlsConnector,
    server_name: String,
    /// Expected peer MetaNodeId (must match peer leaf CN).
    expected_peer: MetaNodeId,
}

impl PeerClient {
    pub fn new(
        material: TlsMaterial,
        server_name: impl Into<String>,
        expected_peer: MetaNodeId,
    ) -> TransportResult<Self> {
        Ok(Self {
            connector: super::tls::build_client_connector(material)?,
            server_name: server_name.into(),
            expected_peer,
        })
    }

    pub async fn vote(
        &self,
        addr: SocketAddr,
        request: &PeerVoteRequest,
    ) -> TransportResult<PeerVoteResponse> {
        let frame = self
            .round_trip(
                addr,
                VtpmFrame {
                    kind: KIND_VOTE_REQ,
                    payload: request.encode()?,
                },
                KIND_VOTE_RESP,
            )
            .await?;
        Ok(PeerVoteResponse::decode(&frame.payload)?)
    }

    pub async fn append(
        &self,
        addr: SocketAddr,
        request: &PeerAppendRequest,
    ) -> TransportResult<PeerAppendResponse> {
        let frame = self
            .round_trip(
                addr,
                VtpmFrame {
                    kind: KIND_APPEND_REQ,
                    payload: request.encode()?,
                },
                KIND_APPEND_RESP,
            )
            .await?;
        Ok(PeerAppendResponse::decode(&frame.payload)?)
    }

    pub async fn install(
        &self,
        addr: SocketAddr,
        request: &PeerInstallRequest,
    ) -> TransportResult<PeerInstallResponse> {
        let frame = self
            .round_trip(
                addr,
                VtpmFrame {
                    kind: KIND_INSTALL_REQ,
                    payload: request.encode()?,
                },
                KIND_INSTALL_RESP,
            )
            .await?;
        Ok(PeerInstallResponse::decode(&frame.payload)?)
    }

    async fn round_trip(
        &self,
        addr: SocketAddr,
        request: VtpmFrame,
        expected_kind: u16,
    ) -> TransportResult<VtpmFrame> {
        let tcp = TcpStream::connect(addr).await?;
        let name = server_name(&self.server_name)?;
        let mut stream = self
            .connector
            .connect(name, tcp)
            .await
            .map_err(|error| TransportError::Tls(format!("peer connect: {error}")))?;
        let (_, conn) = stream.get_ref();
        assert_peer_identity(conn.peer_certificates(), self.expected_peer)?;
        write_frame(&mut stream, &request).await?;
        let response = read_frame(&mut stream).await?;
        if response.kind != expected_kind {
            return Err(TransportError::UnexpectedKind(response.kind));
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::hardstate::HardState;
    use crate::transport::tls::{
        assert_peer_identity, build_client_connector, build_server_acceptor,
    };
    use crate::transport::wire::WireLogId;
    use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
    use rustls::pki_types::PrivatePkcs8KeyDer;
    use rustls::RootCertStore;
    use std::time::Duration;

    struct EchoHandler;

    #[async_trait]
    impl PeerRpcHandler for EchoHandler {
        async fn handle_vote(&self, request: PeerVoteRequest) -> TransportResult<PeerVoteResponse> {
            Ok(PeerVoteResponse {
                vote: request.vote,
                vote_granted: true,
                last_log_id: request.last_log_id,
            })
        }

        async fn handle_append(
            &self,
            _request: PeerAppendRequest,
        ) -> TransportResult<PeerAppendResponse> {
            Ok(PeerAppendResponse::Success)
        }

        async fn handle_install(
            &self,
            request: PeerInstallRequest,
        ) -> TransportResult<PeerInstallResponse> {
            Ok(PeerInstallResponse { vote: request.vote })
        }
    }

    /// Self-signed leaf with decimal CN.
    fn mint_leaf(
        cn: &str,
    ) -> (
        rustls::pki_types::CertificateDer<'static>,
        PrivatePkcs8KeyDer<'static>,
    ) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        params.distinguished_name = DistinguishedName::new();
        params.distinguished_name.push(DnType::CommonName, cn);
        let cert = params.self_signed(&key).unwrap();
        (
            cert.der().clone(),
            PrivatePkcs8KeyDer::from(key.serialize_der()),
        )
    }

    #[tokio::test]
    async fn loopback_mtls_vote_round_trip() {
        // Mint both leaves first so each side can trust the other.
        let (server_leaf, server_key) = mint_leaf("1");
        let (client_leaf, client_key) = mint_leaf("2");

        let mut server_roots = RootCertStore::empty();
        server_roots.add(client_leaf.clone()).unwrap();
        let server_material = TlsMaterial {
            certificate_chain: vec![server_leaf.clone()],
            private_key: server_key.into(),
            trust_roots: server_roots,
        };

        let mut client_roots = RootCertStore::empty();
        client_roots.add(server_leaf).unwrap();
        let client_material = TlsMaterial {
            certificate_chain: vec![client_leaf],
            private_key: client_key.into(),
            trust_roots: client_roots,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server =
            PeerServer::new(server_material, MetaNodeId(1), Arc::new(EchoHandler)).unwrap();
        let server_task = tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });

        // Wall-clock settle; this live test is not seed-deterministic.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let client = PeerClient::new(client_material, "localhost", MetaNodeId(1)).unwrap();
        let response = client
            .vote(
                addr,
                &PeerVoteRequest {
                    vote: HardState {
                        term: 3,
                        voted_for: Some(MetaNodeId(2)),
                        vote_committed: false,
                    },
                    last_log_id: Some(WireLogId { term: 2, index: 5 }),
                },
            )
            .await
            .unwrap();
        assert!(response.vote_granted);
        assert_eq!(response.vote.term, 3);
        assert_eq!(response.last_log_id, Some(WireLogId { term: 2, index: 5 }));

        server_task.abort();
    }

    #[tokio::test]
    async fn mtls_rejects_wrong_peer_cn() {
        let (server_leaf, server_key) = mint_leaf("1");
        let (client_leaf, client_key) = mint_leaf("2");

        let mut server_roots = RootCertStore::empty();
        server_roots.add(client_leaf.clone()).unwrap();
        let server_material = TlsMaterial {
            certificate_chain: vec![server_leaf.clone()],
            private_key: server_key.into(),
            trust_roots: server_roots,
        };

        let mut client_roots = RootCertStore::empty();
        client_roots.add(server_leaf).unwrap();
        let client_material = TlsMaterial {
            certificate_chain: vec![client_leaf],
            private_key: client_key.into(),
            trust_roots: client_roots,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = build_server_acceptor(server_material).unwrap();
        let server_task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let _ = acceptor.accept(tcp).await;
        });

        let connector = build_client_connector(client_material).unwrap();
        let tcp = TcpStream::connect(addr).await.unwrap();
        let name = server_name("localhost").unwrap();
        let stream = connector.connect(name, tcp).await.unwrap();
        let (_, conn) = stream.get_ref();
        let err = assert_peer_identity(conn.peer_certificates(), MetaNodeId(9)).unwrap_err();
        assert!(matches!(err, TransportError::Identity(_)));
        server_task.abort();
    }
}

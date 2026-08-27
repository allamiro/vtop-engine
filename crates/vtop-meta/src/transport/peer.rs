//! Framed mTLS peer connection and request/response exchange.
//!
//! A connection is one TCP+TLS session that carries VTPM request/response
//! pairs. The [`crate::raft::network`] adapter opens a short-lived
//! connection per RPC in this slice (no connection pool yet).

use super::maybe_tls::MaybeTls;
use super::tls::{assert_peer_identity, server_name, TlsMaterial};
use super::wire::{
    read_frame, write_frame, PeerAppendRequest, PeerAppendResponse, PeerInstallRequest,
    PeerInstallResponse, PeerVoteRequest, PeerVoteResponse, TransportError, TransportResult,
    VtpmFrame, KIND_APPEND_REQ, KIND_APPEND_RESP, KIND_INSTALL_REQ, KIND_INSTALL_RESP,
    KIND_VOTE_REQ, KIND_VOTE_RESP,
};
use crate::keys::MetaNodeId;
use crate::storage::hardstate::HardState;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
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

/// How far a PLAINTEXT peer endpoint may be reachable.
///
/// Same shape as the admin plane's (#294 slice 1), deliberately: two planes
/// that make the same decision should not make it two different ways.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlaintextExposure {
    /// The default, and a refusal rather than a warning: loopback or nothing.
    LoopbackOnly,
    /// Explicitly acknowledged at the call site.
    AnyInterface,
}

/// Accept peer connections and dispatch VTPM RPCs to `handler`.
///
/// mTLS by default. Plaintext is possible (#294 slice 2) and costs something
/// specific that the admin plane's plaintext did not: see
/// [`PeerServer::plaintext`].
pub struct PeerServer {
    /// `None` means plaintext. Not "TLS we failed to build" — the two want
    /// opposite defaults, so the absence is constructed deliberately or not
    /// at all.
    acceptor: Option<TlsAcceptor>,
    local_id: MetaNodeId,
    handler: Arc<dyn PeerRpcHandler>,
    exposure: PlaintextExposure,
}

impl PeerServer {
    pub fn new(
        material: TlsMaterial,
        local_id: MetaNodeId,
        handler: Arc<dyn PeerRpcHandler>,
    ) -> TransportResult<Self> {
        Ok(Self {
            acceptor: Some(super::tls::build_server_acceptor(material)?),
            local_id,
            handler,
            exposure: PlaintextExposure::LoopbackOnly,
        })
    }

    /// A peer endpoint with NO TLS, on loopback only (#294 slice 2).
    ///
    /// # What this gives up, which is more than confidentiality
    ///
    /// On this plane the certificate CN is a Raft SAFETY input, not merely an
    /// authorization one. Every peer RPC carries a claimed sender in
    /// `HardState.voted_for`, and [`assert_sender_identity`] cross-checks it
    /// against the CN the connection authenticated. That check is what makes
    /// membership mean anything: without it a peer can assert it is node 1,
    /// vote as node 1, and append entries as node 1.
    ///
    /// With no TLS there is no certificate, so there is no CN, so there is
    /// nothing to cross-check against. The sender becomes SELF-ASSERTED and
    /// the group's integrity rests entirely on network isolation. That is what
    /// a plaintext etcd or Kafka development cluster does, and it is defensible
    /// — but only when it is stated rather than discovered, which is why this
    /// is a separate constructor, why the default is loopback, and why the
    /// skipped check below says UNVERIFIED out loud instead of quietly
    /// returning `Ok`.
    ///
    /// Loopback bounds the blast radius to the machine. For a routable address
    /// you must say so with [`PeerServer::plaintext_on_any_interface`].
    pub fn plaintext(local_id: MetaNodeId, handler: Arc<dyn PeerRpcHandler>) -> Self {
        Self {
            acceptor: None,
            local_id,
            handler,
            exposure: PlaintextExposure::LoopbackOnly,
        }
    }

    /// Plaintext peer traffic on a NON-LOOPBACK address, acknowledged
    /// explicitly.
    ///
    /// Separate constructor rather than a boolean, because the risk deserves a
    /// name at the call site. This is the certificate-free multi-node cluster
    /// — the shape the compose lab wants — and it means any host that can
    /// reach this port can join the Raft group as any node id, vote in
    /// elections, and append entries. There is no check left that would
    /// notice.
    ///
    /// Legitimate on a trusted segment or inside a sidecar mesh that provides
    /// the authentication this plane is giving up. Nothing about the
    /// configuration would otherwise say so, which is why the default refuses.
    pub fn plaintext_on_any_interface(
        local_id: MetaNodeId,
        handler: Arc<dyn PeerRpcHandler>,
    ) -> Self {
        let mut server = Self::plaintext(local_id, handler);
        server.exposure = PlaintextExposure::AnyInterface;
        server
    }

    /// Serve until the listener is closed. Spawns one task per accepted
    /// connection. Not deterministic under wall-clock scheduling.
    pub async fn serve(self, listener: TcpListener) -> TransportResult<()> {
        // CHECKED AT BIND TIME, not per connection: the question is about the
        // address this endpoint is reachable on, and answering it once means a
        // misconfigured deployment fails to start rather than serving until
        // the first unwanted peer arrives (#294 slice 1 set this pattern).
        if self.acceptor.is_none() {
            let bound = listener.local_addr()?;
            if self.exposure == PlaintextExposure::LoopbackOnly && !bound.ip().is_loopback() {
                return Err(TransportError::Identity(format!(
                    "refusing to serve a plaintext peer endpoint on {bound}: it is not a \
                     loopback address, and a plaintext peer endpoint has no certificate and \
                     therefore no verified sender — any host that can reach this port could \
                     join the Raft group as any node id, vote in elections, and append \
                     entries. Bind to 127.0.0.1 for local use, enable TLS to expose it, or \
                     construct the server with `plaintext_on_any_interface` if that is \
                     genuinely intended."
                )));
            }
            // Every time, not once per process: silence makes "no policy"
            // indistinguishable from a policy that happens to permit what is
            // being observed.
            eprintln!(
                "warning: metadata peer endpoint {bound} is PLAINTEXT: peer requests carry no \
                 certificate, so the Raft sender in each request is SELF-ASSERTED and cannot be \
                 verified. Group membership rests entirely on network isolation."
            );
        }
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
    acceptor: Option<TlsAcceptor>,
    tcp: TcpStream,
    handler: Arc<dyn PeerRpcHandler>,
    _local_id: MetaNodeId,
) -> TransportResult<()> {
    let mut stream = match acceptor {
        Some(acceptor) => MaybeTls::Tls(Box::new(
            acceptor
                .accept(tcp)
                .await
                .map_err(|error| TransportError::Tls(format!("peer accept handshake: {error}")))?
                .into(),
        )),
        None => MaybeTls::Plain(tcp),
    };
    // Under mTLS: the chain is authenticated, and the CN→MetaNodeId binding
    // must also match the Raft sender claimed in each request's
    // HardState.voted_for.
    //
    // Under plaintext: there is NO identity. `None` here does not mean the
    // check passed, it means there was nothing to check — see
    // `dispatch_peer_rpc`, which is where that distinction is spent.
    let peer_id = peer_id_from_server_stream(&stream)?;
    loop {
        let frame = match read_frame(&mut stream).await {
            Ok(frame) => frame,
            Err(TransportError::Closed) => return Ok(()),
            Err(error) => return Err(error),
        };
        let response = dispatch_peer_rpc(handler.as_ref(), peer_id, frame).await?;
        write_frame(&mut stream, &response).await?;
    }
}

/// The authenticated peer id, or `None` when the connection carries no TLS.
///
/// `None` is not "the certificate was unreadable" — that is an error, and it
/// stays one. It is "this plane is running plaintext, so no identity exists",
/// which callers must treat as an unverified peer rather than as an
/// authenticated one whose name they failed to read. The two want opposite
/// defaults, which is exactly the distinction `MaybeTls::peer_certificates`
/// documents for the admin plane.
fn peer_id_from_server_stream(stream: &MaybeTls<TcpStream>) -> TransportResult<Option<MetaNodeId>> {
    match stream {
        MaybeTls::Plain(_) => Ok(None),
        MaybeTls::Tls(_) => {
            let leaf = stream
                .peer_certificates()
                .and_then(|c| c.first())
                .ok_or_else(|| {
                    TransportError::Identity("peer presented no certificate".to_owned())
                })?;
            super::tls::meta_node_id_from_cert(leaf).map(Some)
        }
    }
}

/// Cross-check the Raft sender a request claims against the identity the
/// connection authenticated.
///
/// `peer_id` is `None` only on a plaintext plane, and the skip is EXPLICIT
/// rather than a `match` arm that quietly returns `Ok`. The difference matters
/// to a reader more than to the compiler: "the check ran and agreed" and
/// "there was nothing to check against" are opposite facts, and code that
/// renders them identically is how the second gets mistaken for the first.
///
/// On a plaintext plane the sender is UNVERIFIED. Any peer may claim any node
/// id; nothing here can tell. That is the cost recorded on #294 and accepted
/// deliberately (option 2), bounded by loopback unless the operator typed
/// `plaintext_on_any_interface`.
fn assert_sender_identity(peer_id: Option<MetaNodeId>, vote: &HardState) -> TransportResult<()> {
    let Some(peer_id) = peer_id else {
        // UNVERIFIED SENDER. No certificate, so no CN, so nothing binds this
        // request's claimed sender to the connection it arrived on.
        return Ok(());
    };
    match vote.voted_for {
        Some(claimed) if claimed == peer_id => Ok(()),
        Some(claimed) => Err(TransportError::Identity(format!(
            "certificate CN maps to node {peer_id}, but request claims sender {claimed}"
        ))),
        None => Err(TransportError::Identity(
            "peer request HardState is missing voted_for (sender id)".to_owned(),
        )),
    }
}

async fn dispatch_peer_rpc(
    handler: &dyn PeerRpcHandler,
    peer_id: Option<MetaNodeId>,
    frame: VtpmFrame,
) -> TransportResult<VtpmFrame> {
    match frame.kind {
        KIND_VOTE_REQ => {
            let request = PeerVoteRequest::decode(&frame.payload)?;
            assert_sender_identity(peer_id, &request.vote)?;
            let response = handler.handle_vote(request).await?;
            Ok(VtpmFrame {
                kind: KIND_VOTE_RESP,
                payload: response.encode()?,
            })
        }
        KIND_APPEND_REQ => {
            let request = PeerAppendRequest::decode(&frame.payload)?;
            assert_sender_identity(peer_id, &request.vote)?;
            let response = handler.handle_append(request).await?;
            Ok(VtpmFrame {
                kind: KIND_APPEND_RESP,
                payload: response.encode()?,
            })
        }
        KIND_INSTALL_REQ => {
            let request = PeerInstallRequest::decode(&frame.payload)?;
            assert_sender_identity(peer_id, &request.vote)?;
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
    /// `None` means plaintext, constructed deliberately — never a TLS setup
    /// that failed.
    connector: Option<TlsConnector>,
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
            connector: Some(super::tls::build_client_connector(material)?),
            server_name: server_name.into(),
            expected_peer,
        })
    }

    /// A peer client that speaks to PLAINTEXT peers (#294 slice 2).
    ///
    /// The counterpart to [`PeerServer::plaintext`], and it exists for the
    /// reason slice 1 recorded: a transport that can only be *served* without
    /// TLS is the "looks configurable, isn't" failure this work is meant to
    /// remove. A plaintext group needs both ends.
    ///
    /// `expected_peer` is still taken, and still meaningful — it is who this
    /// client BELIEVES it is dialling, and it is what the address book says.
    /// What changes is that nothing can confirm it: there is no certificate to
    /// compare against, so the belief is unchecked. It is kept rather than
    /// dropped so the two constructors have the same shape and a later slice
    /// can tighten this without changing every call site.
    pub fn plaintext(server_name: impl Into<String>, expected_peer: MetaNodeId) -> Self {
        Self {
            connector: None,
            server_name: server_name.into(),
            expected_peer,
        }
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
        let mut stream = match self.connector.as_ref() {
            Some(connector) => {
                let name = server_name(&self.server_name)?;
                let stream = connector
                    .connect(name, tcp)
                    .await
                    .map_err(|error| TransportError::Tls(format!("peer connect: {error}")))?;
                MaybeTls::Tls(Box::new(stream.into()))
            }
            None => MaybeTls::Plain(tcp),
        };
        // Skipped EXPLICITLY under plaintext, for the same reason the server
        // side skips its sender check out loud: there is no certificate, so
        // this is not a verification that passed, it is one that could not be
        // performed. The peer this client reached is whatever answered the
        // address.
        if stream.is_encrypted() {
            assert_peer_identity(stream.peer_certificates(), self.expected_peer)?;
        }
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

    /// A plaintext Raft group forms and exchanges votes with no PKI at all
    /// (#294 slice 2). This is the shape the compose lab wants and the reason
    /// option (2) was chosen over refusing plaintext on this plane entirely.
    #[tokio::test]
    async fn loopback_plaintext_vote_round_trip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = PeerServer::plaintext(MetaNodeId(1), Arc::new(EchoHandler));
        let server_task = tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let client = PeerClient::plaintext("localhost", MetaNodeId(1));
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
        server_task.abort();
    }

    /// THE COST, executed rather than described. On a plaintext plane a peer
    /// may claim to be ANY node id and nothing can contradict it — the CN that
    /// would have been cross-checked does not exist.
    ///
    /// The mTLS twin of this test (`mtls_rejects_vote_sender_spoof`) requires
    /// the same request to be REFUSED. Both are correct, and keeping them side
    /// by side is the point: the difference between them is exactly what
    /// enabling plaintext on this plane gives up, and #294 chose that
    /// deliberately (option 2) rather than discovering it later.
    #[tokio::test]
    async fn plaintext_cannot_detect_a_spoofed_vote_sender() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = PeerServer::plaintext(MetaNodeId(1), Arc::new(EchoHandler));
        let server_task = tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        // The client believes it is node 2; the request claims node 7.
        let client = PeerClient::plaintext("localhost", MetaNodeId(1));
        let response = client
            .vote(
                addr,
                &PeerVoteRequest {
                    vote: HardState {
                        term: 9,
                        voted_for: Some(MetaNodeId(7)),
                        vote_committed: false,
                    },
                    last_log_id: None,
                },
            )
            .await;
        assert!(
            response.is_ok(),
            "a plaintext peer plane has no certificate to contradict a claimed \
             sender: this request is SERVED, and that is the documented cost of \
             the mode, not a bug to be fixed here"
        );
        server_task.abort();
    }

    /// Plaintext on a routable address is refused AT BIND TIME, so a
    /// misconfigured deployment fails to start instead of serving until the
    /// first unwanted peer arrives.
    #[tokio::test]
    async fn plaintext_refuses_a_non_loopback_bind() {
        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let server = PeerServer::plaintext(MetaNodeId(1), Arc::new(EchoHandler));
        let error = server.serve(listener).await.unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("plaintext peer endpoint") && message.contains("loopback"),
            "the refusal must name the plane and the reason: {message}"
        );
        assert!(
            message.contains("plaintext_on_any_interface"),
            "and it must name the way to say yes on purpose, or an operator's \
             only route past it is to abandon the mode: {message}"
        );
    }

    /// The same bind, acknowledged explicitly, is allowed — otherwise the
    /// certificate-free multi-node cluster that motivated option (2) is
    /// impossible and the mode is useful only where it is least needed.
    #[tokio::test]
    async fn plaintext_on_any_interface_accepts_a_routable_bind() {
        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = PeerServer::plaintext_on_any_interface(MetaNodeId(1), Arc::new(EchoHandler));
        let server_task = tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let client = PeerClient::plaintext("localhost", MetaNodeId(1));
        let response = client
            .vote(
                addr,
                &PeerVoteRequest {
                    vote: HardState {
                        term: 1,
                        voted_for: Some(MetaNodeId(2)),
                        vote_committed: false,
                    },
                    last_log_id: None,
                },
            )
            .await;
        assert!(
            response.is_ok(),
            "an explicitly acknowledged routable plaintext endpoint must serve"
        );
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

    #[tokio::test]
    async fn mtls_rejects_vote_sender_spoof() {
        // Client cert CN=2, but the Vote claims sender 99.
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
        tokio::time::sleep(Duration::from_millis(20)).await;

        let client = PeerClient::new(client_material, "localhost", MetaNodeId(1)).unwrap();
        let err = client
            .vote(
                addr,
                &PeerVoteRequest {
                    vote: HardState {
                        term: 3,
                        voted_for: Some(MetaNodeId(99)),
                        vote_committed: false,
                    },
                    last_log_id: None,
                },
            )
            .await
            .unwrap_err();
        // Server closes after identity reject; client sees closed/io/protocol.
        assert!(matches!(
            err,
            TransportError::Closed | TransportError::Io(_) | TransportError::Protocol(_)
        ));
        server_task.abort();
    }
}

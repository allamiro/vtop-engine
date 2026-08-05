//! Admin/control surface over VTPM mTLS.
//!
//! Operators (and `vtopctl meta`) talk to a single admin endpoint that forwards
//! status and propose requests through the [`crate::raft::consensus::Consensus`]
//! façade. Openraft types never cross this boundary.

use super::authz::{AdminAuthorizer, AdminIdentity, Refusal};
use super::tls::{server_name, TlsMaterial};
use super::wire::{
    read_frame, write_frame, AdminAddLearnerRequest, AdminChangeMembershipRequest, AdminError,
    AdminInitRequest, AdminMembershipResponse, AdminProposeRequest, AdminProposeResponse,
    AdminReadRangeLeaseRequest, AdminReadRangeLeaseResponse, AdminStatusRequest,
    AdminStatusResponse, TransportError, TransportResult, VtpmFrame, KIND_ADMIN_ADD_LEARNER_REQ,
    KIND_ADMIN_CHANGE_MEMBERSHIP_REQ, KIND_ADMIN_ERROR, KIND_ADMIN_INIT_REQ,
    KIND_ADMIN_MEMBERSHIP_RESP, KIND_ADMIN_PROPOSE_REQ, KIND_ADMIN_PROPOSE_RESP,
    KIND_ADMIN_READ_RANGE_LEASE_REQ, KIND_ADMIN_READ_RANGE_LEASE_RESP, KIND_ADMIN_STATUS_REQ,
    KIND_ADMIN_STATUS_RESP,
};
use crate::command::MetadataCommand;
use crate::keys::MetaNodeId;
use async_trait::async_trait;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// Backend for admin RPCs — typically [`crate::raft::consensus::OpenraftConsensus`].
#[async_trait]
pub trait AdminHandler: Send + Sync {
    async fn status(&self) -> TransportResult<AdminStatusResponse>;
    async fn propose(&self, command: MetadataCommand) -> TransportResult<AdminProposeResponse>;
    /// Bootstrap a fresh group (#215 live-cluster surface).
    async fn init(&self, members: Vec<u64>) -> TransportResult<AdminMembershipResponse>;
    async fn add_learner(&self, node_id: u64) -> TransportResult<AdminMembershipResponse>;
    async fn change_membership(
        &self,
        voters: Vec<u64>,
        retain_removed_as_learners: bool,
    ) -> TransportResult<AdminMembershipResponse>;
    /// Linearizable read of a range's lease (#223).
    ///
    /// Linearizable, not best-effort: a candidate deciding whether to take a
    /// range must not act on a stale view. A follower serving its own lagging
    /// copy could report an expired lease that the leader has already renewed,
    /// and the candidate would then fence a perfectly healthy leader.
    async fn read_range_lease(
        &self,
        request: AdminReadRangeLeaseRequest,
    ) -> TransportResult<AdminReadRangeLeaseResponse>;
}

/// mTLS admin server.
pub struct AdminServer {
    acceptor: TlsAcceptor,
    handler: Arc<dyn AdminHandler>,
    authorizer: Arc<AdminAuthorizer>,
}

impl AdminServer {
    /// Build a server that authenticates but does not authorize (#238).
    ///
    /// Equivalent to [`AdminServer::with_authorization`] with
    /// [`AdminAuthorizer::permissive`]. Kept so existing call sites and tests
    /// that predate the policy compile unchanged.
    pub fn new(material: TlsMaterial, handler: Arc<dyn AdminHandler>) -> TransportResult<Self> {
        Self::with_authorization(material, handler, AdminAuthorizer::permissive())
    }

    pub fn with_authorization(
        material: TlsMaterial,
        handler: Arc<dyn AdminHandler>,
        authorizer: AdminAuthorizer,
    ) -> TransportResult<Self> {
        Ok(Self {
            acceptor: super::tls::build_server_acceptor(material)?,
            handler,
            authorizer: Arc::new(authorizer),
        })
    }

    pub async fn serve(self, listener: TcpListener) -> TransportResult<()> {
        loop {
            let (tcp, _) = listener.accept().await?;
            let acceptor = self.acceptor.clone();
            let handler = Arc::clone(&self.handler);
            let authorizer = Arc::clone(&self.authorizer);
            tokio::spawn(async move {
                let _ = serve_admin_connection(acceptor, tcp, handler, authorizer).await;
            });
        }
    }
}

async fn serve_admin_connection(
    acceptor: TlsAcceptor,
    tcp: TcpStream,
    handler: Arc<dyn AdminHandler>,
    authorizer: Arc<AdminAuthorizer>,
) -> TransportResult<()> {
    let mut stream = acceptor
        .accept(tcp)
        .await
        .map_err(|error| TransportError::Tls(format!("admin accept: {error}")))?;
    // Identity is established once, from the certificate the handshake already
    // verified, and is fixed for the connection's lifetime. Deriving it
    // per-frame would invite a caller to influence it through frame content;
    // here the only input is the chain the CA signed.
    let identity = {
        let (_, connection) = stream.get_ref();
        let leaf = connection
            .peer_certificates()
            .and_then(|certs| certs.first())
            .ok_or_else(|| {
                // The acceptor is built with a WebPki *client* verifier, so a
                // certificate-less handshake never completes and this is
                // unreachable in practice. Refusing beats unwrapping: if a
                // future config change made client certs optional, the failure
                // must be a closed connection, not an unauthenticated one.
                TransportError::Identity("admin client presented no certificate".to_owned())
            })?;
        AdminIdentity::from_common_name(&super::tls::common_name_from_cert(leaf)?)
    };
    loop {
        let frame = match read_frame(&mut stream).await {
            Ok(frame) => frame,
            Err(TransportError::Closed) => return Ok(()),
            Err(error) => return Err(error),
        };
        let response = match dispatch_admin(handler.as_ref(), &authorizer, &identity, frame).await {
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
        return message.to_owned();
    }
    let mut end = max;
    while end > 0 && !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_owned()
}

/// Resolve `host:port` (DNS name or literal) to a socket address.
pub fn resolve_endpoint(endpoint: &str) -> TransportResult<SocketAddr> {
    endpoint.to_socket_addrs()?.next().ok_or_else(|| {
        TransportError::Protocol(format!("endpoint {endpoint:?} resolved to no addresses"))
    })
}

/// Dispatch one admin frame, authorizing it first.
///
/// Every arm authorizes **before** calling the handler, and the authorization
/// is what the arm's own frame kind implies — reads are open, membership RPCs
/// are operator-only, and a proposal is classified by the command it carries.
/// A refusal returns [`TransportError::Unauthorized`], which the caller turns
/// into an admin error frame; the connection stays open so a client that
/// overreached once can continue with requests it is entitled to make.
async fn dispatch_admin(
    handler: &dyn AdminHandler,
    authorizer: &AdminAuthorizer,
    identity: &AdminIdentity,
    frame: VtpmFrame,
) -> TransportResult<VtpmFrame> {
    match frame.kind {
        KIND_ADMIN_STATUS_REQ => {
            AdminStatusRequest::decode(&frame.payload)?;
            authorize(authorizer.authorize_read(identity))?;
            let response = handler.status().await?;
            Ok(VtpmFrame {
                kind: KIND_ADMIN_STATUS_RESP,
                payload: response.encode()?,
            })
        }
        KIND_ADMIN_READ_RANGE_LEASE_REQ => {
            let request = AdminReadRangeLeaseRequest::decode(&frame.payload)?;
            authorize(authorizer.authorize_read(identity))?;
            let response = handler.read_range_lease(request).await?;
            Ok(VtpmFrame {
                kind: KIND_ADMIN_READ_RANGE_LEASE_RESP,
                payload: response.encode(),
            })
        }
        KIND_ADMIN_PROPOSE_REQ => {
            let request = AdminProposeRequest::decode(&frame.payload)?;
            authorize(authorizer.authorize_command(identity, &request.command))?;
            let response = handler.propose(request.command).await?;
            Ok(VtpmFrame {
                kind: KIND_ADMIN_PROPOSE_RESP,
                payload: response.encode()?,
            })
        }
        KIND_ADMIN_INIT_REQ => {
            let request = AdminInitRequest::decode(&frame.payload)?;
            authorize(authorizer.authorize_cluster(identity))?;
            let response = handler.init(request.members).await?;
            Ok(VtpmFrame {
                kind: KIND_ADMIN_MEMBERSHIP_RESP,
                payload: response.encode()?,
            })
        }
        KIND_ADMIN_ADD_LEARNER_REQ => {
            let request = AdminAddLearnerRequest::decode(&frame.payload)?;
            authorize(authorizer.authorize_cluster(identity))?;
            let response = handler.add_learner(request.node_id).await?;
            Ok(VtpmFrame {
                kind: KIND_ADMIN_MEMBERSHIP_RESP,
                payload: response.encode()?,
            })
        }
        KIND_ADMIN_CHANGE_MEMBERSHIP_REQ => {
            let request = AdminChangeMembershipRequest::decode(&frame.payload)?;
            authorize(authorizer.authorize_cluster(identity))?;
            let response = handler
                .change_membership(request.voters, request.retain_removed_as_learners)
                .await?;
            Ok(VtpmFrame {
                kind: KIND_ADMIN_MEMBERSHIP_RESP,
                payload: response.encode()?,
            })
        }
        other => Err(TransportError::UnexpectedKind(other)),
    }
}

fn authorize(outcome: Result<(), Refusal>) -> TransportResult<()> {
    outcome.map_err(|refusal| TransportError::Unauthorized(refusal.message()))
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

    /// Read a range's lease through a linearizable fence.
    pub async fn read_range_lease(
        &self,
        topic_uuid: uuid::Uuid,
        range_uuid: uuid::Uuid,
    ) -> TransportResult<AdminReadRangeLeaseResponse> {
        let frame = self
            .round_trip(VtpmFrame {
                kind: KIND_ADMIN_READ_RANGE_LEASE_REQ,
                payload: AdminReadRangeLeaseRequest {
                    topic_uuid,
                    range_uuid,
                }
                .encode(),
            })
            .await?;
        match frame.kind {
            KIND_ADMIN_READ_RANGE_LEASE_RESP => {
                Ok(AdminReadRangeLeaseResponse::decode(&frame.payload)?)
            }
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

    pub async fn init(&self, members: Vec<u64>) -> TransportResult<AdminMembershipResponse> {
        self.membership_round_trip(VtpmFrame {
            kind: KIND_ADMIN_INIT_REQ,
            payload: AdminInitRequest { members }.encode()?,
        })
        .await
    }

    pub async fn add_learner(&self, node_id: u64) -> TransportResult<AdminMembershipResponse> {
        self.membership_round_trip(VtpmFrame {
            kind: KIND_ADMIN_ADD_LEARNER_REQ,
            payload: AdminAddLearnerRequest { node_id }.encode()?,
        })
        .await
    }

    pub async fn change_membership(
        &self,
        voters: Vec<u64>,
        retain_removed_as_learners: bool,
    ) -> TransportResult<AdminMembershipResponse> {
        self.membership_round_trip(VtpmFrame {
            kind: KIND_ADMIN_CHANGE_MEMBERSHIP_REQ,
            payload: AdminChangeMembershipRequest {
                voters,
                retain_removed_as_learners,
            }
            .encode()?,
        })
        .await
    }

    async fn membership_round_trip(
        &self,
        request: VtpmFrame,
    ) -> TransportResult<AdminMembershipResponse> {
        let frame = self.round_trip(request).await?;
        match frame.kind {
            KIND_ADMIN_MEMBERSHIP_RESP => Ok(AdminMembershipResponse::decode(&frame.payload)?),
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

#[cfg(test)]
mod tests {
    use super::*;
    use super::{resolve_endpoint, truncate_error};
    use crate::command::{CommandEnvelope, MAX_ERROR_DETAIL_BYTES};
    use crate::keys::MetaNodeId;
    use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};
    use rustls::pki_types::PrivatePkcs8KeyDer;
    use rustls::RootCertStore;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use uuid::Uuid;

    const NODE_A: Uuid = Uuid::from_u128(0xa1);
    const NODE_B: Uuid = Uuid::from_u128(0xb2);

    /// Counts what actually reached the backend.
    ///
    /// The point of the live tests below is not that a refused client sees an
    /// error — an error could equally come from a handler that ran and then
    /// failed. It is that the handler is never entered at all, which is the
    /// difference between a gate and a warning.
    #[derive(Default)]
    struct CountingHandler {
        proposals: AtomicUsize,
        membership_changes: AtomicUsize,
    }

    #[async_trait]
    impl AdminHandler for CountingHandler {
        async fn status(&self) -> TransportResult<AdminStatusResponse> {
            Ok(stub_status(MetaNodeId(1)))
        }

        async fn propose(
            &self,
            _command: MetadataCommand,
        ) -> TransportResult<AdminProposeResponse> {
            self.proposals.fetch_add(1, Ordering::SeqCst);
            // The backend is irrelevant here; refuse so a test can never mistake
            // "the handler ran" for "the request succeeded".
            Err(TransportError::Protocol("handler reached".to_owned()))
        }

        async fn init(&self, _members: Vec<u64>) -> TransportResult<AdminMembershipResponse> {
            self.membership_changes.fetch_add(1, Ordering::SeqCst);
            Err(TransportError::Protocol("handler reached".to_owned()))
        }

        async fn add_learner(&self, _node_id: u64) -> TransportResult<AdminMembershipResponse> {
            self.membership_changes.fetch_add(1, Ordering::SeqCst);
            Err(TransportError::Protocol("handler reached".to_owned()))
        }

        async fn change_membership(
            &self,
            _voters: Vec<u64>,
            _retain: bool,
        ) -> TransportResult<AdminMembershipResponse> {
            self.membership_changes.fetch_add(1, Ordering::SeqCst);
            Err(TransportError::Protocol("handler reached".to_owned()))
        }

        async fn read_range_lease(
            &self,
            _request: AdminReadRangeLeaseRequest,
        ) -> TransportResult<AdminReadRangeLeaseResponse> {
            Ok(AdminReadRangeLeaseResponse {
                found: false,
                range_generation: 0,
                fencing_epoch: 0,
                lease: None,
                read_at_applied_index: 0,
            })
        }
    }

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

    fn acquire(holder: Uuid) -> MetadataCommand {
        MetadataCommand::AcquireRangeLease {
            env: CommandEnvelope {
                request_id: Uuid::from_u128(1),
                issued_at_ms: 0,
            },
            topic_uuid: Uuid::from_u128(7),
            range_uuid: Uuid::from_u128(8),
            holder_node_uuid: holder,
            expected_range_generation: 0,
            lease_duration_ms: 5_000,
        }
    }

    /// Stand up a real mTLS admin endpoint and return a client speaking as `cn`.
    async fn live_endpoint(
        authorizer: AdminAuthorizer,
        client_cn: &str,
    ) -> (
        AdminClient,
        Arc<CountingHandler>,
        tokio::task::JoinHandle<()>,
    ) {
        let (server_leaf, server_key) = mint_leaf("localhost");
        let (client_leaf, client_key) = mint_leaf(client_cn);

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

        let handler = Arc::new(CountingHandler::default());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = AdminServer::with_authorization(
            server_material,
            Arc::clone(&handler) as Arc<dyn AdminHandler>,
            authorizer,
        )
        .unwrap();
        let task = tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        // Wall-clock settle; this live test is not seed-deterministic.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let client = AdminClient::new(client_material, addr, "localhost").unwrap();
        (client, handler, task)
    }

    /// The rule that makes failover work: a data node drives its own lease
    /// without holding an operator credential.
    #[tokio::test]
    async fn a_node_may_drive_its_own_lease_over_the_wire() {
        let (client, handler, task) =
            live_endpoint(AdminAuthorizer::with_operators([]), &NODE_A.to_string()).await;

        let error = client.propose(acquire(NODE_A)).await.unwrap_err();
        // Reaching the handler IS the pass condition — the stub then refuses,
        // so a bare `is_err()` would prove nothing on its own.
        assert!(
            error.to_string().contains("handler reached"),
            "an authorized proposal must reach the backend: {error}"
        );
        assert_eq!(handler.proposals.load(Ordering::SeqCst), 1);

        task.abort();
    }

    /// The rule that makes it safe. One compromised node must not be able to
    /// fence every range in the cluster — and the proposal must not reach the
    /// state machine at all.
    #[tokio::test]
    async fn a_node_may_not_drive_another_nodes_lease_over_the_wire() {
        let (client, handler, task) =
            live_endpoint(AdminAuthorizer::with_operators([]), &NODE_A.to_string()).await;

        let error = client.propose(acquire(NODE_B)).await.unwrap_err();
        let message = error.to_string();
        assert!(message.contains("unauthorized"), "{message}");
        // Both node UUIDs, so a misconfigured node UUID — the likeliest cause —
        // is diagnosable from the error alone.
        assert!(message.contains(&NODE_A.to_string()), "{message}");
        assert!(message.contains(&NODE_B.to_string()), "{message}");
        assert_eq!(
            handler.proposals.load(Ordering::SeqCst),
            0,
            "a refused proposal must never reach the state machine"
        );

        task.abort();
    }

    /// Membership RPCs carry no command to classify, so they are gated by
    /// frame kind. A node certificate must not rewrite the cluster.
    #[tokio::test]
    async fn a_node_may_not_change_membership_over_the_wire() {
        let (client, handler, task) =
            live_endpoint(AdminAuthorizer::with_operators([]), &NODE_A.to_string()).await;

        for outcome in [
            client.init(vec![1, 2, 3]).await,
            client.add_learner(4).await,
            client.change_membership(vec![1, 2], false).await,
        ] {
            let message = outcome.unwrap_err().to_string();
            assert!(message.contains("unauthorized"), "{message}");
        }
        assert_eq!(
            handler.membership_changes.load(Ordering::SeqCst),
            0,
            "no membership RPC may reach the backend"
        );

        task.abort();
    }

    /// The connection survives a refusal: a client that overreaches once can
    /// continue with requests it is entitled to make, rather than being forced
    /// to re-handshake.
    #[tokio::test]
    async fn a_refusal_does_not_close_the_connection() {
        let (client, handler, task) =
            live_endpoint(AdminAuthorizer::with_operators([]), &NODE_A.to_string()).await;

        client.propose(acquire(NODE_B)).await.unwrap_err();
        let status = client
            .status()
            .await
            .expect("reads stay open after a refusal");
        assert_eq!(status.node_id, MetaNodeId(1));
        assert_eq!(handler.proposals.load(Ordering::SeqCst), 0);

        task.abort();
    }

    /// A configured operator retains the full surface.
    #[tokio::test]
    async fn a_configured_operator_may_change_membership_over_the_wire() {
        let (client, handler, task) = live_endpoint(
            AdminAuthorizer::with_operators(["ops-alice".to_owned()]),
            "ops-alice",
        )
        .await;

        let error = client
            .change_membership(vec![1, 2], false)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("handler reached"),
            "an operator's membership change must reach the backend: {error}"
        );
        assert_eq!(handler.membership_changes.load(Ordering::SeqCst), 1);

        task.abort();
    }

    /// An unconfigured endpoint must behave exactly as it did before #238,
    /// over the real transport and not just in the policy unit tests.
    #[tokio::test]
    async fn an_unconfigured_endpoint_still_permits_everything() {
        let (client, handler, task) =
            live_endpoint(AdminAuthorizer::permissive(), &NODE_A.to_string()).await;

        // Acting for another node, and changing membership — both refused
        // under any policy, both permitted with none.
        client.propose(acquire(NODE_B)).await.unwrap_err();
        client
            .change_membership(vec![1, 2], false)
            .await
            .unwrap_err();
        assert_eq!(handler.proposals.load(Ordering::SeqCst), 1);
        assert_eq!(handler.membership_changes.load(Ordering::SeqCst), 1);

        task.abort();
    }

    #[test]
    fn truncate_error_respects_utf8_byte_bound() {
        // 'é' is two UTF-8 bytes; taking `max` chars would exceed the byte bound.
        let message = "é".repeat(MAX_ERROR_DETAIL_BYTES);
        let truncated = truncate_error(&message);
        assert!(truncated.len() <= MAX_ERROR_DETAIL_BYTES);
        assert!(truncated.is_char_boundary(truncated.len()));
    }

    #[test]
    fn resolve_endpoint_accepts_localhost_name() {
        let addr = resolve_endpoint("localhost:0").expect("localhost resolves");
        assert_eq!(addr.port(), 0);
    }
}

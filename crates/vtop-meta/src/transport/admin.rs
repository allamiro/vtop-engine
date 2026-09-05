//! Admin/control surface over VTPM mTLS.
//!
//! Operators (and `vtopctl meta`) talk to a single admin endpoint that forwards
//! status and propose requests through the [`crate::raft::consensus::Consensus`]
//! façade. Openraft types never cross this boundary.

use super::authz::{AdminAuthorizer, AdminIdentity, Refusal};
use super::maybe_tls::{judge_dialect, tls_handshake_hint, DialectVerdict, MaybeTls};
use super::tls::{server_name, TlsMaterial};
use super::wire::{
    read_frame, write_frame, AdminAddLearnerRequest, AdminChangeMembershipRequest, AdminError,
    AdminInitRequest, AdminMembershipResponse, AdminProposeRequest, AdminProposeResponse,
    AdminReadGroupCursorRequest, AdminReadGroupCursorResponse, AdminReadRangeLeaseRequest,
    AdminReadRangeLeaseResponse, AdminReadRangeTransitionsRequest,
    AdminReadRangeTransitionsResponse, AdminReadSegmentPlacementRequest,
    AdminReadSegmentPlacementResponse, AdminReadTopicRangesRequest, AdminReadTopicRangesResponse,
    AdminStatusRequest, AdminStatusResponse, NotLeaderHint, TransportError, TransportResult,
    VtpmFrame, KIND_ADMIN_ADD_LEARNER_REQ, KIND_ADMIN_CHANGE_MEMBERSHIP_REQ, KIND_ADMIN_ERROR,
    KIND_ADMIN_INIT_REQ, KIND_ADMIN_MEMBERSHIP_RESP, KIND_ADMIN_PROPOSE_REQ,
    KIND_ADMIN_PROPOSE_RESP, KIND_ADMIN_READ_GROUP_CURSOR_REQ, KIND_ADMIN_READ_GROUP_CURSOR_RESP,
    KIND_ADMIN_READ_RANGE_LEASE_REQ, KIND_ADMIN_READ_RANGE_LEASE_RESP,
    KIND_ADMIN_READ_RANGE_TRANSITIONS_REQ, KIND_ADMIN_READ_RANGE_TRANSITIONS_RESP,
    KIND_ADMIN_READ_SEGMENT_PLACEMENT_REQ, KIND_ADMIN_READ_SEGMENT_PLACEMENT_RESP,
    KIND_ADMIN_READ_TOPIC_RANGES_REQ, KIND_ADMIN_READ_TOPIC_RANGES_RESP, KIND_ADMIN_STATUS_REQ,
    KIND_ADMIN_STATUS_RESP,
};
use crate::command::MetadataCommand;
use crate::keys::MetaNodeId;
use async_trait::async_trait;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
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

    async fn read_segment_placement(
        &self,
        request: AdminReadSegmentPlacementRequest,
    ) -> TransportResult<AdminReadSegmentPlacementResponse>;

    /// Linearizable read of a range's transition chain (#240 item 5).
    ///
    /// Defaulted to a refusal rather than required (review): the trait is
    /// public, and a handler that serves no transitions must keep
    /// compiling — and must answer "not served here", never nothing.
    async fn read_range_transitions(
        &self,
        _request: AdminReadRangeTransitionsRequest,
    ) -> TransportResult<AdminReadRangeTransitionsResponse> {
        Err(TransportError::Protocol(
            "this admin handler does not serve transition reads".to_owned(),
        ))
    }
    /// Linearizable read of a group's committed cursor on a range (#457 slice
    /// 2b): what a Kafka gateway answers OffsetFetch with. Defaulted to a
    /// refusal, as the transition read is.
    async fn read_group_cursor(
        &self,
        _request: AdminReadGroupCursorRequest,
    ) -> TransportResult<AdminReadGroupCursorResponse> {
        Err(TransportError::Protocol(
            "this admin handler does not serve group cursor reads".to_owned(),
        ))
    }

    /// Linearizable read of a topic's ranges in partition order (#457 slice
    /// 3): what a Kafka gateway answers Metadata with. Defaulted to a
    /// refusal, as the reads above are.
    async fn read_topic_ranges(
        &self,
        _request: AdminReadTopicRangesRequest,
    ) -> TransportResult<AdminReadTopicRangesResponse> {
        Err(TransportError::Protocol(
            "this admin handler does not serve topic range reads".to_owned(),
        ))
    }
}

/// Admin server, with or without TLS.
pub struct AdminServer {
    /// `None` means PLAINTEXT. Not a missing acceptor to be filled in later —
    /// the absence is the configuration, decided before any connection exists.
    acceptor: Option<TlsAcceptor>,
    handler: Arc<dyn AdminHandler>,
    authorizer: Arc<AdminAuthorizer>,
    /// Only consulted when `acceptor` is `None`; TLS endpoints carry identity
    /// and are bound wherever the operator chooses.
    exposure: PlaintextExposure,
}

/// How far a PLAINTEXT admin endpoint may be reachable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlaintextExposure {
    /// The default, and a refusal rather than a warning: loopback or nothing.
    LoopbackOnly,
    /// Explicitly acknowledged at the call site.
    AnyInterface,
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
            acceptor: Some(super::tls::build_server_acceptor(material)?),
            handler,
            authorizer: Arc::new(authorizer),
            // Unused for a TLS endpoint, which carries caller identity.
            exposure: PlaintextExposure::LoopbackOnly,
        })
    }

    /// Serve the admin plane WITHOUT TLS, on the loopback interface only (#294).
    ///
    /// For local development, CI, and single-node evaluation, where requiring a
    /// minted PKI to start the engine at all is a real barrier and encrypting a
    /// loopback socket buys nothing.
    ///
    /// REFUSES AN ENFORCING AUTHORIZER, and this is the load-bearing part.
    /// Admin authorization identifies callers by the Common Name of their
    /// client certificate; with no TLS there is no certificate, so there is no
    /// CN, so there is no identity to match a policy against. Accepting the
    /// combination would mean an operator who had configured
    /// `admin_authorization` — deliberately, to restrict cluster commands —
    /// would silently get an endpoint where every caller is permitted. A policy
    /// that cannot be enforced must not be accepted quietly; #81 exists because
    /// something defaulted a credential, and this is the same class of mistake
    /// wearing different clothes.
    pub fn plaintext(
        handler: Arc<dyn AdminHandler>,
        authorizer: AdminAuthorizer,
    ) -> TransportResult<Self> {
        if authorizer.is_enforcing() {
            return Err(TransportError::Identity(
                "admin_authorization names operator CNs, but a plaintext admin endpoint has no \
                 client certificate and therefore no CN to match: the policy could not be \
                 enforced and every caller would be permitted. Either enable TLS on this plane \
                 or remove the authorization policy."
                    .to_owned(),
            ));
        }
        Ok(Self {
            acceptor: None,
            handler,
            authorizer: Arc::new(authorizer),
            exposure: PlaintextExposure::LoopbackOnly,
        })
    }

    /// Plaintext admin on a NON-LOOPBACK address, acknowledged explicitly.
    ///
    /// Separate constructor rather than a boolean, because the risk deserves a
    /// name at the call site. A plaintext admin endpoint has no client
    /// certificate, so no caller identity, so no authorization is possible —
    /// `authorize_cluster` permits everyone when no operators are configured,
    /// which is correct for an unconfigured endpoint and catastrophic for an
    /// exposed one. Anything that reaches this port can run `meta init`,
    /// change membership, and propose arbitrary commands.
    ///
    /// On loopback that is a development convenience and the blast radius is
    /// the machine. Bound to a routable address it is an unauthenticated
    /// control plane, and nothing about the config would have said so — which
    /// is why the default refuses and this exists to be typed deliberately.
    pub fn plaintext_on_any_interface(
        handler: Arc<dyn AdminHandler>,
        authorizer: AdminAuthorizer,
    ) -> TransportResult<Self> {
        let mut server = Self::plaintext(handler, authorizer)?;
        server.exposure = PlaintextExposure::AnyInterface;
        Ok(server)
    }

    pub async fn serve(self, listener: TcpListener) -> TransportResult<()> {
        // CHECKED AT BIND TIME, not per connection: the question is about the
        // address this endpoint is reachable on, and answering it once means a
        // misconfigured deployment fails to start rather than serving until the
        // first unwanted caller arrives.
        if self.acceptor.is_none() {
            let bound = listener.local_addr()?;
            if self.exposure == PlaintextExposure::LoopbackOnly && !bound.ip().is_loopback() {
                return Err(TransportError::Identity(format!(
                    "refusing to serve a plaintext admin endpoint on {bound}: it is not a \
                     loopback address, and a plaintext endpoint has no client certificate and \
                     therefore no caller identity — every peer that can reach this port could \
                     run `meta init`, change membership, and propose arbitrary commands. Bind \
                     to 127.0.0.1 for local use, enable TLS to expose it, or construct the \
                     server with `plaintext_on_any_interface` if that is genuinely intended."
                )));
            }
            eprintln!(
                "warning: admin endpoint {bound} is PLAINTEXT and UNAUTHENTICATED: it carries no \
                 client certificate, so no caller identity exists and admin authorization cannot \
                 apply. Every reachable peer may change cluster membership and grant range leases."
            );
        }
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
    acceptor: Option<TlsAcceptor>,
    tcp: TcpStream,
    handler: Arc<dyn AdminHandler>,
    authorizer: Arc<AdminAuthorizer>,
) -> TransportResult<()> {
    // Judged before the handshake or the first frame (#294 slice 6): a peer
    // speaking the other transport is refused by name instead of failing as
    // a bad magic here and a reset there.
    if let DialectVerdict::Refused(message) =
        judge_dialect(&tcp, "admin", "admin_transport", acceptor.is_some()).await?
    {
        return Err(TransportError::CrossMode(message));
    }
    let mut stream = match acceptor {
        Some(acceptor) => MaybeTls::Tls(Box::new(
            acceptor
                .accept(tcp)
                .await
                .map_err(|error| TransportError::Tls(format!("admin accept: {error}")))?
                .into(),
        )),
        None => MaybeTls::Plain(tcp),
    };
    // Identity is established once, from the certificate the handshake already
    // verified, and is fixed for the connection's lifetime. Deriving it
    // per-frame would invite a caller to influence it through frame content;
    // here the only input is the chain the CA signed.
    let identity = match stream.peer_certificates().and_then(|certs| certs.first()) {
        Some(leaf) => AdminIdentity::from_common_name(&super::tls::common_name_from_cert(leaf)?),
        // No TLS, therefore no certificate, therefore NO IDENTITY. Reached only
        // on a plaintext endpoint, which `AdminServer::plaintext` has already
        // refused to build with an enforcing authorizer — so the permissive
        // authorizer is the only one that can see this, and it treats every
        // caller alike anyway. The identity is still explicit rather than
        // invented, because it is what a log line or a refusal will name.
        //
        // Under TLS this branch is unreachable: the acceptor carries a WebPki
        // client verifier, so a certificate-less handshake never completes.
        None if !stream.is_encrypted() => AdminIdentity::Anonymous,
        None => {
            return Err(TransportError::Identity(
                "admin client completed a TLS handshake without presenting a certificate"
                    .to_owned(),
            ))
        }
    };
    loop {
        let frame = match read_frame(&mut stream).await {
            Ok(frame) => frame,
            Err(TransportError::Closed) => return Ok(()),
            Err(error) => return Err(error),
        };
        let response = match dispatch_admin(handler.as_ref(), &authorizer, &identity, frame).await {
            Ok(frame) => frame,
            // The redirect survives the trip to the wire. Building the frame
            // from `to_string()` alone is where it used to be lost: the server
            // knew which node to name and sent only prose about it.
            Err(error) => VtpmFrame {
                kind: KIND_ADMIN_ERROR,
                payload: AdminError {
                    message: truncate_error(&error.to_string()),
                    not_leader: match &error {
                        TransportError::NotLeader { leader, .. } => {
                            Some(NotLeaderHint { leader: *leader })
                        }
                        _ => None,
                    },
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
        KIND_ADMIN_READ_SEGMENT_PLACEMENT_REQ => {
            let request = AdminReadSegmentPlacementRequest::decode(&frame.payload)?;
            // A READ, not a command. The placement it returns is the input to
            // commands the caller may well not be allowed to issue, and
            // requiring command authority to look would push operators toward
            // holding a stronger certificate than the task needs.
            authorize(authorizer.authorize_read(identity))?;
            let response = handler.read_segment_placement(request).await?;
            Ok(VtpmFrame {
                kind: KIND_ADMIN_READ_SEGMENT_PLACEMENT_RESP,
                payload: response.encode(),
            })
        }
        KIND_ADMIN_READ_RANGE_TRANSITIONS_REQ => {
            let request = AdminReadRangeTransitionsRequest::decode(&frame.payload)?;
            // A read: the chain is evidence anybody entitled to look at the
            // cluster may check, and requiring command authority to read it
            // would defeat the point of keeping it.
            authorize(authorizer.authorize_read(identity))?;
            let response = handler.read_range_transitions(request).await?;
            Ok(VtpmFrame {
                kind: KIND_ADMIN_READ_RANGE_TRANSITIONS_RESP,
                payload: response.encode()?,
            })
        }
        KIND_ADMIN_READ_GROUP_CURSOR_REQ => {
            let request = AdminReadGroupCursorRequest::decode(&frame.payload)?;
            // A read, like the others: what a group committed is evidence
            // anybody entitled to look at the cluster may check.
            authorize(authorizer.authorize_read(identity))?;
            let response = handler.read_group_cursor(request).await?;
            Ok(VtpmFrame {
                kind: KIND_ADMIN_READ_GROUP_CURSOR_RESP,
                payload: response.encode()?,
            })
        }
        KIND_ADMIN_READ_TOPIC_RANGES_REQ => {
            let request = AdminReadTopicRangesRequest::decode(&frame.payload)?;
            // A read: the shape of a topic is evidence anybody entitled to
            // look at the cluster may check.
            authorize(authorizer.authorize_read(identity))?;
            let response = handler.read_topic_ranges(request).await?;
            Ok(VtpmFrame {
                kind: KIND_ADMIN_READ_TOPIC_RANGES_RESP,
                payload: response.encode()?,
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

/// One metadata node this client may talk to.
#[derive(Clone, Debug)]
pub struct AdminCandidate {
    /// Known when the candidate came from a configured peer list, which is
    /// what lets a redirect be followed to a SPECIFIC node rather than by
    /// rotating hopefully through all of them.
    pub node_id: Option<MetaNodeId>,
    /// Where this candidate was, last time its name was looked up.
    pub endpoint: SocketAddr,
    /// The `host:port` it was configured under, when it was a name (#367).
    ///
    /// A candidate that was absent from DNS at startup, or that has since
    /// moved, is otherwise dialled at a dead address for the life of the
    /// client — and this list is where redirects LAND. If the unreachable one
    /// becomes the metadata leader, every other candidate names it, the walk
    /// visits an address that answers nothing, and the request fails with two
    /// healthy members in the list who both know exactly where the leader is
    /// (review). Re-resolved before each attempt, and only when the previous
    /// one to this candidate failed.
    ///
    /// `None` for a candidate given as a literal address.
    pub host: Option<String>,
    pub server_name: String,
    /// Speak to this endpoint WITHOUT TLS (#294).
    ///
    /// Per candidate rather than per client because a cluster can legitimately
    /// be mid-migration, with some nodes converted and some not. A single flag
    /// would force the whole group to move at once, which is exactly the
    /// pressure that produces a big-bang change nobody can roll back.
    pub plaintext: bool,
}

/// Client for the admin endpoint.
///
/// Holds every metadata node it may ask, not one. Reads and writes on this
/// plane must reach the Raft LEADER; a non-leader refuses, and with a single
/// fixed endpoint that refusal was terminal — which is why a co-located
/// deployment only worked on whichever pod happened to co-locate the leader,
/// and which pod that is depends on an election (#292).
pub struct AdminClient {
    /// `None` when every candidate is plaintext: a client that never speaks
    /// TLS should not require the material to build a connector it will not
    /// use, or plaintext would still need a certificate to start.
    connector: Option<TlsConnector>,
    candidates: Vec<AdminCandidate>,
    /// Index of the candidate that last answered as leader. Steady state is
    /// therefore one round trip, not a scan: leadership changes rarely, so
    /// remembering the answer is what keeps this from taxing every request.
    preferred: std::sync::atomic::AtomicUsize,
    /// How many redirects this client has actually followed.
    ///
    /// Observable on purpose. A caller that wants to know its configured
    /// endpoint is not the leader can only learn it by watching what happened —
    /// inferring it from a role snapshot taken beforehand is a guess that
    /// leadership can invalidate between the snapshot and the request. A test
    /// asserting redirect support needs the same thing, for the same reason.
    redirects: std::sync::atomic::AtomicUsize,
    /// Where each candidate is now, and whether that is still believed.
    ///
    /// Index-aligned with `candidates`, and it is where a refreshed address
    /// LIVES (#367). Resolving into a local copy would find the peer once and
    /// forget it — the next request would dial the configured address again
    /// and pay the connect timeout to rediscover the same thing (review).
    resolutions: Mutex<Vec<Resolution>>,
}

/// Clears a candidate's in-flight lookup flag however the lookup ends.
///
/// The flag is set before an await, and the callers put deadlines over whole
/// rounds — `LeaseAgent::bounded` does exactly this — so the future CAN be
/// dropped mid-lookup (review). A flag cleared only on the paths that RETURN
/// would then stay set forever, excluding the candidate from re-resolution for
/// the life of the client: the fix for a race turning into a worse bug than
/// the race. Drop runs on cancellation, which is the only thing that does.
struct ResolvingGuard<'a> {
    resolutions: &'a Mutex<Vec<Resolution>>,
    index: usize,
}

impl Drop for ResolvingGuard<'_> {
    fn drop(&mut self) {
        // A poisoned lock is not worth a panic inside a Drop; the flag simply
        // stays set on a client that has already failed harder than this.
        if let Ok(mut resolutions) = self.resolutions.lock() {
            if let Some(resolution) = resolutions.get_mut(self.index) {
                resolution.resolving = false;
            }
        }
    }
}

/// One candidate's live address, and what is known about it.
struct Resolution {
    /// The address in use: configured at first, whatever the name last
    /// resolved to afterwards.
    endpoint: SocketAddr,
    /// Set when the last attempt to this candidate failed, for any reason —
    /// including a stale address that accepts TCP and then is not the service
    /// we wanted (review). Cleared by a success, which is the only thing that
    /// proves an address.
    stale: bool,
    /// When the name was last looked up, so a candidate that is simply down
    /// is not a name query per request.
    resolved_at: Option<std::time::Instant>,
    /// A lookup for this candidate is running right now.
    ///
    /// The throttle alone does not bound concurrency: it is 200 ms and a
    /// lookup may take up to `RESOLVE_TIMEOUT`, so a second request can start
    /// one while the first is still out — and if the name changed between
    /// them, the SLOWER lookup lands last and reverts the candidate to the
    /// older address (review). One at a time removes the race rather than
    /// ordering it. The same guard is on the Raft peer directory, and having
    /// fixed one and not the other is how this was found.
    resolving: bool,
}

/// How long one candidate may take to become a usable connection before the
/// walk moves on.
///
/// UNBOUNDED BEFORE, AND THAT WAS THE WHOLE FAILURE (#367). `attempt` awaited
/// `TcpStream::connect` and the TLS handshake with no deadline of its own, so
/// a candidate whose address no longer belongs to anything hung in the
/// kernel's SYN retry — roughly two minutes on Linux — while the caller's
/// entire budget (the lease agent's is 5 s) drained against it. Two healthy
/// members sat in the same list, unasked, and every metadata call from that
/// node failed with "no answer within 5s".
///
/// Two seconds against a five-second budget: a three-member cluster can lose
/// its first two candidates to this and still reach the third inside the
/// caller's deadline, and it matches the replication plane's own connect
/// timeout so the two planes fail over on comparable timescales.
///
/// ESTABLISHMENT ONLY, and the boundary is load-bearing (review). A first
/// draft bounded the whole exchange, which would have made this deadline a
/// limit on how long a legitimate operation may take on the SERVER — and
/// `init` alone can spend `MEMBERSHIP_PUBLISH_MS` waiting for membership to
/// publish, which is also two seconds. The client would have reported failure
/// for a mutation that succeeded, which is a worse answer than a slow one.
/// Once a connection exists, the operation is bounded by the caller's own
/// budget, which is the thing that knows how long it is willing to wait.
const CANDIDATE_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// How often one candidate's name may be looked up again.
///
/// A candidate that is down fails every request, and each failure asks for a
/// lookup; without a floor that is a name query per metadata call (review).
const RERESOLVE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// How long one name lookup may take before it is abandoned.
///
/// This runs inside the caller's own budget — five seconds for a lease round —
/// so a resolver that stalls must not be able to consume it (review).
const RESOLVE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

impl AdminClient {
    pub fn new(
        material: TlsMaterial,
        endpoint: SocketAddr,
        server_name: impl Into<String>,
    ) -> TransportResult<Self> {
        Self::with_candidates(
            material,
            vec![AdminCandidate {
                node_id: None,
                endpoint,
                host: None,
                server_name: server_name.into(),
                plaintext: false,
            }],
        )
    }

    /// Build a client that can follow a redirect to any of `candidates`.
    ///
    /// Refuses an empty list rather than constructing a client that can reach
    /// nothing: the failure would otherwise surface on first use as an
    /// unhelpful "no candidates" at a point far from the configuration that
    /// caused it.
    pub fn with_candidates(
        material: TlsMaterial,
        candidates: Vec<AdminCandidate>,
    ) -> TransportResult<Self> {
        Self::build(Some(material), candidates)
    }

    /// A client that speaks to plaintext endpoints only (#294).
    ///
    /// Takes no TLS material, deliberately: requiring a certificate to talk to
    /// an endpoint that will never ask for one would leave plaintext needing a
    /// PKI anyway, which defeats the point of having the mode.
    pub fn plaintext(candidates: Vec<AdminCandidate>) -> TransportResult<Self> {
        if let Some(needs_tls) = candidates.iter().find(|candidate| !candidate.plaintext) {
            return Err(TransportError::Tls(format!(
                "endpoint {} expects TLS but this client was built without any material; \
                 build it with `with_candidates` instead, or mark the endpoint plaintext",
                needs_tls.endpoint
            )));
        }
        Self::build(None, candidates)
    }

    fn build(
        material: Option<TlsMaterial>,
        candidates: Vec<AdminCandidate>,
    ) -> TransportResult<Self> {
        if candidates.is_empty() {
            return Err(TransportError::Protocol(
                "an admin client needs at least one metadata endpoint".to_owned(),
            ));
        }
        // Built only when something will use it. A TLS candidate with no
        // material is refused HERE rather than at first use, so a
        // misconfiguration surfaces at construction instead of as a connection
        // failure at an unrelated moment.
        let connector = match material {
            Some(material) => Some(super::tls::build_client_connector(material)?),
            None => None,
        };
        if connector.is_none() && candidates.iter().any(|candidate| !candidate.plaintext) {
            return Err(TransportError::Tls(
                "a TLS endpoint was configured without any TLS material".to_owned(),
            ));
        }
        // A CANDIDATE WHOSE NAME HAD NOT RESOLVED CARRIES A PLACEHOLDER, NOT AN
        // ADDRESS (#367). Seeding it as already-unreachable makes the very
        // first attempt look the name up rather than dial nothing — otherwise
        // the peer would have to fail once before it could ever be found, and
        // for a peer that later becomes the leader that is one failed request
        // per lease round, forever, since a redirect always lands back on it.
        let resolutions = candidates
            .iter()
            .map(|candidate| Resolution {
                endpoint: candidate.endpoint,
                // A candidate whose name had not resolved carries a
                // placeholder, not an address, so the first attempt must look
                // it up rather than dial nothing.
                stale: candidate.host.is_some() && candidate.endpoint.port() == 0,
                resolved_at: None,
                resolving: false,
            })
            .collect();
        Ok(Self {
            connector,
            candidates,
            preferred: std::sync::atomic::AtomicUsize::new(0),
            redirects: std::sync::atomic::AtomicUsize::new(0),
            resolutions: Mutex::new(resolutions),
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

    /// Read a range's transition chain through a linearizable fence
    /// (#240 item 5).
    pub async fn read_range_transitions(
        &self,
        topic_uuid: uuid::Uuid,
        range_uuid: uuid::Uuid,
        from_epoch: u64,
        limit: u16,
    ) -> TransportResult<AdminReadRangeTransitionsResponse> {
        let frame = self
            .round_trip(VtpmFrame {
                kind: KIND_ADMIN_READ_RANGE_TRANSITIONS_REQ,
                payload: AdminReadRangeTransitionsRequest {
                    topic_uuid,
                    range_uuid,
                    from_epoch,
                    limit,
                }
                .encode(),
            })
            .await?;
        match frame.kind {
            KIND_ADMIN_READ_RANGE_TRANSITIONS_RESP => {
                Ok(AdminReadRangeTransitionsResponse::decode(&frame.payload)?)
            }
            KIND_ADMIN_ERROR => {
                let error = AdminError::decode(&frame.payload)?;
                Err(TransportError::Protocol(error.message))
            }
            other => Err(TransportError::UnexpectedKind(other)),
        }
    }

    /// Linearizable read of what `group_uuid` committed on `range_uuid`
    /// (#457 slice 2b).
    /// A topic's ranges in partition order (#457 slice 3), through the same
    /// linearizable fence as every admin read.
    pub async fn read_topic_ranges(
        &self,
        topic_uuid: uuid::Uuid,
    ) -> TransportResult<AdminReadTopicRangesResponse> {
        let frame = self
            .round_trip(VtpmFrame {
                kind: KIND_ADMIN_READ_TOPIC_RANGES_REQ,
                payload: AdminReadTopicRangesRequest { topic_uuid }.encode(),
            })
            .await?;
        match frame.kind {
            KIND_ADMIN_READ_TOPIC_RANGES_RESP => {
                Ok(AdminReadTopicRangesResponse::decode(&frame.payload)?)
            }
            KIND_ADMIN_ERROR => {
                let error = AdminError::decode(&frame.payload)?;
                Err(TransportError::Protocol(error.message))
            }
            other => Err(TransportError::UnexpectedKind(other)),
        }
    }

    pub async fn read_group_cursor(
        &self,
        group_uuid: uuid::Uuid,
        topic_uuid: uuid::Uuid,
        range_uuid: uuid::Uuid,
    ) -> TransportResult<AdminReadGroupCursorResponse> {
        let frame = self
            .round_trip(VtpmFrame {
                kind: KIND_ADMIN_READ_GROUP_CURSOR_REQ,
                payload: AdminReadGroupCursorRequest {
                    group_uuid,
                    topic_uuid,
                    range_uuid,
                }
                .encode(),
            })
            .await?;
        match frame.kind {
            KIND_ADMIN_READ_GROUP_CURSOR_RESP => {
                Ok(AdminReadGroupCursorResponse::decode(&frame.payload)?)
            }
            KIND_ADMIN_ERROR => {
                let error = AdminError::decode(&frame.payload)?;
                Err(TransportError::Protocol(error.message))
            }
            other => Err(TransportError::UnexpectedKind(other)),
        }
    }

    /// Read a segment's placement through a linearizable fence.
    pub async fn read_segment_placement(
        &self,
        topic_uuid: uuid::Uuid,
        range_uuid: uuid::Uuid,
        segment_uuid: uuid::Uuid,
        for_replication_factor: u8,
    ) -> TransportResult<AdminReadSegmentPlacementResponse> {
        let frame = self
            .round_trip(VtpmFrame {
                kind: KIND_ADMIN_READ_SEGMENT_PLACEMENT_REQ,
                payload: AdminReadSegmentPlacementRequest {
                    topic_uuid,
                    range_uuid,
                    segment_uuid,
                    for_replication_factor,
                }
                .encode(),
            })
            .await?;
        match frame.kind {
            KIND_ADMIN_READ_SEGMENT_PLACEMENT_RESP => {
                Ok(AdminReadSegmentPlacementResponse::decode(&frame.payload)?)
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

    /// Send `request`, following a leader redirect if the answer is one.
    ///
    /// A redirect names a node id, not an address — Raft has no address to give
    /// (`EmptyNode`) — so it is resolved against the configured candidates. When
    /// the named node is not among them, or the answering node knows of no
    /// leader at all, this falls back to trying the remaining candidates in
    /// order: an election gap is a "come back shortly", not a routing decision,
    /// and chasing a leader that does not exist yet would be worse than asking
    /// around.
    ///
    /// Bounded by the number of candidates, so a cluster mid-election cannot
    /// turn one request into an unbounded chase. The last redirect is what gets
    /// returned if they all refuse, because "none of these is the leader" is the
    /// useful message — not a connection error from whichever one happened to
    /// be tried last.
    async fn round_trip(&self, request: VtpmFrame) -> TransportResult<VtpmFrame> {
        use std::sync::atomic::Ordering;

        let mut index = self.preferred.load(Ordering::Relaxed) % self.candidates.len();
        // VISITED, not a counter. Counting attempts bounds the work but does not
        // guarantee coverage, and the difference is a real failure: during an
        // election two followers can hold stale views of each other, so A names
        // B and B names A. Following those hints alternates A→B→A and burns the
        // whole budget without ever contacting C — which may be the leader,
        // configured in the same list. The request then fails for want of
        // trying, which is the very outage this exists to prevent.
        let mut visited = vec![false; self.candidates.len()];
        let mut last_error: Option<TransportError> = None;

        while let Some(current) = self.next_unvisited(index, &visited) {
            index = current;
            visited[index] = true;
            let candidate = &self.candidates[index];
            match self.attempt(index, &request).await {
                Ok(frame) => {
                    // A frame is not automatically success: a non-leader
                    // answers with KIND_ADMIN_ERROR, and the redirect inside it
                    // is the whole point of this loop.
                    if let Some(hint) = redirect_of(&frame) {
                        // Remember nothing on a redirect — this candidate just
                        // told us it is the wrong one. Steer to the node it
                        // named only if that node is still unvisited; otherwise
                        // fall through to the next one that is, so a bouncing
                        // pair cannot trap the walk.
                        let named = hint
                            .leader
                            .and_then(|leader| self.index_of(leader))
                            .filter(|found| !visited[*found]);
                        last_error = Some(TransportError::NotLeader {
                            message: format!(
                                "{} is not the metadata leader{}",
                                candidate.endpoint,
                                match hint.leader {
                                    Some(id) => format!("; it named node {id}"),
                                    None => "; and knows of no leader".to_owned(),
                                }
                            ),
                            leader: hint.leader,
                        });
                        self.redirects.fetch_add(1, Ordering::Relaxed);
                        index = named.unwrap_or(index);
                        continue;
                    }
                    self.preferred.store(index, Ordering::Relaxed);
                    // A NON-REDIRECT ANSWER CLEARS THE STALENESS; A REDIRECT
                    // DOES NOT. Review has moved this line three times, so
                    // here is the invariant rather than another preference.
                    //
                    // A redirect is structurally the one response that carries
                    // no information about WHO answered: it names the leader,
                    // not the responder. So an address that has been reassigned
                    // to a different member — plaintext, or a shared SAN —
                    // answers a redirect perfectly well, and treating that as
                    // proof freezes the wrong address in place forever. Every
                    // other answer came back from the endpoint we selected, for
                    // the operation we asked it.
                    //
                    // The cost is real and bounded: a candidate that keeps
                    // redirecting is looked up again once per throttle
                    // interval. In steady state `preferred` is the leader and
                    // answers directly, so this is paid during a leadership
                    // change, which is when being right about addresses
                    // matters most.
                    if let Some(resolution) =
                        self.resolutions.lock().expect("resolutions").get_mut(index)
                    {
                        resolution.stale = false;
                    }
                    return Ok(frame);
                }
                // An unreachable candidate is worth moving past for the same
                // reason as a redirect: one node being down must not make the
                // cluster unusable when the others can answer.
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            TransportError::Protocol("no metadata endpoint could be reached".to_owned())
        }))
    }

    /// `from` if it has not been tried, else the next unvisited candidate in
    /// ring order, else `None` once every candidate has been asked exactly once.
    fn next_unvisited(&self, from: usize, visited: &[bool]) -> Option<usize> {
        (0..self.candidates.len())
            .map(|offset| (from + offset) % self.candidates.len())
            .find(|index| !visited[*index])
    }

    /// How many redirects this client has followed since it was built.
    ///
    /// Zero means every request reached a leader on its first hop — which is
    /// the normal case, and is exactly why a test that only checks the request
    /// SUCCEEDED cannot tell whether redirect support is present at all.
    pub fn redirects_followed(&self) -> usize {
        self.redirects.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn index_of(&self, leader: MetaNodeId) -> Option<usize> {
        self.candidates
            .iter()
            .position(|candidate| candidate.node_id == Some(leader))
    }

    /// `attempt`, but it gives up on one candidate instead of on the request.
    ///
    /// The timeout is per candidate rather than over the whole walk on
    /// purpose: the walk's job is to find a member that answers, and spending
    /// the whole budget proving that the first one does not is the opposite of
    /// that job.
    /// Attempt this candidate, giving ESTABLISHMENT its own deadline.
    ///
    /// The address is looked up again first when the candidate carries a name
    /// and its last attempt failed (#367): this list is where redirects land,
    /// so a candidate stuck at a dead address is not merely one member fewer —
    /// if it becomes the leader, every other candidate names it and the walk
    /// visits an address that answers nothing, with two healthy members in the
    /// list who both know exactly where the leader is.
    async fn attempt(&self, index: usize, request: &VtpmFrame) -> TransportResult<VtpmFrame> {
        // ONE BUDGET PER CANDIDATE, COVERING THE LOOKUP TOO (review). Bounding
        // only the connection let a stalled resolver add its own half second
        // on top, so a candidate could cost 2.5 s of the caller's 5 s and two
        // of them could spend the lot before the third — healthy — member was
        // ever asked. Finding out where a candidate is, is part of reaching
        // it, and the documented arithmetic below depends on that being true.
        // PRESUMED FAILED UNTIL A FRAME COMES BACK (review). Marking staleness
        // in the error arms could not survive cancellation: `LeaseAgent::bounded`
        // cancels this future at its round deadline, so an endpoint that
        // accepted a connection and then hung was left marked GOOD and every
        // later request reconnected to it — never re-resolving the name that
        // would have found the peer. Presuming failure is cancellation-safe by
        // construction, and `round_trip` clears it on the only evidence that
        // counts, which is an answer. See the mark's placement below, which is
        // as load-bearing as the inversion itself.
        let reached = tokio::time::timeout(CANDIDATE_CONNECT_TIMEOUT, async {
            let candidate = self.resolved_candidate(index).await;
            // MARKED HERE: after the lookup decision has read the old value,
            // and before the exchange that can be cancelled. Marking earlier
            // was cancellation-safe but made every request past the 200 ms
            // throttle re-resolve a candidate that had just answered — a name
            // query on the lease agent's hot path, and up to half a second of
            // stalled resolver in front of a cached endpoint that was
            // perfectly reachable (review).
            //
            // Nothing is lost by moving it: cancellation DURING the lookup can
            // only happen when the candidate was already stale, which is why
            // the lookup was running.
            self.mark_stale(index);
            let endpoint = candidate.endpoint;
            self.establish(&candidate)
                .await
                .map(|stream| (stream, endpoint))
        })
        .await;
        // THE ADDRESS ACTUALLY TRIED, not the one this client was built with
        // (review). A timeout that names the configured endpoint — or worse,
        // the `0.0.0.0:0` placeholder — sends an operator looking at the wrong
        // machine.
        let attempted = self
            .resolutions
            .lock()
            .expect("resolutions")
            .get(index)
            .map(|resolution| resolution.endpoint)
            .unwrap_or(self.candidates[index].endpoint);
        let mut stream = match reached {
            Ok(Ok((stream, _))) => stream,
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(TransportError::Protocol(format!(
                    "{attempted} could not be reached within {CANDIDATE_CONNECT_TIMEOUT:?}"
                )));
            }
        };
        // Beyond the connection the caller's budget is the bound, because it
        // is the thing that knows how long this operation is worth waiting
        // for. A server-side deadline imposed here would report a completed
        // mutation as a failure.
        //
        // A FAILURE HERE IS ALSO A REASON TO DOUBT THE ADDRESS (review). An
        // address something else has taken over accepts the connection and
        // then is not the service we wanted, so keying the refresh on connect
        // failures alone would leave that candidate believed and wrong.
        match async {
            write_frame(&mut stream, request).await?;
            read_frame(&mut stream).await
        }
        .await
        {
            Ok(frame) => Ok(frame),
            Err(error) => Err(error),
        }
    }

    /// This candidate, with its address re-resolved if the last attempt failed
    /// and the throttle has passed — and the answer KEPT.
    async fn resolved_candidate(&self, index: usize) -> AdminCandidate {
        let mut candidate = self.candidates[index].clone();
        let Some(host) = candidate.host.clone() else {
            return candidate;
        };
        {
            // The stamp is claimed before the await, so concurrent requests to
            // a candidate whose resolver has stopped answering do not each
            // start their own query (review).
            let mut resolutions = self.resolutions.lock().expect("resolutions");
            let Some(resolution) = resolutions.get_mut(index) else {
                return candidate;
            };
            candidate.endpoint = resolution.endpoint;
            let due = resolution.stale
                && !resolution.resolving
                && !resolution
                    .resolved_at
                    .is_some_and(|at| at.elapsed() < RERESOLVE_INTERVAL);
            if !due {
                return candidate;
            }
            resolution.resolved_at = Some(std::time::Instant::now());
            resolution.resolving = true;
        }
        // Armed BEFORE the await that can be cancelled.
        let _resolving = ResolvingGuard {
            resolutions: &self.resolutions,
            index,
        };
        let resolved =
            match tokio::time::timeout(RESOLVE_TIMEOUT, tokio::net::lookup_host(&host)).await {
                Ok(Ok(mut addrs)) => addrs.next(),
                Ok(Err(error)) => {
                    eprintln!("could not resolve metadata endpoint {host:?}: {error}");
                    None
                }
                Err(_) => {
                    eprintln!("resolving metadata endpoint {host:?} exceeded {RESOLVE_TIMEOUT:?}");
                    None
                }
            };
        // A lookup that fails leaves the last known address standing: a
        // resolver hiccup is not evidence that a member moved.
        if let Some(addr) = resolved {
            // PERSISTED, not used and forgotten. An address that lived only in
            // this clone would be rediscovered — at the cost of a connect
            // timeout — on every request after the next success cleared the
            // staleness flag (review).
            if let Some(resolution) = self.resolutions.lock().expect("resolutions").get_mut(index) {
                resolution.endpoint = addr;
            }
            candidate.endpoint = addr;
        }
        candidate
    }

    fn mark_stale(&self, index: usize) {
        if let Some(resolution) = self.resolutions.lock().expect("resolutions").get_mut(index) {
            resolution.stale = true;
        }
    }

    async fn establish(&self, candidate: &AdminCandidate) -> TransportResult<MaybeTls<TcpStream>> {
        let tcp = TcpStream::connect(candidate.endpoint).await?;
        if candidate.plaintext {
            return Ok(MaybeTls::Plain(tcp));
        }
        let connector = self.connector.as_ref().ok_or_else(|| {
            // Unreachable: `build` refuses this combination. Written as a
            // refusal anyway, because the alternative is an unwrap that
            // turns a configuration mistake into a panic in a server.
            TransportError::Tls("no TLS material for a TLS endpoint".to_owned())
        })?;
        let name = server_name(&candidate.server_name)?;
        Ok(MaybeTls::Tls(Box::new(
            connector
                .connect(name, tcp)
                .await
                .map_err(|error| {
                    TransportError::Tls(format!(
                        "admin connect: {error}{}",
                        tls_handshake_hint(&error)
                    ))
                })?
                .into(),
        )))
    }
}

/// The redirect an error frame carries, if it is one.
///
/// A frame that fails to decode is deliberately NOT treated as a redirect: the
/// callers decode it again and report the real parse failure, which is more
/// useful than silently trying the next node on a malformed reply.
fn redirect_of(frame: &VtpmFrame) -> Option<NotLeaderHint> {
    if frame.kind != KIND_ADMIN_ERROR {
        return None;
    }
    AdminError::decode(&frame.payload).ok()?.not_leader
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

        async fn read_range_transitions(
            &self,
            _request: AdminReadRangeTransitionsRequest,
        ) -> TransportResult<AdminReadRangeTransitionsResponse> {
            Ok(AdminReadRangeTransitionsResponse {
                found: false,
                transitions: Vec::new(),
                read_at_applied_index: 0,
            })
        }

        async fn read_segment_placement(
            &self,
            _request: AdminReadSegmentPlacementRequest,
        ) -> TransportResult<AdminReadSegmentPlacementResponse> {
            Ok(AdminReadSegmentPlacementResponse {
                found: false,
                generation: 0,
                declared_replication_factor: 0,
                replica_nodes: Vec::new(),
                rebalance_intent: None,
                segment: None,
                proposal: None,
                read_at_applied_index: 0,
            })
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

    /// The admin plane over a bare TCP socket: no certificates anywhere (#294).
    ///
    /// This is the first plane made optional, and the one an operator meets
    /// first through `vtopctl`. Until now the engine could not be started at
    /// all without minting a PKI, which is a real barrier for local
    /// development, CI, and evaluation — and encrypting a loopback socket buys
    /// nothing.
    ///
    /// The assertion is a real REQUEST AND RESPONSE, not a successful connect.
    /// A handshake-free socket that then failed to frame would look like
    /// success to anything weaker.
    #[tokio::test]
    async fn the_admin_plane_serves_a_request_over_plaintext() {
        let handler = Arc::new(CountingHandler::default());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = AdminServer::plaintext(
            Arc::clone(&handler) as Arc<dyn AdminHandler>,
            AdminAuthorizer::permissive(),
        )
        .expect("a permissive plaintext endpoint is allowed");
        let task = tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        // NO TLS MATERIAL on the client either. Requiring a certificate to talk
        // to an endpoint that never asks for one would leave plaintext needing
        // a PKI anyway, which is the whole thing being removed.
        let client = AdminClient::plaintext(vec![AdminCandidate {
            node_id: Some(MetaNodeId(1)),
            endpoint: addr,
            host: None,
            server_name: String::new(),
            plaintext: true,
        }])
        .unwrap();

        let status = client
            .status()
            .await
            .expect("a plaintext admin endpoint must answer a status request");
        assert_eq!(status.node_id, MetaNodeId(1));

        // A write too, so this covers the propose path and not only the read
        // one — they take different routes through dispatch.
        client.propose(acquire(NODE_A)).await.unwrap_err();
        assert_eq!(handler.proposals.load(Ordering::SeqCst), 1);

        task.abort();
    }

    /// A candidate that was not in DNS at startup is found once it appears
    /// (#367).
    ///
    /// The address in the list is where a member was, and a candidate whose
    /// name had not resolved yet has no address at all — only a placeholder.
    /// This list is also where every redirect LANDS, so a candidate frozen at
    /// an address that answers nothing is not merely one member fewer: if it
    /// becomes the leader, the other members all name it, the walk visits
    /// nothing, and the request fails with two healthy nodes in the list who
    /// both know exactly where the leader is.
    ///
    /// The client here has ONE candidate and no fallback, so the request can
    /// only succeed through the name.
    #[tokio::test]
    async fn a_candidate_that_was_not_resolvable_at_startup_is_found_once_it_answers() {
        let handler = Arc::new(CountingHandler::default());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = AdminServer::plaintext(
            Arc::clone(&handler) as Arc<dyn AdminHandler>,
            AdminAuthorizer::permissive(),
        )
        .expect("a permissive plaintext endpoint is allowed");
        let task = tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let client = AdminClient::plaintext(vec![AdminCandidate {
            node_id: Some(MetaNodeId(1)),
            // The placeholder a node carries for a peer whose name did not
            // resolve when the client was built. Nothing listens here.
            endpoint: "0.0.0.0:0".parse().unwrap(),
            host: Some(format!("127.0.0.1:{}", addr.port())),
            server_name: String::new(),
            plaintext: true,
        }])
        .unwrap();

        let status = client.status().await.expect(
            "the only candidate is reachable through its NAME, so a client that dials \
             the placeholder it was built with can never answer this",
        );
        assert_eq!(status.node_id, MetaNodeId(1));

        task.abort();
    }

    /// A plaintext endpoint REFUSES an enforcing authorization policy.
    ///
    /// This is the load-bearing refusal of the whole slice. Admin authorization
    /// identifies callers by the Common Name of their client certificate; with
    /// no TLS there is no certificate, so no CN, so nothing for the policy to
    /// match. Accepting the pair would hand an operator who had deliberately
    /// restricted cluster commands an endpoint where every caller is permitted
    /// — a policy that silently does not apply, which is worse than no policy
    /// because it reads as protection in the config.
    #[tokio::test]
    async fn a_plaintext_admin_endpoint_refuses_a_policy_it_could_not_enforce() {
        let refused = AdminServer::plaintext(
            Arc::new(CountingHandler::default()) as Arc<dyn AdminHandler>,
            AdminAuthorizer::with_operators(["ops-alice".to_owned()]),
        )
        .map(|_| ())
        .expect_err("an unenforceable policy must not be accepted quietly");

        let text = refused.to_string();
        assert!(
            text.contains("no client certificate"),
            "the refusal must name the cause: {text}"
        );
        assert!(
            text.contains("enable TLS") && text.contains("remove the authorization policy"),
            "and both ways out, since either is a legitimate choice: {text}"
        );
    }

    /// An anonymous caller is never an operator.
    ///
    /// Unreachable today — a plaintext endpoint refuses to be built with an
    /// enforcing policy, so the two never meet — but asserted rather than left
    /// to that argument. The day those facts drift apart, this is what decides
    /// whether a policy holds, and a `match` arm that returned `true` would be
    /// an identity invented in order to authorize it.
    #[test]
    fn an_anonymous_caller_is_never_an_operator() {
        let authorizer = AdminAuthorizer::with_operators(["ops-alice".to_owned()]);
        assert!(
            authorizer
                .authorize_cluster(&AdminIdentity::Anonymous)
                .is_err(),
            "a caller that presented no claim must not pass an operator check"
        );
        // And it describes itself as what it is, since this string lands in
        // refusals an operator has to interpret.
        assert!(AdminIdentity::Anonymous
            .describe()
            .contains("unauthenticated"));
    }

    /// A plaintext admin endpoint REFUSES a non-loopback address by default.
    ///
    /// The refusal above covers a policy that could not be enforced. This
    /// covers the other half, which is worse: NO policy at all. A plaintext
    /// endpoint has no client certificate, so no caller identity, so
    /// `authorize_cluster` permits everyone — correct for an unconfigured
    /// endpoint and catastrophic for an exposed one. Anything that reaches the
    /// port could run `meta init`, change membership, and propose arbitrary
    /// commands, and nothing in the configuration would have said so.
    ///
    /// Checked at BIND time rather than per connection: a misconfigured
    /// deployment should fail to start, not serve until the first unwanted
    /// caller arrives.
    #[tokio::test]
    async fn a_plaintext_admin_endpoint_refuses_a_routable_address() {
        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let server = AdminServer::plaintext(
            Arc::new(CountingHandler::default()) as Arc<dyn AdminHandler>,
            AdminAuthorizer::permissive(),
        )
        .unwrap();

        let refused = server
            .serve(listener)
            .await
            .expect_err("an unauthenticated admin port must not bind to a routable address");
        let text = refused.to_string();
        assert!(
            text.contains("not a loopback address"),
            "the refusal must name the cause: {text}"
        );
        assert!(
            text.contains("meta init") && text.contains("membership"),
            "and what a reachable caller could do, since that is the reason: {text}"
        );
        assert!(
            text.contains("127.0.0.1")
                && text.contains("enable TLS")
                && text.contains("plaintext_on_any_interface"),
            "and all three ways out, since each is legitimate in a different \
             situation: {text}"
        );
    }

    /// Loopback is allowed, which is the case the mode exists for.
    ///
    /// Paired with the refusal above deliberately: a guard that refused
    /// everything would pass that test while making the feature useless, and
    /// only asserting both pins the boundary rather than the direction.
    #[tokio::test]
    async fn a_plaintext_admin_endpoint_serves_on_loopback() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = AdminServer::plaintext(
            Arc::new(CountingHandler::default()) as Arc<dyn AdminHandler>,
            AdminAuthorizer::permissive(),
        )
        .unwrap();
        let task = tokio::spawn(async move { server.serve(listener).await });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let client = AdminClient::plaintext(vec![AdminCandidate {
            node_id: Some(MetaNodeId(1)),
            endpoint: addr,
            host: None,
            server_name: String::new(),
            plaintext: true,
        }])
        .unwrap();
        assert_eq!(client.status().await.unwrap().node_id, MetaNodeId(1));

        task.abort();
    }

    /// #294 slice 6: the admin plane refuses a peer speaking the other
    /// transport by name, in both directions, before any frame or handshake.
    #[tokio::test]
    async fn the_admin_plane_refuses_a_cross_mode_peer_naming_both_sides() {
        use tokio::io::AsyncWriteExt;
        async fn accepted_with(first_bytes: &[u8]) -> TcpStream {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let mut client = TcpStream::connect(addr).await.unwrap();
            let (server, _) = listener.accept().await.unwrap();
            client.write_all(first_bytes).await.unwrap();
            // Held open: the judgment must not depend on a hang-up.
            std::mem::forget(client);
            server
        }
        let handler = Arc::new(CountingHandler::default()) as Arc<dyn AdminHandler>;

        // A TLS hello on a plaintext plane.
        let tcp = accepted_with(&[0x16, 0x03, 0x01, 0x00, 0x40]).await;
        let refused = serve_admin_connection(
            None,
            tcp,
            Arc::clone(&handler),
            Arc::new(AdminAuthorizer::permissive()),
        )
        .await
        .unwrap_err();
        let TransportError::CrossMode(message) = refused else {
            panic!("expected a cross-mode refusal, got {refused}")
        };
        assert!(
            message.contains("opened a TLS handshake")
                && message.contains("`admin_transport: plaintext`"),
            "{message}"
        );

        // A plaintext frame on a TLS plane.
        let pki = SharedPki::new();
        let acceptor = crate::transport::tls::build_server_acceptor(pki.server_material()).unwrap();
        let tcp = accepted_with(b"VTPM\0\x01").await;
        let refused = serve_admin_connection(
            Some(acceptor),
            tcp,
            handler,
            Arc::new(AdminAuthorizer::permissive()),
        )
        .await
        .unwrap_err();
        let TransportError::CrossMode(message) = refused else {
            panic!("expected a cross-mode refusal, got {refused}")
        };
        assert!(
            message.contains("sent a plaintext frame")
                && message.contains("`admin_transport: tls`"),
            "{message}"
        );
    }

    /// The escape hatch works, and has to be typed to get it.
    ///
    /// Some deployments genuinely want this — a trusted network segment, a
    /// sidecar-terminated mesh. The point is not to forbid it but to make it
    /// impossible to arrive at by accident.
    #[tokio::test]
    async fn a_routable_plaintext_endpoint_is_available_when_asked_for_explicitly() {
        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = AdminServer::plaintext_on_any_interface(
            Arc::new(CountingHandler::default()) as Arc<dyn AdminHandler>,
            AdminAuthorizer::permissive(),
        )
        .unwrap();
        let task = tokio::spawn(async move { server.serve(listener).await });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let client = AdminClient::plaintext(vec![AdminCandidate {
            node_id: Some(MetaNodeId(1)),
            endpoint: format!("127.0.0.1:{port}").parse().unwrap(),
            host: None,
            server_name: String::new(),
            plaintext: true,
        }])
        .unwrap();
        assert_eq!(client.status().await.unwrap().node_id, MetaNodeId(1));

        task.abort();
    }

    /// A non-leader redirects, and the client goes to the node it names.
    ///
    /// This is #292 end to end. Reads and writes on this plane must reach the
    /// Raft leader, and a non-leader has always refused them — but the refusal
    /// arrived as the consensus engine's Display text folded into a generic
    /// protocol error,
    /// so no caller could act on it. In Kubernetes every pod pointed its lease
    /// client at its own co-located metadata node, and only the one that
    /// happened to co-locate the leader ever worked: two of three replicas
    /// failed closed permanently, and which one survived depended on an
    /// election.
    ///
    /// Two live endpoints, because the bug only exists between nodes: one
    /// answers as a follower naming node 2, the other serves. The assertion is
    /// that the request SUCCEEDS after being pointed elsewhere, and that the
    /// follower is not asked twice.
    #[tokio::test]
    async fn a_client_follows_a_redirect_to_the_node_the_follower_names() {
        let pki = SharedPki::new();
        let leader_id = MetaNodeId(2);
        let follower = Arc::new(NotLeaderHandler {
            leader: Some(leader_id),
            asked: AtomicUsize::new(0),
        });
        let (follower_addr, follower_task) = live_handler(&pki, Arc::clone(&follower)).await;
        let (leader_addr, leader_task) =
            live_handler(&pki, Arc::new(CountingHandler::default())).await;

        let client = AdminClient::with_candidates(
            pki.client_material(),
            vec![
                AdminCandidate {
                    node_id: Some(MetaNodeId(1)),
                    endpoint: follower_addr,
                    host: None,
                    server_name: "localhost".to_owned(),
                    plaintext: false,
                },
                AdminCandidate {
                    node_id: Some(leader_id),
                    endpoint: leader_addr,
                    host: None,
                    server_name: "localhost".to_owned(),
                    plaintext: false,
                },
            ],
        )
        .unwrap();

        // The follower is FIRST in the list, so this only succeeds by being
        // redirected. Before the fix it returned the follower's refusal.
        let status = client
            .status()
            .await
            .expect("a redirect must be followed, not reported as a failure");
        assert_eq!(status.node_id, MetaNodeId(1));
        assert_eq!(
            follower.asked.load(Ordering::SeqCst),
            1,
            "the follower must be asked exactly once, then abandoned"
        );

        // And the leader is remembered: a second request must not pay for the
        // redirect again, or every call would scan the cluster.
        client.status().await.expect("the second request works too");
        assert_eq!(
            follower.asked.load(Ordering::SeqCst),
            1,
            "the client must remember which node answered as leader"
        );

        follower_task.abort();
        leader_task.abort();
    }

    /// Two followers naming each other must not trap the walk before the real
    /// leader is tried.
    ///
    /// During an election, peer views go stale in both directions: A believes B
    /// leads and B believes A does. Following those hints alternates A→B→A, and
    /// a loop that merely COUNTS attempts spends its whole budget on the pair
    /// while C — configured in the same list, and actually the leader — is never
    /// contacted. The request then fails for want of trying, which is the outage
    /// this whole mechanism exists to prevent.
    ///
    /// So coverage is tracked, not attempts: every candidate is asked at most
    /// once and at least once before giving up.
    #[tokio::test]
    async fn two_followers_naming_each_other_do_not_starve_the_third_candidate() {
        let pki = SharedPki::new();
        let a = Arc::new(NotLeaderHandler {
            leader: Some(MetaNodeId(2)),
            asked: AtomicUsize::new(0),
        });
        let b = Arc::new(NotLeaderHandler {
            leader: Some(MetaNodeId(1)),
            asked: AtomicUsize::new(0),
        });
        let (a_addr, a_task) = live_handler(&pki, Arc::clone(&a)).await;
        let (b_addr, b_task) = live_handler(&pki, Arc::clone(&b)).await;
        let (c_addr, c_task) = live_handler(&pki, Arc::new(CountingHandler::default())).await;

        let client = AdminClient::with_candidates(
            pki.client_material(),
            vec![
                AdminCandidate {
                    node_id: Some(MetaNodeId(1)),
                    endpoint: a_addr,
                    host: None,
                    server_name: "localhost".to_owned(),
                    plaintext: false,
                },
                AdminCandidate {
                    node_id: Some(MetaNodeId(2)),
                    endpoint: b_addr,
                    host: None,
                    server_name: "localhost".to_owned(),
                    plaintext: false,
                },
                AdminCandidate {
                    node_id: Some(MetaNodeId(3)),
                    endpoint: c_addr,
                    host: None,
                    server_name: "localhost".to_owned(),
                    plaintext: false,
                },
            ],
        )
        .unwrap();

        client
            .status()
            .await
            .expect("the third candidate must be reached despite the bouncing pair");

        // Each of the bouncing pair asked exactly once: bounded, and not
        // re-asked just because it was named again.
        assert_eq!(a.asked.load(Ordering::SeqCst), 1, "A must be asked once");
        assert_eq!(b.asked.load(Ordering::SeqCst), 1, "B must be asked once");

        a_task.abort();
        b_task.abort();
        c_task.abort();
    }

    /// A MEMBERSHIP CHANGE through a follower is redirected too.
    ///
    /// `propose` and the linearizable read were classified as redirectable when
    /// the client learned to follow them; `add_learner` and `change_membership`
    /// were not, and still flattened every engine error to a generic protocol
    /// failure. So `vtopctl meta add-learner` against a follower failed outright
    /// while `propose` against the same node was quietly redirected — an
    /// inconsistency invisible from outside, because both look like "the command
    /// failed".
    ///
    /// Membership changes are exactly the commands an operator runs while
    /// something is already wrong, which is the worst time for the endpoint they
    /// happened to name to matter.
    #[tokio::test]
    async fn a_membership_change_is_redirected_like_any_other_write() {
        let pki = SharedPki::new();
        let leader_id = MetaNodeId(2);
        let follower = Arc::new(NotLeaderHandler {
            leader: Some(leader_id),
            asked: AtomicUsize::new(0),
        });
        let (follower_addr, follower_task) = live_handler(&pki, Arc::clone(&follower)).await;
        let leader = Arc::new(CountingHandler::default());
        let (leader_addr, leader_task) = live_handler(&pki, Arc::clone(&leader)).await;

        let client = AdminClient::with_candidates(
            pki.client_material(),
            vec![
                AdminCandidate {
                    node_id: Some(MetaNodeId(1)),
                    endpoint: follower_addr,
                    host: None,
                    server_name: "localhost".to_owned(),
                    plaintext: false,
                },
                AdminCandidate {
                    node_id: Some(leader_id),
                    endpoint: leader_addr,
                    host: None,
                    server_name: "localhost".to_owned(),
                    plaintext: false,
                },
            ],
        )
        .unwrap();

        // The follower is first, so reaching the backend at all means the
        // redirect was followed. `CountingHandler` answers membership RPCs with
        // "handler reached", which is the marker that the request arrived.
        let error = client
            .change_membership(vec![1, 2], false)
            .await
            .expect_err("CountingHandler reports reaching the backend as an error");
        assert!(
            error.to_string().contains("handler reached"),
            "a membership change must reach the leader after a redirect: {error}"
        );
        assert_eq!(leader.membership_changes.load(Ordering::SeqCst), 1);
        assert_eq!(follower.asked.load(Ordering::SeqCst), 1);
        assert_eq!(
            client.redirects_followed(),
            1,
            "and the client must report the redirect it followed"
        );

        follower_task.abort();
        leader_task.abort();
    }

    /// A redirect naming nobody is an ELECTION GAP, not a routing decision.
    ///
    /// `leader: None` means the answering node has no leader either. Treating
    /// it as a redirect to a specific node would send the client chasing one
    /// that does not exist, so it falls back to the others — one of which may
    /// have a newer view.
    #[tokio::test]
    async fn a_redirect_with_no_known_leader_still_tries_the_other_nodes() {
        let pki = SharedPki::new();
        let blind = Arc::new(NotLeaderHandler {
            leader: None,
            asked: AtomicUsize::new(0),
        });
        let (blind_addr, blind_task) = live_handler(&pki, Arc::clone(&blind)).await;
        let (leader_addr, leader_task) =
            live_handler(&pki, Arc::new(CountingHandler::default())).await;

        let client = AdminClient::with_candidates(
            pki.client_material(),
            vec![
                AdminCandidate {
                    node_id: Some(MetaNodeId(1)),
                    endpoint: blind_addr,
                    host: None,
                    server_name: "localhost".to_owned(),
                    plaintext: false,
                },
                AdminCandidate {
                    node_id: Some(MetaNodeId(2)),
                    endpoint: leader_addr,
                    host: None,
                    server_name: "localhost".to_owned(),
                    plaintext: false,
                },
            ],
        )
        .unwrap();

        client
            .status()
            .await
            .expect("an unknown leader must not stop the client trying the rest");
        assert_eq!(blind.asked.load(Ordering::SeqCst), 1);

        blind_task.abort();
        leader_task.abort();
    }

    /// When NO node leads, the client reports THAT — not a connection error
    /// from whichever one it happened to try last.
    #[tokio::test]
    async fn all_nodes_refusing_reports_the_redirect_rather_than_the_last_hop() {
        let pki = SharedPki::new();
        let first = Arc::new(NotLeaderHandler {
            leader: None,
            asked: AtomicUsize::new(0),
        });
        let second = Arc::new(NotLeaderHandler {
            leader: None,
            asked: AtomicUsize::new(0),
        });
        let (first_addr, t1) = live_handler(&pki, Arc::clone(&first)).await;
        let (second_addr, t2) = live_handler(&pki, Arc::clone(&second)).await;

        let client = AdminClient::with_candidates(
            pki.client_material(),
            vec![
                AdminCandidate {
                    node_id: Some(MetaNodeId(1)),
                    endpoint: first_addr,
                    host: None,
                    server_name: "localhost".to_owned(),
                    plaintext: false,
                },
                AdminCandidate {
                    node_id: Some(MetaNodeId(2)),
                    endpoint: second_addr,
                    host: None,
                    server_name: "localhost".to_owned(),
                    plaintext: false,
                },
            ],
        )
        .unwrap();

        let error = client.status().await.expect_err("nobody leads");
        assert!(
            matches!(error, TransportError::NotLeader { .. }),
            "a cluster with no leader must say so: {error}"
        );
        assert!(
            error.to_string().contains("knows of no leader"),
            "and say it in a way an operator can act on: {error}"
        );
        // Every candidate tried exactly once: bounded, so a cluster
        // mid-election cannot turn one request into an unbounded chase.
        assert_eq!(first.asked.load(Ordering::SeqCst), 1);
        assert_eq!(second.asked.load(Ordering::SeqCst), 1);

        t1.abort();
        t2.abort();
    }

    /// A metadata node that does not lead: every request is refused with a
    /// redirect, exactly as the consensus façade now does when the engine
    /// reports a forward-to-leader.
    struct NotLeaderHandler {
        leader: Option<MetaNodeId>,
        asked: AtomicUsize,
    }

    impl NotLeaderHandler {
        fn refuse<T>(&self) -> TransportResult<T> {
            self.asked.fetch_add(1, Ordering::SeqCst);
            Err(TransportError::NotLeader {
                message: match self.leader {
                    Some(id) => format!("not the metadata leader; ask node {id}"),
                    None => "not the metadata leader; no leader is known yet".to_owned(),
                },
                leader: self.leader,
            })
        }
    }

    #[async_trait]
    impl AdminHandler for NotLeaderHandler {
        async fn status(&self) -> TransportResult<AdminStatusResponse> {
            self.refuse()
        }
        async fn read_range_transitions(
            &self,
            _: AdminReadRangeTransitionsRequest,
        ) -> TransportResult<AdminReadRangeTransitionsResponse> {
            self.refuse()
        }
        async fn read_segment_placement(
            &self,
            _: AdminReadSegmentPlacementRequest,
        ) -> TransportResult<AdminReadSegmentPlacementResponse> {
            self.refuse()
        }
        async fn propose(&self, _: MetadataCommand) -> TransportResult<AdminProposeResponse> {
            self.refuse()
        }
        async fn init(&self, _: Vec<u64>) -> TransportResult<AdminMembershipResponse> {
            self.refuse()
        }
        async fn add_learner(&self, _: u64) -> TransportResult<AdminMembershipResponse> {
            self.refuse()
        }
        async fn change_membership(
            &self,
            _: Vec<u64>,
            _: bool,
        ) -> TransportResult<AdminMembershipResponse> {
            self.refuse()
        }
        async fn read_range_lease(
            &self,
            _: AdminReadRangeLeaseRequest,
        ) -> TransportResult<AdminReadRangeLeaseResponse> {
            self.refuse()
        }
    }

    /// One CA-less PKI shared by every endpoint in a test.
    ///
    /// `mint_leaf` generates a fresh self-signed leaf per call, so two
    /// endpoints minted separately present different certificates and one
    /// client cannot trust both. The redirect tests need exactly that — two
    /// endpoints, one client — so the DER is minted once and materials are
    /// rebuilt from it per endpoint (`PrivateKeyDer` is not `Clone`).
    struct SharedPki {
        server_leaf: rustls::pki_types::CertificateDer<'static>,
        server_key: Vec<u8>,
        client_leaf: rustls::pki_types::CertificateDer<'static>,
        client_key: Vec<u8>,
    }

    impl SharedPki {
        fn new() -> Self {
            let (server_leaf, server_key) = mint_leaf("localhost");
            let (client_leaf, client_key) = mint_leaf("operator");
            Self {
                server_leaf,
                server_key: server_key.secret_pkcs8_der().to_vec(),
                client_leaf,
                client_key: client_key.secret_pkcs8_der().to_vec(),
            }
        }

        fn server_material(&self) -> TlsMaterial {
            let mut roots = RootCertStore::empty();
            roots.add(self.client_leaf.clone()).unwrap();
            TlsMaterial {
                certificate_chain: vec![self.server_leaf.clone()],
                private_key: PrivatePkcs8KeyDer::from(self.server_key.clone()).into(),
                trust_roots: roots,
            }
        }

        fn client_material(&self) -> TlsMaterial {
            let mut roots = RootCertStore::empty();
            roots.add(self.server_leaf.clone()).unwrap();
            TlsMaterial {
                certificate_chain: vec![self.client_leaf.clone()],
                private_key: PrivatePkcs8KeyDer::from(self.client_key.clone()).into(),
                trust_roots: roots,
            }
        }
    }

    /// Stand up an admin endpoint over a caller-supplied handler, returning the
    /// CONCRETE handler so a test can read its counters.
    async fn live_handler<H>(
        pki: &SharedPki,
        handler: Arc<H>,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>)
    where
        H: AdminHandler + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = AdminServer::new(
            pki.server_material(),
            Arc::clone(&handler) as Arc<dyn AdminHandler>,
        )
        .unwrap();
        let task = tokio::spawn(async move {
            let _ = server.serve(listener).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (addr, task)
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

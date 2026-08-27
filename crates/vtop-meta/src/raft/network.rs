//! Openraft network adapter over the VTPM mTLS peer transport.
//!
//! Converts openraft RPC types ↔ VTOP peer wire messages field-by-field, then
//! exchanges them over short-lived mTLS connections. Peer addresses come from
//! an explicit directory because [`openraft::EmptyNode`] carries no address.
//!
//! The directory holds the NAME a peer was configured under, not only the
//! address that name resolved to. A `SocketAddr` is where a peer was the one
//! time anybody looked; under an orchestrator that rebuilds a member with a
//! new address — which is every orchestrator — that snapshot is wrong from the
//! first replacement onward, and a directory written once at startup never
//! finds out. So a failed RPC re-resolves the name (#367).
//!
//! The cost of NOT doing that is not a slow peer, it is a permanently
//! disrupted group. The break is one-way — the returning member resolved its
//! neighbours after they existed, so it can reach them and they cannot reach
//! it — and openraft 0.9 implements no pre-vote, so the isolated member burns
//! a real term on every election timeout while the leader-lease check keeps
//! the healthy majority from ever adopting those terms. The group stays up
//! and the member stays out, indefinitely, until something restarts it.

#![allow(clippy::result_large_err)]

use crate::keys::MetaNodeId;
use crate::raft::convert::{
    entry_to_meta, hard_state_to_vote, membership_to_meta, meta_to_entry, meta_to_membership,
    to_meta_index, to_raft_index, vote_to_hard_state,
};
use crate::raft::type_config::{MetaRaftTypeConfig, NodeId};
use crate::storage::hardstate::HardState;
use crate::transport::peer::{PeerClient, PeerRpcHandler};
use crate::transport::tls::TlsMaterial;
use crate::transport::wire::{
    PeerAppendRequest, PeerAppendResponse, PeerInstallRequest, PeerInstallResponse,
    PeerVoteRequest, PeerVoteResponse, TransportError, TransportResult, WireLogId,
};
use async_trait::async_trait;
use openraft::error::{RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{EmptyNode, Raft, SnapshotMeta, StoredMembership, Vote};
use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

type MemRaft = Raft<MetaRaftTypeConfig>;

/// How often one peer's name may be re-resolved.
///
/// Replication heartbeats fire every `heartbeat_interval` (60 ms by default),
/// and a peer that is genuinely down fails every one of them. Re-resolving on
/// each would put a name lookup on that same cadence for no benefit, so the
/// floor is a fraction of the shortest election timeout (300 ms): a peer that
/// really did move is found well inside one election, and a peer that is
/// simply down is asked about five times a second rather than seventeen.
const RERESOLVE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// How long one name lookup may take before it is abandoned.
///
/// A resolver that stalls must not become a stall in the RPC that asked
/// (review): replication's hard deadline is the heartbeat interval, 60 ms, and
/// this runs on its failure path. Generous next to that and still finite —
/// the point is that it ends, not that it is fast.
const RESOLVE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

/// Peer address directory keyed by Raft node id.
#[derive(Clone, Debug, Default)]
pub struct PeerDirectory {
    peers: Arc<Mutex<BTreeMap<NodeId, Peer>>>,
}

/// One directory entry: where a peer is, and how to find out again.
#[derive(Clone, Debug)]
struct Peer {
    /// The `host:port` this peer was configured under, when it was a name
    /// rather than a literal address. `None` for peers inserted as an
    /// already-resolved address — a test harness, or a static deployment —
    /// which are therefore never re-resolved, because there is nothing to
    /// re-resolve them from.
    host: Option<String>,
    server_name: String,
    /// Where the peer was, last time anybody looked. `None` when the name has
    /// never resolved: a member configured before its pod exists is not a
    /// configuration error, it is a peer that is not there YET, and a node
    /// that refuses to start over one cannot be part of a group that starts
    /// together (#367).
    addr: Option<SocketAddr>,
    resolved_at: Option<std::time::Instant>,
}

#[derive(Clone, Debug)]
pub struct PeerEndpoint {
    pub addr: SocketAddr,
    /// rustls server name (SNI / cert name check), usually `localhost` in tests.
    pub server_name: String,
}

impl PeerDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a peer at a known address, with no name to re-resolve from.
    pub fn insert(&self, id: NodeId, endpoint: PeerEndpoint) {
        self.peers.lock().expect("peer directory").insert(
            id,
            Peer {
                host: None,
                server_name: endpoint.server_name,
                addr: Some(endpoint.addr),
                resolved_at: None,
            },
        );
    }

    /// Insert a peer by NAME, to be resolved now and again whenever an RPC to
    /// it fails.
    ///
    /// Resolution failure here is not an error. The name of a member that has
    /// not been created yet does not resolve, and every member of a group that
    /// boots together is in that position for some of its neighbours; the peer
    /// simply reads as unreachable until the name answers.
    pub async fn insert_by_name(&self, id: NodeId, host: String, server_name: String) {
        self.peers.lock().expect("peer directory").insert(
            id,
            Peer {
                host: Some(host),
                server_name,
                addr: None,
                resolved_at: None,
            },
        );
        self.re_resolve(id).await;
    }

    pub fn get(&self, id: NodeId) -> Option<PeerEndpoint> {
        let peers = self.peers.lock().expect("peer directory");
        let peer = peers.get(&id)?;
        Some(PeerEndpoint {
            addr: peer.addr?,
            server_name: peer.server_name.clone(),
        })
    }

    /// Look this peer's name up again, throttled by [`RERESOLVE_INTERVAL`].
    ///
    /// Called after a failed RPC, never on the success path: the address that
    /// just worked does not need checking, and this must not sit on the
    /// heartbeat path.
    pub async fn re_resolve(&self, id: NodeId) {
        // The lock is taken twice on purpose and is never held across the
        // lookup: a name that does not answer takes as long as the resolver
        // takes, and every other peer's RPCs would queue behind it.
        //
        // THE STAMP IS CLAIMED BEFORE THE AWAIT, not written after it
        // (review). Replication drives one of these per failing RPC and they
        // run concurrently, so a throttle that only takes effect once a lookup
        // RETURNS is no throttle at all against exactly the case it is for: a
        // resolver that has stopped answering, where every caller would start
        // its own query and none would finish.
        let host = {
            let mut peers = self.peers.lock().expect("peer directory");
            let Some(peer) = peers.get_mut(&id) else {
                return;
            };
            // Nothing to re-resolve from: this peer was given as a literal
            // address, which is exactly as current as it ever was.
            let Some(host) = peer.host.clone() else {
                return;
            };
            if peer
                .resolved_at
                .is_some_and(|at| at.elapsed() < RERESOLVE_INTERVAL)
            {
                return;
            }
            peer.resolved_at = Some(std::time::Instant::now());
            host
        };
        // BOUNDED, because a resolver can stall and this is on the path of an
        // RPC that has its own hard deadline — 60 ms for replication (review).
        // A lookup that outlives the bound is abandoned, not waited for; the
        // next failure will ask again.
        let resolved = match tokio::time::timeout(RESOLVE_TIMEOUT, tokio::net::lookup_host(&host))
            .await
        {
            Ok(Ok(mut addrs)) => addrs.next(),
            Ok(Err(error)) => {
                // SAID, because a misspelt peer and a transient DNS race look
                // identical from the outside and only one of them is worth
                // paging someone about (review).
                eprintln!("could not resolve metadata peer {id} at {host:?}: {error}");
                None
            }
            Err(_) => {
                eprintln!("resolving metadata peer {id} at {host:?} exceeded {RESOLVE_TIMEOUT:?}");
                None
            }
        };
        let mut peers = self.peers.lock().expect("peer directory");
        let Some(peer) = peers.get_mut(&id) else {
            return;
        };
        // A lookup that failed leaves the last known address standing. It may
        // still be right — a resolver hiccup is not evidence a peer moved —
        // and replacing it with nothing would turn a transient DNS failure
        // into an unreachable member.
        if let Some(addr) = resolved {
            peer.addr = Some(addr);
        }
    }
}

/// Factory that builds one [`TlsRaftNetwork`] client per target.
pub struct TlsRaftNetworkFactory {
    directory: PeerDirectory,
    /// Template material; each client clones trust roots + presents this identity.
    material: Arc<TlsMaterialOwned>,
    source: NodeId,
}

/// Cloneable TLS material (rustls private keys use `clone_key`).
struct TlsMaterialOwned {
    certificate_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
    private_key: rustls::pki_types::PrivateKeyDer<'static>,
    trust_roots: rustls::RootCertStore,
}

impl TlsMaterialOwned {
    fn from_material(material: TlsMaterial) -> Self {
        Self {
            certificate_chain: material.certificate_chain,
            private_key: material.private_key,
            trust_roots: material.trust_roots,
        }
    }

    fn to_material(&self) -> TlsMaterial {
        TlsMaterial {
            certificate_chain: self.certificate_chain.clone(),
            private_key: self.private_key.clone_key(),
            trust_roots: self.trust_roots.clone(),
        }
    }
}

impl TlsRaftNetworkFactory {
    pub fn new(source: NodeId, directory: PeerDirectory, material: TlsMaterial) -> Self {
        Self {
            directory,
            material: Arc::new(TlsMaterialOwned::from_material(material)),
            source,
        }
    }
}

impl RaftNetworkFactory<MetaRaftTypeConfig> for TlsRaftNetworkFactory {
    type Network = TlsRaftNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &EmptyNode) -> Self::Network {
        TlsRaftNetwork {
            directory: self.directory.clone(),
            material: Arc::clone(&self.material),
            source: self.source,
            target,
        }
    }
}

pub struct TlsRaftNetwork {
    directory: PeerDirectory,
    material: Arc<TlsMaterialOwned>,
    source: NodeId,
    target: NodeId,
}

impl TlsRaftNetwork {
    fn unreachable(
        &self,
        reason: impl std::fmt::Display,
    ) -> RPCError<NodeId, EmptyNode, RaftError<NodeId>> {
        RPCError::Unreachable(Unreachable::new(&io::Error::other(format!(
            "meta peer {} -> {}: {reason}",
            self.source, self.target
        ))))
    }

    /// A client for this target, re-resolving first if we have no address.
    ///
    /// The no-address case is a peer whose name had not resolved when the
    /// directory was built — a member configured before its pod existed. It
    /// is not a permanent condition and must not read as one (#367).
    async fn client_or_re_resolve(
        &self,
    ) -> Result<(PeerClient, SocketAddr), RPCError<NodeId, EmptyNode, RaftError<NodeId>>> {
        if self.directory.get(self.target).is_none() {
            self.directory.re_resolve(self.target).await;
        }
        self.client()
    }

    /// Report a failed RPC AND look the peer's name up again.
    ///
    /// The two belong together. Every transport failure is a candidate for
    /// "this peer is no longer where we think it is", and the address is only
    /// ever wrong on this path — the success path proves it right. Ordered
    /// so the lookup happens before the error is returned, because openraft
    /// retries on its own schedule and the next attempt should already have
    /// the new address.
    fn unreachable_and_re_resolve(
        &self,
        error: impl std::fmt::Display,
    ) -> RPCError<NodeId, EmptyNode, RaftError<NodeId>> {
        // DETACHED, because this RPC has a hard deadline and the lookup does
        // not belong inside it (review). Awaiting here added the resolver's
        // latency AFTER the deadline had already expired, delaying the
        // failure's return — and with a 60 ms heartbeat against a 300 ms
        // minimum election timeout, delaying a heartbeat is how a live peer
        // is pushed into campaigning. The refresh is maintenance; the RPC's
        // job is to return.
        //
        // Safe to detach: `re_resolve` throttles itself, so a burst of
        // failures cannot become a burst of queries, and the next attempt
        // uses whatever it has by then.
        let directory = self.directory.clone();
        let target = self.target;
        tokio::spawn(async move { directory.re_resolve(target).await });
        self.unreachable(error)
    }

    fn client(
        &self,
    ) -> Result<(PeerClient, SocketAddr), RPCError<NodeId, EmptyNode, RaftError<NodeId>>> {
        let endpoint = self
            .directory
            .get(self.target)
            .ok_or_else(|| self.unreachable("no peer address in directory"))?;
        let client = PeerClient::new(
            self.material.to_material(),
            endpoint.server_name.clone(),
            MetaNodeId(self.target),
        )
        .map_err(|error| self.unreachable(error))?;
        Ok((client, endpoint.addr))
    }
}

impl RaftNetwork<MetaRaftTypeConfig> for TlsRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MetaRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, EmptyNode, RaftError<NodeId>>> {
        let (client, addr) = self.client_or_re_resolve().await?;
        let request = append_to_wire(&rpc).map_err(|e| self.unreachable(e))?;
        let response =
            match with_rpc_deadline(option.hard_ttl(), client.append(addr, &request)).await {
                Ok(response) => response,
                Err(error) => return Err(self.unreachable_and_re_resolve(error)),
            };
        append_from_wire(response).map_err(|e| self.unreachable(e))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, EmptyNode, RaftError<NodeId>>> {
        let (client, addr) = self.client_or_re_resolve().await?;
        let request = vote_req_to_wire(&rpc).map_err(|e| self.unreachable(e))?;
        let response = match with_rpc_deadline(option.hard_ttl(), client.vote(addr, &request)).await
        {
            Ok(response) => response,
            Err(error) => return Err(self.unreachable_and_re_resolve(error)),
        };
        vote_resp_from_wire(response).map_err(|e| self.unreachable(e))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MetaRaftTypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, EmptyNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        let (client, addr) = self.client_or_re_resolve().await.map_err(|e| match e {
            RPCError::Unreachable(u) => RPCError::Unreachable(u),
            other => install_unreachable(other),
        })?;
        let request = install_to_wire(&rpc).map_err(install_unreachable)?;
        let response =
            match with_rpc_deadline(option.hard_ttl(), client.install(addr, &request)).await {
                Ok(response) => response,
                Err(error) => {
                    self.directory.re_resolve(self.target).await;
                    return Err(install_unreachable(error));
                }
            };
        install_from_wire(response).map_err(install_unreachable)
    }
}

fn install_unreachable(
    error: impl std::fmt::Display,
) -> RPCError<NodeId, EmptyNode, RaftError<NodeId, openraft::error::InstallSnapshotError>> {
    RPCError::Unreachable(Unreachable::new(&io::Error::other(error.to_string())))
}

async fn with_rpc_deadline<T, E, F>(
    deadline: std::time::Duration,
    fut: F,
) -> Result<T, TransportError>
where
    F: std::future::Future<Output = Result<T, E>>,
    E: Into<TransportError>,
{
    match tokio::time::timeout(deadline, fut).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(error.into()),
        Err(_) => Err(TransportError::Protocol(format!(
            "peer RPC exceeded hard deadline ({deadline:?})"
        ))),
    }
}

/// PeerRpcHandler that forwards into a live [`Raft`] handle.
pub struct RaftPeerHandler {
    raft: MemRaft,
}

impl RaftPeerHandler {
    pub fn new(raft: MemRaft) -> Self {
        Self { raft }
    }
}

#[async_trait]
impl PeerRpcHandler for RaftPeerHandler {
    async fn handle_vote(&self, request: PeerVoteRequest) -> TransportResult<PeerVoteResponse> {
        let rpc = vote_req_from_wire(request)?;
        let response = self
            .raft
            .vote(rpc)
            .await
            .map_err(|error| TransportError::Protocol(error.to_string()))?;
        vote_resp_to_wire(response)
    }

    async fn handle_append(
        &self,
        request: PeerAppendRequest,
    ) -> TransportResult<PeerAppendResponse> {
        let rpc = append_from_peer(request)?;
        let response = self
            .raft
            .append_entries(rpc)
            .await
            .map_err(|error| TransportError::Protocol(error.to_string()))?;
        append_to_peer(response)
    }

    async fn handle_install(
        &self,
        request: PeerInstallRequest,
    ) -> TransportResult<PeerInstallResponse> {
        let rpc = install_from_peer(request)?;
        let response = self
            .raft
            .install_snapshot(rpc)
            .await
            .map_err(|error| TransportError::Protocol(error.to_string()))?;
        install_to_peer(response)
    }
}

// ---------------------------------------------------------------------------
// Field-by-field converts (openraft ↔ wire)
// ---------------------------------------------------------------------------

fn wire_log_id(log_id: Option<openraft::LogId<NodeId>>) -> Option<WireLogId> {
    log_id.map(|id| WireLogId {
        term: id.leader_id.term,
        index: to_meta_index(id.index),
    })
}

fn raft_log_id(id: Option<WireLogId>) -> TransportResult<Option<openraft::LogId<NodeId>>> {
    match id {
        None => Ok(None),
        Some(WireLogId { term, index }) => {
            let raft_index = to_raft_index(index).ok_or_else(|| {
                TransportError::Protocol(format!("wire log index {index} is below openraft offset"))
            })?;
            Ok(Some(crate::raft::convert::raft_log_id(term, raft_index)))
        }
    }
}

fn vote_to_wire(vote: &Vote<NodeId>) -> HardState {
    vote_to_hard_state(vote)
}

fn vote_from_wire(state: HardState) -> TransportResult<Vote<NodeId>> {
    hard_state_to_vote(&state).ok_or_else(|| {
        TransportError::Protocol("empty hard state cannot form an openraft Vote".to_owned())
    })
}

fn vote_req_to_wire(rpc: &VoteRequest<NodeId>) -> TransportResult<PeerVoteRequest> {
    Ok(PeerVoteRequest {
        vote: vote_to_wire(&rpc.vote),
        last_log_id: wire_log_id(rpc.last_log_id),
    })
}

fn vote_req_from_wire(request: PeerVoteRequest) -> TransportResult<VoteRequest<NodeId>> {
    Ok(VoteRequest {
        vote: vote_from_wire(request.vote)?,
        last_log_id: raft_log_id(request.last_log_id)?,
    })
}

fn vote_resp_to_wire(response: VoteResponse<NodeId>) -> TransportResult<PeerVoteResponse> {
    Ok(PeerVoteResponse {
        vote: vote_to_wire(&response.vote),
        vote_granted: response.vote_granted,
        last_log_id: wire_log_id(response.last_log_id),
    })
}

fn vote_resp_from_wire(response: PeerVoteResponse) -> TransportResult<VoteResponse<NodeId>> {
    Ok(VoteResponse {
        vote: vote_from_wire(response.vote)?,
        vote_granted: response.vote_granted,
        last_log_id: raft_log_id(response.last_log_id)?,
    })
}

fn append_to_wire(
    rpc: &AppendEntriesRequest<MetaRaftTypeConfig>,
) -> TransportResult<PeerAppendRequest> {
    let mut entries = Vec::with_capacity(rpc.entries.len());
    for entry in &rpc.entries {
        entries.push(entry_to_meta(entry).map_err(|e| TransportError::Protocol(e.to_string()))?);
    }
    Ok(PeerAppendRequest {
        vote: vote_to_wire(&rpc.vote),
        prev_log_id: wire_log_id(rpc.prev_log_id),
        entries,
        leader_commit: wire_log_id(rpc.leader_commit),
    })
}

fn append_from_peer(
    request: PeerAppendRequest,
) -> TransportResult<AppendEntriesRequest<MetaRaftTypeConfig>> {
    let mut entries = Vec::with_capacity(request.entries.len());
    for entry in request.entries {
        entries.push(meta_to_entry(entry).map_err(|e| TransportError::Protocol(e.to_string()))?);
    }
    Ok(AppendEntriesRequest {
        vote: vote_from_wire(request.vote)?,
        prev_log_id: raft_log_id(request.prev_log_id)?,
        entries,
        leader_commit: raft_log_id(request.leader_commit)?,
    })
}

fn append_to_peer(response: AppendEntriesResponse<NodeId>) -> TransportResult<PeerAppendResponse> {
    Ok(match response {
        AppendEntriesResponse::Success => PeerAppendResponse::Success,
        AppendEntriesResponse::PartialSuccess(id) => {
            PeerAppendResponse::PartialSuccess(wire_log_id(id))
        }
        AppendEntriesResponse::Conflict => PeerAppendResponse::Conflict,
        AppendEntriesResponse::HigherVote(vote) => {
            PeerAppendResponse::HigherVote(vote_to_wire(&vote))
        }
    })
}

fn append_from_wire(
    response: PeerAppendResponse,
) -> TransportResult<AppendEntriesResponse<NodeId>> {
    Ok(match response {
        PeerAppendResponse::Success => AppendEntriesResponse::Success,
        PeerAppendResponse::PartialSuccess(id) => {
            AppendEntriesResponse::PartialSuccess(raft_log_id(id)?)
        }
        PeerAppendResponse::Conflict => AppendEntriesResponse::Conflict,
        PeerAppendResponse::HigherVote(vote) => {
            AppendEntriesResponse::HigherVote(vote_from_wire(vote)?)
        }
    })
}

fn install_to_wire(
    rpc: &InstallSnapshotRequest<MetaRaftTypeConfig>,
) -> TransportResult<PeerInstallRequest> {
    let last_membership = membership_to_meta(rpc.meta.last_membership.membership())
        .map_err(|e| TransportError::Protocol(e.to_string()))?;
    Ok(PeerInstallRequest {
        vote: vote_to_wire(&rpc.vote),
        last_log_id: wire_log_id(rpc.meta.last_log_id),
        membership_log_id: wire_log_id(*rpc.meta.last_membership.log_id()),
        last_membership,
        snapshot_id: rpc.meta.snapshot_id.clone(),
        offset: rpc.offset,
        data: rpc.data.clone(),
        done: rpc.done,
    })
}

fn install_from_peer(
    request: PeerInstallRequest,
) -> TransportResult<InstallSnapshotRequest<MetaRaftTypeConfig>> {
    let membership = meta_to_membership(&request.last_membership)
        .map_err(|e| TransportError::Protocol(e.to_string()))?;
    let last_log_id = raft_log_id(request.last_log_id)?;
    let membership_log_id = raft_log_id(request.membership_log_id)?;
    Ok(InstallSnapshotRequest {
        vote: vote_from_wire(request.vote)?,
        meta: SnapshotMeta {
            last_log_id,
            last_membership: StoredMembership::new(membership_log_id, membership),
            snapshot_id: request.snapshot_id,
        },
        offset: request.offset,
        data: request.data,
        done: request.done,
    })
}

fn install_to_peer(
    response: InstallSnapshotResponse<NodeId>,
) -> TransportResult<PeerInstallResponse> {
    Ok(PeerInstallResponse {
        vote: vote_to_wire(&response.vote),
    })
}

fn install_from_wire(
    response: PeerInstallResponse,
) -> TransportResult<InstallSnapshotResponse<NodeId>> {
    Ok(InstallSnapshotResponse {
        vote: vote_from_wire(response.vote)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    /// A member configured before it exists is a member that is not there
    /// YET, and must not stop this node from starting (#367).
    ///
    /// Resolution used to happen once, at boot, and a failure was fatal. In a
    /// group whose members are created in parallel that is a startup race:
    /// whoever wins it resolves nobody and exits, and the orchestrator
    /// restarts it until its neighbours happen to have addresses. The pod
    /// events read as a crash-looping node; the cause was a name that had not
    /// been published yet.
    #[tokio::test]
    async fn a_peer_whose_name_does_not_resolve_yet_is_absent_rather_than_fatal() {
        let directory = PeerDirectory::new();
        // A lookup that yields no address, expressed so it yields none
        // ANYWHERE: the name is missing its port, which fails before a
        // resolver is consulted. A name chosen to be absent from DNS is not
        // portable — plenty of resolvers answer for everything, including one
        // this was first written on — and a test that depends on the host's
        // search domains is a test that reports the host.
        directory
            .insert_by_name(
                2,
                "vtop-1.vtop-headless.absent.svc".to_owned(),
                "vtop-1".to_owned(),
            )
            .await;
        assert!(
            directory.get(2).is_none(),
            "a name that does not answer must read as a peer we cannot reach, which \
             the RPC path already knows how to say"
        );
        // And the entry is still THERE, so the next failed RPC re-resolves it
        // rather than finding nothing to look up.
        assert!(
            directory.peers.lock().expect("peers").contains_key(&2),
            "the peer must stay in the directory, or it can never be found later"
        );
    }

    /// A stale address is replaced by whatever the name says now.
    ///
    /// This is the failure itself: a member's pod is replaced, it returns at a
    /// new address, and every surviving member keeps dialling the old one for
    /// the life of its process.
    ///
    /// The "name" here is a literal so the test asserts the re-resolution
    /// machinery rather than the host's resolver — using a real DNS name would
    /// pin this test to the machine it runs on.
    #[tokio::test]
    async fn a_failed_rpc_re_resolves_a_peer_that_moved() {
        let directory = PeerDirectory::new();
        directory
            .insert_by_name(2, "127.0.0.1:9300".to_owned(), "vtop-1".to_owned())
            .await;
        assert_eq!(directory.get(2).expect("resolved").addr, endpoint(9300));

        // The address the peer used to be at, and the throttle wound back so
        // this stands for a lookup a moment later rather than the same one.
        {
            let mut peers = directory.peers.lock().expect("peers");
            let peer = peers.get_mut(&2).expect("peer");
            peer.addr = Some(endpoint(1));
            peer.resolved_at = None;
        }
        directory.re_resolve(2).await;
        assert_eq!(
            directory.get(2).expect("resolved").addr,
            endpoint(9300),
            "the address must come back from the NAME; a directory that trusts what \
             it resolved once can never find a peer that moved"
        );
    }

    /// Re-resolution is throttled, because the thing that triggers it fires
    /// every heartbeat.
    ///
    /// A peer that is genuinely down fails every replication RPC, and each
    /// failure asks for a lookup. Without a floor that is a name query on the
    /// heartbeat interval, forever, for a peer that has not moved.
    #[tokio::test]
    async fn re_resolution_is_throttled_so_a_down_peer_is_not_a_name_query_storm() {
        let directory = PeerDirectory::new();
        directory
            .insert_by_name(2, "127.0.0.1:9300".to_owned(), "vtop-1".to_owned())
            .await;
        {
            let mut peers = directory.peers.lock().expect("peers");
            let peer = peers.get_mut(&2).expect("peer");
            peer.addr = Some(endpoint(1));
            // Resolved just now: inside the floor.
            peer.resolved_at = Some(std::time::Instant::now());
        }
        directory.re_resolve(2).await;
        assert_eq!(
            directory.get(2).expect("resolved").addr,
            endpoint(1),
            "a lookup inside the floor must not have happened at all"
        );

        {
            let mut peers = directory.peers.lock().expect("peers");
            peers.get_mut(&2).expect("peer").resolved_at =
                Some(std::time::Instant::now() - RERESOLVE_INTERVAL * 2);
        }
        directory.re_resolve(2).await;
        assert_eq!(
            directory.get(2).expect("resolved").addr,
            endpoint(9300),
            "and once the floor has passed the peer must be looked up again"
        );
    }

    /// A peer given as an address has no name to be looked up from, and asking
    /// must be a no-op rather than an erasure.
    ///
    /// The deterministic harnesses insert peers this way. Treating a missing
    /// name as "resolve to nothing" would empty their directory on the first
    /// RPC that failed for any other reason.
    #[tokio::test]
    async fn a_peer_given_as_a_literal_address_is_left_exactly_as_it_was() {
        let directory = PeerDirectory::new();
        directory.insert(
            2,
            PeerEndpoint {
                addr: endpoint(9300),
                server_name: "vtop-1".to_owned(),
            },
        );
        directory.re_resolve(2).await;
        assert_eq!(
            directory.get(2).expect("still there").addr,
            endpoint(9300),
            "there is nothing to re-resolve from, so there is nothing to change"
        );
    }

    /// A resolver that fails leaves the last known address standing.
    ///
    /// A hiccup in name resolution is not evidence that a peer moved, and
    /// replacing a working address with nothing would turn it into an
    /// unreachable member for as long as the hiccup lasted.
    #[tokio::test]
    async fn a_failed_lookup_does_not_erase_the_address_that_was_working() {
        let directory = PeerDirectory::new();
        directory
            .insert_by_name(2, "127.0.0.1:9300".to_owned(), "vtop-1".to_owned())
            .await;
        {
            let mut peers = directory.peers.lock().expect("peers");
            let peer = peers.get_mut(&2).expect("peer");
            // Port-less again, for the same portability reason as above.
            peer.host = Some("vtop-1.absent.svc".to_owned());
            peer.resolved_at = None;
        }
        directory.re_resolve(2).await;
        assert_eq!(
            directory.get(2).expect("still there").addr,
            endpoint(9300),
            "a name that stopped answering must not cost us the address that did"
        );
    }
}

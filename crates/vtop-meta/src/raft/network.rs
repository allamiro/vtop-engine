//! Openraft network adapter over the VTPM mTLS peer transport.
//!
//! Converts openraft RPC types ↔ VTOP peer wire messages field-by-field, then
//! exchanges them over short-lived mTLS connections. Peer addresses come from
//! an explicit directory because [`openraft::EmptyNode`] carries no address.

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

/// Peer address directory keyed by Raft node id.
#[derive(Clone, Debug, Default)]
pub struct PeerDirectory {
    peers: Arc<Mutex<BTreeMap<NodeId, PeerEndpoint>>>,
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

    pub fn insert(&self, id: NodeId, endpoint: PeerEndpoint) {
        self.peers
            .lock()
            .expect("peer directory")
            .insert(id, endpoint);
    }

    pub fn get(&self, id: NodeId) -> Option<PeerEndpoint> {
        self.peers.lock().expect("peer directory").get(&id).cloned()
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
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, EmptyNode, RaftError<NodeId>>> {
        let (client, addr) = self.client()?;
        let request = append_to_wire(&rpc).map_err(|e| self.unreachable(e))?;
        let response = client
            .append(addr, &request)
            .await
            .map_err(|e| self.unreachable(e))?;
        append_from_wire(response).map_err(|e| self.unreachable(e))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, EmptyNode, RaftError<NodeId>>> {
        let (client, addr) = self.client()?;
        let request = vote_req_to_wire(&rpc).map_err(|e| self.unreachable(e))?;
        let response = client
            .vote(addr, &request)
            .await
            .map_err(|e| self.unreachable(e))?;
        vote_resp_from_wire(response).map_err(|e| self.unreachable(e))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MetaRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, EmptyNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        let (client, addr) = self.client().map_err(|e| match e {
            RPCError::Unreachable(u) => RPCError::Unreachable(u),
            other => RPCError::Unreachable(Unreachable::new(&io::Error::other(format!("{other}")))),
        })?;
        let request = install_to_wire(&rpc).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&io::Error::other(e.to_string())))
        })?;
        let response = client.install(addr, &request).await.map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&io::Error::other(e.to_string())))
        })?;
        install_from_wire(response)
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&io::Error::other(e.to_string()))))
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
    let log_id_for_membership = last_log_id;
    Ok(InstallSnapshotRequest {
        vote: vote_from_wire(request.vote)?,
        meta: SnapshotMeta {
            last_log_id,
            last_membership: StoredMembership::new(log_id_for_membership, membership),
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

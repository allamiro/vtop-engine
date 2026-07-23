//! Peer/admin mTLS transport for the metadata control plane — stage-5 PR 3.
//!
//! # Containment
//!
//! This module tree has **no** consensus-crate imports. Wire codecs speak VTOP
//! types ([`crate::HardState`], [`crate::MetaLogEntry`], [`crate::MetaMembership`],
//! [`crate::MetadataCommand`]). The [`crate::raft::network`] adapter translates
//! at the boundary so consensus crate types never cross the VTPM wire or leave
//! `raft/`.
//!
//! # VTOP encoding
//!
//! Frames use magic `VTPM`, version 1, bounded payloads, and a BLAKE3 checksum
//! over the header prefix plus payload. Log indexes are meta indexes
//! (`raft_index + 1`), matching durable store coordinates.
//!
//! # Determinism
//!
//! Codecs and identity parsing are deterministic. Live TCP/TLS I/O is not:
//! accept scheduling, record boundaries, and timeouts depend on the OS and
//! wall clock. Loopback tests exercise the path but do not claim seed-level
//! reproducibility.

pub mod admin;
pub mod peer;
pub mod tls;
pub mod wire;

pub use admin::{stub_status, AdminClient, AdminHandler, AdminServer};
pub use peer::{PeerClient, PeerRpcHandler, PeerServer};
pub use tls::{
    assert_peer_identity, build_client_connector, build_server_acceptor, meta_node_id_from_cert,
    server_name, TlsMaterial,
};
pub use wire::{
    read_frame, write_frame, AdminError, AdminProposeRequest, AdminProposeResponse,
    AdminStatusRequest, AdminStatusResponse, PeerAppendRequest, PeerAppendResponse,
    PeerInstallRequest, PeerInstallResponse, PeerVoteRequest, PeerVoteResponse, TransportError,
    TransportResult, VtpmFrame, WireLogId, KIND_ADMIN_ERROR, KIND_ADMIN_PROPOSE_REQ,
    KIND_ADMIN_PROPOSE_RESP, KIND_ADMIN_STATUS_REQ, KIND_ADMIN_STATUS_RESP, KIND_APPEND_REQ,
    KIND_APPEND_RESP, KIND_INSTALL_REQ, KIND_INSTALL_RESP, KIND_VOTE_REQ, KIND_VOTE_RESP,
    MAX_FRAME_BYTES, VTPM_MAGIC, VTPM_VERSION,
};

//! Raft type configuration for the metadata control plane.

use crate::command::MetadataCommand;
use openraft::declare_raft_types;
use std::io::Cursor;

/// Openraft node id. Distinct from [`crate::keys::MetaNodeId`] only by type
/// context: both are `u64`, and the adapter converts at the storage boundary.
pub type NodeId = u64;

/// Application response: VTOP-encoded [`crate::command::MetadataResponse`]
/// bytes for normal entries; empty for blank / membership entries.
pub type Response = Vec<u8>;

declare_raft_types!(
    /// Type configuration wiring openraft to VTOP metadata commands.
    pub MetaRaftTypeConfig:
        D = MetadataCommand,
        R = Response,
        NodeId = NodeId,
        Node = openraft::EmptyNode,
        Entry = openraft::Entry<MetaRaftTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

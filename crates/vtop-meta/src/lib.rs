//! Deterministic metadata state machine, durable store, Raft adapter, and
//! mTLS peer/admin transport — stage-5 through stage-8 placement foundation.
//!
//! This crate provides the storage half of the replicated metadata control
//! plane, the contained consensus adapter, and the VTPM mTLS transport:
//!
//! - typed, canonically encoded metadata keys ([`keys`]);
//! - hand-coded, bounded, checksummed codecs for commands and responses
//!   ([`command`]) — no serde anywhere in this crate;
//! - a pure, deterministic state machine with CAS generations, strictly
//!   monotonic fencing epochs, and an exactly-once request dedup table that
//!   travels inside snapshots ([`state`]);
//! - deterministic weighted-rendezvous placement over failure-domain
//!   constraints ([`placement`]), committed as durable segment replica sets;
//! - durable hard state, a chunked checksummed raft log, atomic snapshots,
//!   and a deterministic recovery orchestrator ([`storage`]), all running
//!   through the [`vtop_log::env::Env`] seam so crash sweeps drive the exact
//!   production byte paths;
//! - a Raft storage adapter ([`raft`]) that translates consensus engine types
//!   field-by-field into the durable codecs above — every consensus-crate
//!   import is confined to that module tree;
//! - a peer/admin mTLS transport ([`transport`]) speaking VTPM frames with
//!   VTOP-encoded payloads; consensus-crate types never appear here.
//!
//! [`storage::MetaStorage`] flushes a durable `meta.applied` frontier on apply;
//! reopen replays only through that cursor so uncommitted log tails stay out
//! of the state machine. Disks without the file keep the legacy single-node
//! full-log replay behaviour. The adapter also persists `meta.purged` and
//! `meta.membership_log_id` so reopen does not invent LogIds after purge or
//! blank-follower snapshot install.

pub mod command;
pub mod keys;
pub mod placement;
pub mod raft;
pub mod state;
pub mod storage;
pub mod transport;
mod wire;

pub use command::{
    CommandEnvelope, MetadataCommand, MetadataError, MetadataResponse, NodeState, RangeAssignment,
    MAX_ASSIGNED_RANGES, MAX_ERROR_DETAIL_BYTES, MAX_NODE_ADDR_BYTES,
};
pub use keys::{
    validate_group_name, validate_topic_name, MetaKey, MetaNodeId, MAX_GROUP_NAME_BYTES,
    MAX_TOPIC_NAME_BYTES, META_SHARD_ID,
};
pub use placement::{
    rendezvous_score, select_replicas, PlacementCandidate, PlacementError,
    DEFAULT_PLACEMENT_WEIGHT, MAX_FAILURE_DOMAIN_BYTES, MAX_REPLICAS, MIN_PLACEMENT_WEIGHT,
};
pub use raft::{
    CommitReceipt, Consensus, ConsensusError, ConsensusResult, MetaRaftLogStore,
    MetaRaftStateMachine, MetaRaftStore, MetaRaftTypeConfig, OpenraftConsensus, PeerDirectory,
    PeerEndpoint, RaftPeerHandler, ReadFence, TlsRaftNetworkFactory,
};
pub use state::{
    ConsumerGroupRecord, CursorCheckpointRecord, GroupMemberRecord, GroupNameRecord, KeyRecord,
    KeyState, LeaseRecord, MetaStateMachine, MetaValue, NodeRecord, RangeRecord,
    SegmentPlacementRecord, SegmentRecord, SegmentState, TopicNameRecord, TopicRecord,
    DEDUP_CAPACITY,
};
pub use storage::hardstate::{HardState, HardStateFile};
pub use storage::log::{
    MetaLog, MetaLogConfig, MetaLogEntry, MetaLogPayload, MetaMembership, DEFAULT_MAX_CHUNK_BYTES,
    MIN_MAX_CHUNK_BYTES,
};
pub use storage::membership_log_id::MembershipLogId;
pub use storage::snapshot::{MetaSnapshots, SnapshotMeta};
pub use storage::{MetaStorage, MetaStorageConfig, MetaStoreError, MetaStoreResult};
pub use transport::{
    resolve_endpoint, AdminClient, AdminHandler, AdminProposeResponse, AdminServer,
    AdminStatusResponse, PeerClient, PeerRpcHandler, PeerServer, TlsMaterial, TransportError,
    TransportResult, VtpmFrame, WireLogId,
};
pub use wire::CodecError;

//! Deterministic metadata state machine, durable store, and Raft adapter
//! — stage-5 PR 1 + PR 2.
//!
//! This crate provides the storage half of the replicated metadata control
//! plane, plus the contained consensus adapter:
//!
//! - typed, canonically encoded metadata keys ([`keys`]);
//! - hand-coded, bounded, checksummed codecs for commands and responses
//!   ([`command`]) — no serde anywhere in this crate;
//! - a pure, deterministic state machine with CAS generations, strictly
//!   monotonic fencing epochs, and an exactly-once request dedup table that
//!   travels inside snapshots ([`state`]);
//! - durable hard state, a chunked checksummed raft log, atomic snapshots,
//!   and a deterministic recovery orchestrator ([`storage`]), all running
//!   through the [`vtop_log::env::Env`] seam so crash sweeps drive the exact
//!   production byte paths;
//! - a Raft storage adapter ([`raft`]) that translates consensus engine types
//!   field-by-field into the durable codecs above — every consensus-crate
//!   import is confined to that module tree.
//!
//! PR 3 adds the mTLS peer/admin transport. [`storage::MetaStorage`] still
//! treats every durable log entry as committed during single-node recovery;
//! under the adapter, the consensus engine decides the commit frontier.

pub mod command;
pub mod keys;
pub mod raft;
pub mod state;
pub mod storage;
mod wire;

pub use command::{
    CommandEnvelope, MetadataCommand, MetadataError, MetadataResponse, NodeState,
    MAX_ERROR_DETAIL_BYTES, MAX_NODE_ADDR_BYTES,
};
pub use keys::{validate_topic_name, MetaKey, MetaNodeId, MAX_TOPIC_NAME_BYTES, META_SHARD_ID};
pub use state::{
    KeyRecord, KeyState, LeaseRecord, MetaStateMachine, MetaValue, NodeRecord, RangeRecord,
    SegmentRecord, SegmentState, TopicNameRecord, TopicRecord, DEDUP_CAPACITY,
};
pub use storage::hardstate::{HardState, HardStateFile};
pub use storage::log::{
    MetaLog, MetaLogConfig, MetaLogEntry, MetaLogPayload, MetaMembership, DEFAULT_MAX_CHUNK_BYTES,
    MIN_MAX_CHUNK_BYTES,
};
pub use storage::snapshot::{MetaSnapshots, SnapshotMeta};
pub use storage::{MetaStorage, MetaStorageConfig, MetaStoreError, MetaStoreResult};
pub use wire::CodecError;

//! Openraft adapter over the vtop-meta durable store — stage-5 PR 2 + PR 3.
//!
//! # Containment
//!
//! Every `openraft` import in this crate lives under `crates/vtop-meta/src/raft/`.
//! A workspace policy test walks `crates/*/src` and asserts the string
//! `"openraft"` appears nowhere else. Consensus types never leak into the
//! codec, state machine, storage, or transport modules.
//!
//! # VTOP encoding on disk and wire
//!
//! Openraft's in-memory generics (`Entry`, `Vote`, `Membership`, snapshots)
//! are translated field-by-field into [`crate::MetaLogEntry`],
//! [`crate::HardState`], and [`crate::MetaSnapshots`] before any byte reaches
//! the [`vtop_log::env::Env`] seam, and into VTPM peer frames before any byte
//! reaches TCP. There is no `serde_json` / `bincode` of openraft types onto
//! disk or the wire: the VTOP codecs remain the only durable/network formats.
//!
//! # Index offset
//!
//! Openraft's first log entry is at index `0` (`LogId::default()` for the
//! bootstrap membership). [`crate::MetaStorage`] is 1-based: a fresh store
//! has `last_applied == 0` and expects the first durable entry at index `1`
//! (recovery replays `last_applied + 1..`). The adapter stores
//! `meta_index = openraft_index + 1` and translates on every boundary
//! (including the peer wire).
//!
//! # Determinism (honest)
//!
//! Disk I/O is deterministic under the sim seam. The three-node harness uses
//! an in-memory router with seeded partition decisions, a paused-clock
//! current-thread tokio runtime, and explicit `trigger().elect()` /
//! `trigger().heartbeat()` calls instead of wall-clock election timers.
//! The mTLS transport is best-effort under OS scheduling; codec tests are
//! deterministic, live loopback tests are not. Tokio task scheduling is still
//! best-effort; tests assert invariants and print the seed on failure.

// openraft's StorageError is large by design; trait signatures require it by
// value, so boxing at every adapter boundary is not workable.
#![allow(clippy::result_large_err)]

pub mod consensus;
pub mod convert;
pub mod log_store;
pub mod network;
pub mod state_machine;
pub mod store;
pub mod type_config;

pub use consensus::{
    CommitReceipt, Consensus, ConsensusError, ConsensusResult, OpenraftConsensus, ReadFence,
};
pub use log_store::MetaRaftLogStore;
pub use network::{PeerDirectory, PeerEndpoint, RaftPeerHandler, TlsRaftNetworkFactory};
pub use state_machine::MetaRaftStateMachine;
pub use store::MetaRaftStore;
pub use type_config::{MetaRaftTypeConfig, NodeId};

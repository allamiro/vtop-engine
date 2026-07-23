//! Openraft adapter over the vtop-meta durable store — stage-5 PR 2.
//!
//! # Containment
//!
//! Every `openraft` import in this crate lives under `crates/vtop-meta/src/raft/`.
//! A workspace policy test walks `crates/*/src` and asserts the string
//! `"openraft"` appears nowhere else. Consensus types never leak into the
//! codec, state machine, or storage modules.
//!
//! # VTOP encoding on disk
//!
//! Openraft's in-memory generics (`Entry`, `Vote`, `Membership`, snapshots)
//! are translated field-by-field into [`crate::MetaLogEntry`],
//! [`crate::HardState`], and [`crate::MetaSnapshots`] before any byte reaches
//! the [`vtop_log::env::Env`] seam. There is no `serde_json` / `bincode` of
//! openraft types onto disk: the on-disk codecs from PR 1 remain the only
//! durable formats.
//!
//! # Index offset
//!
//! Openraft's first log entry is at index `0` (`LogId::default()` for the
//! bootstrap membership). [`crate::MetaStorage`] is 1-based: a fresh store
//! has `last_applied == 0` and expects the first durable entry at index `1`
//! (recovery replays `last_applied + 1..`). The adapter stores
//! `meta_index = openraft_index + 1` and translates on every boundary.
//!
//! # Determinism (honest)
//!
//! Disk I/O is deterministic under the sim seam. The three-node harness uses
//! an in-memory router with seeded partition decisions, a paused-clock
//! current-thread tokio runtime, and explicit `trigger().elect()` /
//! `trigger().heartbeat()` calls instead of wall-clock election timers.
//! Tokio task scheduling is still best-effort; tests assert invariants and
//! print the seed on failure so a flake can be replayed.

// openraft's StorageError is large by design; trait signatures require it by
// value, so boxing at every adapter boundary is not workable.
#![allow(clippy::result_large_err)]

pub mod convert;
pub mod log_store;
pub mod state_machine;
pub mod store;
pub mod type_config;

pub use log_store::MetaRaftLogStore;
pub use state_machine::MetaRaftStateMachine;
pub use store::MetaRaftStore;
pub use type_config::{MetaRaftTypeConfig, NodeId};

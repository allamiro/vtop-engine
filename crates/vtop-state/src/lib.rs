//! # vtop-state
//!
//! Replay-safe state store for the VTOP Engine. Persists every batch state
//! transition to SQLite so the engine can recover incomplete batches after a
//! crash without ever advancing source progress for unverified data.

pub mod models;
pub mod sqlite_store;

pub use models::{BatchPatch, BatchRecord};
pub use sqlite_store::SqliteStateStore;

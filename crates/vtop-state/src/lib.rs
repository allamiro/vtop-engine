//! # vtop-state
//!
//! Replay-safe state store for the VTOP Engine. Persists every batch state
//! transition through the backend-agnostic [`StateStore`] trait so the engine
//! can recover incomplete batches after a crash without ever advancing source
//! progress for unverified data. SQLite is the built-in backend; the engine
//! depends only on the trait.

pub mod models;
pub mod sqlite_store;
pub mod store;

pub use models::{BatchPatch, BatchRecord};
pub use sqlite_store::SqliteStateStore;
pub use store::{connect_state_store, StateStore};

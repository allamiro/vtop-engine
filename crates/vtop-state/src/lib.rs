//! # vtop-state
//!
//! Replay-safe state store for the VTOP Engine. Persists every batch state
//! transition through the backend-agnostic [`StateStore`] trait so the engine
//! can recover incomplete batches after a crash without ever advancing source
//! progress for unverified data. SQLite is the built-in backend; the engine
//! depends only on the trait.

/// Every column a whole-row read returns, by name: `SELECT` and `RETURNING`
/// name them rather than `*` (review of #664). A statement is prepared once
/// per connection and reused, and a `*` in it is the column list of the
/// moment it was prepared: once another connection added a column - `vtopctl
/// migrate`, or a newer release opening the same SQLite file, while this
/// engine runs - Postgres refused it ("cached plan must not change result
/// type") until the connection was replaced, and SQLite re-prepared it with
/// one column more than the driver expected, which panicked. A column added
/// later is not named here, so it changes nothing this release reads.
/// `PgStateStore::check_schema` names the same list, so startup refuses a
/// schema that lacks one.
macro_rules! batch_columns {
    () => {
        "batch_id, tenant, source_type, source_name, format, state, \
         progress_start_json, progress_end_json, object_uri, manifest_uri, \
         object_sha256, manifest_sha256, manifest_version_id, object_size_bytes, \
         verification_class, record_count, error_message, owner, lease_expires_at, \
         created_at, updated_at, claim_gen"
    };
}

pub mod deadline;
mod fence;
pub mod models;
#[cfg(feature = "postgres")]
pub mod pg_store;
pub mod sqlite_store;
pub mod store;

/// The backend-agnostic behavioural contract every [`StateStore`] must pass.
/// Compiled for in-crate tests and, for cross-crate use (e.g. a Postgres
/// integration test), behind the `test-support` feature.
#[cfg(any(test, feature = "test-support"))]
pub mod test_battery;

pub use models::{
    BatchPatch, BatchRecord, CommittedCursor, CommittedKafkaOffset, VerificationClass,
};
#[cfg(feature = "postgres")]
pub use pg_store::PgStateStore;
pub use sqlite_store::SqliteStateStore;
pub use store::{
    connect_state_store, connect_state_store_with_deadline, migrate_state_store, StateStore,
};

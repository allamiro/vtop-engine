//! The `StateStore` abstraction.
//!
//! The engine depends on this trait, not on any concrete backend, so the same
//! verify-before-commit ledger can live in SQLite (dev / single node) or a
//! Postgres-compatible database (HA fleet) without the engine changing. Every
//! backend routes its transitions through [`vtop_core::state_machine`], so the
//! invariant "source progress is never committed for unverified data" holds
//! regardless of which store is plugged in.
//!
//! Phase 1 of the HA plan: this trait exists and SQLite implements it; the
//! engine holds a `Box<dyn StateStore>`. Postgres arrives in Phase 3 behind a
//! Cargo feature and implements the exact same trait.

use crate::models::{BatchPatch, BatchRecord};
use crate::sqlite_store::SqliteStateStore;
use async_trait::async_trait;
use vtop_core::errors::VtopError;
use vtop_core::state_machine::BatchState;

/// Construct a state store from a connection string, dispatching on the URI
/// scheme. The engine calls this instead of naming a concrete backend, so the
/// deployment picks the store with one config value.
///
/// - `sqlite://path`, `sqlite:path`, `sqlite::memory:`, or a bare path → SQLite
/// - `postgres://…` / `postgresql://…` → Postgres (Phase 3, behind the
///   `postgres` feature; an unhelpful build without it returns a clear error)
pub async fn connect_state_store(conn_str: &str) -> Result<Box<dyn StateStore>, VtopError> {
    if conn_str.starts_with("postgres://") || conn_str.starts_with("postgresql://") {
        #[cfg(feature = "postgres")]
        {
            let store = crate::pg_store::PgStateStore::connect(conn_str).await?;
            return Ok(Box::new(store));
        }
        #[cfg(not(feature = "postgres"))]
        {
            return Err(VtopError::State(
                "postgres:// state store requires a build with --features postgres".to_string(),
            ));
        }
    }
    // Everything else is treated as SQLite (including a bare filesystem path),
    // matching the existing single-node default.
    let store = SqliteStateStore::connect(conn_str).await?;
    Ok(Box::new(store))
}

/// A durable, replay-safe ledger of batch state transitions.
///
/// The method set is deliberately small and matches exactly what the engine
/// needs; it is the single seam between the engine and any storage backend.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Insert a batch when it first enters the store. A duplicate `batch_id`
    /// MUST fail rather than overwrite an existing — possibly committed — row.
    async fn save_batch_state(&self, rec: &BatchRecord) -> Result<(), VtopError>;

    /// Validate and persist a state transition. The transition MUST be checked
    /// against [`vtop_core::state_machine::transition`] so an illegal move
    /// (notably any non-VERIFIED → SOURCE_COMMITTED) is rejected and the store
    /// is left unchanged.
    async fn update_batch_state(
        &self,
        batch_id: &str,
        to: BatchState,
        patch: &BatchPatch,
    ) -> Result<(), VtopError>;

    /// Fetch a single batch, or `None` if it does not exist.
    async fn get_batch(&self, batch_id: &str) -> Result<Option<BatchRecord>, VtopError>;

    /// All batches, newest first.
    async fn list_batches(&self) -> Result<Vec<BatchRecord>, VtopError>;

    /// Batches that have entered the store but not yet reached
    /// `SOURCE_COMMITTED` — the recovery work-list.
    async fn list_incomplete_batches(&self) -> Result<Vec<BatchRecord>, VtopError>;

    /// Batches in the `FAILED` terminal state.
    async fn list_failed_batches(&self) -> Result<Vec<BatchRecord>, VtopError>;

    /// Mark a batch `FAILED` with an error message (legal from any state).
    async fn mark_failed(&self, batch_id: &str, message: &str) -> Result<(), VtopError>;

    /// Mark a batch `VERIFIED` (legal only from `MANIFEST_UPLOADED`).
    async fn mark_verified(&self, batch_id: &str) -> Result<(), VtopError>;

    /// Commit source progress (legal only from `VERIFIED`).
    async fn mark_source_committed(&self, batch_id: &str) -> Result<(), VtopError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn factory_builds_sqlite_from_various_schemes() {
        // Every SQLite spelling the engine might see must produce a working
        // store returned as the trait object (dispatch happens here, not in the
        // engine).
        for conn in ["sqlite::memory:", ":memory:"] {
            let store = connect_state_store(conn).await.expect(conn);
            // A trivial round-trip proves the boxed backend is live.
            assert!(store.list_batches().await.unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn factory_builds_sqlite_from_a_file_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let conn = format!("sqlite://{}", path.display());
        let store = connect_state_store(&conn).await.unwrap();
        assert!(store.list_batches().await.unwrap().is_empty());
        assert!(path.exists(), "the db file should have been created");
    }

    #[tokio::test]
    async fn factory_rejects_postgres_without_the_feature() {
        // Until Phase 3 ships the postgres feature, a postgres:// URI must fail
        // with a clear, actionable error rather than being silently treated as
        // a SQLite path named "postgres:" (which would create a junk file).
        for conn in [
            "postgres://vtop@pg:5432/vtop",
            "postgresql://vtop@pg:5432/vtop",
        ] {
            // `unwrap_err` would need `Box<dyn StateStore>: Debug`; match instead.
            let msg = match connect_state_store(conn).await {
                Ok(_) => panic!("expected {conn} to be rejected"),
                Err(e) => e.to_string(),
            };
            assert!(
                msg.contains("postgres") && msg.contains("feature"),
                "unexpected error for {conn}: {msg}"
            );
        }
    }
}

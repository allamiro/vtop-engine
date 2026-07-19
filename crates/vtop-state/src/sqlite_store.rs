//! SQLite-backed [`StateStore`] using `sqlx`.
//!
//! The state store is the durable journal that makes the engine
//! crash-recoverable. Every state transition is persisted here, and the
//! transition itself is validated through [`vtop_core::state_machine`] so the
//! verification-before-commit rule cannot be violated even at the storage
//! layer.
//!
//! This is the SQLite implementation of the backend-agnostic [`StateStore`]
//! trait (Phase 1 of the HA plan). The Postgres implementation (Phase 3) lives
//! behind a Cargo feature and implements the same trait.

use crate::models::{BatchPatch, BatchRecord};
use crate::store::StateStore;
use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;
use vtop_core::errors::VtopError;
use vtop_core::state_machine::{transition, BatchState};
use vtop_core::types::{ProgressMarker, SourceType, TelemetryFormat};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    tenant TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_name TEXT NOT NULL,
    format TEXT NOT NULL,
    state TEXT NOT NULL,
    progress_start_json TEXT NOT NULL,
    progress_end_json TEXT NOT NULL,
    object_uri TEXT,
    manifest_uri TEXT,
    object_sha256 TEXT,
    manifest_sha256 TEXT,
    record_count INTEGER,
    error_message TEXT,
    owner TEXT,
    lease_expires_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_state ON batches(state);
CREATE INDEX IF NOT EXISTS idx_batches_source ON batches(source_type, source_name);
"#;

/// Handle to the persistent SQLite state store.
#[derive(Clone)]
pub struct SqliteStateStore {
    pool: SqlitePool,
}

fn map_sqlx(e: sqlx::Error) -> VtopError {
    VtopError::State(e.to_string())
}

impl SqliteStateStore {
    /// Open (creating if needed) a state store from a connection string such as
    /// `sqlite:///data/vtop-state.db` or `sqlite::memory:`.
    pub async fn connect(conn_str: &str) -> Result<Self, VtopError> {
        let opts = parse_sqlite_opts(conn_str)?;
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await
            .map_err(map_sqlx)?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<(), VtopError> {
        // Execute each statement in the schema script.
        for stmt in SCHEMA.split(';') {
            let stmt = stmt.trim();
            if stmt.is_empty() {
                continue;
            }
            sqlx::query(stmt)
                .execute(&self.pool)
                .await
                .map_err(map_sqlx)?;
        }
        // Pre-#93 databases lack the ownership columns; ALTER is idempotent-by
        // -failure here (SQLite has no IF NOT EXISTS for columns, and the only
        // failure mode for a duplicate add is the error we ignore).
        let _ = sqlx::query("ALTER TABLE batches ADD COLUMN owner TEXT")
            .execute(&self.pool)
            .await;
        let _ = sqlx::query("ALTER TABLE batches ADD COLUMN lease_expires_at TEXT")
            .execute(&self.pool)
            .await;
        Ok(())
    }

    async fn get_state(&self, batch_id: &str) -> Result<BatchState, VtopError> {
        let row = sqlx::query("SELECT state FROM batches WHERE batch_id = ?")
            .bind(batch_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?
            .ok_or_else(|| VtopError::NotFound(format!("batch {batch_id}")))?;
        let s: String = row.get("state");
        BatchState::from_str(&s)
    }

    /// Shared read helper for the `list_*` queries. `sql` is `'static` because
    /// every caller passes a string literal; sqlx 0.9 ties the query lifetime to
    /// the SQL, so a borrowed `&str` here would escape the executor call.
    async fn query_records(
        &self,
        sql: &'static str,
        bind: Option<String>,
    ) -> Result<Vec<BatchRecord>, VtopError> {
        let mut q = sqlx::query(sql);
        if let Some(b) = bind {
            q = q.bind(b);
        }
        let rows = q.fetch_all(&self.pool).await.map_err(map_sqlx)?;
        rows.into_iter().map(row_to_record).collect()
    }
}

#[async_trait]
impl StateStore for SqliteStateStore {
    /// Insert a new batch. Plain INSERT (not INSERT OR REPLACE): a `batch_id` is
    /// created once. A duplicate id must fail loudly rather than silently
    /// overwrite an existing — possibly already-committed — row.
    async fn save_batch_state(&self, rec: &BatchRecord) -> Result<(), VtopError> {
        let ps = serde_json::to_string(&rec.progress_start)?;
        let pe = serde_json::to_string(&rec.progress_end)?;
        sqlx::query(
            r#"INSERT INTO batches
               (batch_id, tenant, source_type, source_name, format, state,
                progress_start_json, progress_end_json, object_uri, manifest_uri,
                object_sha256, manifest_sha256, record_count, error_message,
                owner, lease_expires_at, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"#,
        )
        .bind(&rec.batch_id)
        .bind(&rec.tenant)
        .bind(rec.source_type.as_str())
        .bind(&rec.source_name)
        .bind(rec.format.as_str())
        .bind(rec.state.as_str())
        .bind(ps)
        .bind(pe)
        .bind(&rec.object_uri)
        .bind(&rec.manifest_uri)
        .bind(&rec.object_sha256)
        .bind(&rec.manifest_sha256)
        .bind(rec.record_count)
        .bind(&rec.error_message)
        .bind(&rec.owner)
        .bind(&rec.lease_expires_at)
        .bind(&rec.created_at)
        .bind(&rec.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Validate and persist a state transition. The new state is checked against
    /// [`vtop_core::state_machine::transition`]; an illegal transition (including
    /// any non-VERIFIED -> SOURCE_COMMITTED) is rejected and the store is left
    /// unchanged.
    async fn update_batch_state(
        &self,
        batch_id: &str,
        to: BatchState,
        patch: &BatchPatch,
    ) -> Result<(), VtopError> {
        let current = self.get_state(batch_id).await?;
        let validated = transition(current, to)?; // enforces the core invariant

        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            r#"UPDATE batches SET
                 state = ?,
                 object_uri = COALESCE(?, object_uri),
                 manifest_uri = COALESCE(?, manifest_uri),
                 object_sha256 = COALESCE(?, object_sha256),
                 manifest_sha256 = COALESCE(?, manifest_sha256),
                 record_count = COALESCE(?, record_count),
                 error_message = COALESCE(?, error_message),
                 updated_at = ?
               WHERE batch_id = ?"#,
        )
        .bind(validated.as_str())
        .bind(&patch.object_uri)
        .bind(&patch.manifest_uri)
        .bind(&patch.object_sha256)
        .bind(&patch.manifest_sha256)
        .bind(patch.record_count)
        .bind(&patch.error_message)
        .bind(now)
        .bind(batch_id)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_batch(&self, batch_id: &str) -> Result<Option<BatchRecord>, VtopError> {
        let row = sqlx::query("SELECT * FROM batches WHERE batch_id = ?")
            .bind(batch_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;
        row.map(row_to_record).transpose()
    }

    async fn list_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.query_records("SELECT * FROM batches ORDER BY created_at DESC", None)
            .await
    }

    async fn claim_incomplete_batches(
        &self,
        owner: &str,
        now: &str,
        lease_until: &str,
    ) -> Result<Vec<BatchRecord>, VtopError> {
        // ONE statement claims; the statement is atomic, so concurrent
        // recoveries cannot both take a batch. RFC3339 strings compare
        // lexicographically, which is what makes the TEXT lease comparison
        // sound (both writers use Utc::to_rfc3339).
        sqlx::query(
            "UPDATE batches SET owner = ?1, lease_expires_at = ?2, updated_at = ?3 \
             WHERE state != ?4 \
               AND (owner IS NULL OR owner = ?1 OR lease_expires_at IS NULL OR lease_expires_at < ?3)",
        )
        .bind(owner)
        .bind(lease_until)
        .bind(now)
        .bind(BatchState::SourceCommitted.as_str())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        let rows = sqlx::query(
            "SELECT * FROM batches WHERE state != ? AND owner = ? ORDER BY created_at ASC",
        )
        .bind(BatchState::SourceCommitted.as_str())
        .bind(owner)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(row_to_record).collect()
    }

    async fn max_committed_end_bytes(
        &self,
        source_type: SourceType,
    ) -> Result<Vec<(String, u64)>, VtopError> {
        // Aggregated in SQL: the ledger grows without bound and this runs at
        // every startup, so the per-path MAX must not materialise the rows
        // (#77). ProgressMarker serializes internally tagged, so the payload
        // fields sit at the JSON top level.
        let rows = sqlx::query(
            "SELECT json_extract(progress_end_json, '$.path') AS path, \
                    MAX(json_extract(progress_end_json, '$.end_byte')) AS end_byte \
             FROM batches WHERE state = ? AND source_type = ? \
             GROUP BY json_extract(progress_end_json, '$.path')",
        )
        .bind(BatchState::SourceCommitted.as_str())
        .bind(source_type.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let path: Option<String> = r.get("path");
                let end: Option<i64> = r.get("end_byte");
                Some((path?, end?.max(0) as u64))
            })
            .collect())
    }

    async fn list_incomplete_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.query_records(
            "SELECT * FROM batches WHERE state != ? ORDER BY created_at ASC",
            Some(BatchState::SourceCommitted.as_str().to_string()),
        )
        .await
    }

    async fn list_failed_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.query_records(
            "SELECT * FROM batches WHERE state = ? ORDER BY created_at ASC",
            Some(BatchState::Failed.as_str().to_string()),
        )
        .await
    }

    async fn mark_failed(&self, batch_id: &str, message: &str) -> Result<(), VtopError> {
        let patch = BatchPatch {
            error_message: Some(message.to_string()),
            ..Default::default()
        };
        self.update_batch_state(batch_id, BatchState::Failed, &patch)
            .await
    }

    async fn mark_verified(&self, batch_id: &str) -> Result<(), VtopError> {
        self.update_batch_state(batch_id, BatchState::Verified, &BatchPatch::default())
            .await
    }

    async fn mark_source_committed(&self, batch_id: &str) -> Result<(), VtopError> {
        self.update_batch_state(
            batch_id,
            BatchState::SourceCommitted,
            &BatchPatch::default(),
        )
        .await
    }
}

fn parse_sqlite_opts(conn_str: &str) -> Result<SqliteConnectOptions, VtopError> {
    // Accept "sqlite::memory:", "sqlite://path", "sqlite:path", or a bare path.
    let opts = if conn_str == "sqlite::memory:" || conn_str == ":memory:" {
        SqliteConnectOptions::from_str("sqlite::memory:").map_err(map_sqlx)?
    } else {
        let path = conn_str
            .strip_prefix("sqlite://")
            .or_else(|| conn_str.strip_prefix("sqlite:"))
            .unwrap_or(conn_str);
        SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
    };
    Ok(opts.busy_timeout(std::time::Duration::from_secs(10)))
}

fn row_to_record(row: sqlx::sqlite::SqliteRow) -> Result<BatchRecord, VtopError> {
    let source_type_s: String = row.get("source_type");
    let format_s: String = row.get("format");
    let state_s: String = row.get("state");
    let ps: String = row.get("progress_start_json");
    let pe: String = row.get("progress_end_json");

    Ok(BatchRecord {
        batch_id: row.get("batch_id"),
        tenant: row.get("tenant"),
        source_type: SourceType::from_str(&source_type_s).map_err(VtopError::State)?,
        source_name: row.get("source_name"),
        format: TelemetryFormat::from_str(&format_s).map_err(VtopError::State)?,
        state: BatchState::from_str(&state_s)?,
        progress_start: serde_json::from_str::<ProgressMarker>(&ps)?,
        progress_end: serde_json::from_str::<ProgressMarker>(&pe)?,
        object_uri: row.get("object_uri"),
        manifest_uri: row.get("manifest_uri"),
        object_sha256: row.get("object_sha256"),
        manifest_sha256: row.get("manifest_sha256"),
        record_count: row.get("record_count"),
        error_message: row.get("error_message"),
        owner: row.get("owner"),
        lease_expires_at: row.get("lease_expires_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_battery;

    // The SQLite backend must pass the same behavioural contract every backend
    // passes. Individual scenarios (save/reload, duplicate rejection, commit-
    // before-verified refusal, the full legal walk, incomplete/failed listing)
    // live once in test_battery::run_all so SQLite and Postgres cannot diverge.
    #[tokio::test]
    async fn sqlite_passes_the_state_store_battery() {
        let store = SqliteStateStore::connect("sqlite::memory:").await.unwrap();
        test_battery::run_all(&store).await;
    }
}

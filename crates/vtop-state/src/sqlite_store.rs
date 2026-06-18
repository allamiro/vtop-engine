//! SQLite-backed state store using `sqlx`.
//!
//! The state store is the durable journal that makes the engine
//! crash-recoverable. Every state transition is persisted here, and the
//! transition itself is validated through [`vtop_core::state_machine`] so the
//! verification-before-commit rule cannot be violated even at the storage
//! layer.

use crate::models::{BatchPatch, BatchRecord};
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
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batches_state ON batches(state);
CREATE INDEX IF NOT EXISTS idx_batches_source ON batches(source_type, source_name);
"#;

/// Handle to the persistent state store.
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
        Ok(())
    }

    /// Insert a new batch (idempotent on `batch_id` via REPLACE semantics for
    /// the initial save). Used when a batch first enters the store.
    pub async fn save_batch_state(&self, rec: &BatchRecord) -> Result<(), VtopError> {
        let ps = serde_json::to_string(&rec.progress_start)?;
        let pe = serde_json::to_string(&rec.progress_end)?;
        sqlx::query(
            r#"INSERT OR REPLACE INTO batches
               (batch_id, tenant, source_type, source_name, format, state,
                progress_start_json, progress_end_json, object_uri, manifest_uri,
                object_sha256, manifest_sha256, record_count, error_message,
                created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"#,
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
        .bind(&rec.created_at)
        .bind(&rec.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Validate and persist a state transition. The new state is checked
    /// against [`vtop_core::state_machine::transition`]; an illegal transition
    /// (including any non-VERIFIED -> SOURCE_COMMITTED) is rejected and the
    /// store is left unchanged.
    pub async fn update_batch_state(
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

    pub async fn get_batch(&self, batch_id: &str) -> Result<Option<BatchRecord>, VtopError> {
        let row = sqlx::query("SELECT * FROM batches WHERE batch_id = ?")
            .bind(batch_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;
        row.map(row_to_record).transpose()
    }

    pub async fn list_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.query_records("SELECT * FROM batches ORDER BY created_at DESC", None)
            .await
    }

    /// Batches that have entered the store but not yet reached
    /// `SOURCE_COMMITTED` — the recovery work-list.
    pub async fn list_incomplete_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.query_records(
            "SELECT * FROM batches WHERE state != ? ORDER BY created_at ASC",
            Some(BatchState::SourceCommitted.as_str().to_string()),
        )
        .await
    }

    pub async fn list_failed_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.query_records(
            "SELECT * FROM batches WHERE state = ? ORDER BY created_at ASC",
            Some(BatchState::Failed.as_str().to_string()),
        )
        .await
    }

    async fn query_records(
        &self,
        sql: &str,
        bind: Option<String>,
    ) -> Result<Vec<BatchRecord>, VtopError> {
        let mut q = sqlx::query(sql);
        if let Some(b) = bind {
            q = q.bind(b);
        }
        let rows = q.fetch_all(&self.pool).await.map_err(map_sqlx)?;
        rows.into_iter().map(row_to_record).collect()
    }

    /// Mark a batch FAILED with an error message (legal from any state).
    pub async fn mark_failed(&self, batch_id: &str, message: &str) -> Result<(), VtopError> {
        let patch = BatchPatch {
            error_message: Some(message.to_string()),
            ..Default::default()
        };
        self.update_batch_state(batch_id, BatchState::Failed, &patch)
            .await
    }

    /// Mark a batch VERIFIED (legal only from MANIFEST_UPLOADED).
    pub async fn mark_verified(&self, batch_id: &str) -> Result<(), VtopError> {
        self.update_batch_state(batch_id, BatchState::Verified, &BatchPatch::default())
            .await
    }

    /// Commit source progress (legal only from VERIFIED — enforced by the
    /// state machine inside `update_batch_state`).
    pub async fn mark_source_committed(&self, batch_id: &str) -> Result<(), VtopError> {
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
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker() -> ProgressMarker {
        ProgressMarker::Kafka {
            topic: "app_events".into(),
            partition: 0,
            start_offset: 0,
            end_offset: 10,
            consumer_group: "vtop-engine".into(),
        }
    }

    fn new_record(id: &str) -> BatchRecord {
        let now = chrono::Utc::now().to_rfc3339();
        BatchRecord {
            batch_id: id.into(),
            tenant: "default".into(),
            source_type: SourceType::Kafka,
            source_name: "app_events".into(),
            format: TelemetryFormat::Cef,
            state: BatchState::Batching,
            progress_start: marker(),
            progress_end: marker(),
            object_uri: None,
            manifest_uri: None,
            object_sha256: None,
            manifest_sha256: None,
            record_count: None,
            error_message: None,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn persists_and_reloads() {
        let store = SqliteStateStore::connect("sqlite::memory:").await.unwrap();
        store.save_batch_state(&new_record("b1")).await.unwrap();
        let got = store.get_batch("b1").await.unwrap().unwrap();
        assert_eq!(got.state, BatchState::Batching);
        assert_eq!(got.source_name, "app_events");
    }

    #[tokio::test]
    async fn rejects_commit_before_verified_at_store_layer() {
        let store = SqliteStateStore::connect("sqlite::memory:").await.unwrap();
        store.save_batch_state(&new_record("b1")).await.unwrap();
        // Batching -> SourceCommitted must be refused.
        let err = store.mark_source_committed("b1").await.unwrap_err();
        assert!(matches!(err, VtopError::CommitBeforeVerified { .. }));
    }

    #[tokio::test]
    async fn full_legal_walk_commits() {
        let store = SqliteStateStore::connect("sqlite::memory:").await.unwrap();
        store.save_batch_state(&new_record("b1")).await.unwrap();
        let p = BatchPatch::default();
        for st in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
            BatchState::Verified,
            BatchState::SourceCommitted,
        ] {
            store.update_batch_state("b1", st, &p).await.unwrap();
        }
        let got = store.get_batch("b1").await.unwrap().unwrap();
        assert_eq!(got.state, BatchState::SourceCommitted);
        assert!(!got.is_incomplete());
    }

    #[tokio::test]
    async fn lists_incomplete_and_failed() {
        let store = SqliteStateStore::connect("sqlite::memory:").await.unwrap();
        store.save_batch_state(&new_record("b1")).await.unwrap();
        store.save_batch_state(&new_record("b2")).await.unwrap();
        store.mark_failed("b2", "boom").await.unwrap();

        let incomplete = store.list_incomplete_batches().await.unwrap();
        assert_eq!(incomplete.len(), 2);
        let failed = store.list_failed_batches().await.unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].error_message.as_deref(), Some("boom"));
    }
}

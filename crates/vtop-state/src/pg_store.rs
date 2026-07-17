//! Postgres-compatible [`StateStore`] using `sqlx` (Phase 3 of the HA plan).
//!
//! This is the durable, shared ledger for an HA fleet: every engine instance
//! points at the same Postgres-compatible database (PostgreSQL, or a
//! self-HA store such as YugabyteDB / CockroachDB). It is behaviourally
//! identical to the SQLite backend — it passes the same [`crate::test_battery`]
//! — and differs only in the driver (`PgPool`), the `$N` placeholders, the
//! Postgres DDL, and two production concerns SQLite does not have:
//!
//! 1. **Defense in depth.** The verify-before-commit invariant is enforced by
//!    `vtop_core::state_machine` at write time AND by a database trigger, so the
//!    DB rejects an illegal `-> source_committed` transition even if application
//!    logic is ever bypassed. A `CHECK` also constrains the state to the known
//!    enum.
//! 2. **Serialization-failure retry.** Distributed Postgres-compatible stores
//!    can abort a transaction with SQLSTATE `40001`; those are transient, so the
//!    write is retried a bounded number of times.
//!
//! Compiled only with `--features postgres`.

use crate::models::{BatchPatch, BatchRecord};
use crate::store::StateStore;
use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use std::str::FromStr;
use std::time::Duration;
use vtop_core::errors::VtopError;
use vtop_core::state_machine::{transition, BatchState};
use vtop_core::types::{ProgressMarker, SourceType, TelemetryFormat};

/// DDL. The logical schema mirrors the SQLite backend (same columns, same
/// meaning) so a `BatchRecord` round-trips identically; the differences are
/// Postgres types plus the invariant guards.
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
    record_count BIGINT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CONSTRAINT state_enum CHECK (state IN (
        'discovered','batching','sealed','compressed','checksummed',
        'object_uploaded','manifest_uploaded','verified','source_committed',
        'failed','replay_required'))
);
CREATE INDEX IF NOT EXISTS idx_batches_state ON batches(state);
CREATE INDEX IF NOT EXISTS idx_batches_source ON batches(source_type, source_name);
"#;

/// The database-level backstop for THE invariant: a row may only move INTO
/// `source_committed` from `verified`. `vtop_core` already refuses this before
/// the UPDATE runs, so this fires only if that check is ever bypassed — which is
/// precisely why it exists.
const INVARIANT_TRIGGER: &str = r#"
CREATE OR REPLACE FUNCTION vtop_enforce_commit_after_verify() RETURNS trigger AS $fn$
BEGIN
    IF NEW.state = 'source_committed' AND OLD.state <> 'verified' THEN
        RAISE EXCEPTION 'commit before verified: batch % is %', OLD.batch_id, OLD.state
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$fn$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_commit_after_verify ON batches;
CREATE TRIGGER trg_commit_after_verify BEFORE UPDATE ON batches
    FOR EACH ROW EXECUTE FUNCTION vtop_enforce_commit_after_verify();
"#;

/// Retry budget for transient serialization failures (SQLSTATE 40001). One
/// retry per attempt with a small backoff; a single-node Postgres essentially
/// never hits this, a distributed store occasionally does.
const MAX_SERIALIZATION_RETRIES: u32 = 5;

/// Handle to the persistent Postgres-compatible state store.
#[derive(Clone)]
pub struct PgStateStore {
    pool: PgPool,
}

fn map_sqlx(e: sqlx::Error) -> VtopError {
    VtopError::State(e.to_string())
}

/// True if the error is a transient serialization failure worth retrying.
fn is_serialization_failure(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .and_then(|db| db.code())
        .map(|c| c == "40001")
        .unwrap_or(false)
}

/// Run a fallible DB closure, retrying only on SQLSTATE 40001.
async fn with_retry<T, F, Fut>(mut op: F) -> Result<T, VtopError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, sqlx::Error>>,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if is_serialization_failure(&e) && attempt < MAX_SERIALIZATION_RETRIES => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(10 * attempt as u64)).await;
            }
            Err(e) => return Err(map_sqlx(e)),
        }
    }
}

impl PgStateStore {
    /// Open a connection pool and apply the schema + invariant trigger.
    pub async fn connect(conn_str: &str) -> Result<Self, VtopError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(conn_str)
            .await
            .map_err(map_sqlx)?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<(), VtopError> {
        // raw_sql uses the simple query protocol, which runs a string containing
        // MULTIPLE statements. A prepared statement (sqlx::query) cannot, and the
        // trigger DDL in particular is several commands. Table + indexes first,
        // then the trigger that references the table.
        sqlx::raw_sql(SCHEMA)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        sqlx::raw_sql(INVARIANT_TRIGGER)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_state(&self, batch_id: &str) -> Result<BatchState, VtopError> {
        let row = sqlx::query("SELECT state FROM batches WHERE batch_id = $1")
            .bind(batch_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?
            .ok_or_else(|| VtopError::NotFound(format!("batch {batch_id}")))?;
        let s: String = row.get("state");
        BatchState::from_str(&s)
    }

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
impl StateStore for PgStateStore {
    async fn save_batch_state(&self, rec: &BatchRecord) -> Result<(), VtopError> {
        let ps = serde_json::to_string(&rec.progress_start)?;
        let pe = serde_json::to_string(&rec.progress_end)?;
        // Plain INSERT: a duplicate batch_id must fail on the primary key, never
        // overwrite a possibly-committed row.
        with_retry(|| {
            sqlx::query(
                r#"INSERT INTO batches
                   (batch_id, tenant, source_type, source_name, format, state,
                    progress_start_json, progress_end_json, object_uri, manifest_uri,
                    object_sha256, manifest_sha256, record_count, error_message,
                    created_at, updated_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)"#,
            )
            .bind(&rec.batch_id)
            .bind(&rec.tenant)
            .bind(rec.source_type.as_str())
            .bind(&rec.source_name)
            .bind(rec.format.as_str())
            .bind(rec.state.as_str())
            .bind(&ps)
            .bind(&pe)
            .bind(&rec.object_uri)
            .bind(&rec.manifest_uri)
            .bind(&rec.object_sha256)
            .bind(&rec.manifest_sha256)
            .bind(rec.record_count)
            .bind(&rec.error_message)
            .bind(&rec.created_at)
            .bind(&rec.updated_at)
            .execute(&self.pool)
        })
        .await?;
        Ok(())
    }

    async fn update_batch_state(
        &self,
        batch_id: &str,
        to: BatchState,
        patch: &BatchPatch,
    ) -> Result<(), VtopError> {
        // Application-level guard first: read the current state and validate the
        // transition through the single source of truth. The DB trigger is the
        // backstop, not the primary check.
        let current = self.get_state(batch_id).await?;
        let validated = transition(current, to)?;
        let now = chrono::Utc::now().to_rfc3339();

        with_retry(|| {
            sqlx::query(
                r#"UPDATE batches SET
                     state = $1,
                     object_uri = COALESCE($2, object_uri),
                     manifest_uri = COALESCE($3, manifest_uri),
                     object_sha256 = COALESCE($4, object_sha256),
                     manifest_sha256 = COALESCE($5, manifest_sha256),
                     record_count = COALESCE($6, record_count),
                     error_message = COALESCE($7, error_message),
                     updated_at = $8
                   WHERE batch_id = $9"#,
            )
            .bind(validated.as_str())
            .bind(&patch.object_uri)
            .bind(&patch.manifest_uri)
            .bind(&patch.object_sha256)
            .bind(&patch.manifest_sha256)
            .bind(patch.record_count)
            .bind(&patch.error_message)
            .bind(&now)
            .bind(batch_id)
            .execute(&self.pool)
        })
        .await?;
        Ok(())
    }

    async fn get_batch(&self, batch_id: &str) -> Result<Option<BatchRecord>, VtopError> {
        let row = sqlx::query("SELECT * FROM batches WHERE batch_id = $1")
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

    async fn list_incomplete_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.query_records(
            "SELECT * FROM batches WHERE state != $1 ORDER BY created_at ASC",
            Some(BatchState::SourceCommitted.as_str().to_string()),
        )
        .await
    }

    async fn list_failed_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.query_records(
            "SELECT * FROM batches WHERE state = $1 ORDER BY created_at ASC",
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

fn row_to_record(row: sqlx::postgres::PgRow) -> Result<BatchRecord, VtopError> {
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

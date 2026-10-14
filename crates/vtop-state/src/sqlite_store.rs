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

use crate::models::{BatchPatch, BatchRecord, CommittedCursor};
use crate::store::StateStore;
use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;
use vtop_core::errors::VtopError;
use vtop_core::state_machine::{transition, BatchState};
use vtop_core::types::{ProgressMarker, SourceType, TelemetryFormat};

/// How many times a write that lost a race to another writer is re-validated
/// and retried before giving up.
const MAX_CONFLICT_RETRIES: u32 = 5;

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
    manifest_version_id TEXT,
    object_size_bytes INTEGER,
    record_count INTEGER,
    error_message TEXT,
    owner TEXT,
    lease_expires_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    claim_gen INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_batches_state ON batches(state);
CREATE INDEX IF NOT EXISTS idx_batches_source ON batches(source_type, source_name);
CREATE INDEX IF NOT EXISTS idx_batches_commit_cursor ON batches(
    state,
    source_type,
    COALESCE(json_extract(progress_end_json, '$.path'), source_name),
    COALESCE(CAST(json_extract(progress_end_json, '$.end_byte') AS INTEGER), -1)
);
"#;

/// Prune predicate shared between the store method and the plan-shape test.
/// A row is deletable only when a strictly "greater" committed row — ordered
/// by (end_byte, updated_at, batch_id) — exists for the same source path
/// (source_name when the marker has no path). Exactly one row per path group
/// has no successor, so the per-path MAX(end_byte) aggregate that seeds
/// recovery cursors is preserved verbatim.
///
/// The successor probe expressions must stay textually identical to the
/// `idx_batches_commit_cursor` expressions above: SQLite matches expression
/// indexes structurally, and without the index every candidate row rescans
/// the entire committed set (quadratic on the engine's idle-cycle pass).
macro_rules! prune_committed_sql {
    () => {
        "DELETE FROM batches WHERE batch_id IN ( \
            SELECT b.batch_id FROM batches b \
            WHERE b.state = ?1 AND b.updated_at < ?2 \
              AND EXISTS ( \
                SELECT 1 FROM batches n \
                WHERE n.state = ?1 \
                  AND n.source_type = b.source_type \
                  AND COALESCE(json_extract(n.progress_end_json, '$.path'), n.source_name) \
                      = COALESCE(json_extract(b.progress_end_json, '$.path'), b.source_name) \
                  AND ( \
                    COALESCE(CAST(json_extract(n.progress_end_json, '$.end_byte') AS INTEGER), -1) \
                      > COALESCE(CAST(json_extract(b.progress_end_json, '$.end_byte') AS INTEGER), -1) \
                    OR ( \
                      COALESCE(CAST(json_extract(n.progress_end_json, '$.end_byte') AS INTEGER), -1) \
                        = COALESCE(CAST(json_extract(b.progress_end_json, '$.end_byte') AS INTEGER), -1) \
                      AND (n.updated_at > b.updated_at \
                           OR (n.updated_at = b.updated_at AND n.batch_id > b.batch_id)) \
                    ) \
                  ) \
              ) \
            LIMIT ?3 \
        )"
    };
}

/// Handle to the persistent SQLite state store.
#[derive(Clone)]
pub struct SqliteStateStore {
    pool: SqlitePool,
    /// The claim generations this process holds (#557).
    fence: crate::fence::ClaimFence,
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
        let store = Self {
            pool,
            fence: Default::default(),
        };
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
        // Pre-#93 databases lack the ownership columns; SQLite has no IF NOT
        // EXISTS for columns, so ONLY the duplicate-column error is ignored -
        // any other failure (locked file, corruption) must surface now, not as
        // a missing-column error on the first batch write.
        for stmt in [
            "ALTER TABLE batches ADD COLUMN owner TEXT",
            "ALTER TABLE batches ADD COLUMN lease_expires_at TEXT",
            "ALTER TABLE batches ADD COLUMN object_size_bytes INTEGER",
            "ALTER TABLE batches ADD COLUMN manifest_version_id TEXT",
            // The claim fence (#557): every claim bumps it; a writer holding an
            // older generation is refused.
            "ALTER TABLE batches ADD COLUMN claim_gen INTEGER NOT NULL DEFAULT 0",
        ] {
            if let Err(e) = sqlx::raw_sql(stmt).execute(&self.pool).await {
                if !e.to_string().contains("duplicate column name") {
                    return Err(map_sqlx(e));
                }
            }
        }
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

    /// Whether a conditional write matched no row because the claim moved on
    /// (another engine owns this batch now) - as opposed to the row changing
    /// state under us, which a retry can resolve. `Ok(None)` when the claim is
    /// still the one held.
    async fn lost_claim(
        &self,
        batch_id: &str,
        held: Option<i64>,
    ) -> Result<Option<VtopError>, VtopError> {
        let row = sqlx::query("SELECT claim_gen FROM batches WHERE batch_id = ?")
            .bind(batch_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?
            .ok_or_else(|| VtopError::NotFound(format!("batch {batch_id}")))?;
        let actual: i64 = row.get("claim_gen");
        Ok(held
            .filter(|held| *held != actual)
            .map(|held| crate::fence::claim_lost(batch_id, held, actual)))
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
                object_sha256, manifest_sha256, manifest_version_id, object_size_bytes,
                record_count, error_message, owner, lease_expires_at, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"#,
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
        .bind(&rec.manifest_version_id)
        .bind(rec.object_size_bytes)
        .bind(rec.record_count)
        .bind(&rec.error_message)
        .bind(&rec.owner)
        .bind(&rec.lease_expires_at)
        .bind(&rec.created_at)
        .bind(&rec.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        self.fence.created(&rec.batch_id);
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
        // Another writer can advance the row between the read and the write
        // (a second process on the file, or a concurrent handle). Re-validate:
        // a transition that is still legal from the row's new state is
        // retried, so a `mark_failed` is not lost to an unrelated update; an
        // illegal one fails on the next pass. A LOST CLAIM is not a lost
        // update - retrying cannot help - so it is reported at once (#557).
        let held = self.fence.held(batch_id);
        let mut attempt = 0;
        loop {
            let current = self.get_state(batch_id).await?;
            let validated = transition(current, to)?; // enforces the core invariant

            let now = chrono::Utc::now().to_rfc3339();
            // Conditional on the state we validated against AND, for a row this
            // process holds a claim on, on the claim still being the one it
            // holds. Unconditional, a write could land on a row another engine
            // claimed and moved on; state names recur, so the state guard alone
            // does not stop it.
            let rows = sqlx::query(
                r#"UPDATE batches SET
                     state = ?,
                     object_uri = COALESCE(?, object_uri),
                     manifest_uri = COALESCE(?, manifest_uri),
                     object_sha256 = COALESCE(?, object_sha256),
                     manifest_sha256 = COALESCE(?, manifest_sha256),
                     manifest_version_id = COALESCE(?, manifest_version_id),
                     object_size_bytes = COALESCE(?, object_size_bytes),
                     record_count = COALESCE(?, record_count),
                     error_message = COALESCE(?, error_message),
                     updated_at = ?
                   WHERE batch_id = ? AND state = ? AND (? IS NULL OR claim_gen = ?)"#,
            )
            .bind(validated.as_str())
            .bind(&patch.object_uri)
            .bind(&patch.manifest_uri)
            .bind(&patch.object_sha256)
            .bind(&patch.manifest_sha256)
            .bind(&patch.manifest_version_id)
            .bind(patch.object_size_bytes)
            .bind(patch.record_count)
            .bind(&patch.error_message)
            .bind(now)
            .bind(batch_id)
            .bind(current.as_str())
            .bind(held)
            .bind(held)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?
            .rows_affected();
            if rows >= 1 {
                if crate::fence::ends_work(validated) {
                    self.fence.release(batch_id);
                }
                return Ok(());
            }
            if let Some(lost) = self.lost_claim(batch_id, held).await? {
                return Err(lost);
            }
            attempt += 1;
            if attempt > MAX_CONFLICT_RETRIES {
                return Err(VtopError::State(format!(
                    "persistent concurrent state change for batch {batch_id} after {attempt} attempts"
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(5 * u64::from(attempt))).await;
        }
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
        // recoveries cannot both take a batch. Lease comparison is textual, so
        // both instants are normalized to canonical UTC first.
        let now = crate::store::normalize_rfc3339_utc("now", now)?;
        let lease_until = crate::store::normalize_rfc3339_utc("lease_until", lease_until)?;
        let (now, lease_until) = (now.as_str(), lease_until.as_str());
        // RETURNING gives exactly the rows THIS statement claimed, with the
        // generation it set: a second claim by an engine that shares this
        // `owner` string cannot slip in between an UPDATE and a follow-up read
        // and leave both holding the same generation, and a batch inserted
        // under the same owner name afterwards is not returned without a
        // fence (review of #569).
        let claimed = sqlx::query(
            "UPDATE batches SET owner = ?1, lease_expires_at = ?2, updated_at = ?3, \
                    claim_gen = claim_gen + 1 \
             WHERE state != ?4 \
               AND (owner IS NULL OR owner = ?1 OR lease_expires_at IS NULL OR lease_expires_at <= ?3) \
             RETURNING *",
        )
        .bind(owner)
        .bind(lease_until)
        .bind(now)
        .bind(BatchState::SourceCommitted.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        for row in &claimed {
            let batch_id: String = row.get("batch_id");
            self.fence.claimed(&batch_id, row.get("claim_gen"));
        }
        let mut records = claimed
            .into_iter()
            .map(row_to_record)
            .collect::<Result<Vec<_>, _>>()?;
        records.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        Ok(records)
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

    async fn committed_cursors(
        &self,
        source_type: SourceType,
    ) -> Result<Vec<CommittedCursor>, VtopError> {
        // Aggregated in SQL (#77): the inode of the newest committed row per
        // path, joined to the MAX(end_byte) over that inode's rows. The inode
        // is read as the JSON token (`->`) and parsed here: `json_extract`
        // turns a u64 above i64::MAX into a lossy REAL, and an unparseable
        // value must read as "unknown", not as a wrong identity.
        //
        // "Newest" is the batch CREATED last - the order the windows were read
        // - not the one whose state changed last: recovery can commit an older
        // VERIFIED window after a newer one, and ordering by `updated_at` would
        // then name the older file as the restart cursor (review of #564).
        let rows = sqlx::query(
            "WITH committed AS ( \
                 SELECT json_extract(progress_end_json, '$.path') AS path, \
                        CAST(progress_end_json -> '$.inode' AS TEXT) AS inode, \
                        CAST(json_extract(progress_end_json, '$.end_byte') AS INTEGER) AS end_byte, \
                        CAST(json_extract(progress_end_json, '$.file_size') AS INTEGER) AS file_size, \
                        created_at, batch_id \
                 FROM batches WHERE state = ? AND source_type = ? \
             ), latest AS ( \
                 SELECT path, inode, file_size, ROW_NUMBER() OVER ( \
                            PARTITION BY path ORDER BY created_at DESC, batch_id DESC) AS rn \
                 FROM committed \
             ), per_inode AS ( \
                 SELECT path, inode, MAX(end_byte) AS end_byte \
                 FROM committed GROUP BY path, inode \
             ) \
             SELECT l.path AS path, l.inode AS inode, p.end_byte AS end_byte, \
                    l.file_size AS file_size \
             FROM latest l \
             JOIN per_inode p ON p.path IS l.path AND p.inode IS l.inode \
             WHERE l.rn = 1",
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
                let inode: Option<String> = r.get("inode");
                let end: Option<i64> = r.get("end_byte");
                let size: Option<i64> = r.get("file_size");
                Some(CommittedCursor {
                    path: path?,
                    inode: inode.and_then(|i| i.parse().ok()),
                    end_byte: end?.max(0) as u64,
                    observed_size: size.map(|s| s.max(0) as u64),
                })
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

    async fn prune_committed_history(
        &self,
        older_than: &str,
        limit: u32,
    ) -> Result<u64, VtopError> {
        let result = sqlx::query(prune_committed_sql!())
            .bind(BatchState::SourceCommitted.as_str())
            .bind(older_than)
            .bind(limit)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(result.rows_affected())
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
        // WAL + synchronous=NORMAL. The default (rollback journal, FULL)
        // fsyncs twice per transaction, and the ledger writes ~8 transactions
        // per batch — measured at 54% of total batch time under backlog (#87).
        //
        // Durability trade, argued not assumed: NORMAL in WAL can lose the
        // most recent commits on power loss (never corrupt the DB). Losing a
        // ledger tail only rolls batches back to an EARLIER state, and every
        // recovery action from an earlier state is idempotent at the archive
        // layer: pre-verified states replay the still-uncommitted source range
        // (duplicate object at worst — the documented at-least-once
        // semantics), and a lost SOURCE_COMMITTED row retries an idempotent
        // source commit. The invariant is untouched: source progress still
        // advances only after verification actually verified stored content.
        //
        // The one asymmetric case (#120 review): source progress commits
        // EXTERNALLY (Kafka broker) after the verified write; if a batch's
        // entire ledger tail is lost, the offset stays advanced with no row to
        // reconcile. That still loses no telemetry — verification durably put
        // the object AND manifest in the store before the commit could run —
        // it leaves an ORPHANED (archived, verified, unledgered) object,
        // discoverable by manifest listing. Accepted for the same reason
        // duplicates are.
        SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
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
        manifest_version_id: row.get("manifest_version_id"),
        object_size_bytes: row.get("object_size_bytes"),
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

    /// #557: two handles on one database file stand for two engines.
    #[tokio::test]
    async fn sqlite_fences_an_engine_that_lost_its_claim() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("fence.db").display());
        let a = SqliteStateStore::connect(&url).await.unwrap();
        let b = SqliteStateStore::connect(&url).await.unwrap();
        test_battery::run_fencing_battery(&a, &b).await;
    }

    /// A process holds a fence token only for work in flight (review of #569):
    /// a batch that ends in FAILED, or is handed back for replay, no longer
    /// pins an entry for the life of the engine - but a token whose claim was
    /// LOST stays, because it is what keeps the stalled writer fenced.
    #[tokio::test]
    async fn fence_tokens_are_released_when_work_ends_and_kept_when_the_claim_is_lost() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("tokens.db").display());
        let a = SqliteStateStore::connect(&url).await.unwrap();
        let b = SqliteStateStore::connect(&url).await.unwrap();
        let patch = BatchPatch::default();
        let record = |id: &str| {
            let mut rec = test_battery::sample_record(id);
            rec.owner = Some("engine-a".into());
            rec.lease_expires_at = Some("2000-01-01T00:00:00+00:00".into());
            rec
        };

        // Failed: the work on this process ended.
        a.save_batch_state(&record("tok-failed")).await.unwrap();
        assert_eq!(a.fence.held("tok-failed"), Some(0));
        a.mark_failed("tok-failed", "upload failed").await.unwrap();
        assert_eq!(a.fence.held("tok-failed"), None);

        // Handed back for replay: the recovery pass is done with it.
        a.update_batch_state("tok-failed", BatchState::ReplayRequired, &patch)
            .await
            .unwrap();
        assert_eq!(a.fence.held("tok-failed"), None);

        // A lost claim keeps its token: the stale writer stays fenced.
        a.save_batch_state(&record("tok-lost")).await.unwrap();
        let claimed = b
            .claim_incomplete_batches(
                "engine-b",
                "2050-01-01T00:00:00+00:00",
                "2100-01-01T00:00:00+00:00",
            )
            .await
            .unwrap();
        assert!(claimed.iter().any(|r| r.batch_id == "tok-lost"));
        assert!(a
            .update_batch_state("tok-lost", BatchState::Sealed, &patch)
            .await
            .is_err());
        assert_eq!(
            a.fence.held("tok-lost"),
            Some(0),
            "retired, the stale writer's next attempt would be unfenced"
        );
        assert!(a
            .update_batch_state("tok-lost", BatchState::Sealed, &patch)
            .await
            .is_err());
    }

    /// A database created before the fence existed gains the column and keeps
    /// working: its rows read as generation 0.
    #[tokio::test]
    async fn sqlite_upgrades_a_database_that_predates_the_fence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.db");
        let url = format!("sqlite://{}", path.display());
        {
            // The pre-fence schema: the table exactly as it was, no claim_gen.
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .connect_with(parse_sqlite_opts(&url).unwrap())
                .await
                .unwrap();
            sqlx::raw_sql(
                "CREATE TABLE batches (batch_id TEXT PRIMARY KEY, tenant TEXT NOT NULL, \
                 source_type TEXT NOT NULL, source_name TEXT NOT NULL, format TEXT NOT NULL, \
                 state TEXT NOT NULL, progress_start_json TEXT NOT NULL, \
                 progress_end_json TEXT NOT NULL, object_uri TEXT, manifest_uri TEXT, \
                 object_sha256 TEXT, manifest_sha256 TEXT, manifest_version_id TEXT, \
                 object_size_bytes INTEGER, record_count INTEGER, error_message TEXT, \
                 owner TEXT, lease_expires_at TEXT, created_at TEXT NOT NULL, \
                 updated_at TEXT NOT NULL)",
            )
            .execute(&pool)
            .await
            .unwrap();
        }
        let store = SqliteStateStore::connect(&url).await.unwrap();
        let rec = test_battery::sample_record("old-1");
        store.save_batch_state(&rec).await.unwrap();
        store
            .update_batch_state("old-1", BatchState::Sealed, &BatchPatch::default())
            .await
            .unwrap();
        // Reopening is idempotent: the column is added once.
        SqliteStateStore::connect(&url).await.unwrap();
    }

    // The SQLite backend must pass the same behavioural contract every backend
    // passes. Individual scenarios (save/reload, duplicate rejection, commit-
    // before-verified refusal, the full legal walk, incomplete/failed listing)
    // live once in test_battery::run_all so SQLite and Postgres cannot diverge.
    #[tokio::test]
    async fn sqlite_passes_the_state_store_battery() {
        let store = SqliteStateStore::connect("sqlite::memory:").await.unwrap();
        test_battery::run_all(&store).await;
        // SQLite has no role separation: the same handle is the maintenance
        // identity, so the prune contract runs here directly.
        test_battery::run_prune_battery(&store).await;
    }

    // The prune successor probe must resolve through idx_batches_commit_cursor;
    // a correlated scan over idx_batches_state alone made a converged no-op
    // prune quadratic in the committed row count (PR #213 review). Plan shape,
    // not timing: EXPLAIN QUERY PLAN either names the index or it regressed.
    #[tokio::test]
    async fn sqlite_prune_successor_probe_uses_the_commit_cursor_index() {
        let store = SqliteStateStore::connect("sqlite::memory:").await.unwrap();
        let rows = sqlx::query(concat!("EXPLAIN QUERY PLAN ", prune_committed_sql!()))
            .bind(BatchState::SourceCommitted.as_str())
            .bind("2026-01-01T00:00:00Z")
            .bind(500u32)
            .fetch_all(&store.pool)
            .await
            .unwrap();
        let plan = rows
            .iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains("idx_batches_commit_cursor"),
            "successor probe no longer uses idx_batches_commit_cursor:\n{plan}"
        );
    }
}

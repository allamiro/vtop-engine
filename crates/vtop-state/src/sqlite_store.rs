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

use crate::audit::{AuditInsert, AuditRow};
use crate::models::{
    parse_content_fingerprint, BatchPatch, BatchRecord, CommittedCursor, CommittedKafkaOffset,
    VerificationClass,
};
use crate::store::StateStore;
use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::str::FromStr;
use vtop_core::errors::VtopError;
use vtop_core::state_machine::{transition, BatchState};
use vtop_core::types::{ProgressMarker, SourceType, TelemetryFormat};

/// The SQLite ledger's schema version, kept in the file's `PRAGMA
/// user_version` (#664). 1 is the first: the schema with `verification_class`.
/// Files from before it read 0 and are brought up to it on open. A higher
/// value means a newer release has opened the file for a change this release
/// could misread - the version is raised for nothing else - and this release
/// refuses the file rather than run on it. Releases before #664 never read the
/// version; this is the refusal path for the releases from here on. Raise it,
/// and the `PRAGMA user_version = 1` in `migrate`, together
/// (`sqlite_refuses_a_file_a_newer_release_opened` checks they agree).
const SQLITE_SCHEMA_VERSION: i64 = 1;

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
    claim_gen INTEGER NOT NULL DEFAULT 0,
    verification_class TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_state ON batches(state);
CREATE INDEX IF NOT EXISTS idx_batches_source ON batches(source_type, source_name);
-- Bounds the successor probe in prune_committed_history: each candidate row
-- finds a newer committed row for its path by a range seek on created_at.
-- Supersedes idx_batches_commit_cursor, which ordered by end_byte (#564).
-- The old index is KEPT for a runtime rolled back past #564, whose prune
-- query seeks through it (review). Drop it once that rollback is
-- unsupported.
CREATE INDEX IF NOT EXISTS idx_batches_commit_cursor ON batches(
    state,
    source_type,
    COALESCE(json_extract(progress_end_json, '$.path'), source_name),
    COALESCE(CAST(json_extract(progress_end_json, '$.end_byte') AS INTEGER), -1)
);
CREATE INDEX IF NOT EXISTS idx_batches_commit_newest ON batches(
    state,
    source_type,
    COALESCE(json_extract(progress_end_json, '$.path'), source_name),
    created_at,
    batch_id
);
-- The audit trail (#694): one row per audit record, append only, chained
-- (see crate::audit). A new table: an older release opening this file never
-- reads it, so the schema version is unchanged. AUTOINCREMENT, so a deleted
-- row's seq is never reused.
CREATE TABLE IF NOT EXISTS audit_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    audit_id TEXT NOT NULL UNIQUE,
    time TEXT NOT NULL,
    action TEXT NOT NULL,
    outcome TEXT NOT NULL,
    actor TEXT,
    target TEXT,
    audit_version INTEGER NOT NULL,
    record TEXT NOT NULL,
    prev_hash TEXT,
    hash TEXT NOT NULL,
    chain TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_events_time ON audit_events(time);
"#;

/// Prune predicate shared between the store method and the plan-shape test.
/// A row is deletable only when a NEWER committed row - ordered by
/// (created_at, batch_id), the order `committed_cursors` reads - exists for
/// the same source path (source_name when the marker has no path). Exactly
/// one row per path group has no successor, and it is the one the restart
/// cursor is seeded from. Ordering by end_byte, as this did, kept the highest
/// offset instead, and after a rotation or a copytruncate that is the old
/// content's row: pruning deleted the restart cursor (review of #564).
///
/// A Kafka row's successor must also be of its partition and consumer group:
/// those rows are where the partition resumes (#614), and a topic's newest
/// row on another partition says nothing about this one. Neither field exists
/// on a file or spool marker, and `IS` compares the two NULLs as equal, so
/// those rows are grouped by path alone, as before. The two comparisons
/// filter the rows the index range seek finds; they do not replace it.
///
/// "Newer" is a row-value comparison on `(created_at, batch_id)`: the same
/// order as `created_at >, or equal and batch_id >`, written so the planner
/// can serve it as a range on the index's last two columns. With the OR form,
/// adding the two comparisons above tipped SQLite onto
/// `idx_batches_commit_cursor`, which seeks on the path only, and the plan
/// test below caught it.
///
/// The successor probe expressions must stay textually identical to the
/// `idx_batches_commit_newest` expressions above: SQLite matches expression
/// indexes structurally, and without the index every candidate row rescans
/// the entire committed set (quadratic on the engine's idle-cycle pass).
///
/// A SUPERSEDED row (?4, #567) is deletable on age alone: it is never the
/// row a restart cursor is read from, so nothing has to survive it.
macro_rules! prune_predicate_sql {
    () => {
        "b.updated_at < ?2 AND (b.state = ?4 OR (b.state = ?1 \
              AND EXISTS ( \
                SELECT 1 FROM batches n \
                WHERE n.state = ?1 \
                  AND n.source_type = b.source_type \
                  AND COALESCE(json_extract(n.progress_end_json, '$.path'), n.source_name) \
                      = COALESCE(json_extract(b.progress_end_json, '$.path'), b.source_name) \
                  AND json_extract(n.progress_end_json, '$.partition') \
                      IS json_extract(b.progress_end_json, '$.partition') \
                  AND json_extract(n.progress_end_json, '$.consumer_group') \
                      IS json_extract(b.progress_end_json, '$.consumer_group') \
                  AND (n.created_at, n.batch_id) > (b.created_at, b.batch_id) \
              )))"
    };
}

macro_rules! prune_committed_sql {
    () => {
        concat!(
            "DELETE FROM batches WHERE batch_id IN ( \
                SELECT b.batch_id FROM batches b WHERE ",
            prune_predicate_sql!(),
            " LIMIT ?3 )"
        )
    };
}

/// The rows the prune would delete next, read and not deleted (#560): the
/// caller records them durably before [`prune_listed_sql`] deletes them.
macro_rules! prune_candidates_sql {
    () => {
        concat!(
            "SELECT ",
            batch_columns!(),
            " FROM batches WHERE batch_id IN ( \
                SELECT b.batch_id FROM batches b WHERE ",
            prune_predicate_sql!(),
            " LIMIT ?3 ) ORDER BY created_at, batch_id"
        )
    };
}

/// Delete exactly the listed rows (a JSON array in ?3), each only if it still
/// meets the prune predicate: a row that gained a reason to stay since it was
/// listed (it became the newest of its path) stays.
macro_rules! prune_listed_sql {
    () => {
        concat!(
            "DELETE FROM batches WHERE batch_id IN ( \
                SELECT b.batch_id FROM batches b \
                WHERE b.batch_id IN (SELECT value FROM json_each(?3)) AND ",
            prune_predicate_sql!(),
            " )"
        )
    };
}

/// Handle to the persistent SQLite state store.
#[derive(Clone)]
pub struct SqliteStateStore {
    pool: SqlitePool,
    /// The writes a power loss must not roll back go through here (#626): one
    /// connection at `synchronous=FULL`. See [`SqliteStateStore::writer_for`].
    durable: SqlitePool,
    /// The claim generations this process holds (#557).
    fence: crate::fence::ClaimFence,
}

fn map_sqlx(e: sqlx::Error) -> VtopError {
    VtopError::State(e.to_string())
}

/// A `(path, inode, end_byte, file_size, content, tail)` row of a cursor
/// query. The inode is read as the JSON token and parsed here: `json_extract`
/// turns a u64 above i64::MAX into a lossy REAL, and an unparseable value must
/// read as "unknown", not as a wrong identity. So must a malformed stored
/// fingerprint (#620).
fn committed_cursor_of(r: &sqlx::sqlite::SqliteRow) -> Option<CommittedCursor> {
    let path: Option<String> = r.get("path");
    let inode: Option<String> = r.get("inode");
    let end: Option<i64> = r.get("end_byte");
    let size: Option<i64> = r.get("file_size");
    let content: Option<String> = r.get("content");
    let mtime: Option<String> = r.get("mtime");
    let tail: Option<String> = r.get("tail");
    Some(CommittedCursor {
        path: path?,
        inode: inode.and_then(|i| i.parse().ok()),
        end_byte: end?.max(0) as u64,
        observed_size: size.map(|s| s.max(0) as u64),
        content_fingerprint: content.as_deref().and_then(parse_content_fingerprint),
        observed_mtime: mtime.filter(|m| !m.is_empty()),
        tail_fingerprint: tail.as_deref().and_then(parse_content_fingerprint),
    })
}

impl SqliteStateStore {
    /// Open (creating if needed) a state store from a connection string such as
    /// `sqlite:///data/vtop-state.db` or `sqlite::memory:`.
    pub async fn connect(conn_str: &str) -> Result<Self, VtopError> {
        let opts = parse_sqlite_opts(conn_str)?;
        let in_memory = is_in_memory(conn_str);
        // A file is migrated on a connection of its own, closed before the
        // pools that serve requests open (#664). Migrating through the pool,
        // a statement at a time, could leave a pool connection that opened
        // before an `ALTER TABLE ... ADD COLUMN` ran, holding statements
        // prepared against the old column list; with `SELECT *` the first
        // recovery claim on an upgraded file returned no rows (see
        // `batch_columns!` for why reads now name their columns, which also
        // covers a column another process adds). An in-memory database is
        // new, so its `ALTER`s add nothing, and it lives only as long as its
        // pool.
        if !in_memory {
            let mut conn = sqlx::ConnectOptions::connect(&opts)
                .await
                .map_err(map_sqlx)?;
            migrate(&mut conn).await?;
            sqlx::Connection::close(conn).await.map_err(map_sqlx)?;
        }
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts.clone())
            .await
            .map_err(map_sqlx)?;
        // An in-memory database has nothing to sync.
        let durable = if in_memory {
            pool.clone()
        } else {
            SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(opts.synchronous(sqlx::sqlite::SqliteSynchronous::Full))
                .await
                .map_err(map_sqlx)?
        };
        if in_memory {
            let mut conn = pool.acquire().await.map_err(map_sqlx)?;
            migrate(&mut conn).await?;
        }
        Ok(Self {
            pool,
            durable,
            fence: Default::default(),
        })
    }

    /// Execute raw SQL against the file. TEST-ONLY, as
    /// `PgStateStore::execute_raw`: a test stands for someone who can write
    /// the file (to tamper with the audit table, #694, or make it fail).
    #[cfg(any(test, feature = "test-support"))]
    pub async fn execute_raw(&self, sql: &'static str) -> Result<(), VtopError> {
        sqlx::raw_sql(sql)
            .execute(&self.durable)
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

    /// Whether a conditional write matched no row because the claim moved on
    /// (another engine owns this batch now) - as opposed to the row changing
    /// state under us, which a retry can resolve. `Ok(None)` when the claim is
    /// still the one held.
    async fn lost_claim(&self, batch_id: &str, held: i64) -> Result<Option<VtopError>, VtopError> {
        let row = sqlx::query("SELECT claim_gen FROM batches WHERE batch_id = ?")
            .bind(batch_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?
            .ok_or_else(|| VtopError::NotFound(format!("batch {batch_id}")))?;
        let actual: i64 = row.get("claim_gen");
        Ok((held != actual).then(|| crate::fence::claim_lost(batch_id, held, actual)))
    }

    /// The pool a transition to `to` is written through (#626).
    ///
    /// `VERIFIED` and `SOURCE_COMMITTED` go through the `synchronous=FULL`
    /// connection, so each returns only after the WAL is synced; every other
    /// write stays at NORMAL and is synced at the next checkpoint. In WAL
    /// mode that sync covers every frame already in the file, so it also makes
    /// the earlier NORMAL writes durable. What a power loss can roll back is
    /// then the writes after the last `VERIFIED` or `SOURCE_COMMITTED`:
    ///
    /// - A file source's restart cursor is the newest committed row, so it
    ///   cannot move back past a commit that returned, and no committed range
    ///   is read and archived again.
    /// - A `VERIFIED` row survives, so the Kafka offset commit that follows
    ///   it can never be left without its row, and recovery commits the row
    ///   rather than re-archiving the range.
    ///
    /// What can still be lost is the rows of batches in flight that had not
    /// reached `VERIFIED`. Those are replayed, as after a plain crash.
    fn writer_for(&self, to: BatchState) -> &SqlitePool {
        match to {
            BatchState::Verified | BatchState::SourceCommitted => &self.durable,
            _ => &self.pool,
        }
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
        crate::store::require_batching_insert(rec)?;
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
        .bind(crate::store::canonical_created_at(&rec.created_at))
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
        crate::store::require_class_only_at_verified(batch_id, to, patch)?;
        let held = self.fence.held(batch_id);
        let mut attempt = 0;
        loop {
            let current = self.get_state(batch_id).await?;
            let validated = transition(current, to)?; // enforces the core invariant
                                                      // Only rows this process created or claimed are written; see the
                                                      // fence module for why a missing token refuses rather than
                                                      // writing unconditionally.
            let held = held.ok_or_else(|| crate::fence::not_held(batch_id))?;

            let now = chrono::Utc::now().to_rfc3339();
            // Conditional on the state we validated against AND on the claim
            // still being the one this process holds. Without the second, a
            // write could land on a row another engine claimed and moved on;
            // state names recur, so the state guard alone does not stop it.
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
                     verification_class = CASE WHEN ? THEN ? ELSE verification_class END,
                     updated_at = ?
                   WHERE batch_id = ? AND state = ? AND claim_gen = ?"#,
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
            // The transition to VERIFIED writes the class it was given, NULL
            // included, rather than keeping an older one: the class describes
            // the verification that admitted the row this time (#664).
            .bind(validated == BatchState::Verified)
            .bind(patch.verification_class.map(VerificationClass::as_str))
            .bind(now)
            .bind(batch_id)
            .bind(current.as_str())
            .bind(held)
            .execute(self.writer_for(validated))
            .await
            .map_err(map_sqlx)?
            .rows_affected();
            if rows >= 1 {
                self.fence.wrote(batch_id, validated, held);
                return Ok(());
            }
            if let Some(lost) = self.lost_claim(batch_id, held).await? {
                self.fence.lost(batch_id, held);
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
        let row = sqlx::query(concat!(
            "SELECT ",
            batch_columns!(),
            " FROM batches WHERE batch_id = ?"
        ))
        .bind(batch_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.map(row_to_record).transpose()
    }

    async fn committed_kafka_offsets(&self) -> Result<Vec<CommittedKafkaOffset>, VtopError> {
        // Aggregated in SQL (#77): per (topic, partition, consumer group), the
        // NEWEST committed row's end offset, in the order `committed_cursors`
        // and pruning use - so the row read here always survives a prune.
        let rows = sqlx::query(
            "WITH committed AS ( \
                 SELECT json_extract(progress_end_json, '$.topic') AS topic, \
                        CAST(json_extract(progress_end_json, '$.partition') AS INTEGER) AS part, \
                        json_extract(progress_end_json, '$.consumer_group') AS grp, \
                        CAST(json_extract(progress_end_json, '$.end_offset') AS INTEGER) AS end_offset, \
                        created_at, batch_id \
                 FROM batches WHERE state = ? AND source_type = ? \
             ), latest AS ( \
                 SELECT topic, part, grp, end_offset, ROW_NUMBER() OVER ( \
                            PARTITION BY topic, part, grp \
                            ORDER BY created_at DESC, batch_id DESC) AS rn \
                 FROM committed \
             ) \
             SELECT topic, part, grp, end_offset FROM latest WHERE rn = 1",
        )
        .bind(BatchState::SourceCommitted.as_str())
        .bind(SourceType::Kafka.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let topic: Option<String> = r.get("topic");
                let partition: Option<i64> = r.get("part");
                let group: Option<String> = r.get("grp");
                let end: Option<i64> = r.get("end_offset");
                Some(CommittedKafkaOffset {
                    topic: topic?,
                    partition: i32::try_from(partition?).ok()?,
                    consumer_group: group?,
                    end_offset: end?,
                })
            })
            .collect())
    }

    async fn list_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.query_records(
            concat!(
                "SELECT ",
                batch_columns!(),
                " FROM batches ORDER BY created_at DESC"
            ),
            None,
        )
        .await
    }

    async fn renew_claim(&self, batch_id: &str, lease_until: &str) -> Result<bool, VtopError> {
        let Some(held) = self.fence.held(batch_id) else {
            return Ok(false);
        };
        let lease_until = crate::store::normalize_rfc3339_utc("lease_until", lease_until)?;
        // Conditional on the generation held: a claim by another engine bumps
        // it, and this write then matches nothing (#624, #557).
        let renewed = sqlx::query(
            "UPDATE batches SET lease_expires_at = ? WHERE batch_id = ? AND claim_gen = ?",
        )
        .bind(lease_until)
        .bind(batch_id)
        .bind(held)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?
        .rows_affected();
        Ok(renewed == 1)
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
        // fence (review of #569). A row in either terminal state is never
        // claimed. Before SUPERSEDED existed (#567), a range it now retires
        // was claimed, rewritten and re-planned at every restart.
        let claimed = sqlx::query(concat!(
            "UPDATE batches SET owner = ?1, lease_expires_at = ?2, updated_at = ?3, \
                    claim_gen = claim_gen + 1 \
             WHERE state NOT IN (?4, ?5) \
               AND (owner IS NULL OR owner = ?1 OR lease_expires_at IS NULL OR lease_expires_at <= ?3) \
             RETURNING ",
            batch_columns!()
        ))
        .bind(owner)
        .bind(lease_until)
        .bind(now)
        .bind(BatchState::SourceCommitted.as_str())
        .bind(BatchState::Superseded.as_str())
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
        // Aggregated in SQL (#77): per path, the NEWEST committed row - its
        // inode, its end byte and the length it observed. The inode is read as
        // the JSON token (`->`) and parsed here: `json_extract` turns a u64
        // above i64::MAX into a lossy REAL, and an unparseable value must read
        // as "unknown", not as a wrong identity.
        //
        // "Newest" is the batch CREATED last - the order the windows were read
        // - not the one whose state changed last: recovery can commit an older
        // VERIFIED window after a newer one, and ordering by `updated_at` would
        // then name the older file as the restart cursor (review of #564).
        // `created_at` is stored in one fixed-width form, so its text order is
        // time order (`canonical_created_at`), and this is the same order
        // pruning keeps the newest row by - the row read here always survives.
        //
        // The end byte is the newest row's own, not a maximum over the rows
        // that share its inode. A path's newest window is in its newest
        // generation, and within one generation the newest window has read
        // furthest - a rewind clamps to the committed point, so a replay never
        // ends below it. A newer window ending LOWER on the same inode is a
        // `copytruncate`, and the maximum would hand the new content the old
        // content's offset, skipping what was written between (review of #564).
        let rows = sqlx::query(
            "WITH committed AS ( \
                 SELECT json_extract(progress_end_json, '$.path') AS path, \
                        CAST(progress_end_json -> '$.inode' AS TEXT) AS inode, \
                        CAST(json_extract(progress_end_json, '$.end_byte') AS INTEGER) AS end_byte, \
                        CAST(json_extract(progress_end_json, '$.file_size') AS INTEGER) AS file_size, \
                        CAST(progress_end_json -> '$.content_fingerprint' AS TEXT) AS content, \
                        CAST(progress_end_json -> '$.tail_fingerprint' AS TEXT) AS tail, \
                        json_extract(progress_end_json, '$.mtime') AS mtime, \
                        created_at, batch_id \
                 FROM batches WHERE state = ? AND source_type = ? \
             ), latest AS ( \
                 SELECT path, inode, end_byte, file_size, content, tail, mtime, ROW_NUMBER() OVER ( \
                            PARTITION BY path ORDER BY created_at DESC, batch_id DESC) AS rn \
                 FROM committed \
             ) \
             SELECT path, inode, end_byte, file_size, content, tail, mtime FROM latest WHERE rn = 1",
        )
        .bind(BatchState::SourceCommitted.as_str())
        .bind(source_type.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(rows.iter().filter_map(committed_cursor_of).collect())
    }

    async fn committed_cursors_by_file(
        &self,
        source_type: SourceType,
    ) -> Result<Vec<CommittedCursor>, VtopError> {
        // `committed_cursors`, partitioned by FILE (path and inode) instead of
        // by path (#621): the newest committed row of each file that has
        // lived at each path, the path's newest first.
        let rows = sqlx::query(
            "WITH committed AS ( \
                 SELECT json_extract(progress_end_json, '$.path') AS path, \
                        CAST(progress_end_json -> '$.inode' AS TEXT) AS inode, \
                        CAST(json_extract(progress_end_json, '$.end_byte') AS INTEGER) AS end_byte, \
                        CAST(json_extract(progress_end_json, '$.file_size') AS INTEGER) AS file_size, \
                        CAST(progress_end_json -> '$.content_fingerprint' AS TEXT) AS content, \
                        CAST(progress_end_json -> '$.tail_fingerprint' AS TEXT) AS tail, \
                        json_extract(progress_end_json, '$.mtime') AS mtime, \
                        created_at, batch_id \
                 FROM batches WHERE state = ? AND source_type = ? \
             ), latest AS ( \
                 SELECT path, inode, end_byte, file_size, content, tail, mtime, \
                        created_at, batch_id, ROW_NUMBER() OVER ( \
                            PARTITION BY path, inode ORDER BY created_at DESC, batch_id DESC) AS rn \
                 FROM committed \
             ) \
             SELECT path, inode, end_byte, file_size, content, tail, mtime FROM latest \
             WHERE rn = 1 ORDER BY path, created_at DESC, batch_id DESC",
        )
        .bind(BatchState::SourceCommitted.as_str())
        .bind(source_type.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(rows.iter().filter_map(committed_cursor_of).collect())
    }

    async fn list_incomplete_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        let rows = sqlx::query(concat!(
            "SELECT ",
            batch_columns!(),
            " FROM batches WHERE state NOT IN (?, ?) ORDER BY created_at ASC"
        ))
        .bind(BatchState::SourceCommitted.as_str())
        .bind(BatchState::Superseded.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(row_to_record).collect()
    }

    async fn list_failed_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        self.query_records(
            concat!(
                "SELECT ",
                batch_columns!(),
                " FROM batches WHERE state = ? ORDER BY created_at ASC"
            ),
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

    async fn mark_verified(
        &self,
        batch_id: &str,
        class: VerificationClass,
    ) -> Result<(), VtopError> {
        let patch = BatchPatch {
            verification_class: Some(class),
            ..Default::default()
        };
        self.update_batch_state(batch_id, BatchState::Verified, &patch)
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
            .bind(BatchState::Superseded.as_str())
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(result.rows_affected())
    }

    async fn prune_candidates(
        &self,
        older_than: &str,
        limit: u32,
    ) -> Result<Vec<BatchRecord>, VtopError> {
        let rows = sqlx::query(prune_candidates_sql!())
            .bind(BatchState::SourceCommitted.as_str())
            .bind(older_than)
            .bind(limit)
            .bind(BatchState::Superseded.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx)?;
        rows.into_iter().map(row_to_record).collect()
    }

    async fn prune_listed(&self, older_than: &str, batch_ids: &[String]) -> Result<u64, VtopError> {
        if batch_ids.is_empty() {
            return Ok(0);
        }
        let result = sqlx::query(prune_listed_sql!())
            .bind(BatchState::SourceCommitted.as_str())
            .bind(older_than)
            .bind(serde_json::to_string(batch_ids)?)
            .bind(BatchState::Superseded.as_str())
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(result.rows_affected())
    }

    async fn append_audit(
        &self,
        row: &AuditInsert,
        key: Option<&vtop_core::manifest::ManifestMacKey>,
    ) -> Result<AuditRow, VtopError> {
        // Through the synchronous=FULL connection: the row is on disk before
        // the action it records runs. BEGIN IMMEDIATE takes the write lock
        // before the last link is read, so two writers (two vtopctl runs)
        // cannot both chain to the same row.
        let mut conn = self.durable.acquire().await.map_err(map_sqlx)?;
        sqlx::raw_sql("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(map_sqlx)?;
        let appended = append_audit_in(&mut conn, row, key).await;
        match appended {
            Ok(stored) => {
                sqlx::raw_sql("COMMIT")
                    .execute(&mut *conn)
                    .await
                    .map_err(map_sqlx)?;
                Ok(stored)
            }
            Err(e) => {
                let _ = sqlx::raw_sql("ROLLBACK").execute(&mut *conn).await;
                Err(e)
            }
        }
    }

    async fn list_audit(&self, after_seq: i64, limit: u32) -> Result<Vec<AuditRow>, VtopError> {
        let rows = sqlx::query(concat!(
            "SELECT ",
            audit_columns!(),
            " FROM audit_events WHERE seq > ? ORDER BY seq LIMIT ?"
        ))
        .bind(after_seq)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(rows.into_iter().map(row_to_audit).collect())
    }
}

async fn append_audit_in(
    conn: &mut sqlx::SqliteConnection,
    row: &AuditInsert,
    key: Option<&vtop_core::manifest::ManifestMacKey>,
) -> Result<AuditRow, VtopError> {
    let previous: Option<String> =
        sqlx::query_scalar("SELECT hash FROM audit_events ORDER BY seq DESC LIMIT 1")
            .fetch_optional(&mut *conn)
            .await
            .map_err(map_sqlx)?;
    let (hash, chain) = crate::audit::link_for(previous.as_deref(), &row.record, key);
    let seq: i64 = sqlx::query_scalar(
        "INSERT INTO audit_events (audit_id, time, action, outcome, actor, target, \
         audit_version, record, prev_hash, hash, chain) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING seq",
    )
    .bind(&row.audit_id)
    .bind(&row.time)
    .bind(&row.action)
    .bind(&row.outcome)
    .bind(&row.actor)
    .bind(&row.target)
    .bind(row.audit_version)
    .bind(&row.record)
    .bind(&previous)
    .bind(&hash)
    .bind(chain)
    .fetch_one(&mut *conn)
    .await
    .map_err(map_sqlx)?;
    Ok(AuditRow {
        seq,
        audit_id: row.audit_id.clone(),
        time: row.time.clone(),
        action: row.action.clone(),
        outcome: row.outcome.clone(),
        actor: row.actor.clone(),
        target: row.target.clone(),
        audit_version: row.audit_version,
        record: row.record.clone(),
        prev_hash: previous,
        hash,
        chain: chain.to_owned(),
    })
}

fn row_to_audit(row: sqlx::sqlite::SqliteRow) -> AuditRow {
    AuditRow {
        seq: row.get("seq"),
        audit_id: row.get("audit_id"),
        time: row.get("time"),
        action: row.get("action"),
        outcome: row.get("outcome"),
        actor: row.get("actor"),
        target: row.get("target"),
        audit_version: row.get("audit_version"),
        record: row.get("record"),
        prev_hash: row.get("prev_hash"),
        hash: row.get("hash"),
        chain: row.get("chain"),
    }
}

/// Create the schema, or bring an older one up to date: idempotent, and run
/// on every open. See [`SqliteStateStore::connect`] for why it runs on one
/// connection.
///
/// One `BEGIN IMMEDIATE` transaction from the version check to the version
/// write (review of #664): it takes the write lock before reading the
/// version, so a newer release raising the version at the same moment is
/// either seen and refused, or waits; this release never writes its version
/// over a newer one.
async fn migrate(conn: &mut sqlx::SqliteConnection) -> Result<(), VtopError> {
    sqlx::raw_sql("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .map_err(map_sqlx)?;
    let added = match migrate_in_transaction(&mut *conn).await {
        Ok(added) => added,
        Err(e) => {
            // The refusal or failure is the error reported; a failed
            // rollback adds nothing to it, and the connection is closed.
            let _ = sqlx::raw_sql("ROLLBACK").execute(&mut *conn).await;
            return Err(e);
        }
    };
    sqlx::raw_sql("COMMIT")
        .execute(&mut *conn)
        .await
        .map_err(map_sqlx)?;
    if added {
        // Counted after the commit, and best effort: the column is there
        // either way, and a failed count must not fail the open.
        match sqlx::query_scalar::<_, i64>("SELECT count(*) FROM batches")
            .fetch_one(&mut *conn)
            .await
        {
            Ok(rows) => crate::models::log_class_column_added("SQLite", rows),
            Err(e) => crate::models::log_class_column_added_uncounted("SQLite", &e.to_string()),
        }
    }
    Ok(())
}

/// [`migrate`]'s statements, in its transaction. `true` when this run added
/// the verification class column to a table that already existed.
async fn migrate_in_transaction(conn: &mut sqlx::SqliteConnection) -> Result<bool, VtopError> {
    let mut added = false;
    // A file a newer release has opened is left alone (#664): see
    // SQLITE_SCHEMA_VERSION.
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut *conn)
        .await
        .map_err(map_sqlx)?;
    if version > SQLITE_SCHEMA_VERSION {
        return Err(VtopError::State(format!(
            "SQLite state-store schema version {version} (the ledger file's PRAGMA user_version) \
             is newer than this release supports (newest {SQLITE_SCHEMA_VERSION}): a newer \
             release has opened this file. Nothing was started or changed. Run that release or a \
             later one; downgrade is not supported"
        )));
    }
    // Execute each statement in the schema script.
    for stmt in SCHEMA.split(';') {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        sqlx::query(stmt)
            .execute(&mut *conn)
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
        // The class of verification a VERIFIED row passed (#664). NULL on
        // every row from before it: unknown, decided as those rows always
        // were. An older binary opening this file ignores the column: it
        // reads rows by column name and never writes it.
        "ALTER TABLE batches ADD COLUMN verification_class TEXT",
    ] {
        match sqlx::raw_sql(stmt).execute(&mut *conn).await {
            Err(e) if !e.to_string().contains("duplicate column name") => {
                return Err(map_sqlx(e));
            }
            Err(_) => {}
            // Added to a table that already existed: a new table is created
            // with the column, and this ALTER finds it there.
            Ok(_) if stmt.contains("verification_class") => added = true,
            Ok(_) => {}
        }
    }
    // A row is born only as batching (#625), in the database too, so a
    // write that skips save_batch_state cannot create one born verified.
    // A trigger, not a CHECK: an existing table gains it without a
    // rebuild. Run apart from SCHEMA, whose split on ';' would cut the
    // trigger body. Every release inserts only batching, so an older
    // binary opening this file is unaffected.
    sqlx::raw_sql(
        "CREATE TRIGGER IF NOT EXISTS trg_batches_insert_as_batching \
         BEFORE INSERT ON batches WHEN NEW.state <> 'batching' \
         BEGIN SELECT RAISE(ABORT, 'a batch must be inserted as batching'); END",
    )
    .execute(&mut *conn)
    .await
    .map_err(map_sqlx)?;
    // The audit table is append only (#694): an outcome is a second row,
    // never an update of the attempt. Anyone who can write this file can
    // drop the triggers; the chain is what shows that.
    for trigger in [
        "CREATE TRIGGER IF NOT EXISTS trg_audit_events_no_update \
         BEFORE UPDATE ON audit_events \
         BEGIN SELECT RAISE(ABORT, 'audit_events is append-only'); END",
        "CREATE TRIGGER IF NOT EXISTS trg_audit_events_no_delete \
         BEFORE DELETE ON audit_events \
         BEGIN SELECT RAISE(ABORT, 'audit_events is append-only'); END",
    ] {
        sqlx::raw_sql(trigger)
            .execute(&mut *conn)
            .await
            .map_err(map_sqlx)?;
    }
    if version < SQLITE_SCHEMA_VERSION {
        sqlx::raw_sql("PRAGMA user_version = 1")
            .execute(&mut *conn)
            .await
            .map_err(map_sqlx)?;
    }
    Ok(added)
}

fn is_in_memory(conn_str: &str) -> bool {
    conn_str == "sqlite::memory:" || conn_str == ":memory:"
}

fn parse_sqlite_opts(conn_str: &str) -> Result<SqliteConnectOptions, VtopError> {
    // Accept "sqlite::memory:", "sqlite://path", "sqlite:path", or a bare path.
    let opts = if is_in_memory(conn_str) {
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
        // NORMAL in WAL syncs at checkpoints only, so a power loss can lose
        // every commit since the last one (never corrupt the DB). Losing a
        // ledger tail rolls batches back to an EARLIER state, which is safe:
        // a pre-verified row replays its range, and source progress still
        // advances only after verification. But it is not cheap: a file
        // source resumes from the newest committed row that survived, and
        // every range committed after it is read and archived again under a
        // new key (#626). So VERIFIED and SOURCE_COMMITTED are written through
        // a second connection at FULL (`writer_for`): one WAL sync each, and
        // a lost tail can then hold only the batches still in flight.
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
        verification_class: VerificationClass::from_stored(
            row.get::<Option<String>, _>("verification_class")
                .as_deref(),
        ),
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

    /// A process holds a fence token while it may still write the batch, and
    /// writes nothing it holds no token for (review of #569):
    /// - FAILED keeps the token: recovery writes REPLAY_REQUIRED over it next,
    ///   and an engine sharing this one's owner name can claim the row in
    ///   between. Released at FAILED, that write went through unfenced.
    /// - REPLAY_REQUIRED releases it.
    /// - A lost claim keeps it: it is what keeps the stalled writer fenced.
    /// - A row this process never created or claimed is refused.
    #[tokio::test]
    async fn fence_tokens_are_kept_while_the_batch_can_still_be_written() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("tokens.db").display());
        let a = SqliteStateStore::connect(&url).await.unwrap();
        let b = SqliteStateStore::connect(&url).await.unwrap();
        let patch = BatchPatch::default();
        let record = |id: &str| {
            let mut rec = test_battery::sample_record(id);
            rec.owner = Some("engine-a".into());
            rec.lease_expires_at = Some("2100-01-01T00:00:00+00:00".into());
            rec
        };
        let claim_as_engine_a = || {
            b.claim_incomplete_batches(
                "engine-a",
                "2050-01-01T00:00:00+00:00",
                "2100-01-01T00:00:00+00:00",
            )
        };

        // FAILED then REPLAY_REQUIRED, uncontended: the token spans both.
        a.save_batch_state(&record("tok-handoff")).await.unwrap();
        a.mark_failed("tok-handoff", "upload failed").await.unwrap();
        assert_eq!(a.fence.held("tok-handoff"), Some(0));
        a.update_batch_state("tok-handoff", BatchState::ReplayRequired, &patch)
            .await
            .unwrap();
        assert_eq!(a.fence.held("tok-handoff"), None);

        // FAILED, then an engine under the same name claims the row, then the
        // first writes REPLAY_REQUIRED: refused, and the row is left as the
        // new claimant has it.
        a.save_batch_state(&record("tok-race")).await.unwrap();
        a.mark_failed("tok-race", "upload failed").await.unwrap();
        let claimed = claim_as_engine_a().await.unwrap();
        assert!(claimed.iter().any(|r| r.batch_id == "tok-race"));
        let refused = a
            .update_batch_state("tok-race", BatchState::ReplayRequired, &patch)
            .await;
        assert!(
            refused
                .as_ref()
                .is_err_and(|e| e.to_string().contains("claimed by another engine")),
            "the handoff write went through over the new claim: {refused:?}"
        );
        assert_eq!(
            a.get_batch("tok-race").await.unwrap().unwrap().state,
            BatchState::Failed
        );
        assert_eq!(
            a.fence.held("tok-race"),
            Some(0),
            "kept: the stale writer's next attempt must be refused too"
        );

        // A row this process never created or claimed is not written at all.
        let c = SqliteStateStore::connect(&url).await.unwrap();
        let refused = c
            .update_batch_state("tok-race", BatchState::ReplayRequired, &patch)
            .await;
        assert!(
            refused
                .as_ref()
                .is_err_and(|e| e.to_string().contains("not claimed by this engine")),
            "written without a token: {refused:?}"
        );
        assert_eq!(
            c.get_batch("tok-race").await.unwrap().unwrap().state,
            BatchState::Failed
        );
    }

    /// #626: VERIFIED and SOURCE_COMMITTED are written by the connection that
    /// runs at `synchronous=FULL`, and no other transition is. `total_changes()`
    /// counts the rows the connection it runs on has changed, so it shows
    /// which connection actually wrote, not only which one is configured.
    #[tokio::test]
    async fn verified_and_source_committed_are_written_at_synchronous_full() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("durable.db").display());
        let store = SqliteStateStore::connect(&url).await.unwrap();
        let synchronous = |pool: &SqlitePool| {
            let pool = pool.clone();
            async move {
                sqlx::query_scalar::<_, i64>("PRAGMA synchronous")
                    .fetch_one(&pool)
                    .await
                    .unwrap()
            }
        };
        assert_eq!(synchronous(&store.durable).await, 2, "FULL");
        assert_eq!(synchronous(&store.pool).await, 1, "NORMAL");
        let written_at_full =
            || sqlx::query_scalar::<_, i64>("SELECT total_changes()").fetch_one(&store.durable);

        store
            .save_batch_state(&test_battery::sample_record("sync-1"))
            .await
            .unwrap();
        let patch = BatchPatch::default();
        for to in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
        ] {
            store
                .update_batch_state("sync-1", to, &patch)
                .await
                .unwrap();
        }
        assert_eq!(
            written_at_full().await.unwrap(),
            0,
            "nothing before VERIFIED"
        );
        store
            .mark_verified("sync-1", crate::VerificationClass::Strong)
            .await
            .unwrap();
        assert_eq!(written_at_full().await.unwrap(), 1, "VERIFIED");
        store.mark_source_committed("sync-1").await.unwrap();
        assert_eq!(written_at_full().await.unwrap(), 2, "SOURCE_COMMITTED");
        assert_eq!(
            store.get_batch("sync-1").await.unwrap().unwrap().state,
            BatchState::SourceCommitted
        );
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

    /// #664: a row's verification class survives a restart - the store
    /// closed and the file reopened - and recovery's claim reads it back.
    #[tokio::test]
    async fn sqlite_keeps_the_verification_class_across_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("class.db").display());
        {
            let store = SqliteStateStore::connect(&url).await.unwrap();
            test_battery::verification_class_before_restart(&store).await;
        }
        let reopened = SqliteStateStore::connect(&url).await.unwrap();
        test_battery::verification_class_after_restart(&reopened).await;
    }

    /// #664: a database from before the verification class gains the column
    /// on open, and its VERIFIED rows read with no class - unknown, which
    /// recovery decides as it did before the class existed.
    #[tokio::test]
    async fn sqlite_upgrades_a_database_that_predates_the_verification_class() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pre-664.db");
        let url = format!("sqlite://{}", path.display());
        {
            // The table exactly as releases before #664 left it, with a
            // VERIFIED row in it.
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
                 updated_at TEXT NOT NULL, claim_gen INTEGER NOT NULL DEFAULT 0)",
            )
            .execute(&pool)
            .await
            .unwrap();
            let marker =
                serde_json::to_string(&test_battery::sample_record("pre-664").progress_end)
                    .unwrap();
            // A row in each state a ledger holds.
            for (id, state) in [
                ("pre-664", "verified"),
                ("pre-664-committed", "source_committed"),
                ("pre-664-failed", "failed"),
                ("pre-664-batching", "batching"),
            ] {
                sqlx::query(
                    "INSERT INTO batches (batch_id, tenant, source_type, source_name, format, \
                     state, progress_start_json, progress_end_json, created_at, updated_at) \
                     VALUES (?, 'default', 'kafka', 'app_events', 'cef', ?, ?, ?, 't', 't')",
                )
                .bind(id)
                .bind(state)
                .bind(&marker)
                .bind(&marker)
                .execute(&pool)
                .await
                .unwrap();
            }
        }
        // The message the open logs is asserted in tests/operator_messages.rs.
        let store = SqliteStateStore::connect(&url).await.unwrap();
        for (id, state) in [
            ("pre-664", BatchState::Verified),
            ("pre-664-committed", BatchState::SourceCommitted),
            ("pre-664-failed", BatchState::Failed),
            ("pre-664-batching", BatchState::Batching),
        ] {
            let got = store.get_batch(id).await.unwrap().unwrap();
            assert_eq!(
                (got.state, got.verification_class),
                (state, None),
                "{id}: a row written before #664 keeps its state and has no class"
            );
        }
        let claimed = store
            .claim_incomplete_batches(
                "engine-upgraded",
                "2070-01-01T00:00:00+00:00",
                "2070-01-01T00:05:00+00:00",
            )
            .await
            .unwrap();
        let verified = claimed
            .iter()
            .find(|row| row.batch_id == "pre-664")
            .unwrap_or_else(|| panic!("{claimed:?}"));
        assert_eq!(
            (verified.state, verified.verification_class),
            (BatchState::Verified, None),
            "a row verified before #664 has no class"
        );
        store.mark_source_committed("pre-664").await.unwrap();
        // And the upgraded file records a class from now on.
        store
            .save_batch_state(&test_battery::sample_record("post-664"))
            .await
            .unwrap();
        for st in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
        ] {
            store
                .update_batch_state("post-664", st, &BatchPatch::default())
                .await
                .unwrap();
        }
        store
            .mark_verified("post-664", crate::VerificationClass::SizeOnly)
            .await
            .unwrap();
        let got = store.get_batch("post-664").await.unwrap().unwrap();
        assert_eq!(
            got.verification_class,
            Some(crate::VerificationClass::SizeOnly)
        );
        // Reopening is idempotent: the column is added once.
        drop(store);
        SqliteStateStore::connect(&url).await.unwrap();
    }

    /// #664: a file a newer release has opened - its schema version above
    /// the newest this release reads - is refused, naming the version and
    /// the newest this release reads, and left as it was. A file this
    /// release opens carries its version.
    #[tokio::test]
    async fn sqlite_refuses_a_file_a_newer_release_opened() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("newer.db").display());
        let store = SqliteStateStore::connect(&url).await.unwrap();
        store
            .save_batch_state(&test_battery::sample_record("kept"))
            .await
            .unwrap();
        drop(store);
        let mut other = sqlx::ConnectOptions::connect(&parse_sqlite_opts(&url).unwrap())
            .await
            .unwrap();
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&mut other)
            .await
            .unwrap();
        assert_eq!(version, SQLITE_SCHEMA_VERSION);
        sqlx::raw_sql("PRAGMA user_version = 2")
            .execute(&mut other)
            .await
            .unwrap();
        let refused = match SqliteStateStore::connect(&url).await {
            Ok(_) => panic!("a file a newer release opened was accepted"),
            Err(e) => e.to_string(),
        };
        for part in [
            "schema version 2",
            "newest 1",
            "newer release",
            "downgrade is not supported",
        ] {
            assert!(refused.contains(part), "{part:?} missing from: {refused}");
        }
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&mut other)
            .await
            .unwrap();
        assert_eq!(version, 2, "left as it was");
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM batches")
            .fetch_one(&mut other)
            .await
            .unwrap();
        assert_eq!(rows, 1);
    }

    /// Review of #664: the version check and the version write are one
    /// transaction with the migration. A newer release that raises the
    /// version while this one opens the file must be refused, not have its
    /// version written back down: this release read 0, then ran its DDL,
    /// then wrote 1 over the newer release's 2.
    #[tokio::test]
    async fn sqlite_does_not_write_a_newer_releases_version_back_down() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("race.db");
        let url = format!("sqlite://{}", path.display());
        // A pre-#664 file: version 0, no class column.
        {
            let mut conn = sqlx::ConnectOptions::connect(&parse_sqlite_opts(&url).unwrap())
                .await
                .unwrap();
            sqlx::raw_sql(
                "CREATE TABLE batches (batch_id TEXT PRIMARY KEY, tenant TEXT NOT NULL, \
                 source_type TEXT NOT NULL, source_name TEXT NOT NULL, format TEXT NOT NULL, \
                 state TEXT NOT NULL, progress_start_json TEXT NOT NULL, \
                 progress_end_json TEXT NOT NULL, created_at TEXT NOT NULL, \
                 updated_at TEXT NOT NULL)",
            )
            .execute(&mut conn)
            .await
            .unwrap();
        }
        // The newer release holds the write lock and raises the version.
        let mut newer = sqlx::ConnectOptions::connect(&parse_sqlite_opts(&url).unwrap())
            .await
            .unwrap();
        sqlx::raw_sql("BEGIN IMMEDIATE; PRAGMA user_version = 2;")
            .execute(&mut newer)
            .await
            .unwrap();
        let opening = tokio::spawn({
            let url = url.clone();
            async move { SqliteStateStore::connect(&url).await.map(|_| ()) }
        });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        sqlx::raw_sql("COMMIT").execute(&mut newer).await.unwrap();
        let opened = opening.await.unwrap();
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&mut newer)
            .await
            .unwrap();
        assert_eq!(
            version, 2,
            "the newer release's version was written back down"
        );
        let refused = opened.expect_err("this release opened a file a newer release had raised");
        assert!(
            refused.to_string().contains("schema version 2"),
            "{refused}"
        );
    }

    /// Review of #664: a store that is open while another connection adds a
    /// column - a newer release opening the same file - keeps reading whole
    /// rows.
    #[tokio::test]
    async fn sqlite_reads_whole_rows_after_another_connection_adds_a_column() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("later.db").display());
        let store = SqliteStateStore::connect(&url).await.unwrap();
        test_battery::seed_rows_for_every_read_path(&store).await;
        test_battery::every_read_path_returns_whole_rows(&store).await;
        let mut other = sqlx::ConnectOptions::connect(&parse_sqlite_opts(&url).unwrap())
            .await
            .unwrap();
        sqlx::raw_sql("ALTER TABLE batches ADD COLUMN vtop_test_later TEXT")
            .execute(&mut other)
            .await
            .unwrap();
        test_battery::every_read_path_returns_whole_rows(&store).await;
    }

    /// A value this build does not know - written by a newer release - reads
    /// as unknown, not as an error and not as either class.
    #[tokio::test]
    async fn sqlite_reads_an_unknown_verification_class_as_unknown() {
        let store = SqliteStateStore::connect("sqlite::memory:").await.unwrap();
        store
            .save_batch_state(&test_battery::sample_record("newer-class"))
            .await
            .unwrap();
        sqlx::query("UPDATE batches SET verification_class = 'quantum' WHERE batch_id = ?")
            .bind("newer-class")
            .execute(&store.pool)
            .await
            .unwrap();
        let got = store.get_batch("newer-class").await.unwrap().unwrap();
        assert_eq!(got.verification_class, None);
    }

    /// #625, in the database: a write that skips `save_batch_state` (a tool
    /// with its own connection, a future import) still cannot create a row in
    /// any state but BATCHING.
    #[tokio::test]
    async fn sqlite_refuses_a_direct_insert_in_any_state_but_batching() {
        let store = SqliteStateStore::connect("sqlite::memory:").await.unwrap();
        let insert = |id: &'static str, state: &'static str| {
            sqlx::query(
                "INSERT INTO batches (batch_id, tenant, source_type, source_name, format, \
                 state, progress_start_json, progress_end_json, created_at, updated_at) \
                 VALUES (?, 'default', 'kafka', 'app', 'cef', ?, '{}', '{}', 't', 't')",
            )
            .bind(id)
            .bind(state)
            .execute(&store.pool)
        };
        for state in ["verified", "source_committed", "sealed"] {
            let err = insert("direct-refused", state)
                .await
                .expect_err("a direct insert in a later state must be refused");
            assert!(
                err.to_string().contains("must be inserted as batching"),
                "expected the trigger's message for {state}, got: {err}"
            );
        }
        assert!(store.get_batch("direct-refused").await.unwrap().is_none());
        insert("direct-batching", "batching").await.unwrap();
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
        test_battery::run_audit_battery(&store).await;
    }

    /// #694: the audit table is append only in the database too, created on
    /// open of a file from before it without raising the schema version, and
    /// a keyed chain round-trips through the store.
    #[tokio::test]
    async fn sqlite_keeps_an_append_only_chained_audit_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let conn = format!("sqlite://{}", path.display());
        // A file as a release before #694 left it: the ledger, no audit table.
        {
            let store = SqliteStateStore::connect(&conn).await.unwrap();
            sqlx::raw_sql(
                "DROP TRIGGER trg_audit_events_no_update; \
                 DROP TRIGGER trg_audit_events_no_delete; DROP TABLE audit_events",
            )
            .execute(&store.pool)
            .await
            .unwrap();
        }
        let store = SqliteStateStore::connect(&conn).await.unwrap();
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(version, SQLITE_SCHEMA_VERSION, "a new table needs no bump");
        let key = vtop_core::manifest::ManifestMacKey::from_hex(&"ab".repeat(32)).unwrap();
        for i in 0..3 {
            let record = format!(
                r#"{{"audit_version":1,"audit_id":"k{i}","time":"t","action":"ledger.replay","outcome":"attempt","actor":{{"kind":"os","uid":1,"source":"os"}},"target":{{"kind":"ledger"}}}}"#
            );
            let row = crate::audit::AuditInsert::from_record(&record).unwrap();
            let stored = store.append_audit(&row, Some(&key)).await.unwrap();
            assert_eq!(stored.chain, "blake3-keyed");
        }
        let rows = store.list_audit(0, 10).await.unwrap();
        assert!(crate::audit::verify_chain(&rows, Some(&key)).passed());
        for (sql, what) in [
            (
                "UPDATE audit_events SET outcome = 'success' WHERE seq = 1",
                "UPDATE",
            ),
            ("DELETE FROM audit_events WHERE seq = 1", "DELETE"),
        ] {
            let err = sqlx::raw_sql(sql)
                .execute(&store.pool)
                .await
                .expect_err(what);
            assert!(err.to_string().contains("append-only"), "{what}: {err}");
        }
        // Someone with the file drops the triggers and edits a row: the
        // chain shows it.
        sqlx::raw_sql(
            "DROP TRIGGER trg_audit_events_no_update; \
             UPDATE audit_events SET record = replace(record, 'attempt', 'success') WHERE seq = 2",
        )
        .execute(&store.pool)
        .await
        .unwrap();
        let rows = store.list_audit(0, 10).await.unwrap();
        let report = crate::audit::verify_chain(&rows, Some(&key));
        assert!(!report.passed());
        assert!(
            report.problems[0].contains("row 2"),
            "{:?}",
            report.problems
        );
    }

    // The prune successor probe must resolve through idx_batches_commit_newest;
    // a correlated scan over idx_batches_state alone made a converged no-op
    // prune quadratic in the committed row count (PR #213 review). Plan shape,
    // not timing: EXPLAIN QUERY PLAN either names the index or it regressed.
    #[tokio::test]
    async fn sqlite_prune_successor_probe_uses_the_commit_newest_index() {
        let store = SqliteStateStore::connect("sqlite::memory:").await.unwrap();
        let rows = sqlx::query(concat!("EXPLAIN QUERY PLAN ", prune_committed_sql!()))
            .bind(BatchState::SourceCommitted.as_str())
            .bind("2026-01-01T00:00:00Z")
            .bind(500u32)
            .bind(BatchState::Superseded.as_str())
            .fetch_all(&store.pool)
            .await
            .unwrap();
        let plan = rows
            .iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains("idx_batches_commit_newest"),
            "successor probe no longer uses idx_batches_commit_newest:\n{plan}"
        );
    }
}

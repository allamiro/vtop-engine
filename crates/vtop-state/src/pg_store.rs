//! Postgres-compatible [`StateStore`] using `sqlx` (Phase 3 of the HA plan).
//!
//! This is the durable, shared ledger for an HA fleet: every engine instance
//! points at the same Postgres-compatible database (PostgreSQL, or a
//! self-HA store such as YugabyteDB / CockroachDB). It is behaviourally
//! identical to the SQLite backend — it passes the same [`crate::test_battery`]
//! — and differs only in the driver (`PgPool`), the `$N` placeholders, the
//! Postgres DDL, and four production concerns SQLite does not have:
//!
//! 1. **Defense in depth.** The verify-before-commit invariant is enforced by
//!    `vtop_core::state_machine` at write time AND by a database trigger, so the
//!    DB rejects an illegal `-> source_committed` transition even if application
//!    logic is ever bypassed. A `CHECK` also constrains the state to the known
//!    enum, and startup refuses a schema whose `CHECK` predates a state this
//!    build writes (`superseded`, #567).
//! 2. **Serialization-failure retry.** Distributed Postgres-compatible stores
//!    can abort a transaction with SQLSTATE `40001`; those are transient, so the
//!    write is retried a bounded number of times.
//! 3. **Transport and credential boundary.** Parsed connection options keep the
//!    URL out of errors, and every non-loopback connection requires
//!    `sslmode=verify-full` using rustls.
//! 4. **Separate migration identity.** Runtime startup performs only a read-only
//!    schema readiness check. DDL is applied explicitly through
//!    [`PgStateStore::migrate`], so the engine role needs table DML, not schema
//!    ownership or `CREATE`/`ALTER` privileges.
//!
//! Compiled only with `--features postgres`.

use crate::models::{
    BatchPatch, BatchRecord, CommittedCursor, CommittedKafkaOffset, VerificationClass,
};
use crate::store::StateStore;
use async_trait::async_trait;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};
use sqlx::Row;
use std::net::IpAddr;
use std::str::FromStr;
use std::time::Duration;
use vtop_core::errors::VtopError;
use vtop_core::state_machine::{transition, BatchState};
use vtop_core::types::{ProgressMarker, SourceType, TelemetryFormat};

/// Idempotent DDL owned by the deployment/migration identity. Keeping it in a
/// standalone SQL file makes the exact privileged operation auditable and lets
/// operators apply the same artifact outside the engine if desired.
const MIGRATION: &str = include_str!("../migrations/postgres/0001_state_store.sql");

/// The first non-empty line of the invariant trigger's function body is
/// `-- vtop-state-schema N` (the body starts with the newline after `$fn$`,
/// hence "non-empty"). Startup requires at least this N. 2 is the first
/// body that refuses an insert in any state but batching (#625); a schema from
/// before it carries no marker. 3 is the migration that also admits
/// `superseded` in the state CHECK (#567). The marker lives in the body, not in a
/// `COMMENT ON FUNCTION`, because `CREATE OR REPLACE` replaces the body but
/// keeps a comment: an older release's migrate would leave its older body
/// under the newer comment.
const SCHEMA_MARKER_PREFIX: &str = "-- vtop-state-schema ";
const MIN_SCHEMA_VERSION: u32 = 3;
/// The newest marker this build reads: the one its own migration writes.
/// A higher marker means a newer release migrated the database for a change
/// this build could misread - the marker is bumped for nothing else - so this
/// build refuses it, at startup and in `vtopctl migrate`, rather than run on
/// it or reinstall its older trigger over it (#664). Releases before this one
/// accepted any marker at or above their minimum, and cannot be made to
/// refuse; this is the refusal path for the releases from here on.
const SCHEMA_VERSION: u32 = 3;

/// The schema version the trigger function's body opens with: the first
/// non-empty line. A marker anywhere else, or a missing or unparseable one,
/// is no marker.
fn schema_marker(function_body: Option<&str>) -> Option<u32> {
    function_body
        .and_then(|body| body.lines().map(str::trim).find(|line| !line.is_empty()))
        .and_then(|line| line.strip_prefix(SCHEMA_MARKER_PREFIX))
        .and_then(|v| v.trim().parse::<u32>().ok())
}

/// Whether the trigger function's body opens with a schema version this
/// engine accepts. A missing or older marker is an old schema.
fn schema_marker_is_current(function_body: Option<&str>) -> bool {
    schema_marker(function_body).is_some_and(|v| (MIN_SCHEMA_VERSION..=SCHEMA_VERSION).contains(&v))
}

/// The refusal for a schema a newer release migrated, or `None`.
fn newer_schema_refusal(function_body: Option<&str>) -> Option<VtopError> {
    let marker = schema_marker(function_body).filter(|v| *v > SCHEMA_VERSION)?;
    Some(VtopError::State(format!(
        "PostgreSQL state-store schema marker {marker} (`{SCHEMA_MARKER_PREFIX}{marker}` in the \
         commit-rule trigger) is newer than this release supports (minimum {MIN_SCHEMA_VERSION}, \
         newest {SCHEMA_VERSION}): a newer release has migrated this database. Nothing was \
         started or changed. Run the release that migrated it, or a later one; do not run this \
         release's `vtopctl migrate` against it"
    )))
}

/// Retry budget for transient serialization failures (SQLSTATE 40001). One
/// retry per attempt with a small backoff; a single-node Postgres essentially
/// never hits this, a distributed store occasionally does.
const MAX_SERIALIZATION_RETRIES: u32 = 5;

/// Retry budget for a lost update (the optimistic `AND state = …` guard matched
/// zero rows because another writer moved the row first). Bounded so a genuine
/// livelock cannot spin forever.
const MAX_CONFLICT_RETRIES: u32 = 5;

/// The prune predicate over a candidate row `b`, shared by the prune, the
/// candidate listing and the listed delete (#560) so the three cannot drift.
/// A row is deletable only when a newer committed row - ordered by
/// (created_at, batch_id), the order committed_cursors reads - exists for
/// its source path, so the restart cursor's row always survives (#564) - and,
/// for a Kafka row, of its partition and consumer group, where that
/// partition resumes (#614; see the SQLite predicate). The
/// successor probe's expressions must stay identical to
/// idx_batches_commit_newest in migrations/postgres/0001_state_store.sql.
/// A SUPERSEDED row ($4, #567) is deletable on age alone: it is never a
/// restart cursor.
macro_rules! pg_prune_predicate_sql {
    () => {
        "b.updated_at < $2 AND (b.state = $4 OR (b.state = $1 \
          AND EXISTS ( \
            SELECT 1 FROM batches n \
            WHERE n.state = $1 \
              AND n.source_type = b.source_type \
              AND COALESCE(n.progress_end_json::jsonb ->> 'path', n.source_name) \
                  = COALESCE(b.progress_end_json::jsonb ->> 'path', b.source_name) \
              AND (n.progress_end_json::jsonb ->> 'partition') \
                  IS NOT DISTINCT FROM (b.progress_end_json::jsonb ->> 'partition') \
              AND (n.progress_end_json::jsonb ->> 'consumer_group') \
                  IS NOT DISTINCT FROM (b.progress_end_json::jsonb ->> 'consumer_group') \
              AND (n.created_at > b.created_at \
                   OR (n.created_at = b.created_at AND n.batch_id > b.batch_id)) \
          )))"
    };
}

/// The states the `state_enum` CHECK must admit for this build to write every
/// state it can reach. `superseded` arrived with #567; a schema migrated by an
/// older release refuses it, so startup asks for `vtopctl migrate` instead of
/// failing the first retirement.
///
/// Checked on the constraint itself, not only through the schema marker
/// (version 3 since #567): the marker describes the function body a
/// migration installed, and the CHECK is a separate object. A table restored
/// on its own from an older archive, or a constraint replaced by hand, leaves
/// a current marker over an old CHECK - and an older release's migrate leaves
/// the widened CHECK under its older marker. Either half alone is refused.
const STATE_ADDED_SINCE_FIRST_SCHEMA: &str = "superseded";

/// Handle to the persistent Postgres-compatible state store.
#[derive(Clone)]
pub struct PgStateStore {
    pool: PgPool,
    /// The claim generations this process holds (#557).
    fence: crate::fence::ClaimFence,
}

fn map_sqlx(e: sqlx::Error) -> VtopError {
    VtopError::State(redact_postgres_urls(&e.to_string()))
}

/// A `(path, inode, end_byte, file_size, content, tail)` row of a cursor
/// query. The inode is read as text and parsed here so a u64 above i64::MAX
/// stays exact; a malformed stored fingerprint reads as unknown (#620).
fn committed_cursor_of(r: &sqlx::postgres::PgRow) -> Option<CommittedCursor> {
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
        content_fingerprint: content
            .as_deref()
            .and_then(crate::models::parse_content_fingerprint),
        observed_mtime: mtime.filter(|m| !m.is_empty()),
        tail_fingerprint: tail
            .as_deref()
            .and_then(crate::models::parse_content_fingerprint),
    })
}

/// Remove complete PostgreSQL URLs from any error before it can reach logs or
/// CLI output. `connect_with(PgConnectOptions)` means sqlx normally has no URL
/// string to print, but this is a defense-in-depth boundary for future errors.
fn redact_postgres_urls(message: &str) -> String {
    let mut out = message.to_owned();
    for scheme in ["postgres://", "postgresql://"] {
        while let Some(start) = out.to_ascii_lowercase().find(scheme) {
            let end = out[start..]
                .find(|c: char| c.is_whitespace() || matches!(c, '\'' | '"' | ')' | ']' | '}'))
                .map(|offset| start + offset)
                .unwrap_or(out.len());
            out.replace_range(start..end, "[REDACTED POSTGRES URL]");
        }
    }
    out
}

fn is_local_postgres(options: &PgConnectOptions) -> bool {
    if options.get_socket().is_some() || options.get_host().starts_with('/') {
        return true;
    }
    let host = options.get_host().trim_matches(|c| matches!(c, '[' | ']'));
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

fn enforce_tls_policy(options: &PgConnectOptions) -> Result<(), VtopError> {
    if is_local_postgres(options) || matches!(options.get_ssl_mode(), PgSslMode::VerifyFull) {
        return Ok(());
    }
    Err(VtopError::Config(
        "remote PostgreSQL state stores require verified TLS with sslmode=verify-full; configure a trusted root with sslrootcert when the bundled public roots do not contain the database CA"
            .into(),
    ))
}

async fn connect_pool(conn_str: &str, max_connections: u32) -> Result<PgPool, VtopError> {
    let options = PgConnectOptions::from_str(conn_str).map_err(|_| {
        VtopError::Config(
            "invalid PostgreSQL state-store connection (connection details redacted)".into(),
        )
    })?;
    enforce_tls_policy(&options)?;
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(options)
        .await
        .map_err(map_sqlx)
}

fn schema_migration_required() -> VtopError {
    VtopError::State(
        "PostgreSQL state-store schema is missing or outdated; run `vtopctl migrate --config <path>` with the privileged migration identity before starting the engine"
            .into(),
    )
}

/// [`schema_migration_required`], naming what the database reported missing
/// (a column such as `verification_class`, #664, or the table), so the
/// operator can see which release's migration has not run.
fn schema_migration_required_because(what: &str) -> VtopError {
    VtopError::State(format!(
        "PostgreSQL state-store schema is missing or outdated ({}); run `vtopctl migrate --config <path>` with the privileged migration identity before starting the engine",
        redact_postgres_urls(what)
    ))
}

fn map_schema_check(error: sqlx::Error) -> VtopError {
    let what = error
        .as_database_error()
        .map(|database_error| database_error.message().to_owned());
    match error
        .as_database_error()
        .and_then(|database_error| database_error.code())
        .as_deref()
    {
        Some("42P01" | "42703" | "42883" | "3F000") => match what {
            Some(what) => schema_migration_required_because(&what),
            None => schema_migration_required(),
        },
        Some("42501") => VtopError::State(
            "PostgreSQL runtime identity lacks schema/table access; grant USAGE on the state-store schema and SELECT, INSERT, UPDATE on batches"
                .into(),
        ),
        _ => map_sqlx(error),
    }
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
    /// Open a runtime connection pool without executing any DDL.
    ///
    /// The connected identity needs only `USAGE` on the schema and
    /// `SELECT, INSERT, UPDATE` on `batches`. Deployments must run
    /// [`Self::migrate`] first with a separate privileged identity.
    pub async fn connect(conn_str: &str) -> Result<Self, VtopError> {
        let pool = connect_pool(conn_str, 10).await?;
        let store = Self {
            pool,
            fence: Default::default(),
        };
        store.check_schema().await?;
        Ok(store)
    }

    /// Apply the PostgreSQL schema and invariant trigger with a privileged
    /// deployment identity. The engine never calls this during startup.
    pub async fn migrate(conn_str: &str) -> Result<(), VtopError> {
        let pool = connect_pool(conn_str, 1).await?;
        // A schema a newer release migrated is left alone: this release's
        // `CREATE OR REPLACE` would install its older trigger body over it
        // (#664).
        let body: Option<String> = sqlx::query_scalar(
            "SELECT p.prosrc \
             FROM pg_catalog.pg_trigger t \
             JOIN pg_catalog.pg_proc p ON p.oid = t.tgfoid \
             WHERE t.tgrelid = to_regclass('batches') \
               AND t.tgname = 'trg_commit_after_verify' \
               AND NOT t.tgisinternal",
        )
        .fetch_optional(&pool)
        .await
        .map_err(map_sqlx)?;
        if let Some(refusal) = newer_schema_refusal(body.as_deref()) {
            return Err(refusal);
        }
        // Whether this run adds the verification class column (#664) to a
        // table that already holds rows: those rows keep no class, and the
        // operator is told how many.
        let adds_class_column: bool = sqlx::query_scalar(
            "SELECT to_regclass('batches') IS NOT NULL AND NOT EXISTS ( \
                SELECT 1 FROM pg_catalog.pg_attribute \
                WHERE attrelid = to_regclass('batches') \
                  AND attname = 'verification_class' AND NOT attisdropped)",
        )
        .fetch_one(&pool)
        .await
        .map_err(map_sqlx)?;
        sqlx::raw_sql(MIGRATION)
            .execute(&pool)
            .await
            .map_err(map_sqlx)?;
        if adds_class_column {
            let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM batches")
                .fetch_one(&pool)
                .await
                .map_err(map_sqlx)?;
            crate::models::log_class_column_added("PostgreSQL", rows);
        }
        Self {
            pool,
            fence: Default::default(),
        }
        .check_schema()
        .await
    }

    /// Execute a raw SQL string against the pool. TEST-ONLY: the battery uses it
    /// to reset the table and to exercise the invariant trigger by writing an
    /// illegal transition straight to the DB (bypassing `update_batch_state`).
    /// Gated so it never exists in a production build, and so the Postgres
    /// driver stays out of the default SQLite-only test build.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn execute_raw(&self, sql: &'static str) -> Result<(), VtopError> {
        // `&'static str` both satisfies sqlx 0.9's SqlSafeStr and avoids the
        // borrowed-data-escapes error; test callers pass string literals.
        sqlx::raw_sql(sql)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }

    async fn check_schema(&self) -> Result<(), VtopError> {
        // Listing every runtime column makes an old/partial schema fail during
        // startup instead of halfway through a batch. This is a SELECT only.
        // The list every whole-row read names (`batch_columns!`).
        sqlx::query(concat!(
            "SELECT ",
            batch_columns!(),
            " FROM batches WHERE FALSE"
        ))
        .fetch_optional(&self.pool)
        .await
        .map_err(map_schema_check)?;

        // The trigger must be enabled and call a function whose body carries a
        // current schema marker, not merely exist by name: a schema restored
        // from an older archive, never re-migrated, or re-migrated by an older
        // release keeps the name with an older body, and a restore that
        // disabled the trigger may never have enabled it again.
        let body: Option<String> = sqlx::query_scalar(
            "SELECT p.prosrc \
             FROM pg_catalog.pg_trigger t \
             JOIN pg_catalog.pg_proc p ON p.oid = t.tgfoid \
             WHERE t.tgrelid = 'batches'::regclass \
               AND t.tgname = 'trg_commit_after_verify' \
               AND NOT t.tgisinternal \
               AND t.tgenabled IN ('O', 'A')",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(map_schema_check)?;
        if let Some(refusal) = newer_schema_refusal(body.as_deref()) {
            return Err(refusal);
        }
        if !schema_marker_is_current(body.as_deref()) {
            return Err(schema_migration_required());
        }

        // The state CHECK must admit every state this build writes (#567),
        // under a current marker or not: see STATE_ADDED_SINCE_FIRST_SCHEMA.
        // Without it the first retirement would fail at runtime; refuse to
        // start instead, with the same guidance as any other outdated schema.
        let admits_every_state: bool = sqlx::query_scalar(
            "SELECT EXISTS ( \
                SELECT 1 FROM pg_catalog.pg_constraint \
                WHERE conrelid = 'batches'::regclass \
                  AND conname = 'state_enum' \
                  AND contype = 'c' \
                  AND pg_catalog.pg_get_constraintdef(oid) LIKE '%''' || $1 || '''%' \
             )",
        )
        .bind(STATE_ADDED_SINCE_FIRST_SCHEMA)
        .fetch_one(&self.pool)
        .await
        .map_err(map_schema_check)?;
        if !admits_every_state {
            return Err(schema_migration_required());
        }
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
        crate::store::require_batching_insert(rec)?;
        let ps = serde_json::to_string(&rec.progress_start)?;
        let pe = serde_json::to_string(&rec.progress_end)?;
        // Plain INSERT: a duplicate batch_id must fail on the primary key, never
        // overwrite a possibly-committed row.
        with_retry(|| {
            sqlx::query(
                r#"INSERT INTO batches
                   (batch_id, tenant, source_type, source_name, format, state,
                    progress_start_json, progress_end_json, object_uri, manifest_uri,
                    object_sha256, manifest_sha256, manifest_version_id, object_size_bytes,
                    record_count, error_message, owner, lease_expires_at, created_at, updated_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)"#,
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
            .bind(&rec.manifest_version_id)
            .bind(rec.object_size_bytes)
            .bind(rec.record_count)
            .bind(&rec.error_message)
            .bind(&rec.owner)
            .bind(&rec.lease_expires_at)
            .bind(crate::store::canonical_created_at(&rec.created_at))
            .bind(&rec.updated_at)
            .execute(&self.pool)
        })
        .await?;
        self.fence.created(&rec.batch_id);
        Ok(())
    }

    async fn update_batch_state(
        &self,
        batch_id: &str,
        to: BatchState,
        patch: &BatchPatch,
    ) -> Result<(), VtopError> {
        // Read -> validate -> conditional write, retried on a lost update.
        //
        // The UPDATE only applies if the row is STILL in the state we validated
        // against (`AND state = $11`). In an HA fleet two instances could both
        // read the same state, both validate, and both write; the guard makes
        // the loser's UPDATE affect zero rows. Rather than surface that as a hard
        // error to the orchestrator (which does not retry), re-read and
        // re-validate: if the batch's new state still permits the intended
        // transition, retry the write transparently; if it no longer does (e.g.
        // another writer already advanced it), `transition` returns the correct
        // error on the next pass. A genuinely illegal transition therefore still
        // fails immediately - only a lost update loops. The 40001 serialization
        // retry sits inside, on the write itself.
        //
        // The write is ALSO conditional on the claim generation this process
        // holds (#557): the state guard alone lets an engine whose lease
        // lapsed write over the engine that claimed the row, because state
        // names recur. A lost claim is not a lost update — retrying cannot
        // help — so it is reported at once. A row this process holds no
        // generation for is refused (see the fence module).
        crate::store::require_class_only_at_verified(batch_id, to, patch)?;
        let held = self.fence.held(batch_id);
        let mut attempt = 0;
        loop {
            let current = self.get_state(batch_id).await?;
            let validated = transition(current, to)?;
            let held = held.ok_or_else(|| crate::fence::not_held(batch_id))?;
            let now = chrono::Utc::now().to_rfc3339();
            let expected = current.as_str();
            let rows = with_retry(|| {
                sqlx::query(
                    r#"UPDATE batches SET
                         state = $1,
                         object_uri = COALESCE($2, object_uri),
                         manifest_uri = COALESCE($3, manifest_uri),
                         object_sha256 = COALESCE($4, object_sha256),
                         manifest_sha256 = COALESCE($5, manifest_sha256),
                         manifest_version_id = COALESCE($6, manifest_version_id),
                         object_size_bytes = COALESCE($7, object_size_bytes),
                         record_count = COALESCE($8, record_count),
                         error_message = COALESCE($9, error_message),
                         updated_at = $10,
                         verification_class = CASE WHEN $14 THEN $15 ELSE verification_class END
                       WHERE batch_id = $11 AND state = $12 AND claim_gen = $13"#,
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
                .bind(&now)
                .bind(batch_id)
                .bind(expected)
                .bind(held)
                // As SQLite: the transition to VERIFIED writes the class it
                // was given, NULL included (#664).
                .bind(validated == BatchState::Verified)
                .bind(patch.verification_class.map(VerificationClass::as_str))
                .execute(&self.pool)
            })
            .await?
            .rows_affected();

            if rows >= 1 {
                self.fence.wrote(batch_id, validated, held);
                return Ok(());
            }
            let actual: Option<i64> =
                sqlx::query_scalar("SELECT claim_gen FROM batches WHERE batch_id = $1")
                    .bind(batch_id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(map_sqlx)?;
            if let Some(actual) = actual.filter(|actual| *actual != held) {
                self.fence.lost(batch_id, held);
                return Err(crate::fence::claim_lost(batch_id, held, actual));
            }
            // Lost update: the row's state changed between our read and write.
            attempt += 1;
            if attempt > MAX_CONFLICT_RETRIES {
                return Err(VtopError::State(format!(
                    "persistent concurrent state change for batch {batch_id} after {attempt} attempts"
                )));
            }
            tokio::time::sleep(Duration::from_millis(5 * attempt as u64)).await;
        }
    }

    async fn get_batch(&self, batch_id: &str) -> Result<Option<BatchRecord>, VtopError> {
        let row = sqlx::query(concat!(
            "SELECT ",
            batch_columns!(),
            " FROM batches WHERE batch_id = $1"
        ))
        .bind(batch_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.map(row_to_record).transpose()
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
        // See the SQLite implementation (#624).
        let renewed = with_retry(|| {
            sqlx::query(
                "UPDATE batches SET lease_expires_at = $1 WHERE batch_id = $2 AND claim_gen = $3",
            )
            .bind(&lease_until)
            .bind(batch_id)
            .bind(held)
            .execute(&self.pool)
        })
        .await?
        .rows_affected();
        Ok(renewed == 1)
    }

    async fn claim_incomplete_batches(
        &self,
        owner: &str,
        now: &str,
        lease_until: &str,
    ) -> Result<Vec<BatchRecord>, VtopError> {
        // ONE atomic statement claims (see the SQLite impl for the invariant
        // argument); instants are normalized to canonical UTC because the
        // comparison is textual. Routed through with_retry like every other
        // write: concurrent fleet recoveries must not fail startup on a
        // transient serialization error (SQLSTATE 40001).
        let now = crate::store::normalize_rfc3339_utc("now", now)?;
        let lease_until = crate::store::normalize_rfc3339_utc("lease_until", lease_until)?;
        let (now, lease_until) = (now.as_str(), lease_until.as_str());
        // RETURNING gives exactly the rows THIS statement claimed, with the
        // generation it set (see the SQLite implementation and #557).
        let claimed = with_retry(|| {
            sqlx::query(concat!(
                "UPDATE batches SET owner = $1, lease_expires_at = $2, updated_at = $3, \
                        claim_gen = claim_gen + 1 \
                 WHERE state NOT IN ($4, $5) \
                   AND (owner IS NULL OR owner = $1 OR lease_expires_at IS NULL OR lease_expires_at <= $3) \
                 RETURNING ",
                batch_columns!()
            ))
            .bind(owner)
            .bind(lease_until)
            .bind(now)
            .bind(BatchState::SourceCommitted.as_str())
            .bind(BatchState::Superseded.as_str())
            .fetch_all(&self.pool)
        })
        .await?;
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
        // Aggregated in SQL — see the SQLite impl and #77. The column is TEXT,
        // so cast to jsonb; markers are internally tagged (fields at top level).
        let rows = sqlx::query(
            "SELECT progress_end_json::jsonb ->> 'path' AS path, \
                    MAX((progress_end_json::jsonb ->> 'end_byte')::bigint) AS end_byte \
             FROM batches WHERE state = $1 AND source_type = $2 \
             GROUP BY progress_end_json::jsonb ->> 'path'",
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
        // See the SQLite implementation and #77: the newest committed row per
        // path, by (created_at, batch_id) - stored fixed-width, so text order
        // is time order, and the order pruning keeps - with its own end byte.
        // The markers column is TEXT, so cast to jsonb; markers are internally
        // tagged (fields at top level). The inode is read as text and parsed
        // here so a u64 above i64::MAX stays exact.
        let rows = sqlx::query(
            "WITH committed AS ( \
                 SELECT progress_end_json::jsonb ->> 'path' AS path, \
                        progress_end_json::jsonb ->> 'inode' AS inode, \
                        (progress_end_json::jsonb ->> 'end_byte')::bigint AS end_byte, \
                        (progress_end_json::jsonb ->> 'file_size')::bigint AS file_size, \
                        progress_end_json::jsonb ->> 'content_fingerprint' AS content, \
                        progress_end_json::jsonb ->> 'tail_fingerprint' AS tail, \
                        progress_end_json::jsonb ->> 'mtime' AS mtime, \
                        created_at, batch_id \
                 FROM batches WHERE state = $1 AND source_type = $2 \
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
        // See the SQLite implementation (#621): `committed_cursors`,
        // partitioned by path and inode instead of by path.
        let rows = sqlx::query(
            "WITH committed AS ( \
                 SELECT progress_end_json::jsonb ->> 'path' AS path, \
                        progress_end_json::jsonb ->> 'inode' AS inode, \
                        (progress_end_json::jsonb ->> 'end_byte')::bigint AS end_byte, \
                        (progress_end_json::jsonb ->> 'file_size')::bigint AS file_size, \
                        progress_end_json::jsonb ->> 'content_fingerprint' AS content, \
                        progress_end_json::jsonb ->> 'tail_fingerprint' AS tail, \
                        progress_end_json::jsonb ->> 'mtime' AS mtime, \
                        created_at, batch_id \
                 FROM batches WHERE state = $1 AND source_type = $2 \
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

    async fn committed_kafka_offsets(&self) -> Result<Vec<CommittedKafkaOffset>, VtopError> {
        // See the SQLite implementation: the newest committed row per (topic,
        // partition, consumer group), in the order pruning keeps.
        let rows = sqlx::query(
            "WITH committed AS ( \
                 SELECT progress_end_json::jsonb ->> 'topic' AS topic, \
                        (progress_end_json::jsonb ->> 'partition')::bigint AS part, \
                        progress_end_json::jsonb ->> 'consumer_group' AS grp, \
                        (progress_end_json::jsonb ->> 'end_offset')::bigint AS end_offset, \
                        created_at, batch_id \
                 FROM batches WHERE state = $1 AND source_type = $2 \
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

    async fn list_incomplete_batches(&self) -> Result<Vec<BatchRecord>, VtopError> {
        let rows = sqlx::query(concat!(
            "SELECT ",
            batch_columns!(),
            " FROM batches WHERE state NOT IN ($1, $2) ORDER BY created_at ASC"
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
                " FROM batches WHERE state = $1 ORDER BY created_at ASC"
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
        // Same predicate as the SQLite store: a row is deletable only when a
        // newer committed row — ordered by (created_at, batch_id), the order
        // committed_cursors reads — exists for its source path, so the
        // restart cursor's row always survives (review of #564). The
        // successor probe's expressions must stay identical to
        // idx_batches_commit_newest in migrations/postgres/0001_state_store.sql,
        // which bounds it.
        let result = with_retry(|| {
            sqlx::query(concat!(
                "DELETE FROM batches WHERE batch_id IN ( \
                    SELECT b.batch_id FROM batches b WHERE ",
                pg_prune_predicate_sql!(),
                " LIMIT $3 )"
            ))
            .bind(BatchState::SourceCommitted.as_str())
            .bind(older_than)
            .bind(i64::from(limit))
            .bind(BatchState::Superseded.as_str())
            .execute(&self.pool)
        })
        .await?;
        Ok(result.rows_affected())
    }

    async fn prune_candidates(
        &self,
        older_than: &str,
        limit: u32,
    ) -> Result<Vec<BatchRecord>, VtopError> {
        let rows = with_retry(|| {
            sqlx::query(concat!(
                "SELECT ",
                batch_columns!(),
                " FROM batches WHERE batch_id IN ( \
                    SELECT b.batch_id FROM batches b WHERE ",
                pg_prune_predicate_sql!(),
                " LIMIT $3 ) ORDER BY created_at, batch_id"
            ))
            .bind(BatchState::SourceCommitted.as_str())
            .bind(older_than)
            .bind(i64::from(limit))
            .bind(BatchState::Superseded.as_str())
            .fetch_all(&self.pool)
        })
        .await?;
        rows.into_iter().map(row_to_record).collect()
    }

    async fn prune_listed(&self, older_than: &str, batch_ids: &[String]) -> Result<u64, VtopError> {
        if batch_ids.is_empty() {
            return Ok(0);
        }
        let result = with_retry(|| {
            sqlx::query(concat!(
                "DELETE FROM batches WHERE batch_id IN ( \
                    SELECT b.batch_id FROM batches b \
                    WHERE b.batch_id = ANY($3) AND ",
                pg_prune_predicate_sql!(),
                " )"
            ))
            .bind(BatchState::SourceCommitted.as_str())
            .bind(older_than)
            .bind(batch_ids)
            .bind(BatchState::Superseded.as_str())
            .execute(&self.pool)
        })
        .await?;
        Ok(result.rows_affected())
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
mod security_tests {
    use super::*;

    fn options(url: &str) -> PgConnectOptions {
        PgConnectOptions::from_str(url).unwrap()
    }

    #[test]
    fn only_a_current_schema_marker_passes_startup() {
        let body = |marker: &str| format!("\n{marker}\nBEGIN\n    RETURN NEW;\nEND;\n");
        for refused in [
            "",
            "-- vtop-state-schema",
            "-- vtop-state-schema x",
            "-- vtop-state-schema 1",
            // #625's marker: its schema predates the #567 state CHECK.
            "-- vtop-state-schema 2",
            "vtop-state-schema 3",
            "-- must be inserted as batching",
        ] {
            assert!(
                !schema_marker_is_current(Some(&body(refused))),
                "{refused:?}"
            );
        }
        assert!(!schema_marker_is_current(None));
        // Only the first non-empty line counts.
        assert!(!schema_marker_is_current(Some(
            "\nBEGIN\n-- vtop-state-schema 3\n    RETURN NEW;\nEND;\n"
        )));
        assert!(!schema_marker_is_current(Some(
            "\n-- an earlier comment\n-- vtop-state-schema 3\nBEGIN\nEND;\n"
        )));
        assert!(schema_marker_is_current(Some(&body(
            "-- vtop-state-schema 3"
        ))));
        // A newer release's marker is not current: it is refused as newer,
        // naming the marker and the versions this release reads (#664).
        assert!(!schema_marker_is_current(Some(&body(
            "    -- vtop-state-schema 4"
        ))));
        let refusal = newer_schema_refusal(Some(&body("-- vtop-state-schema 4")))
            .expect("a newer marker is refused")
            .to_string();
        for part in [
            "schema marker 4",
            "-- vtop-state-schema 4",
            "minimum 3",
            "newest 3",
        ] {
            assert!(refusal.contains(part), "{part:?} missing from: {refusal}");
        }
        for not_newer in ["-- vtop-state-schema 3", "-- vtop-state-schema 2", ""] {
            assert!(newer_schema_refusal(Some(&body(not_newer))).is_none());
        }
    }

    #[test]
    fn remote_postgres_requires_hostname_verified_tls() {
        for url in [
            "postgres://vtop:secret@db.example/vtop",
            "postgres://vtop:secret@db.example/vtop?sslmode=disable",
            "postgres://vtop:secret@db.example/vtop?sslmode=require",
            "postgres://vtop:secret@db.example/vtop?sslmode=verify-ca",
        ] {
            let err = enforce_tls_policy(&options(url)).unwrap_err().to_string();
            assert!(err.contains("sslmode=verify-full"), "{url}: {err}");
            assert!(!err.contains("secret"));
        }

        enforce_tls_policy(&options(
            "postgres://vtop:secret@db.example/vtop?sslmode=verify-full",
        ))
        .unwrap();
    }

    #[test]
    fn loopback_postgres_may_use_plaintext_for_local_development() {
        for url in [
            "postgres://vtop:secret@localhost/vtop?sslmode=disable",
            "postgres://vtop:secret@127.0.0.1/vtop?sslmode=disable",
            "postgres://vtop:secret@[::1]/vtop?sslmode=disable",
        ] {
            enforce_tls_policy(&options(url)).unwrap();
        }
    }

    #[test]
    fn postgres_urls_are_removed_from_errors() {
        let secret = "highly-sensitive-password";
        let message = format!(
            "connection to postgres://vtop:{secret}@db.example/vtop?sslmode=disable failed; fallback postgresql://other:{secret}@db2/vtop refused"
        );
        let redacted = redact_postgres_urls(&message);
        assert!(!redacted.contains(secret));
        assert!(!redacted.contains("vtop:"));
        assert_eq!(redacted.matches("[REDACTED POSTGRES URL]").count(), 2);
    }
}

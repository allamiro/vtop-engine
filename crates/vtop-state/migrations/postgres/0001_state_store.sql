BEGIN;

-- Refuse a schema a newer release migrated (#664): its marker (see the
-- trigger function below) is above the one this migration installs, and
-- CREATE OR REPLACE would put this release's older body over it. In this
-- transaction, under an advisory lock every migration from #664 on takes,
-- so two of them cannot interleave the check and the replace. `vtopctl
-- migrate` checks first for a clearer message; this guard also covers the
-- SQL applied by an external migration system. Keep 3 equal to the marker
-- below and SCHEMA_VERSION in pg_store.rs.
SELECT pg_advisory_xact_lock(hashtext('vtop-state-schema'));
DO $newer$
DECLARE
    marker integer;
BEGIN
    SELECT substring(p.prosrc FROM '^\s*-- vtop-state-schema ([0-9]+)')::integer
      INTO marker
      FROM pg_catalog.pg_proc p
     WHERE p.proname = 'vtop_enforce_commit_after_verify'
       AND p.pronamespace = current_schema()::regnamespace;
    IF marker > 3 THEN
        RAISE EXCEPTION 'state-store schema marker % (-- vtop-state-schema %) is newer than this migration (3): a newer release has migrated this database; nothing was changed. Run the release that migrated it, or a later one', marker, marker
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
END
$newer$;

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
    object_size_bytes BIGINT,
    record_count BIGINT,
    error_message TEXT,
    owner TEXT,
    lease_expires_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CONSTRAINT state_enum CHECK (state IN (
        'discovered','batching','sealed','compressed','checksummed',
        'object_uploaded','manifest_uploaded','verified','source_committed',
        'failed','replay_required','superseded'))
);

-- Upgrade databases created before ownership leases and object sizes shipped.
ALTER TABLE batches ADD COLUMN IF NOT EXISTS owner TEXT;
ALTER TABLE batches ADD COLUMN IF NOT EXISTS lease_expires_at TEXT;
ALTER TABLE batches ADD COLUMN IF NOT EXISTS object_size_bytes BIGINT;
ALTER TABLE batches ADD COLUMN IF NOT EXISTS manifest_version_id TEXT;
-- The claim fence (#557): every claim bumps it, and a writer that holds an older
-- generation is refused. Nullable-free with a default so rows written by older
-- builds, which never name the column, read as generation 0.
--
-- The fence binds only engines that know it. A build from before #557 neither
-- bumps nor checks claim_gen, so while one is still pointed at this database a
-- stalled old engine can still overwrite a new claimant: drain every older
-- engine before relying on it.
ALTER TABLE batches ADD COLUMN IF NOT EXISTS claim_gen BIGINT NOT NULL DEFAULT 0;
-- The class of verification a VERIFIED row passed (#664): 'strong' or
-- 'size_only', written with the transition to verified. Nullable, no
-- default, no CHECK: NULL is every row from before it, and a value an engine
-- does not know (a later release's) reads as unknown, which is decided as a
-- NULL is, never refused. Engine startup names this column
-- (PgStateStore::check_schema), so engines of this release refuse a schema
-- without it and ask for `vtopctl migrate`.
--
-- Older engines keep working on the column once their connections are new:
-- they map rows by column name, insert by an explicit column list, and never
-- write it, so a row an older engine verifies keeps NULL. But engines before
-- #664 read rows with `SELECT *` / `RETURNING *` through statements cached
-- per connection, and after this ALTER Postgres refuses those ("cached plan
-- must not change result type") until the connection is replaced. A running
-- engine reads whole rows only in startup recovery and when it settles a
-- VERIFIED row whose source commit failed; that read fails and the row waits
-- for the next restart. Restart every older engine right after this
-- migration (or drain them first).
-- Engines of this release name their columns, so a column a later migration
-- adds does not affect them. The class decides only which counter a
-- recovered commit is counted in; whether the row commits is still decided
-- by the re-check, in every release. No marker bump: see the marker note
-- below.
ALTER TABLE batches ADD COLUMN IF NOT EXISTS verification_class TEXT;

-- SUPERSEDED (#567): the terminal state of a handed-back range its source has
-- since committed past. A table created before it carries the CHECK above
-- without it, and CREATE TABLE IF NOT EXISTS does not touch an existing
-- table, so the constraint is replaced - only when it lacks the state, so a
-- re-run of migrate neither rescans the table nor takes its lock again.
-- Engine startup refuses a CHECK without 'superseded' (PgStateStore::
-- check_schema), so engines of this release need this migration first.
--
-- Older engines keep working on the widened CHECK: they never write the
-- state. One that READS a superseded row fails closed - its recovery claim
-- and `vtopctl status` stop with "unknown batch state: superseded" - so once
-- an engine of this release has run, an older engine on the same ledger
-- refuses to start rather than misreading the row. Drain older engines first.
DO $sup$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint
        WHERE conrelid = 'batches'::regclass
          AND conname = 'state_enum'
          AND contype = 'c'
          AND pg_catalog.pg_get_constraintdef(oid) LIKE '%''superseded''%'
    ) THEN
        ALTER TABLE batches DROP CONSTRAINT IF EXISTS state_enum;
        ALTER TABLE batches ADD CONSTRAINT state_enum CHECK (state IN (
            'discovered','batching','sealed','compressed','checksummed',
            'object_uploaded','manifest_uploaded','verified','source_committed',
            'failed','replay_required','superseded'));
    END IF;
END
$sup$;

CREATE INDEX IF NOT EXISTS idx_batches_state ON batches(state);
CREATE INDEX IF NOT EXISTS idx_batches_source ON batches(source_type, source_name);

-- Bounds the successor probe in prune_committed_history: each candidate row
-- finds a newer committed row for its path by a range seek on created_at. The
-- expressions must stay identical to the ones in the prune query. It
-- supersedes idx_batches_commit_cursor, which ordered by end_byte and so kept
-- the old content's row after a rotation or copytruncate (#564).
--
-- The old index is KEPT (review): a runtime rolled back to a release before
-- #564 runs on this schema, and its prune query seeks by end_byte through
-- that index - without it, every candidate rescans its whole path. Drop it
-- in the first release that no longer supports rolling back past #564.
CREATE INDEX IF NOT EXISTS idx_batches_commit_cursor ON batches(
    state,
    source_type,
    (COALESCE(progress_end_json::jsonb ->> 'path', source_name)),
    (COALESCE((progress_end_json::jsonb ->> 'end_byte')::bigint, -1))
);
CREATE INDEX IF NOT EXISTS idx_batches_commit_newest ON batches(
    state,
    source_type,
    (COALESCE(progress_end_json::jsonb ->> 'path', source_name)),
    created_at,
    batch_id
);

-- Database-level backstop for the verify-before-source-commit invariant.
--
-- A row is also born only as batching (#625): born verified, it could be
-- committed without ever having been verified. Every engine release inserts
-- only batching, so an older binary on an upgraded database is unaffected.
-- CREATE OR REPLACE means re-running `vtopctl migrate` installs the new body.
--
-- Schema version marker: the first non-empty line of the body, the line right
-- after `$fn$` (the body itself starts with that line's newline). Keep it
-- there: engine startup (PgStateStore::check_schema) reads only that line,
-- and refuses a trigger function whose body does not open with a marker of
-- at least MIN_SCHEMA_VERSION, so a schema restored from an older
-- archive, never re-migrated, or re-migrated by an older release (whose
-- CREATE OR REPLACE installs its own, unmarked or lower-marked body) is
-- reported as needing `vtopctl migrate`. It is in the body, not a COMMENT ON
-- FUNCTION, because CREATE OR REPLACE keeps a comment. Version 2 is the first
-- with the #625 insert check; bodies before it carry no marker. Version 3 is
-- the migration that also widens the state_enum CHECK to admit 'superseded'
-- (#567, above): the body is unchanged, but the marker names the schema this
-- migration leaves, so an older release's migrate - which installs its own
-- marker-2 body and leaves the CHECK as it found it - is refused. Startup
-- checks the CHECK itself as well, so a CHECK replaced or restored on its own
-- is refused under a current marker too. ANY change to the trigger or its
-- function, or to the schema this migration leaves that an engine could
-- misread or an older release's migrate would undo, MUST bump this number,
-- SCHEMA_VERSION and (when older schemas stop being readable)
-- MIN_SCHEMA_VERSION together. A new nullable column that startup names
-- (batch_columns!) and that older engines never name needs no bump: startup
-- already refuses a schema without it, and an older release's migrate never
-- drops it. The verification_class column (#664) is such a change. From #664
-- on, engines and migrate refuse a marker above their SCHEMA_VERSION (a newer
-- release's); releases before it accept every marker at or above their
-- minimum, which is why the marker is bumped only for what they would
-- misread.
CREATE OR REPLACE FUNCTION vtop_enforce_commit_after_verify() RETURNS trigger AS $fn$
-- vtop-state-schema 3
BEGIN
    IF NEW.state = 'source_committed' THEN
        IF TG_OP = 'INSERT' THEN
            RAISE EXCEPTION 'commit before verified: batch % inserted directly as source_committed', NEW.batch_id
                USING ERRCODE = 'check_violation';
        ELSIF OLD.state <> 'verified' THEN
            RAISE EXCEPTION 'commit before verified: batch % is %', OLD.batch_id, OLD.state
                USING ERRCODE = 'check_violation';
        END IF;
    END IF;
    IF TG_OP = 'INSERT' AND NEW.state <> 'batching' THEN
        RAISE EXCEPTION 'batch % must be inserted as batching, not %', NEW.batch_id, NEW.state
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$fn$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_commit_after_verify ON batches;
CREATE TRIGGER trg_commit_after_verify BEFORE INSERT OR UPDATE ON batches
    FOR EACH ROW EXECUTE FUNCTION vtop_enforce_commit_after_verify();

-- The audit trail (#694): one row per audit record of a ledger command
-- (prune-ledger, replay, migrate), append only and hash-chained (see
-- crates/vtop-state/src/audit.rs). A new table, so no marker bump: an older
-- release never reads it, and engines of this release refuse a schema
-- without it at startup (PgStateStore::check_schema names it). The runtime
-- role needs SELECT, INSERT on it and USAGE on its sequence, nothing else:
--
--   GRANT SELECT, INSERT ON TABLE audit_events TO <runtime role>;
--   GRANT USAGE ON SEQUENCE audit_events_seq_seq TO <runtime role>;
CREATE TABLE IF NOT EXISTS audit_events (
    seq BIGSERIAL PRIMARY KEY,
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

-- Append only as a database property, not only a grant: UPDATE, DELETE and
-- TRUNCATE are refused for every role but the table's owner (the migration
-- identity), so a maintenance role granted DELETE for prune-ledger still
-- cannot remove an audit row.
CREATE OR REPLACE FUNCTION vtop_audit_events_append_only() RETURNS trigger AS $fn$
BEGIN
    IF current_user <> (SELECT pg_catalog.pg_get_userbyid(c.relowner)
                          FROM pg_catalog.pg_class c WHERE c.oid = TG_RELID) THEN
        RAISE EXCEPTION 'audit_events is append-only: % by % refused', TG_OP, current_user
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    IF TG_LEVEL = 'STATEMENT' THEN
        RETURN NULL;
    ELSIF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$fn$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_audit_events_append_only ON audit_events;
CREATE TRIGGER trg_audit_events_append_only BEFORE UPDATE OR DELETE ON audit_events
    FOR EACH ROW EXECUTE FUNCTION vtop_audit_events_append_only();
DROP TRIGGER IF EXISTS trg_audit_events_no_truncate ON audit_events;
CREATE TRIGGER trg_audit_events_no_truncate BEFORE TRUNCATE ON audit_events
    FOR EACH STATEMENT EXECUTE FUNCTION vtop_audit_events_append_only();

COMMIT;

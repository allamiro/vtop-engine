BEGIN;

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
        'failed','replay_required'))
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
CREATE OR REPLACE FUNCTION vtop_enforce_commit_after_verify() RETURNS trigger AS $fn$
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
    RETURN NEW;
END;
$fn$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_commit_after_verify ON batches;
CREATE TRIGGER trg_commit_after_verify BEFORE INSERT OR UPDATE ON batches
    FOR EACH ROW EXECUTE FUNCTION vtop_enforce_commit_after_verify();

COMMIT;

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

CREATE INDEX IF NOT EXISTS idx_batches_state ON batches(state);
CREATE INDEX IF NOT EXISTS idx_batches_source ON batches(source_type, source_name);

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

//! The Postgres backend must pass the SAME behavioural contract as SQLite, its
//! database-level invariant trigger must reject a bypass, and its runtime role
//! must work without any DDL/table-owner privileges.
//!
//! Runs only when `VTOP_TEST_POSTGRES` points at a reachable Postgres (the CI
//! `postgres backend (battery)` job sets it; locally, point it at a throwaway
//! container). Without it the test skips rather than fails, so a plain
//! `cargo test` on a dev box needs no database.
//!
//! Uses only `PgStateStore`'s (feature-gated) API — no direct `sqlx` dependency
//! — so the Postgres driver compiles only with `--features postgres`, never in
//! the default SQLite-only test build.
//!
//! Build with `--features postgres,test-support`.

use tracing::instrument::WithSubscriber;
use vtop_state::pg_store::PgStateStore;

/// Log lines captured as JSON, to assert the messages an operator reads.
#[derive(Clone, Default)]
struct Captured(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl Captured {
    fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync {
        tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::INFO)
            .with_writer(self.clone())
            .finish()
    }
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl std::io::Write for Captured {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
use vtop_state::test_battery;
use vtop_state::StateStore;

#[tokio::test]
async fn postgres_backend_passes_battery_and_enforces_invariant_at_db() {
    let Ok(migrator_conn) = std::env::var("VTOP_TEST_POSTGRES") else {
        eprintln!("VTOP_TEST_POSTGRES not set; skipping the Postgres backend test");
        return;
    };
    let runtime_conn = std::env::var("VTOP_TEST_POSTGRES_RUNTIME").expect(
        "VTOP_TEST_POSTGRES_RUNTIME must name the narrow runtime role when the Postgres test runs",
    );

    // ---- 1. Runtime startup never repairs or creates a schema ----
    PgStateStore::migrate(&migrator_conn).await.unwrap();
    let admin = PgStateStore::connect(&migrator_conn).await.unwrap();
    admin
        .execute_raw(
            "DROP TABLE batches CASCADE; \
             DROP FUNCTION IF EXISTS vtop_enforce_commit_after_verify() CASCADE;",
        )
        .await
        .unwrap();
    let err = match PgStateStore::connect(&migrator_conn).await {
        Ok(_) => panic!("runtime connect must not create a missing schema"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("vtopctl migrate"),
        "missing schema must give migration guidance: {err}"
    );

    // A table without the database invariant is also not runtime-ready.
    PgStateStore::migrate(&migrator_conn).await.unwrap();
    let admin = PgStateStore::connect(&migrator_conn).await.unwrap();
    admin
        .execute_raw("DROP TRIGGER trg_commit_after_verify ON batches")
        .await
        .unwrap();
    let err = match PgStateStore::connect(&migrator_conn).await {
        Ok(_) => panic!("runtime connect must reject a missing invariant trigger"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("vtopctl migrate"),
        "missing trigger must give migration guidance: {err}"
    );

    // #567: a state CHECK without SUPERSEDED is not runtime-ready either, even
    // under the current schema marker: the first retirement would fail
    // at runtime. `vtopctl migrate` widens the CHECK in place, keeping the
    // rows, and a second run leaves the constraint alone.
    PgStateStore::migrate(&migrator_conn).await.unwrap();
    let admin = PgStateStore::connect(&migrator_conn).await.unwrap();
    admin
        .execute_raw(
            "TRUNCATE batches; \
             INSERT INTO batches \
             (batch_id, tenant, source_type, source_name, format, state, \
              progress_start_json, progress_end_json, created_at, updated_at) \
             VALUES ('pre-567','default','kafka','app','cef','batching','{}','{}','t','t'); \
             ALTER TABLE batches DROP CONSTRAINT state_enum; \
             ALTER TABLE batches ADD CONSTRAINT state_enum CHECK (state IN ( \
                 'discovered','batching','sealed','compressed','checksummed', \
                 'object_uploaded','manifest_uploaded','verified','source_committed', \
                 'failed','replay_required'));",
        )
        .await
        .unwrap();
    let err = match PgStateStore::connect(&migrator_conn).await {
        Ok(_) => panic!("runtime connect must reject a state CHECK without 'superseded'"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("vtopctl migrate"),
        "a pre-#567 state CHECK must give migration guidance: {err}"
    );
    PgStateStore::migrate(&migrator_conn).await.unwrap();
    let admin = PgStateStore::connect(&migrator_conn)
        .await
        .expect("migrate widens the state CHECK");
    admin
        .execute_raw(
            "DROP TABLE IF EXISTS vtop_test_state_enum; \
             CREATE TABLE vtop_test_state_enum AS \
               SELECT oid FROM pg_catalog.pg_constraint \
               WHERE conrelid = 'batches'::regclass AND conname = 'state_enum'; \
             UPDATE batches SET state = 'superseded' WHERE batch_id = 'pre-567'; \
             DO $chk$ BEGIN \
               IF (SELECT state FROM batches WHERE batch_id = 'pre-567') IS DISTINCT FROM 'superseded' THEN \
                 RAISE EXCEPTION 'the migration lost the row, or the CHECK refused superseded'; \
               END IF; \
             END $chk$;",
        )
        .await
        .unwrap();
    let err = admin
        .execute_raw("UPDATE batches SET state = 'not-a-state' WHERE batch_id = 'pre-567'")
        .await
        .expect_err("the widened CHECK still refuses an unknown state");
    assert!(
        err.to_string().contains("state_enum"),
        "expected the state CHECK to refuse it, got: {err}"
    );
    PgStateStore::migrate(&migrator_conn).await.unwrap();
    admin
        .execute_raw(
            "DO $chk$ BEGIN \
               IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint c \
                              JOIN vtop_test_state_enum t ON t.oid = c.oid \
                              WHERE c.conname = 'state_enum') THEN \
                 RAISE EXCEPTION 'a second migrate replaced a CHECK that already admits superseded'; \
               END IF; \
             END $chk$; \
             DROP TABLE vtop_test_state_enum;",
        )
        .await
        .unwrap();

    // #664: a schema from before the verification class lacks its column,
    // and holds rows in every state a ledger has. Startup names every column
    // it reads, so it refuses the schema with the migrate guidance, naming
    // the column; `vtopctl migrate` adds the column, says how many rows it
    // left without a class, and every row reads back as it was, with no class
    // (unknown), as SQLite reads one.
    PgStateStore::migrate(&migrator_conn).await.unwrap();
    let admin = PgStateStore::connect(&migrator_conn).await.unwrap();
    admin
        .execute_raw(
            "TRUNCATE batches; \
             INSERT INTO batches \
             (batch_id, tenant, source_type, source_name, format, state, \
              progress_start_json, progress_end_json, created_at, updated_at) \
             SELECT id, 'default', 'kafka', 'app', 'cef', 'batching', \
              '{\"source_type\":\"kafka\",\"topic\":\"app\",\"partition\":0,\"start_offset\":0,\"end_offset\":1,\"consumer_group\":\"g\"}', \
              '{\"source_type\":\"kafka\",\"topic\":\"app\",\"partition\":0,\"start_offset\":0,\"end_offset\":1,\"consumer_group\":\"g\"}', \
              't', 't' \
             FROM unnest(ARRAY['pre-664','pre-664-committed','pre-664-failed','pre-664-batching']) AS id; \
             UPDATE batches SET state = 'verified' WHERE batch_id IN ('pre-664', 'pre-664-committed'); \
             UPDATE batches SET state = 'source_committed' WHERE batch_id = 'pre-664-committed'; \
             UPDATE batches SET state = 'failed', error_message = 'upload failed' WHERE batch_id = 'pre-664-failed'; \
             ALTER TABLE batches DROP COLUMN verification_class;",
        )
        .await
        .unwrap();
    let err = match PgStateStore::connect(&migrator_conn).await {
        Ok(_) => panic!("runtime connect must reject a schema without verification_class"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("vtopctl migrate") && err.contains("verification_class"),
        "a pre-#664 schema must give migration guidance naming the column: {err}"
    );
    let logs = Captured::default();
    PgStateStore::migrate(&migrator_conn)
        .with_subscriber(logs.subscriber())
        .await
        .unwrap();
    assert!(
        logs.text()
            .contains("added the verification_class column; 4 existing rows left NULL"),
        "the migration must say what it did to the existing rows: {}",
        logs.text()
    );
    let admin = PgStateStore::connect(&migrator_conn)
        .await
        .expect("migrate adds the verification_class column");
    use vtop_core::state_machine::BatchState;
    for (id, state) in [
        ("pre-664", BatchState::Verified),
        ("pre-664-committed", BatchState::SourceCommitted),
        ("pre-664-failed", BatchState::Failed),
        ("pre-664-batching", BatchState::Batching),
    ] {
        let legacy = admin.get_batch(id).await.unwrap().unwrap();
        assert_eq!(
            (legacy.state, legacy.verification_class),
            (state, None),
            "{id}: a row written before #664 keeps its state and has no class"
        );
    }
    // A second migrate adds nothing and says nothing.
    let logs = Captured::default();
    PgStateStore::migrate(&migrator_conn)
        .with_subscriber(logs.subscriber())
        .await
        .unwrap();
    assert!(!logs.text().contains("added the verification_class column"));
    // A value this build does not know (a newer release's) reads as unknown,
    // not as an error and not as either class.
    admin
        .execute_raw("UPDATE batches SET verification_class = 'quantum' WHERE batch_id = 'pre-664'")
        .await
        .unwrap();
    let newer = admin.get_batch("pre-664").await.unwrap().unwrap();
    assert_eq!(newer.verification_class, None);

    // #664: a schema a NEWER release migrated - its marker above the newest
    // this release reads - is refused, by startup and by this release's
    // migrate (which would install its older trigger over it), with the
    // marker and the versions this release reads in the message. Nothing is
    // changed: the newer body is still there afterwards.
    admin
        .execute_raw(
            "CREATE OR REPLACE FUNCTION vtop_enforce_commit_after_verify() RETURNS trigger AS $fn$
-- vtop-state-schema 4
BEGIN
    RETURN NEW;
END;
$fn$ LANGUAGE plpgsql;",
        )
        .await
        .unwrap();
    for (what, refused) in [
        (
            "startup",
            PgStateStore::connect(&migrator_conn)
                .await
                .err()
                .map(|e| e.to_string()),
        ),
        (
            "migrate",
            PgStateStore::migrate(&migrator_conn)
                .await
                .err()
                .map(|e| e.to_string()),
        ),
    ] {
        let refused = refused.unwrap_or_else(|| panic!("{what} accepted a newer schema marker"));
        for part in [
            "schema marker 4",
            "-- vtop-state-schema 4",
            "minimum 3",
            "newest 3",
            "newer release",
        ] {
            assert!(
                refused.contains(part),
                "{what}: {part:?} missing from: {refused}"
            );
        }
    }
    admin
        .execute_raw(
            "DO $chk$ BEGIN \
               IF (SELECT prosrc FROM pg_catalog.pg_proc WHERE proname = 'vtop_enforce_commit_after_verify') \
                  NOT LIKE '%vtop-state-schema 4%' THEN \
                 RAISE EXCEPTION 'the refused migrate replaced the newer trigger body'; \
               END IF; \
             END $chk$; \
             DROP FUNCTION vtop_enforce_commit_after_verify() CASCADE;",
        )
        .await
        .unwrap();
    PgStateStore::migrate(&migrator_conn).await.unwrap();

    // Nor is a trigger that exists by name but whose function body has no
    // current schema marker, or one a restore left disabled. The old body is
    // the trigger function as released before #625 (no marker):
    // - restored from an old archive: the function dropped and recreated;
    // - installed by an older release's `vtopctl migrate`: CREATE OR REPLACE
    //   over the current function. A marker kept in a COMMENT ON FUNCTION
    //   survived this, which is why the marker lives in the body.
    macro_rules! pre_625_function {
        () => {
            " FUNCTION vtop_enforce_commit_after_verify() RETURNS trigger AS $fn$ \
             BEGIN \
                 IF NEW.state = 'source_committed' THEN \
                     IF TG_OP = 'INSERT' THEN \
                         RAISE EXCEPTION 'commit before verified: batch % inserted directly as source_committed', NEW.batch_id \
                             USING ERRCODE = 'check_violation'; \
                     ELSIF OLD.state <> 'verified' THEN \
                         RAISE EXCEPTION 'commit before verified: batch % is %', OLD.batch_id, OLD.state \
                             USING ERRCODE = 'check_violation'; \
                     END IF; \
                 END IF; \
                 RETURN NEW; \
             END; \
             $fn$ LANGUAGE plpgsql;"
        };
    }
    for (setup, what) in [
        (
            concat!(
                "DROP FUNCTION vtop_enforce_commit_after_verify() CASCADE; CREATE",
                pre_625_function!(),
                " CREATE TRIGGER trg_commit_after_verify BEFORE INSERT OR UPDATE ON batches \
                     FOR EACH ROW EXECUTE FUNCTION vtop_enforce_commit_after_verify();"
            ),
            "a pre-#625 function restored from an old archive",
        ),
        (
            concat!("CREATE OR REPLACE", pre_625_function!()),
            "a pre-#625 function installed over the current one by an older migrate",
        ),
        (
            "ALTER TABLE batches DISABLE TRIGGER trg_commit_after_verify",
            "a disabled trigger",
        ),
    ] {
        PgStateStore::migrate(&migrator_conn).await.unwrap();
        let admin = PgStateStore::connect(&migrator_conn).await.unwrap();
        admin.execute_raw(setup).await.unwrap();
        let err = match PgStateStore::connect(&migrator_conn).await {
            Ok(_) => panic!("runtime connect must reject {what}"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("vtopctl migrate"),
            "{what} must give migration guidance: {err}"
        );
    }
    // `vtopctl migrate` repairs each: it installs the marked function and
    // re-creates the trigger enabled.
    PgStateStore::migrate(&migrator_conn).await.unwrap();
    PgStateStore::connect(&migrator_conn)
        .await
        .expect("the current schema must pass startup");

    // #567: the other half of the marker/CHECK pair. The release between #625
    // and #567 installs its marker-2 body with CREATE OR REPLACE and leaves
    // the state CHECK alone, so a re-run of ITS migrate leaves the widened
    // CHECK under an older marker. Refused all the same (the reverse, a
    // current marker over an old CHECK, is refused above).
    PgStateStore::migrate(&migrator_conn).await.unwrap();
    let admin = PgStateStore::connect(&migrator_conn).await.unwrap();
    admin
        .execute_raw(
            "CREATE OR REPLACE FUNCTION vtop_enforce_commit_after_verify() RETURNS trigger AS $fn$
-- vtop-state-schema 2
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
$fn$ LANGUAGE plpgsql;",
        )
        .await
        .unwrap();
    let err = match PgStateStore::connect(&migrator_conn).await {
        Ok(_) => panic!("runtime connect must reject a marker-2 body over the widened CHECK"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("vtopctl migrate"),
        "a marker-2 body must give migration guidance: {err}"
    );
    PgStateStore::migrate(&migrator_conn).await.unwrap();
    PgStateStore::connect(&migrator_conn)
        .await
        .expect("migrate restores the marker-3 body");

    // The deployment identity applies DDL once, then creates a deliberately
    // narrow role. PostgreSQL's default PUBLIC schema privileges are revoked so
    // the test does not accidentally pass through an implicit grant.
    PgStateStore::migrate(&migrator_conn).await.unwrap();
    let admin = PgStateStore::connect(&migrator_conn).await.unwrap();
    admin
        .execute_raw(
            "TRUNCATE batches; \
             REVOKE CREATE ON SCHEMA public FROM PUBLIC; \
             DO $role$ BEGIN \
               IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vtop_runtime_test') THEN \
                 CREATE ROLE vtop_runtime_test LOGIN PASSWORD 'vtop-runtime-test'; \
               END IF; \
             END $role$; \
             ALTER ROLE vtop_runtime_test LOGIN PASSWORD 'vtop-runtime-test'; \
             REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM vtop_runtime_test; \
             REVOKE CREATE ON SCHEMA public FROM vtop_runtime_test; \
             GRANT USAGE ON SCHEMA public TO vtop_runtime_test; \
             GRANT SELECT, INSERT, UPDATE ON TABLE batches TO vtop_runtime_test;",
        )
        .await
        .unwrap();

    let store = PgStateStore::connect(&runtime_conn)
        .await
        .expect("the DML-only runtime identity must pass schema readiness");

    // Prove this is genuinely a non-owner, non-DDL identity. These statements
    // are harmless even if a broken grant lets one through; the unique names
    // and WHERE FALSE avoid modifying ledger data.
    for (sql, operation) in [
        (
            "CREATE TABLE vtop_runtime_must_not_create (id INT)",
            "CREATE",
        ),
        (
            "ALTER TABLE batches ADD COLUMN vtop_runtime_must_not_alter TEXT",
            "ALTER",
        ),
        ("DELETE FROM batches WHERE FALSE", "DELETE"),
        ("TRUNCATE batches", "TRUNCATE"),
    ] {
        let err = match store.execute_raw(sql).await {
            Ok(()) => panic!("runtime identity unexpectedly gained {operation}"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("permission denied")
                || err.to_string().contains("must be owner"),
            "expected {operation} to be denied, got: {err}"
        );
    }
    PgStateStore::migrate(&runtime_conn)
        .await
        .expect_err("the runtime identity must not be able to run migrations");

    // ---- 2. Same behavioural contract as every backend, using DML only ----
    test_battery::run_all(&store).await;

    // ---- 2a. The claim fence (#557): two runtime connections stand for two
    // engines on one shared ledger.
    let second = PgStateStore::connect(&runtime_conn).await.unwrap();
    test_battery::run_fencing_battery(&store, &second).await;

    // ---- 2b. Prune contract (#128) runs as the MAINTENANCE identity ----
    // The runtime role is denied DELETE by design (asserted above), so the
    // owner connection exercises pruning — mirroring production, where
    // `vtopctl prune-ledger` runs with a maintenance identity.
    let maintenance = PgStateStore::connect(&migrator_conn).await.unwrap();
    test_battery::run_prune_battery(&maintenance).await;
    store
        .prune_committed_history("2100-01-01T00:00:00+00:00", 10)
        .await
        .expect_err("the runtime identity must be denied ledger pruning");

    // ---- 2c. #664: the verification class survives a restart ----
    // A second runtime connection, with no claims of its own, stands for the
    // engine after a restart. Last of the batteries: it claims every
    // claimable row.
    test_battery::verification_class_before_restart(&store).await;
    let restarted = PgStateStore::connect(&runtime_conn).await.unwrap();
    test_battery::verification_class_after_restart(&restarted).await;

    // ---- 2d. Review of #664: `vtopctl migrate` adding a column while an
    // engine runs. The runtime store's statements are prepared and cached
    // per connection; the migration identity adds a column; the same store
    // must keep reading whole rows. The column is dropped again after.
    test_battery::seed_rows_for_every_read_path(&store).await;
    test_battery::every_read_path_returns_whole_rows(&store).await;
    maintenance
        .execute_raw("ALTER TABLE batches ADD COLUMN vtop_test_later TEXT")
        .await
        .unwrap();
    test_battery::every_read_path_returns_whole_rows(&store).await;
    maintenance
        .execute_raw("ALTER TABLE batches DROP COLUMN vtop_test_later")
        .await
        .unwrap();

    // ---- 3. Defense in depth: the DB trigger rejects a bypass ----
    // Write straight to the table (skipping update_batch_state's guard) to force
    // the illegal commit-before-verify the trigger exists to catch.
    store
        .execute_raw(
            "INSERT INTO batches \
             (batch_id, tenant, source_type, source_name, format, state, \
              progress_start_json, progress_end_json, created_at, updated_at) \
             VALUES ('trig-1','default','kafka','app','cef','batching','{}','{}','t','t')",
        )
        .await
        .unwrap();

    let err = store
        .execute_raw("UPDATE batches SET state = 'source_committed' WHERE batch_id = 'trig-1'")
        .await
        .expect_err("the trigger must reject commit-before-verified at the DB layer");
    assert!(
        err.to_string().contains("commit before verified"),
        "expected the trigger's message, got: {err}"
    );

    // Going through verified first is allowed.
    store
        .execute_raw("UPDATE batches SET state = 'verified' WHERE batch_id = 'trig-1'")
        .await
        .unwrap();
    store
        .execute_raw("UPDATE batches SET state = 'source_committed' WHERE batch_id = 'trig-1'")
        .await
        .expect("commit from verified must be allowed");

    // ---- 4. The trigger fires on INSERT too, not just UPDATE ----
    // A batch cannot be born committed; a direct INSERT of a source_committed row
    // would otherwise slip past an UPDATE-only trigger.
    let err = store
        .execute_raw(
            "INSERT INTO batches \
             (batch_id, tenant, source_type, source_name, format, state, \
              progress_start_json, progress_end_json, created_at, updated_at) \
             VALUES ('born-committed','default','kafka','app','cef','source_committed','{}','{}','t','t')",
        )
        .await
        .expect_err("inserting a source_committed row must be rejected");
    assert!(
        err.to_string().contains("commit before verified"),
        "expected the trigger to reject a born-committed insert, got: {err}"
    );

    // ---- 5. Nor born in any other state but batching (#625) ----
    // Born verified, a row could then be committed without ever having been
    // verified: the UPDATE to source_committed passes the trigger above.
    let err = store
        .execute_raw(
            "INSERT INTO batches \
             (batch_id, tenant, source_type, source_name, format, state, \
              progress_start_json, progress_end_json, created_at, updated_at) \
             VALUES ('born-verified','default','kafka','app','cef','verified','{}','{}','t','t')",
        )
        .await
        .expect_err("inserting a verified row must be rejected");
    assert!(
        err.to_string().contains("must be inserted as batching"),
        "expected the trigger to reject a born-verified insert, got: {err}"
    );
}

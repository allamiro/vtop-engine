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

use vtop_state::pg_store::PgStateStore;
use vtop_state::test_battery;

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
}

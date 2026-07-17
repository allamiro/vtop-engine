//! The Postgres backend must pass the SAME behavioural contract as SQLite, and
//! its database-level invariant trigger must actually reject a bypass.
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
    let Ok(conn) = std::env::var("VTOP_TEST_POSTGRES") else {
        eprintln!("VTOP_TEST_POSTGRES not set; skipping the Postgres backend test");
        return;
    };

    let store = PgStateStore::connect(&conn).await.unwrap();
    // The battery assumes a FRESH, empty store. TRUNCATE (not DROP) keeps the
    // schema + trigger in place; it is not an UPDATE, so the trigger stays quiet.
    store.execute_raw("TRUNCATE batches").await.unwrap();

    // ---- 1. Same behavioural contract as every backend ----
    test_battery::run_all(&store).await;

    // ---- 2. Defense in depth: the DB trigger rejects a bypass ----
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

    // ---- 3. The trigger fires on INSERT too, not just UPDATE ----
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

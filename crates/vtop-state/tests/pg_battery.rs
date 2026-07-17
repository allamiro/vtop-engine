//! The Postgres backend must pass the SAME behavioural contract as SQLite, and
//! its database-level invariant trigger must actually reject a bypass.
//!
//! Runs only when `VTOP_TEST_POSTGRES` points at a reachable Postgres (the CI
//! `postgres backend (battery)` job sets it; locally, point it at a throwaway
//! container). Without it the test skips rather than fails, so a plain
//! `cargo test` on a dev box needs no database.
//!
//! One test, run sequentially: the two checks share the single `batches` table,
//! so splitting them into parallel tests would race on drop/recreate.
//!
//! Build with `--features postgres,test-support`.

use sqlx::postgres::PgPoolOptions;
use vtop_state::pg_store::PgStateStore;
use vtop_state::test_battery;

#[tokio::test]
async fn postgres_backend_passes_battery_and_enforces_invariant_at_db() {
    let Ok(conn) = std::env::var("VTOP_TEST_POSTGRES") else {
        eprintln!("VTOP_TEST_POSTGRES not set; skipping the Postgres backend test");
        return;
    };

    // ---- Fresh schema (the battery assumes an empty store) ----
    // Drop first so run_all sees an empty table; the trigger + function go with
    // it via CASCADE, and PgStateStore::connect recreates schema + trigger.
    let pool = PgPoolOptions::new().connect(&conn).await.unwrap();
    sqlx::query("DROP TABLE IF EXISTS batches CASCADE")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    let store = PgStateStore::connect(&conn).await.unwrap();

    // ---- 1. Same behavioural contract as every backend ----
    test_battery::run_all(&store).await;

    // ---- 2. Defense in depth: the DB trigger rejects a bypass ----
    // Write straight to the table (skipping update_batch_state's guard) to force
    // the illegal commit-before-verify the trigger exists to catch.
    let pool = PgPoolOptions::new().connect(&conn).await.unwrap();
    sqlx::query(
        r#"INSERT INTO batches
           (batch_id, tenant, source_type, source_name, format, state,
            progress_start_json, progress_end_json, created_at, updated_at)
           VALUES ('trig-1','default','kafka','app','cef','batching','{}','{}','t','t')"#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let err =
        sqlx::query("UPDATE batches SET state = 'source_committed' WHERE batch_id = 'trig-1'")
            .execute(&pool)
            .await
            .expect_err("the trigger must reject commit-before-verified at the DB layer");
    assert!(
        err.to_string().contains("commit before verified"),
        "expected the trigger's message, got: {err}"
    );

    // Going through verified first is allowed.
    sqlx::query("UPDATE batches SET state = 'verified' WHERE batch_id = 'trig-1'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE batches SET state = 'source_committed' WHERE batch_id = 'trig-1'")
        .execute(&pool)
        .await
        .expect("commit from verified must be allowed");
    pool.close().await;
}

//! The messages an operator reads when the state store meets a schema it
//! changes or refuses (#664). Its own test binary, with one process-wide log
//! capture: a capture scoped to one test thread misses events when other
//! tests in the same process race it for the tracing callsite cache.

use std::sync::{Arc, Mutex, OnceLock};
use vtop_core::state_machine::BatchState;
use vtop_state::{SqliteStateStore, StateStore};

/// Every log line this process writes, as JSON.
fn logs() -> &'static Captured {
    static LOGS: OnceLock<Captured> = OnceLock::new();
    LOGS.get_or_init(|| {
        let captured = Captured::default();
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .json()
                .with_max_level(tracing::Level::INFO)
                .with_writer(captured.clone())
                .finish(),
        )
        .expect("the only global subscriber in this binary");
        captured
    })
}

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Captured {
    /// The captured lines that mention `needle`.
    fn lines_with(&self, needle: &str) -> Vec<String> {
        String::from_utf8_lossy(&self.0.lock().unwrap())
            .lines()
            .filter(|line| line.contains(needle))
            .map(str::to_owned)
            .collect()
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

/// A SQLite ledger from before #664 at `path`, holding `rows` VERIFIED rows.
async fn pre_664_file(path: &std::path::Path, rows: usize) {
    use sqlx::ConnectOptions;
    let mut conn = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .connect()
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
    .execute(&mut conn)
    .await
    .unwrap();
    let marker = r#"{"source_type":"kafka","topic":"app","partition":0,"start_offset":0,"end_offset":1,"consumer_group":"g"}"#;
    for i in 0..rows {
        sqlx::query(
            "INSERT INTO batches (batch_id, tenant, source_type, source_name, format, \
             state, progress_start_json, progress_end_json, created_at, updated_at) \
             VALUES (?, 'default', 'kafka', 'app', 'cef', 'verified', ?, ?, 't', 't')",
        )
        .bind(format!("legacy-{i}"))
        .bind(marker)
        .bind(marker)
        .execute(&mut conn)
        .await
        .unwrap();
    }
}

/// Opening a SQLite ledger from before #664 says what it did: the column it
/// added, how many existing rows it left without a class, what that means
/// for them, and that nothing needs doing - with the count and the backend in
/// structured fields. Opening it again says nothing.
#[tokio::test]
async fn adding_the_class_column_to_an_existing_sqlite_ledger_says_what_it_did() {
    let logs = logs();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pre-664.db");
    pre_664_file(&path, 3).await;
    let url = format!("sqlite://{}", path.display());
    let said = |logs: &Captured| logs.lines_with("added the verification_class column; 3 existing");
    let before = said(logs).len();
    let store = SqliteStateStore::connect(&url).await.unwrap();
    let lines = said(logs);
    assert_eq!(lines.len(), before + 1, "{lines:?}");
    let line = lines.last().unwrap();
    for part in [
        "3 existing rows left NULL (verification class unknown)",
        "re-check of the stored object",
        "Nothing to do",
        r#""backend":"SQLite""#,
        r#""existing_rows":3"#,
        r#""column":"verification_class""#,
        r#""level":"INFO""#,
    ] {
        assert!(line.contains(part), "{part:?} missing from: {line}");
    }
    let row = store.get_batch("legacy-0").await.unwrap().unwrap();
    assert_eq!(
        (row.state, row.verification_class),
        (BatchState::Verified, None)
    );
    drop(store);
    SqliteStateStore::connect(&url).await.unwrap();
    assert_eq!(
        said(logs).len(),
        before + 1,
        "a second open adds nothing and says nothing"
    );
}

/// Opening a SQLite ledger a v0.9.0 engine left (schema version 1: the
/// verification class, no seal instant) says what it did (#626): the column
/// it added, how many rows it left without a seal instant, what that means
/// for a replay of one of them, and that nothing needs doing. The file is
/// still at version 1 afterwards, so v0.9.0 can open it again. Opening it
/// again says nothing.
#[tokio::test]
async fn adding_the_sealed_at_column_to_a_version_1_sqlite_ledger_says_what_it_did() {
    use sqlx::ConnectOptions;
    let logs = logs();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("v1.db");
    pre_664_file(&path, 2).await;
    {
        let mut conn = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .connect()
            .await
            .unwrap();
        sqlx::raw_sql(
            "ALTER TABLE batches ADD COLUMN verification_class TEXT; PRAGMA user_version = 1;",
        )
        .execute(&mut conn)
        .await
        .unwrap();
    }
    let url = format!("sqlite://{}", path.display());
    let said = |logs: &Captured| logs.lines_with("added the sealed_at column; 2 existing");
    let before = said(logs).len();
    let store = SqliteStateStore::connect(&url).await.unwrap();
    let lines = said(logs);
    assert_eq!(lines.len(), before + 1, "{lines:?}");
    let line = lines.last().unwrap();
    for part in [
        "2 existing rows left NULL (seal instant unknown)",
        "filed under the hour it is replayed in",
        "may write a second object",
        "Nothing to do",
        r#""backend":"SQLite""#,
        r#""existing_rows":2"#,
        r#""column":"sealed_at""#,
        r#""level":"INFO""#,
    ] {
        assert!(line.contains(part), "{part:?} missing from: {line}");
    }
    assert!(
        logs.lines_with("added the verification_class column; 2 existing")
            .is_empty(),
        "only the column that was missing is reported"
    );
    let row = store.get_batch("legacy-0").await.unwrap().unwrap();
    assert_eq!((row.state, row.sealed_at), (BatchState::Verified, None));
    drop(store);
    let mut conn = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&path)
        .connect()
        .await
        .unwrap();
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(version, 1, "a new nullable column raises no version");
    drop(conn);
    SqliteStateStore::connect(&url).await.unwrap();
    assert_eq!(
        said(logs).len(),
        before + 1,
        "a second open adds nothing and says nothing"
    );
}

/// #626: a `sealed_at` that is not an RFC 3339 instant is read as unknown,
/// and the operator is told which row, what it means for its replay and
/// what to do - with the row and the column as fields, and not the value,
/// which no writer produced.
#[tokio::test]
async fn a_seal_instant_that_is_not_one_is_read_as_unknown_and_says_so() {
    use sqlx::ConnectOptions;
    let logs = logs();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("odd.db");
    pre_664_file(&path, 1).await;
    let url = format!("sqlite://{}", path.display());
    let store = SqliteStateStore::connect(&url).await.unwrap();
    let mut conn = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&path)
        .connect()
        .await
        .unwrap();
    sqlx::query("UPDATE batches SET sealed_at = 'not-a-time' WHERE batch_id = 'legacy-0'")
        .execute(&mut conn)
        .await
        .unwrap();
    let said = |logs: &Captured| logs.lines_with("ledger row legacy-0 holds a sealed_at");
    let before = said(logs).len();
    let row = store.get_batch("legacy-0").await.unwrap().unwrap();
    assert_eq!(row.sealed_at, None);
    // Read again: rate-limited, not said again.
    store.list_batches().await.unwrap();
    store.get_batch("legacy-0").await.unwrap();
    let lines = said(logs);
    assert_eq!(
        lines.len(),
        before + 1,
        "said once, not on every read: {lines:?}"
    );
    let line = lines.last().unwrap();
    for part in [
        "is not an RFC 3339 instant",
        "it is read as unknown",
        "filed under the hour it runs in",
        "Nothing to do unless this repeats",
        r#""batch_id":"legacy-0""#,
        r#""column":"sealed_at""#,
        r#""stored_len":10"#,
        r#""suppressed":0"#,
        r#""level":"WARN""#,
    ] {
        assert!(line.contains(part), "{part:?} missing from: {line}");
    }
    assert!(
        !line.contains("not-a-time"),
        "the stored value is not logged: {line}"
    );
}

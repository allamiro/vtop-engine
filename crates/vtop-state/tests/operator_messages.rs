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

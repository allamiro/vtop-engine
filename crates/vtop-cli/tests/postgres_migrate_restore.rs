//! The PostgreSQL schema on the server major a CI leg runs (#723).
//!
//! - `vtopctl migrate`, run twice on an empty database, leaves the schema as
//!   its first run left it: same tables, triggers, functions and constraints.
//! - A ledger dumped with `pg_dump` restores into a migrated schema in the
//!   ways docs/POSTGRES_DEPLOYMENT.md ("Restoring the ledger from a backup")
//!   documents, and in no other: a data-only restore is refused by the
//!   commit rule's trigger; a restore with the trigger disabled is possible
//!   only for the tables' owner; and no engine starts on the result until
//!   `vtopctl migrate` has re-enabled the trigger.
//!
//! Invariant (safety): a row in a state past `batching` enters a migrated
//! ledger only through the trigger or by its owner's explicit act, and the
//! runtime role can neither disable the trigger nor commit a row that was
//! never verified, after a restore as before it.
//!
//! Runs when `VTOP_TEST_POSTGRES` names a role that may create databases and
//! roles (CI: the service container's superuser; the managed-service job:
//! the instance's admin user) on a THROWAWAY server: it drops and creates the
//! `vtop_test_*` databases and the roles below. Also reads
//! `VTOP_TEST_POSTGRES_RUNTIME` (the narrow runtime role, as `pg_battery`
//! does) and `VTOP_TEST_PG_EXEC`, the command prefix that runs the PostgreSQL
//! client tools of the server's own major (CI: `docker exec -i <service
//! container>`), because `pg_dump` refuses a newer server and a newer
//! `pg_dump` writes settings an older server rejects. Without
//! `VTOP_TEST_POSTGRES` the tests skip. Build with `--features postgres`.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use vtop_core::state_machine::BatchState;
use vtop_state::test_battery::sample_record;
use vtop_state::{BatchPatch, PgStateStore, StateStore, VerificationClass};

/// The role a restore runs as: it owns the migrated tables and is not a
/// superuser, as a deployment's migration identity is.
const OWNER: (&str, &str) = ("vtop_restore_owner", "vtop-restore-owner");

struct Pg {
    admin: String,
    runtime: (String, String),
    exec: Vec<String>,
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A name or password the tests put into SQL text.
fn plain(what: &str, value: &str) -> String {
    assert!(
        !value.is_empty()
            && value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
        "{what} must be letters, digits, '_' or '-' for this test to quote it, got {value:?}"
    );
    value.to_owned()
}

/// `url` with its database replaced by `db`, and its user and password by
/// `user` when one is given. Query parameters (`sslmode`, `sslrootcert`)
/// are kept.
fn url_with(url: &str, user: Option<(&str, &str)>, db: &str) -> String {
    let (scheme, rest) = url
        .split_once("://")
        .unwrap_or_else(|| panic!("VTOP_TEST_POSTGRES is not a postgres:// URL"));
    let (authority_path, query) = match rest.split_once('?') {
        Some((a, q)) => (a, Some(q)),
        None => (rest, None),
    };
    let authority = authority_path
        .split_once('/')
        .map_or(authority_path, |(a, _)| a);
    let (creds, host) = match authority.rsplit_once('@') {
        Some((c, h)) => (Some(c), h),
        None => (None, authority),
    };
    let creds = match (user, creds) {
        (Some((u, p)), _) => format!("{u}:{p}@"),
        (None, Some(c)) => format!("{c}@"),
        (None, None) => String::new(),
    };
    let query = query.map_or(String::new(), |q| format!("?{q}"));
    format!("{scheme}://{creds}{host}/{db}{query}")
}

/// The user and password in a URL's authority.
fn url_user(url: &str) -> (String, String) {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let authority = rest.split(['/', '?']).next().unwrap_or_default();
    let creds = authority
        .rsplit_once('@')
        .map(|(c, _)| c)
        .expect("VTOP_TEST_POSTGRES_RUNTIME must carry the runtime role's user and password");
    let (user, password) = creds
        .split_once(':')
        .expect("VTOP_TEST_POSTGRES_RUNTIME must carry the runtime role's password");
    (
        plain("the runtime role's name", user),
        plain("the runtime role's password", password),
    )
}

impl Pg {
    fn from_env() -> Option<Self> {
        let Ok(admin) = std::env::var("VTOP_TEST_POSTGRES") else {
            eprintln!(
                "VTOP_TEST_POSTGRES not set; skipping the PostgreSQL migrate and restore tests"
            );
            return None;
        };
        let runtime = std::env::var("VTOP_TEST_POSTGRES_RUNTIME").expect(
            "VTOP_TEST_POSTGRES_RUNTIME must name the narrow runtime role when the PostgreSQL tests run",
        );
        let exec = std::env::var("VTOP_TEST_PG_EXEC").expect(
            "VTOP_TEST_PG_EXEC must name the command that runs the PostgreSQL client tools of \
             the server's major (CI: `docker exec -i <service container>`) when VTOP_TEST_POSTGRES \
             is set",
        );
        let exec: Vec<String> = exec.split_whitespace().map(str::to_owned).collect();
        assert!(!exec.is_empty(), "VTOP_TEST_PG_EXEC is empty");
        let pg = Self {
            admin,
            runtime: url_user(&runtime),
            exec,
        };
        pg.client_matches_server();
        Some(pg)
    }

    /// Run a client tool through the prefix, feeding it `stdin`.
    fn client(&self, args: &[&str], stdin: Option<&[u8]>) -> Output {
        let mut child = Command::new(&self.exec[0])
            .args(&self.exec[1..])
            .args(args)
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("could not run {:?}: {e}", self.exec));
        // Written from another thread: a tool that reports errors while it
        // still reads its input must not block on a full stderr pipe.
        let feeder = stdin.map(|input| {
            let mut pipe = child.stdin.take().unwrap();
            let input = input.to_vec();
            std::thread::spawn(move || {
                let _ = pipe.write_all(&input);
            })
        });
        let out = child.wait_with_output().unwrap();
        if let Some(feeder) = feeder {
            feeder.join().unwrap();
        }
        out
    }

    /// `psql` with each statement as its own `-c`, so `CREATE DATABASE`
    /// does not run inside a transaction block. Returns the unaligned output.
    fn psql(&self, url: &str, statements: &[&str]) -> String {
        let mut args = vec![
            "psql",
            "--no-psqlrc",
            "-v",
            "ON_ERROR_STOP=1",
            "-tA",
            "-d",
            url,
        ];
        for statement in statements {
            args.extend(["-c", statement]);
        }
        let out = self.client(&args, None);
        assert!(
            out.status.success(),
            "psql {statements:?} failed: {}",
            text(&out.stderr)
        );
        text(&out.stdout).trim().to_owned()
    }

    /// The client tools must be the server's major: the CI leg's name, its
    /// service image and the tools that dump and restore must all agree.
    fn client_matches_server(&self) {
        let out = self.client(&["pg_dump", "--version"], None);
        assert!(
            out.status.success(),
            "pg_dump --version: {}",
            text(&out.stderr)
        );
        let version = text(&out.stdout);
        let client: u32 = version
            .split_whitespace()
            .nth(2)
            .and_then(|v| v.split('.').next())
            .and_then(|m| m.parse().ok())
            .unwrap_or_else(|| panic!("unreadable pg_dump version: {version}"));
        let server: u32 = self
            .psql(&self.admin, &["SHOW server_version_num"])
            .parse()
            .expect("server_version_num is a number");
        assert_eq!(
            client,
            server / 10000,
            "VTOP_TEST_PG_EXEC runs the client tools of PostgreSQL {client} against a \
             PostgreSQL {} server ({server}); point it at the server's own major",
            server / 10000
        );
    }

    /// Drop and create database `name`; its URL for the admin role.
    fn fresh_database(&self, name: &str) -> String {
        let drop = format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)");
        let create = format!("CREATE DATABASE {name}");
        self.psql(&self.admin, &[&drop, &create]);
        url_with(&self.admin, None, name)
    }

    /// A role `(user, password)` that may log in and holds nothing else.
    fn role(&self, (user, password): (&str, &str)) {
        let user = plain("a role name", user);
        let password = plain("a role password", password);
        self.psql(
            &self.admin,
            &[&format!(
                "DO $role$ BEGIN \
                   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{user}') THEN \
                     CREATE ROLE {user} LOGIN PASSWORD '{password}'; \
                   END IF; \
                 END $role$; \
                 ALTER ROLE {user} LOGIN PASSWORD '{password}'"
            )],
        );
        // Asserted, not set: naming SUPERUSER or NOSUPERUSER in ALTER ROLE
        // needs a superuser, which a managed service's admin is not.
        let rolsuper = self.psql(
            &self.admin,
            &[&format!(
                "SELECT rolsuper FROM pg_roles WHERE rolname = '{user}'"
            )],
        );
        assert_eq!(
            rolsuper, "f",
            "role {user} is a superuser, so it would prove nothing about the owner's \
             and the runtime role's own privileges; drop it or use another server"
        );
    }

    /// A new database `name` set up as POSTGRES_DEPLOYMENT.md says: migrated
    /// by `vtopctl migrate` as the (non-superuser) owner, with the runtime
    /// grants applied. Returns the owner's and the runtime role's URLs.
    fn migrated_target(&self, config: &Path, name: &str) -> (String, String) {
        let admin = self.fresh_database(name);
        self.role(OWNER);
        let (runtime_user, runtime_password) = (self.runtime.0.as_str(), self.runtime.1.as_str());
        self.role((runtime_user, runtime_password));
        let owner = OWNER.0;
        self.psql(
            &admin,
            &[
                "REVOKE CREATE ON SCHEMA public FROM PUBLIC",
                &format!("GRANT USAGE, CREATE ON SCHEMA public TO {owner}"),
            ],
        );
        let owner_url = url_with(&self.admin, Some(OWNER), name);
        vtopctl_migrate(config, &owner_url);
        self.psql(
            &owner_url,
            &[&format!(
                "GRANT USAGE ON SCHEMA public TO {runtime_user}; \
                 GRANT SELECT, INSERT, UPDATE ON TABLE batches TO {runtime_user}; \
                 GRANT SELECT, INSERT ON TABLE audit_events TO {runtime_user}; \
                 GRANT USAGE ON SEQUENCE audit_events_seq_seq TO {runtime_user}"
            )],
        );
        let runtime_url = url_with(&self.admin, Some((runtime_user, runtime_password)), name);
        (owner_url, runtime_url)
    }

    /// The schema as `pg_dump --schema-only` prints it, without the
    /// `\restrict` lines whose key is random per dump (pg_dump 17.6, 16.10,
    /// 15.14 and 14.19 on).
    fn schema(&self, url: &str) -> String {
        let out = self.client(&["pg_dump", "--schema-only", "-d", url], None);
        assert!(
            out.status.success(),
            "pg_dump --schema-only: {}",
            text(&out.stderr)
        );
        text(&out.stdout)
            .lines()
            .filter(|l| !l.starts_with("\\restrict ") && !l.starts_with("\\unrestrict "))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The ledger's rows, as a custom-format data-only archive.
    fn dump_data(&self, url: &str) -> Vec<u8> {
        let out = self.client(
            &["pg_dump", "--format=custom", "--data-only", "-d", url],
            None,
        );
        assert!(
            out.status.success(),
            "pg_dump --data-only: {}",
            text(&out.stderr)
        );
        out.stdout
    }

    fn restore(&self, url: &str, flags: &[&str], archive: &[u8]) -> Output {
        let target = format!("--dbname={url}");
        let mut args = vec!["pg_restore"];
        args.extend(flags);
        args.push(&target);
        self.client(&args, Some(archive))
    }

    fn count(&self, url: &str, table: &str) -> u64 {
        self.psql(url, &[&format!("SELECT count(*) FROM {table}")])
            .parse()
            .unwrap()
    }
}

fn write_config(dir: &Path) -> PathBuf {
    let config = dir.join("config.yaml");
    let input = dir.join("in.log");
    std::fs::write(&input, "").unwrap();
    let yaml = format!(
        r#"engine:
  name: vtop-pg-restore-test
  tenant: default
  state_store: {{ env: VTOP_STATE_STORE }}
  work_dir: {work}
  log_level: info
batching:
  max_records: 200
  max_bytes: 104857600
  max_batch_age_seconds: 60
  idle_poll_interval_ms: 2000
compression:
  type: gzip
  level: 1
checksum:
  algorithm: sha256
sources:
  file:
    enabled: true
    paths:
      - "{input}"
upload:
  backend: mock
  bucket: "telemetry-data"
  prefix: "pg-restore-test"
  region: us-east-1
"#,
        work = dir.join("work").display(),
        input = input.display(),
    );
    std::fs::write(&config, yaml).unwrap();
    config
}

/// `vtopctl migrate` as an operator runs it, with the state store's URL in
/// the variable the config names.
fn vtopctl_migrate(config: &Path, url: &str) {
    let out = Command::new(env!("CARGO_BIN_EXE_vtopctl"))
        .args(["migrate", "--config", config.to_str().unwrap()])
        .env("VTOP_STATE_STORE", url)
        .env_remove("VTOP_AUDIT_LOG")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "vtopctl migrate failed: {}{}",
        text(&out.stdout),
        text(&out.stderr)
    );
    assert!(
        text(&out.stdout).contains("state-store migration complete (postgresql)"),
        "vtopctl migrate did not report a PostgreSQL migration: {}",
        text(&out.stdout)
    );
}

#[tokio::test]
async fn vtopctl_migrate_run_twice_leaves_the_schema_as_the_first_run_left_it() {
    let Some(pg) = Pg::from_env() else { return };
    let url = pg.fresh_database("vtop_test_migrate_twice");
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path());

    vtopctl_migrate(&config, &url);
    let first = pg.schema(&url);
    for part in [
        "CREATE TABLE public.batches",
        "CREATE TABLE public.audit_events",
        "CONSTRAINT state_enum CHECK",
        "'superseded'",
        "-- vtop-state-schema 3",
        "CREATE TRIGGER trg_commit_after_verify BEFORE INSERT OR UPDATE ON public.batches",
        "CREATE TRIGGER trg_audit_events_append_only BEFORE DELETE OR UPDATE ON public.audit_events",
        "CREATE TRIGGER trg_audit_events_no_truncate BEFORE TRUNCATE ON public.audit_events",
    ] {
        assert!(
            first.contains(part),
            "the first migrate left a schema without {part:?}:\n{first}"
        );
    }
    assert!(
        !first.contains("DISABLE TRIGGER"),
        "the first migrate left a trigger disabled:\n{first}"
    );
    let audit_rows = pg.count(&url, "audit_events");

    vtopctl_migrate(&config, &url);
    let second = pg.schema(&url);
    if first != second {
        let changed: Vec<String> = first
            .lines()
            .zip(second.lines())
            .filter(|(a, b)| a != b)
            .map(|(a, b)| format!("- {a}\n+ {b}"))
            .collect();
        panic!(
            "a second `vtopctl migrate` changed the schema ({} lines before, {} after); first \
             differing lines:\n{}",
            first.lines().count(),
            second.lines().count(),
            changed.into_iter().take(20).collect::<Vec<_>>().join("\n")
        );
    }
    // What a second run does change is the audit trail: its two records
    // (#694), and nothing else.
    assert_eq!(
        pg.count(&url, "audit_events"),
        audit_rows + 2,
        "each `vtopctl migrate` writes its attempt and its outcome to audit_events"
    );
}

/// Rows in each state a ledger restore carries: committed, verified (by
/// size only), failed and still batching.
async fn write_ledger(store: &PgStateStore) {
    for id in ["restore-committed", "restore-verified"] {
        store.save_batch_state(&sample_record(id)).await.unwrap();
        for state in [
            BatchState::Sealed,
            BatchState::Compressed,
            BatchState::Checksummed,
            BatchState::ObjectUploaded,
            BatchState::ManifestUploaded,
        ] {
            store
                .update_batch_state(id, state, &BatchPatch::default())
                .await
                .unwrap();
        }
    }
    store
        .mark_verified("restore-committed", VerificationClass::Strong)
        .await
        .unwrap();
    store
        .mark_source_committed("restore-committed")
        .await
        .unwrap();
    store
        .mark_verified("restore-verified", VerificationClass::SizeOnly)
        .await
        .unwrap();
    store
        .save_batch_state(&sample_record("restore-failed"))
        .await
        .unwrap();
    store
        .mark_failed("restore-failed", "upload failed")
        .await
        .unwrap();
    store
        .save_batch_state(&sample_record("restore-batching"))
        .await
        .unwrap();
}

/// The ledger as the store reads it: every batch row and every audit row.
async fn ledger(url: &str) -> (Vec<String>, Vec<vtop_state::AuditRow>) {
    // The tables' owner reads them whatever the runtime grants are.
    let store = PgStateStore::connect(url).await.unwrap();
    let mut batches: Vec<String> = store
        .list_batches()
        .await
        .unwrap()
        .iter()
        .map(|row| format!("{row:?}"))
        .collect();
    batches.sort();
    let audit = store.list_audit(0, 10_000).await.unwrap();
    (batches, audit)
}

#[tokio::test]
async fn a_ledger_restores_into_a_migrated_schema_only_past_the_commit_rule() {
    let Some(pg) = Pg::from_env() else { return };
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path());

    // Each case below restores into a database of its own: the stores a
    // case opens keep pooled sessions after it, and dropping a database
    // with another role's sessions open takes pg_signal_backend, which a
    // managed service's admin may not hold. The next run drops and
    // creates each again, after this one's sessions have closed.
    // The ledger that was backed up: migrated by `vtopctl migrate`, so its
    // audit trail holds that command's records, then written by an engine.
    let source = pg.fresh_database("vtop_test_restore_source");
    vtopctl_migrate(&config, &source);
    write_ledger(&PgStateStore::connect(&source).await.unwrap()).await;
    let expected = ledger(&source).await;
    assert_eq!(expected.0.len(), 4, "the source ledger: {:?}", expected.0);
    let archive = pg.dump_data(&source);

    // 1. A data-only restore into a migrated schema is refused by the
    //    trigger: the rows past `batching` would bypass the commit rule.
    let (owner, _) = pg.migrated_target(&config, "vtop_test_restore_refused");
    let out = pg.restore(&owner, &["--data-only"], &archive);
    let stderr = text(&out.stderr);
    assert!(
        !out.status.success() && stderr.contains("vtop_enforce_commit_after_verify"),
        "a data-only restore into a migrated schema must be refused by the commit rule's \
         trigger; pg_restore exited {:?}: {stderr}",
        out.status.code()
    );
    assert_eq!(
        pg.count(&owner, "batches"),
        0,
        "no ledger row may land past the trigger"
    );

    // 2. `--disable-triggers` is for the tables' owner: the runtime role is
    //    refused, and nothing lands.
    let (owner, runtime) = pg.migrated_target(&config, "vtop_test_restore_runtime");
    let out = pg.restore(&runtime, &["--data-only", "--disable-triggers"], &archive);
    let stderr = text(&out.stderr);
    assert!(
        !out.status.success() && stderr.contains("must be owner of table batches"),
        "the runtime role must not be able to disable the trigger; pg_restore exited {:?}: \
         {stderr}",
        out.status.code()
    );
    assert_eq!(
        pg.count(&owner, "batches"),
        0,
        "no ledger row may land through the runtime role"
    );

    // 3. As the owner, with `--disable-triggers`, every row lands as it was.
    //    First the owner clears the two audit records `vtopctl migrate` just
    //    wrote into the new database: they hold `seq` 1 and 2, which the
    //    restored trail also holds, and its COPY would fail on the primary
    //    key (#694; the step the deployment doc gives).
    let (owner, runtime) = pg.migrated_target(&config, "vtop_test_restore_owner");
    clear_the_migrations_audit_rows(&pg, &owner);
    let out = pg.restore(&owner, &["--data-only", "--disable-triggers"], &archive);
    assert!(
        out.status.success(),
        "the owner's data-only restore with --disable-triggers failed: {}",
        text(&out.stderr)
    );
    assert_eq!(
        ledger(&owner).await,
        expected,
        "the restored ledger differs from the one dumped"
    );
    after_the_restore(&config, &owner, &runtime).await;

    // 4. Without superuser and without --disable-triggers: the owner
    //    disables the commit rule's trigger by name, restores, and no engine
    //    starts until `vtopctl migrate` has re-enabled it.
    let (owner, runtime) = pg.migrated_target(&config, "vtop_test_restore_by_name");
    clear_the_migrations_audit_rows(&pg, &owner);
    pg.psql(
        &owner,
        &["ALTER TABLE batches DISABLE TRIGGER trg_commit_after_verify"],
    );
    let out = pg.restore(&owner, &["--data-only"], &archive);
    assert!(
        out.status.success(),
        "the owner's data-only restore with the trigger disabled failed: {}",
        text(&out.stderr)
    );
    let refused = match PgStateStore::connect(&runtime).await {
        Ok(_) => panic!("an engine started on a ledger whose commit-rule trigger is disabled"),
        Err(e) => e.to_string(),
    };
    assert!(
        refused.contains("vtopctl migrate"),
        "a disabled trigger must send the operator to `vtopctl migrate`: {refused}"
    );
    after_the_restore(&config, &owner, &runtime).await;
    // Read after the migration (startup refuses the disabled trigger before
    // it), which appended its own two audit records to the restored chain.
    let (batches, audit) = ledger(&owner).await;
    assert_eq!(
        batches, expected.0,
        "the restored rows differ from the ones dumped"
    );
    assert_eq!(
        audit[..audit.len().saturating_sub(2)],
        expected.1[..],
        "the restored audit trail differs from the one dumped"
    );
}

/// The owner's `TRUNCATE audit_events` before a data-only restore: only the
/// two records of the `vtopctl migrate` that created the database are there
/// (they stay in the log stream and the audit file), and only the owner may
/// remove them.
fn clear_the_migrations_audit_rows(pg: &Pg, owner: &str) {
    let actions = pg.psql(
        owner,
        &["SELECT string_agg(action, ',' ORDER BY seq) FROM audit_events"],
    );
    assert_eq!(
        actions, "ledger.migrate,ledger.migrate",
        "a new database holds only its migration's two audit records"
    );
    pg.psql(owner, &["TRUNCATE audit_events"]);
}

/// After a restore: `vtopctl migrate`, then the engine's startup check
/// passes for the runtime role, and that role still cannot commit a row that
/// was never verified, nor disable the trigger.
async fn after_the_restore(config: &Path, owner: &str, runtime: &str) {
    vtopctl_migrate(config, owner);
    // The migration's records follow the restored ones, and the chain holds.
    let out = Command::new(env!("CARGO_BIN_EXE_vtopctl"))
        .args(["audit", "verify", "--config", config.to_str().unwrap()])
        .env("VTOP_STATE_STORE", owner)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "`vtopctl audit verify` after the restore: {}{}",
        text(&out.stdout),
        text(&out.stderr)
    );
    let engine = PgStateStore::connect(runtime)
        .await
        .expect("after the restore and `vtopctl migrate`, the runtime role passes startup");
    let row = engine
        .get_batch("restore-committed")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (row.state, row.verification_class),
        (BatchState::SourceCommitted, Some(VerificationClass::Strong))
    );
    let err = engine
        .execute_raw(
            "UPDATE batches SET state = 'source_committed' WHERE batch_id = 'restore-batching'",
        )
        .await
        .expect_err("the trigger must refuse committing a restored row that was never verified");
    assert!(
        err.to_string().contains("commit before verified"),
        "expected the trigger's refusal, got: {err}"
    );
    let err = engine
        .execute_raw("ALTER TABLE batches DISABLE TRIGGER trg_commit_after_verify")
        .await
        .expect_err("the runtime role must not be able to disable the trigger");
    assert!(
        err.to_string().contains("must be owner"),
        "expected an ownership refusal, got: {err}"
    );
}
